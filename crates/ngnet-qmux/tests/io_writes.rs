//! How many times a transfer reaches for the byte stream, pinned at the connection's own
//! level.
//!
//! These assertions used to pin the opposite of what they pin now. Until write coalescing
//! landed the connection wrote once per record -- it flushed, produced one record, and flushed
//! again -- and this file existed to make the removal of that behaviour fail loudly and
//! precisely. It has now been removed, so the guards are inverted: a payload costs one write
//! per *guaranteed carry* rather than one per record, and what is asserted is the arithmetic
//! that follows from `OUTBOUND_CARRY` and `OUTBOUND_CEILING` in `src/io/conn.rs`, not a figure
//! copied off a run. Each test says which mechanism produces its number, so a change that
//! moves the number has somewhere to be explained.
//!
//! # Why the count cannot be inferred
//!
//! A write count is invisible in the bytes that arrive: four records written in one call and
//! four written in four produce the identical stream, and on a real socket the difference is
//! three system calls. `TestByteStream::write_log` is what makes the difference observable; see
//! its documentation for what an entry means and for the one direction in which it can be
//! wrong.
//!
//! # Why this is the lower-level companion
//!
//! Spec FR-001 is stated over the *driver-visible transmit pass* -- the bounded run of offers
//! the HTTP/3 layer makes plus the writes they cause -- and not over the connection's internal
//! write side. A guard written only at this level can be green while the real join still writes
//! once per record, because the join pumps between offers and a flushing pump there undoes
//! everything the connection accumulates. The guard at the level FR-001 is stated over lives in
//! `tests/ngnet-qmux-h3-tests/tests/driver_writes.rs`; this file pins the mechanism underneath
//! it, where the record boundaries are visible and the arithmetic is exact.

#![cfg(feature = "io")]

mod io_harness;

use std::cell::Cell;
use std::future::poll_fn;
use std::task::{Context, Poll};

use io_harness::{
    announcement_record, counting_waker, drain_written, flush, open_bidi, peer_writes, run,
    run_pair, write_all,
};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, WriteLog, stream_pair};
use ngnet_qmux::io::{
    AsyncByteStream, Config, Connection, OUTBOUND_CARRY, OUTBOUND_CEILING, StreamOpen, StreamWrite,
    Written,
};
use ngnet_qmux::{DEFAULT_MAX_RECORD_SIZE, Role};

/// A byte stream that accepts nothing and says so in the way the contract forbids.
///
/// `Written::Accepted(0)` rather than `Written::NotNow`: the difference is the whole of Spec
/// SC-032. `NotNow` carries an obligation to wake, and this reports the one answer that does
/// not, which is why the connection has to wake itself.
struct AcceptsNothing;

/// What this stream fails with, which is never, since it never fails.
#[derive(Debug)]
struct NeverFails;

impl core::fmt::Display for NeverFails {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("this byte stream cannot fail")
    }
}

impl core::error::Error for NeverFails {}

impl AsyncByteStream for AcceptsNothing {
    type Error = NeverFails;

    fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        _buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        // Nothing ever arrives, and no waker is kept: a wake from the read side would mask
        // the one the write side is being tested for.
        Poll::Pending
    }

    fn poll_write(
        &mut self,
        _cx: &mut Context<'_>,
        _bytes: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        Poll::Ready(Ok(Written::Accepted(0)))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// The largest slice any write was offered.
///
/// The figure that says whether records travelled together: a write longer than one record is
/// a write that carried more than one, and there is no other way to see it from outside.
fn lengths_max(log: &WriteLog) -> usize {
    log.lengths().into_iter().max().unwrap_or(0)
}

/// A payload of roughly a dozen records, inside the default windows.
///
/// Twelve times the 16382-byte record limit and well under both the 256 KiB stream window and
/// the 1 MiB connection window, so the transfer completes without the peer returning any
/// credit -- which matters because a peer here is a test holding the far end of a byte stream
/// and never speaks unless told to. A payload that needed credit would stall rather than fail.
const PAYLOAD: usize = 200_000;

/// A byte at a given offset, chosen so that a reordering is visible.
///
/// A repeated byte would let a duplicated or dropped record pass unnoticed, which is the one
/// failure a byte-for-byte comparison exists to catch.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A client with the far end of its byte stream in the test's hands, and its write log.
///
/// Both the log and any cap have to be arranged here, before the connection exists: a
/// connection takes its byte stream by value and never gives it back, and construction already
/// schedules the transport-parameter announcement. This is the same reason
/// `io_harness::connected_pair_with` takes a preparation closure.
fn client_with_peer(
    prepare: impl FnOnce(&TestByteStream),
) -> (
    Connection<TestByteStream, TestClock>,
    TestByteStream,
    WriteLog,
) {
    let (near, far) = stream_pair();
    let log = near.write_log();
    prepare(&near);
    let conn = Connection::client(near, TestClock::new(), Config::new()).expect("a client");
    (conn, far, log)
}

/// Splits a QMux byte stream into its records, returning each record's total length.
///
/// Deliberately a second implementation of the length prefix rather than a call into
/// [`RecordFramer`](ngnet_qmux::io::RecordFramer): the point of the comparison below is that
/// the write boundaries coincide with the record boundaries, and taking both numbers from the
/// same code would make that comparison a tautology.
///
/// # Panics
///
/// If the bytes do not end on a record boundary, which for a connection that was flushed means
/// the test's own arrangement is wrong rather than the connection's.
fn record_lengths(bytes: &[u8]) -> Vec<usize> {
    let mut lengths = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let first = bytes[offset];
        let width = 1usize << (first >> 6);
        let mut length = u64::from(first & 0x3f);
        for byte in &bytes[offset + 1..offset + width] {
            length = (length << 8) | u64::from(*byte);
        }
        let length = usize::try_from(length).expect("a record length");
        assert!(
            offset + width + length <= bytes.len(),
            "the stream ends partway through a record; it was not flushed"
        );
        lengths.push(width + length);
        offset += width + length;
    }
    lengths
}

/// Sends a fixed payload on one stream and returns what the peer received.
///
/// The peer's transport parameters are delivered first because nothing else can happen until
/// they arrive: every stream limit is zero until a peer says otherwise. They come off a real
/// server connection rather than being handwritten, so this test cannot pass while the layer
/// and the wire disagree.
fn send_fixed_payload(
    conn: &mut Connection<TestByteStream, TestClock>,
    far: &mut TestByteStream,
    data: &[u8],
) -> Vec<u8> {
    peer_writes(far, &announcement_record(Role::Server));
    run(async {
        let stream = open_bidi(conn).await.expect("opening a stream");
        write_all(conn, stream, data, true)
            .await
            .expect("writing the payload");
        flush(conn).await.expect("flushing what was produced");
    });
    drain_written(far)
}

#[test]
fn buffered_event_and_immediate_open_calls_owe_one_forced_flush() {
    let (mut conn, mut far, log) = client_with_peer(|_| {});
    peer_writes(&mut far, &announcement_record(Role::Server));
    let stream = run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        flush(&mut conn).await.expect("flushing setup");
        stream
    });
    drain_written(&mut far);
    let before = log.writes();

    assert!(matches!(
        conn.try_write_stream(stream, b"held", false),
        Ok(StreamWrite::Accepted(4))
    ));
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    if let Poll::Ready(Err(error)) = conn.poll_next_event(&mut cx) {
        panic!("buffered event poll failed: {error}");
    }
    assert!(
        matches!(conn.try_open_bidi(), Ok(StreamOpen::Opened(_))),
        "the peer's stream allowance should permit another open"
    );
    assert_eq!(
        log.writes(),
        before,
        "buffered public calls must not empty a sub-ceiling tail"
    );
    assert!(conn.queued_output() > 0);

    assert!(conn.poll_pump(&mut cx).is_ready());
    assert_eq!(conn.queued_output(), 0);
    assert_eq!(
        log.writes(),
        before + 1,
        "the suspension flush should discharge the whole retained run"
    );
}

/// A payload costs one write per guaranteed carry, not one per record (Spec SC-001).
///
/// The two figures compared are the number of writes the byte stream saw and the number of
/// records those writes carried, and the claim is that the first is far smaller than the
/// second and no larger than the arithmetic allows. Both are consequences of named code in
/// `src/io/conn.rs`: `write_side` produces records into the outbound buffer while
/// `room_for_record` says there is room for another whole one, and flushes only to make room,
/// so a write is offered everything that accumulated since the last one. Since a record is
/// begun only while the buffer holds at most `OUTBOUND_CARRY` bytes, every write but the last
/// is offered more than the carry, and P bytes of output therefore cost at most P divided by
/// the carry, rounded up.
///
/// The bound is asserted over the bytes the peer actually received rather than over the payload
/// length, because the payload is not what is written: each record carries a length prefix and
/// stops short of the record limit, so the wire is a few hundred bytes longer than the payload
/// and a bound stated over the payload would be off by one record's worth of arithmetic in the
/// direction that hides a regression.
///
/// The announcement is deliberately outside the measurement. It is produced at construction and
/// flushed by the open, and SC-001 counts only what a pass produces for the payload it carries.
#[test]
fn a_payload_costs_one_write_per_carry_rather_than_one_per_record() {
    let (mut conn, mut far, log) = client_with_peer(|_| {});
    let data = payload(PAYLOAD);

    peer_writes(&mut far, &announcement_record(Role::Server));
    let stream = run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        flush(&mut conn).await.expect("flushing the announcement");
        stream
    });
    // Everything up to here -- the announcement and the open -- is off the measurement.
    drain_written(&mut far);
    let before = log.writes();

    run(async {
        write_all(&mut conn, stream, &data, true)
            .await
            .expect("writing the payload");
        flush(&mut conn).await.expect("flushing what was produced");
    });

    let produced = drain_written(&mut far);
    let writes = log.writes() - before;
    let records = record_lengths(&produced);

    assert!(
        records.len() > PAYLOAD / (DEFAULT_MAX_RECORD_SIZE as usize),
        "a {PAYLOAD}-byte payload cannot fit in {} records; the workload is not exercising \
         what this test claims to measure",
        records.len()
    );
    assert!(
        writes <= produced.len().div_ceil(OUTBOUND_CARRY),
        "{} bytes of output cost {writes} writes, more than the {} the {OUTBOUND_CARRY}-byte \
         guaranteed carry allows: a record was begun with less than a record's room beneath \
         the ceiling, or something flushed that was not asked to",
        produced.len(),
        produced.len().div_ceil(OUTBOUND_CARRY)
    );
    assert!(
        writes < records.len(),
        "{writes} writes for {} records is one write per record, which is the behaviour \
         coalescing removed",
        records.len()
    );
    assert!(
        lengths_max(&log) > DEFAULT_MAX_RECORD_SIZE as usize,
        "no write was offered more than one record's worth of bytes, so nothing was actually \
         coalesced: the largest write was {} bytes",
        lengths_max(&log)
    );
}

/// One offer fills records until the *buffer* stops it, not until a record does (Spec SC-001).
///
/// The guard on the half of FR-001 that lives in `try_write_stream`, and the level at which the
/// mechanism is visible: a caller with no `Context` offers a payload of many records in one
/// call and is told how many bytes were taken, and the answer has to be more than a record's
/// worth. Removing the loop in `Connection::try_write_stream` makes this fail on its first
/// assertion.
///
/// The second assertion is what makes the first mean something. A short accept must be a
/// *bound* -- the buffer at its ceiling, or credit gone -- and never a record boundary, because
/// the layer above reads a short accept as congestion and stands the stream down for the rest
/// of its pass. So the call that answered short is followed by one that offers the remainder
/// into the same full buffer, and that one must be refused outright: if it were accepted, the
/// first call stopped for a reason that was not a bound and the HTTP/3 layer above was misled
/// about why.
///
/// The third is the resumption. After the byte stream has taken what was buffered, the same
/// remainder is accepted again and the transfer is whole -- which is what says the bytes the
/// short accept left behind were neither sent nor lost.
#[test]
fn one_offer_fills_the_buffer_rather_than_a_single_record() {
    let (mut conn, mut far, _log) = client_with_peer(|_| {});
    let data = payload(PAYLOAD);

    peer_writes(&mut far, &announcement_record(Role::Server));
    let stream = run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        flush(&mut conn).await.expect("flushing the announcement");
        stream
    });
    drain_written(&mut far);

    let taken = match conn.try_write_stream(stream, &data, false) {
        Ok(StreamWrite::Accepted(taken)) => taken,
        other => panic!("an empty connection refused a payload of many records: {other:?}"),
    };
    assert!(
        taken > DEFAULT_MAX_RECORD_SIZE as usize,
        "one call took {taken} bytes, no more than the {} a single record holds, so the offer \
         stopped at a record boundary rather than at the buffer's ceiling",
        DEFAULT_MAX_RECORD_SIZE
    );
    assert!(
        taken < data.len(),
        "the whole {}-byte payload fitted, so this run says nothing about why a call stops",
        data.len()
    );
    assert!(
        conn.queued_output() > OUTBOUND_CARRY,
        "the call stopped with only {} bytes buffered, which is under the {OUTBOUND_CARRY}-byte \
         carry: whatever ended it, it was not the ceiling",
        conn.queued_output()
    );

    assert_eq!(
        conn.try_write_stream(stream, &data[taken..], false)
            .expect("a second offer"),
        StreamWrite::Blocked,
        "the buffer took more after answering short, so the short answer was a record filling \
         rather than a bound being reached -- which is exactly the reading the HTTP/3 layer \
         above cannot make and must not be given"
    );

    let mut sent = taken;
    run(async {
        while sent < data.len() {
            flush(&mut conn).await.expect("making room");
            match conn.try_write_stream(stream, &data[sent..], sent + 1 >= data.len()) {
                Ok(StreamWrite::Accepted(more)) => sent += more,
                other => panic!("the remainder was refused after the buffer drained: {other:?}"),
            }
        }
        flush(&mut conn).await.expect("flushing the remainder");
    });

    let produced = drain_written(&mut far);
    let carried: usize = record_lengths(&produced).iter().sum();
    assert_eq!(
        carried,
        produced.len(),
        "the stream does not divide into whole records, so the resumption wrote something \
         other than the records it produced"
    );
    assert_eq!(
        sent,
        data.len(),
        "the transfer did not complete, so the bytes the short accept left behind were lost \
         rather than merely deferred"
    );
}

/// A pass leaves nothing behind when it returns (Spec SC-003).
///
/// There is no flush in the body of this test, which is the point: `poll_write_stream` ends
/// with a forced flush of its own, because a caller may stop polling the moment it returns and
/// nothing else is obliged to come along and move what it produced. The assertion is made two
/// ways, since either alone can be satisfied by an implementation that is wrong in the other
/// direction: `queued_output` reports the buffer empty, and a subsequent pump produces no
/// further bytes at all.
#[test]
fn a_pass_leaves_nothing_waiting_when_it_returns() {
    let (mut conn, mut far, _log) = client_with_peer(|_| {});
    let data = payload(PAYLOAD);

    peer_writes(&mut far, &announcement_record(Role::Server));
    run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        write_all(&mut conn, stream, &data, true)
            .await
            .expect("writing the payload");
    });

    assert_eq!(
        conn.queued_output(),
        0,
        "the pass returned with bytes still in the outbound buffer, which is a driver waiting \
         on a peer that has heard nothing"
    );

    let during = drain_written(&mut far);
    run(async { flush(&mut conn).await.expect("a pump after the pass") });
    let after = drain_written(&mut far);

    assert!(
        !during.is_empty(),
        "the pass wrote nothing at all, so this proves nothing about what it left behind"
    );
    assert!(
        after.is_empty(),
        "a pump after the pass moved {} more bytes, so the pass had left them waiting",
        after.len()
    );
    // Parses the whole of what arrived, which fails unless the stream ends on a record
    // boundary: a pass that stopped mid-record would leave a truncated one here.
    record_lengths(&during);
}

/// A slow consumer does not push the buffer past the ceiling (Spec SC-004).
///
/// The byte stream takes a few kilobytes and then refuses until the far end reads, which is
/// what a backed-up transport does. Production must stop at the ceiling rather than following
/// the caller's backlog -- that bound is the answer to the first of the three arguments the old
/// one-record rule was defended by, and it is the only one of the three that is a *quantity*
/// rather than an ordering property, so it is measured rather than argued.
///
/// Two figures, and both matter. The high-water mark must stay at or below `OUTBOUND_CEILING`,
/// which is the promise; and it must reach past `OUTBOUND_CARRY`, or the byte stream was not
/// slow enough for the test to have been about anything. The mark is sampled after every write
/// the caller makes; the peak *within* a production run is covered by the debug assertion in
/// `Connection::produce`, which this test runs under.
#[test]
fn a_slow_consumer_does_not_push_the_buffer_past_the_ceiling() {
    /// Small enough that the pipe backs up almost immediately, and not a divisor of the record
    /// size, so accepts stop inside records rather than between them.
    const PIPE: usize = 3_000;

    let (mut conn, mut far, _log) = client_with_peer(|stream| stream.set_capacity(Some(PIPE)));
    let data = payload(PAYLOAD);
    peer_writes(&mut far, &announcement_record(Role::Server));

    let high_water = Cell::new(0usize);
    let sending_done = Cell::new(false);

    let sender = async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        let mut written = 0usize;
        while written < data.len() {
            let taken = poll_fn(|cx| {
                let outcome = conn.poll_write_stream(cx, stream, &data[written..], false);
                high_water.set(high_water.get().max(conn.queued_output()));
                outcome
            })
            .await
            .expect("writing the payload");
            written += taken;
        }
        flush(&mut conn).await.expect("flushing the payload");
        high_water.set(high_water.get().max(conn.queued_output()));
        sending_done.set(true);
    };

    let consumer = async {
        let mut received = Vec::new();
        let mut buffer = [0u8; 512];
        loop {
            let taken = poll_fn(|cx| match far.poll_read(cx, &mut buffer) {
                // Nothing left to read and the sender has finished, which is the only way
                // this side can know there is nothing further coming: the pipe reports the
                // end of a stream only when the writer shuts it down, and this one does not.
                Poll::Pending if sending_done.get() => Poll::Ready(0),
                Poll::Pending => Poll::Pending,
                Poll::Ready(outcome) => Poll::Ready(outcome.expect("the byte stream failed")),
            })
            .await;
            if taken == 0 {
                return received;
            }
            received.extend_from_slice(&buffer[..taken]);
        }
    };

    let (_, received) = run_pair(sender, consumer);

    assert!(
        high_water.get() <= OUTBOUND_CEILING,
        "the outbound buffer reached {} bytes against a ceiling of {OUTBOUND_CEILING}: a slow \
         peer is making this side hold the caller's backlog",
        high_water.get()
    );
    assert!(
        high_water.get() > OUTBOUND_CARRY,
        "the buffer never filled past the {OUTBOUND_CARRY}-byte carry ({} bytes at its \
         highest), so the byte stream was not slow enough for this to be a test of the bound",
        high_water.get()
    );
    let records = record_lengths(&received);
    assert!(
        records.len() > PAYLOAD / (DEFAULT_MAX_RECORD_SIZE as usize),
        "the transfer stopped short: {} records for a {PAYLOAD}-byte payload",
        records.len()
    );
}

/// A byte stream that accepts nothing is woken rather than left to stall (Spec SC-032).
///
/// `Written::Accepted(0)` is forbidden by the contract on [`AsyncByteStream::poll_write`],
/// because zero bytes accepted carries no obligation to wake and a caller offered it can only
/// spin. The connection answers it with a self-wake -- the one wake this layer issues to itself
/// -- so an implementation that breaks the rule gets a busy connection rather than a silent
/// stall. Coalescing changed how much is offered to a write and nothing about that path, and
/// this test is what says so: the outbound buffer is now several records long, and the
/// zero-accept case has to behave exactly as it did when it was one.
///
/// The stream is hand-written rather than a `TestByteStream`, which reports `Written::NotNow`
/// when it can take nothing and so cannot reach this path at all.
#[test]
fn a_byte_stream_that_accepts_nothing_is_woken_rather_than_stalled() {
    let (waker, wakes) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut conn = Connection::client(AcceptsNothing, TestClock::new(), Config::new())
        .expect("a client over a stream that takes nothing");

    for poll in 1..=4 {
        assert!(
            conn.poll_pump(&mut cx).is_pending(),
            "a pump that wrote nothing must report pending"
        );
        assert_eq!(
            wakes.count(),
            poll,
            "a write that accepted zero bytes must wake the connection itself; poll {poll} \
             registered no wake, which is a connection that stops for good"
        );
    }

    assert!(
        conn.queued_output() > 0,
        "the announcement should still be waiting, or this test wrote nothing to be refused"
    );
}

/// The octets a peer receives do not depend on how they were written.
///
/// The capture is the point rather than the assertion: later phases change the write shape, and
/// this is the equality they have to preserve. It is stated here as a comparison between two
/// runs of the same workload over byte streams with different write behaviour -- one that
/// accepts everything and one that accepts a single byte per call -- because a recorded literal
/// would pin the transport parameter encoding as well, which is not this test's claim and would
/// have to be re-recorded every time an unrelated default moved.
///
/// The one-byte run also demonstrates that the two figures the previous test compares are
/// genuinely independent: here the writes far outnumber the records and the octets are
/// nevertheless identical.
#[test]
fn the_octets_a_peer_receives_survive_a_change_of_write_shape() {
    let data = payload(PAYLOAD);

    let (mut generous, mut generous_far, generous_log) = client_with_peer(|_| {});
    let from_generous = send_fixed_payload(&mut generous, &mut generous_far, &data);

    let (mut capped, mut capped_far, capped_log) =
        client_with_peer(|stream| stream.set_write_cap(Some(1)));
    let from_capped = send_fixed_payload(&mut capped, &mut capped_far, &data);

    assert_eq!(
        from_generous, from_capped,
        "the byte sequence a peer receives changed with the shape of the writes that produced \
         it"
    );
    assert!(
        capped_log.writes() > generous_log.writes(),
        "the capped run must have written more times than the generous one, or the two runs \
         were not actually different: {} against {}",
        capped_log.writes(),
        generous_log.writes()
    );
}

/// No record on the wire is longer than the two-byte length in front of it can describe.
///
/// The assertion is cheap and the failure it guards against is not. A record is now serialised
/// straight into the outbound buffer, whose free tail is normally several records long, and
/// dwnx does not cap a record on the write path: `dwnx_qre_start` initialises the record with
/// the whole destination it is handed
/// (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_qre.c:36-41`),
/// `dwnx_qre_stream_max_datalen` bounds the payload only by what is left of that destination
/// (`:47-80`), and `dwnx_qre_final` then writes the record's length as a fixed two-byte varint
/// (`:107`) whose encoder asserts the value is below 16384 and, where that assertion is
/// compiled out, truncates it to sixteen bits
/// (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_conv.c:145-157`).
///
/// Which of those happens was checked rather than assumed. This workspace builds dwnx without
/// `NDEBUG` in either profile, so the perturbation -- handing the record writer the buffer's
/// whole tail -- aborts in debug *and* in release rather than truncating quietly. It is still
/// worth asserting on the wire: the same mistake against a dwnx built with assertions off
/// produces a peer that has lost record framing and no error anywhere, and `Conn::record`'s
/// contract refuses an over-long buffer nowhere. This is what refuses it, from the outside.
///
/// The transfer is deliberately large enough that most records begin with an empty buffer in
/// front of them -- the case where the tail is at its longest and a missing clamp would show.
/// `record_lengths` re-derives the framing from the bytes, so a truncated length would fail
/// there, inside the helper, rather than here.
#[test]
fn no_record_exceeds_what_its_own_length_prefix_can_describe() {
    let limit = DEFAULT_MAX_RECORD_SIZE as usize;
    let data = payload(PAYLOAD);

    let (mut conn, mut far, _log) = client_with_peer(|_| {});
    let received = send_fixed_payload(&mut conn, &mut far, &data);
    let lengths = record_lengths(&received);

    assert!(
        lengths.len() > 8,
        "the workload produced only {} records, which is too few for the case this guards -- a \
         missing clamp shows where the buffer's tail is longest",
        lengths.len()
    );
    for (index, length) in lengths.iter().enumerate() {
        assert!(
            *length <= limit,
            "record {index} is {length} bytes, past the {limit}-byte maximum: the record writer \
             was given more than one record's room, and a length above 16383 is truncated to \
             sixteen bits rather than refused"
        );
    }
}

/// A record's bytes are never moved after they are written (Spec FR-036, SC-036).
///
/// `copied_record_bytes` counts what reaches the outbound buffer by memcpy rather than by being
/// serialised there. It used to grow by a whole record per record -- `pack` built each one in a
/// scratch buffer and appended the result -- so a megabyte of payload cost a megabyte of
/// copying, one memcpy of up to 16382 bytes per record. It is now zero however much is sent.
///
/// The close at the end is what shows the counter can move at all: `encode_close_record` builds
/// an owned buffer, because this layer encodes a close itself where dwnx has no writer for one,
/// so those bytes are copied in and counted. An assertion of zero with nothing able to raise it
/// would pass just as well against a counter that had been disconnected.
#[cfg(debug_assertions)]
#[test]
fn producing_records_copies_nothing_while_the_close_is_copied() {
    let data = payload(PAYLOAD);

    let (mut conn, mut far, _log) = client_with_peer(|_| {});
    let received = send_fixed_payload(&mut conn, &mut far, &data);

    assert!(
        received.len() > data.len(),
        "the peer received {} bytes for a {}-byte payload, so this run did not send what it \
         meant to",
        received.len(),
        data.len()
    );
    assert_eq!(
        conn.copied_record_bytes(),
        0,
        "{} record bytes were copied into the outbound buffer for a {}-byte payload; records \
         are serialised into that buffer and nothing should move them afterwards",
        conn.copied_record_bytes(),
        data.len()
    );

    let reason = ngnet_qmux::CloseReason::application(7, b"done");
    run(async {
        poll_fn(|cx| conn.poll_close(cx, &reason)).await.ok();
    });
    let close = drain_written(&mut far);

    assert_eq!(
        conn.copied_record_bytes(),
        close.len(),
        "the close record is the one thing copied into the outbound buffer, and the counter \
         should say so exactly"
    );
    assert!(
        !close.is_empty(),
        "no close record reached the peer, so this half of the test proved nothing"
    );
}
