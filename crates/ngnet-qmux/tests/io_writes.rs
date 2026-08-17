//! How many times a transfer reaches for the byte stream, pinned at the connection's own
//! level.
//!
//! Everything here asserts what the connection does **today**, which is one write per record.
//! That is not a property worth having -- it is the property a later phase exists to remove --
//! and the assertions are written so that removing it fails here loudly and precisely, with the
//! expected figures stated as consequences of named code rather than as numbers copied off a
//! run. Phase 4 (write coalescing) is the phase expected to invert them.
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
//! write side, which satisfies the weaker property trivially by writing each record before
//! producing the next. That is exactly the behaviour being removed, so a guard written only at
//! this level could stay green through a change that left the real join writing once per
//! record. The guard at the level FR-001 is stated over lives in
//! `tests/ngnet-qmux-h3-tests/tests/driver_writes.rs`; this file pins the mechanism underneath
//! it, where the record boundaries are visible and the arithmetic is exact.

#![cfg(feature = "io")]

mod io_harness;

use io_harness::{
    announcement_record, drain_written, flush, open_bidi, peer_writes, run, write_all,
};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, WriteLog, stream_pair};
use ngnet_qmux::io::{Config, Connection};
use ngnet_qmux::{DEFAULT_MAX_RECORD_SIZE, Role};

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

/// Today: exactly one write per record, each write offered exactly that record's bytes.
///
/// The two sequences are compared element by element rather than only by length, which is what
/// makes the claim "one write per record" rather than "as many writes as records". Both figures
/// are consequences of the same mechanism in `src/io/conn.rs`: `produce` appends one record to
/// an empty outbound buffer, `flush` offers that buffer to the byte stream until it is empty,
/// and `write_side` alternates the two so that a record is fully written before the next one
/// exists. With a byte stream that accepts every write in full -- this one, with no caps set --
/// each of those flushes is a single accepted call carrying exactly one record.
///
/// **Phase 4 (write coalescing) is expected to invert this.** After it, a pass that produces
/// several records offers them in one call, so the write count drops below the record count and
/// the lengths stop matching one for one. The replacement assertion has to state its own
/// expected figure and the mechanism that produces it, not merely a smaller number.
#[test]
fn today_every_record_costs_its_own_write() {
    let (mut conn, mut far, log) = client_with_peer(|_| {});
    let data = payload(PAYLOAD);
    let received = send_fixed_payload(&mut conn, &mut far, &data);

    let records = record_lengths(&received);
    assert!(
        records.len() > PAYLOAD / (DEFAULT_MAX_RECORD_SIZE as usize),
        "a {PAYLOAD}-byte payload cannot fit in {} records; the workload is not exercising \
         what this test claims to measure",
        records.len()
    );

    assert_eq!(
        log.writes(),
        records.len(),
        "one write per record is today's behaviour: `write_side` flushes the outbound buffer \
         empty before producing into it again, so a record cannot share a write with its \
         neighbour"
    );
    assert_eq!(
        log.lengths(),
        records,
        "each write was offered exactly one record, prefix included"
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
