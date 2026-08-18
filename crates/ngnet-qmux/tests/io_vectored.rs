//! What a payload lent in several fragments costs, and what it must not cost.
//!
//! The HTTP/3 layer above does not hand this connection one slice. `StreamSource::write_next`
//! lends a stream's pending output as a *list* -- a request's QPACK-encoded headers and the
//! first of its body, most often -- and until the vectored form landed each of those became a
//! record of its own, because each was a separate call and a call begins a record. The tests
//! here are stated over `Connection::try_write_stream_vectored`, which is the call that made
//! the boundary between two fragments stop being a boundary between two records.
//!
//! # Why the assertions are record counts
//!
//! These assertions are counts because a count is what establishes this change. Spec FR-021
//! accepts a mechanism established by a count, and a count is a property of the code that holds
//! on every machine, which a timing on one machine is not.
//!
//! A timing was *expected* to be pointless and turned out not to be, which is recorded here
//! rather than quietly dropped. The Phase 2 screen put the saving at about one record per
//! request with a body -- one write out of two at 1 KiB, **one out of six at 64 KiB**, and one
//! out of sixty-six at 1 MiB -- and the argument for not timing it was built on the first and
//! last of those three, against a run-to-run drift of 0.5% to 5%. The middle point is the one
//! that does not fit that argument, and it is where
//! `docs/benchmarks/data/xeon-8370c-azure/07-qmux-per-commit-attribution.md` later measured
//! -7.7% against a control worst of 5.18% -- outside the band, in the noisiest step of seven,
//! and marginal rather than settled. So: the counts below are the establishment, and the timing
//! that does exist is weak evidence that this is worth more than it was predicted to be.
//!
//! # The one that is not a count
//!
//! [`a_lender_may_invalidate_its_fragments_as_soon_as_the_call_returns`] is a safety property,
//! not a performance one. dwnx copies the vectors into the record during the call and retains
//! only the destination buffer, which is what lets `ngnet-qmux-h3` declare
//! `RETAINS_BUFFERS = false`; a vectored path that held a lent fragment would make that
//! declaration a use-after-free rather than a mistake.

#![cfg(feature = "io")]

mod io_harness;

use std::io::IoSlice;

use io_harness::{
    announcement_record_configured, connected_pair, drain_written, flush, next_event, open_bidi,
    peer_writes, run, run_pair,
};
use ngnet_qmux::Role;
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection, Event, StreamWrite};

/// A window wide enough that nothing here stops for flow control.
///
/// Every test below that counts records needs the count to be a property of the packing rather
/// than of the peer's generosity, so the peer is generous.
const WIDE: u64 = 8 << 20;

/// A byte at a given offset, chosen so that a reordering is visible.
///
/// A repeated byte would let a fragment delivered twice, or in the wrong order, pass a
/// byte-for-byte comparison -- which is the one failure the resumption walk can produce.
fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i % 251) as u8).wrapping_add(seed))
        .collect()
}

/// Splits a QMux byte stream into its records, returning each record's total length.
///
/// A second implementation of the length prefix rather than a call into the framer, for the
/// same reason `io_writes.rs` keeps one: a record count taken from the code that produced the
/// records would agree with itself whatever either of them did.
///
/// # Panics
///
/// If the bytes do not end on a record boundary, which for a flushed connection means the
/// test's own arrangement is wrong rather than the connection's.
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

/// A client whose peer is the far end of its byte stream, with a stream already open.
///
/// The peer's announcement is delivered first because nothing can be opened until it arrives,
/// and everything written up to and including it is drained, so what a test reads afterwards is
/// only what its own offer produced.
fn client_with_open_stream() -> (
    Connection<TestByteStream, TestClock>,
    TestByteStream,
    ngnet_qmux::StreamId,
) {
    let (near, far) = stream_pair();
    let mut conn = Connection::client(near, TestClock::new(), Config::new()).expect("a client");
    let mut far = far;
    peer_writes(
        &mut far,
        &announcement_record_configured(
            Role::Server,
            Config::new()
                .initial_max_stream_data(WIDE)
                .initial_max_data(WIDE),
        ),
    );
    let stream = run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        flush(&mut conn).await.expect("flushing the announcement");
        stream
    });
    drain_written(&mut far);
    (conn, far, stream)
}

/// Offers `fragments` in one vectored call and returns what the peer received.
fn offer(
    conn: &mut Connection<TestByteStream, TestClock>,
    far: &mut TestByteStream,
    stream: ngnet_qmux::StreamId,
    fragments: &[&[u8]],
    fin: bool,
) -> (StreamWrite, Vec<u8>) {
    let slices: Vec<IoSlice<'_>> = fragments.iter().map(|f| IoSlice::new(f)).collect();
    let outcome = conn
        .try_write_stream_vectored(stream, &slices, fin)
        .expect("a write outcome");
    run(async {
        flush(conn).await.expect("flushing what was produced");
    });
    let produced = drain_written(far);
    (outcome, produced)
}

/// Spec SC-009, first half. Fragments that fit inside one record become one record.
///
/// The assertion that was inverted from `tests/ngnet-qmux-h3-tests/tests/fragmented_offers.rs`
/// and stated here at the level the packing happens. Three fragments, none of them close to a
/// record, and one record comes out carrying all of them -- where a call per fragment produced
/// three records, each with sixteen thousand bytes of room left in it.
#[test]
fn fragments_that_fit_inside_one_record_become_one_record() {
    let (mut conn, mut far, stream) = client_with_open_stream();
    let fragments: [&[u8]; 3] = [b"the headers", b"and the body", b"and a little more"];
    let total: usize = fragments.iter().map(|f| f.len()).sum();

    let (outcome, produced) = offer(&mut conn, &mut far, stream, &fragments, false);

    assert_eq!(
        outcome,
        StreamWrite::Accepted(total),
        "every fragment must be reported taken; a count short of the offer is what the layer \
         above reads as congestion, and it would stand the stream down for the rest of its pass"
    );
    assert_eq!(
        record_lengths(&produced).len(),
        1,
        "the three fragments produced {:?} rather than one record, so a fragment boundary is \
         still a record boundary",
        record_lengths(&produced)
    );

    let concatenated: Vec<u8> = fragments.concat();
    assert!(
        produced
            .windows(concatenated.len())
            .any(|w| w == concatenated),
        "the record does not carry the fragments in order, which is the failure the resumption \
         walk produces and the one nothing above would notice"
    );
}

/// Spec SC-009, second half. Fragments beyond one record take as few as the capacity allows.
///
/// "The minimum number of records that capacity allows" is asserted by comparison rather than
/// by arithmetic: the same bytes are sent once as a list of fragments and once as a single
/// slice, over two identically configured connections, and the two must produce the *same*
/// records. Computing the minimum instead would be a second implementation of dwnx's framing,
/// which would agree with this test and with nothing else.
#[test]
fn fragments_beyond_one_record_cost_no_more_records_than_the_same_bytes_contiguous() {
    // Six fragments, none of them a multiple of a record, so every record boundary but the
    // first falls partway through a fragment -- which is where the resumption walk is wrong if
    // it assumes whole fragments were taken.
    const FRAGMENT: usize = 10_240;
    const COUNT: usize = 6;

    let whole = pattern(FRAGMENT * COUNT, 0);
    let fragments: Vec<&[u8]> = whole.chunks(FRAGMENT).collect();

    let (mut conn, mut far, stream) = client_with_open_stream();
    let (vectored_outcome, vectored) = offer(&mut conn, &mut far, stream, &fragments, false);

    let (mut conn, mut far, stream) = client_with_open_stream();
    let (contiguous_outcome, contiguous) = offer(&mut conn, &mut far, stream, &[&whole], false);

    assert_eq!(
        (vectored_outcome, contiguous_outcome),
        (
            StreamWrite::Accepted(whole.len()),
            StreamWrite::Accepted(whole.len())
        ),
        "both offers must be taken whole, or the two runs are not carrying the same bytes"
    );
    assert_eq!(
        record_lengths(&vectored),
        record_lengths(&contiguous),
        "{} fragments totalling {} bytes produced {} records where the same bytes in one slice \
         produced {}, so the fragment boundaries cost records of their own",
        COUNT,
        whole.len(),
        record_lengths(&vectored).len(),
        record_lengths(&contiguous).len()
    );
    assert_eq!(
        vectored, contiguous,
        "the two runs produced different bytes, so packing the fragments changed the wire and \
         not merely the number of records it took"
    );
}

/// Spec SC-010. A lender may reclaim its fragments the moment the call returns.
///
/// dwnx copies each vector into the record while the call is running -- `dwnx_cpymem` inside
/// `dwnx_frame_encode_stream` -- and retains only the destination buffer. Nothing else would
/// do: `ngnet-qmux-h3` declares `RETAINS_BUFFERS = false` to the HTTP/3 layer, which is a
/// promise that the bytes are the application's again as soon as a write returns.
///
/// So the fragments are overwritten with a byte that appears nowhere in them, *before* the
/// connection is flushed. A vectored push that kept the pointers rather than the bytes would
/// put the overwriting byte on the wire.
#[test]
fn a_lender_may_invalidate_its_fragments_as_soon_as_the_call_returns() {
    let (mut conn, mut far, stream) = client_with_open_stream();

    let mut first = pattern(4_000, 1);
    let mut second = pattern(9_000, 128);
    let expected: Vec<u8> = first.iter().chain(&second).copied().collect();

    let outcome = {
        let slices = [IoSlice::new(&first), IoSlice::new(&second)];
        conn.try_write_stream_vectored(stream, &slices, false)
            .expect("a write outcome")
    };
    assert_eq!(outcome, StreamWrite::Accepted(expected.len()));

    // The lender reclaims its memory. Nothing has reached the byte stream yet: the records are
    // still in the connection's outbound buffer and only the flush below moves them, which is
    // exactly the window in which a retained pointer would be read.
    first.fill(0xff);
    second.fill(0xff);

    run(async {
        flush(&mut conn).await.expect("flushing what was produced");
    });
    let produced = drain_written(&mut far);

    assert!(
        produced.windows(expected.len()).any(|w| w == expected),
        "the bytes on the wire are not the ones that were lent, so the record was built from \
         the fragments' memory rather than from a copy of it"
    );
    assert!(
        !produced.windows(64).any(|w| w == [0xffu8; 64]),
        "a run of the byte the lender overwrote its buffers with reached the peer, which means \
         dwnx read the fragments after the call that lent them had returned"
    );
}

/// A short take stops wherever the buffer did, which is inside a fragment.
///
/// The hazard this pins is the one that is silent when it is wrong. A vectored push reports
/// **one total across every fragment**, not a count per fragment, so both the loop inside
/// `pack_vectored` and the caller resuming afterwards have to walk the list against a byte
/// count. A walk that assumed whole fragments were taken would send some bytes twice and
/// others never, and would report a count that agreed with itself -- there is nothing above it
/// to disagree.
///
/// The fragment size is deliberately not a divisor of anything the connection bounds itself
/// by, so the take stops partway through a fragment rather than neatly between two; the first
/// assertion is that it did, because a run in which it did not says nothing about resumption.
#[test]
fn a_take_that_stops_inside_a_fragment_resumes_inside_it() {
    // Larger than the outbound buffer's ceiling several times over, so the offer is stopped by
    // a bound rather than by running out.
    const FRAGMENT: usize = 7_000;
    const COUNT: usize = 20;

    let config = Config::new()
        .initial_max_stream_data(WIDE)
        .initial_max_data(WIDE);
    let (mut client, mut server) = connected_pair(config);
    let whole = pattern(FRAGMENT * COUNT, 7);

    let (first_take, received) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            let mut sent = 0usize;
            let mut first_take = None;
            while sent < whole.len() {
                // The remainder is re-offered as fragments rather than as one slice, so every
                // turn exercises the vectored path and not only the first.
                let rest = &whole[sent..];
                let slices: Vec<IoSlice<'_>> = rest.chunks(FRAGMENT).map(IoSlice::new).collect();
                match client
                    .try_write_stream_vectored(stream, &slices, false)
                    .expect("a write outcome")
                {
                    StreamWrite::Accepted(taken) => {
                        first_take.get_or_insert(taken);
                        sent += taken;
                    }
                    other => panic!("the offer was refused after the buffer drained: {other:?}"),
                }
                flush(&mut client).await.expect("flushing");
            }
            flush(&mut client).await.expect("flushing");
            first_take.expect("at least one offer was made")
        },
        async {
            let mut received: Vec<u8> = Vec::new();
            while received.len() < FRAGMENT * COUNT {
                if let Event::StreamData { data, .. } =
                    next_event(&mut server).await.expect("an event")
                {
                    received.extend_from_slice(&data);
                }
            }
            received
        },
    );

    assert!(
        first_take < whole.len(),
        "the whole payload fitted in one call, so nothing here stopped short and the \
         resumption was never exercised"
    );
    assert_ne!(
        first_take % FRAGMENT,
        0,
        "the first offer stopped at {first_take} bytes, exactly on a fragment boundary, so \
         this run exercises resumption between fragments rather than inside one"
    );
    assert_eq!(
        received, whole,
        "the peer did not receive the payload it was lent. A walk that resumed at the wrong \
         place sends some bytes twice and others never, and reports a count that agrees with \
         itself"
    );
}

/// Spec SC-033, at the level the marker is either sent or not.
///
/// Two offers that look identical to a caller and must not behave identically. Neither carries
/// a byte; one carries an end-of-stream marker, and that one is the only way a stream that has
/// finished writing is ever ended. Both are accepted rather than refused -- a refusal would
/// have the layer above stand the stream down and offer the same nothing again, which is a
/// pass repeated for no progress.
///
/// The record count is deliberately not the assertion here. A QMux record carries frames, and
/// an acknowledgement owed to the peer rides in whichever record goes out next, so "a record
/// appeared" at this level does not mean "the offer produced one". The short-circuit that
/// keeps an empty offer from reaching this call at all lives in
/// `crates/ngnet-qmux-h3/src/transmit.rs`, and the count belongs with it; what belongs here is
/// that the marker went out when it was set and did not when it was not.
#[test]
fn an_offer_of_only_empty_fragments_ends_the_stream_only_for_its_end_of_stream() {
    let empty: [&[u8]; 3] = [b"", b"", b""];

    let (mut conn, mut far, stream) = client_with_open_stream();
    let (outcome, _) = offer(&mut conn, &mut far, stream, &empty, false);
    assert_eq!(
        outcome,
        StreamWrite::Accepted(0),
        "an offer of nothing must be taken, not refused"
    );
    assert_eq!(
        conn.try_write_stream(stream, b"still open", false)
            .expect("a write outcome"),
        StreamWrite::Accepted(10),
        "the write side was ended by an offer that carried no end-of-stream marker, so an \
         empty offer now finishes a stream the application is still writing to"
    );

    let (mut conn, mut far, stream) = client_with_open_stream();
    let (outcome, _) = offer(&mut conn, &mut far, stream, &empty, true);
    assert_eq!(
        outcome,
        StreamWrite::Accepted(0),
        "an end-of-stream carrying no bytes is still an offer taken whole"
    );
    assert_eq!(
        conn.try_write_stream(stream, b"after the end", false)
            .expect("a write outcome"),
        StreamWrite::Closed,
        "the write side is still open, so the marker was dropped along with the empty \
         fragments -- and a stream that has finished writing is never ended, which leaves the \
         peer waiting out an idle timeout for a body it already has"
    );
}

/// A trailing empty fragment does not take the end-of-stream marker away from the payload.
///
/// The per-slice loop this replaced had to find the last *non-empty* slice by index, because
/// the marker rode a slice and an empty one could take it, be refused, and leave the driver
/// believing a stream had ended that QMux had sent no marker for. Nothing computes an index
/// now: an empty fragment is not submitted, and dwnx applies the marker when the data it was
/// handed fits entirely. This is the guard on that reasoning still holding.
#[test]
fn a_trailing_empty_fragment_does_not_take_the_end_of_stream_marker() {
    let (mut conn, mut far, stream) = client_with_open_stream();
    let payload = pattern(1_500, 3);
    let fragments: [&[u8]; 3] = [&payload[..500], &payload[500..], b""];

    let (outcome, produced) = offer(&mut conn, &mut far, stream, &fragments, true);

    assert_eq!(
        outcome,
        StreamWrite::Accepted(payload.len()),
        "the payload must be reported taken whole, trailing empty fragment or not"
    );
    assert_eq!(
        record_lengths(&produced).len(),
        1,
        "the offer produced {:?} rather than one record: either the empty fragment was given a \
         record of its own, or the two real fragments were not packed together",
        record_lengths(&produced)
    );

    let mut conn = conn;
    assert_eq!(
        conn.try_write_stream(stream, b"after the end", false)
            .expect("a write outcome"),
        StreamWrite::Closed,
        "the stream's write side is still open, so the end-of-stream marker did not go out \
         with the payload -- which is the failure that leaves a peer waiting for an end that \
         never comes"
    );
}

/// More fragments than one push submits still end the stream in the right place.
///
/// A push hands dwnx a fixed-size array -- sixteen entries, matching the widest offer
/// `crates/ngnet-h3/src/send.rs` can make -- so a longer list is submitted across two pushes
/// into the same record. The delicate part is the end-of-stream marker: dwnx applies it when
/// the data handed to *that call* fits entirely, so the first push must not carry it while
/// fragments are still waiting, or the stream ends with bytes unsent and the peer sees a body
/// truncated to whatever the first sixteen fragments held.
///
/// The list here is deliberately longer than the array by a margin, so the second push is not
/// the empty tail case.
#[test]
fn a_list_longer_than_one_push_still_ends_the_stream_after_its_last_fragment() {
    const FRAGMENT: usize = 100;
    const COUNT: usize = 40;

    let config = Config::new()
        .initial_max_stream_data(WIDE)
        .initial_max_data(WIDE);
    let (mut client, mut server) = connected_pair(config);
    let whole = pattern(FRAGMENT * COUNT, 19);

    let (taken, (received, ended)) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            let slices: Vec<IoSlice<'_>> = whole.chunks(FRAGMENT).map(IoSlice::new).collect();
            let taken = client
                .try_write_stream_vectored(stream, &slices, true)
                .expect("a write outcome");
            flush(&mut client).await.expect("flushing");
            taken
        },
        async {
            let mut received: Vec<u8> = Vec::new();
            loop {
                if let Event::StreamData { data, fin, .. } =
                    next_event(&mut server).await.expect("an event")
                {
                    received.extend_from_slice(&data);
                    if fin {
                        break (received, true);
                    }
                }
            }
        },
    );

    assert_eq!(
        taken,
        StreamWrite::Accepted(whole.len()),
        "{COUNT} fragments were not taken whole, so a list longer than one push submits is \
         being truncated rather than continued"
    );
    assert!(ended, "the peer never saw the stream end");
    assert_eq!(
        received,
        whole,
        "the peer received {} of the {} bytes lent: the end-of-stream marker rode a push that \
         still had fragments behind it, and dwnx ended the stream where that push stopped",
        received.len(),
        whole.len()
    );
}
