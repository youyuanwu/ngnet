//! What a multi-fragment offer costs.
//!
//! # The mechanism
//!
//! The HTTP/3 layer offers a stream's pending output as a vector of slices. For the first write
//! of a request that has a body ready, that vector has two fragments: the QPACK-encoded headers
//! and the body. QMux takes them one at a time, and each `try_write_stream` *begins* a record of
//! its own, so the headers land in a record with room for sixteen thousand more bytes and the
//! body starts in the next one. What a call does after that record has changed -- it now fills
//! records until the buffer or the peer's window stops it -- but it still cannot reach back into
//! the record the previous fragment closed, which is why the fragment boundary is still a record
//! boundary and why this file still has something to measure.
//!
//! Write coalescing changed what that costs without changing the record count. It used to be
//! worse: the second fragment was *refused* while a record was outstanding, so the offer ended
//! after its first fragment and the body waited for a later offer. The outbound buffer now holds
//! several records, so the second fragment is accepted in the same offer and the two records
//! leave in one write. What remains is the record itself: a
//! fragmented offer still costs one record per fragment where a vectored push would pack them
//! into one.
//!
//! Phase 3 (vectored record input) is the phase that should break the assertions below: an
//! offer whose fragments go into one record leaves no undersized header record behind, and the
//! request costs one record fewer.
//!
//! # Why this is measured as a difference between two requests
//!
//! The obvious test -- look for a small write immediately followed by a full one -- passes for
//! the wrong reason. The connection preamble ends with a small record too, so when the header
//! record disappears the *preamble's* last record inherits the position immediately before the
//! body's first full one and the shape is still there. That was not a hypothesis: an
//! experimental patch that packed both fragments into one record left an adjacency-only version
//! of this test passing, which is exactly the regression this file exists to catch.
//!
//! So the cost is measured as a difference instead. Two exchanges run over identical
//! configurations, one request with a body and one without. Everything they have in common --
//! the connection preamble, the control streams, the QPACK stream setup -- cancels, and what is
//! left is what carrying the body cost. The header record does not cancel: the body-less
//! request has one too. So the difference counts the body's own records *plus the one the
//! separate header fragment forced*, and packing the fragments takes that one away.
//!
//! # Why the difference is counted in bytes rather than in writes
//!
//! It used to be counted in writes, because one record was one write. Coalescing broke that
//! instrument rather than the thing it measured: a write now carries whatever records a transmit
//! pass produced, and one offer now fills records to the buffer's ceiling, so the body run's
//! writes hold many records apiece. Bytes are what survived. Every
//! record's length prefix and dwnx's per-record STREAM header are in the byte total, so a record
//! that stops existing removes its framing from it, which is the direction this test needs to be
//! sensitive in. The figure is exact rather than approximate, and the arithmetic that accounts
//! for every byte of it is written out at [`BODY_COST`].
//!
//! # A correction to the plan's wording
//!
//! The implementation plan describes the cost as "one record *and one driver turn* per
//! fragment". The record is what measurement shows; the driver turn is not, and the assertions
//! here say only what was measured. `drain` in `crates/ngnet-qmux-h3/src/transmit.rs` takes up to
//! `MAX_OFFERS` = 64 offers per pass, and since coalescing it no longer flushes between them, so
//! a fragment costs a record and nothing else at all: not a turn, and since the buffer has room
//! for it, not even a re-offer.

mod transmit_harness;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use ngnet_qmux_h3_tests::{Payload, drain, ok, pattern};
use transmit_harness::{Turns, collected};

/// A whole record, prefix included. It is the unit the body cost below is counted in; the
/// largest *write* is now the outbound ceiling rather than a record -- see `driver_writes.rs`.
const RECORD: usize = DEFAULT_MAX_RECORD_SIZE as usize;

/// The body the second request carries.
///
/// Small enough that every write it causes can be accounted for individually below, large
/// enough to need several records so that the record after the header record is a full one.
const BODY: usize = 64 * 1024;

/// How many full records this body takes today, measured rather than derived.
///
/// 64 KiB of body, less the two-byte length prefix and the STREAM frame header dwnx puts in
/// each record, leaves four full records and a remainder of a few dozen bytes. Arithmetic here
/// would be a second implementation of dwnx's framing rather than an observation of it.
const FULL_RECORDS: usize = 4;

/// How many records the body costs, over what a body-less request costs.
///
/// The four full records above and the remainder record the body ends in. The header record and
/// the end-of-stream record do not appear in the difference because the body-less request pays
/// for both of those too -- which is the point of subtracting one run from the other, and also
/// what makes the figure sensitive to the separate header fragment in the direction that
/// matters.
///
/// Today the headers occupy a record of their own and contribute nothing to the body's five, so
/// the difference is five. Once Phase 3 packs the two fragments, the headers ride inside the
/// body's first record and the body's last byte still lands inside the same fifth record: the
/// body run then has five records where the body-less run has one, and this figure falls to
/// four. That is a deliberate change to this constant, not an adjustment.
const BODY_RECORDS: usize = FULL_RECORDS + 1;

/// What carrying the body costs in bytes over carrying none, measured rather than derived.
///
/// Every byte of it is accounted for, because a figure this size is otherwise indistinguishable
/// from the payload and would not notice a record appearing or disappearing:
///
/// - `4 x 16382` = 65 528, the four full records;
/// - `54`, the remainder record the body ends in;
/// - `5`, the extra bytes a five-digit `content-length` costs the header record, which is the
///   only part of the header record that does *not* cancel between the two runs.
///
/// Phase 3 lowers it by one record's framing -- the length prefix and dwnx's STREAM header, a
/// single-digit number of bytes here -- because the header record stops existing. That is a small
/// difference and it is an exact one, which is why the constant is exact.
const BODY_COST: usize = 4 * RECORD + 54 + 5;

/// The largest a record carrying nothing but a request's headers can be here.
///
/// Taken from the body-less run, where the header record is still a write of its own: 67 bytes,
/// a length prefix, dwnx's STREAM frame header and QPACK's encoding of the request. The body
/// run's is a handful of bytes larger for the longer `content-length` and is no longer visible on
/// its own, since it now travels in the same write as the body's first records. The bound is
/// loose because the exact figure is QPACK's business and this test is not about QPACK; what
/// matters is that it is nowhere near a full record, which is what makes it evidence of a
/// fragment that was given a record of its own rather than of a record that simply filled.
const HEADER_RECORD: usize = 128;

/// Runs one request over a fresh connection and reports what the client's byte stream saw.
///
/// Both calls use the same configuration and the same server, so everything up to the request
/// itself is identical between them; that is what makes the difference between their write
/// counts attributable to the body.
fn exchange(body: Bytes) -> (Bytes, Turns) {
    let (client_io, server_io) = stream_pair();
    // Taken before the stream is moved into the connection: the log is a handle to shared
    // state, and there is no way to reach the stream again once the connection owns it.
    let log = client_io.write_log();
    let clock = TestClock::new();
    // Raised for the same reason as in `driver_writes.rs`: a pass cut short by flow control
    // would split the request's records across turns for a reason unrelated to what is being
    // measured. Both ends get the same configuration, because a QMux end's transport
    // parameters are what it permits its peer.
    let transport = TransportConfig::new()
        .initial_max_stream_data(8 << 20)
        .initial_max_data(16 << 20);
    let http = HttpConfig::default();

    let serving = ngnet_qmux_h3::serve_with(
        server_io,
        clock.clone(),
        |request| async move {
            let (_parts, incoming) = request.into_parts();
            let received = drain(incoming).await.expect("the request body");
            // Reported back rather than asserted here: a panic inside the handler would
            // surface as a panic in whichever poll happened to be running it, and the report
            // would name the harness rather than the mismatch.
            ok(Bytes::from(received.len().to_string()))
        },
        transport,
        http,
    )
    .expect("serving");

    let (sender, connection) =
        ngnet_qmux_h3::connect_with::<_, _, Payload>(client_io, clock, transport, http)
            .expect("a client");

    let request = Request::builder()
        .method("POST")
        .uri("https://qmux.test/upload")
        // One chunk, so the layer has the whole body available at the moment it has the
        // headers and the first offer is the two-fragment one this test is about. A body
        // delivered in pieces would offer its first piece after the headers had already gone
        // out, and there would be no second fragment to refuse.
        .body(Full::new(body))
        .expect("a request");

    Turns::drive(&log, connection, serving, async move {
        let response = sender.send_request(request).await.expect("a response");
        collected(response.into_body()).await
    })
}

#[test]
fn a_two_fragment_offer_still_costs_a_record_of_its_own() {
    let (empty_echo, empty) = exchange(Bytes::new());
    let (body_echo, body) = exchange(pattern(BODY));
    assert_eq!(
        (empty_echo, body_echo),
        (Bytes::from("0"), Bytes::from(BODY.to_string())),
        "the server did not receive what was sent, so the records counted below are not the \
         cost of the transfer this test claims to measure"
    );

    // The two runs share everything up to the request, and the shared part is identified by
    // agreement rather than by a hard-coded count: how many records the connection preamble
    // takes is the HTTP/3 layer's business and this test should not have an opinion about it.
    let shared = empty
        .lengths
        .iter()
        .zip(&body.lengths)
        .take_while(|(left, right)| left == right)
        .count();
    assert!(
        shared >= 1 && shared < body.lengths.len(),
        "the two runs should agree on the connection preamble and then diverge at the request, \
         and instead agree on {shared} of {} and {} writes. Without a shared prefix the \
         difference below is not attributable to the body. Body-less run: {:?}; body run: {:?}",
        empty.lengths.len(),
        body.lengths.len(),
        empty.lengths,
        body.lengths
    );

    // The first write that is not part of the preamble. Before coalescing it was the header
    // record alone, because the body fragment offered alongside it was refused while that
    // record was outstanding. It now carries the header record and the body's first records
    // together, which is what the buffer is for -- and it is why the cost below is counted in
    // bytes: this write is one write and several records.
    let first = body.lengths[shared];
    assert!(
        first > RECORD,
        "the write after the preamble was {first} bytes, no larger than a single \
         {RECORD}-byte record, so the header record and the body's first record did not travel \
         together. Either the outbound buffer is being flushed between records again, or the \
         offer is being cut short after its first fragment as it was before coalescing"
    );

    let bytes = |lengths: &[usize]| lengths.iter().sum::<usize>();
    let cost = bytes(&body.lengths) - bytes(&empty.lengths);
    assert_eq!(
        cost, BODY_COST,
        "carrying {BODY} bytes cost {cost} bytes more than carrying none, where {BODY_COST} is \
         today's figure: {FULL_RECORDS} full records, one remainder record, and the five bytes \
         the longer content-length adds to the header record. Phase 3 (vectored record input) is \
         expected to break this by packing the headers into the body's first record, which \
         removes one record and its framing from the total. Body-less run wrote {:?}; body run \
         wrote {:?}",
        empty.lengths, body.lengths
    );

    // The byte figure above is a total; this is the record count inside it. The body's own
    // records occupy every byte of the cost except the header record's five, and dividing by a
    // full record recovers how many there were -- four that filled and one that did not, which
    // is the figure Phase 3 lowers.
    let body_record_bytes = cost - 5;
    let full = body_record_bytes / RECORD;
    let remainder = body_record_bytes % RECORD;
    assert_eq!(
        (full, full + usize::from(remainder > 0)),
        (FULL_RECORDS, BODY_RECORDS),
        "the body should occupy {FULL_RECORDS} full records and one remainder, \
         {BODY_RECORDS} in all; {body_record_bytes} bytes of records divide into {full} full \
         records and {remainder} left over"
    );

    // The count above says the header record is there; this says it is avoidable rather than
    // merely present. The headers and the body's last record fit together inside one record, so
    // packing the fragments would have shifted every subsequent byte by the headers' length and
    // still ended inside the same final record -- one record fewer for the same bytes, rather
    // than the same records rearranged. The header record's size is taken from the body-less
    // run, where it is still a write of its own; the body run's is five bytes larger and the
    // margin here is three orders of magnitude wider than that.
    let header = empty.lengths[shared];
    assert!(
        header <= HEADER_RECORD && header + remainder <= RECORD,
        "the header record ({header}) and the body's last record ({remainder}) do not fit \
         inside one {RECORD}-byte record, so this workload no longer demonstrates that the \
         header record is an avoidable cost"
    );
}
