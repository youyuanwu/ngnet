//! What a multi-fragment offer costs, now that it costs as little as it can.
//!
//! # The mechanism
//!
//! The HTTP/3 layer offers a stream's pending output as a vector of slices. For the first write
//! of a request that has a body ready, that vector has two fragments: the QPACK-encoded headers
//! and the body. QMux submits the whole vector to dwnx in one call --
//! `Connection::try_write_stream_vectored` over `dwnx_conn_writev_stream` -- so the fragments
//! share records and the boundary between them costs nothing. The headers ride inside the
//! body's records instead of occupying an undersized one of their own.
//!
//! **These assertions used to pin the opposite.** Until Phase 3 the join called
//! `try_write_stream` once per slice, and a call *begins* a record, so the headers landed in a
//! record with sixteen thousand bytes of room left in it and the body started in the next one.
//! This file was written then, to pin that cost so that removing it would show. It has been
//! inverted rather than deleted, because the cost can come back: anything that goes back to a
//! call per fragment, or that stops packing after the first fragment, restores the record and
//! its framing, and the figures below are what notices.
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
//! request has one too, and still does, because a request with no body offers one fragment and
//! one fragment is one record however it is submitted. So the difference counts the body's own
//! records *plus whatever the separate header fragment forced* -- which is now nothing, and
//! that is the assertion.
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
//! # Why the establishment here is a count
//!
//! Spec FR-021 accepts a mechanism established by a count, and the eight bytes and one record
//! below are that: properties of the code rather than of the machine it ran on.
//!
//! The original argument was stronger than the evidence supported, and the correction is worth
//! keeping. The Phase 2 screen put the saving at one write out of two at 1 KiB, **one out of six
//! at 64 KiB**, and one out of sixty-six at 1 MiB. The case for not timing this quoted the first
//! and the last against a drift of 0.5% to 5%; the 64 KiB point is around seventeen percent and
//! does not sit inside that band. A timing was in fact taken --
//! `docs/benchmarks/data/xeon-8370c-azure/07-qmux-per-commit-attribution.md` -- and reported
//! -7.7% at 64 KiB against a control worst of 5.18%, which is outside the band and marginal.
//! The count remains the establishment; the timing is weak evidence in the same direction.
//!
//! # A correction to the plan's wording
//!
//! The implementation plan describes the cost as "one record *and one driver turn* per
//! fragment". The record is what measurement showed; the driver turn was not, and the assertions
//! here say only what was measured. `drain` in `crates/ngnet-qmux-h3/src/transmit.rs` takes up to
//! `MAX_OFFERS` = 64 offers per pass, and since coalescing it no longer flushes between them, so
//! a fragment cost a record and nothing else at all: not a turn, and since the buffer had room
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

/// How many records the body's write holds, measured rather than derived.
///
/// Four full records and the one it ends in. The same figure as before the fragments were
/// packed -- the body did not change size -- but the *write* that carries them holds five
/// records now where it held six, because the header record that used to sit in front of them
/// is gone. That sixth record is what this file exists to keep away.
const BODY_RECORDS: usize = FULL_RECORDS + 1;

/// What a request's headers cost when they had a record to themselves, measured.
///
/// The body run's header record as it was before the fragments were packed: 67 bytes in the
/// body-less run, plus the five a five-digit `content-length` adds.
const HEADER_RECORD_BYTES: usize = 72;

/// What the same headers cost now that they share the body's records, measured.
///
/// The difference between this and [`HEADER_RECORD_BYTES`] is [`RECORD_FRAMING`], and it is the
/// whole of what Phase 3 saved on this workload: the headers still cost their own bytes, and
/// they no longer cost a record's worth of framing on top.
const HEADER_PAYLOAD: usize = 64;

/// A record's own overhead: its two-byte length prefix and dwnx's STREAM frame header.
///
/// Derived from the two figures above rather than asserted independently, because it is their
/// difference that matters and stating it twice would let the two disagree.
const RECORD_FRAMING: usize = HEADER_RECORD_BYTES - HEADER_PAYLOAD;

/// The bytes the remainder record held before the headers joined it.
const BODY_TAIL: usize = 54;

/// The extra bytes a five-digit `content-length` costs over a one-digit one.
///
/// The only part of the headers that does *not* cancel between the two runs.
const CONTENT_LENGTH_COST: usize = 5;

/// What carrying the body costs in bytes over carrying none, measured rather than derived.
///
/// Every byte of it is accounted for, because a figure this size is otherwise indistinguishable
/// from the payload and would not notice a record appearing or disappearing:
///
/// - `4 x 16382` = 65 528, the four full records;
/// - `54`, the bytes of the record the body ends in that are the body's own;
/// - `5`, the extra bytes a five-digit `content-length` costs;
/// - **less `8`**, the length prefix and STREAM frame header of the record the headers used to
///   need. That subtraction is Phase 3. Going back to a call per fragment puts it back, and
///   this constant is what refuses it.
///
/// It is a small difference and an exact one, which is why the constant is exact rather than a
/// bound: a bound loose enough to be comfortable would be loose enough to miss eight bytes.
const BODY_COST: usize = FULL_RECORDS * RECORD + BODY_TAIL + CONTENT_LENGTH_COST - RECORD_FRAMING;

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

/// A two-fragment offer costs no record of its own (Spec SC-009, FR-010).
///
/// The inversion of what this test used to assert. The name says what is now true; the
/// assertions are the same measurements with the figures moved by exactly one record's framing.
#[test]
fn a_two_fragment_offer_costs_no_record_of_its_own() {
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

    // The first write that is not part of the preamble. It carries the whole request: the
    // headers and the body's records together, which is what the outbound buffer is for -- and
    // it is why the cost below is counted in bytes rather than in writes.
    let first = body.lengths[shared];
    assert!(
        first > RECORD,
        "the write after the preamble was {first} bytes, no larger than a single \
         {RECORD}-byte record, so the headers and the body's first record did not travel \
         together. Either the outbound buffer is being flushed between records again, or the \
         offer is being cut short after its first fragment as it was before coalescing"
    );

    let bytes = |lengths: &[usize]| lengths.iter().sum::<usize>();
    let cost = bytes(&body.lengths) - bytes(&empty.lengths);
    assert_eq!(
        cost, BODY_COST,
        "carrying {BODY} bytes cost {cost} bytes more than carrying none, where {BODY_COST} is \
         the figure with the fragments packed: {FULL_RECORDS} full records, the {BODY_TAIL} \
         bytes of the record the body ends in, the {CONTENT_LENGTH_COST} bytes the longer \
         content-length adds, less the {RECORD_FRAMING}-byte framing of the record the headers \
         no longer need. A cost {RECORD_FRAMING} bytes higher than this means the headers are \
         back in a record of their own, which is a call per fragment. Body-less run wrote \
         {:?}; body run wrote {:?}",
        empty.lengths, body.lengths
    );

    // The byte figure above is a total; this is the record count inside the write that carries
    // the request. Every record but the last is full, so dividing recovers how many there were.
    // Before the fragments were packed this write held six records -- a 72-byte header record
    // and then the body's five. It holds five now, and the header record is the one missing.
    let full = first / RECORD;
    let remainder = first % RECORD;
    assert_eq!(
        (full, full + usize::from(remainder > 0)),
        (FULL_RECORDS, BODY_RECORDS),
        "the request's write should hold {FULL_RECORDS} full records and one remainder, \
         {BODY_RECORDS} in all; {first} bytes divide into {full} full records and {remainder} \
         left over. A sixth record here is the headers having been given one of their own"
    );

    // And this says where the headers went, rather than merely that a record is missing. The
    // record the body ends in is larger than the body's own tail by exactly what the headers
    // contribute, which is only true if the two fragments were packed into the same run of
    // records and every byte after the headers was shifted along by their length.
    assert_eq!(
        remainder,
        BODY_TAIL + HEADER_PAYLOAD,
        "the body's last record is {remainder} bytes, not the {} the body's own tail plus the \
         headers riding inside it come to. The headers are somewhere other than inside the \
         body's records",
        BODY_TAIL + HEADER_PAYLOAD
    );

    // The saving is real rather than an accounting rearrangement: the headers are nowhere near
    // a record's worth, so the record they used to occupy was almost entirely empty. Taken from
    // the body-less run, where the header record is still a write of its own -- a request with
    // no body lends one fragment, and one fragment is one record however it is submitted.
    let header = empty.lengths[shared];
    assert!(
        header <= HEADER_RECORD,
        "the header record in the body-less run is {header} bytes, which is not far enough \
         below a full {RECORD}-byte record for its removal from the body run to be a saving \
         rather than a rearrangement"
    );
    assert_eq!(
        header + CONTENT_LENGTH_COST,
        HEADER_RECORD_BYTES,
        "the header record is no longer the {HEADER_RECORD_BYTES} bytes the constants above \
         are derived from, so [`RECORD_FRAMING`] no longer means what it says and the cost \
         assertion is measuring something else"
    );
}
