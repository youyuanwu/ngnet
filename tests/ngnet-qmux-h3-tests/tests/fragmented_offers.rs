//! What a multi-fragment offer costs today.
//!
//! # The mechanism
//!
//! The HTTP/3 layer offers a stream's pending output as a vector of slices. For the first write
//! of a request that has a body ready, that vector has two fragments: the QPACK-encoded headers
//! and the body. QMux takes them one at a time and allows one record to be outstanding at a
//! time, so the first fragment goes into a record and the second is refused --
//! `try_write_stream` in `crates/ngnet-qmux/src/io/conn.rs` answers `Blocked` while the
//! outbound buffer is not empty. The offer therefore ends after its first fragment, having put
//! nineteen bytes into a record with room for sixteen thousand more, and the body starts in a
//! record of its own.
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
//! refused fragment forced*, and packing the fragments takes that one away.
//!
//! # A correction to the plan's wording
//!
//! The implementation plan describes the cost as "one record *and one driver turn* per
//! fragment". The record is what measurement shows; the driver turn is not, and the assertions
//! here say only what was measured. `drain` in `crates/ngnet-qmux-h3/src/transmit.rs` pumps the
//! connection *between* offers and takes up to `MAX_OFFERS` = 64 of them per pass, so a refused
//! fragment is re-offered later in the same pass: it costs one iteration of that bounded run --
//! one sixty-fourth of the pass's budget -- and a whole extra driver turn only when the refusal
//! falls on the pass's last offer. In this workload the whole request leaves in a single turn,
//! so the fragment costs a record and an offer and no turn at all. Phase 3 recovers both.

mod transmit_harness;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use ngnet_qmux_h3_tests::{Payload, drain, ok, pattern};
use transmit_harness::{Turns, collected};

/// A whole record, prefix included; see `driver_writes.rs` for why this is the largest write a
/// connection that writes one record at a time can issue.
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

/// What the body costs today, in records, over what a body-less request costs.
///
/// Measured at five: the four full records above and the remainder record the body ends in.
/// The header record and the end-of-stream record do not appear in the difference because the
/// body-less request pays for both of those too -- which is the point of subtracting one run
/// from the other, and also what makes the figure sensitive to the fragment refusal in the
/// direction that matters.
///
/// Today the headers occupy a record of their own and contribute nothing to the body's five,
/// so the difference is five. Once Phase 3 packs the two fragments, the headers ride inside the
/// body's first record and the body's last byte still lands inside the same fifth record: the
/// body run then has five records where the body-less run has one, and this figure falls to
/// four. That is a deliberate change to this constant, not an adjustment.
const BODY_RECORDS: usize = FULL_RECORDS + 1;

/// The largest a record carrying nothing but a request's headers can be here.
///
/// The measured figure is 24 bytes: a two-byte length prefix, dwnx's STREAM frame header and
/// nineteen bytes of QPACK. The bound is loose because the exact figure is QPACK's business and
/// this test is not about QPACK; what matters is that it is nowhere near a full record, which
/// is what makes it evidence of an offer cut short rather than of a record that simply filled.
const HEADER_RECORD: usize = 64;

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
fn today_a_two_fragment_offer_costs_a_record_of_its_own() {
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
        shared >= 1 && shared + 1 + FULL_RECORDS < body.lengths.len(),
        "the two runs should agree on the connection preamble and then diverge at the request, \
         and instead agree on {shared} of {} and {} writes. Without a shared prefix the \
         difference below is not attributable to the body. Body-less run: {:?}; body run: {:?}",
        empty.lengths.len(),
        body.lengths.len(),
        empty.lengths,
        body.lengths
    );

    // The first record that is not part of the preamble. Today it is the header record the
    // refused fragment forced; packing the fragments would make it the body's first full
    // record instead.
    let first = body.lengths[shared];
    assert!(
        first <= HEADER_RECORD && body.lengths[shared + 1] == RECORD,
        "the request's headers should occupy a record of their own -- measured at 24 bytes, \
         followed immediately by a full {RECORD}-byte body record -- because the body fragment \
         offered alongside them was refused while that record was outstanding. The record after \
         the preamble was {first} bytes and the one after it {} bytes",
        body.lengths[shared + 1]
    );

    let cost = body.total() - empty.total();
    assert_eq!(
        cost,
        BODY_RECORDS,
        "carrying {BODY} bytes cost {cost} records more than carrying none, where \
         {BODY_RECORDS} is today's figure: {FULL_RECORDS} full records and one remainder \
         record, the header and end-of-stream records having cancelled against the body-less \
         run's own. Phase 3 (vectored record input) is expected to break this by packing the \
         headers into the body's first record, which leaves the body-less run paying for a \
         header record the body run no longer has and makes the figure {}. Body-less run wrote \
         {:?}; body run wrote {:?}",
        BODY_RECORDS - 1,
        empty.lengths,
        body.lengths
    );

    // The count above says the header record is there; this says it is avoidable rather than
    // merely present. The headers and the body's last record fit together inside one record, so
    // packing the fragments would have shifted every subsequent byte by the headers' length and
    // still ended inside the same final record -- one record fewer for the same bytes, rather
    // than the same records rearranged.
    let remainder = body.lengths[shared + 1 + FULL_RECORDS];
    assert!(
        first + remainder <= RECORD,
        "the header record ({first}) and the body's last record ({remainder}) do not fit inside \
         one {RECORD}-byte record, so this workload no longer demonstrates that the header \
         record is an avoidable cost"
    );
}
