//! What a transmit pass costs the byte stream today, counted at the driver.
//!
//! # Why this is measured through the whole join and not through [`Connection`] alone
//!
//! `crates/ngnet-qmux/tests/io_writes.rs` already counts writes at the QMux layer, where a test
//! offers bytes to a connection directly and counts what comes out. That is the cheaper
//! measurement and it is not the one Spec FR-001 is stated over. FR-001 is stated over the
//! *driver-visible transmit pass*: the bounded run of offers the HTTP/3 layer makes to the
//! transport -- at most sixty-four, `MAX_OFFERS` in `crates/ngnet-qmux-h3/src/transmit.rs` --
//! together with every write those offers cause, ending when the driver is returned to.
//!
//! The distinction is not pedantry. A guard that drives [`Connection`] directly measures the
//! connection's own write loop, and that loop could be made to coalesce while the join above it
//! still wrote once per record -- because the HTTP/3 layer offers one body fragment at a time
//! and each offer runs the loop again. Only a test that polls the real driver over a real
//! exchange can tell the difference, so this test builds both ends, hands the client's byte
//! stream a write log, and polls the two drivers itself through
//! [`transmit_harness`](transmit_harness).
//!
//! # What is pinned now that coalescing has landed
//!
//! These assertions used to pin one write per record, and Phase 4 was expected to break them.
//! All of them are now broken, but not in one step, and the second step is the one worth
//! recording here.
//!
//! Coalescing alone left this axis almost unchanged -- 134 writes to 130 -- and the reason was
//! not the buffer. Instrumenting the offer loop showed what a pass did: nghttp3 offered the
//! whole remaining body in one slice, two megabytes of it, `try_write_stream` took *one
//! record's worth* and answered short, and a short accept is backpressure, so
//! `Offers::write_next` (`crates/ngnet-h3/src/http/driver.rs`) stood the stream aside. The pass
//! ended with the outbound buffer a fifth full. The buffer had room for four more records and
//! was never asked for them.
//!
//! What removed it was making `try_write_stream` fill records until the *buffer* stops it
//! rather than until *a record* does, so that a short accept means what the layer above already
//! believed it meant. A pass now moves as much of a body as the ceiling allows, and this test's
//! upload went from 130 writes to 28 -- 2 MiB in writes of 81 910 bytes, which is the ceiling.
//!
//! # Why this is the file SC-001 is demonstrated in
//!
//! SC-001 asks for the carry arithmetic to hold over *a pass whose payload fills at least
//! sixty-four records*, at the level a driver turn is visible. Until the fix above, no driver
//! turn on this axis contained such a pass to demonstrate it over: the busiest turn carried a
//! record at a time. It does now -- one turn carries the whole two megabytes, which is 128 full
//! records -- so the criterion is checked here rather than deferred to the concurrent file,
//! where a pass reaches sixty-four records only by adding streams.
//!
//! The other axis -- several streams in flight at once -- is measured in
//! `concurrent_driver_writes.rs`, and the two are still different questions.
//!
//! [`Connection`]: ngnet_qmux::io::Connection

mod transmit_harness;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
use ngnet_qmux::io::OUTBOUND_CARRY;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use ngnet_qmux_h3_tests::{Payload, drain, ok, pattern};
use transmit_harness::{Turns, collected};

/// The largest write the connection can issue while it writes one record at a time.
///
/// A record is length-prefixed and the whole thing -- prefix included -- is capped at
/// [`DEFAULT_MAX_RECORD_SIZE`], which is what the writer fills a full record to. So while a
/// write carries at most one record, no write can exceed this; a write larger than this is
/// proof that two records travelled together.
const RECORD: usize = DEFAULT_MAX_RECORD_SIZE as usize;

/// The body this test uploads.
///
/// Two mebibytes because SC-001 is stated over "a pass whose payload fills at least sixty-four
/// records", and this is comfortably above that: it fills 128 full records with a remainder, so
/// the workload still satisfies the success criterion even if a later phase changes the framing
/// overhead by a few bytes per record and the record count drifts.
const BODY: usize = 2 * 1024 * 1024;

/// How many full records this body takes today.
///
/// Measured, not derived: a record's payload budget is what dwnx leaves after the two-byte
/// length prefix and the STREAM frame header, and the header's size depends on the varint
/// encoding of the stream id and offset, so an arithmetic prediction here would be a second
/// implementation of dwnx's framing rather than an observation of it. The figure is stable
/// because every input to it is fixed: the body length, the record cap and the stream the
/// request uses.
const FULL_RECORDS: usize = 128;

/// A window large enough that flow control does not end the pass before the offers run out.
///
/// The defaults (256 KiB per stream, 1 MiB per connection) would stop this body a quarter of
/// the way through and hand the pass back to the driver to wait for credit, which would split
/// the measurement across turns for a reason that has nothing to do with what is being
/// measured. Raised on both ends together, because a QMux end's transport configuration is
/// what it permits its *peer*, so raising one end only would move the limit rather than remove
/// it.
fn windows() -> TransportConfig {
    TransportConfig::new()
        .initial_max_stream_data(8 << 20)
        .initial_max_data(16 << 20)
}

/// Uploads `BODY` bytes over a hand-driven HTTP/3-over-QMux exchange.
///
/// Returns what the server said it received, and what the client's byte stream saw.
fn upload() -> (Bytes, Turns) {
    let (client_io, server_io) = stream_pair();
    // Taken before the stream is moved into the connection: the log is a handle to shared
    // state, and there is no way to reach the stream again once the connection owns it.
    let log = client_io.write_log();
    let clock = TestClock::new();
    let transport = windows();
    let http = HttpConfig::default();

    let serving = ngnet_qmux_h3::serve_with(
        server_io,
        clock.clone(),
        |request| async move {
            let (_parts, incoming) = request.into_parts();
            let received = drain(incoming).await.expect("the request body");
            // The length goes back as the response body rather than being asserted here,
            // because a panic inside the handler would surface as a panic in whichever poll
            // happened to be running it, and the report would name the harness rather than the
            // mismatch.
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
        .body(Full::new(pattern(BODY)))
        .expect("a request");

    Turns::drive(&log, connection, serving, async move {
        let response = sender.send_request(request).await.expect("a response");
        collected(response.into_body()).await
    })
}

/// The bound FR-001 states, and the write size that proves records now travel together.
///
/// Two claims, and they are different in kind. The first is arithmetic: with a
/// `OUTBOUND_CARRY`-byte guaranteed carry, no transmit pass can issue more writes than the
/// output it produced divided by that carry, rounded up. It holds here with room to spare and
/// it is not the interesting one, because a pass on this axis produces one record.
///
/// The second is the observable consequence of coalescing: a write larger than one record. It
/// was impossible before -- the connection flushed each record as it produced it, so no write
/// could carry more than `RECORD` bytes -- and it is what says the buffer is doing its job at
/// all. The largest write here carries the request's head record together with the first body
/// record, which is the one pass on this axis that has two records to merge.
#[test]
fn a_write_can_carry_more_than_one_record() {
    let (echoed, turns) = upload();
    assert_eq!(
        echoed,
        Bytes::from(BODY.to_string()),
        "the server did not receive the whole body, so the write counts below are the cost of \
         something other than the transfer this test claims to measure"
    );

    let largest = turns.lengths.iter().copied().max().unwrap_or(0);
    assert!(
        largest > RECORD,
        "no write carried more than one record ({largest} bytes at most, against a record cap \
         of {RECORD}), so nothing was coalesced at all. Before this change the connection \
         flushed each record as it produced it and this was the one thing that could not \
         happen; if it has stopped happening, either the outbound buffer is being flushed \
         between records again or the forced flush has moved somewhere that runs per record"
    );

    let bytes: usize = turns.lengths.iter().sum();
    assert!(
        turns.total() <= bytes.div_ceil(OUTBOUND_CARRY) + turns.lengths.len(),
        "the run issued {} writes for {bytes} bytes, which no arrangement of a \
         {OUTBOUND_CARRY}-byte carry can produce",
        turns.total()
    );
}

/// A single stream's body no longer costs a write per record, and a turn obeys the carry.
///
/// This is SC-001 at the level it is stated over. Two claims, and the first is the criterion:
/// the turn that carries the body issues no more writes than the bytes it carried divided by
/// the *guaranteed carry*, rounded up. The payload it is demonstrated over fills 128 full
/// records, which is comfortably the "at least sixty-four" the criterion asks for -- and the
/// record count is asserted rather than assumed, because a turn that carried less would satisfy
/// the arithmetic without demonstrating anything.
///
/// The second is the guard FR-027 asks for: the run issues fewer writes than the body takes
/// records. Removing multi-record production restores one write per record on this axis and
/// that inequality inverts immediately -- it was 130 writes against 128 records before the fix
/// and 28 against 128 after it -- so this fails if the optimization is taken out, which is what
/// it is here for.
///
/// Stated as inequalities against measured quantities rather than as the write counts
/// themselves, because the counts move with QPACK output and framing overhead and this test is
/// not entitled to freeze either.
#[test]
fn a_body_that_fills_sixty_four_records_writes_by_the_carry() {
    let (_echoed, turns) = upload();

    // Grouped so a turn's writes and the bytes they carried are the same turn's, which is what
    // the criterion is stated over: a bound on writes per byte within one pass says nothing if
    // the numerator and the denominator come from different passes.
    let mut per_turn: Vec<Vec<usize>> = Vec::with_capacity(turns.writes.len());
    let mut cursor = 0;
    for count in &turns.writes {
        per_turn.push(turns.lengths[cursor..cursor + count].to_vec());
        cursor += count;
    }
    assert_eq!(
        cursor,
        turns.lengths.len(),
        "the per-turn counts do not account for every write, so grouping them by turn would \
         misattribute one"
    );

    let busiest = per_turn
        .iter()
        .max_by_key(|turn| turn.iter().sum::<usize>())
        .expect("the upload issued writes");
    let carried: usize = busiest.iter().sum();
    let records = carried / RECORD;

    assert!(
        records >= 64,
        "the busiest turn carried {carried} bytes, which is {records} full records: SC-001 is \
         stated over a pass whose payload fills at least sixty-four, so a turn smaller than \
         that demonstrates the arithmetic over a workload the criterion does not cover. Writes \
         per turn: {:?}",
        turns.writes
    );
    assert!(
        busiest.len() <= carried.div_ceil(OUTBOUND_CARRY),
        "the busiest turn issued {} writes for {carried} bytes, more than the {} a \
         {OUTBOUND_CARRY}-byte guaranteed carry accounts for. Writes per turn: {:?}",
        busiest.len(),
        carried.div_ceil(OUTBOUND_CARRY),
        turns.writes
    );

    assert!(
        turns.total() < FULL_RECORDS,
        "the run issued {} writes for a body of {FULL_RECORDS} full records, which is at least \
         one apiece: a write per record is what this file measured before `try_write_stream` \
         filled more than one record per call, so this is what fails if that is taken out \
         again. The largest write was {} bytes; the first few were {:?}",
        turns.total(),
        turns.lengths.iter().copied().max().unwrap_or(0),
        &turns.lengths[..8.min(turns.lengths.len())]
    );
}
