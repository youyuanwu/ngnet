//! What a transmit pass costs the byte stream today, counted at the driver.
//!
//! # Why this is measured through the whole join and not through [`Connection`] alone
//!
//! `crates/ngnet-qmux/tests/io_writes.rs` already pins one write per record at the QMux layer,
//! where a test offers bytes to a connection and counts what comes out. That is the cheaper
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
//! # What is pinned, and what is expected to invert it
//!
//! Everything asserted here is today's *unoptimized* behaviour. Phase 4 (write coalescing) is
//! the phase that should make these assertions fail: when a pass fills its outbound buffer with
//! many records and writes once, `RECORD_WRITES` stops being the number of writes and the
//! largest write stops being one record. A phase that changes the shape of the writes without
//! failing anything here has not done what it says.
//!
//! [`Connection`]: ngnet_qmux::io::Connection

mod transmit_harness;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
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

#[test]
fn today_a_transmit_pass_writes_once_per_record() {
    let (echoed, turns) = upload();
    assert_eq!(
        echoed,
        Bytes::from(BODY.to_string()),
        "the server did not receive the whole body, so the write counts below are the cost of \
         something other than the transfer this test claims to measure"
    );

    let full = turns.lengths.iter().filter(|len| **len == RECORD).count();
    assert_eq!(
        full,
        FULL_RECORDS,
        "the body should have filled {FULL_RECORDS} records and did not, so the workload is no \
         longer the one the counts below were measured for. {} writes were issued in total, the \
         largest {} bytes; the first few were {:?}",
        turns.total(),
        turns.lengths.iter().copied().max().unwrap_or(0),
        &turns.lengths[..8.min(turns.lengths.len())]
    );

    let largest = turns.lengths.iter().copied().max().unwrap_or(0);
    assert_eq!(
        largest, RECORD,
        "today the connection flushes each record as it is produced -- `write_record` in \
         `crates/ngnet-qmux/src/io/conn.rs` flushes, produces one record and flushes again -- so \
         no write can carry more than the one record that was outstanding. Phase 4 (write \
         coalescing) is expected to break this: a pass that fills the outbound buffer and writes \
         once will issue writes far larger than a single record"
    );

    assert!(
        turns.busiest() >= FULL_RECORDS,
        "today one poll of the driver carries the whole body and pays a write for every record \
         in it, so the busiest turn should issue at least {FULL_RECORDS} writes; it issued {}. \
         The measured figure is 134: the four remaining preamble records, the request's header \
         record, {FULL_RECORDS} full body records and the body's remainder record, all in the \
         turn that follows the one carrying the client's announcement. Phase 4 is expected to \
         break this assertion, since FR-001 requires that pass to write a number of times \
         proportional to the payload divided by what one write can carry, not to the record \
         count. Per-turn writes: {:?}",
        turns.busiest(),
        turns.writes
    );

    assert!(
        turns.total() > FULL_RECORDS,
        "the run as a whole issued {} writes for {FULL_RECORDS} body records, which is fewer \
         than the records themselves; the log is not recording what this test thinks it is",
        turns.total()
    );
}
