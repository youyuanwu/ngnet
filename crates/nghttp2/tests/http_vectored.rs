//! The vectored drain strategy, driven one pass at a time (Spec SC-006 … SC-012b).
//!
//! The other write-path tests ask whether an exchange completes. These ask something
//! narrower and harder: *how many* write operations one driver pass costs, and what each
//! one was handed. That needs the connection stepped by hand — a single poll, then the
//! record read off — because [`block_on`](nghttp2::http::testing::block_on) runs the
//! connection to completion and by then every pass has blurred into one tally.
//!
//! # Why the transport is never answered
//!
//! Every test here drives the client against a peer that is present but silent: its half of
//! the duplex is held open so the connection does not see a close, and nothing is ever
//! written back. That is deliberate. The client can send its whole request — preface,
//! `SETTINGS`, `HEADERS` and as much body as the initial flow-control window admits —
//! without a single octet arriving, so the first pass is a complete, self-contained,
//! *reproducible* write pass. Introducing a peer would add `WINDOW_UPDATE`s and `SETTINGS`
//! acknowledgements whose arrival time decides how the passes divide, which is exactly the
//! variable these assertions cannot afford.
//!
//! # The block sizes are libnghttp2's, not this file's
//!
//! Nothing here fabricates a block sequence; each test picks a request body whose
//! serialisation is known and lets the session produce it. A body of `n` octets becomes
//! `ceil(n / 16384)` `DATA` frames of 16393 octets, a shorter final frame if `n` is not a
//! multiple, and — when the body ends exactly on a frame boundary or the stream is still
//! open — a nine-octet empty `DATA` carrying `END_STREAM`. The frame header is nine octets,
//! so a body of 247 is a block of exactly the driver's threshold. That relationship is what
//! makes the threshold assertions below exact rather than approximate.

#![cfg(feature = "http")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use nghttp2::http::testing::{
    Duplex, DuplexReader, Full, bytes_crate as bytes, duplex, duplex_offering_both,
    duplex_owned_regions, duplex_vectored, http_crate as http,
};
use nghttp2::http::transport::{Transport, TransportRead, TransportWrite};

use bytes::BytesMut;

/// The driver's threshold, restated here so the expectations below say what they mean.
///
/// Not imported: it is an internal tuning constant, not public API. Restating it costs a
/// test failure if the two ever drift, which is the right outcome — a threshold that moved
/// without anyone revisiting these cases is a threshold that moved by accident.
const THRESHOLD: usize = 256;

/// The driver's ceiling on descriptors held for one gathering write, restated here for the
/// same reason as [`THRESHOLD`]: an internal tuning constant, not public API, that a test
/// should fail loudly against if it ever drifts. A gathering write materialises at most
/// `MAX_REGIONS + 1` `IoSlice`s — the retained descriptor list plus one live session block.
const MAX_REGIONS: usize = 64;

/// The nine-octet frame header every `DATA` frame carries.
const FRAME_HEADER: usize = 9;

/// HTTP/2's default maximum frame payload, and so the largest `DATA` frame the session emits.
const MAX_FRAME: usize = 16 * 1024;

/// The default initial connection flow-control window: how much body one pass can send
/// before the peer has said anything.
const INITIAL_WINDOW: usize = 65535;

// ----- stepping a connection by hand -----

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn step<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

/// Reads everything the peer half currently holds, without waiting for more.
///
/// Polls once per iteration and stops the moment the read parks, which is the only way to
/// say "whatever has arrived by now" against a transport whose read blocks until it has
/// something.
fn drain(reader: &mut DuplexReader, waker: &Waker) -> Vec<u8> {
    let mut seen = Vec::new();
    loop {
        let read = reader.read(BytesMut::with_capacity(64 * 1024));
        let mut read = core::pin::pin!(read);
        match step(read.as_mut(), waker) {
            Poll::Ready((result, buf)) => {
                let count = result.expect("reading from the peer half");
                if count == 0 {
                    return seen;
                }
                seen.extend_from_slice(&buf);
            }
            Poll::Pending => return seen,
        }
    }
}

/// Which of the transport shapes a run uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Neither fast path: the driver coalesces the pass into one owned write.
    Owned,
    /// The vectored path alone.
    Vectored,
    /// Both fast paths, so the driver's precedence rule has something to decide.
    Both,
    /// The owned-region (completion) path: a gathering write over an owned `Vec<Bytes>`.
    OwnedRegions,
}

impl Shape {
    fn pair(self) -> (Duplex, Duplex) {
        match self {
            Self::Owned => duplex(false),
            Self::Vectored => duplex_vectored(),
            Self::Both => duplex_offering_both(),
            Self::OwnedRegions => duplex_owned_regions(),
        }
    }
}

/// How a run is set up, and what it is allowed to do.
struct Run {
    shape: Shape,
    /// Length of the request body, in octets.
    body: usize,
    /// Per-call acceptance caps handed to [`Duplex::accept_at_most`], in order.
    caps: Vec<usize>,
    /// After this many performed vectored writes the transport stops offering the path.
    decline_after: Option<usize>,
    /// How many times the connection is polled.
    passes: usize,
    /// Whether the body is handed over (`handshake_shared`) rather than copied
    /// (`handshake`). The shared path frames each `DATA` as a record the driver offers as
    /// its own regions, which is what grows a gathering write past two regions.
    shared: bool,
    /// Raw octets written to the peer half before the connection is polled, letting a run
    /// hand the client crafted inbound frames — a large `SETTINGS`/`WINDOW_UPDATE` pair, so
    /// the flow-control window admits a whole body in one pass and the region list is driven
    /// past `MAX_REGIONS`. Empty for the silent-peer runs that are the norm here.
    prelude: Vec<u8>,
}

impl Run {
    fn new(shape: Shape, body: usize) -> Self {
        Self {
            shape,
            body,
            caps: Vec::new(),
            decline_after: None,
            passes: 1,
            shared: false,
            prelude: Vec::new(),
        }
    }

    fn caps(mut self, caps: impl IntoIterator<Item = usize>) -> Self {
        self.caps = caps.into_iter().collect();
        self
    }

    fn decline_after(mut self, writes: usize) -> Self {
        self.decline_after = Some(writes);
        self
    }

    fn passes(mut self, passes: usize) -> Self {
        self.passes = passes;
        self
    }

    fn shared(mut self) -> Self {
        self.shared = true;
        self
    }

    fn prelude(mut self, prelude: Vec<u8>) -> Self {
        self.prelude = prelude;
        self
    }
}

/// What one run produced.
struct Observed {
    /// The region lengths of each polled gathering call, in order. Empty unless the
    /// vectored path ran.
    calls: Vec<Vec<usize>>,
    /// Calls that re-offered the remainder of a short write rather than new octets.
    retries: usize,
    /// Every octet the peer half actually received, in order.
    peer: Vec<u8>,
    /// The connection's verdict, if it reached one within the run's passes.
    outcome: Option<Result<(), nghttp2::http::Error>>,
}

/// Drives one client request over the shape a run names, polling the connection by hand.
///
/// The request future is stepped alongside the connection so a completed response is
/// collected rather than left to strand the stream, but its value is not of interest here:
/// with a silent peer it never completes, and everything asserted on is on the write side.
fn observe(run: Run) -> Observed {
    let (client_side, server_side) = run.shape.pair();
    let log = client_side.vectored_log();
    if !run.caps.is_empty() {
        client_side.accept_at_most(run.caps.iter().copied());
    }
    if let Some(limit) = run.decline_after {
        client_side.decline_vectored_after(limit);
    }
    // Split rather than dropped: a dropped writing half closes the pipe, and a closed pipe
    // is a peer that hung up, which ends the connection before the pass under test.
    let (mut peer_reader, mut peer_writer) = server_side.split();

    // Hand the client any crafted inbound the run supplies before it is polled. The duplex
    // performs the write synchronously as the future is built, so the returned ready future
    // can simply be dropped; the octets are in the client's inbound pipe by the time the
    // first poll reads them.
    if !run.prelude.is_empty() {
        drop(peer_writer.write(bytes::Bytes::from(run.prelude.clone())));
    }

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://example.test/vectored")
        .body(Full::new(vec![b'x'; run.body]))
        .expect("building a request");

    let waker = Waker::from(Arc::new(Flag(AtomicBool::new(false))));

    // The push and no-copy entry points return the same handle and the same response future;
    // only the connection future's concrete type differs, so the polling loop is shared and
    // the branch is only which one to build. `requests` is held to the end of each branch —
    // dropping it would close the request half before the pass under test.
    let outcome = if run.shared {
        let (requests, connection) =
            nghttp2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
        let response = requests.send_request(request);
        run_passes(connection, response, run.passes, &waker)
    } else {
        let (requests, connection) =
            nghttp2::http::handshake::<_, Full>(client_side).expect("handshake");
        let response = requests.send_request(request);
        run_passes(connection, response, run.passes, &waker)
    };

    Observed {
        calls: log.calls(),
        retries: log.retries(),
        peer: drain(&mut peer_reader, &waker),
        outcome,
    }
}

/// Steps the connection and its response future by hand for `passes` polls, returning the
/// connection's verdict if it reached one.
///
/// Generic over the connection future so the same loop drives both the copying and the
/// handed-over entry points; the response is stepped alongside so a completed response is
/// collected rather than left to strand the stream, but its value is not of interest here.
fn run_passes(
    connection: impl Future<Output = Result<(), nghttp2::http::Error>>,
    response: nghttp2::http::ResponseFuture,
    passes: usize,
    waker: &Waker,
) -> Option<Result<(), nghttp2::http::Error>> {
    let mut connection = core::pin::pin!(connection);
    let mut response = core::pin::pin!(response);

    let mut outcome = None;
    for _ in 0..passes {
        if outcome.is_none() {
            if let Poll::Ready(result) = step(connection.as_mut(), waker) {
                outcome = Some(result);
            }
        }
        let _ = step(response.as_mut(), waker);
    }
    outcome
}

/// The single write a pass of only sub-threshold blocks costs.
///
/// A body of `MAX_FRAME` splits into one full `DATA` frame and an empty one carrying
/// `END_STREAM`; a body below the threshold does not split at all, so the whole pass —
/// preface, `SETTINGS`, `HEADERS`, the body and its terminator — is small blocks only.
const SMALL_BODY: usize = THRESHOLD - FRAME_HEADER - 1;

#[test]
fn a_pass_of_only_small_blocks_costs_one_write() {
    // SC-006. Every block the session produces here is below the threshold, so all of them
    // accumulate and the pass ends with a single gathering call carrying the lot.
    let observed = observe(Run::new(Shape::Vectored, SMALL_BODY));

    assert_eq!(
        observed.calls.len(),
        1,
        "a pass of nothing but small blocks must cost exactly one write, saw {:?}",
        observed.calls,
    );
    assert_eq!(
        observed.calls[0].len(),
        1,
        "with no large block to ride beside, the accumulation goes out on its own",
    );
    assert_eq!(
        observed.calls[0][0],
        observed.peer.len(),
        "the one write carried every octet the peer received",
    );
    assert_eq!(observed.retries, 0, "nothing was short-written");
}

#[test]
fn the_same_pass_over_the_coalescing_transport_produces_the_same_octets() {
    // SC-008 for the all-small case: gathering is a syscall-count optimisation, not a
    // change of what goes on the wire.
    let vectored = observe(Run::new(Shape::Vectored, SMALL_BODY));
    let owned = observe(Run::new(Shape::Owned, SMALL_BODY));

    assert_eq!(
        vectored.peer, owned.peer,
        "the gathered pass put different octets on the wire than the coalesced one",
    );
}

#[test]
fn a_block_at_the_threshold_takes_the_large_path_and_one_below_it_does_not() {
    // SC-012b. The frame header is nine octets, so a body of `THRESHOLD - 9` is a block of
    // exactly the threshold. Three bodies, one octet apart, pin the comparison as `>=`:
    // were it ever weakened to `>`, the middle case would collapse into the first.
    let below = observe(Run::new(Shape::Vectored, THRESHOLD - FRAME_HEADER - 1));
    let at = observe(Run::new(Shape::Vectored, THRESHOLD - FRAME_HEADER));
    let above = observe(Run::new(Shape::Vectored, THRESHOLD - FRAME_HEADER + 1));

    assert_eq!(
        below.calls.len(),
        1,
        "one octet below the threshold the block accumulates, saw {:?}",
        below.calls,
    );

    assert_eq!(
        at.calls.first().map(Vec::len),
        Some(2),
        "a block of exactly the threshold is large, and goes out beside the accumulation, \
         saw {:?}",
        at.calls,
    );
    assert_eq!(
        at.calls[0][1], THRESHOLD,
        "the second region is the block itself, uncopied",
    );

    assert_eq!(
        above.calls.first().map(Vec::len),
        Some(2),
        "one octet above the threshold is large too, saw {:?}",
        above.calls,
    );
    assert_eq!(above.calls[0][1], THRESHOLD + 1);
}

/// A body that fills the initial window exactly, so the pass ends on a large block with
/// nothing small behind it.
const WINDOW_FILLING_BODY: usize = INITIAL_WINDOW;

/// A body ending exactly on a frame boundary, so a nine-octet `END_STREAM` frame follows
/// the last full one.
const BOUNDARY_BODY: usize = MAX_FRAME * 3;

#[test]
fn large_blocks_cost_one_write_each_and_no_more() {
    // SC-007, the "exactly L" half. The body fills the initial flow-control window to the
    // octet, so the pass stops with a large block and never reaches the stream's
    // terminator — there is no trailing small block to pay an extra write for.
    let observed = observe(Run::new(Shape::Vectored, WINDOW_FILLING_BODY));

    let large = observed
        .calls
        .iter()
        .filter(|regions| regions.iter().any(|&len| len >= THRESHOLD))
        .count();
    assert_eq!(
        observed.calls.len(),
        large,
        "every write in this pass carried a large block; a write that did not would be an \
         extra syscall, saw {:?}",
        observed.calls,
    );
    assert_eq!(
        observed.calls.len(),
        INITIAL_WINDOW.div_ceil(MAX_FRAME),
        "one write per DATA frame the window admitted, and not one more, saw {:?}",
        observed.calls,
    );
    assert_eq!(
        observed.calls[0].len(),
        2,
        "the small blocks ahead of the first large one rode with it rather than costing a \
         write of their own",
    );
    assert!(
        observed.calls[1..].iter().all(|regions| regions.len() == 1),
        "with nothing accumulated behind them, later large blocks go out alone, saw {:?}",
        observed.calls,
    );
    assert_eq!(observed.retries, 0, "nothing was short-written");
}

#[test]
fn a_trailing_small_block_costs_one_extra_write_and_only_one() {
    // SC-007, the "at most L+1" half, and the small-then-large, large-then-large and
    // large-then-small orderings in a single pass: the head accumulates and rides with the
    // first frame, three full frames follow, and the stream's nine-octet terminator is left
    // over at the end.
    let observed = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));

    let large = BOUNDARY_BODY / MAX_FRAME;
    assert_eq!(
        observed.calls.len(),
        large + 1,
        "L large blocks and a small tail cost L+1 writes, saw {:?}",
        observed.calls,
    );
    assert_eq!(
        observed.calls[0],
        vec![observed.calls[0][0], MAX_FRAME + FRAME_HEADER],
        "small-then-large: the head rides with the first frame",
    );
    assert_eq!(
        observed.calls[1],
        vec![MAX_FRAME + FRAME_HEADER],
        "large-then-large: a frame with nothing accumulated behind it goes out alone",
    );
    assert_eq!(
        observed.calls[large],
        vec![FRAME_HEADER],
        "large-then-small: the terminator is the tail write, and carries only itself",
    );
    assert_eq!(observed.retries, 0, "nothing was short-written");
}

#[test]
fn every_ordering_puts_the_same_octets_on_the_wire_as_coalescing_would() {
    // SC-008 across the orderings SC-007 names. Each body produces a different mix of
    // accumulated and gathered blocks; none of them may change a single octet.
    for body in [
        SMALL_BODY,
        THRESHOLD - FRAME_HEADER,
        MAX_FRAME,
        BOUNDARY_BODY,
        WINDOW_FILLING_BODY,
    ] {
        let vectored = observe(Run::new(Shape::Vectored, body));
        let owned = observe(Run::new(Shape::Owned, body));
        assert_eq!(
            vectored.peer, owned.peer,
            "a {body}-octet body reached the peer differently on the two paths",
        );
        assert!(
            !vectored.peer.is_empty(),
            "a {body}-octet body sent nothing at all, so the comparison proved nothing",
        );
    }
}

#[test]
fn no_call_is_ever_offered_more_than_two_regions_or_an_empty_one() {
    // SC-003 / SC-011. Two facts, one universal and one path-specific.
    //
    // Universal: no call is ever offered an empty region. A zero-length `IoSlice` is legal
    // to write but is a region counted for nothing, and the trait's contract promises never
    // to produce one however the pass divides or a short write lands.
    //
    // Path-specific ceiling: the *push* path — a body copied into libnghttp2's buffer —
    // still gathers at most two regions, one live session block beside memory the driver
    // already owns, never two blocks. Phase 3 leaves that path untouched, so the original
    // two-region cap is retained and asserted here exactly as before.
    //
    // The no-copy (shared) path is what Phase 3 changed: each `DATA` frame becomes a record
    // the driver offers as its own region rather than copying, so one pass can gather many
    // regions. Its ceiling is not two but `MAX_REGIONS + 1` — the retained descriptor list
    // plus one live block — and that bound, not the push path's, is what the shared branch
    // below asserts. Retargeting this case rather than deleting it keeps the empty-region
    // guarantee under test on both paths while recording that the two-region cap is now a
    // property of the push path alone.
    for body in [
        SMALL_BODY,
        THRESHOLD - FRAME_HEADER,
        MAX_FRAME,
        BOUNDARY_BODY,
        WINDOW_FILLING_BODY,
    ] {
        for caps in [vec![], vec![1], vec![74], vec![3, 17, 1, 20_000]] {
            let pushed = observe(Run::new(Shape::Vectored, body).caps(caps.clone()).passes(4));
            for regions in &pushed.calls {
                assert!(
                    (1..=2).contains(&regions.len()),
                    "push path, body {body}, caps {caps:?}: a call was offered {} regions, saw {:?}",
                    regions.len(),
                    pushed.calls,
                );
                assert!(
                    regions.iter().all(|&len| len > 0),
                    "push path, body {body}, caps {caps:?}: a call was offered an empty region, saw {:?}",
                    pushed.calls,
                );
            }

            let shared = observe(
                Run::new(Shape::Vectored, body)
                    .caps(caps.clone())
                    .passes(4)
                    .shared(),
            );
            for regions in &shared.calls {
                assert!(
                    (1..=MAX_REGIONS + 1).contains(&regions.len()),
                    "shared path, body {body}, caps {caps:?}: a call was offered {} regions, saw {:?}",
                    regions.len(),
                    shared.calls,
                );
                assert!(
                    regions.iter().all(|&len| len > 0),
                    "shared path, body {body}, caps {caps:?}: a call was offered an empty region, saw {:?}",
                    shared.calls,
                );
            }
        }
    }
}

/// Builds one raw HTTP/2 frame: the nine-octet header — length, type, flags, stream id —
/// followed by the payload. Used only to hand the client crafted inbound; the client's own
/// output is produced by the session, never fabricated here.
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut out = Vec::with_capacity(9 + len);
    out.push((len >> 16) as u8);
    out.push((len >> 8) as u8);
    out.push(len as u8);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&(stream & 0x7fff_ffff).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// A `SETTINGS` frame advertising `INITIAL_WINDOW_SIZE`, which opens the client's per-stream
/// send window so a stream can carry far more than the 65535-octet default in one pass.
fn settings_initial_window(size: u32) -> Vec<u8> {
    const INITIAL_WINDOW_SIZE_ID: u16 = 0x0004;
    let mut payload = Vec::new();
    payload.extend_from_slice(&INITIAL_WINDOW_SIZE_ID.to_be_bytes());
    payload.extend_from_slice(&size.to_be_bytes());
    frame(0x04, 0, 0, &payload)
}

/// A connection-level `WINDOW_UPDATE`, which opens the client's connection send window —
/// the second of the two windows a body must fit inside, and the one `SETTINGS` cannot move.
fn window_update(increment: u32) -> Vec<u8> {
    frame(0x08, 0, 0, &(increment & 0x7fff_ffff).to_be_bytes())
}

/// A body several times larger than a full region list can name at once: at 16384 octets a
/// `DATA` frame, this is roughly ninety frames, so ninety records, so a hundred and eighty
/// payload-and-header regions — comfortably past `MAX_REGIONS` even after the list is flushed
/// and refilled.
const REGION_CAP_BODY: usize = 1_500_000;

#[test]
fn a_pass_driven_past_the_region_cap_holds_the_bound_and_stays_correct() {
    // SC-003 / D6. The region list is capped at `MAX_REGIONS` descriptors so it can be
    // materialised into a fixed stack array; a `send_into` call that frames more `DATA` than
    // the cap admits must flush the full list mid-pass and carry on, never overrun the array.
    // Reaching that path needs a pass that emits far more than the sixty-five-region ceiling,
    // which the silent-peer default cannot do — its window admits only the 65535-octet
    // default, four or so frames. So the peer is handed a crafted `SETTINGS`/`WINDOW_UPDATE`
    // pair that opens both send windows wide, and a body large enough to fill them: the whole
    // 1.5 MB then leaves in one pass, driving the list past the cap several times over.
    let mut prelude = settings_initial_window(0x7fff_ffff);
    prelude.extend(window_update(0x0080_0000));

    let vectored = observe(
        Run::new(Shape::Vectored, REGION_CAP_BODY)
            .shared()
            .prelude(prelude.clone())
            .passes(16),
    );
    // The same workload coalesced, as the independent oracle for what should reach the peer:
    // the owned path copies every octet, so its flat wire is libnghttp2's own serialisation
    // with no gathering decision of the driver's in it.
    let owned = observe(
        Run::new(Shape::Owned, REGION_CAP_BODY)
            .shared()
            .prelude(prelude)
            .passes(16),
    );

    for regions in &vectored.calls {
        assert!(
            regions.len() <= MAX_REGIONS + 1,
            "a call was offered {} regions, past the {} ceiling; the mid-pass flush failed to \
             bound the list, saw lengths {:?}",
            regions.len(),
            MAX_REGIONS + 1,
            vectored.calls.iter().map(Vec::len).collect::<Vec<_>>(),
        );
        assert!(
            regions.iter().all(|&len| len > 0),
            "a call was offered an empty region, saw {regions:?}",
        );
    }

    let widest = vectored
        .calls
        .iter()
        .map(Vec::len)
        .max()
        .expect("the pass performed at least one gathering write");
    assert!(
        widest >= MAX_REGIONS,
        "the widest call held only {widest} regions, so the list never reached the cap and the \
         mid-pass flush was never exercised; the window or body is too small",
    );

    assert_eq!(
        vectored.peer, owned.peer,
        "driving the list past the cap changed the octets the peer received",
    );
    assert!(
        !vectored.peer.is_empty(),
        "the body never left, so the comparison proved nothing",
    );
}

#[test]
fn a_partial_acceptor_across_a_multi_region_write_drops_and_duplicates_nothing() {
    // SC-009 on the no-copy path. The push tests above cut a two-region write on every
    // interesting boundary; this does the same to a write of *many* regions, the shape only
    // the shared path produces. A handed-over body frames several `DATA` records, so one
    // gathering write carries a run of alternating header and payload regions; the caps chop
    // the transport's acceptance at awkward points — inside a payload, exactly on a region
    // boundary, one octet short of the whole — and the retry-and-resume path must put every
    // octet on the wire once, in order, matching what the coalescing path would have sent.
    let unlimited = observe(Run::new(Shape::Vectored, BOUNDARY_BODY).shared().passes(8));
    assert!(
        unlimited.calls.iter().any(|regions| regions.len() > 2),
        "the workload never produced a multi-region write, so nothing multi-region was tested, \
         saw {:?}",
        unlimited.calls,
    );
    let first_call: usize = unlimited.calls[0].iter().sum();
    let head = unlimited.calls[0][0];

    // The oracle: the same handed-over body over the coalescing transport, which copies the
    // lot into one owned write. Its octets are what a partial acceptor must reproduce.
    let coalesced = observe(Run::new(Shape::Owned, BOUNDARY_BODY).shared().passes(8));

    for caps in [
        vec![1],
        vec![head],
        vec![head - 1],
        vec![first_call - 1],
        vec![1, head, 7, first_call - 1, 3],
        vec![head, 1, MAX_FRAME, 5],
    ] {
        let observed = observe(
            Run::new(Shape::Vectored, BOUNDARY_BODY)
                .shared()
                .caps(caps.clone())
                .passes(8),
        );
        assert_eq!(
            observed.peer, coalesced.peer,
            "caps {caps:?} changed the octets the peer received on the multi-region path",
        );
        assert!(
            observed.outcome.is_none(),
            "caps {caps:?} ended the connection: {:?}",
            observed.outcome.as_ref().map(Result::is_err),
        );
    }
}

#[test]
fn an_owned_region_pass_driven_past_the_region_cap_holds_the_bound_and_stays_correct() {
    // SC-003 / D6 on the completion path. FR-007's region bound is transport-independent, so
    // the owned-region strategy caps its `Vec<Bytes>` exactly as the vectored path caps its
    // descriptor list: a `send_into` call that frames more `DATA` than the cap admits flushes
    // the full list mid-pass and carries on. The bound is tighter here — the whole list is
    // owned, so there is no separate live block riding beside it, and the ceiling is
    // `MAX_REGIONS` rather than `MAX_REGIONS + 1`. Reaching the path needs the same crafted
    // window-opening prelude and 1.5 MB body the vectored cap test uses.
    let mut prelude = settings_initial_window(0x7fff_ffff);
    prelude.extend(window_update(0x0080_0000));

    let owned_regions = observe(
        Run::new(Shape::OwnedRegions, REGION_CAP_BODY)
            .shared()
            .prelude(prelude.clone())
            .passes(16),
    );
    // The coalescing path as the independent oracle: it copies every octet, so its flat wire
    // is libnghttp2's own serialisation with no gathering decision of the driver's in it.
    let owned = observe(
        Run::new(Shape::Owned, REGION_CAP_BODY)
            .shared()
            .prelude(prelude)
            .passes(16),
    );

    for regions in &owned_regions.calls {
        assert!(
            regions.len() <= MAX_REGIONS,
            "a call was offered {} regions, past the {MAX_REGIONS} ceiling; the mid-pass \
             flush failed to bound the list, saw lengths {:?}",
            regions.len(),
            owned_regions.calls.iter().map(Vec::len).collect::<Vec<_>>(),
        );
        assert!(
            regions.iter().all(|&len| len > 0),
            "a call was offered an empty region, saw {regions:?}",
        );
    }

    let widest = owned_regions
        .calls
        .iter()
        .map(Vec::len)
        .max()
        .expect("the pass performed at least one gathering write");
    assert!(
        widest >= MAX_REGIONS - 1,
        "the widest call held only {widest} regions, so the list never approached the cap and \
         the mid-pass flush was never exercised; the window or body is too small",
    );

    assert_eq!(
        owned_regions.peer, owned.peer,
        "driving the owned-region list past the cap changed the octets the peer received",
    );
    assert!(
        !owned_regions.peer.is_empty(),
        "the body never left, so the comparison proved nothing",
    );
}

#[test]
fn an_owned_region_partial_acceptor_drops_and_duplicates_nothing() {
    // SC-009 on the completion path. A handed-over body frames several `DATA` records, so one
    // owned-region write carries a run of alternating header and payload regions; the caps
    // chop the transport's acceptance at awkward points — inside a payload, exactly on a
    // region boundary, one octet short of the whole. The retry-and-resume path drops the
    // fully written regions from the front and `Bytes::advance`s the first partial one (both
    // free, since `Bytes` is a view), and must put every octet on the wire once, in order,
    // matching what the coalescing path would have sent.
    let unlimited = observe(
        Run::new(Shape::OwnedRegions, BOUNDARY_BODY)
            .shared()
            .passes(8),
    );
    assert!(
        unlimited.calls.iter().any(|regions| regions.len() > 2),
        "the workload never produced a multi-region write, so nothing multi-region was tested, \
         saw {:?}",
        unlimited.calls,
    );
    // Every region offered is non-empty and the per-call count stays within the cap — the
    // invariants the resume path must preserve across a short write, checked here so a cap
    // that quietly dropped or duplicated a region would fail on shape before octets.
    for regions in &unlimited.calls {
        assert!(
            (1..=MAX_REGIONS).contains(&regions.len()) && regions.iter().all(|&len| len > 0),
            "a call was offered {} regions or an empty one, saw {:?}",
            regions.len(),
            unlimited.calls,
        );
    }
    let first_call: usize = unlimited.calls[0].iter().sum();
    let head = unlimited.calls[0][0];

    // The oracle: the same handed-over body over the coalescing transport, which copies the
    // lot into one owned write. Its octets are what a partial acceptor must reproduce.
    let coalesced = observe(Run::new(Shape::Owned, BOUNDARY_BODY).shared().passes(8));

    for caps in [
        vec![1],
        vec![head],
        vec![head - 1],
        vec![first_call - 1],
        vec![1, head, 7, first_call - 1, 3],
        vec![head, 1, MAX_FRAME, 5],
    ] {
        let observed = observe(
            Run::new(Shape::OwnedRegions, BOUNDARY_BODY)
                .shared()
                .caps(caps.clone())
                .passes(8),
        );
        assert_eq!(
            observed.peer, coalesced.peer,
            "caps {caps:?} changed the octets the peer received on the owned-region path",
        );
        assert!(
            observed.outcome.is_none(),
            "caps {caps:?} ended the connection: {:?}",
            observed.outcome.as_ref().map(Result::is_err),
        );
    }
}

#[test]
fn an_owned_region_transport_accepting_nothing_fails_the_connection_rather_than_spinning() {
    // SC-010 on the completion path. A successful write of zero octets is a transport that
    // will never make progress; the owned-region path must turn it into a `Transport` error
    // exactly as the vectored and borrowed paths do, rather than spin re-offering the list.
    let observed = observe(
        Run::new(Shape::OwnedRegions, MAX_FRAME)
            .shared()
            .caps([0])
            .passes(2),
    );

    let outcome = observed
        .outcome
        .expect("the connection must end rather than spin on a transport accepting nothing");
    let error = outcome.expect_err("accepting no octets is a failure, not a clean close");
    assert_eq!(error.kind(), nghttp2::http::ErrorKind::Transport);
}

#[test]
fn a_short_write_landing_on_the_region_boundary_retries_with_one_region() {
    // SC-011's named trap. Accepting exactly the first region's worth leaves the second
    // region alone to re-offer; offering it beside a now-empty first would hand the
    // transport a zero-length `IoSlice` — legal to write, but a region counted for nothing,
    // and a shape the trait's contract promises never to produce.
    let unlimited = observe(Run::new(Shape::Vectored, MAX_FRAME));
    let head = unlimited.calls[0][0];
    assert!(
        head > 0 && unlimited.calls[0].len() == 2,
        "this test needs a two-region first call to cut on the boundary of, saw {:?}",
        unlimited.calls,
    );

    let observed = observe(Run::new(Shape::Vectored, MAX_FRAME).caps([head]).passes(4));

    assert_eq!(
        observed.calls[0],
        vec![head, MAX_FRAME + FRAME_HEADER],
        "the first call is unchanged: the cap decides what is accepted, not what is offered",
    );
    assert_eq!(
        observed.calls[1],
        vec![MAX_FRAME + FRAME_HEADER],
        "the retry re-offers the block alone, with no empty region beside it",
    );
    assert_eq!(
        observed.retries, 1,
        "exactly one call re-offered octets rather than new ones",
    );
    assert_eq!(
        observed.peer, unlimited.peer,
        "cutting the write on the region boundary changed what reached the peer",
    );
}

#[test]
fn an_arbitrary_prefix_acceptor_still_delivers_every_octet_in_order() {
    // SC-009. A real socket accepts what it feels like; the caps make the interesting cuts
    // reachable on purpose — one octet, a cut inside the first region, a cut exactly on the
    // boundary, and all but one octet of the whole offer.
    let unlimited = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));
    let head = unlimited.calls[0][0];
    let first_call: usize = unlimited.calls[0].iter().sum();

    for caps in [
        vec![1],
        vec![head],
        vec![head - 1],
        vec![first_call - 1],
        vec![1, head, first_call - 1, 7],
        vec![head, 1, MAX_FRAME, 3],
    ] {
        let observed = observe(
            Run::new(Shape::Vectored, BOUNDARY_BODY)
                .caps(caps.clone())
                .passes(8),
        );
        assert_eq!(
            observed.peer, unlimited.peer,
            "caps {caps:?} changed the octets the peer received",
        );
        assert!(
            observed.outcome.is_none(),
            "caps {caps:?} ended the connection: {:?}",
            observed.outcome.as_ref().map(|result| result.is_err()),
        );
    }
}

#[test]
fn a_transport_accepting_nothing_fails_the_connection_rather_than_spinning() {
    // SC-010. A successful write of zero octets is a transport that will never make
    // progress; treating it as anything but an error is an infinite loop, and the two
    // pre-existing drain paths already guard it the same way.
    let observed = observe(Run::new(Shape::Vectored, MAX_FRAME).caps([0]).passes(2));

    let outcome = observed
        .outcome
        .expect("the connection must end rather than spin on a transport accepting nothing");
    let error = outcome.expect_err("accepting no octets is a failure, not a clean close");
    assert_eq!(error.kind(), nghttp2::http::ErrorKind::Transport);
}

#[test]
fn a_transport_offering_both_paths_takes_the_gathering_one() {
    // SC-012. With either override alone there is nothing to arbitrate; the precedence rule
    // is only observable against a transport that genuinely offers both. Were the borrowed
    // path to win, the vectored record would be empty.
    let both = observe(Run::new(Shape::Both, BOUNDARY_BODY));
    let vectored = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));

    assert!(
        !both.calls.is_empty(),
        "offering both paths took neither gathering call: the borrowed path won",
    );
    assert_eq!(
        both.calls, vectored.calls,
        "offering both must drain exactly as offering gathering alone does",
    );
    assert_eq!(both.peer, vectored.peer);
}

#[test]
fn a_transport_that_declines_part_way_through_still_delivers_every_octet() {
    // SC-012a. The election is meant to be a fixed property of the transport, but nothing
    // in the signature enforces it. Failing the connection over a transport that reneges
    // would be a worse answer than paying the copy, so the driver falls back to coalescing:
    // the remainder joins the owned buffer, in order, and the pass finishes.
    let unlimited = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));

    for after in [1usize, 2, 3] {
        let observed = observe(
            Run::new(Shape::Vectored, BOUNDARY_BODY)
                .decline_after(after)
                .passes(4),
        );
        assert_eq!(
            observed.calls.len(),
            after,
            "the transport stopped offering the path after {after} writes, so no more \
             happened, saw {:?}",
            observed.calls,
        );
        assert_eq!(
            observed.peer, unlimited.peer,
            "declining after {after} writes lost or reordered octets",
        );
    }
}

#[test]
fn declining_on_the_retry_after_a_short_write_recovers_both_regions() {
    // The other end of SC-012a, and the branch a decline alone cannot reach: the transport
    // reneges *while* the driver's accumulation buffer still holds unwritten octets and the
    // block in hand is untouched. The fallback has to recover the remainder of the buffer
    // and then the whole block, in that order — an off-by-one in either would drop or
    // duplicate octets, and only this ordering can tell.
    let unlimited = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));
    let head = unlimited.calls[0][0];
    assert!(
        head > 1,
        "this test needs an accumulation the short write can land inside, saw {head}",
    );

    // The cut is taken inside the accumulation, exactly on the boundary between the two
    // regions, and inside the block, so both arms of the fallback's arithmetic are driven.
    for accepted in [1usize, head / 2, head - 1, head, head + 100] {
        let observed = observe(
            Run::new(Shape::Vectored, BOUNDARY_BODY)
                .caps([accepted])
                .decline_after(1)
                .passes(4),
        );
        assert_eq!(
            observed.calls,
            vec![vec![head, MAX_FRAME + FRAME_HEADER]],
            "accepting {accepted} then declining should leave exactly one performed call",
        );
        assert_eq!(
            observed.peer, unlimited.peer,
            "declining on the retry after accepting {accepted} octets lost or reordered the \
             rest",
        );
    }
}

#[test]
fn declining_before_a_single_gathering_write_leaves_the_pass_to_the_owned_path() {
    // The degenerate end of SC-012a: the transport offers the path to the election probe
    // and then refuses the very first real call, with the driver's accumulation buffer
    // already holding the head of the pass. That makes it the `done == 0` case of the
    // fallback — nothing of the offer was accepted, so the whole accumulation *and* the
    // whole block have to be recovered into the coalescing buffer, in that order.
    //
    // The fixture can express this only because it recognises the probe by its empty region
    // list; counting it as a write would decline the election itself and quietly turn this
    // into a second test of the plain owned path.
    let unlimited = observe(Run::new(Shape::Vectored, BOUNDARY_BODY));
    let observed = observe(
        Run::new(Shape::Vectored, BOUNDARY_BODY)
            .decline_after(0)
            .passes(4),
    );

    assert!(
        observed.calls.is_empty(),
        "no gathering call should have been performed, saw {:?}",
        observed.calls,
    );
    assert_eq!(
        observed.peer, unlimited.peer,
        "falling back before the first write lost or reordered octets",
    );
}
