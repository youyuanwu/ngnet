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
    duplex_vectored, http_crate as http,
};
use nghttp2::http::transport::{Transport, TransportRead};

use bytes::BytesMut;

/// The driver's threshold, restated here so the expectations below say what they mean.
///
/// Not imported: it is an internal tuning constant, not public API. Restating it costs a
/// test failure if the two ever drift, which is the right outcome — a threshold that moved
/// without anyone revisiting these cases is a threshold that moved by accident.
const THRESHOLD: usize = 256;

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
}

impl Shape {
    fn pair(self) -> (Duplex, Duplex) {
        match self {
            Self::Owned => duplex(false),
            Self::Vectored => duplex_vectored(),
            Self::Both => duplex_offering_both(),
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
}

impl Run {
    fn new(shape: Shape, body: usize) -> Self {
        Self {
            shape,
            body,
            caps: Vec::new(),
            decline_after: None,
            passes: 1,
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
    let (mut peer_reader, _peer_writer) = server_side.split();

    let (requests, connection) =
        nghttp2::http::handshake::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/vectored")
            .body(Full::new(vec![b'x'; run.body]))
            .expect("building a request"),
    );

    let waker = Waker::from(Arc::new(Flag(AtomicBool::new(false))));
    let mut connection = core::pin::pin!(connection);
    let mut response = core::pin::pin!(response);

    let mut outcome = None;
    for _ in 0..run.passes {
        if outcome.is_none() {
            if let Poll::Ready(result) = step(connection.as_mut(), &waker) {
                outcome = Some(result);
            }
        }
        let _ = step(response.as_mut(), &waker);
    }

    Observed {
        calls: log.calls(),
        retries: log.retries(),
        peer: drain(&mut peer_reader, &waker),
        outcome,
    }
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
    // SC-011. The invariant that makes the whole design safe: a gathering write needs one
    // live session block beside memory the driver already owns, never two blocks, so two
    // regions is the ceiling however the pass divides.
    for body in [
        SMALL_BODY,
        THRESHOLD - FRAME_HEADER,
        MAX_FRAME,
        BOUNDARY_BODY,
        WINDOW_FILLING_BODY,
    ] {
        for caps in [vec![], vec![1], vec![74], vec![3, 17, 1, 20_000]] {
            let observed = observe(Run::new(Shape::Vectored, body).caps(caps.clone()).passes(4));
            for regions in &observed.calls {
                assert!(
                    (1..=2).contains(&regions.len()),
                    "body {body}, caps {caps:?}: a call was offered {} regions, saw {:?}",
                    regions.len(),
                    observed.calls,
                );
                assert!(
                    regions.iter().all(|&len| len > 0),
                    "body {body}, caps {caps:?}: a call was offered an empty region, saw {:?}",
                    observed.calls,
                );
            }
        }
    }
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
