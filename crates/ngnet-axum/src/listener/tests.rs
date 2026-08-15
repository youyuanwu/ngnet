//! Tests for the retry policy [`RetryingListener`] supplies.
//!
//! Two kinds live here, and the distinction matters more than it usually does.
//!
//! The *timed* tests drive the wrapper for a real few seconds against a competing arm and
//! count how often the underlying source was actually asked. They are how a backoff that
//! fails to pace itself is caught, and they are the tests the whole design exists to pass.
//!
//! The *single-poll* tests poll the accept future exactly once, by hand, against a source
//! that panics if it is called more times than it should be. They exist because a timed test
//! cannot catch a future that never returns from `poll`: a `tokio::time::timeout` is itself a
//! cooperative future, so a wrapper that spins inside one poll starves the very timer meant
//! to detect it, and the test hangs rather than failing. Polling by hand and letting the
//! source panic moves the failure inside the poll the test drives, where it is observable.

use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::DuplexStream;

use super::*;
use crate::TokioIo;

/// A `FallibleListener` that fails in a way the test chooses, and counts the asking.
///
/// It is written through `FallibleListener` alone -- no retry, no classification, no timing,
/// no yielding -- which is the point: it is also the evidence that the documented shortest
/// path is writable by someone outside this crate.
struct Programmable {
    outcome: Outcome,
    calls: Arc<AtomicUsize>,
    /// Panic rather than return once this many calls have been made, if set.
    cap: Option<usize>,
}

#[derive(Clone, Copy)]
enum Outcome {
    /// Fail with something outside the transient set, so it is paced.
    Systemic,
    /// Fail with something inside the transient set, so it is retried at once.
    Transient,
    /// Succeed the first time, then fail systemically.
    ThenSystemic,
}

impl Programmable {
    fn new(outcome: Outcome) -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                outcome,
                calls: Arc::clone(&calls),
                cap: None,
            },
            calls,
        )
    }

    fn capped(outcome: Outcome, cap: usize) -> (Self, Arc<AtomicUsize>) {
        let (mut listener, calls) = Self::new(outcome);
        listener.cap = Some(cap);
        (listener, calls)
    }
}

impl FallibleListener for Programmable {
    type Io = TokioIo<DuplexStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        let seen = self.calls.fetch_add(1, Ordering::SeqCst);

        if let Some(cap) = self.cap {
            assert!(
                seen < cap,
                "the retry wrapper called the underlying accept {} times within a single \
                 poll, where at most {cap} was correct: it is not pacing itself and would \
                 spin",
                seen + 1
            );
        }

        match self.outcome {
            Outcome::ThenSystemic if seen == 0 => {
                let (io, _peer) = tokio::io::duplex(64);
                Ok((TokioIo::new(io), "127.0.0.1:1".parse().unwrap()))
            }
            Outcome::Transient => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            // `EMFILE` reaches Rust as `Uncategorized`, which is what this stands in for.
            Outcome::Systemic | Outcome::ThenSystemic => {
                Err(io::Error::from_raw_os_error(24))
            }
        }
    }
}

/// Polls a future exactly once and reports whether it finished.
///
/// The waker is a no-op: nothing here needs to be woken, because the assertion is about what
/// happened *during* the poll rather than about what happens later.
fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = pin!(future);
    let waker = std::task::Waker::noop();
    future.as_mut().poll(&mut Context::from_waker(waker))
}

/// Drives a listener the way the server does, with a competing arm, and counts attempts.
///
/// This reproduces the arbitration in `server::run`: an inner `select!` whose other arm
/// becomes ready every 100ms, so the accept future is dropped and rebuilt constantly. That
/// dropping is the whole hazard, and a listener that keeps its backoff inside the future
/// rather than in itself is starved by it.
async fn attempts_under_a_competing_arm<L: Listener>(mut listener: L, window: Duration) -> usize {
    let start = tokio::time::Instant::now();
    let mut competing = tokio::time::interval(Duration::from_millis(100));
    competing.tick().await;

    while start.elapsed() < window {
        tokio::select! {
            _ = listener.accept() => {}
            _ = competing.tick() => {}
        }
    }

    0 // the caller reads the count from its own handle
}

/// SC-005. The regression this whole design exists to prevent.
///
/// A systemic failure every time, a competing arm every 100ms, and a listener that already
/// owes a backoff. Correct pacing attempts the underlying accept about once a second, so
/// about three times in 3.4 seconds. A wrapper whose backoff is a relative sleep is starved
/// to zero; a wrapper with no backoff, or one that clears its deadline before sleeping
/// rather than after, spins to more than thirty.
#[tokio::test(flavor = "current_thread")]
async fn a_systemic_failure_is_paced_even_though_the_accept_future_keeps_being_dropped() {
    let (inner, calls) = Programmable::new(Outcome::Systemic);
    let mut listener = RetryingListener::new(inner);

    // Arm the backoff, and discount the attempt that armed it, so the count below is purely
    // about pacing rather than about the first failure.
    let _ = poll_once(listener.accept());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the first attempt arms the backoff");
    calls.store(0, Ordering::SeqCst);

    attempts_under_a_competing_arm(listener, Duration::from_millis(3400)).await;

    let observed = calls.load(Ordering::SeqCst);
    assert!(
        (2..=4).contains(&observed),
        "expected the underlying accept to be attempted about once a second -- roughly 3 \
         times in 3.4s -- but it was attempted {observed} times. Zero means the backoff is \
         relative and is being reset every time the accept future is dropped, so the \
         listener is never retried at all. Many means it is not pacing itself and is \
         spinning on a failure that recurs."
    );
}

/// SC-005a. The mutation-pressure half of the criterion above.
///
/// A single poll, against a source that panics if asked more than twice. A wrapper with no
/// backoff reaches that panic inside this very poll, where the assertion can see it; a timed
/// test could not, because a spinning future starves the timer meant to time it out.
#[tokio::test(flavor = "current_thread")]
async fn a_systemic_failure_does_not_spin_within_a_single_poll() {
    let (inner, calls) = Programmable::capped(Outcome::Systemic, 2);
    let mut listener = RetryingListener::new(inner);

    let polled = poll_once(listener.accept());

    assert!(polled.is_pending(), "a listener that cannot accept must yield to the loop");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one systemic failure should arm the backoff and then wait, not try again"
    );
}

/// SC-006. Transient failures are retried at once rather than paced.
#[tokio::test(flavor = "current_thread")]
async fn a_transient_failure_is_retried_immediately() {
    let (inner, calls) = Programmable::new(Outcome::Transient);
    let listener = RetryingListener::new(inner);

    attempts_under_a_competing_arm(listener, Duration::from_millis(3400)).await;

    let observed = calls.load(Ordering::SeqCst);
    assert!(
        observed > 20,
        "a failure naming one client should be retried at once, but the underlying accept \
         was attempted only {observed} times -- it is being paced when it should not be"
    );
}

/// SC-006. The classification itself, which is what decides between the two tests above.
#[test]
fn only_per_client_failures_are_transient() {
    for kind in [
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::Interrupted,
    ] {
        assert!(
            is_transient(&io::Error::from(kind)),
            "{kind:?} names one client and should be retried at once"
        );
    }

    for kind in [
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::OutOfMemory,
        io::ErrorKind::InvalidInput,
    ] {
        assert!(
            !is_transient(&io::Error::from(kind)),
            "{kind:?} is about the listener rather than one client and should be paced"
        );
    }

    // The case the backoff exists for. `EMFILE` maps to no named `ErrorKind` at all -- the
    // variant it lands on is unstable and cannot be written down here -- so this pins the
    // property that actually matters: that it is paced rather than quietly treated as a
    // per-client failure and spun on.
    let emfile = io::Error::from_raw_os_error(24);
    assert!(!is_transient(&emfile), "EMFILE must be paced, not spun on");
}

/// SC-007. The cooperative yield, under the same single-poll pressure as SC-005a.
///
/// A source that fails transiently forever and panics on its second call. Correct behaviour
/// asks once and yields; a wrapper that does not yield asks again inside this poll and trips
/// the panic. A timed test cannot express this: the non-yielding wrapper would monopolise the
/// poll and hang rather than fail.
#[tokio::test(flavor = "current_thread")]
async fn a_transient_failure_yields_before_retrying() {
    let (inner, calls) = Programmable::capped(Outcome::Transient, 1);
    let mut listener = RetryingListener::new(inner);

    let polled = poll_once(listener.accept());

    assert!(
        polled.is_pending(),
        "yielding must leave the accept future pending so the loop's other arms are polled"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a transient failure should be retried, but only after giving the loop a turn"
    );
}

/// SC-008. A success clears the debt, so the next failure waits a full period.
#[tokio::test(flavor = "current_thread")]
async fn a_success_clears_the_backoff() {
    let (inner, calls) = Programmable::new(Outcome::ThenSystemic);
    let mut listener = RetryingListener::new(inner);

    let accepted = poll_once(listener.accept());
    assert!(accepted.is_ready(), "the first accept succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The next attempt fails systemically and arms a fresh backoff. If success had not
    // cleared the deadline, this would wait out a stale one instead of trying immediately.
    let started = tokio::time::Instant::now();
    let polled = poll_once(listener.accept());
    assert!(polled.is_pending());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "after a success the next accept should be attempted at once"
    );
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "the attempt after a success should not have waited"
    );
}

/// The deadline survives the accept future being dropped, which is the mechanism the timed
/// tests observe from the outside. Asserted directly so a regression names its own cause.
#[tokio::test(flavor = "current_thread")]
async fn the_backoff_deadline_outlives_the_accept_future() {
    let (inner, _calls) = Programmable::new(Outcome::Systemic);
    let mut listener = RetryingListener::new(inner);

    assert!(listener.backoff.is_none(), "nothing is owed before the first failure");

    let polled = poll_once(listener.accept());
    assert!(polled.is_pending());

    // The future from that poll has been dropped by now. The deadline must still be here:
    // it lives in the listener rather than in the future, which is the whole design.
    let deadline = listener
        .backoff
        .expect("the backoff deadline must survive the accept future being dropped");
    assert!(
        deadline > tokio::time::Instant::now(),
        "the deadline should still be in the future"
    );
}
