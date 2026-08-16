//! Tests for the accept-retry policy in [`pace_after`].
//!
//! The policy used to be a public wrapper type holding an absolute deadline, because the
//! server's accept future was dropped and rebuilt constantly and a relative sleep inside it
//! never elapsed. Most of the tests here were about that. The loop has two arms now, the
//! policy is an ordinary `async fn` with an ordinary sleep, and what is left to test is
//! smaller and more direct: how it classifies a failure, and what it does about each class.
//!
//! Two kinds live here, and the distinction still matters.
//!
//! The *single-poll* tests poll the policy exactly once, by hand. They exist because a timed
//! test cannot catch a future that never returns from `poll`: a `tokio::time::timeout` is
//! itself a cooperative future, so a policy that spins inside one poll starves the very timer
//! meant to detect it, and the test hangs rather than failing. Polling by hand moves the
//! failure inside the poll the test drives, where it is observable.
//!
//! The *virtual-time* tests await the policy to completion under [`tokio::time::pause`] and
//! read the clock. They cost no real time at all, which is why the systemic case can assert
//! the full one-second pace rather than a proxy for it.

use std::future::Future;
use std::io;
use std::pin::pin;
use std::task::{Context, Poll};

use super::*;

/// Polls a future exactly once and reports whether it finished.
///
/// The waker is a no-op: nothing here needs to be woken, because the assertion is about what
/// happened *during* the poll rather than about what happens later.
fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = pin!(future);
    let waker = std::task::Waker::noop();
    future.as_mut().poll(&mut Context::from_waker(waker))
}

/// The four failures that name one client rather than the listener.
const TRANSIENT: [io::ErrorKind; 4] = [
    io::ErrorKind::ConnectionAborted,
    io::ErrorKind::ConnectionRefused,
    io::ErrorKind::ConnectionReset,
    io::ErrorKind::Interrupted,
];

/// SC-006. The classification, which is what decides between the two behaviours below.
#[test]
fn only_per_client_failures_are_transient() {
    for kind in TRANSIENT {
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

/// SC-032. What each class actually costs, measured on the clock rather than inferred.
///
/// Virtual time, so the one-second pace is asserted exactly and takes no real time. A
/// transient failure must cost nothing: pacing one would stop the listener accepting the
/// client waiting behind the one that vanished. A systemic failure must cost the full
/// period, because it will recur immediately and retrying it at once is a spin.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn each_class_of_failure_costs_what_it_should() {
    for kind in TRANSIENT {
        let started = tokio::time::Instant::now();
        pace_after(&io::Error::from(kind)).await;

        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "{kind:?} names one client, and the next accept will succeed: pacing it would \
             make one vanished client delay every client behind it"
        );
    }

    for systemic in [
        io::Error::from(io::ErrorKind::PermissionDenied),
        // `EMFILE`, the case that motivates the whole policy.
        io::Error::from_raw_os_error(24),
    ] {
        let started = tokio::time::Instant::now();
        pace_after(&systemic).await;

        assert_eq!(
            started.elapsed(),
            ACCEPT_BACKOFF,
            "a failure about the listener rather than about one client recurs immediately, \
             so it must be paced by a full period; {systemic:?} was not"
        );
    }
}

/// SC-005a. A systemic failure does not spin within a single poll.
///
/// Asserted by hand rather than by timing, because a policy that spins would starve the
/// timer meant to time it out and the test would hang rather than fail.
#[tokio::test(flavor = "current_thread")]
async fn a_systemic_failure_yields_rather_than_returning() {
    let polled = poll_once(pace_after(&io::Error::from_raw_os_error(24)));

    assert!(
        polled.is_pending(),
        "a systemic failure must leave the caller pending for a full period: returning \
         within the poll would put the listener straight back into an accept that is about \
         to fail the same way"
    );
}

/// SC-007. The cooperative yield, under the same single-poll pressure.
///
/// A transient failure is retried at once -- but *at once* must still mean after a return to
/// the runtime. The only other arm of the server's loop is the stop signal, so a listener
/// that retries without yielding is a server that cannot be shut down.
#[tokio::test(flavor = "current_thread")]
async fn a_transient_failure_yields_before_retrying() {
    let polled = poll_once(pace_after(&io::Error::from(
        io::ErrorKind::ConnectionAborted,
    )));

    assert!(
        polled.is_pending(),
        "yielding must leave the accept future pending so the loop's stop arm is polled: \
         without it a listener failing transiently in a loop monopolises the poll and the \
         server can never see its own shutdown signal"
    );
}

/// A source of accept results, scripted by the test and counting the asking.
///
/// It stands in for a socket, because no real socket can be made to fail on demand -- which
/// is the whole reason the retry loop was extracted into a function taking a closure instead
/// of being written out inside each shipped listener.
struct Scripted {
    /// Returned in order; the last is repeated once exhausted.
    script: Vec<io::ErrorKind>,
    calls: usize,
    /// Panic rather than return once this many calls have been made, if set.
    cap: Option<usize>,
}

impl Scripted {
    /// Fails with each kind in turn, then succeeds.
    fn failing(script: &[io::ErrorKind]) -> Self {
        Self {
            script: script.to_vec(),
            calls: 0,
            cap: None,
        }
    }

    /// Fails forever, and panics if asked more than `cap` times.
    fn capped(kind: io::ErrorKind, cap: usize) -> Self {
        Self {
            script: vec![kind],
            calls: 0,
            cap: Some(cap),
        }
    }

    fn next(&mut self) -> io::Result<usize> {
        let seen = self.calls;
        self.calls += 1;

        if let Some(cap) = self.cap {
            assert!(
                seen < cap,
                "the retry loop asked the underlying accept {} times within a single poll, \
                 where at most {cap} was correct: it is not pacing itself and would spin",
                seen + 1
            );
        }

        match self.script.get(seen) {
            Some(kind) => Err(io::Error::from(*kind)),
            None => Ok(seen),
        }
    }
}

/// SC-005. The retry loop retries until it succeeds, and hands back the success.
///
/// Virtual time, so the two systemic failures cost two seconds of clock and no real time.
/// This is the property the shipped listeners get by calling the helper: an accept that
/// fails is not an accept that stops.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_failing_source_is_retried_until_it_succeeds() {
    let mut source = Scripted::failing(&[
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::PermissionDenied,
    ]);

    let started = tokio::time::Instant::now();
    let accepted = accept_retrying(|| std::future::ready(source.next())).await;

    assert_eq!(
        accepted, 3,
        "the retry loop must return the first success, not a failure and not a later one"
    );
    assert_eq!(
        started.elapsed(),
        ACCEPT_BACKOFF * 2,
        "one transient failure costs nothing and two systemic ones cost a period each: a \
         loop that paced the transient one, or failed to pace a systemic one, lands elsewhere"
    );
}

/// SC-005a. The retry loop does not spin within a single poll.
///
/// Asserted by hand rather than by timing: a spinning loop starves the timer that would time
/// it out, so a timed test hangs rather than failing. The source panics on its second call,
/// which puts the failure inside the poll this test drives.
#[tokio::test(flavor = "current_thread")]
async fn a_source_that_always_fails_is_not_spun_on() {
    let mut source = Scripted::capped(io::ErrorKind::PermissionDenied, 1);

    let polled = poll_once(accept_retrying(|| std::future::ready(source.next())));

    assert!(
        polled.is_pending(),
        "a listener that cannot accept must yield to the server's loop rather than \
         monopolising the poll"
    );
    assert_eq!(
        source.calls, 1,
        "one systemic failure should be paced and then waited out, not tried again at once"
    );
}
