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
