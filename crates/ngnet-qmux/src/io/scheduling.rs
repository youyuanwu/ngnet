//! Parking, waking, and the bound on how far the layer reads ahead of its caller.
//!
//! Two mechanisms live here, and they are together because they answer the same question from
//! opposite ends: *when may this connection stop, and what starts it again?*
//!
//! # Parking, and why a self-wake is not a placeholder one can leave in
//!
//! An operation that cannot proceed has two ways to return [`Pending`](core::task::Poll):
//! register the caller's waker somewhere that will fire, or wake the caller immediately and
//! hope the condition has changed by the next poll. The second is a busy loop wearing a
//! future's clothes. It costs a core per stalled connection, it makes an idle connection
//! indistinguishable from a spinning one in a profile, and on a runtime that runs the woken
//! task before checking any I/O it can starve the very reads that would unblock it.
//!
//! So every wait in this layer is parked against the event that ends it:
//!
//! - An open the peer's stream limit forbids waits on [`Signals::park_open`], and the
//!   `extend_max_streams` handler wakes it. That callback is dwnx telling this side the peer
//!   raised the limit, which is the only thing that can make the open succeed.
//! - A write with no flow-control credit waits on [`Signals::park_credit`], and the
//!   `extend_max_stream_data` handler wakes it. The connection-level window has **no
//!   callback** -- dwnx updates `tx.max_offset` from a MAX_DATA frame and tells nobody
//!   (`deps/dwnx/lib/dwnx_conn.c:1045-1056`) -- so the connection watches
//!   [`max_data_left`](crate::Conn::max_data_left) across a read and wakes the same slot when
//!   it moves. Waking on *any* inbound bytes would have been simpler and would have spun a
//!   blocked writer once per arriving record for as long as the peer kept talking.
//! - A read the caller has not made room for waits on [`Signals::park_read_ahead`], and
//!   extending connection credit wakes it.
//!
//! A slot holds one waker. Only one task can poll a connection at a time -- every entry point
//! takes `&mut self` -- so a second waker arriving in the same slot means the connection has
//! changed hands. The displaced waker is woken rather than dropped: a spurious poll costs one
//! wasted pass, and a lost wakeup costs a connection that never moves again. Wakes happen
//! outside the lock, because a waker may do arbitrary work and this one is held by handlers
//! running inside a C callback.
//!
//! # Read-ahead, measured as delivered-but-uncredited
//!
//! [`ReadAhead`] counts bytes the caller has been *handed* and has not yet reported consuming.
//! Not queue depth: a caller that drains events into a `Vec` of its own without crediting them
//! has taken responsibility for exactly as much memory, and a bound on the queue would read
//! zero while that `Vec` grew without limit. Queue depth is bounded anyway, by the protocol's
//! own receive window, which reopens only when the caller credits -- so measuring the thing
//! the protocol does not already measure is what makes this bound worth having.
//!
//! The allowance is a starting figure, not a ceiling on the connection: the caller may be
//! handed `allowance` bytes before crediting anything, and every byte credited back buys
//! another. Which is to say the ceiling on total delivery is `allowance + credited`, and the
//! configured number governs only the part before any credit has been extended.
//!
//! ## Only connection-level credit counts, and getting that wrong is unbounded
//!
//! [`ReadAhead::credited`] is called from
//! [`extend_connection_credit`](super::Connection::extend_connection_credit) and from nowhere
//! else. The HTTP/3 layer above reports every consumed byte **twice**, once naming a stream
//! and once naming the connection, because stream-level credit does not imply connection-level
//! credit (`crates/ngnet-h3/src/http/quic.rs:319-321`). Counting both would credit two bytes
//! for every one delivered, so the outstanding figure would fall as fast as it rose, the bound
//! would never bind, and read-ahead would be limited by nothing at all. The failure is silent:
//! a connection that reads as fast as the peer can send and grows until something else runs
//! out.

use core::task::{Context, Waker};
use std::sync::{Arc, Mutex, PoisonError};

/// Which wait a waker belongs to.
///
/// Separate slots rather than one, because the conditions are separate: a peer that extends a
/// stream window has not raised the stream limit, and a caller that credits back consumed
/// bytes has done neither. One shared slot would wake every waiter for every event, which is
/// correct but is the spin this module exists to avoid.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Slot {
    /// Waiting for the peer to raise a stream-count limit.
    Open,
    /// Waiting for flow-control credit, on a stream or across the connection.
    Credit,
    /// Waiting for the caller to credit back bytes it has been delivered.
    ReadAhead,
}

/// The wakers the layer parks, shared with the handlers that fire them.
///
/// Cloneable and shared for the same reason the event queue is: the state machine requires its
/// handlers to be `Send`, and two of them need to reach a waker parked by an entry point that
/// has long since returned.
#[derive(Clone, Debug, Default)]
pub(crate) struct Signals {
    parked: Arc<Mutex<Parked>>,
}

#[derive(Debug, Default)]
struct Parked {
    open: Option<Waker>,
    credit: Option<Waker>,
    read_ahead: Option<Waker>,
}

impl Parked {
    fn slot(&mut self, slot: Slot) -> &mut Option<Waker> {
        match slot {
            Slot::Open => &mut self.open,
            Slot::Credit => &mut self.credit,
            Slot::ReadAhead => &mut self.read_ahead,
        }
    }
}

impl Signals {
    /// No one waiting for anything.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Waits for the peer to raise a stream-count limit.
    pub(crate) fn park_open(&self, cx: &Context<'_>) {
        self.park(Slot::Open, cx);
    }

    /// The peer raised a stream-count limit.
    pub(crate) fn wake_open(&self) {
        self.wake(Slot::Open);
    }

    /// Waits for flow-control credit.
    pub(crate) fn park_credit(&self, cx: &Context<'_>) {
        self.park(Slot::Credit, cx);
    }

    /// The peer extended a window, on a stream or across the connection.
    pub(crate) fn wake_credit(&self) {
        self.wake(Slot::Credit);
    }

    /// Waits for the caller to credit back delivered bytes.
    pub(crate) fn park_read_ahead(&self, cx: &Context<'_>) {
        self.park(Slot::ReadAhead, cx);
    }

    /// The caller credited back delivered bytes, so reading may resume.
    pub(crate) fn wake_read_ahead(&self) {
        self.wake(Slot::ReadAhead);
    }

    fn park(&self, slot: Slot, cx: &Context<'_>) {
        let displaced = {
            let mut parked = self.lock();
            let held = parked.slot(slot);
            match held {
                // Re-parking the same task is the common case -- a poll that finds the same
                // condition -- and cloning a waker on every one of those is pure cost.
                Some(existing) if existing.will_wake(cx.waker()) => None,
                _ => held.replace(cx.waker().clone()),
            }
        };
        if let Some(waker) = displaced {
            waker.wake();
        }
    }

    fn wake(&self, slot: Slot) {
        // Taken under the lock and woken outside it. A waker is caller-supplied code, and two
        // of these fire from inside a dwnx callback; holding a lock across it would make the
        // layer's internals part of that code's contract.
        let waker = self.lock().slot(slot).take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Parked> {
        // Recovered from rather than propagated, exactly as the event queue does: poisoning
        // requires a panic while the lock was held, and a panic in a handler aborts the
        // process regardless, since it would otherwise unwind through C.
        self.parked.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// How far the layer has read ahead of its caller.
///
/// See the [module documentation](self) for why this is measured in delivered bytes and why
/// only connection-level credit is subtracted from it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadAhead {
    allowance: u64,
    delivered: u64,
    credited: u64,
}

impl ReadAhead {
    /// A fresh count with the configured allowance.
    pub(crate) const fn new(allowance: u64) -> Self {
        Self {
            allowance,
            delivered: 0,
            credited: 0,
        }
    }

    /// Records bytes handed to the caller.
    pub(crate) const fn delivered(&mut self, bytes: u64) {
        self.delivered = self.delivered.saturating_add(bytes);
    }

    /// Records bytes the caller reported consuming, connection-wide.
    pub(crate) const fn credited(&mut self, bytes: u64) {
        self.credited = self.credited.saturating_add(bytes);
    }

    /// Bytes delivered and not yet credited back.
    ///
    /// Saturating, so a caller that credits more than it was delivered banks the difference
    /// rather than wrapping into an enormous figure that would stop the connection reading
    /// until it was closed.
    pub(crate) const fn outstanding(&self) -> u64 {
        self.delivered.saturating_sub(self.credited)
    }

    /// Whether the layer must stop reading until the caller credits something back.
    pub(crate) const fn is_exhausted(&self) -> bool {
        self.outstanding() >= self.allowance
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::task::Poll;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    #[derive(Default)]
    struct Counter {
        wakes: AtomicUsize,
    }

    impl Counter {
        fn count(&self) -> usize {
            self.wakes.load(Ordering::SeqCst)
        }
    }

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Waker, Arc<Counter>) {
        let counter = Arc::new(Counter::default());
        (Waker::from(Arc::clone(&counter)), counter)
    }

    /// Parking is not waking: the whole point is that nothing happens until the event does.
    #[test]
    fn parking_alone_wakes_nobody() {
        let signals = Signals::new();
        let (waker, counter) = counting_waker();
        signals.park_open(&Context::from_waker(&waker));
        signals.park_credit(&Context::from_waker(&waker));
        signals.park_read_ahead(&Context::from_waker(&waker));
        assert_eq!(
            counter.count(),
            0,
            "parking woke the caller, which is the spin this module exists to prevent"
        );
    }

    #[test]
    fn each_slot_is_woken_only_by_its_own_event() {
        let signals = Signals::new();
        let (waker, counter) = counting_waker();

        signals.park_open(&Context::from_waker(&waker));
        signals.wake_credit();
        assert_eq!(
            counter.count(),
            0,
            "a credit extension woke a waiter for stream capacity"
        );
        signals.wake_open();
        assert_eq!(
            counter.count(),
            1,
            "the limit was raised and nobody noticed"
        );
    }

    /// A slot is emptied by the wake, so a second event does not wake a task that has moved on.
    #[test]
    fn a_woken_slot_is_empty_until_it_is_parked_again() {
        let signals = Signals::new();
        let (waker, counter) = counting_waker();

        signals.park_credit(&Context::from_waker(&waker));
        signals.wake_credit();
        signals.wake_credit();
        assert_eq!(counter.count(), 1);
    }

    /// Re-parking the same task replaces nothing and wakes nobody; a different one is woken
    /// rather than dropped, because a lost wakeup is a connection that never moves again.
    #[test]
    fn a_displaced_waker_is_woken_and_the_same_one_is_not() {
        let signals = Signals::new();
        let (first, first_count) = counting_waker();
        let (second, second_count) = counting_waker();

        signals.park_credit(&Context::from_waker(&first));
        signals.park_credit(&Context::from_waker(&first));
        assert_eq!(first_count.count(), 0, "re-parking the same task woke it");

        signals.park_credit(&Context::from_waker(&second));
        assert_eq!(
            first_count.count(),
            1,
            "the displaced task was dropped rather than woken, and will wait forever"
        );
        signals.wake_credit();
        assert_eq!(second_count.count(), 1);
    }

    /// The bound the handlers depend on: a signal set has to satisfy the state machine's
    /// `Send` requirement on handlers, or two of these wakes could not be issued at all.
    #[test]
    fn signals_are_sendable() {
        fn require_send<T: Send>(_: &T) {}
        require_send(&Signals::new());
    }

    #[test]
    fn read_ahead_is_exhausted_at_the_allowance_and_not_before() {
        let mut read_ahead = ReadAhead::new(100);
        read_ahead.delivered(99);
        assert!(!read_ahead.is_exhausted());
        read_ahead.delivered(1);
        assert!(
            read_ahead.is_exhausted(),
            "the allowance is a limit, not a hint"
        );
        assert_eq!(read_ahead.outstanding(), 100);
    }

    /// Credit raises the ceiling by exactly what it credits: the allowance governs only the
    /// stretch before any credit has been extended.
    #[test]
    fn credit_buys_exactly_as_much_read_ahead_as_it_reports() {
        let mut read_ahead = ReadAhead::new(100);
        read_ahead.delivered(100);
        assert!(read_ahead.is_exhausted());
        read_ahead.credited(40);
        assert_eq!(read_ahead.outstanding(), 60);
        assert!(!read_ahead.is_exhausted());
        read_ahead.delivered(40);
        assert!(
            read_ahead.is_exhausted(),
            "40 credited bought 40 more, not 80"
        );
    }

    /// Banked credit is not negative outstanding, which would otherwise wrap.
    #[test]
    fn crediting_more_than_was_delivered_banks_rather_than_wraps() {
        let mut read_ahead = ReadAhead::new(10);
        read_ahead.credited(1_000);
        assert_eq!(read_ahead.outstanding(), 0);
        read_ahead.delivered(500);
        assert_eq!(read_ahead.outstanding(), 0);
        assert!(!read_ahead.is_exhausted());
    }

    /// A zero allowance stops reading until the caller says something, which is a legitimate
    /// configuration and must not be mistaken for "unbounded".
    #[test]
    fn a_zero_allowance_is_exhausted_from_the_start() {
        let read_ahead = ReadAhead::new(0);
        assert!(read_ahead.is_exhausted());
        assert_eq!(read_ahead.outstanding(), 0);
    }

    /// Nothing here is a future, but the slots are polled by things that are; this pins that a
    /// parked slot leaves the caller pending rather than ready by accident.
    #[test]
    fn a_parked_slot_is_not_a_completed_one() {
        let signals = Signals::new();
        let (waker, counter) = counting_waker();
        let outcome: Poll<()> = {
            signals.park_read_ahead(&Context::from_waker(&waker));
            Poll::Pending
        };
        assert!(outcome.is_pending());
        assert_eq!(counter.count(), 0);
        signals.wake_read_ahead();
        assert_eq!(counter.count(), 1);
    }
}
