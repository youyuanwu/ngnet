//! The pool: one eligible connection per origin, and the state machine that gets there.
//!
//! # Why this is a state machine and not a `OnceCell`
//!
//! The requirement is easy to state — N concurrent requests to an origin with nothing pooled
//! must open one connection — and the standard answer,
//! [`tokio::sync::OnceCell::get_or_try_init`], is wrong for it. When the initialiser fails,
//! tokio does not deliver that error to the callers already waiting. It releases the permit
//! and lets one of them start its own attempt. A burst of ten at a dead origin therefore
//! makes up to ten *serial* connection attempts, each waiting for the last to time out, and
//! the callers in one burst see unrelated outcomes.
//!
//! So the dial state is explicit. Each origin has a [`Slot`], holding a [`Dial`] behind a
//! `std::sync::Mutex` and a `watch` channel used purely as a "the state moved" bell:
//!
//! ```text
//! Idle ──acquire──► Dialing ──success──► Ready(handle)
//!                      │                     │
//!                      └──failure──► Failed(e)│
//!                                        │    │
//!                            new arrival ▼    ▼ evicted (GOAWAY / closed)
//!                                     Dialing ◄──────────
//! ```
//!
//! The rule that makes it correct is one line: **a caller that has already waited takes
//! whatever it wakes to, and never starts a dial on the strength of a `Failed` it woke to.**
//! Without it, a woken waiter would see `Failed`, treat it as "nothing here, dial one", and
//! the fan-out would degrade back into the serial retry `OnceCell` produces. With it, a
//! failure is fanned out to everyone waiting on it and spent for everyone arriving after.
//!
//! A caller that waited may still wake to find a *newcomer* has moved the slot back to
//! `Dialing`; it waits again, and may end up with a working connection rather than the error.
//! That is allowed and is better for that caller — what is never allowed is a waiter dialling
//! for itself.
//!
//! # The lost wakeup, and why `subscribe()` after the unlock would not do
//!
//! A waiter must release the slot lock before awaiting, or nothing can ever transition. If it
//! then called [`watch::Sender::subscribe`] and awaited a change, a transition landing in the
//! window between the unlock and the subscribe would be missed and the waiter would park for
//! ever — the receiver's initial version is whatever was current when `subscribe` ran, so the
//! change it needed is already in its past.
//!
//! The fix is to capture the generation number **while still holding the slot lock**, and to
//! wait on the *value* rather than on a change: [`watch::Receiver::wait_for`] evaluates its
//! predicate against the current value before it awaits, so a generation that moved during
//! the window is seen immediately.
//!
//! # Two locks, never held at once
//!
//! There are exactly two: the pool's, over the origin map and shutdown bookkeeping, and each
//! slot's, over its `Dial`. **They are never held simultaneously**, so there is no lock order
//! to get wrong and no way for a `Drop` running under one to deadlock on the other. Slot
//! `Arc`s are cloned out from under the pool lock, which is then released before the slot
//! lock is taken.
//!
//! Both are `std::sync::Mutex` rather than `tokio::sync::Mutex`, which is load-bearing twice
//! over. Its guard is not `Send`, so holding one across an `await` does not compile — the
//! no-lock-across-a-suspension-point invariant is enforced by the compiler on every future
//! change rather than by a test on the code as written. And `Drop` cannot `await`, so the
//! cancellation guards below, which must release state synchronously when a caller's future
//! is dropped, could not use an async lock at all.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body::Body;
use ngnet_h2::http::Config;
use ngnet_h2::http::client::SendRequest;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::connect::dial;
use crate::error::{Error, Reason};
use crate::origin::Origin;

/// Where a given origin's connection has got to.
enum Dial<B> {
    /// Nothing here, and nobody working on it.
    Idle,
    /// Somebody is dialling. Everyone else waits.
    Dialing,
    /// A live connection. Cloned out to every caller.
    Ready(SendRequest<B>),
    /// The last attempt failed. Handed to the callers that were waiting on it; a caller
    /// arriving afterwards replaces it with a fresh attempt.
    ///
    /// `Arc` because one failure is reported to arbitrarily many waiters, and [`Error`] is
    /// not `Clone` — it carries a boxed cause that may itself not be.
    Failed(Arc<Error>),
}

/// What one pass of [`Pool::acquire`]'s loop decided, computed under the slot lock and acted
/// on after it is released.
///
/// Splitting the decision from the action is not ceremony: it makes it structurally
/// impossible to await while holding the lock, rather than merely illegal.
enum Decision<B> {
    Ready(SendRequest<B>),
    Failed(Arc<Error>),
    Dial,
    Wait(u64),
}

/// One origin's dial state, plus the bell that says it moved.
struct Slot<B> {
    state: Mutex<Dial<B>>,
    /// Incremented on every transition. Waiters capture the value under the lock and wait
    /// for it to differ, which is what makes a transition in the unlock/await window
    /// impossible to miss.
    settled: watch::Sender<u64>,
}

impl<B> Slot<B> {
    fn new() -> Self {
        Self {
            state: Mutex::new(Dial::Idle),
            settled: watch::Sender::new(0),
        }
    }

    /// Publishes a new state and rings the bell.
    ///
    /// `send_modify` rather than `send`: `send` fails when there are no receivers, and a
    /// transition with nobody currently waiting still has to be *retained* so that the next
    /// subscriber sees the new generation rather than a stale zero.
    ///
    /// It is also `send_modify` rather than `send_replace(self.settled.borrow() + 1)`, and
    /// that is not a style preference. `borrow()` returns a guard over the channel's own
    /// lock which lives to the end of the *statement* it appears in, so folding it into the
    /// argument leaves it held while `send_replace` asks for the same lock to write. That
    /// deadlocks — and because the lock is a blocking one, it deadlocks the executor thread
    /// rather than the task, so even a `tokio::time::timeout` wrapped around the request
    /// never fires. `send_modify` takes the lock once and increments in place.
    ///
    /// The slot lock is deliberately held across the bump, so that the state and the
    /// generation move together as far as any waiter is concerned. A waiter captures the
    /// generation under this same lock, so a split here would let one capture the new state's
    /// old generation and park for a transition that had already happened.
    fn settle(&self, state: Dial<B>) {
        let mut current = self.state.lock().expect("slot lock poisoned");
        *current = state;
        self.settled
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// The pool's own state, all of it under one lock.
struct State<B> {
    connections: HashMap<Origin, Arc<Slot<B>>>,
    /// Driver tasks. A `Vec<JoinHandle>` and deliberately not a [`tokio::task::JoinSet`]:
    /// `JoinSet` aborts its tasks when dropped, and dropping the last `Client` must release
    /// the pool's interest in its connections without cancelling exchanges still running on
    /// them. `JoinHandle` detaches on drop, which is the behaviour required.
    drivers: Vec<JoinHandle<()>>,
    closed: bool,
    /// Acquires that have started and not yet finished registering their driver.
    ///
    /// Shutdown waits for this to reach zero *before* draining, because an acquire in flight
    /// may be about to add a connection the drain would otherwise walk straight past — and
    /// that connection would then be left running with nobody waiting for it.
    acquires: usize,
}

/// The connection pool shared by every clone of a [`Client`](crate::Client).
pub(crate) struct Pool<B> {
    state: Mutex<State<B>>,
    /// Mirrors `State::acquires` for awaiting. Kept in step under the same lock.
    acquires: watch::Sender<usize>,
    /// Set once the drain has completed. Every caller of `shutdown` awaits it, including the
    /// one that performed the drain, so concurrent callers report the same completion.
    finished: watch::Sender<bool>,
    /// A lock-free fast path for "is this client closed", so a request offered after shutdown
    /// is refused without touching the pool lock.
    closed: AtomicBool,
    config: Config,
    /// How many times a name has been resolved. The only observable for the claim that a
    /// request served by a pooled connection resolves nothing — a lookup that did not happen
    /// leaves no trace at a peer, which saw no connection either way.
    resolutions: AtomicUsize,
    /// A barrier used only by this crate's own tests, to park a dial at a chosen instant.
    ///
    /// A field rather than a `static` because Rust runs a crate's unit tests as threads in
    /// one process: a global would let one test's barrier stall another's pool.
    #[cfg(test)]
    pub(crate) dial_barrier: Mutex<Option<Arc<tokio::sync::Notify>>>,
}

impl<B> Pool<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub(crate) fn new(config: Config) -> Self {
        Self {
            state: Mutex::new(State {
                connections: HashMap::new(),
                drivers: Vec::new(),
                closed: false,
                acquires: 0,
            }),
            acquires: watch::Sender::new(0),
            finished: watch::Sender::new(false),
            closed: AtomicBool::new(false),
            config,
            resolutions: AtomicUsize::new(0),
            #[cfg(test)]
            dial_barrier: Mutex::new(None),
        }
    }

    /// How many name resolutions this pool has performed. See [`Pool::resolutions`].
    pub(crate) fn resolution_count(&self) -> usize {
        self.resolutions.load(Ordering::Relaxed)
    }

    /// Whether this pool has been shut down.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Whether an origin written as `host:port` currently has a connection this pool would
    /// hand to a new request.
    ///
    /// Exists for the eviction test, through the hidden testing module: "the client has
    /// noticed the peer's `GOAWAY`" has no observable at the peer — the peer sent it and
    /// learns nothing more — so the only alternative is to sleep and hope.
    pub(crate) fn has_eligible_connection(&self, authority: &str) -> bool {
        let Ok(uri) = format!("http://{authority}/").parse::<http::Uri>() else {
            return false;
        };
        let Ok(origin) = Origin::from_uri(&uri) else {
            return false;
        };

        let slot = {
            let state = self.state.lock().expect("pool lock poisoned");
            state.connections.get(&origin).map(Arc::clone)
        };

        let Some(slot) = slot else { return false };
        let state = slot.state.lock().expect("slot lock poisoned");
        match &*state {
            Dial::Ready(handle) => !handle.is_closed() && !handle.is_refusing(),
            _ => false,
        }
    }

    /// Returns a handle for `origin`, dialling one if there is not already a usable one.
    ///
    /// This is the whole of the pool's hot path. The loop exists because a caller can be
    /// woken into a state that is neither an answer nor a wait — a newcomer may have started
    /// a fresh dial in the meantime — and each pass takes the slot lock exactly once,
    /// deciding and transitioning under it so that two callers cannot both become the dialer.
    pub(crate) async fn acquire(
        self: &Arc<Self>,
        origin: &Origin,
    ) -> Result<SendRequest<B>, Error> {
        // Registered before anything else, and released by the guard's `Drop`, so that a
        // shutdown starting now waits for this acquire to finish rather than draining around
        // it. The guard is synchronous precisely because `Drop` cannot await.
        let _acquire = AcquireGuard::register(self)?;

        let slot = self.slot_for(origin)?;
        let mut waited = false;

        loop {
            // Each pass takes the slot lock exactly once and leaves it holding a *decision*
            // rather than a guard, so that nothing below can accidentally await under it.
            // The compiler would reject that anyway — which is the point of the lock being
            // `std::sync::Mutex` — but expressing it this way means the rejection never has
            // to happen.
            let decision = {
                let mut state = slot.state.lock().expect("slot lock poisoned");

                match &*state {
                    Dial::Ready(handle) => {
                        // The eligibility check lives here, under the same lock that stops
                        // two callers replacing one dead connection twice, rather than at the
                        // point of use where they could race.
                        if handle.is_closed() || handle.is_refusing() {
                            // Evicted. Dropping the old handle does not cancel the exchanges
                            // still running on it: its driver holds the connection open until
                            // they finish, which is what FR-012 requires.
                            *state = Dial::Dialing;
                            Decision::Dial
                        } else {
                            Decision::Ready(handle.clone())
                        }
                    }
                    Dial::Failed(error) if waited => {
                        // The rule that makes the fan-out work. This caller waited for the
                        // dial that produced this error, so this error is its answer. It does
                        // not get to try again on its own behalf — that is exactly what turns
                        // one failed dial into N serial ones.
                        Decision::Failed(Arc::clone(error))
                    }
                    Dial::Idle | Dial::Failed(_) => {
                        *state = Dial::Dialing;
                        Decision::Dial
                    }
                    // Captured under the lock. Waiting on the *value* rather than on a change
                    // is what closes the unlock/subscribe window.
                    Dial::Dialing => Decision::Wait(*slot.settled.borrow()),
                }
            };

            match decision {
                Decision::Ready(handle) => return Ok(handle),
                Decision::Failed(error) => return Err(Error::from_shared(&error)),
                Decision::Dial => return self.dial_into(&slot, origin).await,
                Decision::Wait(generation) => {
                    waited = true;
                    let mut settled = slot.settled.subscribe();
                    // `wait_for` checks its predicate against the current value before
                    // awaiting, so a transition that landed while the lock was being released
                    // is seen at once rather than parked behind for ever.
                    if settled.wait_for(|g| *g != generation).await.is_err() {
                        // The sender lives in the slot, which this task holds an `Arc` to, so
                        // this is unreachable in practice. Reported rather than unwrapped: a
                        // panic in a pool would take out every request sharing it.
                        return Err(Error::closed(Reason("the connection slot went away")));
                    }
                }
            }
        }
    }

    /// Looks the slot up, or inserts a fresh one, under the pool lock.
    ///
    /// The pool lock is released before the returned `Arc` is used, which is what keeps the
    /// two locks from ever being held together.
    fn slot_for(&self, origin: &Origin) -> Result<Arc<Slot<B>>, Error> {
        let mut state = self.state.lock().expect("pool lock poisoned");
        if state.closed {
            return Err(Error::closed(Reason("the client has been shut down")));
        }
        Ok(Arc::clone(
            state
                .connections
                .entry(origin.clone())
                .or_insert_with(|| Arc::new(Slot::new())),
        ))
    }

    /// Performs a dial this caller has claimed by moving the slot to `Dialing`.
    ///
    /// The caller *must* have made that transition: this function is responsible for leaving
    /// the slot in a settled state whatever happens, including if its own future is dropped
    /// part-way, which [`DialGuard`] handles.
    async fn dial_into(
        self: &Arc<Self>,
        slot: &Arc<Slot<B>>,
        origin: &Origin,
    ) -> Result<SendRequest<B>, Error> {
        let guard = DialGuard::new(slot);

        #[cfg(test)]
        {
            // Test-only park, after the slot is `Dialing` and before anything is registered.
            let barrier = self
                .dial_barrier
                .lock()
                .expect("barrier lock poisoned")
                .clone();
            if let Some(barrier) = barrier {
                barrier.notified().await;
            }
        }

        self.resolutions.fetch_add(1, Ordering::Relaxed);
        let outcome = dial::<B>(origin, self.config).await;

        match outcome {
            Ok((handle, driver)) => {
                let mut driver_slot = Some(driver);
                // Registered *before* this acquire's count is released, which is the ordering
                // that makes shutdown correct: a drain that started while this dial was in
                // flight waits for the count, and by the time the count drops the driver is
                // already in the vector for it to find.
                let closed = {
                    let mut state = self.state.lock().expect("pool lock poisoned");
                    if !state.closed {
                        // Reap finished drivers here rather than in a sweeper task: this is
                        // the one place already holding the lock for a reason, and a pool
                        // with no traffic needs no sweeping.
                        state.drivers.retain(|driver| !driver.is_finished());
                        state
                            .drivers
                            .push(driver_slot.take().expect("driver taken twice"));
                    }
                    state.closed
                };

                if closed {
                    // Shut down underneath us. Wind the fresh connection down rather than
                    // leaking it, and do not publish it — a caller must not be handed a
                    // connection by a pool that has closed.
                    handle.shutdown();
                    drop(handle);
                    if let Some(driver) = driver_slot {
                        let _ = driver.await;
                    }
                    guard.settle(Dial::Idle);
                    return Err(Error::closed(Reason("the client has been shut down")));
                }

                guard.settle(Dial::Ready(handle.clone()));
                Ok(handle)
            }
            Err(error) => {
                let shared = Arc::new(error);
                guard.settle(Dial::Failed(Arc::clone(&shared)));
                Err(Error::from_shared(&shared))
            }
        }
    }

    /// Winds every connection down and waits for them all to end.
    ///
    /// Ordering matters more than anything else here, and it is: set the flag, wait for
    /// in-flight acquires, take the map, tell each connection to go away, drop the handles,
    /// await the drivers, publish completion.
    ///
    /// Waiting for the acquires *first* is what closes the race. With the flag set no new
    /// acquire can register, so every one in flight resolves and files its driver, and the
    /// drain that follows sees all of them. Draining first would let a dial that was already
    /// in the air land afterwards, leaving a live connection nobody is waiting for and a
    /// completion signal that lied.
    ///
    /// Dropping the handles is what lets each driver finish: `ngnet-h2`'s driver completes
    /// when its handle count reaches zero *and* its stream registry is empty, so a retained
    /// handle would hold the connection open for ever.
    pub(crate) async fn shutdown(self: &Arc<Self>) {
        let leader = {
            let mut state = self.state.lock().expect("pool lock poisoned");
            let first = !state.closed;
            state.closed = true;
            // Both flags move in the one transition, so the lock-free fast path can never
            // report open after the locked one reports closed.
            self.closed.store(true, Ordering::Release);
            first
        };

        if leader {
            // No lock held across this await; the count is mirrored into a `watch` for
            // exactly that reason.
            let mut acquires = self.acquires.subscribe();
            let _ = acquires.wait_for(|count| *count == 0).await;

            let (connections, drivers) = {
                let mut state = self.state.lock().expect("pool lock poisoned");
                (
                    std::mem::take(&mut state.connections),
                    std::mem::take(&mut state.drivers),
                )
            };

            for slot in connections.into_values() {
                // `take` rather than read: the handle must be *dropped*, and dropping it
                // while the slot still held a clone would keep the connection alive.
                let state = std::mem::replace(
                    &mut *slot.state.lock().expect("slot lock poisoned"),
                    Dial::Idle,
                );
                if let Dial::Ready(handle) = state {
                    handle.shutdown();
                    drop(handle);
                }
            }

            for driver in drivers {
                let _ = driver.await;
            }

            self.finished.send_replace(true);
        }

        // Every caller awaits the same completion, the leader included. A second caller
        // returning early would be reporting a drain it did not observe.
        let mut finished = self.finished.subscribe();
        let _ = finished.wait_for(|done| *done).await;
    }
}

/// Counts an acquire in, and — crucially — back out again however the acquire ends.
///
/// The `Drop` is the point. A caller whose future is dropped mid-dial must still release its
/// count, or a later shutdown waits for ever on somebody who has gone. `Drop` cannot await,
/// which is one of the two reasons the pool's locks are synchronous.
struct AcquireGuard<'a, B> {
    pool: &'a Pool<B>,
}

impl<'a, B> AcquireGuard<'a, B> {
    fn register(pool: &'a Pool<B>) -> Result<Self, Error> {
        let mut state = pool.state.lock().expect("pool lock poisoned");
        if state.closed {
            return Err(Error::closed(Reason("the client has been shut down")));
        }
        state.acquires += 1;
        pool.acquires.send_replace(state.acquires);
        Ok(Self { pool })
    }
}

impl<B> Drop for AcquireGuard<'_, B> {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().expect("pool lock poisoned");
        state.acquires -= 1;
        self.pool.acquires.send_replace(state.acquires);
    }
}

/// Guarantees a claimed `Dialing` slot is left settled, even if the dialer is dropped.
///
/// Without this, a caller cancelled mid-dial would leave the slot in `Dialing` for ever and
/// every other caller for that origin would park behind a dial that is not happening.
struct DialGuard<'a, B> {
    slot: &'a Arc<Slot<B>>,
    settled: bool,
}

impl<'a, B> DialGuard<'a, B> {
    fn new(slot: &'a Arc<Slot<B>>) -> Self {
        Self {
            slot,
            settled: false,
        }
    }

    fn settle(mut self, state: Dial<B>) {
        self.settled = true;
        self.slot.settle(state);
    }
}

impl<B> Drop for DialGuard<'_, B> {
    fn drop(&mut self) {
        if !self.settled {
            // Back to `Idle`, not `Failed`: nothing was learned about the origin. The next
            // caller should dial rather than inherit an error that was never observed.
            self.slot.settle(Dial::Idle);
        }
    }
}
