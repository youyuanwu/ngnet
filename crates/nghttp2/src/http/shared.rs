//! State that handles, bodies and the driver all reach.
//!
//! The session lives inside the driver and is deliberately not `Sync`, so nothing outside
//! the driver may touch it. Everything that wants the session's attention — a handle
//! submitting a request, a body announcing it has more to give — leaves a note here and
//! wakes the driver, which acts on it during its next pass.
//!
//! # Lock discipline
//!
//! Every lock in this module is a *leaf*: it is taken, a small amount of state is moved in
//! or out, and it is released before anything else is called. The driver never holds one
//! across a call into the session.
//!
//! That rule is load-bearing rather than tidy. A body may wake its own waker from inside
//! [`BodySource::fill`](crate::BodySource::fill), which runs re-entrantly inside
//! `Session::send` — so waking must never need a lock the driver is already holding.
//! Waking the driver itself is done *after* releasing the lock, for the same reason.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::task::Waker;

use super::error::Error;

/// The de-duplicating ready set, the driver's waker, and the connection's liveness.
///
/// Carries no type parameter, because [`super::waker::StreamWaker`] holds one and
/// [`std::task::Wake`] requires `Send + Sync`. Keeping the body type out of here means a
/// non-`Send` body makes the *handle* non-`Send` by inference without making the waker
/// impossible to write.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Streams whose bodies have announced they are ready to be asked again.
    ///
    /// A set rather than a queue: a stream woken five times before the driver next runs
    /// still needs resuming exactly once.
    ready: BTreeSet<i32>,
    /// The driver task's waker, replaced whenever the runtime hands over a different one.
    driver: Option<Waker>,
    /// Set once the driver is gone, so nothing waits for an answer that cannot come.
    gone: bool,
}

impl Shared {
    /// Records the waker the driver is currently being polled with.
    ///
    /// Called at the top of every driver poll. A waker captured once at submission would
    /// go stale the first time the runtime moved or re-polled the driver, so the slot is
    /// refreshed rather than filled — and [`Waker::will_wake`] keeps the common case, the
    /// same waker every time, from doing any work.
    pub(crate) fn refresh_driver(&self, waker: &Waker) {
        let mut inner = self.lock();
        match &inner.driver {
            Some(current) if current.will_wake(waker) => {}
            _ => inner.driver = Some(waker.clone()),
        }
    }

    /// Asks the driver to run another pass.
    pub(crate) fn wake_driver(&self) {
        // Taken out under the lock and woken outside it: a waker may do arbitrary work,
        // including re-entering this type, and must not do so while the lock is held.
        let waker = self.lock().driver.clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Notes that `stream`'s body is ready to be consulted again.
    ///
    /// Returns whether the note was kept. A wake whose stream is no longer live is
    /// discarded here, which is what bounds the set by the number of live streams rather
    /// than by the number of wakers that were ever handed out.
    pub(crate) fn mark_ready(&self, stream: i32, liveness: &Weak<()>) -> bool {
        if liveness.upgrade().is_none() {
            return false;
        }
        if stream <= 0 {
            // The stream identifier is assigned after submission; a waker fired before
            // then has nothing to name.
            return false;
        }
        self.lock().ready.insert(stream);
        true
    }

    /// Takes the streams to resume, leaving the set empty.
    pub(crate) fn take_ready(&self) -> Vec<i32> {
        let mut inner = self.lock();
        core::mem::take(&mut inner.ready).into_iter().collect()
    }

    /// How many streams are waiting to be resumed.
    pub(crate) fn ready_len(&self) -> usize {
        self.lock().ready.len()
    }

    /// Records that the driver has stopped, and wakes anything waiting on it.
    pub(crate) fn set_gone(&self) {
        self.lock().gone = true;
    }

    /// Whether the driver has stopped.
    pub(crate) fn is_gone(&self) -> bool {
        self.lock().gone
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock here would mean a panic escaped one of these very short
        // critical sections, none of which call out into caller code. Recovering the
        // guard is more useful than turning an unrelated panic into a second one.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Proof that at least one handle still exists.
///
/// The driver holds a [`Weak`] to this. When the strong count reaches zero no further
/// request can ever arrive, which is what lets an idle connection finish instead of
/// waiting forever — and dropping the last one wakes the driver so it notices.
#[derive(Debug)]
pub(crate) struct HandleToken {
    shared: Arc<Shared>,
}

impl HandleToken {
    pub(crate) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl Drop for HandleToken {
    fn drop(&mut self) {
        self.shared.wake_driver();
    }
}

/// Something a handle wants the driver to do with the session.
pub(crate) enum Command<B> {
    /// Submit this request and answer through the attached slot.
    SendRequest {
        request: http::Request<B>,
        slot: Arc<Slot>,
    },
}

/// The queue commands travel on.
///
/// Separate from [`Shared`] because it is the only part that names the body type. A
/// non-`Send` body therefore infects the handle, as it should, without reaching the waker.
pub(crate) struct Queue<B> {
    commands: Mutex<VecDeque<Command<B>>>,
}

impl<B> Default for Queue<B> {
    fn default() -> Self {
        Self {
            commands: Mutex::new(VecDeque::new()),
        }
    }
}

impl<B> Queue<B> {
    pub(crate) fn push(&self, command: Command<B>) {
        self.lock().push_back(command);
    }

    pub(crate) fn drain(&self) -> Vec<Command<B>> {
        self.lock().drain(..).collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Command<B>>> {
        self.commands
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Where one exchange's answer is delivered.
///
/// The driver fills it; the response future reads it. Both ends are reachable from
/// different tasks, so it carries its own lock rather than relying on the driver's.
#[derive(Debug, Default)]
pub(crate) struct Slot {
    state: Mutex<SlotState>,
}

#[derive(Debug, Default)]
struct SlotState {
    head: Option<http::Response<()>>,
    error: Option<Error>,
    waker: Option<Waker>,
    settled: bool,
}

impl Slot {
    /// Delivers a response head.
    pub(crate) fn complete(&self, head: http::Response<()>) {
        let waker = {
            let mut state = self.lock();
            if state.settled {
                return;
            }
            state.settled = true;
            state.head = Some(head);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Delivers a failure, unless an answer has already been delivered.
    ///
    /// Idempotent on purpose: a stream that closed with an error after its head arrived
    /// has already been answered, and teardown must not overwrite that.
    pub(crate) fn fail(&self, error: Error) {
        let waker = {
            let mut state = self.lock();
            if state.settled {
                return;
            }
            state.settled = true;
            state.error = Some(error);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether an answer has been delivered.
    pub(crate) fn is_settled(&self) -> bool {
        self.lock().settled
    }

    pub(crate) fn poll(
        &self,
        waker: &Waker,
    ) -> core::task::Poll<super::error::Result<http::Response<()>>> {
        let mut state = self.lock();
        if let Some(head) = state.head.take() {
            return core::task::Poll::Ready(Ok(head));
        }
        if let Some(error) = state.error.take() {
            return core::task::Poll::Ready(Err(error));
        }
        match &state.waker {
            Some(current) if current.will_wake(waker) => {}
            _ => state.waker = Some(waker.clone()),
        }
        core::task::Poll::Pending
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// What the driver knows about one live stream.
#[derive(Debug)]
pub(crate) struct Entry {
    /// Where this exchange's answer goes.
    pub(crate) slot: Arc<Slot>,
    /// Proof that the stream is still live.
    ///
    /// Held here and nowhere else, so removing the entry is what makes every waker that
    /// ever named this stream inert. Never read — its existence is the whole signal.
    _liveness: Arc<()>,
}

/// The streams currently in flight.
///
/// Shared with [`super::driver::DriverGuard`] rather than kept as a driver local, because
/// a driver future that is dropped — including one that was never polled — must still be
/// able to tell everyone waiting on it that no answer is coming.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    streams: Mutex<std::collections::BTreeMap<i32, Entry>>,
}

impl Registry {
    pub(crate) fn insert(&self, stream: i32, slot: Arc<Slot>, liveness: Arc<()>) {
        self.lock().insert(
            stream,
            Entry {
                slot,
                _liveness: liveness,
            },
        );
    }

    /// The slot for a live stream.
    pub(crate) fn slot(&self, stream: i32) -> Option<Arc<Slot>> {
        self.lock()
            .get(&stream)
            .map(|entry| Arc::clone(&entry.slot))
    }

    /// Forgets a stream, retiring every waker that named it.
    pub(crate) fn remove(&self, stream: i32) -> Option<Entry> {
        self.lock().remove(&stream)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Empties the registry, handing back everything that was in it.
    pub(crate) fn take_all(&self) -> Vec<Entry> {
        core::mem::take(&mut *self.lock()).into_values().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<i32, Entry>> {
        self.streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
