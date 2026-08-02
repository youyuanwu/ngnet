//! Handler futures, polled inside the driver.
//!
//! A server has to run many handlers at once on one connection, and this crate spawns
//! nothing and takes no executor. So the handlers live here, in the driver, and the driver
//! polls them between passes of moving octets.
//!
//! # Why each handler gets its own waker
//!
//! Polling every handler whenever anything happened would work and would be quadratic: a
//! connection carrying a hundred streams would poll a hundred futures every time one byte
//! arrived. Each handler is given a waker naming its own stream instead, so a wake marks
//! one stream and the driver polls exactly that one. The set of marked streams is the
//! whole scheduling algorithm.
//!
//! # What this is not
//!
//! It is not an executor. Handlers run on whatever task polls the driver, in the order the
//! driver reaches them, and a handler that *blocks* — rather than returning `Pending` —
//! stalls its connection, because there is no other thread for the connection to be on.
//! That is stated in [`serve`](super::server::serve)'s documentation too, since it is the
//! one thing a caller has to know about this design.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Wake, Waker};

use super::shared::Shared;

/// The streams whose handlers have been woken.
#[derive(Debug, Default)]
pub(crate) struct Ready {
    inner: Mutex<HashSet<i32>>,
}

impl Ready {
    fn mark(&self, stream: i32) {
        self.lock().insert(stream);
    }

    /// Moves the woken streams into `out`, leaving the set empty.
    ///
    /// Takes a caller-owned scratch buffer rather than returning a fresh `Vec`, the same
    /// discipline the body path's [`Shared::take_ready_into`](super::shared::Shared::take_ready_into)
    /// follows: [`HashSet::drain`] keeps the set's capacity and clearing `out` keeps the
    /// vector's, so a steady state waking the same handlers every pass allocates in
    /// neither. That is the property the server allocation harness pins.
    fn take_into(&self, out: &mut Vec<i32>) {
        out.clear();
        out.extend(self.lock().drain());
    }

    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn forget(&self, stream: i32) {
        self.lock().remove(&stream);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<i32>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Wakes one handler.
///
/// Distinct from [`StreamWaker`](super::waker::StreamWaker), which resumes a *body* that
/// deferred inside the session. Both name a stream and both wake the driver; what differs
/// is which of the driver's two ready sets they mark, and conflating them would mean a
/// woken handler asked the session to resume a body that was never deferred.
#[derive(Debug)]
struct HandlerWaker {
    stream: i32,
    ready: Arc<Ready>,
    shared: Arc<Shared>,
    /// Gates the wake on the *handler* still existing — not on the stream.
    ///
    /// A body waker is gated on its stream, because resuming a body for a stream that has
    /// gone is meaningless. A handler is the opposite case: a stream the peer reset is
    /// precisely when its handler most needs to be polled again, so that it can notice.
    /// Gating this on the stream would make a cancelled handler unwakeable, which is the
    /// same as never telling it.
    liveness: Weak<()>,
}

impl Wake for HandlerWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.liveness.upgrade().is_none() {
            return;
        }
        self.ready.mark(self.stream);
        // After the mark, and outside the lock that protects it.
        self.shared.wake_driver();
    }
}

/// The handlers currently running on a connection.
pub(crate) struct Tasks<F> {
    running: BTreeMap<i32, Handler<F>>,
    ready: Arc<Ready>,
    shared: Arc<Shared>,
}

/// One running handler.
struct Handler<F> {
    future: Pin<Box<F>>,
    waker: Waker,
    /// The only strong handle to this handler's liveness, so dropping the handler is what
    /// makes every waker naming it inert.
    _alive: Arc<()>,
}

impl<F> Tasks<F> {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            running: BTreeMap::new(),
            ready: Arc::new(Ready::default()),
            shared,
        }
    }

    /// A handle to the ready set, for the driver's park predicate.
    pub(crate) fn ready(&self) -> Arc<Ready> {
        Arc::clone(&self.ready)
    }

    /// Starts a handler for `stream`, ready to be polled at once.
    ///
    /// Marked ready on arrival rather than waiting for a wake, because a future that has
    /// never been polled has had no chance to register one.
    pub(crate) fn start(&mut self, stream: i32, future: F) {
        let alive = Arc::new(());
        let waker = Waker::from(Arc::new(HandlerWaker {
            stream,
            ready: Arc::clone(&self.ready),
            shared: Arc::clone(&self.shared),
            liveness: Arc::downgrade(&alive),
        }));
        self.running.insert(
            stream,
            Handler {
                future: Box::pin(future),
                waker,
                _alive: alive,
            },
        );
        self.ready.mark(stream);
    }

    /// Moves the streams whose handlers should be polled into `out`, leaving the set
    /// empty. Reuses `out` across passes; see [`Ready::take_into`].
    pub(crate) fn take_woken_into(&self, out: &mut Vec<i32>) {
        self.ready.take_into(out);
    }

    /// How many handlers are currently running.
    ///
    /// Includes handlers retained after their stream was reset — the cap on concurrent
    /// handlers is enforced against this, so a retained handler still occupies a slot.
    pub(crate) fn len(&self) -> usize {
        self.running.len()
    }

    /// Forgets a handler without running it to completion.
    pub(crate) fn abandon_all(&mut self) {
        self.running.clear();
    }

    fn forget(&mut self, stream: i32) {
        self.running.remove(&stream);
        self.ready.forget(stream);
    }
}

impl<F: Future> Tasks<F> {
    /// Polls one handler, handing back what it produced if it finished.
    ///
    /// Polled with its own waker, so whatever it registers wakes only itself.
    pub(crate) fn poll(&mut self, stream: i32) -> Option<F::Output> {
        let handler = self.running.get_mut(&stream)?;
        let mut context = Context::from_waker(&handler.waker);

        match handler.future.as_mut().poll(&mut context) {
            Poll::Pending => None,
            Poll::Ready(output) => {
                self.forget(stream);
                Some(output)
            }
        }
    }
}

impl Ready {
    /// Whether any handler is waiting to be polled.
    pub(crate) fn any(&self) -> bool {
        !self.is_empty()
    }
}
