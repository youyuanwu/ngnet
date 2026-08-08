//! Handler futures the driver holds, rather than tasks something else runs.
//!
//! # Why nothing is spawned
//!
//! This layer takes no executor, so there is nowhere to spawn to. Handlers are ordinary
//! futures the driver polls between passes, each with a waker naming its own stream so that
//! only the ones that asked to be woken are polled again.
//!
//! The consequence is worth stating plainly: a handler that *blocks* rather than returning
//! `Pending` stalls its whole connection, because there is no other thread for that
//! connection to be on. A handler with blocking work in it should move that work elsewhere
//! and await the result.
//!
//! # Liveness is gated on the handler, not the stream
//!
//! A handler outlives its stream in the ordinary course of things: the peer resets an
//! exchange, and the future that was answering it is still running. It must still be
//! pollable, or it would be held forever un-woken. So the waker's liveness token belongs to
//! the handler rather than to the registry entry — which is the opposite of the body waker,
//! and the difference is deliberate.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::shared::Shared;
use crate::stream::StreamId;

/// Streams whose handler has been woken.
#[derive(Default)]
pub(crate) struct Ready {
    woken: Mutex<Vec<StreamId>>,
}

impl Ready {
    fn mark(&self, stream: StreamId) {
        let mut woken = self.woken.lock().unwrap_or_else(|e| e.into_inner());
        if !woken.contains(&stream) {
            woken.push(stream);
        }
    }

    fn take(&self, into: &mut Vec<StreamId>) {
        let mut woken = self.woken.lock().unwrap_or_else(|e| e.into_inner());
        into.append(&mut woken);
    }

    fn any(&self) -> bool {
        !self
            .woken
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    fn forget(&self, stream: StreamId) {
        self.woken
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|s| *s != stream);
    }
}

/// Wakes one handler.
struct HandlerWaker {
    stream: StreamId,
    ready: Arc<Ready>,
    shared: Arc<Shared>,
    /// Goes inert when the handler is forgotten — not when its stream is.
    liveness: std::sync::Weak<()>,
}

impl std::task::Wake for HandlerWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.liveness.upgrade().is_none() {
            return;
        }
        self.ready.mark(self.stream);
        self.shared.wake_driver();
    }
}

/// One running handler.
struct Handler<F> {
    future: Pin<Box<F>>,
    waker: Waker,
    /// Held so the handler's waker stays live exactly as long as the handler does.
    _alive: Arc<()>,
}

/// Every handler the driver is holding.
pub(crate) struct Tasks<F> {
    running: BTreeMap<StreamId, Handler<F>>,
    ready: Arc<Ready>,
    shared: Arc<Shared>,
}

impl<F: Future> Tasks<F> {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            running: BTreeMap::new(),
            ready: Arc::new(Ready::default()),
            shared,
        }
    }

    /// How many handlers are running.
    pub(crate) fn len(&self) -> usize {
        self.running.len()
    }

    /// Starts a handler, and marks it ready so it is polled once without being woken.
    pub(crate) fn start(&mut self, stream: StreamId, future: F) {
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

    /// Takes the streams whose handlers have been woken.
    pub(crate) fn take_woken(&self, into: &mut Vec<StreamId>) {
        self.ready.take(into);
    }

    /// Whether any handler is waiting to be polled.
    pub(crate) fn any_woken(&self) -> bool {
        self.ready.any()
    }

    /// Polls one handler with its own waker, returning its output when it finishes.
    pub(crate) fn poll(&mut self, stream: StreamId) -> Option<F::Output> {
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

    /// Drops a handler and anything it had queued.
    pub(crate) fn forget(&mut self, stream: StreamId) {
        self.running.remove(&stream);
        self.ready.forget(stream);
    }

    /// Drops every handler, for teardown.
    pub(crate) fn abandon_all(&mut self) {
        self.running.clear();
    }
}
