//! The value that *is* a connection.
//!
//! A driver is handed back rather than started, because where it runs is the caller's
//! business and this crate spawns nothing. The consequence is a trap worth guarding: a
//! caller who takes the handle and forgets the driver has a connection that compiles,
//! type-checks, and never sends a byte. Requests simply queue and never resolve.
//!
//! Naming the type is what lets the compiler say so. `impl Future` cannot carry
//! `#[must_use]`; this can.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use super::error::Result;
use super::shared::Shared;

/// A connection's driver: poll it, and the connection runs.
///
/// Until it is polled nothing moves — requests submitted to a handle are queued and never
/// sent, and a server accepts nothing. Spawn it, join it, or poll it alongside whatever
/// else the caller has; this crate takes no executor, spawner or timer.
///
/// Resolves when the peer goes away or when there is nothing left to do. Dropping it fails
/// every exchange it was carrying, so nothing is ever left waiting on a connection that no
/// longer exists.
/// Discarding it is a mistake the compiler catches:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # use ngnet_h2::http::testing::{Duplex, Empty, duplex};
/// # use ngnet_h2::http::testing::Vectored;
/// # fn example() -> Result<(), ngnet_h2::http::Error> {
/// let (transport, _peer) = duplex();
/// // The handle is kept and the driver thrown away, so nothing will ever be sent.
/// ngnet_h2::http::handshake::<Duplex<Vectored>, Empty>(transport)?;
/// # Ok(())
/// # }
/// ```
///
/// Keeping it is not:
///
/// ```
/// # use ngnet_h2::http::testing::{Duplex, Empty, duplex};
/// # use ngnet_h2::http::testing::Vectored;
/// # fn example() -> Result<(), ngnet_h2::http::Error> {
/// let (transport, _peer) = duplex();
/// let (requests, connection) = ngnet_h2::http::handshake::<Duplex<Vectored>, Empty>(transport)?;
/// # let _ = (requests, connection);
/// # Ok(())
/// # }
/// ```
#[must_use = "a connection does nothing until its driver is polled: requests submitted to \
              its handle will queue and never be sent"]
pub struct Connection<F> {
    // Boxed so the projection below needs no `unsafe`, which this subtree does not have.
    // One allocation for the life of a connection, not one per operation — the bound the
    // design actually cares about is untouched.
    inner: Pin<Box<F>>,
    shared: Arc<Shared>,
}

impl<F> Connection<F> {
    pub(crate) fn new(future: F, shared: Arc<Shared>) -> Self {
        Self {
            inner: Box::pin(future),
            shared,
        }
    }

    /// A handle that can wind this connection down from another task.
    ///
    /// Taken from the connection rather than returned alongside it because the connection
    /// is the value a caller already has, and because a drain is not something most callers
    /// want: adding it to the four server entry points and the client handshake would make
    /// every caller pay attention to a capability few use.
    ///
    /// Cheap, and may be taken more than once.
    #[must_use]
    pub fn drain_handle(&self) -> Drain {
        Drain {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// Winds a connection down without waiting for it.
///
/// Cloneable and usable from any task: asking only sets a flag and wakes the driver, so it
/// never blocks and never needs the session.
///
/// # What a drain does
///
/// It sends the peer a `GOAWAY` naming the last stream this end processed, which is the
/// whole of the contract: every stream up to and including that one is answered normally,
/// and the peer is told it may retry anything above it elsewhere. Exchanges already in
/// flight are *not* cancelled, and their handlers are not dropped. New ones are refused.
///
/// # What it does not do
///
/// It sets no deadline. A drain waits for the requests in flight, and a request that never
/// finishes will keep the connection alive indefinitely — a handler that awaits forever is
/// indistinguishable, from here, from one that is merely slow, and this crate owns no
/// clock to tell them apart with. Bounding it is the caller's job, who has both a timer and
/// the knowledge of what its own handlers are supposed to do. Dropping the driver remains
/// the blunt instrument, and still fails everything it was carrying.
///
/// # The two roles
///
/// On a **server** this is the only way to end a connection from this side, and the
/// connection's future resolves once the last stream finishes.
///
/// On a **client** it does what [`SendRequest::shutdown`](super::SendRequest::shutdown)
/// does — the `GOAWAY` names zero either way, since a client that accepts no pushed streams
/// has processed nothing — and it does *not* by itself end the connection, because a client
/// driver finishes when its handles are dropped. That difference is a property of the two
/// roles rather than of this handle: a server has no handles to drop, which is why it
/// needed a way to be asked.
#[derive(Clone)]
pub struct Drain {
    shared: Arc<Shared>,
}

impl Drain {
    /// Asks the connection to wind down, and returns immediately.
    ///
    /// Idempotent: asking twice is asking once, and asking after the connection has already
    /// gone does nothing at all.
    pub fn drain(&self) {
        self.shared.request_drain(crate::ErrorCode::NO_ERROR);
        // Without this a connection sitting between requests would never learn it had been
        // asked. The driver parks on a predicate it only re-evaluates when something wakes
        // it, and an idle connection has nothing else coming — so the flag would be set,
        // correctly, and read by nobody until the peer happened to send something.
        self.shared.wake_driver();
    }
}

impl core::fmt::Debug for Drain {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Drain").finish_non_exhaustive()
    }
}

impl<F: Future<Output = Result<()>>> Future for Connection<F> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

impl<F> core::fmt::Debug for Connection<F> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}
