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

use super::error::Result;

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
/// # use ngnet_h2::http::transport::Coalesced;
/// # fn example() -> Result<(), ngnet_h2::http::Error> {
/// let (transport, _peer) = duplex();
/// // The handle is kept and the driver thrown away, so nothing will ever be sent.
/// ngnet_h2::http::handshake::<Duplex<Coalesced>, Empty>(transport)?;
/// # Ok(())
/// # }
/// ```
///
/// Keeping it is not:
///
/// ```
/// # use ngnet_h2::http::testing::{Duplex, Empty, duplex};
/// # use ngnet_h2::http::transport::Coalesced;
/// # fn example() -> Result<(), ngnet_h2::http::Error> {
/// let (transport, _peer) = duplex();
/// let (requests, connection) = ngnet_h2::http::handshake::<Duplex<Coalesced>, Empty>(transport)?;
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
}

impl<F> Connection<F> {
    pub(crate) fn new(future: F) -> Self {
        Self {
            inner: Box::pin(future),
        }
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
