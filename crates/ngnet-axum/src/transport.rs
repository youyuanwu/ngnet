//! Proving that an accepted transport produces a connection this crate can spawn.
//!
//! This module exists for one reason, and it is worth stating plainly because the shape is
//! otherwise puzzling.
//!
//! `ngnet-h2`'s [`Transport`] traits describe their operations with `-> impl Future` in
//! trait position, and deliberately put no [`Send`] bound on those futures. That is not an
//! oversight: the `completion` transport is thread-per-core and built on types that are not
//! `Send`, so requiring it would have excluded the very runtime the abstraction exists to
//! admit. The cost lands here. Auto traits do not leak out of an opaque return type in
//! generic code, so a function generic over `T: Transport` cannot prove that the connection
//! it builds is `Send` -- and [`JoinSet::spawn`], which is how this crate runs connections
//! concurrently, requires exactly that.
//!
//! So a bound of `Transport` alone is not enough to spawn, and no amount of adding
//! `Reader: Send` style bounds fixes it: the futures themselves are the opaque types, not
//! the transports. The notation that would express it directly,
//! `T: TransportRead<read(..): Send>`, is return-type notation, which is nightly-only.
//!
//! [`ServableTransport`] is the smallest thing that works on stable. It is a trait whose one
//! operation is implemented *at a concrete transport type*, which is the only place the
//! compiler can see through the opacity and check `Send` for itself. The crate ships a
//! blanket implementation covering every tokio byte stream, so in practice nobody writes it.
//!
//! [`JoinSet::spawn`]: tokio::task::JoinSet::spawn

use std::fmt::Debug;
use std::future::Future;

use axum::Router;
use ngnet_h2::http::transport::{TokioIo, Transport};
use ngnet_h2::http::{Config, Connection, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::connection::serve_connection;

/// A [`Transport`] whose connections this crate can spawn onto a task.
///
/// A listener's connection type is bounded by this rather than by `Transport`
/// alone, and rather than by `AsyncRead + AsyncWrite`. The transport bound is the point: a
/// TCP stream is one implementation of a transport here, not a privileged case.
///
/// # You almost certainly do not need to implement this
///
/// The blanket implementation below covers [`TokioIo<S>`] for every `S` that is
/// [`AsyncRead`] + [`AsyncWrite`] + [`Send`] + `'static`. That is every tokio byte stream:
/// TCP, Unix-domain, in-memory duplex pipes, and TLS sessions built over any of them. If
/// your transport is a tokio stream, wrap it in [`TokioIo`] and you are done.
///
/// # Why the operation is not defaulted
///
/// A default body would be type-checked once, generically, against `Self: Transport`. At
/// that point the futures returned by the transport's read and write operations are opaque
/// types with no [`Send`] bound, and the compiler cannot prove the connection built from
/// them is `Send`. Implementing the operation at a concrete transport type is what makes the
/// proof available. [`require_spawnable`] is the whole body an implementation needs.
///
/// # Implementing it anyway
///
/// If you have a transport that is not a tokio byte stream, the implementation is two lines:
///
/// ```ignore
/// impl ServableTransport for MyTransport {
///     fn serve<A>(self, router: Router, peer: A, config: Config)
///         -> Result<Connection<impl Future<Output = Result<()>> + Send + 'static>>
///     where Self: Sized, A: Clone + Debug + Send + Sync + 'static
///     {
///         ngnet_axum::require_spawnable(ngnet_axum::serve_connection(self, router, peer, config))
///     }
/// }
/// ```
///
/// [`TokioIo<S>`]: ngnet_h2::http::transport::TokioIo
pub trait ServableTransport: Transport {
    /// Builds the connection that serves `router` over this transport, proving as it does so
    /// that the connection can be moved to another task.
    ///
    /// The returned [`Connection`] is a future, and its [`drain_handle`] must still be
    /// reachable before it is spawned -- that is how graceful shutdown works -- which is why
    /// this returns the connection rather than an erased boxed future.
    ///
    /// # Errors
    ///
    /// Fails if the HTTP/2 session cannot be created, before any connection exists.
    ///
    /// [`drain_handle`]: ngnet_h2::http::Connection::drain_handle
    fn serve<A>(
        self,
        router: Router,
        peer: A,
        config: Config,
    ) -> Result<Connection<impl Future<Output = Result<()>> + Send + 'static>>
    where
        Self: Sized,
        A: Clone + Debug + Send + Sync + 'static;
}

/// Asserts that a connection can be spawned, and returns it unchanged.
///
/// This is an identity function. Its entire content is its `where` clause: calling it forces
/// the compiler to check, at the concrete transport type where the check is possible, that
/// the connection's future is [`Send`] and `'static`. It is the whole body of a
/// [`ServableTransport`] implementation, and it is public so that an implementation outside
/// this crate is the same two lines the blanket one is.
///
/// # Errors
///
/// Returns its argument's error unchanged; it does not fail on its own account.
pub fn require_spawnable<F>(connection: Result<Connection<F>>) -> Result<Connection<F>>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    connection
}

impl<S> ServableTransport for TokioIo<S>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn serve<A>(
        self,
        router: Router,
        peer: A,
        config: Config,
    ) -> Result<Connection<impl Future<Output = Result<()>> + Send + 'static>>
    where
        Self: Sized,
        A: Clone + Debug + Send + Sync + 'static,
    {
        require_spawnable(serve_connection(self, router, peer, config))
    }
}
