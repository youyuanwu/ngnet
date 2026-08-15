//! Accepting connections, and the retry policy that goes with it.
//!
//! [`Listener`] is the seam that makes this crate transport-generic. It is modelled on
//! `axum::serve::Listener`, deliberately: the point of this crate is that an axum user's
//! knowledge transfers, and that includes the shape of the listener trait.
//!
//! The one place this crate's contract is genuinely more demanding than axum's is retry, and
//! the reason is the shape of this crate's accept loop rather than any difference of taste.
//! [`Listener::accept`] documents it, [`FallibleListener`] is the easier path that avoids
//! it, and [`RetryingListener`] is the thing that actually gets it right.
//!
//! Two implementations ship: [`TcpListener`] and, on Unix, [`UnixListener`]. Both are built
//! by wrapping a [`FallibleListener`], which is the shape third-party listeners should copy.

mod tcp;
#[cfg(unix)]
mod unix;

pub use tcp::TcpListener;
#[cfg(unix)]
pub use unix::UnixListener;

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::time::Instant;

use crate::transport::ServableTransport;

/// How long to wait before accepting again after a failure that will recur.
///
/// A transient accept error is retried at once. A systemic one -- the process being out of
/// file descriptors is the usual case -- is true of the *listener* rather than of one
/// client, so retrying immediately produces an unbounded stream of identical failures and no
/// progress. Backing off turns that into one attempt a second. The value matches
/// `axum::serve`'s.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// A source of connections for [`serve`](crate::serve).
///
/// Implemented for the listeners this crate ships, and implementable for anything else that
/// produces connections: a TLS acceptor, an in-memory pipe, a listener that reads from a
/// queue.
///
/// # Retry is the implementation's job, and it is easy to get wrong here
///
/// [`accept`](Self::accept) returns a connection, not a `Result`. There is no way to report
/// a failed accept to the server, which means an implementation must handle acceptance
/// failure itself: classify it, retry, and pace the retries so a failure that recurs does
/// not become a spin. This matches `axum`'s trait exactly.
///
/// What does *not* match axum is what happens to the future this method returns. axum's
/// accept loop awaits it to completion and nothing ever cancels it. **This crate's loop
/// arbitrates accepting against harvesting finished connections and against the stop signal
/// in a [`tokio::select!`], so whenever another arm wins, the future returned by this method
/// is dropped and rebuilt on the next pass.** On a busy server the harvest arm wins often.
///
/// Two consequences, and both have bitten:
///
/// 1. **A relative sleep will not pace anything.** `sleep(Duration::from_secs(1))` inside
///    this future restarts from zero every time the future is rebuilt. Measured against this
///    loop's shape, one connection completing every 100ms stopped a one-second relative
///    backoff from *ever* elapsing -- 30 accept attempts, 0 completions, over three seconds.
///    The listener was never retried at all while the server stayed busy, which is precisely
///    the wrong moment to stop accepting. State that must survive the drop has to live in
///    the listener, not in the future; an absolute [`Instant`] slept to with
///    [`sleep_until`](tokio::time::sleep_until) is immune, because a rebuilt future
///    recomputes the remaining time to the same instant.
///
/// 2. **Do not await a handshake here.** An implementation that performs a TLS handshake
///    inside this method will have that handshake cancelled part-way through whenever
///    another arm wins, dropping the underlying connection. Return the un-negotiated
///    transport and let the handshake happen on first use instead, so this method holds no
///    cancellable work.
///
/// # The easier path
///
/// Implementing [`FallibleListener`] instead and wrapping it in [`RetryingListener`] gets
/// correct classification, pacing and cooperative yielding for free, and is strictly less
/// code: one method that returns `io::Result` and nothing else. Both listeners this crate
/// ships are built that way, and so is the one its tests use. Prefer it unless you need
/// retry behaviour that differs from this crate's.
pub trait Listener: Send + 'static {
    /// The connection this listener produces.
    ///
    /// Bounded by [`ServableTransport`] rather than by `AsyncRead + AsyncWrite`, which is
    /// what makes a TCP stream one transport implementation here rather than a privileged
    /// case. For any tokio byte stream, [`TokioIo<S>`](crate::TokioIo) is already one.
    ///
    /// It is additionally [`Send`] and `'static` because an accepted connection is moved
    /// onto a task of its own, which is how this crate serves connections concurrently.
    type Io: ServableTransport + Send + 'static;

    /// How this listener names its peers.
    ///
    /// Reaches handlers as [`PeerAddr<Self::Addr>`](crate::PeerAddr) and appears in any
    /// [`Error`](crate::Error) this listener's connections produce. The bounds are what
    /// those two uses require: [`Clone`] and [`Send`] + [`Sync`] + `'static` to be inserted
    /// into a request's extensions, and [`Debug`] to be formatted into an error, since
    /// [`std::error::Error`] requires its implementors to be `Debug` and not every address
    /// type is [`Display`](std::fmt::Display) -- `tokio::net::unix::SocketAddr` is not.
    type Addr: Clone + Debug + Send + Sync + 'static;

    /// Accepts one connection, waiting until there is one.
    ///
    /// This does not return a `Result`. An implementation that cannot accept must retry
    /// internally rather than reporting, and must pace those retries. **Read the trait
    /// documentation before writing this method**: the future it returns is dropped and
    /// rebuilt by the server's `select!` whenever another arm wins, which makes a relative
    /// sleep useless and makes an in-progress handshake unsafe.
    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send;
}

/// A source of connections whose acceptance can fail.
///
/// This is the trait to implement. It is [`Listener`] minus the part that is easy to get
/// wrong: report the failure and stop there. [`RetryingListener`] turns one of these into a
/// [`Listener`], supplying classification, pacing and cooperative yielding, and it is the
/// only implementation of that policy in this crate -- the shipped TCP and Unix listeners
/// hold one rather than repeating it.
///
/// ```no_run
/// use std::io;
/// use ngnet_axum::{FallibleListener, RetryingListener, TokioIo};
/// use tokio::net::{TcpStream, TcpListener as TokioTcp};
///
/// struct MyAcceptor(TokioTcp);
///
/// impl FallibleListener for MyAcceptor {
///     type Io = TokioIo<TcpStream>;
///     type Addr = std::net::SocketAddr;
///
///     async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
///         let (stream, peer) = self.0.accept().await?;
///         Ok((TokioIo::new(stream), peer))
///     }
/// }
///
/// # async fn run(tcp: TokioTcp, router: axum::Router) {
/// ngnet_axum::serve(RetryingListener::new(MyAcceptor(tcp)), router).await;
/// # }
/// ```
pub trait FallibleListener: Send + 'static {
    /// The connection this listener produces. See [`Listener::Io`].
    type Io: ServableTransport + Send + 'static;

    /// How this listener names its peers. See [`Listener::Addr`].
    type Addr: Clone + Debug + Send + Sync + 'static;

    /// Accepts one connection, or reports why it could not.
    ///
    /// Return the error and stop. Do not retry, sleep, or classify: that is
    /// [`RetryingListener`]'s job, and doing it here as well would be the second
    /// implementation of a policy that is difficult enough with one.
    ///
    /// # Errors
    ///
    /// Whatever the underlying source reports. Failures naming one client
    /// (`ConnectionAborted`, `ConnectionRefused`, `ConnectionReset`, `Interrupted`) are
    /// retried immediately; anything else is paced.
    fn accept(&mut self) -> impl Future<Output = io::Result<(Self::Io, Self::Addr)>> + Send;
}

/// Whether an accept failure is about one client rather than about the listener.
///
/// A client that vanishes between the kernel queueing its connection and the loop reaching
/// it produces one of these, and the next accept will succeed. Anything else -- `EMFILE`
/// being the case that matters, which reaches Rust as
/// [`Uncategorized`](io::ErrorKind) -- is a property of the process or the
/// listener and will recur immediately, so it is paced instead.
fn is_transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

/// Adds retry, classification and pacing to a [`FallibleListener`].
///
/// This is the only place in the crate that implements accept-retry policy. Both shipped
/// listeners hold one; the tests drive one; a third-party listener should use one rather
/// than reimplement it.
///
/// # Why the deadline is a field
///
/// The server drops and rebuilds the accept future whenever another arm of its `select!`
/// wins, so anything held *inside* that future is lost on every harvest. The backoff
/// deadline is therefore stored here, in the listener, where it survives; and it is stored
/// as an absolute [`Instant`] rather than a remaining duration, so a rebuilt future
/// inherits the progress already made instead of starting over.
///
/// This is the whole difficulty of the trait contract, solved once.
#[derive(Debug)]
pub struct RetryingListener<A> {
    inner: A,
    /// The instant before which the next accept must not be attempted, if one is owed.
    backoff: Option<Instant>,
}

impl<A> RetryingListener<A> {
    /// Wraps a [`FallibleListener`], giving it this crate's retry policy.
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            backoff: None,
        }
    }

    /// Returns the wrapped listener.
    pub fn into_inner(self) -> A {
        self.inner
    }

    /// Borrows the wrapped listener, for a shipped listener to expose its own accessors.
    pub const fn get_ref(&self) -> &A {
        &self.inner
    }
}

impl<A: FallibleListener> Listener for RetryingListener<A> {
    type Io = A::Io;
    type Addr = A::Addr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // Read the deadline; do not `take` it. Clearing it before the sleep completes
            // would lose it whenever this future is dropped mid-sleep -- which, on a busy
            // server, is most of the time -- and the next attempt would come immediately
            // instead of a second later. That degenerates to a spin: measured at 34 attempts
            // in 3.4 seconds against the 3 the deadline is meant to allow.
            if let Some(deadline) = self.backoff {
                tokio::time::sleep_until(deadline).await;
                self.backoff = None;
            }

            match self.inner.accept().await {
                Ok(accepted) => {
                    self.backoff = None;
                    return accepted;
                }

                // Retried at once, but not *quite* at once: without a yield an acceptor that
                // fails transiently every time would loop here without ever returning
                // `Pending`, monopolising the poll and starving the arms that carry the stop
                // signal and the harvest. The existing code avoided this only because every
                // error left the accept future; retrying internally is what makes it live.
                Err(error) if is_transient(&error) => tokio::task::yield_now().await,

                Err(_) => self.backoff = Some(Instant::now() + ACCEPT_BACKOFF),
            }
        }
    }
}

#[cfg(test)]
mod tests;
