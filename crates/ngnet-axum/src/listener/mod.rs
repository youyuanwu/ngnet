//! Accepting connections, and the retry policy that goes with it.
//!
//! [`Listener`] is the seam that makes this crate transport-generic. It is modelled on
//! `axum::serve::Listener`, deliberately: the point of this crate is that an axum user's
//! knowledge transfers, and that includes the shape of the listener trait.
//!
//! The contract is now axum's, with nothing added. It used to be more demanding: this
//! crate's accept loop had a third `select!` arm that harvested finished connections, so the
//! accept future was dropped and rebuilt constantly, and an implementor had to keep any
//! retry state outside it. Two extra public traits existed to make that survivable. The loop
//! has two arms now, the hazard is gone, and so are they.
//!
//! Two implementations ship: [`TcpListener`] and, on Unix, [`UnixListener`]. Both implement
//! [`Listener`] directly, retrying in an ordinary loop with an ordinary sleep, which is the
//! shape third-party listeners should copy.

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
/// # Retry is the implementation's job
///
/// [`accept`](Self::accept) returns a connection, not a `Result`. There is no way to report
/// a failed accept to the server, which means an implementation must handle acceptance
/// failure itself: classify it, retry, and pace the retries so a failure that recurs does
/// not become a spin. This matches `axum`'s trait exactly, and so does what you may write to
/// do it -- an ordinary loop, and an ordinary [`sleep`](tokio::time::sleep) held across an
/// await inside this method, both work.
///
/// That is worth stating because it was not always so. This crate's accept loop used to
/// arbitrate accepting against *harvesting finished connections*, so the future returned by
/// this method was dropped and rebuilt every time any connection ended. A relative sleep
/// inside it never elapsed. The loop now has two arms, as `axum::serve`'s does, and the
/// hazard is gone.
///
/// # What is still true about the future being dropped
///
/// Once. At shutdown. The server's `select!` also carries the stop signal, and when that
/// fires the loop breaks -- dropping whatever accept future was in flight. It is dropped at
/// most once in a server's life, and never again while the server is accepting.
///
/// That is enough to matter for one kind of implementation. **Work in progress inside this
/// method is lost if the server is shut down while it is in progress**, and a half-completed
/// TLS handshake is the case to think about: the connection it was negotiating goes with it,
/// and the peer sees the negotiation abandoned rather than refused. If that distinction
/// matters to your transport, return the un-negotiated transport and let the handshake
/// happen on first use, so this method holds no cancellable work. If it does not -- and for
/// most listeners it does not, since a connection dropped at shutdown was going to be
/// drained moments later anyway -- a handshake here is fine.
///
/// What is *not* still true: retry state does not need to live outside this future. Both
/// listeners this crate ships keep theirs in an ordinary loop.
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
    /// internally rather than reporting, and must pace those retries -- a failure such as
    /// `EMFILE` is about the listener rather than about one client and will recur at once,
    /// so retrying without pacing is a spin.
    ///
    /// A plain loop with a plain [`sleep`](tokio::time::sleep) is the right shape. The
    /// future returned here is dropped at most once per server, at shutdown; see the trait
    /// documentation for the one case where that is worth thinking about.
    ///
    /// It must also yield to the runtime rather than spin: a retry loop that never returns
    /// [`Pending`](std::task::Poll::Pending) never lets the server see its own stop signal.
    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send;
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

/// Accepts from `source`, retrying until it succeeds.
///
/// The retry loop the [`Listener`] contract asks every implementation to write, written once
/// because both shipped listeners want the same one. It is deliberately *not* public: the
/// contract is that an implementor retries, not that they retry like this, and a public
/// wrapper here is exactly the thing this crate used to have and no longer needs.
///
/// `source` is a closure rather than a `&mut self` method so that both a `TcpListener` and a
/// `UnixListener` -- which share no trait -- can pass their own accept, and so that a test can
/// pass one that fails on demand. No real socket can be made to fail on demand, which is why
/// this is the only place the retry loop is testable at all.
async fn accept_retrying<T, F>(mut source: impl FnMut() -> F) -> T
where
    F: Future<Output = io::Result<T>>,
{
    loop {
        match source().await {
            Ok(accepted) => return accepted,
            Err(error) => pace_after(&error).await,
        }
    }
}

/// Reacts to an accept failure, and returns when it is worth trying again.
///
/// The whole of this crate's accept-retry policy, in one place so that the two shipped
/// listeners share it rather than each carrying a copy. A third-party listener is welcome to
/// a different policy; this is not a contract, it is what these two do.
///
/// Transient failures are retried at once -- but not *quite* at once. Without the yield, an
/// acceptor failing transiently every time would loop inside its own `accept` without ever
/// returning [`Pending`](std::task::Poll::Pending), monopolising the poll. That matters more
/// than it looks: the only other arm of the server's loop is the stop signal, so a listener
/// that never returns from `poll` is a server that cannot be shut down. `axum` does not
/// yield here; this crate does, deliberately.
///
/// Systemic failures are paced with a plain relative sleep. Plain, because the future this
/// runs inside is no longer dropped and rebuilt while the server is accepting -- which is
/// exactly the change that let the absolute-deadline machinery this replaced be deleted.
async fn pace_after(error: &io::Error) {
    if is_transient(error) {
        tokio::task::yield_now().await;
    } else {
        tokio::time::sleep(ACCEPT_BACKOFF).await;
    }
}

#[cfg(test)]
mod tests;
