//! The Unix-domain-socket listener, which is the second shipped transport.

use std::io;

use ngnet_h2::http::transport::TokioIo;
use tokio::net::UnixStream;
use tokio::net::unix::SocketAddr;

use super::{FallibleListener, Listener, RetryingListener};

/// Accepts Unix-domain-socket connections for [`serve`](crate::serve).
///
/// Wraps a bound [`tokio::net::UnixListener`]:
///
/// ```no_run
/// use axum::{Router, routing::get};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let router = Router::new().route("/hello", get(|| async { "world" }));
/// let unix = tokio::net::UnixListener::bind("/tmp/ngnet.sock")?;
///
/// ngnet_axum::serve(ngnet_axum::UnixListener::new(unix), router).await;
/// # Ok(())
/// # }
/// ```
///
/// # The peer address is usually unnamed
///
/// A client that has not itself bound a path -- which is the normal case -- has no address,
/// and [`tokio::net::unix::SocketAddr`] represents that as unnamed rather than as an error.
/// So handlers reading [`PeerAddr`](crate::PeerAddr) here will typically find
/// `(unnamed)` where a TCP server would have given them something to log or rate-limit on.
/// That is a property of the transport, not a gap in this crate, and it is the reason the
/// peer address had to become generic instead of being widened to some union type: there is
/// no useful `SocketAddr` to manufacture.
///
/// Unlike a TCP port, the socket file outlives the process. Binding fails with
/// `AddrInUse` against a leftover file even when nothing is listening on it.
#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(unix)))]
pub struct UnixListener(RetryingListener<UnixAcceptor>);

impl UnixListener {
    /// Wraps a bound [`tokio::net::UnixListener`].
    pub const fn new(listener: tokio::net::UnixListener) -> Self {
        Self(RetryingListener::new(UnixAcceptor(listener)))
    }

    /// Returns the local address the underlying socket is bound to.
    ///
    /// See [`TcpListener::local_addr`](crate::TcpListener::local_addr) for why this is an
    /// inherent method rather than part of [`Listener`].
    ///
    /// # Errors
    ///
    /// Whatever the underlying socket reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().0.local_addr()
    }
}

impl Listener for UnixListener {
    type Io = TokioIo<UnixStream>;
    type Addr = SocketAddr;

    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
        self.0.accept()
    }
}

/// Pins the composition, as in the TCP listener. See the note there.
const _: fn(UnixListener) -> RetryingListener<UnixAcceptor> = |listener| listener.0;

/// The fallible half: accept once, or say why not.
///
/// There is no `set_nodelay` analogue -- Nagle is a TCP algorithm, and this is the point of
/// having moved that call out of the accept loop.
#[derive(Debug)]
struct UnixAcceptor(tokio::net::UnixListener);

impl FallibleListener for UnixAcceptor {
    type Io = TokioIo<UnixStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        let (stream, peer) = self.0.accept().await?;
        Ok((TokioIo::new(stream), peer))
    }
}
