//! The TCP listener, which is one transport implementation among several.

use std::io;
use std::net::SocketAddr;

use ngnet_h2::http::transport::TokioIo;
use tokio::net::TcpStream;

use super::{FallibleListener, Listener, RetryingListener};

/// Accepts TCP connections for [`serve`](crate::serve).
///
/// Wraps a bound [`tokio::net::TcpListener`]:
///
/// ```no_run
/// use axum::{Router, routing::get};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let router = Router::new().route("/hello", get(|| async { "world" }));
/// let tcp = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
///
/// ngnet_axum::serve(ngnet_axum::TcpListener::new(tcp), router).await;
/// # Ok(())
/// # }
/// ```
///
/// # Why this is a wrapper rather than an impl on `tokio::net::TcpListener`
///
/// axum implements its listener trait directly on the tokio type, and this crate could not.
/// A backoff deadline has to survive the accept future being dropped -- see
/// [`Listener::accept`] for why it is dropped at all -- which means it has to live in the
/// listener. A bare [`tokio::net::TcpListener`] has nowhere to keep one.
///
/// The cost is one visible line at the call site, which is the intended trade: the change is
/// one a reader can see, and it stops TCP from being the case the API is shaped around.
#[derive(Debug)]
pub struct TcpListener(RetryingListener<TcpAcceptor>);

impl TcpListener {
    /// Wraps a bound [`tokio::net::TcpListener`].
    pub const fn new(listener: tokio::net::TcpListener) -> Self {
        Self(RetryingListener::new(TcpAcceptor(listener)))
    }

    /// Returns the local address the underlying socket is bound to.
    ///
    /// Not part of [`Listener`]: nothing in this crate needs a listener's address, an
    /// in-memory transport has no honest answer to give, and a caller who wants one can ask
    /// the concrete listener, as here.
    ///
    /// # Errors
    ///
    /// Whatever the underlying socket reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().0.local_addr()
    }
}

impl Listener for TcpListener {
    type Io = TokioIo<TcpStream>;
    type Addr = SocketAddr;

    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
        self.0.accept()
    }
}

/// Pins that the TCP listener gets its retry policy by holding the shared wrapper.
///
/// There is no behavioural test that can tell the difference between this and an equivalent
/// retry loop written out again here, because no test can make a real socket's `accept` fail
/// on demand. So the guard is structural: this stops compiling if the composition is
/// replaced, which is the mutation worth catching.
const _: fn(TcpListener) -> RetryingListener<TcpAcceptor> = |listener| listener.0;

/// The fallible half: accept once, or say why not.
///
/// Retry, classification and pacing are [`RetryingListener`]'s, which is why they are absent
/// here.
#[derive(Debug)]
struct TcpAcceptor(tokio::net::TcpListener);

impl FallibleListener for TcpAcceptor {
    type Io = TokioIo<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        let (stream, peer) = self.0.accept().await?;

        // Nagle would otherwise hold back the small writes that HTTP/2 control frames are
        // made of, waiting for data that is not coming. This lives here rather than in the
        // accept loop because it is a property of TCP, and the loop no longer knows what a
        // socket is.
        let _ = stream.set_nodelay(true);

        Ok((TokioIo::new(stream), peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SC-004: `TCP_NODELAY` is set on accepted sockets.
    ///
    /// Read back off the socket the listener hands over rather than asserting that a call
    /// was made, because the point of this test is that moving `set_nodelay` out of the
    /// accept loop and into the TCP listener did not quietly lose it. A test double could
    /// not have caught that -- only the shipped acceptor can.
    #[tokio::test]
    async fn accepted_sockets_have_nagle_disabled() {
        let bound = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let mut acceptor = TcpAcceptor(bound);

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (accepted, _peer) = acceptor.accept().await.unwrap();
        let _client = client.await.unwrap();

        assert!(
            accepted.into_inner().nodelay().unwrap(),
            "the TCP listener must disable Nagle on accepted sockets: HTTP/2 control frames \
             are small writes, and Nagle holds them back waiting for data that is not coming"
        );
    }

    /// The wrapper's `local_addr` reports the bound socket's address.
    #[tokio::test]
    async fn local_addr_reports_the_bound_address() {
        let bound = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let expected = bound.local_addr().unwrap();

        let listener = TcpListener::new(bound);

        assert_eq!(listener.local_addr().unwrap(), expected);
    }
}
