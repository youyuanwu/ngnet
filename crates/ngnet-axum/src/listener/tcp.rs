//! The TCP listener, which is one transport implementation among several.

use std::io;
use std::net::SocketAddr;

use ngnet_h2::http::transport::TokioIo;
use tokio::net::TcpStream;

use super::{Listener, accept_retrying};

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
/// It once had to be, because retry state had to outlive the accept future. It no longer
/// does, and the wrapper stays for two smaller reasons that are nonetheless the real ones.
///
/// It disables Nagle on every accepted socket, and it offers [`local_addr`](Self::local_addr).
/// Both are TCP's business rather than the accept loop's, and neither has anywhere to live on
/// a bare [`tokio::net::TcpListener`].
///
/// And it keeps TCP from being privileged. An impl on the tokio type would make bare TCP the
/// one transport that needs no wrapping at the call site, which is precisely the shape this
/// crate exists not to have: TCP is one implementation of [`Listener`] among several. The
/// cost is one visible line, and a reader can see what it does.
#[derive(Debug)]
pub struct TcpListener(tokio::net::TcpListener);

impl TcpListener {
    /// Wraps a bound [`tokio::net::TcpListener`].
    pub const fn new(listener: tokio::net::TcpListener) -> Self {
        Self(listener)
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
        self.0.local_addr()
    }
}

impl Listener for TcpListener {
    type Io = TokioIo<TcpStream>;
    type Addr = SocketAddr;

    /// Accepts, retrying internally, in the shape the trait documentation recommends.
    ///
    /// An ordinary loop holding an ordinary sleep. Nothing survives outside this future,
    /// because nothing has to: the server drops it at shutdown and not before.
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, peer) = accept_retrying(|| self.0.accept()).await;

        // Nagle would otherwise hold back the small writes that HTTP/2 control frames are
        // made of, waiting for data that is not coming. This lives here rather than in the
        // accept loop because it is a property of TCP, and the loop no longer knows what a
        // socket is.
        let _ = stream.set_nodelay(true);

        (TokioIo::new(stream), peer)
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
    /// not have caught that -- only the shipped listener can.
    #[tokio::test]
    async fn accepted_sockets_have_nagle_disabled() {
        let bound = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let mut listener = TcpListener::new(bound);

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (accepted, _peer) = listener.accept().await;
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
