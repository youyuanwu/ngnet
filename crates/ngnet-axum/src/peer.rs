//! The peer address of the connection a request arrived on.

use std::net::SocketAddr;

/// The address of the peer that opened the connection a request arrived on.
///
/// Inserted into every request's extensions, so a handler reads it with axum's
/// [`Extension`] extractor:
///
/// ```no_run
/// use axum::Extension;
/// use ngnet_axum::PeerAddr;
///
/// async fn who(Extension(PeerAddr(peer)): Extension<PeerAddr>) -> String {
///     format!("hello {peer}")
/// }
/// ```
///
/// This is where axum users would reach for `ConnectInfo`. That extractor is unavailable
/// here: it is gated behind axum's `tokio` feature, which pulls in `hyper-util` and with it
/// the engine this crate replaces. The fuller argument, and what is lost by the
/// substitution, is in the crate documentation.
///
/// [`Extension`]: axum::Extension
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddr(pub SocketAddr);

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<PeerAddr> for SocketAddr {
    fn from(peer: PeerAddr) -> Self {
        peer.0
    }
}
