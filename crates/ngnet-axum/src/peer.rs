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
/// # Addresses that are not socket addresses
///
/// The address type is a parameter because a [`Listener`](crate::Listener) chooses it, and
/// not every transport names its peers with a [`SocketAddr`]: a Unix-domain listener uses
/// [`tokio::net::unix::SocketAddr`], and an in-memory transport may have no address worth
/// the name at all. The parameter defaults to [`SocketAddr`], so `PeerAddr` written without
/// one still means what it always did.
///
/// The derives are auto-bounded on `A`, so `PeerAddr<SocketAddr>` is still [`Copy`], [`Eq`]
/// and [`Hash`], while a `PeerAddr` over an address that is none of those is still usable.
///
/// [`Extension`]: axum::Extension
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddr<A = SocketAddr>(pub A);

impl<A> PeerAddr<A> {
    /// Returns the wrapped address, consuming the wrapper.
    ///
    /// [`From`] would be the idiomatic spelling, but a blanket `impl<A> From<PeerAddr<A>>
    /// for A` is not coherent: `A` is uncovered in the target position, so the compiler
    /// cannot rule out a downstream crate writing the same impl. The conversion for
    /// [`SocketAddr`] is spelled as a `From` because that one names a concrete target and
    /// is therefore allowed.
    pub fn into_inner(self) -> A {
        self.0
    }
}

impl<A: std::fmt::Display> std::fmt::Display for PeerAddr<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<PeerAddr<SocketAddr>> for SocketAddr {
    fn from(peer: PeerAddr<SocketAddr>) -> Self {
        peer.0
    }
}
