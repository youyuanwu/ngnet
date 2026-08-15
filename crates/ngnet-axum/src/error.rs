//! What a running server reports to the caller when something fails.
//!
//! A server that has started serving has nobody left to return a `Result` to: the accept
//! loop outlives every individual failure and is expected to keep going. So failures are
//! *reported* rather than returned, and the thing they are reported through needs to answer
//! two questions a caller acts on differently. Did the listener fail, or did one connection
//! fail? And if a connection failed, which peer was it?
//!
//! Both are carried here rather than flattened into a message, for the same reason
//! `ngnet-h2`'s own [`Error`](ngnet_h2::http::Error) carries a kind: a caller that has to
//! string-match to find out which peer misbehaved has been given a log line, not an error.
//!
//! # Why the address is a type parameter rather than erased
//!
//! Now that a [`Listener`](crate::Listener) chooses its own address type, this error has to
//! either carry that type or throw it away and keep a string. Carrying it means the
//! parameter propagates into [`Serve::on_error`](crate::Serve::on_error)'s callback, which
//! is a real cost in signature noise.
//!
//! It is carried anyway, because the alternative destroys the only thing the address is
//! for. A caller logging the peer would be equally well served by a string; a caller
//! *acting* on it -- shedding a client, feeding a rate limiter, correlating with a
//! connection table -- needs the address back as an address, and cannot parse one out of a
//! Unix socket's `(unnamed)`. Erasure would leave that caller with nothing, and it is the
//! caller with a reason to match on this type at all.
//!
//! The parameter defaults to [`SocketAddr`], so existing mentions of `Error` still mean what
//! they did.

use std::error::Error as StdError;
#[cfg(test)]
use std::io;
use std::fmt;
use std::net::SocketAddr;

use crate::peer::PeerAddr;

/// The category of a server failure.
///
/// Marked non-exhaustive: new categories are additive, and matching on this must always
/// carry a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Accepting a connection failed. No peer is known, because none was obtained.
    ///
    /// The server does not stop for this. Most causes are transient — a client that
    /// vanished between the kernel queueing it and the loop reaching it — but a process out
    /// of file descriptors reports it on every attempt, so the loop throttles rather than
    /// spinning.
    Accept,
    /// One connection failed. Every stream on it failed with it; the server carries on.
    ///
    /// This is also how a handler panic arrives: panics unwind out of the connection
    /// future, so isolation is per connection rather than per request.
    Connection,
}

impl ErrorKind {
    const fn describe(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Connection => "connection",
        }
    }
}

/// A failure reported by a running server.
///
/// `A` is the peer address type the [`Listener`](crate::Listener) produces, defaulting to
/// [`SocketAddr`].
#[derive(Debug)]
pub struct Error<A = SocketAddr> {
    kind: ErrorKind,
    peer: Option<PeerAddr<A>>,
    source: Box<dyn StdError + Send + Sync>,
}

impl<A> Error<A> {
    /// A failure accepting a connection. No peer exists to name.
    pub(crate) fn accept(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self {
            kind: ErrorKind::Accept,
            peer: None,
            source: source.into(),
        }
    }

    /// A failure on an established connection, naming the peer it was with.
    pub(crate) fn connection(
        peer: A,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self {
            kind: ErrorKind::Connection,
            peer: Some(PeerAddr(peer)),
            source: source.into(),
        }
    }

    /// Which kind of failure this is.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The peer this failure was with, for failures that had one.
    ///
    /// Borrowed rather than returned by value, because a listener's address type need not be
    /// [`Copy`] — a Unix-domain address is not.
    pub const fn peer(&self) -> Option<&PeerAddr<A>> {
        self.peer.as_ref()
    }
}

impl<A: Copy> Error<A> {
    /// The peer this failure was with, by value, when the address type is [`Copy`].
    ///
    /// Convenience for the common [`SocketAddr`] case, where borrowing is pure ceremony.
    pub const fn peer_addr(&self) -> Option<PeerAddr<A>> {
        self.peer
    }
}

impl<A: fmt::Debug> fmt::Display for Error<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Formatted with `Debug` rather than `Display`: a Unix-domain address does not
        // implement `Display` at all, so requiring it here would bar the listener this
        // crate ships from ever reporting an error.
        match &self.peer {
            Some(PeerAddr(peer)) => {
                write!(formatter, "{} error with {peer:?}", self.kind.describe())
            }
            None => write!(formatter, "{} error", self.kind.describe()),
        }
    }
}

impl<A: fmt::Debug> StdError for Error<A> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An address that is neither [`Copy`] nor [`Display`](fmt::Display), like a
    /// Unix-domain one.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AwkwardAddr(String);

    /// SC-014: an address that is not `Copy` and not `Display` still flows through `Error`.
    ///
    /// This is the case the generic parameter exists for. If `Display` were required of the
    /// address, or the accessor returned by value, the Unix-domain listener this crate ships
    /// could not report an error at all -- so the double here is deliberately as awkward as
    /// the real thing.
    #[test]
    fn an_address_that_is_neither_copy_nor_display_survives_the_round_trip() {
        let error = Error::connection(
            AwkwardAddr("/tmp/ngnet.sock".to_owned()),
            io::Error::other("connection reset"),
        );

        assert_eq!(
            error.peer().map(|PeerAddr(addr)| addr.clone()),
            Some(AwkwardAddr("/tmp/ngnet.sock".to_owned())),
            "the peer address must be recoverable as an address, not just as a message: a \
             caller shedding a misbehaving client cannot parse one back out of a string"
        );
        assert!(
            error.to_string().contains("/tmp/ngnet.sock"),
            "Display must fall back to Debug so addresses without Display still render, \
             got {error}"
        );
    }

    /// The accept variant still names no peer.
    #[test]
    fn an_accept_failure_names_no_peer() {
        let error = Error::<SocketAddr>::accept(io::Error::other("out of descriptors"));

        assert!(error.peer().is_none());
        assert_eq!(error.to_string(), "accept error");
    }
}
