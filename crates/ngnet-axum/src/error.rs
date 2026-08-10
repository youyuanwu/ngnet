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
//! string-match to find out whether its listener is dead has been given a log line, not an
//! error.

use std::error::Error as StdError;
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
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    peer: Option<PeerAddr>,
    source: Box<dyn StdError + Send + Sync>,
}

impl Error {
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
        peer: SocketAddr,
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
    /// Always present for [`ErrorKind::Connection`] and never for [`ErrorKind::Accept`],
    /// which is the distinction [`kind`](Self::kind) already draws — this is the address
    /// itself, for logging or for shedding a misbehaving client.
    pub const fn peer(&self) -> Option<PeerAddr> {
        self.peer
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.peer {
            Some(peer) => write!(formatter, "{} error with {peer}", self.kind.describe()),
            None => write!(formatter, "{} error", self.kind.describe()),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}
