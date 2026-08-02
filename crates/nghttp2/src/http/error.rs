//! What can go wrong on an asynchronous connection.
//!
//! Three things fail in different ways and a caller usually wants to tell them apart: the
//! byte transport underneath, the connection as a whole, and one stream out of many. A
//! single opaque error would force every caller to string-match, so the distinction is
//! carried in [`ErrorKind`] and the underlying cause is kept as a [`source`] rather than
//! being flattened into the message.
//!
//! [`source`]: std::error::Error::source

use core::fmt;
use std::error::Error as StdError;

/// The category of an asynchronous connection failure.
///
/// Marked non-exhaustive: new categories are additive, and matching on this must always
/// carry a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The byte transport failed — a socket error, or a peer that truncated a frame.
    Transport,
    /// The connection is unusable. Every stream on it fails with it.
    Connection,
    /// One stream failed; the connection carries on.
    Stream,
    /// The peer, or the caller, produced something HTTP/2 does not allow.
    Protocol,
    /// The connection is shutting down or already gone, so nothing further can be sent.
    Closed,
    /// A caller-supplied message body reported an error.
    Body,
    /// The peer went away before it began this exchange, so nothing was attempted.
    ///
    /// The one failure here that is safe to retry without knowing anything else about the
    /// request: a peer that names a last stream in its `GOAWAY` is stating that everything
    /// above it was never looked at, so a retry on a fresh connection cannot duplicate a
    /// side effect. See [`Error::is_retriable`].
    Refused,
}

impl ErrorKind {
    const fn describe(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::Connection => "connection",
            Self::Stream => "stream",
            Self::Protocol => "protocol",
            Self::Closed => "closed",
            Self::Body => "body",
            Self::Refused => "refused",
        }
    }
}

/// A failure on an asynchronous HTTP/2 connection.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    detail: &'static str,
    /// The peer's own reason, for failures the peer named one for.
    reason: Option<crate::ErrorCode>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    pub(crate) const fn new(kind: ErrorKind, detail: &'static str) -> Self {
        Self {
            kind,
            detail,
            reason: None,
            source: None,
        }
    }

    pub(crate) fn with_source(
        kind: ErrorKind,
        detail: &'static str,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            detail,
            reason: None,
            source: Some(source.into()),
        }
    }

    /// Records the code the peer gave for this failure.
    pub(crate) const fn because(mut self, reason: crate::ErrorCode) -> Self {
        self.reason = Some(reason);
        self
    }

    /// The connection is gone, so this request will never be answered.
    ///
    /// Produced fresh for each affected stream rather than cloned, because an error
    /// carrying a boxed source cannot be cloned without losing it.
    pub(crate) const fn closed() -> Self {
        Self::new(
            ErrorKind::Closed,
            "the connection was shut down before this exchange completed",
        )
    }

    /// Which kind of failure this is.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether this reports a connection that has gone away.
    pub const fn is_closed(&self) -> bool {
        matches!(self.kind, ErrorKind::Closed)
    }

    /// The code the peer gave, for a failure the peer named a reason for.
    ///
    /// Present on a stream the peer reset and on a connection the peer closed with
    /// `GOAWAY`; absent for anything this end decided. That distinction is the point —
    /// it is how a caller tells "the peer refused this" from "we could not send it".
    pub const fn reason(&self) -> Option<crate::ErrorCode> {
        self.reason
    }

    /// Whether retrying this request on a fresh connection is safe.
    ///
    /// True only for [`ErrorKind::Refused`], which means the peer stated it never began
    /// this exchange. Deliberately narrow: any other failure may have been delivered and
    /// acted on before it failed, and this crate cannot know whether the request was
    /// idempotent.
    pub const fn is_retriable(&self) -> bool {
        matches!(self.kind, ErrorKind::Refused)
    }

    /// The connection is going away, and this exchange was never begun.
    pub(crate) const fn refused() -> Self {
        Self::new(
            ErrorKind::Refused,
            "the peer went away before it began this exchange",
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.describe(), self.detail)?;
        if let Some(reason) = self.reason {
            write!(f, " ({reason})")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|boxed| &**boxed as &dyn StdError)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::with_source(ErrorKind::Transport, "the transport failed", error)
    }
}

impl From<crate::Error> for Error {
    /// A failure from the sans-I/O core is fatal to the connection.
    ///
    /// The core only errors on conditions it cannot recover from — ordinary protocol
    /// violations are reported through frames and handlers instead — so there is no
    /// per-stream case to map here.
    fn from(error: crate::Error) -> Self {
        Self::with_source(
            ErrorKind::Connection,
            "the HTTP/2 session failed",
            Box::new(error),
        )
    }
}

/// The result of an operation on an asynchronous connection.
pub type Result<T> = core::result::Result<T, Error>;
