//! What can go wrong on an asynchronous HTTP/3 connection.
//!
//! Four things fail in different ways and a caller usually wants to tell them apart: the
//! QUIC connection underneath, the HTTP/3 connection as a whole, one exchange out of many,
//! and the caller's own message body. A single opaque error would force every caller to
//! string-match, so the distinction is carried in [`ErrorKind`] and the underlying cause is
//! kept as a [`source`] rather than being flattened into the message.
//!
//! [`source`]: std::error::Error::source

use core::fmt;
use std::error::Error as StdError;

use crate::error::ErrorCode;

/// The category of an asynchronous connection failure.
///
/// Marked non-exhaustive: new categories are additive, and matching on this must always
/// carry a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The QUIC backend failed — a socket error, a closed connection, a refused stream.
    Transport,
    /// The connection is unusable. Every exchange on it fails with it.
    Connection,
    /// One exchange failed; the connection carries on.
    Stream,
    /// The peer, or the caller, produced something HTTP/3 does not allow.
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

/// A failure on an asynchronous HTTP/3 connection.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    detail: &'static str,
    code: Option<ErrorCode>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, detail: &'static str) -> Self {
        Self {
            kind,
            detail,
            code: None,
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
            code: None,
            source: Some(source.into()),
        }
    }

    /// What kind of failure this is.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether the connection is shutting down or already gone.
    pub fn is_closed(&self) -> bool {
        matches!(self.kind, ErrorKind::Closed)
    }

    /// The HTTP/3 application error code, where one was carried.
    pub fn code(&self) -> Option<ErrorCode> {
        self.code
    }

    /// Whether retrying this exchange on a fresh connection is safe.
    ///
    /// True only for [`ErrorKind::Refused`], which is the one failure that states the peer
    /// never looked at the request. Anything else may have been acted on.
    pub fn is_retriable(&self) -> bool {
        matches!(self.kind, ErrorKind::Refused)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.describe(), self.detail)?;
        if let Some(code) = self.code {
            write!(f, " ({code})")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|source| &**source as &(dyn StdError + 'static))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::with_source(ErrorKind::Transport, "the QUIC backend failed", error)
    }
}

impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        // Whether the *connection* survived, which is not the same question as whether
        // nghttp3 calls the code fatal: a path failure poisons the connection whatever the
        // code says, and `is_fatal` is the accessor that reports the former.
        let kind = if error.is_fatal() {
            ErrorKind::Connection
        } else {
            ErrorKind::Stream
        };
        let code = error.app_error_code();
        let mut error = Self::with_source(kind, "the HTTP/3 state machine failed", error);
        error.code = code;
        error
    }
}

/// The result of an asynchronous HTTP/3 operation.
pub type Result<T> = core::result::Result<T, Error>;
