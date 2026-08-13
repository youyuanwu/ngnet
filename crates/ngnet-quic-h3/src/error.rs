//! How this crate fails.

use core::fmt;

/// What went wrong.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The QUIC transport failed. The connection is not usable.
    Transport,
    /// The connection ended, whether the peer closed it or it timed out.
    Closed,
    /// The endpoint that routes for this connection is no longer running.
    EndpointGone,
    /// The handshake did not complete.
    Handshake,
}

/// A failure from the QUIC transport, on its way to an HTTP/3 caller.
///
/// Carries the originating cause rather than flattening it. The HTTP/3 layer turns whatever
/// it gets here into the error a caller sees, and a caller that cannot tell a transport
/// failure from an unrelated one has nothing to act on.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
    source: Option<ngnet_quic::Error>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            context,
            source: None,
        }
    }

    /// Wraps a failure from the QUIC layer, keeping it reachable.
    pub(crate) fn transport(source: ngnet_quic::Error) -> Self {
        Self {
            kind: ErrorKind::Transport,
            context: "the QUIC transport failed",
            source: Some(source),
        }
    }

    /// Wraps a failure from the endpoint layer.
    pub(crate) fn endpoint(source: ngnet_quic::endpoint::Error) -> Self {
        Self {
            kind: ErrorKind::Closed,
            context: "the connection ended",
            source: None,
        }
        .with_note(source)
    }

    fn with_note(mut self, source: ngnet_quic::endpoint::Error) -> Self {
        self.context = match source.kind() {
            ngnet_quic::endpoint::ErrorKind::DriverGone => {
                self.kind = ErrorKind::EndpointGone;
                "the endpoint driver is not running"
            }
            ngnet_quic::endpoint::ErrorKind::HandshakeTimeout
            | ngnet_quic::endpoint::ErrorKind::HandshakeRejected => {
                self.kind = ErrorKind::Handshake;
                "the handshake did not complete"
            }
            _ => "the connection ended",
        };
        self
    }

    /// What kind of failure this is.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The failure this came from, where it came from the QUIC layer.
    pub fn source_error(&self) -> Option<&ngnet_quic::Error> {
        self.source.as_ref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(f, "{}: {source}", self.context),
            None => f.write_str(self.context),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("source", &self.source)
            .finish()
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e as &(dyn core::error::Error + 'static))
    }
}

impl From<ngnet_quic::Error> for Error {
    fn from(source: ngnet_quic::Error) -> Self {
        Self::transport(source)
    }
}

/// The result of an operation in this crate.
pub type Result<T> = core::result::Result<T, Error>;
