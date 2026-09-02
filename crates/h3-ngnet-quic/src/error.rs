//! How a transport ending becomes something hyperium H3 can act on.
//!
//! Two vocabularies meet here. `ngnet-quic` reports a close as a [`CloseError`] carrying a
//! [`CloseReason`], which distinguishes an application close from an idle timeout from a
//! transport error. Hyperium models the same event as [`ConnectionErrorIncoming`], which
//! keeps only the application code and folds everything else into a timeout or an opaque
//! cause. The mapping is therefore lossy in one direction and must be written down rather
//! than inferred at each call site, which is why it lives in one place.

use std::sync::Arc;

use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_quic::{CloseError, CloseReason};

/// Why this adapter's connection ended.
///
/// Public because a caller that holds a [`Connection`](crate::Connection) may want the
/// reason in its own terms rather than hyperium's, and because hyperium's
/// [`ConnectionErrorIncoming::Undefined`] carries a `dyn Error` that has to be *some*
/// concrete type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The peer closed the connection with an application code.
    ApplicationClose {
        /// Peer-supplied application code.
        error_code: u64,
    },
    /// Nothing arrived for the idle timeout, so the connection lapsed.
    IdleTimeout,
    /// The adapter detected local misuse or a broken invariant.
    Internal(Arc<str>),
    /// The transport failed for a reason with no HTTP/3 meaning.
    Undefined(Arc<str>),
}

impl Error {
    pub(crate) fn internal(message: impl Into<Arc<str>>) -> Self {
        Self::Internal(message.into())
    }

    pub(crate) fn undefined(message: impl Into<Arc<str>>) -> Self {
        Self::Undefined(message.into())
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ApplicationClose { error_code } => {
                write!(f, "peer application close, code {error_code}")
            }
            Self::IdleTimeout => f.write_str("the connection lapsed on its idle timeout"),
            Self::Internal(message) | Self::Undefined(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

/// How the connection ended, in the adapter's own terms.
///
/// Separate from [`Error`] only so the mapping into hyperium's two error enums is written
/// once. `IdleTimeout` is kept distinct from `Undefined` because hyperium has a dedicated
/// `Timeout` variant and collapsing the two would tell an HTTP/3 caller that a peer which
/// simply went quiet had failed in some unspecified way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionTerminal {
    Application(u64),
    IdleTimeout,
    Internal(Arc<str>),
    Undefined(Arc<str>),
}

impl ConnectionTerminal {
    /// Classifies a transport close.
    ///
    /// Only an application close carries a code HTTP/3 can interpret; a transport-level
    /// close, a version-negotiation failure, a drop or a Retry all mean "the connection
    /// ended and HTTP/3 was not told why", which is exactly `Undefined`.
    pub(crate) fn from_close(close: &CloseError) -> Self {
        match close.reason() {
            CloseReason::Application(code) => Self::Application(code.get()),
            CloseReason::IdleTimeout => Self::IdleTimeout,
            other => {
                Self::Undefined(format!("the transport closed the connection: {other:?}").into())
            }
        }
    }

    pub(crate) fn internal(message: impl Into<Arc<str>>) -> Self {
        Self::Internal(message.into())
    }

    pub(crate) fn undefined(message: impl Into<Arc<str>>) -> Self {
        Self::Undefined(message.into())
    }

    pub(crate) fn error(&self) -> Error {
        match self {
            Self::Application(error_code) => Error::ApplicationClose {
                error_code: *error_code,
            },
            Self::IdleTimeout => Error::IdleTimeout,
            Self::Internal(message) => Error::internal(Arc::clone(message)),
            Self::Undefined(message) => Error::undefined(Arc::clone(message)),
        }
    }

    pub(crate) fn connection_error(&self) -> ConnectionErrorIncoming {
        match self {
            Self::Application(error_code) => ConnectionErrorIncoming::ApplicationClose {
                error_code: *error_code,
            },
            Self::IdleTimeout => ConnectionErrorIncoming::Timeout,
            Self::Internal(message) => ConnectionErrorIncoming::InternalError(message.to_string()),
            Self::Undefined(_) => ConnectionErrorIncoming::Undefined(Arc::new(self.error())),
        }
    }

    pub(crate) fn stream_error(&self) -> StreamErrorIncoming {
        StreamErrorIncoming::ConnectionErrorIncoming {
            connection_error: self.connection_error(),
        }
    }
}

/// Why one direction of one stream ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectionTerminal {
    /// The peer asked this endpoint to stop sending.
    Stopped(u64),
    /// The peer reset the stream it was sending on.
    Reset(u64),
    /// This endpoint reset its own sending half.
    ///
    /// Kept apart from the two above because they describe what the *peer* did. This one is
    /// the local `reset`, and it has to be recorded as a send-side terminal rather than left
    /// implicit: ngtcp2 refuses a write to a stream whose sending half it has already shut,
    /// and a caller that goes on to offer one would turn a stream-level decision into a
    /// transport error and take the connection down with it.
    Abandoned(u64),
}

impl DirectionTerminal {
    pub(crate) fn stream_error(self) -> StreamErrorIncoming {
        match self {
            Self::Stopped(error_code) | Self::Reset(error_code) | Self::Abandoned(error_code) => {
                StreamErrorIncoming::StreamTerminated { error_code }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_timeout_is_a_timeout_and_not_an_opaque_failure() {
        assert!(matches!(
            ConnectionTerminal::IdleTimeout.connection_error(),
            ConnectionErrorIncoming::Timeout
        ));
    }

    #[test]
    fn an_application_close_keeps_its_code() {
        assert!(matches!(
            ConnectionTerminal::Application(0x105).connection_error(),
            ConnectionErrorIncoming::ApplicationClose { error_code: 0x105 }
        ));
    }

    #[test]
    fn a_direction_terminal_carries_the_code_the_peer_sent() {
        assert!(matches!(
            DirectionTerminal::Reset(9).stream_error(),
            StreamErrorIncoming::StreamTerminated { error_code: 9 }
        ));
        assert!(matches!(
            DirectionTerminal::Stopped(9).stream_error(),
            StreamErrorIncoming::StreamTerminated { error_code: 9 }
        ));
    }

    #[test]
    fn every_terminal_has_a_stable_public_classification() {
        assert_eq!(
            ConnectionTerminal::Application(17).error(),
            Error::ApplicationClose { error_code: 17 }
        );
        assert_eq!(ConnectionTerminal::IdleTimeout.error(), Error::IdleTimeout);
        assert_eq!(
            ConnectionTerminal::internal("invariant").error(),
            Error::Internal("invariant".into())
        );
        assert_eq!(
            ConnectionTerminal::undefined("transport").error(),
            Error::Undefined("transport".into())
        );
    }
}
