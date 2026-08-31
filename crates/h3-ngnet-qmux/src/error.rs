use std::sync::Arc;

use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_qmux::{CloseKind, CloseReason};

/// Stable classification returned by the caller-polled adapter driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The peer closed the connection with an application code.
    ApplicationClose {
        /// Peer-supplied application code.
        error_code: u64,
    },
    /// The adapter detected local misuse or an invariant violation.
    Internal,
    /// QMux, its byte stream, or another underlying component failed.
    Undefined,
}

/// A stable adapter-driver failure.
#[derive(Clone, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: Arc<str>,
}

impl Error {
    pub(crate) fn application(error_code: u64) -> Self {
        Self {
            kind: ErrorKind::ApplicationClose { error_code },
            message: format!("peer application close, code {error_code}").into(),
        }
    }

    pub(crate) fn internal(message: impl Into<Arc<str>>) -> Self {
        Self {
            kind: ErrorKind::Internal,
            message: message.into(),
        }
    }

    pub(crate) fn undefined(message: impl Into<Arc<str>>) -> Self {
        Self {
            kind: ErrorKind::Undefined,
            message: message.into(),
        }
    }

    /// Stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub(crate) enum ConnectionTerminal {
    Application(u64),
    Internal(Arc<str>),
    Undefined(Error),
}

impl ConnectionTerminal {
    pub(crate) fn from_lower(error: &ngnet_qmux::io::Error) -> Self {
        if let Some(reason) = error.close_reason()
            && reason.kind() == CloseKind::Application
        {
            return Self::Application(reason.error_code());
        }
        Self::Undefined(Error::undefined(error.to_string()))
    }

    pub(crate) fn connection_error(&self) -> ConnectionErrorIncoming {
        match self {
            Self::Application(error_code) => ConnectionErrorIncoming::ApplicationClose {
                error_code: *error_code,
            },
            Self::Internal(message) => ConnectionErrorIncoming::InternalError(message.to_string()),
            Self::Undefined(error) => ConnectionErrorIncoming::Undefined(Arc::new(error.clone())),
        }
    }

    pub(crate) fn stream_error(&self) -> StreamErrorIncoming {
        StreamErrorIncoming::ConnectionErrorIncoming {
            connection_error: self.connection_error(),
        }
    }

    pub(crate) fn driver_error(&self) -> Error {
        match self {
            Self::Application(error_code) => Error::application(*error_code),
            Self::Internal(message) => Error::internal(Arc::clone(message)),
            Self::Undefined(error) => error.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectionTerminal {
    Finished,
    Stopped(u64),
    Reset(u64),
    Closed,
}

pub(crate) fn close_reason(code: u64, reason: &[u8]) -> CloseReason {
    CloseReason::application(code, reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_terminal_has_a_stable_public_driver_classification() {
        assert_eq!(
            ConnectionTerminal::Application(17).driver_error().kind(),
            ErrorKind::ApplicationClose { error_code: 17 }
        );
        assert_eq!(
            ConnectionTerminal::Internal("invariant".into())
                .driver_error()
                .kind(),
            ErrorKind::Internal
        );
        assert_eq!(
            ConnectionTerminal::Undefined(Error::undefined("lower"))
                .driver_error()
                .kind(),
            ErrorKind::Undefined
        );
    }
}
