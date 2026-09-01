use std::sync::Arc;

use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_qmux::{CloseKind, CloseReason};

/// Stable error returned by the caller-polled adapter driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The peer closed the connection with an application code.
    ApplicationClose {
        /// Peer-supplied application code.
        error_code: u64,
    },
    /// The adapter detected local misuse or an invariant violation.
    Internal(Arc<str>),
    /// QMux, its byte stream, or another underlying component failed.
    Undefined(Arc<str>),
}

impl Error {
    pub(crate) fn application(error_code: u64) -> Self {
        Self::ApplicationClose { error_code }
    }

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
            Self::Internal(message) | Self::Undefined(message) => f.write_str(message),
        }
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
            ConnectionTerminal::Application(17).driver_error(),
            Error::ApplicationClose { error_code: 17 }
        );
        assert_eq!(
            ConnectionTerminal::Internal("invariant".into()).driver_error(),
            Error::Internal("invariant".into())
        );
        assert_eq!(
            ConnectionTerminal::Undefined(Error::undefined("lower")).driver_error(),
            Error::Undefined("lower".into())
        );
    }
}
