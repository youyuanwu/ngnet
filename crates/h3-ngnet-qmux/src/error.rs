use std::sync::Arc;

use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_qmux::{CloseKind, CloseReason};

/// A stable adapter failure carried by hyperium's undefined-error variant.
#[derive(Clone, Debug)]
pub struct Error {
    message: Arc<str>,
}

impl Error {
    pub(crate) fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
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
        Self::Undefined(Error::new(error.to_string()))
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectionTerminal {
    Finished,
    Stopped(u64),
    Reset(u64),
}

pub(crate) fn close_reason(code: u64, reason: &[u8]) -> CloseReason {
    CloseReason::application(code, reason)
}
