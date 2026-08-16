//! How this crate fails.

use core::fmt;

use ngnet_qmux::io::{Error as LayerError, ErrorKind as LayerErrorKind};

/// What went wrong.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The byte stream underneath the connection failed.
    ///
    /// Fatal, and nothing above it can recover: QMux has no path independent of the stream
    /// it runs over.
    ByteStream,

    /// The byte stream ended between records.
    ///
    /// The peer simply stopped speaking. Everything it had sent was complete, so this is the
    /// benign ending, and it is what a peer that exited without closing looks like.
    EndOfStream,

    /// The byte stream ended partway through a record.
    ///
    /// Data was lost. Kept apart from [`ErrorKind::EndOfStream`] because a caller that
    /// treats a truncation as a clean ending has silently accepted an incomplete transfer.
    TruncatedRecord,

    /// The peer violated the QMux protocol.
    Protocol,

    /// The connection has ended, whether this endpoint closed it or the peer did.
    Closed,

    /// The QMux state machine refused something this crate asked of it.
    Internal,

    /// The HTTP/3 layer refused to start on this connection.
    ///
    /// Only reachable from [`connect`](crate::connect) and [`serve`](crate::serve), which
    /// build the HTTP/3 driver themselves. Once the driver is running its failures are its
    /// own and come back through its future, not through this type.
    Http3,
}

/// A failure on the way between QMux and HTTP/3.
///
/// # Why the cause is a string and not a source
///
/// A connection's ending is reported to the HTTP/3 layer once and then reproduced on every
/// later operation, because every operation on a dead connection has to fail somehow. The
/// layer below has the same problem and solves it the same way: [`LayerError`] owns a boxed
/// source that cannot be cloned, so an ending that had to be handed out repeatedly could not
/// keep it. Rendering the cause once, when the ending is latched, is what makes this type
/// `Clone`; the alternative — handing the real error out the first time and a poorer one
/// after — makes the diagnostic depend on which call happened to lose the race.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
    cause: Option<String>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            context,
            cause: None,
        }
    }

    /// Renders a failure from the QMux layer, keeping what it said.
    pub(crate) fn layer(source: &LayerError) -> Self {
        Self {
            kind: kind_of(source.kind()),
            context: context_of(source.kind()),
            cause: Some(source.to_string()),
        }
    }

    /// Renders a failure from the HTTP/3 layer's construction.
    pub(crate) fn http3(source: ngnet_h3::http::Error) -> Self {
        Self {
            kind: ErrorKind::Http3,
            context: "the HTTP/3 layer refused the connection",
            cause: Some(source.to_string()),
        }
    }

    /// What kind of failure this is.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

/// The kind a QMux failure becomes here.
///
/// A one-for-one mapping, deliberately. Flattening the layer's endings into a single
/// "transport failed" would cost the HTTP/3 caller the one distinction it can act on: a
/// connection whose peer stopped speaking cleanly is a connection worth reopening, and a
/// protocol violation is not.
const fn kind_of(kind: LayerErrorKind) -> ErrorKind {
    match kind {
        LayerErrorKind::ByteStream => ErrorKind::ByteStream,
        LayerErrorKind::EndOfStream => ErrorKind::EndOfStream,
        LayerErrorKind::TruncatedRecord => ErrorKind::TruncatedRecord,
        LayerErrorKind::Protocol => ErrorKind::Protocol,
        LayerErrorKind::PeerClosed | LayerErrorKind::LocallyClosed => ErrorKind::Closed,
        // `LayerErrorKind` is non-exhaustive, and anything added to it is by construction
        // something this crate did not ask for and cannot interpret.
        _ => ErrorKind::Internal,
    }
}

const fn context_of(kind: LayerErrorKind) -> &'static str {
    match kind {
        LayerErrorKind::ByteStream => "the byte stream failed",
        LayerErrorKind::EndOfStream => "the peer stopped sending",
        LayerErrorKind::TruncatedRecord => "the byte stream ended inside a record",
        LayerErrorKind::Protocol => "the peer violated the QMux protocol",
        LayerErrorKind::PeerClosed => "the peer closed the connection",
        LayerErrorKind::LocallyClosed => "the connection was closed",
        _ => "the QMux layer refused the operation",
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(f, "{}: {cause}", self.context),
            None => f.write_str(self.context),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("cause", &self.cause)
            .finish()
    }
}

impl core::error::Error for Error {}

/// The result of an operation in this crate.
pub type Result<T> = core::result::Result<T, Error>;
