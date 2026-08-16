//! What can go wrong once there is a byte stream involved.
//!
//! The state machine's [`Error`](crate::Error) describes what dwnx refused to do. This layer
//! additionally has a transport that can fail, a transport that can *end* -- in two ways that
//! mean different things -- and a peer that can close deliberately. A caller here needs to
//! tell those apart, and the state machine never had to.
//!
//! # Why the kinds are what they are
//!
//! Each variant exists because a caller would plausibly *do* something different about it,
//! not because the failures arise in different places. The pair worth dwelling on is
//! [`ErrorKind::EndOfStream`] against [`ErrorKind::TruncatedRecord`]. Both are "the byte
//! stream ended", and they are not the same event: the first is a peer that stopped speaking
//! between records, which is rude but harmless and is what a closed TCP connection looks like
//! when the process at the far end exited cleanly; the second is a peer that stopped speaking
//! *partway through* a record, which means bytes were lost in transit or the substrate
//! truncated the stream, and a caller who treats it as a clean ending has silently accepted
//! an incomplete transfer.
//!
//! dwnx cannot make that distinction: it buffers a partial record and reports that it needs
//! more input, which is indistinguishable from a record that has merely not finished arriving
//! yet. The layer tracks record boundaries itself in order to be able to say which of the two
//! happened.
//!
//! [`ErrorKind::PeerClosed`] and [`ErrorKind::LocallyClosed`] are outcomes rather than
//! failures, and they are errors here for the same reason the QUIC layer makes them errors:
//! every operation on a closed connection has to fail somehow, and a caller reading the reason
//! off the failure is better served than one who must consult a separate state enquiry.

// The constructors are used by the connection, the pump and the stream handles, which are
// assembled in later phases of this layer; the type has to exist before the things that
// produce it, and it is the type every one of them is written against.
#![allow(dead_code)]

use core::fmt;

use crate::ccerr::CloseReason;

/// The result of an operation on an asynchronous connection.
pub type Result<T> = core::result::Result<T, Error>;

/// What went wrong.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The byte stream itself failed.
    ///
    /// Fatal, and nothing above it can recover: QMux has no path that is independent of the
    /// stream it runs over. The transport's own failure is attached as the source.
    ByteStream,

    /// The byte stream ended between records.
    ///
    /// A clean ending in the only sense QMux offers one without a CONNECTION_CLOSE: everything
    /// the peer sent was complete and was processed. Distinct from
    /// [`ErrorKind::PeerClosed`] because no close was received, so there is no error code and
    /// no reason -- the peer simply stopped, and may not consider the connection over at all.
    EndOfStream,

    /// The byte stream ended partway through a record.
    ///
    /// Data was lost. Whatever the partial record contained is unrecoverable, and any stream
    /// it carried is incomplete in a way the peer does not know about. A caller that retries
    /// must not assume the peer received anything it had not already acknowledged.
    TruncatedRecord,

    /// The peer violated the protocol.
    ///
    /// What dwnx reports when the bytes it was fed cannot be part of a valid QMux stream: a
    /// record larger than the negotiated maximum, a frame in a state that forbids it, flow
    /// control exceeded. Retrying achieves nothing; the peer is broken, or something between
    /// the two is corrupting the stream.
    Protocol,

    /// The peer closed the connection.
    ///
    /// The kind, code, frame type and reason it sent are on the error itself; see
    /// [`Error::close_reason`]. This is the orderly ending, and the only one that carries an
    /// explanation.
    PeerClosed,

    /// This endpoint closed the connection.
    ///
    /// Every subsequent operation fails this way, carrying the reason that was supplied to the
    /// close, so a caller who finds it on an operation they did not expect to fail can see
    /// which close it was.
    LocallyClosed,

    /// The state machine refused a request this layer or its caller made.
    ///
    /// Not the peer's doing and not the transport's: an operation on a stream that does not
    /// exist, an argument dwnx rejected, an allocation it could not make. Separated from
    /// [`ErrorKind::Protocol`] because the two lead somewhere different -- a protocol failure
    /// is a report about the far end, and this is a report about this end.
    Internal,
}

impl ErrorKind {
    /// Whether the connection ended in an orderly way rather than by failing.
    ///
    /// True for the two closes and for a stream that ended between records. False for a
    /// truncation, a protocol violation and a transport failure, each of which means something
    /// was lost or something is wrong.
    #[must_use]
    pub const fn is_orderly(self) -> bool {
        matches!(
            self,
            Self::EndOfStream | Self::PeerClosed | Self::LocallyClosed
        )
    }
}

/// A failure from the asynchronous layer.
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
    close: Option<CloseReason>,
    source: Option<Box<dyn core::error::Error + Send + Sync>>,
}

impl Error {
    /// Builds an error of `kind` with a static description.
    pub(crate) fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            context,
            close: None,
            source: None,
        }
    }

    /// Attaches the reason a connection closed.
    pub(crate) fn with_close(mut self, close: CloseReason) -> Self {
        self.close = Some(close);
        self
    }

    /// Attaches an underlying cause.
    pub(crate) fn with_source(
        mut self,
        source: impl core::error::Error + Send + Sync + 'static,
    ) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Attaches an already-boxed cause.
    ///
    /// This is the one the byte-stream seam uses. [`AsyncByteStream::Error`] is bounded only
    /// by its conversion into a boxed error, so a transport failure arrives here already
    /// boxed and cannot be boxed again by [`Error::with_source`], which needs a concrete
    /// `'static` type.
    ///
    /// [`AsyncByteStream::Error`]: crate::io::AsyncByteStream::Error
    pub(crate) fn with_boxed_source(
        mut self,
        source: Box<dyn core::error::Error + Send + Sync>,
    ) -> Self {
        self.source = Some(source);
        self
    }

    /// Replaces the description.
    ///
    /// The mapping from a state-machine failure keeps that layer's phrasing, which is accurate
    /// but says nothing about what this layer was doing when it happened. Where the doing is
    /// the interesting part -- a record that failed to serialise, say -- the connection
    /// substitutes its own description and keeps the original as the source.
    pub(crate) const fn with_context(mut self, context: &'static str) -> Self {
        self.context = context;
        self
    }

    /// What kind of failure this is.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The description this error was built with.
    ///
    /// Not public: a caller reads the description through [`fmt::Display`], which also renders
    /// the close reason. This exists so the connection can reproduce a latched ending on every
    /// later operation without keeping the original error, whose boxed source cannot be cloned.
    pub(crate) const fn context(&self) -> &'static str {
        self.context
    }

    /// Why the connection closed, when the failure was a close.
    ///
    /// Present for [`ErrorKind::PeerClosed`] and [`ErrorKind::LocallyClosed`], carrying all
    /// four fields of the CONNECTION_CLOSE: its kind, the error code, the frame type that
    /// provoked it and the reason phrase.
    #[must_use]
    pub fn close_reason(&self) -> Option<&CloseReason> {
        self.close.as_ref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.context)?;
        if let Some(close) = &self.close {
            write!(f, ": {:?} close, code {}", close.kind(), close.error_code())?;
            if !close.reason().is_empty() {
                write!(f, " ({})", String::from_utf8_lossy(close.reason()))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("close", &self.close)
            .field("source", &self.source.as_ref().map(|s| s.to_string()))
            .finish()
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|s| &**s as &(dyn core::error::Error + 'static))
    }
}

impl From<crate::error::Error> for Error {
    /// Maps a state-machine failure onto a layer one.
    ///
    /// The state machine has no transport, no clock and no notion of a byte stream ending, so
    /// the only distinctions to preserve are between a peer that sent something invalid, a
    /// connection that is already closed, and a request this side should not have made. The
    /// conditions dwnx raises while parsing inbound bytes are the first of those; the rest are
    /// reports about this end, not the far one, and are kept apart for that reason.
    fn from(err: crate::error::Error) -> Self {
        let kind = match err.kind() {
            crate::ErrorKind::Closed => ErrorKind::LocallyClosed,
            crate::ErrorKind::Protocol
            | crate::ErrorKind::LimitExceeded
            | crate::ErrorKind::TransportParameter
            | crate::ErrorKind::Stream => ErrorKind::Protocol,
            _ => ErrorKind::Internal,
        };
        Self::new(kind, "the connection failed").with_source(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ccerr::CloseKind;

    /// Stands in for a caller's transport failure, which reaches this layer already boxed.
    #[derive(Debug)]
    struct TransportFailure;

    impl fmt::Display for TransportFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the socket went away")
        }
    }

    impl core::error::Error for TransportFailure {}

    #[test]
    fn a_close_reason_reaches_the_display_and_the_accessor() {
        let error = Error::new(ErrorKind::PeerClosed, "the peer closed the connection")
            .with_close(CloseReason::application(7, b"done here"));

        let close = error.close_reason().expect("the close reason is preserved");
        assert_eq!(close.kind(), CloseKind::Application);
        assert_eq!(close.error_code(), 7);
        assert_eq!(close.reason(), b"done here");

        let shown = error.to_string();
        assert!(shown.contains("the peer closed the connection"), "{shown}");
        assert!(shown.contains("done here"), "{shown}");
    }

    #[test]
    fn a_boxed_transport_failure_is_kept_as_the_source() {
        // The shape the byte-stream seam produces: its error type is bounded only by the
        // conversion into a box, so this is the only way one can be attached.
        let boxed: Box<dyn core::error::Error + Send + Sync> = Box::new(TransportFailure);
        let error =
            Error::new(ErrorKind::ByteStream, "the byte stream failed").with_boxed_source(boxed);

        let source = core::error::Error::source(&error).expect("the source survives");
        assert!(source.to_string().contains("the socket went away"));
    }

    /// The two endings that are easiest to conflate, kept apart deliberately.
    #[test]
    fn the_two_endings_are_distinct_and_only_one_is_orderly() {
        assert_ne!(ErrorKind::EndOfStream, ErrorKind::TruncatedRecord);
        assert!(ErrorKind::EndOfStream.is_orderly());
        assert!(!ErrorKind::TruncatedRecord.is_orderly());
        assert!(!ErrorKind::ByteStream.is_orderly());
        assert!(!ErrorKind::Protocol.is_orderly());
        assert!(!ErrorKind::Internal.is_orderly());
    }

    /// The bound the HTTP/3 join will need of anything that reaches it from here.
    #[test]
    fn the_error_is_sendable_shareable_and_boxable() {
        fn require<E: core::error::Error + Send + Sync + 'static>(
            error: E,
        ) -> Box<dyn core::error::Error + Send + Sync> {
            Box::new(error)
        }
        let boxed = require(Error::new(
            ErrorKind::Protocol,
            "the peer violated the protocol",
        ));
        assert!(boxed.to_string().contains("violated"));
    }
}
