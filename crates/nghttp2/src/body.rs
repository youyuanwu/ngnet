//! Outgoing message bodies.
//!
//! A body is produced progressively: libnghttp2 asks for payload as flow-control capacity
//! becomes available, and the caller writes into the buffer it is handed. Nothing is
//! buffered up front.

use core::error::Error as StdError;

/// An error reported by a caller's body source.
///
/// Bounded `Send` because it is parked in session state until the stream closes, and a
/// session may be moved between threads.
pub type BodyError = Box<dyn StdError + Send>;

/// What a body source did with the buffer it was given.
#[derive(Debug)]
pub enum BodyOutcome {
    /// Wrote this many octets; more will follow.
    Wrote(usize),
    /// Wrote this many octets, and that is the whole body.
    Eof(usize),
    /// Wrote this many octets, and trailers will follow.
    ///
    /// Only the caller knows whether trailers are coming, and the decision has to be made
    /// before the body ends: the frame that ends a body either closes the stream or
    /// leaves it open for a trailing header block, and that cannot be revised afterwards.
    /// Returning this keeps the stream open, after which
    /// [`Session::submit_trailer`](crate::Session::submit_trailer) becomes legal.
    EofWithTrailers(usize),
    /// Abandon the message. The stream is reset and the error is reported to the
    /// stream-close handler.
    Fail(BodyError),
}

/// Produces the payload of an outgoing message.
///
/// Implementations are owned by the session once submitted, and dropped when the stream
/// closes. A source is never asked for more octets after its stream has closed.
pub trait BodySource: Send {
    /// Writes up to `buf.len()` octets of body into `buf`.
    ///
    /// Returning [`BodyOutcome::Wrote`] with zero octets is permitted but will simply be
    /// asked again; prefer [`BodyOutcome::Eof`] when there is nothing left.
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome;
}

/// A body already held in memory.
///
/// The common case, and a worked example of the trait: hand over the octets, then report
/// end of body.
#[derive(Debug)]
pub struct BytesBody {
    data: Vec<u8>,
    offset: usize,
    trailers: bool,
}

impl BytesBody {
    /// A body consisting of exactly these octets.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            offset: 0,
            trailers: false,
        }
    }

    /// Announces that trailers will follow this body.
    ///
    /// Without this the body closes the stream, and trailers can no longer be sent.
    #[must_use]
    pub fn with_trailers(mut self) -> Self {
        self.trailers = true;
        self
    }
}

impl BodySource for BytesBody {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        let remaining = self.data.len() - self.offset;
        let take = remaining.min(buf.len());
        buf[..take].copy_from_slice(&self.data[self.offset..self.offset + take]);
        self.offset += take;

        if self.offset < self.data.len() {
            BodyOutcome::Wrote(take)
        } else if self.trailers {
            BodyOutcome::EofWithTrailers(take)
        } else {
            BodyOutcome::Eof(take)
        }
    }
}
