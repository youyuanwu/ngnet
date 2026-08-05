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
///
/// Marked non-exhaustive: matching on it must include a wildcard arm, so that a future
/// variant is not a breaking change for callers.
#[derive(Debug)]
#[non_exhaustive]
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
    /// Nothing is available yet. Suspend this stream and ask again only once
    /// [`Session::resume_body`](crate::Session::resume_body) is called for it.
    ///
    /// This is the outcome an asynchronous body needs, and the only correct way to say
    /// "not yet". Returning [`BodyOutcome::Wrote`] with zero octets says something quite
    /// different: it emits an empty `DATA` frame and reschedules the stream immediately,
    /// so a source that is repeatedly not ready will spin, filling the connection with
    /// empty frames.
    ///
    /// **The stream stalls until it is resumed.** Nothing else will wake it — not another
    /// stream's traffic, not a `SETTINGS` exchange, not flow-control capacity arriving.
    /// A caller that defers without arranging for `resume_body` to be called has stalled
    /// that stream permanently, and the peer will simply wait. Only this stream is
    /// affected; the rest of the connection continues.
    Defer,
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
    /// asked again; prefer [`BodyOutcome::Eof`] when there is nothing left, or
    /// [`BodyOutcome::Defer`] when there is nothing left *yet*.
    ///
    /// The reported count must not exceed `buf.len()`. A larger one is treated as a body
    /// failure and terminates the stream rather than being forwarded, since acting on it
    /// would read past the buffer.
    ///
    /// On the push path `buf` is cleared before each call, so reading from it yields
    /// zeros rather than anything left by an earlier frame. There is nothing useful to
    /// read; it is an output buffer. (The no-copy [`SharedBodySource`] path hands over
    /// octets it already owns and is never given a buffer to clear.)
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome;
}

/// What a no-copy body source produced for one `DATA` frame.
///
/// The no-copy counterpart of [`BodyOutcome`]: instead of writing into a buffer the
/// session offers, a [`SharedBodySource`] hands back reference-counted octets it already
/// owns, which libnghttp2 serialises as a no-copy `DATA` frame — only the nine-octet
/// header is written, and the payload travels to the transport untouched.
///
/// Each chunk must not exceed the `limit` the source was given. An overlong chunk is a
/// source failure that terminates the stream rather than being forwarded, exactly as an
/// over-long count from [`BodySource::fill`] is treated on the push path: acting on it
/// would claim a frame length libnghttp2 never agreed to.
#[cfg(feature = "http")]
// Phase 1 has no production constructor for these variants — the shared body adapter that
// builds them arrives in Phase 2 — so in a non-test build they are never constructed.
// Removed when Phase 2 wires up `SharedOutgoing`.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum SharedOutcome {
    /// Handed over these octets; more will follow.
    Wrote(bytes::Bytes),
    /// Handed over these octets, and that is the whole body.
    Eof(bytes::Bytes),
    /// Handed over these octets, and trailers will follow.
    ///
    /// Keeps the stream open for a trailing header block, as
    /// [`BodyOutcome::EofWithTrailers`] does on the push path. The octets may be empty,
    /// which emits a lone end-of-body `DATA` frame ahead of the trailers.
    EofWithTrailers(bytes::Bytes),
    /// Nothing is available yet. Suspends the stream until it is resumed, exactly as
    /// [`BodyOutcome::Defer`] does; no chunk is staged and no frame is emitted.
    Defer,
    /// Abandon the message. The stream is reset and the error is reported to the
    /// stream-close handler.
    Fail(BodyError),
}

/// Produces the payload of an outgoing message as octets it already owns.
///
/// The no-copy counterpart of [`BodySource`]. Where a [`BodySource`] writes into a buffer
/// the session provides, a `SharedBodySource` hands back a [`bytes::Bytes`] the caller
/// already holds, so the payload is never copied into libnghttp2's serialisation buffer.
///
/// Implementations are owned by the session once submitted, and dropped when the stream
/// closes. A source is never asked for more octets after its stream has closed.
///
/// This is an internal adapter interface — the public opt-in is the connection entry
/// point, not this trait — which is why it is `pub(crate)`.
#[cfg(feature = "http")]
pub(crate) trait SharedBodySource: Send {
    /// Hands over up to `limit` octets of body.
    ///
    /// The returned chunk's length must not exceed `limit`. A longer one is treated as a
    /// source failure and terminates the stream rather than being forwarded, since
    /// libnghttp2 was told the frame is exactly the returned length and reading past it
    /// would corrupt the framing.
    ///
    /// Returning [`SharedOutcome::Wrote`] with an empty chunk is permitted, but it is not
    /// free and it is not a way of saying "nothing yet": libnghttp2 emits a zero-length
    /// `DATA` frame for it — nine octets of header on the wire, and a header-only record —
    /// and then asks again, so a source that keeps doing it spins while producing traffic.
    /// Prefer [`SharedOutcome::Eof`] when there is nothing left, and
    /// [`SharedOutcome::Defer`] when there is nothing left *yet*; `Defer` stages no chunk
    /// and emits no frame, which is what "nothing yet" should cost.
    fn take(&mut self, limit: usize) -> SharedOutcome;
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
