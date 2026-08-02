//! Carrying an `http_body::Body` into the session as a body source.
//!
//! The two sides want opposite things. `http_body` is pull-based and asynchronous: it is
//! polled with a [`Context`] and may answer `Pending`. The session is pull-based and
//! synchronous: it asks for octets and expects an answer immediately, with
//! [`BodyOutcome::Defer`] as the way to say "not yet". The adapter here is what turns one
//! into the other, and the [`Waker`] it carries is what makes the deferral recoverable —
//! without it, a body that said `Pending` would never be asked again.
//!
//! # One chunk, never two
//!
//! The session hands over a bounded buffer; an `http_body` frame is whatever size its
//! producer chose. When a frame does not fit, the remainder is held here and drained on
//! the next consultation — **one** chunk, in one field, with no container that could hold
//! a second. That is not an implementation detail. Buffering ahead would mean polling a
//! body that had not been asked for, which turns the caller's backpressure into this
//! crate's memory.
//!
//! # Why trailers cost an extra question
//!
//! `http_body` announces trailers by yielding them *after* the last data frame, so at the
//! moment the final data goes out there is no way to know it was final. HTTP/2 needs that
//! decision earlier: the frame ending a body either closes the stream or leaves it open
//! for a trailing block, and it cannot be revised afterwards. Learning it in time would
//! mean polling one frame ahead — exactly the buffering the section above forbids — so
//! the trailing block is announced on the next consultation instead, which writes no
//! octets.
//!
//! That costs one further consultation of the body and nothing on the wire: libnghttp2
//! cancels a zero-length `DATA` frame that would carry no end-of-stream rather than
//! sending it (`nghttp2_session.c:7585`), which is exactly the frame this would produce.
//! The trailing block follows the last data frame directly.
//!
//! [`Context`]: core::task::Context

use core::pin::Pin;
use core::task::{Context, Poll};
use std::error::Error as StdError;
use std::sync::Arc;
use std::task::Waker;

use bytes::Buf;
use http_body::Body;

use crate::{BodyOutcome, BodySource};

use super::super::error::{Error, ErrorKind};
use super::super::shared::Shared;
use super::super::waker::StreamWaker;

/// How many empty data frames to skip before handing control back.
///
/// An empty frame carries nothing and costs nine octets on the wire, so it is skipped
/// rather than forwarded. The bound is what stops a body yielding nothing but empty frames
/// from spinning inside `Session::send`, where there is no yield point and the whole
/// connection would be held up behind it.
const EMPTY_FRAME_LIMIT: usize = 16;

/// Presents an [`http_body::Body`] to the session.
pub(crate) struct Outgoing<B: Body> {
    body: Pin<Box<B>>,
    /// Wakes the driver and names this stream. Handed to the body on every consultation,
    /// so a body that stores it keeps a valid one.
    waker: Waker,
    /// The same waker, kept for the stream identifier it was given at submission.
    naming: Arc<StreamWaker>,
    /// Where a trailing block is left: the session cannot accept one from in here.
    shared: Arc<Shared>,
    /// What a previous consultation could not fit in the buffer it was given.
    ///
    /// One chunk. See the module documentation for why there is deliberately nowhere to
    /// put a second.
    leftover: Option<B::Data>,
}

impl<B: Body> Outgoing<B> {
    pub(crate) fn new(body: B, notify: Arc<StreamWaker>, shared: Arc<Shared>) -> Self {
        Self {
            body: Box::pin(body),
            waker: Waker::from(Arc::clone(&notify)),
            naming: notify,
            shared,
            leftover: None,
        }
    }

    /// Moves as much of `data` into `buf` as fits, reporting how much moved.
    fn drain_into(data: &mut B::Data, buf: &mut [u8]) -> usize {
        let mut written = 0;
        while written < buf.len() && data.has_remaining() {
            let chunk = data.chunk();
            let take = chunk.len().min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&chunk[..take]);
            data.advance(take);
            written += take;
        }
        written
    }

    /// Writes `data` out, keeping any remainder, and reports what is being held back.
    fn hand_over(&mut self, mut data: B::Data, buf: &mut [u8]) -> BodyOutcome {
        let written = Self::drain_into(&mut data, buf);
        if data.has_remaining() {
            self.leftover = Some(data);
        }
        self.shared
            .note_buffered(usize::from(self.leftover.is_some()));
        BodyOutcome::Wrote(written)
    }
}

impl<B> BodySource for Outgoing<B>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        if let Some(data) = self.leftover.take() {
            return self.hand_over(data, buf);
        }

        let mut context = Context::from_waker(&self.waker);

        for _ in 0..EMPTY_FRAME_LIMIT {
            let frame = match self.body.as_mut().poll_frame(&mut context) {
                // The stream suspends here and nothing but `resume_body` will restart it,
                // so the waker handed over above is the only thing keeping this body
                // alive.
                Poll::Pending => return BodyOutcome::Defer,
                Poll::Ready(None) => return BodyOutcome::Eof(0),
                Poll::Ready(Some(Err(error))) => {
                    // Boxed as this crate's own error so the driver can recover it by type
                    // when the session hands it back at stream close, rather than reducing
                    // the caller's cause to a printed string.
                    return BodyOutcome::Fail(Box::new(Error::with_source(
                        ErrorKind::Body,
                        "the outgoing body reported an error",
                        error,
                    )));
                }
                Poll::Ready(Some(Ok(frame))) => frame,
            };

            let frame = match frame.into_data() {
                Ok(data) => {
                    if !data.has_remaining() {
                        continue;
                    }
                    return self.hand_over(data, buf);
                }
                Err(frame) => frame,
            };

            return match frame.into_trailers() {
                Ok(trailers) => {
                    self.shared.stash_trailers(self.naming.stream(), trailers);
                    BodyOutcome::EofWithTrailers(0)
                }
                // Neither data nor trailers: a frame kind `http_body` has grown and this
                // crate does not know. Ending the message is the conservative reading —
                // the alternative is to carry on as though a frame that was never sent
                // had been.
                Err(_unknown) => BodyOutcome::Eof(0),
            };
        }

        // Reachable only from a body producing empty frames without end. Writing nothing
        // returns control to the session, which will ask again: a slow loop rather than a
        // connection that has stopped.
        BodyOutcome::Wrote(0)
    }
}
