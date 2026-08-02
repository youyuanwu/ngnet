//! Carrying an `http_body::Body` into the session as a body source.
//!
//! The two sides want opposite things. `http_body` is pull-based and asynchronous: it is
//! polled with a [`Context`] and may answer `Pending`. The session is pull-based and
//! synchronous: it asks for octets and expects an answer immediately, with
//! [`BodyOutcome::Defer`] as the way to say "not yet". The adapter here is what turns one
//! into the other, and the [`Waker`] it carries is what makes the deferral recoverable —
//! without it, a body that said `Pending` would never be asked again.
//!
//! [`Context`]: core::task::Context

use core::pin::Pin;
use core::task::{Context, Poll};
use std::error::Error as StdError;
use std::task::Waker;

use bytes::Buf;
use http_body::Body;

use crate::{BodyOutcome, BodySource};

use super::super::error::{Error, ErrorKind};

/// Presents an [`http_body::Body`] to the session.
pub(crate) struct Outgoing<B: Body> {
    body: Pin<Box<B>>,
    /// Wakes the driver and names this stream. Handed to the body on every consultation,
    /// so a body that stores it keeps a valid one.
    waker: Waker,
    /// What a previous consultation could not fit in the buffer it was given.
    ///
    /// The session hands over a bounded buffer and a frame from `http_body` is whatever
    /// size the producer chose, so the two do not line up. Without this the remainder
    /// would simply be lost.
    leftover: Option<B::Data>,
}

impl<B: Body> Outgoing<B> {
    pub(crate) fn new(body: B, waker: Waker) -> Self {
        Self {
            body: Box::pin(body),
            waker,
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
}

impl<B> BodySource for Outgoing<B>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        if let Some(mut data) = self.leftover.take() {
            let written = Self::drain_into(&mut data, buf);
            if data.has_remaining() {
                self.leftover = Some(data);
            }
            return BodyOutcome::Wrote(written);
        }

        let mut context = Context::from_waker(&self.waker);
        match self.body.as_mut().poll_frame(&mut context) {
            // The stream suspends here and nothing but `resume_body` will restart it, so
            // the waker handed over above is the only thing keeping this body alive.
            Poll::Pending => BodyOutcome::Defer,
            Poll::Ready(None) => BodyOutcome::Eof(0),
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(mut data) => {
                    let written = Self::drain_into(&mut data, buf);
                    if data.has_remaining() {
                        self.leftover = Some(data);
                    }
                    BodyOutcome::Wrote(written)
                }
                // A trailers frame. Sending it is Phase 5's job; ending the body here is
                // the conservative reading, since the alternative would be to drop the
                // frame and carry on as though the body were still running.
                Err(_frame) => BodyOutcome::Eof(0),
            },
            Poll::Ready(Some(Err(error))) => BodyOutcome::Fail(Box::new(Error::with_source(
                ErrorKind::Body,
                "the outgoing body reported an error",
                error,
            ))),
        }
    }
}
