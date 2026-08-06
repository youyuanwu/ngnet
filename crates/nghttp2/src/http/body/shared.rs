//! Carrying an `http_body::Body` into the session as a *no-copy* body source.
//!
//! The push-model sibling of this module, [`super::outgoing`], copies each chunk into the
//! buffer libnghttp2 offers. This one does not: because the body's `Data` is a
//! [`bytes::Bytes`] the crate already owns, it hands that `Bytes` straight back through
//! [`SharedBodySource::take`], and libnghttp2 serialises only the frame header. Nothing
//! zeroes the frame buffer for this payload — the memset the push path performs is the
//! crate's own, in `read_push_body`, and it exists because that path must hand libnghttp2's
//! reused, uninitialised buffer to a source that may fill less of it than it was offered.
//! A handed-over payload never enters that buffer, so there is nothing to zero and this
//! source never copies into it: the payload is no longer *touched twice* the way the push
//! path touches it. On the
//! two readiness strategies it is not touched by the driver either: it travels to the
//! transport as its own region, in the caller's own memory. The owned strategy still
//! coalesces it once, which is inherent to a transport that takes ownership of what it is
//! handed rather than something left to remove. The saved copy is
//! the whole difference; everything else, the deferral bridge and the trailer question
//! below, is the same shape as the push path, for the same reasons.
//!
//! # One chunk, never two
//!
//! libnghttp2 asks for at most `limit` octets at a time; a body frame is whatever size its
//! producer chose. When a frame is larger, the remainder is held here — as **one**
//! [`bytes::Bytes`] in one field, sliced off the front with [`Bytes::split_to`], which
//! copies nothing — and handed over on the next consultation. There is deliberately
//! nowhere to hold a second, exactly as [`super::outgoing`] documents: buffering ahead
//! would mean polling a body that had not been asked for, turning the caller's
//! backpressure into this crate's memory.
//!
//! # Why trailers cost an extra question
//!
//! `http_body` yields trailers *after* the last data frame, but HTTP/2 must decide whether
//! a data frame ends the stream before it is sent. Learning it in time would mean polling
//! one frame ahead — the buffering the section above forbids — so the trailing block is
//! announced on the next consultation, which hands over no octets. See [`super::outgoing`]
//! for the full account; it holds here unchanged.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::error::Error as StdError;
use std::sync::Arc;
use std::task::Waker;

use bytes::Bytes;
use http_body::Body;

use crate::body::{SharedBodySource, SharedOutcome};

use super::super::error::{Error, ErrorKind};
use super::super::shared::Shared;
use super::super::waker::StreamWaker;

/// How many empty data frames to skip before handing control back.
///
/// Carried over from [`super::outgoing`] unchanged, and for the same reason: an empty frame
/// carries nothing and costs nine octets on the wire, so it is skipped rather than
/// forwarded, but a body that yields nothing but empty frames must not be allowed to spin
/// forever inside `Session::send`, where there is no yield point and the whole connection
/// waits behind it. The bound turns an unbounded spin into a slow leak the peer can see —
/// one empty frame for every sixteen the body produces — which is the better failure for a
/// body that is misbehaving either way.
const EMPTY_FRAME_LIMIT: usize = 16;

/// Presents an [`http_body::Body`] to the session as octets it already owns.
///
/// The no-copy counterpart of [`super::outgoing::Outgoing`]: same fields, same waker and
/// trailer plumbing, differing only in that [`take`](SharedBodySource::take) returns the
/// body's own [`bytes::Bytes`] rather than draining it into a buffer.
pub(crate) struct SharedOutgoing<B: Body> {
    body: Pin<Box<B>>,
    /// Wakes the driver and names this stream. Handed to the body on every consultation,
    /// so a body that stores it keeps a valid one.
    waker: Waker,
    /// The same waker, kept for the stream identifier it was given at submission.
    naming: Arc<StreamWaker>,
    /// Where a trailing block is left: the session cannot accept one from in here.
    shared: Arc<Shared>,
    /// What a previous consultation could not fit in the `limit` it was given.
    ///
    /// One chunk. See the module documentation for why there is deliberately nowhere to
    /// put a second.
    leftover: Option<Bytes>,
}

impl<B: Body> SharedOutgoing<B> {
    pub(crate) fn new(body: B, notify: Arc<StreamWaker>, shared: Arc<Shared>) -> Self {
        Self {
            body: Box::pin(body),
            waker: Waker::from(Arc::clone(&notify)),
            naming: notify,
            shared,
            leftover: None,
        }
    }

    /// Hands over up to `limit` octets of `data`, keeping any remainder.
    ///
    /// [`Bytes::split_to`] takes the front off in place, so the octets handed over and the
    /// octets retained are two views of the one allocation — no copy either way. The
    /// buffered-chunk gauge is updated on exactly the same terms as the push path
    /// ([`super::outgoing`]), because the driver's accounting reads it the same way for
    /// both.
    fn hand_over(&mut self, mut data: Bytes, limit: usize) -> SharedOutcome {
        let front = if data.len() > limit {
            let front = data.split_to(limit);
            self.leftover = Some(data);
            front
        } else {
            data
        };
        self.shared
            .note_buffered(usize::from(self.leftover.is_some()));
        SharedOutcome::Wrote(front)
    }
}

impl<B> SharedBodySource for SharedOutgoing<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn take(&mut self, limit: usize) -> SharedOutcome {
        if let Some(data) = self.leftover.take() {
            return self.hand_over(data, limit);
        }

        let mut context = Context::from_waker(&self.waker);

        for _ in 0..EMPTY_FRAME_LIMIT {
            let frame = match self.body.as_mut().poll_frame(&mut context) {
                // The stream suspends here and nothing but `resume_body` will restart it,
                // so the waker handed over above is the only thing keeping this body
                // alive.
                Poll::Pending => return SharedOutcome::Defer,
                Poll::Ready(None) => return SharedOutcome::Eof(Bytes::new()),
                Poll::Ready(Some(Err(error))) => {
                    // Boxed as this crate's own error so the driver can recover it by type
                    // when the session hands it back at stream close, rather than reducing
                    // the caller's cause to a printed string.
                    return SharedOutcome::Fail(Box::new(Error::with_source(
                        ErrorKind::Body,
                        "the outgoing body reported an error",
                        error,
                    )));
                }
                Poll::Ready(Some(Ok(frame))) => frame,
            };

            let frame = match frame.into_data() {
                Ok(data) => {
                    if data.is_empty() {
                        continue;
                    }
                    return self.hand_over(data, limit);
                }
                Err(frame) => frame,
            };

            return match frame.into_trailers() {
                Ok(trailers) => {
                    self.shared.stash_trailers(self.naming.stream(), trailers);
                    SharedOutcome::EofWithTrailers(Bytes::new())
                }
                // Neither data nor trailers: a frame kind `http_body` has grown and this
                // crate does not know. Ending the message is the conservative reading —
                // the alternative is to carry on as though a frame that was never sent had
                // been.
                Err(_unknown) => SharedOutcome::Eof(Bytes::new()),
            };
        }

        // Reachable only from a body producing empty frames without end. Handing over
        // nothing returns control to the session, which will ask again: a slow loop rather
        // than a connection that has stopped.
        SharedOutcome::Wrote(Bytes::new())
    }
}
