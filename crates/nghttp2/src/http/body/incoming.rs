//! The receiving half of a message body.
//!
//! # Where backpressure comes from
//!
//! HTTP/2 gives every stream and the connection a receive window, and the peer may only
//! send what the window allows. libnghttp2 can re-open that window automatically as
//! octets arrive, which is the opposite of backpressure: a caller reading slowly would
//! still be inviting the peer to send more, and the octets would pile up in memory
//! instead of on the wire.
//!
//! So the session is built to report consumption explicitly, and this type is the only
//! place that report is made — when [`poll_frame`](http_body::Body::poll_frame) hands a
//! chunk to the application, and when a body is dropped with octets still unread. A body
//! nobody reads therefore produces no credit, the window closes, and the peer stops. That
//! is the whole mechanism.
//!
//! # Zero copy
//!
//! The chunks handed out here are refcounted views of the buffer the driver read into, not
//! copies of it. Holding one is allowed and costs nothing beyond keeping that buffer out
//! of the driver's pool until it is dropped.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

use super::super::error::Error;
use super::super::shared::{Incoming, Shared};

/// Which message this body belongs to, which decides what dropping it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// A response a client is receiving.
    ///
    /// Dropping it unread says the caller has stopped caring, and the only way to stop the
    /// peer sending the rest is to reset the stream.
    Response,
    /// A request a server is receiving.
    ///
    /// Dropping it unread says nothing of the sort. A handler that ignores a request body
    /// is entitled to answer anyway, and resetting would destroy the response it is about
    /// to send.
    Request,
}

/// The body of a received message.
///
/// Yields data frames in the order they arrived, then a trailers frame if the peer sent
/// one. Reading it is what gives the peer room to send more.
///
/// Dropping it discards whatever has not been read and returns that capacity to the peer,
/// so an unwanted body never becomes a stalled connection. For a **response** that has not
/// finished arriving, dropping also resets the stream: returning the window would
/// otherwise invite the peer to send the rest of something nobody will read. A **request**
/// body on a server is not reset, because a handler that ignores the body still has a
/// response to give.
#[derive(Debug)]
pub struct IncomingBody {
    stream: i32,
    direction: Direction,
    incoming: Arc<Incoming>,
    shared: Arc<Shared>,
}

impl IncomingBody {
    pub(crate) const fn new(
        stream: i32,
        direction: Direction,
        incoming: Arc<Incoming>,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            stream,
            direction,
            incoming,
            shared,
        }
    }

    /// Reports `len` octets consumed and asks the driver to hand the window back.
    fn credit(&self, len: usize) {
        if len == 0 {
            return;
        }
        self.shared.credit(self.stream, len);
        // The session is the driver's, so the report can only be acted on there. Waking
        // outside every lock this touches is deliberate: a waker may run arbitrary code.
        self.shared.wake_driver();
    }
}

impl Body for IncomingBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        let polled = self.incoming.poll_frame(cx.waker());

        // Credited only once the chunk is on its way to the caller. Crediting before the
        // frame is taken would re-open the window for octets that are still sitting here.
        if let Poll::Ready(Some(Ok(frame))) = &polled {
            if let Some(data) = frame.data_ref() {
                self.credit(data.len());
            }
        }

        polled
    }

    fn is_end_stream(&self) -> bool {
        self.incoming.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        // Deliberately unbounded. A `content-length` field is the peer's claim about a
        // message it has not finished sending, and reporting it here would turn a lying
        // or truncated peer into a caller-visible contract violation rather than the
        // stream error it is.
        SizeHint::default()
    }
}

impl Drop for IncomingBody {
    fn drop(&mut self) {
        let complete = self.incoming.is_finished();
        let unread = self.incoming.abandon();
        self.credit(unread);

        // Only a response, and only one still arriving. A stream that already ended has
        // nothing left to stop, and resetting it would tell the peer something went wrong
        // when nothing did.
        if self.direction == Direction::Response && !complete {
            self.shared.reset(self.stream, crate::ErrorCode::CANCEL);
            self.shared.wake_driver();
        }
    }
}
