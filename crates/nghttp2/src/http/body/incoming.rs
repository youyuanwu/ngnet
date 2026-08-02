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

/// The body of a received message.
///
/// Yields data frames in the order they arrived, then a trailers frame if the peer sent
/// one. Reading it is what gives the peer room to send more.
///
/// Dropping it discards whatever has not been read and returns that capacity to the peer,
/// so an unwanted body never becomes a stalled connection.
#[derive(Debug)]
pub struct IncomingBody {
    stream: i32,
    incoming: Arc<Incoming>,
    shared: Arc<Shared>,
}

impl IncomingBody {
    pub(crate) const fn new(stream: i32, incoming: Arc<Incoming>, shared: Arc<Shared>) -> Self {
        Self {
            stream,
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
        let unread = self.incoming.abandon();
        self.credit(unread);
    }
}
