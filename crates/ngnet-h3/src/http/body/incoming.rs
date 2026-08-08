//! A message body received from the peer.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

use super::super::error::Error;
use super::super::shared::{Incoming, Received, Shared};
use crate::error::ErrorCode;
use crate::stream::StreamId;

/// A body arriving from the peer.
///
/// Implements [`http_body::Body`], so it composes with the ecosystem. Chunks are refcounted
/// views of the buffers the QUIC backend produced wherever that is possible, so reading
/// costs no copy in the common case.
///
/// # Reading is what returns flow control
///
/// The state machine deliberately does not credit body payload — it reports only framing
/// overhead — because only the application knows when it has finished with the bytes. This
/// type credits them as they are handed out. A caller that holds a body without reading it
/// therefore stops the peer sending more, which is the point: it is backpressure, not a
/// stall.
pub struct IncomingBody {
    stream: StreamId,
    /// Whether dropping this unread should abandon the exchange.
    ///
    /// True for a response, which is the only body a client reads: giving up on it means
    /// giving up on the exchange, so the peer is told to stop producing.
    ///
    /// It will be false for a server's *request* body, and that asymmetry is not cosmetic —
    /// a handler that ignores the body it was given still owes a response, so abandoning
    /// the stream would destroy an exchange that is very much alive. The server role does
    /// not exist yet; when it does, this is the flag it sets differently.
    abandon_on_drop: bool,
    incoming: Arc<Incoming>,
    shared: Arc<Shared>,
    /// Kept so a waker naming this stream goes inert once the exchange is forgotten.
    _liveness: Arc<()>,
}

impl IncomingBody {
    pub(crate) fn new(
        stream: StreamId,
        abandon_on_drop: bool,
        incoming: Arc<Incoming>,
        shared: Arc<Shared>,
        liveness: Arc<()>,
    ) -> Self {
        Self {
            stream,
            abandon_on_drop,
            incoming,
            shared,
            _liveness: liveness,
        }
    }

    /// Returns flow-control credit for bytes the caller has finished with.
    fn credit(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.shared.credit(self.stream, bytes);
        self.shared.wake_driver();
    }
}

impl Body for IncomingBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        match self.incoming.poll(context.waker()) {
            Received::Data(chunk) => {
                // Credited as it is handed over, not as it arrived: the peer may send this
                // much again once the caller has it.
                self.credit(chunk.len() as u64);
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Received::Trailers(trailers) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Received::Finished => Poll::Ready(None),
            Received::Failed(error) => Poll::Ready(Some(Err(error))),
            Received::Nothing => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.incoming.is_finished()
    }

    fn size_hint(&self) -> SizeHint {
        // HTTP/3 carries no length nghttp3 exposes here, and `content-length` is the peer's
        // claim rather than a fact. Reporting it as a bound would let a lying peer shape an
        // allocation.
        SizeHint::default()
    }
}

impl core::fmt::Debug for IncomingBody {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Nothing about the bytes: a body's contents are the caller's, and printing them in
        // a panic message is how a secret ends up in a log.
        f.debug_struct("IncomingBody")
            .field("stream", &self.stream)
            .field("abandon_on_drop", &self.abandon_on_drop)
            .field("complete", &self.incoming.is_finished())
            .finish()
    }
}

impl Drop for IncomingBody {
    fn drop(&mut self) {
        let complete = self.incoming.is_finished();
        let unread = self.incoming.abandon();
        // Always credited back, complete or not: those bytes cost the peer window it would
        // otherwise never get back.
        self.credit(unread);

        // See `abandon_on_drop`: a response abandons, a request will not.
        if self.abandon_on_drop && !complete {
            self.shared.reset(self.stream, ErrorCode::new(0x10c));
            self.shared.wake_driver();
        }
    }
}
