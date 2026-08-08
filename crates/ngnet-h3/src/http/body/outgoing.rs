//! Adapting a caller's [`http_body::Body`] into something the state machine can pull from.
//!
//! # Why chunks must be `Bytes`
//!
//! nghttp3 has no copying data source. Whatever this hands over is borrowed and read through
//! until the transport reports it released, so the adapter must give it a buffer that stays
//! put and stays alive. A `Bytes` already is one, and
//! [`RetainedBytes::from_owner`](crate::RetainedBytes::from_owner) retains it without a copy.
//! A generic `B::Data` would have to be copied into an owned buffer first, which is exactly
//! the cost the requirement avoids.
//!
//! # Two kinds of "not now", which must not be confused
//!
//! A body with nothing available *yet* defers, and the stream is resumed when its waker
//! fires. A transport refusing bytes blocks, and the stream is unblocked when the transport
//! can take more. They look alike and are not: treating a deferral as congestion means
//! waiting for a transport that is perfectly willing, and treating congestion as a deferral
//! means waiting for a body that has already spoken. Only the first is handled here; the
//! second belongs to the driver.

use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use http_body::Body;

use super::super::shared::Shared;
use crate::body::{BodyOutcome, BodySource, RetainedBytes};
use crate::stream::StreamId;

/// What a finished body left behind.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum Ending {
    /// The body ended with nothing more to say.
    Clean,
    /// The body ended and left a trailing field section to submit.
    Trailers(http::HeaderMap),
    /// The body failed.
    ///
    /// Deliberately *not* reported to the state machine as a body failure: that signal is
    /// connection-fatal, and one caller's file read going wrong must not take down every
    /// other exchange sharing the connection. The driver ends the body here and resets this
    /// stream instead.
    Failed,
}

/// A caller's body, wrapped so the state machine can pull from it.
pub(crate) struct Outgoing<B> {
    body: Pin<Box<B>>,
    /// Woken when the body has more to give after deferring.
    waker: Waker,
    /// Set once the body has finished, and read by the driver afterwards.
    ending: Arc<std::sync::Mutex<Option<Ending>>>,
    finished: bool,
}

impl<B> Outgoing<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    pub(crate) fn new(
        body: B,
        stream: StreamId,
        shared: Arc<Shared>,
        ending: Arc<std::sync::Mutex<Option<Ending>>>,
    ) -> Self {
        Self {
            body: Box::pin(body),
            waker: Waker::from(Arc::new(BodyWaker { stream, shared })),
            ending,
            finished: false,
        }
    }

    fn finish(&mut self, ending: Ending) {
        self.finished = true;
        if let Ok(mut slot) = self.ending.lock() {
            *slot = Some(ending);
        }
    }
}

impl<B> BodySource for Outgoing<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    fn next(&mut self) -> BodyOutcome {
        if self.finished {
            return BodyOutcome::Eof(Vec::new());
        }

        let mut context = Context::from_waker(&self.waker);
        let mut pieces: Vec<RetainedBytes> = Vec::new();

        loop {
            match self.body.as_mut().poll_frame(&mut context) {
                Poll::Pending => {
                    if pieces.is_empty() {
                        // Nothing now, and the body will wake the driver when there is.
                        // Distinct from congestion; see the module documentation.
                        return BodyOutcome::Defer;
                    }
                    return BodyOutcome::Wrote(pieces);
                }
                Poll::Ready(None) => {
                    self.finish(Ending::Clean);
                    return BodyOutcome::Eof(pieces);
                }
                Poll::Ready(Some(Err(_))) => {
                    // A caller's body failing must not poison the connection, so the body
                    // simply ends here and the driver resets this one stream.
                    self.finish(Ending::Failed);
                    return BodyOutcome::Eof(pieces);
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(mut data) => {
                        if data.has_remaining() {
                            let chunk = data.copy_to_bytes(data.remaining());
                            // The whole point: retained without a copy, so the bytes
                            // nghttp3 reads through are the caller's own allocation.
                            pieces.push(RetainedBytes::from_owner(chunk));
                        }
                    }
                    Err(frame) => match frame.into_trailers() {
                        Ok(trailers) => {
                            self.finish(Ending::Trailers(trailers));
                            return BodyOutcome::EofWithTrailers(pieces);
                        }
                        Err(_) => {
                            // A frame that is neither data nor trailers is something this
                            // layer does not carry; ignoring it is safer than guessing.
                        }
                    },
                },
            }
        }
    }
}

/// Wakes the driver when a deferred body has more to give.
///
/// Naming the stream is what lets the driver resume exactly that one rather than re-polling
/// every body it holds.
struct BodyWaker {
    stream: StreamId,
    shared: Arc<Shared>,
}

impl std::task::Wake for BodyWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.shared.mark_ready(self.stream) {
            self.shared.wake_driver();
        }
    }
}
