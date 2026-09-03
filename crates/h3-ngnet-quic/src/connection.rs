//! The connection-level trait implementations.
//!
//! Thin by design: every method takes the shared core, runs one pump pass, and answers from
//! the state that pass produced. Nothing here holds transport knowledge that `pump.rs` does
//! not already own.

use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use h3::quic::{self, ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_quic::{ApplicationErrorCode, Session, StreamId};

use crate::core::Core;
use crate::error::ConnectionTerminal;
use crate::pump;
use crate::stream::{BidiStream, RecvStream, SendStream, Shared};

/// Which kind of stream an open is asking for.
#[derive(Clone, Copy)]
enum Open {
    Bidi,
    Uni,
}

/// A cloneable capability to open streams and close the connection.
///
/// Hyperium takes one of these from the connection with [`quic::Connection::opener`] and may
/// clone it freely, so it holds nothing but a handle onto the shared core.
pub struct OpenStreams<S: Session> {
    shared: Shared<S>,
}

impl<S: Session> Clone for OpenStreams<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<S: Session> OpenStreams<S> {
    pub(crate) fn new(shared: Shared<S>) -> Self {
        Self { shared }
    }
}

/// Opens a stream, parking until the peer raises this endpoint's allowance if it is exhausted.
///
/// A refused open is reported as a temporary block, and `Observed::StreamsExtended` is the
/// only signal that the block has lifted — so this must park rather than fail, or a caller
/// that hits the stream limit would see a spurious error instead of backpressure.
fn poll_open<S: Session>(
    shared: &Shared<S>,
    kind: Open,
    cx: &mut Context<'_>,
) -> Poll<Result<StreamId, StreamErrorIncoming>> {
    // Cloned once so the closure below does not borrow `cx`, which `pump` needs mutably.
    let waker = cx.waker().clone();
    let outcome = shared.pump(cx, |core| {
        if let Some(terminal) = &core.terminal {
            return Err(Some(terminal.stream_error()));
        }
        let opened = match kind {
            Open::Bidi => core.detached.conn.open_bidi_stream(),
            Open::Uni => core.detached.conn.open_uni_stream(),
        };
        match opened {
            Ok(stream) => {
                core.state(stream);
                // The stream's first frames leave with the next datagram.
                pump::produce(core, None);
                Ok(stream)
            }
            Err(err) => {
                // Distinguish "no stream credit left" — a temporary block that only
                // `Observed::StreamsExtended` lifts — from a real failure. Parking on a
                // permanent error would wait forever for a signal that is never coming.
                let blocked = match kind {
                    Open::Bidi => core.detached.conn.streams_bidi_left() == 0,
                    Open::Uni => core.detached.conn.streams_uni_left() == 0,
                };
                if blocked {
                    // Registered while the core is still held. `Observed::StreamsExtended`
                    // is the only thing that lifts this block, and it is routed by whichever
                    // task happens to pump -- so a wake delivered between releasing the core
                    // and reaching the registry would find nobody and park this task for
                    // good.
                    shared.wakers.register_connection(&waker);
                    Err(None)
                } else {
                    let terminal = ConnectionTerminal::undefined(format!(
                        "the transport refused to open a stream: {err}"
                    ));
                    core.fail(terminal.clone());
                    Err(Some(terminal.stream_error()))
                }
            }
        }
    });
    match outcome {
        Ok(stream) => Poll::Ready(Ok(stream)),
        Err(Some(err)) => Poll::Ready(Err(err)),
        Err(None) => Poll::Pending,
    }
}

/// Closes the connection with an application code.
///
/// Synchronous, and honoured synchronously: the CONNECTION_CLOSE datagram goes into the
/// outbound queue's reserved final slot and the endpoint task — which owns the socket and is
/// always running — sends it. That is why this crate can implement a synchronous close
/// without a driver future of its own.
fn close_connection<S: Session>(shared: &Shared<S>, code: u64, reason: &[u8]) {
    let mut core = shared.lock();
    if core.terminal.is_some() {
        return;
    }
    let now = core.detached.now();
    let mut datagram = vec![0u8; crate::core::MAX_DATAGRAM];
    let written = core.detached.conn.write_connection_close(
        &mut datagram,
        ApplicationErrorCode::new(code),
        reason,
        now,
    );
    if let Ok(len) = written
        && len > 0
    {
        datagram.truncate(len);
        core.detached.send_close(datagram);
    }
    core.fail(ConnectionTerminal::Application(code));
    pump::release(&mut core);
    drop(core);
    shared.wakers.wake_all();
}

fn poll_accept<S: Session>(
    shared: &Shared<S>,
    kind: Open,
    cx: &mut Context<'_>,
) -> Poll<Result<Option<StreamId>, ConnectionErrorIncoming>> {
    // Cloned once so the closure below does not borrow `cx`, which `pump` needs mutably.
    let waker = cx.waker().clone();
    let outcome = shared.pump(cx, |core| {
        let queued = match kind {
            Open::Bidi => core.accept_bidi.pop_front(),
            Open::Uni => core.accept_uni.pop_front(),
        };
        if let Some(stream) = queued {
            // Give the peer room to open another in its place.
            match kind {
                Open::Bidi => core.detached.conn.extend_max_streams_bidi(1),
                Open::Uni => core.detached.conn.extend_max_streams_uni(1),
            }
            pump::produce(core, None);
            return Ok(Some(stream));
        }
        if let Some(terminal) = &core.terminal {
            return Err(terminal.connection_error());
        }
        // Under the core lock: the HTTP/3 driver spends nearly all its life parked here, and
        // a peer-opened stream routed by another task between the check and the registration
        // would wake an empty list and leave the driver asleep on a queued request.
        shared.wakers.register_connection(&waker);
        Ok(None)
    });
    match outcome {
        Ok(Some(stream)) => Poll::Ready(Ok(Some(stream))),
        Ok(None) => Poll::Pending,
        Err(err) => Poll::Ready(Err(err)),
    }
}

impl<S: Session> core::fmt::Debug for OpenStreams<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OpenStreams").finish_non_exhaustive()
    }
}

impl<S: Session> quic::OpenStreams<Bytes> for OpenStreams<S> {
    type BidiStream = BidiStream<S>;
    type SendStream = SendStream<S>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        poll_open(&self.shared, Open::Bidi, cx)
            .map_ok(|stream| BidiStream::new(self.shared.clone(), stream))
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        poll_open(&self.shared, Open::Uni, cx)
            .map_ok(|stream| SendStream::new(self.shared.clone(), stream))
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        close_connection(&self.shared, code.value(), reason);
    }
}

/// An established `ngnet-quic` connection, presented to hyperium H3.
///
/// Obtained from [`from_detached`](crate::from_detached). Hyperium takes this by value and
/// drives everything through it; the caller's remaining obligation is the one it already had
/// — keep the endpoint's own driver running.
pub struct Connection<S: Session> {
    shared: Shared<S>,
}

impl<S: Session> Connection<S> {
    pub(crate) fn new(
        core: Arc<std::sync::Mutex<Core<S>>>,
        wakers: Arc<crate::core::Wakers>,
    ) -> Self {
        Self {
            shared: Shared { core, wakers },
        }
    }

    /// Why the connection ended, if it has.
    ///
    /// Hyperium reports the same event in its own vocabulary; this is here for a caller that
    /// wants the transport's classification, in particular to tell an idle timeout apart from
    /// a peer that closed deliberately.
    #[must_use]
    pub fn failure(&self) -> Option<crate::Error> {
        self.shared
            .lock()
            .terminal
            .as_ref()
            .map(ConnectionTerminal::error)
    }
}

impl<S: Session> core::fmt::Debug for Connection<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("failure", &self.failure())
            .finish_non_exhaustive()
    }
}

impl<S: Session> quic::OpenStreams<Bytes> for Connection<S> {
    type BidiStream = BidiStream<S>;
    type SendStream = SendStream<S>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        poll_open(&self.shared, Open::Bidi, cx)
            .map_ok(|stream| BidiStream::new(self.shared.clone(), stream))
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        poll_open(&self.shared, Open::Uni, cx)
            .map_ok(|stream| SendStream::new(self.shared.clone(), stream))
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        close_connection(&self.shared, code.value(), reason);
    }
}

impl<S: Session> quic::Connection<Bytes> for Connection<S> {
    type RecvStream = RecvStream<S>;
    type OpenStreams = OpenStreams<S>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionErrorIncoming>> {
        match poll_accept(&self.shared, Open::Uni, cx) {
            Poll::Ready(Ok(Some(stream))) => {
                Poll::Ready(Ok(RecvStream::new(self.shared.clone(), stream)))
            }
            Poll::Ready(Ok(None)) | Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, ConnectionErrorIncoming>> {
        match poll_accept(&self.shared, Open::Bidi, cx) {
            Poll::Ready(Ok(Some(stream))) => {
                Poll::Ready(Ok(BidiStream::new(self.shared.clone(), stream)))
            }
            Poll::Ready(Ok(None)) | Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    fn opener(&self) -> Self::OpenStreams {
        OpenStreams::new(self.shared.clone())
    }
}

impl<S: Session> Drop for Connection<S> {
    fn drop(&mut self) {
        let mut core = self.shared.lock();
        pump::release(&mut core);
    }
}
