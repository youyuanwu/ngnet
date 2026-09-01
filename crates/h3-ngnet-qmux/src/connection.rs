use std::sync::PoisonError;
use std::task::{Context, Poll};

use bytes::Bytes;
use h3::error::Code;
use h3::quic::{self, ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::state::{AcceptKind, Effects, OpenKind};
use crate::stream::{BidiStream, RecvStream, SendStream, Shared, apply_effects, drive};

/// Hyperium's connection handle over one shared QMux core.
pub struct Connection<S: AsyncByteStream, C: Clock> {
    pub(crate) shared: Shared<S, C>,
    opener_id: u64,
}

impl<S: AsyncByteStream, C: Clock> Connection<S, C> {
    pub(crate) fn new(shared: Shared<S, C>, opener_id: u64) -> Self {
        Self { shared, opener_id }
    }

    fn poll_open_parts(
        shared: &Shared<S, C>,
        opener_id: u64,
        kind: OpenKind,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Opened<S, C>, StreamErrorIncoming>> {
        drive(shared);
        let mut effects = Effects::default();
        let opened = shared.with_core(|core| core.open(opener_id, kind, cx.waker(), &mut effects));
        apply_effects(&shared.lower_wake, effects);
        match opened {
            Err(terminal) => Poll::Ready(Err(terminal.stream_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => {
                shared.lower_wake.request_driver();
                let send = SendStream::new(shared.clone(), stream_id);
                if kind == OpenKind::Uni {
                    Poll::Ready(Ok(Opened::Uni(send)))
                } else {
                    let recv = RecvStream::new(shared.clone(), stream_id);
                    Poll::Ready(Ok(Opened::Bidi(BidiStream { send, recv })))
                }
            }
        }
    }

    fn close_inner(&mut self, code: Code, reason: &[u8]) {
        Self::close_parts(&self.shared, code, reason);
    }

    fn close_parts(shared: &Shared<S, C>, code: Code, reason: &[u8]) {
        let effects = shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .close(code.value(), reason);
        apply_effects(&shared.lower_wake, effects);
        shared.lower_wake.request_driver();
    }
}

enum Opened<S: AsyncByteStream, C: Clock> {
    Uni(SendStream<S, C>),
    Bidi(BidiStream<S, C>),
}

impl<S: AsyncByteStream, C: Clock> quic::Connection<Bytes> for Connection<S, C> {
    type RecvStream = RecvStream<S, C>;
    type OpenStreams = OpenStreams<S, C>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionErrorIncoming>> {
        drive(&self.shared);
        let mut effects = Effects::default();
        let accepted = {
            let mut core = self
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            core.accept(AcceptKind::Uni, cx.waker(), &mut effects)
        };
        apply_effects(&self.shared.lower_wake, effects);
        match accepted {
            Err(terminal) => Poll::Ready(Err(terminal.connection_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => Poll::Ready(Ok(RecvStream::new(self.shared.clone(), stream_id))),
        }
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, ConnectionErrorIncoming>> {
        drive(&self.shared);
        let mut effects = Effects::default();
        let accepted = {
            let mut core = self
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            core.accept(AcceptKind::Bidi, cx.waker(), &mut effects)
        };
        apply_effects(&self.shared.lower_wake, effects);
        match accepted {
            Err(terminal) => Poll::Ready(Err(terminal.connection_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => {
                let send = SendStream::new(self.shared.clone(), stream_id);
                let recv = RecvStream::new(self.shared.clone(), stream_id);
                Poll::Ready(Ok(BidiStream { send, recv }))
            }
        }
    }

    fn opener(&self) -> Self::OpenStreams {
        let opener_id = self
            .shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allocate_opener();
        OpenStreams::new(self.shared.clone(), opener_id)
    }
}

impl<S: AsyncByteStream, C: Clock> quic::OpenStreams<Bytes> for Connection<S, C> {
    type BidiStream = BidiStream<S, C>;
    type SendStream = SendStream<S, C>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        match Self::poll_open_parts(&self.shared, self.opener_id, OpenKind::Bidi, cx) {
            Poll::Ready(Ok(Opened::Bidi(stream))) => Poll::Ready(Ok(stream)),
            Poll::Ready(Ok(Opened::Uni(_))) => unreachable!("bidi open returned uni"),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        match Self::poll_open_parts(&self.shared, self.opener_id, OpenKind::Uni, cx) {
            Poll::Ready(Ok(Opened::Uni(stream))) => Poll::Ready(Ok(stream)),
            Poll::Ready(Ok(Opened::Bidi(_))) => unreachable!("uni open returned bidi"),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn close(&mut self, code: Code, reason: &[u8]) {
        self.close_inner(code, reason);
    }
}

impl<S: AsyncByteStream, C: Clock> Drop for Connection<S, C> {
    fn drop(&mut self) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove_opener(self.opener_id);
    }
}

/// A cloneable producer of locally initiated H3 streams.
pub struct OpenStreams<S: AsyncByteStream, C: Clock> {
    shared: Shared<S, C>,
    opener_id: u64,
}

impl<S: AsyncByteStream, C: Clock> OpenStreams<S, C> {
    fn new(shared: Shared<S, C>, opener_id: u64) -> Self {
        Self { shared, opener_id }
    }
}

impl<S: AsyncByteStream, C: Clock> Clone for OpenStreams<S, C> {
    fn clone(&self) -> Self {
        let opener_id = self
            .shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allocate_opener();
        Self::new(self.shared.clone(), opener_id)
    }
}

impl<S: AsyncByteStream, C: Clock> quic::OpenStreams<Bytes> for OpenStreams<S, C> {
    type BidiStream = BidiStream<S, C>;
    type SendStream = SendStream<S, C>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        match Connection::poll_open_parts(&self.shared, self.opener_id, OpenKind::Bidi, cx) {
            Poll::Ready(Ok(Opened::Bidi(stream))) => Poll::Ready(Ok(stream)),
            Poll::Ready(Ok(Opened::Uni(_))) => unreachable!("bidi open returned uni"),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        match Connection::poll_open_parts(&self.shared, self.opener_id, OpenKind::Uni, cx) {
            Poll::Ready(Ok(Opened::Uni(stream))) => Poll::Ready(Ok(stream)),
            Poll::Ready(Ok(Opened::Bidi(_))) => unreachable!("uni open returned bidi"),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn close(&mut self, code: Code, reason: &[u8]) {
        Connection::close_parts(&self.shared, code, reason);
    }
}

impl<S: AsyncByteStream, C: Clock> Drop for OpenStreams<S, C> {
    fn drop(&mut self) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove_opener(self.opener_id);
    }
}
