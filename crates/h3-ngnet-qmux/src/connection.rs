use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use bytes::Buf;
use h3::error::Code;
use h3::quic::{self, ConnectionErrorIncoming, StreamErrorIncoming};
use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::state::{AcceptKind, Effects, OpenKind};
use crate::stream::{BidiStream, RecvStream, SendSlots, SendStream, Shared, apply_effects, drive};

/// Hyperium's connection handle over one shared QMux core.
pub struct Connection<S: AsyncByteStream, C: Clock, B: Buf> {
    pub(crate) shared: Shared<S, C>,
    pub(crate) slots: Arc<Mutex<SendSlots<B>>>,
    opener_id: u64,
    _body: PhantomData<fn() -> B>,
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Connection<S, C, B> {
    pub(crate) fn new(
        shared: Shared<S, C>,
        slots: Arc<Mutex<SendSlots<B>>>,
        opener_id: u64,
    ) -> Self {
        Self {
            shared,
            slots,
            opener_id,
            _body: PhantomData,
        }
    }

    /// A bounded snapshot of adapter-owned state for deterministic resource assertions.
    #[doc(hidden)]
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        snapshot(&self.shared, &self.slots)
    }

    /// Returns a cloneable read-only handle to the same bounded-state snapshot.
    #[doc(hidden)]
    #[must_use]
    pub fn observer(&self) -> Observer<S, C, B> {
        Observer {
            shared: self.shared.clone(),
            slots: Arc::clone(&self.slots),
        }
    }

    fn poll_open_parts(
        shared: &Shared<S, C>,
        slots: &Arc<Mutex<SendSlots<B>>>,
        opener_id: u64,
        kind: OpenKind,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Opened<S, C, B>, StreamErrorIncoming>> {
        drive(shared, slots);
        let mut effects = Effects::default();
        let opened = shared.with_core(|core| core.open(opener_id, kind, cx.waker(), &mut effects));
        apply_effects(&shared.lower_wake, slots, effects);
        match opened {
            Err(terminal) => Poll::Ready(Err(terminal.stream_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => {
                shared.lower_wake.request_driver();
                let send = SendStream::new(shared.clone(), Arc::clone(slots), stream_id);
                if kind == OpenKind::Uni {
                    Poll::Ready(Ok(Opened::Uni(send)))
                } else {
                    let recv = RecvStream::new(shared.clone(), Arc::clone(slots), stream_id);
                    Poll::Ready(Ok(Opened::Bidi(BidiStream { send, recv })))
                }
            }
        }
    }

    fn close_inner(&mut self, code: Code, reason: &[u8]) {
        Self::close_parts(&self.shared, &self.slots, code, reason);
    }

    fn close_parts(
        shared: &Shared<S, C>,
        slots: &Arc<Mutex<SendSlots<B>>>,
        code: Code,
        reason: &[u8],
    ) {
        let effects = shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .close(code.value(), reason);
        apply_effects(&shared.lower_wake, slots, effects);
        shared.lower_wake.request_driver();
    }
}

enum Opened<S: AsyncByteStream, C: Clock, B: Buf> {
    Uni(SendStream<S, C, B>),
    Bidi(BidiStream<S, C, B>),
}

impl<S: AsyncByteStream, C: Clock, B: Buf> quic::Connection<B> for Connection<S, C, B> {
    type RecvStream = RecvStream<S, C, B>;
    type OpenStreams = OpenStreams<S, C, B>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionErrorIncoming>> {
        drive(&self.shared, &self.slots);
        let mut effects = Effects::default();
        let accepted = {
            let mut core = self
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            core.accept(AcceptKind::Uni, cx.waker(), &mut effects)
        };
        apply_effects(&self.shared.lower_wake, &self.slots, effects);
        match accepted {
            Err(terminal) => Poll::Ready(Err(terminal.connection_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => Poll::Ready(Ok(RecvStream::new(
                self.shared.clone(),
                Arc::clone(&self.slots),
                stream_id,
            ))),
        }
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, ConnectionErrorIncoming>> {
        drive(&self.shared, &self.slots);
        let mut effects = Effects::default();
        let accepted = {
            let mut core = self
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            core.accept(AcceptKind::Bidi, cx.waker(), &mut effects)
        };
        apply_effects(&self.shared.lower_wake, &self.slots, effects);
        match accepted {
            Err(terminal) => Poll::Ready(Err(terminal.connection_error())),
            Ok(None) => Poll::Pending,
            Ok(Some(stream_id)) => {
                let send = SendStream::new(self.shared.clone(), Arc::clone(&self.slots), stream_id);
                let recv = RecvStream::new(self.shared.clone(), Arc::clone(&self.slots), stream_id);
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
        OpenStreams::new(self.shared.clone(), Arc::clone(&self.slots), opener_id)
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> quic::OpenStreams<B> for Connection<S, C, B> {
    type BidiStream = BidiStream<S, C, B>;
    type SendStream = SendStream<S, C, B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        match Self::poll_open_parts(
            &self.shared,
            &self.slots,
            self.opener_id,
            OpenKind::Bidi,
            cx,
        ) {
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
        match Self::poll_open_parts(&self.shared, &self.slots, self.opener_id, OpenKind::Uni, cx) {
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

impl<S: AsyncByteStream, C: Clock, B: Buf> Drop for Connection<S, C, B> {
    fn drop(&mut self) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove_opener(self.opener_id);
    }
}

/// A cloneable producer of locally initiated H3 streams.
pub struct OpenStreams<S: AsyncByteStream, C: Clock, B: Buf> {
    shared: Shared<S, C>,
    slots: Arc<Mutex<SendSlots<B>>>,
    opener_id: u64,
    _body: PhantomData<fn() -> B>,
}

impl<S: AsyncByteStream, C: Clock, B: Buf> OpenStreams<S, C, B> {
    fn new(shared: Shared<S, C>, slots: Arc<Mutex<SendSlots<B>>>, opener_id: u64) -> Self {
        Self {
            shared,
            slots,
            opener_id,
            _body: PhantomData,
        }
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Clone for OpenStreams<S, C, B> {
    fn clone(&self) -> Self {
        let opener_id = self
            .shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allocate_opener();
        Self::new(self.shared.clone(), Arc::clone(&self.slots), opener_id)
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> quic::OpenStreams<B> for OpenStreams<S, C, B> {
    type BidiStream = BidiStream<S, C, B>;
    type SendStream = SendStream<S, C, B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        match Connection::poll_open_parts(
            &self.shared,
            &self.slots,
            self.opener_id,
            OpenKind::Bidi,
            cx,
        ) {
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
        match Connection::poll_open_parts(
            &self.shared,
            &self.slots,
            self.opener_id,
            OpenKind::Uni,
            cx,
        ) {
            Poll::Ready(Ok(Opened::Uni(stream))) => Poll::Ready(Ok(stream)),
            Poll::Ready(Ok(Opened::Bidi(_))) => unreachable!("uni open returned bidi"),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn close(&mut self, code: Code, reason: &[u8]) {
        Connection::close_parts(&self.shared, &self.slots, code, reason);
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Drop for OpenStreams<S, C, B> {
    fn drop(&mut self) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove_opener(self.opener_id);
    }
}

fn snapshot<S: AsyncByteStream, C: Clock, B: Buf>(
    shared: &Shared<S, C>,
    send_slots: &Arc<Mutex<SendSlots<B>>>,
) -> Snapshot {
    let core = shared.core.lock().unwrap_or_else(PoisonError::into_inner);
    let slots = send_slots.lock().unwrap_or_else(PoisonError::into_inner);
    Snapshot {
        streams: core.streams.len(),
        pending_accepts: core
            .streams
            .values()
            .filter(|state| state.pending_accept)
            .count(),
        receive_bytes: core
            .streams
            .values()
            .flat_map(|state| state.recv.iter())
            .map(|item| item.data.len())
            .sum(),
        receive_terminals: core
            .streams
            .values()
            .filter(|state| state.recv_terminal.is_some())
            .count(),
        retained_send_bytes: slots.retained_bytes(),
        retained_send_high_water: slots.high_water(),
        lower_queued_output: core.lower.queued_output(),
        #[cfg(debug_assertions)]
        routed_events: core.routed_events,
    }
}

/// Cloneable read-only adapter state observer.
#[doc(hidden)]
pub struct Observer<S: AsyncByteStream, C: Clock, B: Buf> {
    shared: Shared<S, C>,
    slots: Arc<Mutex<SendSlots<B>>>,
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Clone for Observer<S, C, B> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            slots: Arc::clone(&self.slots),
        }
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Observer<S, C, B> {
    /// Returns the current bounded-state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        snapshot(&self.shared, &self.slots)
    }
}

/// Resource counters exposed only as a deterministic test seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Active plus pending-accepted stream entries.
    pub streams: usize,
    /// Streams waiting for an H3 accept poll.
    pub pending_accepts: usize,
    /// Routed receive payload not yet handed to H3.
    pub receive_bytes: usize,
    /// Stream entries carrying a stable receive terminal.
    pub receive_terminals: usize,
    /// Logical framed-send bytes currently retained.
    pub retained_send_bytes: usize,
    /// Maximum logical framed-send bytes retained at once.
    pub retained_send_high_water: usize,
    /// QMux's bounded produced-output buffer.
    pub lower_queued_output: usize,
    /// Total events routed in debug builds.
    #[cfg(debug_assertions)]
    pub routed_events: u64,
}
