use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use h3::quic::{self, StreamErrorIncoming, WriteBuf};
use ngnet_qmux::StreamId;
use ngnet_qmux::io::{AsyncByteStream, Clock, OUTBOUND_CARRY, StreamWrite};

use crate::error::{ConnectionTerminal, DirectionTerminal};
use crate::state::{ABANDONED, Core, Effects, LowerWake};

const SEND_PROGRESS_BUDGET: usize = 64;

pub(crate) struct Shared<S: AsyncByteStream, C: Clock> {
    pub(crate) core: Arc<Mutex<Core<S, C>>>,
    pub(crate) lower_wake: Arc<LowerWake>,
}

impl<S: AsyncByteStream, C: Clock> Clone for Shared<S, C> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            lower_wake: Arc::clone(&self.lower_wake),
        }
    }
}

impl<S: AsyncByteStream, C: Clock> Shared<S, C> {
    pub(crate) fn with_core<R>(&self, operation: impl FnOnce(&mut Core<S, C>) -> R) -> R {
        self.lower_wake.begin_defer();
        let result = {
            let mut core = self.core.lock().unwrap_or_else(PoisonError::into_inner);
            operation(&mut core)
        };
        self.lower_wake.end_defer();
        result
    }
}

pub(crate) fn apply_effects(lower_wake: &Arc<LowerWake>, effects: Effects) {
    #[cfg(feature = "diagnostics")]
    crate::diagnostics::wakes(effects.wakes.len());
    for waker in effects.wakes {
        waker.wake();
    }
    if effects.continuation {
        lower_wake.request_driver();
    }
}

pub(crate) fn drive<S: AsyncByteStream, C: Clock>(shared: &Shared<S, C>) {
    #[cfg(feature = "diagnostics")]
    crate::diagnostics::adapter_poll();
    let effects = shared.with_core(|core| core.drive_turn(&shared.lower_wake));
    apply_effects(&shared.lower_wake, effects);
}

fn h3_id(stream_id: StreamId) -> quic::StreamId {
    (stream_id.get() as u64)
        .try_into()
        .expect("a QMux stream id is an H3 stream id")
}

fn direction_error(terminal: DirectionTerminal) -> StreamErrorIncoming {
    match terminal {
        DirectionTerminal::Stopped(code) | DirectionTerminal::Reset(code) => {
            StreamErrorIncoming::StreamTerminated { error_code: code }
        }
        DirectionTerminal::Finished => ConnectionTerminal::Internal(
            "operation attempted after the stream direction finished".into(),
        )
        .stream_error(),
        DirectionTerminal::Closed => StreamErrorIncoming::Unknown(Box::new(
            crate::Error::undefined("QMux closed the stream direction"),
        )),
    }
}

fn closed_stream_error<S: AsyncByteStream, C: Clock>(
    core: &mut Core<S, C>,
    stream_id: StreamId,
    effects: &mut Effects,
) -> StreamErrorIncoming {
    match core.reconcile_closed_send(stream_id, effects) {
        Ok(terminal) => direction_error(terminal),
        Err(terminal) => terminal.stream_error(),
    }
}

fn waiting_for_output<S: AsyncByteStream, C: Clock>(core: &Core<S, C>) -> bool {
    core.lower.queued_output() >= OUTBOUND_CARRY
}

/// A QMux-backed H3 sending stream.
pub struct SendStream<S: AsyncByteStream, C: Clock> {
    pub(crate) shared: Shared<S, C>,
    pub(crate) stream_id: StreamId,
}

impl<S: AsyncByteStream, C: Clock> SendStream<S, C> {
    pub(crate) fn new(shared: Shared<S, C>, stream_id: StreamId) -> Self {
        Self { shared, stream_id }
    }

    fn terminal(&self) -> Option<StreamErrorIncoming> {
        let core = self
            .shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(terminal) = &core.terminal {
            return Some(terminal.stream_error());
        }
        core.stream_error(self.stream_id, true).map(direction_error)
    }

    fn poll_retained(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        for turn in 0..SEND_PROGRESS_BUDGET {
            let mut effects = Effects::default();
            let step = self.shared.with_core(|core| {
                if let Some(terminal) = &core.terminal {
                    Step::Error(terminal.stream_error())
                } else if let Some(terminal) = core.stream_error(self.stream_id, true) {
                    Step::Error(direction_error(terminal))
                } else {
                    let Some(mut data) = core
                        .streams
                        .get_mut(&self.stream_id)
                        .and_then(|state| state.writing.take())
                    else {
                        return Step::Done;
                    };
                    if data.remaining() == 0 {
                        Step::Done
                    } else {
                        let chunk = data.chunk();
                        if chunk.is_empty() {
                            Step::Error(
                                ConnectionTerminal::Internal(
                                    "Buf returned an empty chunk with bytes remaining".into(),
                                )
                                .stream_error(),
                            )
                        } else {
                            let offered = chunk.len();
                            match core.lower.try_write_stream(self.stream_id, chunk, false) {
                                Ok(StreamWrite::Accepted(accepted)) if accepted > offered => {
                                    Step::Error(
                                        ConnectionTerminal::Internal(
                                            "QMux accepted more bytes than were offered".into(),
                                        )
                                        .stream_error(),
                                    )
                                }
                                Ok(StreamWrite::Accepted(0)) => {
                                    core.streams
                                        .get_mut(&self.stream_id)
                                        .expect("live stream")
                                        .writing = Some(data);
                                    core.park_send(
                                        self.stream_id,
                                        false,
                                        waiting_for_output(core),
                                        cx.waker(),
                                        &mut effects,
                                    );
                                    Step::Pending
                                }
                                Ok(StreamWrite::Accepted(accepted)) => {
                                    data.advance(accepted);
                                    let complete = data.remaining() == 0;
                                    if complete {
                                        #[cfg(feature = "diagnostics")]
                                        crate::diagnostics::send_gauge(core.retained_send_bytes());
                                        Step::Done
                                    } else if accepted < offered {
                                        core.streams
                                            .get_mut(&self.stream_id)
                                            .expect("live stream")
                                            .writing = Some(data);
                                        #[cfg(feature = "diagnostics")]
                                        crate::diagnostics::send_gauge(core.retained_send_bytes());
                                        core.park_send(
                                            self.stream_id,
                                            false,
                                            waiting_for_output(core),
                                            cx.waker(),
                                            &mut effects,
                                        );
                                        Step::Pending
                                    } else {
                                        core.streams
                                            .get_mut(&self.stream_id)
                                            .expect("live stream")
                                            .writing = Some(data);
                                        #[cfg(feature = "diagnostics")]
                                        crate::diagnostics::send_gauge(core.retained_send_bytes());
                                        Step::Progress
                                    }
                                }
                                Ok(StreamWrite::Blocked) => {
                                    core.streams
                                        .get_mut(&self.stream_id)
                                        .expect("live stream")
                                        .writing = Some(data);
                                    core.park_send(
                                        self.stream_id,
                                        false,
                                        waiting_for_output(core),
                                        cx.waker(),
                                        &mut effects,
                                    );
                                    Step::Pending
                                }
                                Ok(StreamWrite::Closed) => Step::Error(closed_stream_error(
                                    core,
                                    self.stream_id,
                                    &mut effects,
                                )),
                                Err(error) => {
                                    let terminal = ConnectionTerminal::from_lower(&error);
                                    effects.merge(core.fail(terminal.clone()));
                                    Step::Error(terminal.stream_error())
                                }
                            }
                        }
                    }
                }
            });
            apply_effects(&self.shared.lower_wake, effects);

            match step {
                Step::Done => return Poll::Ready(Ok(())),
                Step::Pending => {
                    self.shared.lower_wake.request_driver();
                    return Poll::Pending;
                }
                Step::Progress => {
                    self.shared.lower_wake.request_driver();
                    if turn + 1 == SEND_PROGRESS_BUDGET {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Step::Error(error) => return Poll::Ready(Err(error)),
            }
        }
        unreachable!("the bounded loop returns on its final turn")
    }
}

enum Step {
    Done,
    Pending,
    Progress,
    Error(StreamErrorIncoming),
}

impl<S: AsyncByteStream, C: Clock> quic::SendStream<Bytes> for SendStream<S, C> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        drive(&self.shared);
        if let Some(error) = self.terminal() {
            return Poll::Ready(Err(error));
        }
        self.poll_retained(cx)
    }

    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        let mut core = self
            .shared
            .core
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(terminal) = &core.terminal {
            return Err(terminal.stream_error());
        }
        if let Some(terminal) = core.stream_error(self.stream_id, true) {
            return Err(direction_error(terminal));
        }
        let state = core.streams.get_mut(&self.stream_id).expect("live stream");
        if state.writing.is_some() {
            Err(ConnectionTerminal::Internal(
                "send_data called before the previous logical send became ready".into(),
            )
            .stream_error())
        } else {
            state.writing = Some(data.into());
            #[cfg(feature = "diagnostics")]
            {
                crate::diagnostics::send_chunk();
                crate::diagnostics::send_gauge(core.retained_send_bytes());
            }
            Ok(())
        }
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        {
            let core = self
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(terminal) = &core.terminal {
                return Poll::Ready(Err(terminal.stream_error()));
            }
            if core.stream_error(self.stream_id, true) == Some(DirectionTerminal::Finished) {
                return Poll::Ready(Ok(()));
            }
        }
        match self.poll_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }

        let mut effects = Effects::default();
        let result = self.shared.with_core(|core| {
            if let Some(terminal) = &core.terminal {
                Poll::Ready(Err(terminal.stream_error()))
            } else {
                match core.stream_error(self.stream_id, true) {
                    Some(DirectionTerminal::Finished) => Poll::Ready(Ok(())),
                    Some(terminal) => Poll::Ready(Err(direction_error(terminal))),
                    None => match core.lower.try_write_stream(self.stream_id, &[], true) {
                        Ok(StreamWrite::Accepted(0)) => {
                            if let Some(state) = core.streams.get_mut(&self.stream_id) {
                                state.send_terminal = Some(DirectionTerminal::Finished);
                            }
                            effects.continuation = true;
                            Poll::Ready(Ok(()))
                        }
                        Ok(StreamWrite::Blocked) => {
                            core.park_send(
                                self.stream_id,
                                true,
                                waiting_for_output(core),
                                cx.waker(),
                                &mut effects,
                            );
                            effects.continuation = true;
                            Poll::Pending
                        }
                        Ok(StreamWrite::Accepted(_)) => {
                            Poll::Ready(Err(ConnectionTerminal::Internal(
                                "QMux returned an invalid result for an empty FIN".into(),
                            )
                            .stream_error()))
                        }
                        Ok(StreamWrite::Closed) => Poll::Ready(Err(closed_stream_error(
                            core,
                            self.stream_id,
                            &mut effects,
                        ))),
                        Err(error) => {
                            let terminal = ConnectionTerminal::from_lower(&error);
                            effects.merge(core.fail(terminal.clone()));
                            Poll::Ready(Err(terminal.stream_error()))
                        }
                    },
                }
            }
        });
        apply_effects(&self.shared.lower_wake, effects);
        result
    }

    fn reset(&mut self, reset_code: u64) {
        let mut effects = Effects::default();
        self.shared.with_core(|core| {
            if let Some(state) = core.streams.get_mut(&self.stream_id) {
                state.writing = None;
            }
            if let Err(terminal) = core.reset_send(self.stream_id, reset_code) {
                effects.merge(core.fail(terminal));
            }
        });
        apply_effects(&self.shared.lower_wake, effects);
        self.shared.lower_wake.request_driver();
    }

    fn send_id(&self) -> quic::StreamId {
        h3_id(self.stream_id)
    }
}

impl<S: AsyncByteStream, C: Clock> quic::SendStreamUnframed<Bytes> for SendStream<S, C> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        drive(&self.shared);
        if let Some(error) = self.terminal() {
            return Poll::Ready(Err(error));
        }
        if self.shared.with_core(|core| {
            core.streams
                .get(&self.stream_id)
                .is_some_and(|state| state.writing.is_some())
        }) {
            return Poll::Ready(Err(ConnectionTerminal::Internal(
                "unframed send attempted while framed data is retained".into(),
            )
            .stream_error()));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(0));
        }
        let chunk = buf.chunk();
        if chunk.is_empty() {
            return Poll::Ready(Err(ConnectionTerminal::Internal(
                "Buf returned an empty chunk with bytes remaining".into(),
            )
            .stream_error()));
        }
        let offered = chunk.len();
        let mut effects = Effects::default();
        let result = self.shared.with_core(|core| {
            match core.lower.try_write_stream(self.stream_id, chunk, false) {
                Ok(StreamWrite::Accepted(accepted)) if accepted > offered => {
                    Poll::Ready(Err(ConnectionTerminal::Internal(
                        "QMux accepted more bytes than were offered".into(),
                    )
                    .stream_error()))
                }
                Ok(StreamWrite::Accepted(0)) | Ok(StreamWrite::Blocked) => {
                    core.park_send(
                        self.stream_id,
                        false,
                        waiting_for_output(core),
                        cx.waker(),
                        &mut effects,
                    );
                    effects.continuation = true;
                    Poll::Pending
                }
                Ok(StreamWrite::Accepted(accepted)) => {
                    effects.continuation = true;
                    Poll::Ready(Ok(accepted))
                }
                Ok(StreamWrite::Closed) => {
                    Poll::Ready(Err(closed_stream_error(core, self.stream_id, &mut effects)))
                }
                Err(error) => {
                    let terminal = ConnectionTerminal::from_lower(&error);
                    effects.merge(core.fail(terminal.clone()));
                    Poll::Ready(Err(terminal.stream_error()))
                }
            }
        });
        if let Poll::Ready(Ok(accepted)) = &result {
            buf.advance(*accepted);
        }
        apply_effects(&self.shared.lower_wake, effects);
        result
    }
}

impl<S: AsyncByteStream, C: Clock> Drop for SendStream<S, C> {
    fn drop(&mut self) {
        let mut effects = Effects::default();
        self.shared.with_core(|core| {
            if let Some(state) = core.streams.get_mut(&self.stream_id) {
                state.writing = None;
            }
            if core.stream_error(self.stream_id, true).is_none()
                && let Err(terminal) = core.reset_send(self.stream_id, ABANDONED)
            {
                effects.merge(core.fail(terminal));
            }
            core.drop_direction(self.stream_id, true);
        });
        apply_effects(&self.shared.lower_wake, effects);
        self.shared.lower_wake.request_driver();
    }
}

/// A QMux-backed H3 receiving stream.
pub struct RecvStream<S: AsyncByteStream, C: Clock> {
    pub(crate) shared: Shared<S, C>,
    pub(crate) stream_id: StreamId,
}

impl<S: AsyncByteStream, C: Clock> RecvStream<S, C> {
    pub(crate) fn new(shared: Shared<S, C>, stream_id: StreamId) -> Self {
        Self { shared, stream_id }
    }
}

impl<S: AsyncByteStream, C: Clock> quic::RecvStream for RecvStream<S, C> {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        drive(&self.shared);

        let mut effects = Effects::default();
        let result = self.shared.with_core(|core| {
            if let Some(terminal) = &core.terminal {
                Poll::Ready(Err(terminal.stream_error()))
            } else {
                let item = core
                    .streams
                    .get_mut(&self.stream_id)
                    .and_then(|state| state.recv.pop_front());
                #[cfg(feature = "diagnostics")]
                crate::diagnostics::receive_gauge(core.retained_receive_bytes());
                if let Some(item) = item {
                    let bytes = item.data.len() as u64;
                    let credited = core
                        .lower
                        .extend_stream_credit(self.stream_id, bytes)
                        .and_then(|()| core.lower.extend_connection_credit(bytes));
                    if let Err(error) = credited {
                        let terminal = ConnectionTerminal::from_lower(&error);
                        effects.merge(core.fail(terminal.clone()));
                        Poll::Ready(Err(terminal.stream_error()))
                    } else {
                        #[cfg(feature = "diagnostics")]
                        {
                            if bytes != 0 {
                                crate::diagnostics::stream_credit();
                                crate::diagnostics::connection_credit();
                            }
                        }
                        if item.fin
                            && let Some(state) = core.streams.get_mut(&self.stream_id)
                        {
                            state.recv_terminal = Some(DirectionTerminal::Finished);
                        }
                        effects.continuation = true;
                        Poll::Ready(Ok(Some(item.data)))
                    }
                } else {
                    match core.stream_error(self.stream_id, false) {
                        Some(DirectionTerminal::Finished | DirectionTerminal::Stopped(_)) => {
                            Poll::Ready(Ok(None))
                        }
                        Some(DirectionTerminal::Reset(code)) => {
                            Poll::Ready(Err(StreamErrorIncoming::StreamTerminated {
                                error_code: code,
                            }))
                        }
                        Some(DirectionTerminal::Closed) => {
                            Poll::Ready(Err(direction_error(DirectionTerminal::Closed)))
                        }
                        None => {
                            core.park_recv(self.stream_id, cx.waker(), &mut effects);
                            Poll::Pending
                        }
                    }
                }
            }
        });
        apply_effects(&self.shared.lower_wake, effects);
        result
    }

    fn stop_sending(&mut self, error_code: u64) {
        let mut effects = Effects::default();
        self.shared.with_core(|core| {
            if let Err(terminal) = core.discard_receive(self.stream_id, error_code) {
                effects.merge(core.fail(terminal));
            }
        });
        effects.continuation = true;
        apply_effects(&self.shared.lower_wake, effects);
    }

    fn recv_id(&self) -> quic::StreamId {
        h3_id(self.stream_id)
    }
}

impl<S: AsyncByteStream, C: Clock> Drop for RecvStream<S, C> {
    fn drop(&mut self) {
        let mut effects = Effects::default();
        self.shared.with_core(|core| {
            if let Err(terminal) = core.discard_receive(self.stream_id, ABANDONED) {
                effects.merge(core.fail(terminal));
            }
            core.drop_direction(self.stream_id, false);
        });
        effects.continuation = true;
        apply_effects(&self.shared.lower_wake, effects);
    }
}

/// A QMux-backed H3 bidirectional stream.
pub struct BidiStream<S: AsyncByteStream, C: Clock> {
    pub(crate) send: SendStream<S, C>,
    pub(crate) recv: RecvStream<S, C>,
}

impl<S: AsyncByteStream, C: Clock> quic::BidiStream<Bytes> for BidiStream<S, C> {
    type SendStream = SendStream<S, C>;
    type RecvStream = RecvStream<S, C>;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        (self.send, self.recv)
    }
}

impl<S: AsyncByteStream, C: Clock> quic::SendStream<Bytes> for BidiStream<S, C> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        self.send.poll_ready(cx)
    }

    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        self.send.send_data(data)
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        self.send.poll_finish(cx)
    }

    fn reset(&mut self, reset_code: u64) {
        self.send.reset(reset_code);
    }

    fn send_id(&self) -> quic::StreamId {
        self.send.send_id()
    }
}

impl<S: AsyncByteStream, C: Clock> quic::SendStreamUnframed<Bytes> for BidiStream<S, C> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        self.send.poll_send(cx, buf)
    }
}

impl<S: AsyncByteStream, C: Clock> quic::RecvStream for BidiStream<S, C> {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        self.recv.poll_data(cx)
    }

    fn stop_sending(&mut self, error_code: u64) {
        self.recv.stop_sending(error_code);
    }

    fn recv_id(&self) -> quic::StreamId {
        self.recv.recv_id()
    }
}
