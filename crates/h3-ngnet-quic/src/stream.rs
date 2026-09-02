//! Stream handles, and the retained-write state machine behind them.
//!
//! # The retained write
//!
//! Hyperium hands a whole logical send over in one synchronous [`send_data`] call and then
//! expects [`poll_ready`] to report when the transport has taken all of it. The transport,
//! meanwhile, accepts what fits in the packet it is currently filling — sometimes a prefix,
//! sometimes nothing at all, because the packet went to control frames instead.
//!
//! So the buffer has to be kept somewhere across those partial writes, and the obvious place
//! is wrong: advancing it optimistically loses whatever the transport did not take. Instead
//! the buffer is *taken out* of the stream state for the duration of one offer and *put back*
//! with only the accepted prefix consumed. Zero acceptance puts it back untouched.
//!
//! The ngtcp2 stable-address requirement is not this crate's problem to solve, and that is
//! worth being explicit about: `write_stream_vectored` stages its own bounded copy internally
//! and retains *that*, so the bytes this crate offers are free the moment the call returns.
//! This is the same contract the native `ngnet-quic-h3` stack relies on. What is left for
//! this crate is the ordinary correctness obligation above — offer every byte exactly once,
//! in order.
//!
//! [`send_data`]: h3::quic::SendStream::send_data
//! [`poll_ready`]: h3::quic::SendStream::poll_ready

use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use h3::quic::{self, StreamErrorIncoming, WriteBuf};
use ngnet_quic::{ApplicationErrorCode, Session, StreamId};

use crate::core::{Core, WORK_BUDGET, Wakers};
use crate::error::ConnectionTerminal;
use crate::pump::{self, Offered};

/// The error code a stream is reset with when its unfinished sending half is dropped.
///
/// `H3_REQUEST_CANCELLED`. A dropped unfinished send is a cancelled request, and saying so is
/// better than leaving the peer to wait for a stream that will never finish.
const ABANDONED: u64 = 0x010c;

/// What every handle shares.
pub(crate) struct Shared<S: Session> {
    pub(crate) core: Arc<Mutex<Core<S>>>,
    pub(crate) wakers: Arc<Wakers>,
}

impl<S: Session> Clone for Shared<S> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            wakers: Arc::clone(&self.wakers),
        }
    }
}

impl<S: Session> Shared<S> {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Core<S>> {
        self.core
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Runs one pump pass and then fans out to whatever it affected.
    ///
    /// The fan-out happens after the lock is released, so a woken task can take it
    /// immediately rather than bouncing off a lock this pass still holds.
    pub(crate) fn pump<T>(&self, cx: &mut Context<'_>, then: impl FnOnce(&mut Core<S>) -> T) -> T {
        let (changed, value) = {
            let mut core = self.lock();
            let changed = pump::pump(&mut core, cx);
            let value = then(&mut core);
            (changed, value)
        };
        changed.deliver(&self.wakers);
        value
    }
}

pub(crate) fn h3_id(stream: StreamId) -> quic::StreamId {
    // A QUIC stream identifier is non-negative and below 2^62, which is exactly hyperium's
    // domain, so this cannot fail for an id this crate ever produces.
    (stream.get() as u64)
        .try_into()
        .expect("a QUIC stream id is an HTTP/3 stream id")
}

fn ended(terminal: &ConnectionTerminal) -> StreamErrorIncoming {
    terminal.stream_error()
}

fn internal(message: &str) -> StreamErrorIncoming {
    ConnectionTerminal::internal(message).stream_error()
}

/// The sending half of a stream.
pub struct SendStream<S: Session> {
    shared: Shared<S>,
    stream: StreamId,
}

impl<S: Session> SendStream<S> {
    pub(crate) fn new(shared: Shared<S>, stream: StreamId) -> Self {
        Self { shared, stream }
    }

    /// Drains the retained buffer until the transport has taken all of it.
    fn poll_retained(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        // Cloned once so the closure below does not borrow `cx`, which `pump` needs mutably.
        let waker = cx.waker().clone();
        for _ in 0..WORK_BUDGET {
            let step = self.shared.pump(cx, |core| {
                if let Some(terminal) = &core.terminal {
                    return Step::Error(ended(terminal));
                }
                let id = self.stream;
                if let Some(terminal) = core.state(id).send_terminal() {
                    return Step::Error(terminal.stream_error());
                }
                let Some(mut data) = core.state(id).writing.take() else {
                    return Step::Done;
                };
                if data.remaining() == 0 {
                    return Step::Done;
                }
                let chunk = data.chunk();
                if chunk.is_empty() {
                    core.fail(ConnectionTerminal::internal(
                        "a hyperium WriteBuf returned an empty chunk with bytes remaining",
                    ));
                    return Step::Error(internal(
                        "a hyperium WriteBuf returned an empty chunk with bytes remaining",
                    ));
                }
                let offered = chunk.len();
                // `chunk` borrows `data`, so the offer is made against a copy of the slice
                // bounds rather than holding the borrow across the mutation below.
                match pump::offer(core, id, chunk, false, &waker) {
                    Offered::Accepted(accepted) => {
                        pump::advance(&mut data, accepted);
                        if data.remaining() == 0 {
                            Step::Done
                        } else {
                            let park = accepted < offered;
                            core.state(id).writing = Some(data);
                            if park { Step::Pending } else { Step::Progress }
                        }
                    }
                    Offered::Blocked | Offered::NoCapacity => {
                        core.state(id).writing = Some(data);
                        Step::Pending
                    }
                    Offered::Failed => {
                        let terminal = core.terminal.clone().unwrap_or_else(|| {
                            ConnectionTerminal::internal("the transport failed")
                        });
                        Step::Error(ended(&terminal))
                    }
                }
            });
            match step {
                Step::Done => return Poll::Ready(Ok(())),
                Step::Progress => continue,
                Step::Pending => {
                    self.shared
                        .wakers
                        .register_stream(self.stream.get(), cx.waker());
                    return Poll::Pending;
                }
                Step::Error(err) => return Poll::Ready(Err(err)),
            }
        }
        // Budget exhausted with work still to do: yield, but make sure we are polled again
        // rather than waiting for an external event that may never come.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

enum Step {
    Done,
    Progress,
    Pending,
    Error(StreamErrorIncoming),
}

impl<S: Session> core::fmt::Debug for SendStream<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SendStream")
            .field("stream", &self.stream.get())
            .finish_non_exhaustive()
    }
}

impl<S: Session> quic::SendStream<Bytes> for SendStream<S> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        self.poll_retained(cx)
    }

    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        let mut core = self.shared.lock();
        if let Some(terminal) = &core.terminal {
            return Err(ended(terminal));
        }
        let id = self.stream;
        if let Some(terminal) = core.state(id).send_terminal() {
            return Err(terminal.stream_error());
        }
        if core.state(id).writing.is_some() {
            // Accepting a second logical send would interleave two frames on one stream.
            return Err(internal(
                "send_data was called before the previous logical send became ready",
            ));
        }
        core.state(id).writing = Some(data.into());
        Ok(())
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        {
            let mut core = self.shared.lock();
            if core.state(self.stream).send_finished {
                // Idempotent: a second finish must not emit a second FIN.
                return Poll::Ready(Ok(()));
            }
        }
        // Everything already handed over must go out before the FIN, or the FIN would arrive
        // attached to a prefix and truncate the body.
        match self.poll_retained(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        // Cloned once so the closure below does not borrow `cx`, which `pump` needs mutably.
        let waker = cx.waker().clone();
        for _ in 0..WORK_BUDGET {
            let step = self.shared.pump(cx, |core| {
                if let Some(terminal) = &core.terminal {
                    return Step::Error(ended(terminal));
                }
                let id = self.stream;
                if core.state(id).send_finished {
                    return Step::Done;
                }
                if let Some(terminal) = core.state(id).send_terminal() {
                    return Step::Error(terminal.stream_error());
                }
                match pump::offer(core, id, &[], true, &waker) {
                    Offered::Accepted(_) => {
                        core.state(id).send_finished = true;
                        Step::Done
                    }
                    Offered::Blocked | Offered::NoCapacity => Step::Pending,
                    Offered::Failed => {
                        let terminal = core.terminal.clone().unwrap_or_else(|| {
                            ConnectionTerminal::internal("the transport failed")
                        });
                        Step::Error(ended(&terminal))
                    }
                }
            });
            match step {
                Step::Done => return Poll::Ready(Ok(())),
                Step::Progress => continue,
                Step::Pending => {
                    self.shared
                        .wakers
                        .register_stream(self.stream.get(), cx.waker());
                    return Poll::Pending;
                }
                Step::Error(err) => return Poll::Ready(Err(err)),
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn reset(&mut self, reset_code: u64) {
        let mut core = self.shared.lock();
        if core.terminal.is_some() {
            return;
        }
        let id = self.stream;
        let state = core.state(id);
        if state.send_reset || state.send_finished {
            return;
        }
        state.send_reset = true;
        state.writing = None;
        let _ = core
            .detached
            .conn
            .reset_stream(id, ApplicationErrorCode::new(reset_code));
        // A RESET_STREAM frame only leaves in a datagram, and `reset` is synchronous, so
        // nothing else will send it on our behalf.
        pump::produce(&mut core, None);
    }

    fn send_id(&self) -> quic::StreamId {
        h3_id(self.stream)
    }
}

impl<S: Session> quic::SendStreamUnframed<Bytes> for SendStream<S> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        let waker = cx.waker().clone();
        let outcome = self.shared.pump(cx, |core| {
            if let Some(terminal) = &core.terminal {
                return Err(Some(ended(terminal)));
            }
            let id = self.stream;
            if core.state(id).writing.is_some() {
                // The two paths write to the same stream; running both would interleave a
                // framed body with raw bytes.
                return Err(Some(internal(
                    "an unframed send was attempted while framed data is retained",
                )));
            }
            if let Some(terminal) = core.state(id).send_terminal() {
                return Err(Some(terminal.stream_error()));
            }
            let chunk = buf.chunk();
            if chunk.is_empty() {
                return Ok(0);
            }
            match pump::offer(core, id, chunk, false, &waker) {
                Offered::Accepted(accepted) => Ok(accepted),
                Offered::Blocked | Offered::NoCapacity => Err(None),
                Offered::Failed => {
                    let terminal = core
                        .terminal
                        .clone()
                        .unwrap_or_else(|| ConnectionTerminal::internal("the transport failed"));
                    Err(Some(ended(&terminal)))
                }
            }
        });
        match outcome {
            Ok(0) => Poll::Ready(Ok(0)),
            Ok(accepted) => {
                // Advance by exactly what was taken, never by what was offered.
                buf.advance(accepted);
                Poll::Ready(Ok(accepted))
            }
            Err(Some(err)) => Poll::Ready(Err(err)),
            Err(None) => {
                self.shared
                    .wakers
                    .register_stream(self.stream.get(), cx.waker());
                Poll::Pending
            }
        }
    }
}

impl<S: Session> Drop for SendStream<S> {
    fn drop(&mut self) {
        let mut core = self.shared.lock();
        if core.terminal.is_some() {
            return;
        }
        let id = self.stream;
        let state = core.state(id);
        if state.send_finished || state.send_reset || state.terminal.is_some() {
            return;
        }
        // An unfinished send that is simply dropped would leave the peer waiting on a stream
        // that will never end. One reset says so.
        state.send_reset = true;
        state.writing = None;
        let _ = core
            .detached
            .conn
            .reset_stream(id, ApplicationErrorCode::new(ABANDONED));
        pump::produce(&mut core, None);
    }
}

/// The receiving half of a stream.
pub struct RecvStream<S: Session> {
    shared: Shared<S>,
    stream: StreamId,
}

impl<S: Session> RecvStream<S> {
    pub(crate) fn new(shared: Shared<S>, stream: StreamId) -> Self {
        Self { shared, stream }
    }
}

impl<S: Session> core::fmt::Debug for RecvStream<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RecvStream")
            .field("stream", &self.stream.get())
            .finish_non_exhaustive()
    }
}

impl<S: Session> quic::RecvStream for RecvStream<S> {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        let outcome = self.shared.pump(cx, |core| {
            let id = self.stream;
            // Bytes already delivered come out before any terminal: a peer that sent data and
            // then reset the stream sent that data, and discarding it would be a silent
            // truncation.
            if let Some(chunk) = core.state(id).incoming.pop_front() {
                pump::extend_credit(core, id, chunk.len());
                return Ok(Some(chunk));
            }
            if let Some(terminal) = core.state(id).terminal {
                return Err(Some(terminal.stream_error()));
            }
            if core.state(id).finished {
                return Ok(None);
            }
            if let Some(terminal) = &core.terminal {
                return Err(Some(ended(terminal)));
            }
            Err(None)
        });
        match outcome {
            Ok(chunk) => Poll::Ready(Ok(chunk)),
            Err(Some(err)) => Poll::Ready(Err(err)),
            Err(None) => {
                self.shared
                    .wakers
                    .register_stream(self.stream.get(), cx.waker());
                Poll::Pending
            }
        }
    }

    fn stop_sending(&mut self, error_code: u64) {
        let mut core = self.shared.lock();
        if core.terminal.is_some() {
            return;
        }
        let id = self.stream;
        let _ = core
            .detached
            .conn
            .stop_sending(id, ApplicationErrorCode::new(error_code));
        // As with `reset`, the frame only leaves in a datagram and this call is synchronous.
        pump::produce(&mut core, None);
    }

    fn recv_id(&self) -> quic::StreamId {
        h3_id(self.stream)
    }
}

impl<S: Session> Drop for RecvStream<S> {
    fn drop(&mut self) {
        let mut core = self.shared.lock();
        core.streams.remove(&self.stream.get());
        drop(core);
        self.shared.wakers.forget_stream(self.stream.get());
    }
}

/// A bidirectional stream, before it is split.
pub struct BidiStream<S: Session> {
    send: SendStream<S>,
    recv: RecvStream<S>,
}

impl<S: Session> BidiStream<S> {
    pub(crate) fn new(shared: Shared<S>, stream: StreamId) -> Self {
        Self {
            send: SendStream::new(shared.clone(), stream),
            recv: RecvStream::new(shared, stream),
        }
    }
}

impl<S: Session> core::fmt::Debug for BidiStream<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BidiStream")
            .field("stream", &self.send.stream.get())
            .finish_non_exhaustive()
    }
}

impl<S: Session> quic::SendStream<Bytes> for BidiStream<S> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        quic::SendStream::poll_ready(&mut self.send, cx)
    }

    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        quic::SendStream::send_data(&mut self.send, data)
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        quic::SendStream::poll_finish(&mut self.send, cx)
    }

    fn reset(&mut self, reset_code: u64) {
        quic::SendStream::reset(&mut self.send, reset_code);
    }

    fn send_id(&self) -> quic::StreamId {
        quic::SendStream::send_id(&self.send)
    }
}

impl<S: Session> quic::SendStreamUnframed<Bytes> for BidiStream<S> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        quic::SendStreamUnframed::poll_send(&mut self.send, cx, buf)
    }
}

impl<S: Session> quic::RecvStream for BidiStream<S> {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        quic::RecvStream::poll_data(&mut self.recv, cx)
    }

    fn stop_sending(&mut self, error_code: u64) {
        quic::RecvStream::stop_sending(&mut self.recv, error_code);
    }

    fn recv_id(&self) -> quic::StreamId {
        quic::RecvStream::recv_id(&self.recv)
    }
}

impl<S: Session> quic::BidiStream<Bytes> for BidiStream<S> {
    type SendStream = SendStream<S>;
    type RecvStream = RecvStream<S>;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        // Both halves already hold their own handle onto the shared core, so neither depends
        // on the other surviving.
        let send = self.send;
        send.shared.lock().state(send.stream).recv_taken = true;
        (send, self.recv)
    }
}
