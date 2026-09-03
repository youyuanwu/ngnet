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

    /// Runs one pump pass, lets the caller act on the result, and then fans out.
    ///
    /// The order inside is the whole design, and each step depends on the one before it.
    /// `then` runs while the core is still held, which is where a caller that is about to
    /// park registers its waker — so registration and the readiness check it is based on are
    /// one step with respect to every other task. The fan-out happens after the lock is
    /// released, so a woken task can take it immediately rather than bouncing off a lock this
    /// pass still holds.
    ///
    /// That ordering is also what makes `rearm`'s "the timer is still due, do not park"
    /// signal reach the task it is aimed at: `wake_everything` runs after `then`, so the
    /// caller is already in the registry the delivery walks. Waking before it registered
    /// would tell nobody, and the task told not to park would park anyway.
    pub(crate) fn pump<T>(&self, cx: &mut Context<'_>, then: impl FnOnce(&mut Core<S>) -> T) -> T {
        let (changed, value) = {
            let mut core = self.lock();
            let mut changed = pump::pump(&mut core, cx);
            let value = then(&mut core);
            // The caller's operation may have moved when the connection next wants attention —
            // a blocked write creates a pacing deadline — so the timer is armed against the
            // state being left behind, not the state that was found.
            if pump::rearm(&mut core) {
                changed.wake_everything(&core.streams);
            }
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

/// Registers a waker for a stream while the core lock is held.
///
/// Every pending path in this crate goes through here rather than registering after its pass
/// has returned. The registry has a lock of its own, so taking it under the core's is safe --
/// nothing ever takes the core while holding the registry -- and it is what makes the
/// readiness check and the registration one indivisible step with respect to any other task.
fn park_on_stream<S: Session>(shared: &Shared<S>, stream: StreamId, waker: &core::task::Waker) {
    shared.wakers.register_stream(stream.get(), waker);
}

/// The stream-level error for a sending half the transport has already shut.
fn gone(stream: StreamId) -> StreamErrorIncoming {
    let _ = stream;
    // `H3_REQUEST_CANCELLED`, the same code this crate resets an abandoned send with, because
    // by the time the transport refuses a write that is what has happened to the stream.
    StreamErrorIncoming::StreamTerminated {
        error_code: ABANDONED,
    }
}

/// The sending half of a stream.
pub struct SendStream<S: Session> {
    shared: Shared<S>,
    stream: StreamId,
}

impl<S: Session> SendStream<S> {
    pub(crate) fn new(shared: Shared<S>, stream: StreamId) -> Self {
        shared.lock().retain_handle(stream);
        Self { shared, stream }
    }

    /// Drops this stream's waker entry once no handle refers to it.
    ///
    /// Both halves must do this, not just the receiving one: a unidirectional send stream has
    /// no receiving half at all, and a split bidirectional stream may have its sending half
    /// dropped last. Leaving the entry behind leaks one waker slot per stream for the life of
    /// the connection, and every subsequent `wake_all` then wakes tasks that no longer exist.
    fn forget_if_last(&self, last: bool) {
        if last {
            self.shared.wakers.forget_stream(self.stream.get());
        }
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
                            core.state(id).writing = Some(data);
                            if accepted == 0 {
                                // A serialised zero-length STREAM frame, which takes nothing
                                // from a non-empty offer. Re-offering the same prefix in this
                                // pass would spin; wait to be woken.
                                Step::Pending
                            } else {
                                // A partial acceptance is ordinary -- a packet filled -- so
                                // the remainder is offered again immediately. Parking here
                                // instead would cost a packet per wakeup and diverge from
                                // the native stack this crate is measured against, whose
                                // `transmit::drain` keeps filling packets until acceptance
                                // reaches zero or capacity runs out.
                                let _ = offered;
                                Step::Progress
                            }
                        }
                    }
                    Offered::Displaced => {
                        // The packet went to whatever ngtcp2 had queued ahead of this stream
                        // and carried none of it. Offer the same prefix again: what displaced
                        // it has now been produced, so the next attempt has room. The
                        // enclosing loop is bounded, and exhausting it yields with a
                        // self-wake rather than parking on an event that may never come.
                        core.state(id).writing = Some(data);
                        Step::Progress
                    }
                    Offered::Blocked | Offered::NoCapacity => {
                        core.state(id).writing = Some(data);
                        // Registered here, while the core is still held, and not after this
                        // closure returns. Between releasing the core and taking the waker
                        // registry, another task can route the very data being waited on and
                        // fan out to a registry this one has not reached yet; the wake is
                        // then delivered to nobody and the park is permanent.
                        park_on_stream(&self.shared, id, &waker);
                        Step::Pending
                    }
                    Offered::StreamGone => {
                        core.state(id).writing = None;
                        Step::Error(gone(id))
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
                // The registration already happened under the core lock, inside the closure.
                Step::Pending => return Poll::Pending,
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
                    // A zero-length STREAM frame was serialised, and for a FIN-only offer
                    // that frame *is* the write: the stream has ended on the wire.
                    Offered::Accepted(_) => {
                        core.state(id).send_finished = true;
                        Step::Done
                    }
                    // The packet carried no STREAM frame, so the FIN was not written. It is
                    // not in flight and nothing will retransmit it, so it must be offered
                    // again; recording the stream as finished here is precisely the defect
                    // that left a peer waiting until its idle timeout.
                    Offered::Displaced => Step::Progress,
                    Offered::Blocked | Offered::NoCapacity => {
                        park_on_stream(&self.shared, id, &waker);
                        Step::Pending
                    }
                    // Already shut, so there is no FIN left to send and no reason to keep
                    // asking. Reported at stream level, not by failing the connection.
                    Offered::StreamGone => {
                        core.state(id).send_finished = true;
                        Step::Done
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
                Step::Pending => return Poll::Pending,
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
        // Recorded, not merely flagged. ngtcp2 shuts the sending half here, and a later
        // `poll_finish` or `poll_send` that offered to it would come back
        // `ERR_STREAM_SHUT_WR` — a stream-level fact this crate used to escalate into a
        // failed connection, taking every other request on it down too.
        state.send_terminal_state = Some(crate::error::DirectionTerminal::Abandoned(reset_code));
        let _ = core
            .detached
            .conn
            .reset_stream(id, ApplicationErrorCode::new(reset_code));
        // A RESET_STREAM frame only leaves in a datagram, and `reset` is synchronous, so
        // nothing else will send it on our behalf. The timer is re-armed with it, because a
        // frame that pacing refused needs a deadline to be offered again and this path has
        // no caller coming back to create one.
        pump::produce(&mut core, None);
        let due = pump::rearm(&mut core);
        drop(core);
        if due {
            self.shared.wakers.wake_all();
        }
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
        // Bounded, because a packet that carried only transport work leaves the offer
        // untouched and worth making again rather than reporting as a zero-byte write --
        // hyperium would read that as backpressure it was never given.
        for _ in 0..WORK_BUDGET {
            let outcome = self.shared.pump(cx, |core| {
                if let Some(terminal) = &core.terminal {
                    return Err(Some(ended(terminal)));
                }
                let id = self.stream;
                if core.state(id).writing.is_some() {
                    // The two paths write to the same stream; running both would interleave
                    // a framed body with raw bytes.
                    return Err(Some(internal(
                        "an unframed send was attempted while framed data is retained",
                    )));
                }
                if let Some(terminal) = core.state(id).send_terminal() {
                    return Err(Some(terminal.stream_error()));
                }
                let chunk = buf.chunk();
                if chunk.is_empty() {
                    return Ok(Some(0));
                }
                match pump::offer(core, id, chunk, false, &waker) {
                    Offered::Accepted(accepted) => Ok(Some(accepted)),
                    // Nothing of this stream reached the packet, so the offer stands. Try
                    // it again rather than telling hyperium zero bytes were taken.
                    Offered::Displaced => Ok(None),
                    Offered::Blocked | Offered::NoCapacity => {
                        park_on_stream(&self.shared, id, &waker);
                        Err(None)
                    }
                    Offered::StreamGone => Err(Some(gone(id))),
                    Offered::Failed => {
                        let terminal = core.terminal.clone().unwrap_or_else(|| {
                            ConnectionTerminal::internal("the transport failed")
                        });
                        Err(Some(ended(&terminal)))
                    }
                }
            });
            match outcome {
                Ok(None) => continue,
                Ok(Some(0)) => return Poll::Ready(Ok(0)),
                Ok(Some(accepted)) => {
                    // Advance by exactly what was taken, never by what was offered.
                    buf.advance(accepted);
                    return Poll::Ready(Ok(accepted));
                }
                Err(Some(err)) => return Poll::Ready(Err(err)),
                Err(None) => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<S: Session> Drop for SendStream<S> {
    fn drop(&mut self) {
        let mut core = self.shared.lock();
        if core.terminal.is_some() {
            let last = core.release_handle(self.stream);
            drop(core);
            self.forget_if_last(last);
            return;
        }
        let id = self.stream;
        let state = core.state(id);
        if state.send_finished || state.send_reset || state.send_terminal_state.is_some() {
            let last = core.release_handle(id);
            drop(core);
            self.forget_if_last(last);
            return;
        }
        // An unfinished send that is simply dropped would leave the peer waiting on a stream
        // that will never end. One reset says so.
        state.send_reset = true;
        state.writing = None;
        state.send_terminal_state = Some(crate::error::DirectionTerminal::Abandoned(ABANDONED));
        let _ = core
            .detached
            .conn
            .reset_stream(id, ApplicationErrorCode::new(ABANDONED));
        pump::produce(&mut core, None);
        // As in `reset`: the task that owned this half is going away, so if pacing refused
        // the frame nothing else is coming back to offer it again unless a deadline says so.
        let due = pump::rearm(&mut core);
        let last = core.release_handle(id);
        drop(core);
        if due {
            self.shared.wakers.wake_all();
        }
        self.forget_if_last(last);
    }
}

/// The receiving half of a stream.
pub struct RecvStream<S: Session> {
    shared: Shared<S>,
    stream: StreamId,
}

impl<S: Session> RecvStream<S> {
    pub(crate) fn new(shared: Shared<S>, stream: StreamId) -> Self {
        shared.lock().retain_handle(stream);
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
        // Cloned once so the closure below does not borrow `cx`, which `pump` needs mutably.
        let waker = cx.waker().clone();
        let outcome = self.shared.pump(cx, |core| {
            let id = self.stream;
            // Bytes already delivered come out before any terminal: a peer that sent data and
            // then reset the stream sent that data, and discarding it would be a silent
            // truncation.
            if let Some(chunk) = core.state(id).incoming.pop_front() {
                pump::extend_credit(core, id, chunk.len());
                return Ok(Some(chunk));
            }
            // A clean end wins over an abnormal one: if the peer sent its FIN, the body is
            // complete and reporting an error would be a silent truncation.
            if core.state(id).finished {
                return Ok(None);
            }
            if let Some(terminal) = core.state(id).recv_terminal {
                return Err(Some(terminal.stream_error()));
            }
            if let Some(terminal) = &core.terminal {
                return Err(Some(ended(terminal)));
            }
            // Under the core lock, for the reason given in `poll_retained`: a wake delivered
            // between releasing it and registering would reach an empty list.
            park_on_stream(&self.shared, id, &waker);
            Err(None)
        });
        match outcome {
            Ok(chunk) => Poll::Ready(Ok(chunk)),
            Err(Some(err)) => Poll::Ready(Err(err)),
            Err(None) => Poll::Pending,
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
        // As with `reset`, the frame only leaves in a datagram, this call is synchronous, and
        // a deadline is what offers it again if pacing refused it now.
        pump::produce(&mut core, None);
        let due = pump::rearm(&mut core);
        drop(core);
        if due {
            self.shared.wakers.wake_all();
        }
    }

    fn recv_id(&self) -> quic::StreamId {
        h3_id(self.stream)
    }
}

impl<S: Session> Drop for RecvStream<S> {
    fn drop(&mut self) {
        // Only the *last* handle discards the state. A split bidirectional stream has two
        // handles over one stream id, dropped independently and in either order, and the
        // survivor still needs the retained send and the terminals.
        let last = self.shared.lock().release_handle(self.stream);
        if last {
            self.shared.wakers.forget_stream(self.stream.get());
        }
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
        // Both halves already count as handles onto the shared stream state, so neither
        // depends on the other surviving and either may be dropped first.
        (self.send, self.recv)
    }
}
