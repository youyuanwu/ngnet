//! The one operation that drives the connection.
//!
//! Called from every trait method, for the same reason `ngnet-quic-h3` calls its own from
//! every entry point: the HTTP/3 driver's first act is to open its control streams, and it
//! reaches nothing else until it has them. If datagrams only moved while sending stream data,
//! the first flight would never leave and the connection would deadlock before it began.
//!
//! This crate exposes no driver future. That is deliberate — it keeps the public surface to a
//! single constructor, and it keeps the spawned-task count equal to the native
//! `ngnet-quic-h3` stack, which matters because the two are benchmarked against each other.
//! What a driver future would have provided is a stable wake target for the expiry timer;
//! [`Core`](crate::core::Core) provides that directly instead. See `core.rs`.

use std::io::IoSlice;
use std::task::{Context, Poll, Waker};

use bytes::Buf;
use ngnet_quic::{ExpiryOutcome, ReadOutcome, Session, StreamId, StreamWrite, WriteOutcome};

use crate::core::{Core, MAX_DATAGRAM, WORK_BUDGET, Wakers};
use crate::error::ConnectionTerminal;

/// What a pass observed, so the fan-out only fires when something actually changed.
///
/// Waking unconditionally would have the stable timer waker re-wake the task that just
/// pumped, on every pass, forever.
#[derive(Default)]
pub(crate) struct Changed {
    pub(crate) connection: bool,
    pub(crate) streams: Vec<i64>,
}

impl Changed {
    fn stream(&mut self, stream: i64) {
        if !self.streams.contains(&stream) {
            self.streams.push(stream);
        }
    }

    fn any(&self) -> bool {
        self.connection || !self.streams.is_empty()
    }

    /// Fans out to everything this pass affected.
    pub(crate) fn deliver(self, wakers: &Wakers) {
        if !self.any() {
            return;
        }
        if self.connection {
            wakers.wake_connection();
        }
        for stream in self.streams {
            wakers.wake_stream(stream);
        }
    }
}

/// Drives the connection one pass: read, expire, route, produce, re-arm.
///
/// Registers `cx` with the transport so the next routed datagram wakes this task, and arms
/// the expiry sleep under the core's stable waker so a quiet connection still fires its own
/// timers.
pub(crate) fn pump<S: Session>(core: &mut Core<S>, cx: &mut Context<'_>) -> Changed {
    let mut changed = Changed::default();
    // `ngnet-quic` keeps a list of wakers and wakes all of them per routed datagram, so every
    // task that pumps may register without displacing the others.
    let _ = core.detached.register(cx.waker());

    // Two turns at most: a fired timer produces work that wants reading and sending, and
    // going round once more here is cheaper than returning Pending and being woken again.
    for _ in 0..2 {
        read_inbound(core, &mut changed);
        if core.terminal.is_some() {
            break;
        }
        expire(core, &mut changed);
        if core.terminal.is_some() {
            break;
        }
        route_observed(core, &mut changed);
        produce(core, Some(cx.waker()));
        if core.terminal.is_some() {
            break;
        }
        if arm_timer(core).is_pending() {
            break;
        }
    }

    if core.terminal.is_some() {
        // Everything parked anywhere must learn the connection ended rather than wait.
        changed.connection = true;
        let streams: Vec<i64> = core.streams.keys().copied().collect();
        for stream in streams {
            changed.stream(stream);
        }
        release(core);
    }
    changed
}

/// Takes in whatever the endpoint routed here.
fn read_inbound<S: Session>(core: &mut Core<S>, changed: &mut Changed) {
    let now = core.detached.now();
    while let Some(datagram) = core.detached.next_inbound() {
        match core.detached.conn.read_pkt(&datagram, now) {
            Ok(ReadOutcome::Processed | ReadOutcome::SendRetry | ReadOutcome::DropSilently) => {}
            Ok(ReadOutcome::Draining | ReadOutcome::Closing) => {
                let close = core.detached.conn.close_error();
                core.fail(ConnectionTerminal::from_close(&close));
                changed.connection = true;
                // Nothing further is read into a connection that has ended; continuing would
                // re-record the close for every datagram still queued.
                break;
            }
            Err(err) => {
                core.fail(ConnectionTerminal::undefined(format!(
                    "the transport failed while reading a datagram: {err}"
                )));
                changed.connection = true;
                break;
            }
        }
    }
}

/// Fires the expiry timer if it is due.
///
/// Its deadline folds in ngtcp2's pacing deadline, so this is also what releases a connection
/// that is waiting to send rather than waiting to hear.
fn expire<S: Session>(core: &mut Core<S>, changed: &mut Changed) {
    let now = core.detached.now();
    if !core.detached.conn.expiry().is_some_and(|at| at <= now) {
        return;
    }
    match core.detached.conn.handle_expiry(now) {
        Ok(ExpiryOutcome::Handled) => {}
        Ok(ExpiryOutcome::IdleClose) => {
            // The outcome already says why. Asking the connection for a close error here
            // would report ngtcp2's default `ccerr` — a transport NO_ERROR — and an idle
            // timeout would reach HTTP/3 as an unexplained failure instead of the timeout
            // hyperium has a dedicated variant for.
            core.fail(ConnectionTerminal::IdleTimeout);
            changed.connection = true;
        }
        Ok(ExpiryOutcome::Terminal) => {
            let close = core.detached.conn.close_error();
            core.fail(ConnectionTerminal::from_close(&close));
            changed.connection = true;
        }
        Err(err) => {
            core.fail(ConnectionTerminal::undefined(format!(
                "the transport failed while firing its timer: {err}"
            )));
            changed.connection = true;
        }
    }
}

/// Drains what the connection's handlers recorded during the calls above.
fn route_observed<S: Session>(core: &mut Core<S>, changed: &mut Changed) {
    use ngnet_quic::endpoint::Observed;

    for observed in core.detached.take_observed() {
        match observed {
            Observed::Data(stream, bytes, fin) => {
                let state = core.state(stream);
                if !bytes.is_empty() {
                    state.incoming.push_back(bytes.into());
                }
                if fin {
                    state.finished = true;
                }
                changed.stream(stream.get());
            }
            Observed::Opened(stream) => {
                core.record_opened(stream);
                changed.connection = true;
            }
            Observed::Reset(stream, code) => {
                // The peer abandoned the direction it sends on, which is the one we read.
                // Our sending side is untouched: RFC 9000 keeps the two directions
                // independent, and so does hyperium.
                let state = core.state(stream);
                state.recv_terminal = Some(crate::error::DirectionTerminal::Reset(code.get()));
                changed.stream(stream.get());
            }
            Observed::StopSending(stream, code) => {
                // The peer asked us to stop sending, so our sending side ends and anything
                // still retained for it will never be delivered.
                let state = core.state(stream);
                state.send_terminal_state =
                    Some(crate::error::DirectionTerminal::Stopped(code.get()));
                state.writing = None;
                changed.stream(stream.get());
            }
            Observed::StreamsExtended(_) => {
                // The only signal that a refused open may now succeed.
                core.streams_extended = true;
                changed.connection = true;
            }
            Observed::Closed(stream, _) => {
                changed.stream(stream.get());
            }
            Observed::LocallyOpened(_) | Observed::Acked(..) | Observed::HandshakeCompleted => {}
            _ => {}
        }
    }
}

/// Produces datagrams the connection owes, subject to the outbound queue having room.
///
/// Acknowledgements, handshake flights and loss probes come from here rather than from the
/// stream-writing path, so a connection with nothing to say still says it.
pub(crate) fn produce<S: Session>(core: &mut Core<S>, capacity_waker: Option<&Waker>) {
    for _ in 0..WORK_BUDGET {
        let ready = match capacity_waker {
            Some(waker) => core.detached.poll_outbound_capacity(waker),
            None => core.detached.outbound_has_room(),
        };
        if !ready {
            break;
        }
        let now = core.detached.now();
        let mut datagram = core::mem::take(&mut core.scratch);
        datagram.resize(MAX_DATAGRAM, 0);
        match core.detached.conn.write_pkt(&mut datagram, now) {
            Ok(WriteOutcome::Datagram { len }) => {
                datagram.truncate(len);
                core.detached.send(datagram);
            }
            Ok(WriteOutcome::Blocked | WriteOutcome::Idle) => {
                // Untouched storage: keep it rather than dropping it and reallocating.
                datagram.clear();
                core.scratch = datagram;
                break;
            }
            Err(err) => {
                datagram.clear();
                core.scratch = datagram;
                core.fail(ConnectionTerminal::undefined(format!(
                    "the transport failed while producing a datagram: {err}"
                )));
                break;
            }
        }
    }
}

/// Arms, and re-arms, the connection's expiry sleep.
///
/// Polled under the core's stable waker, never a caller's: see `core.rs`. Re-armed whenever
/// the deadline moves, because ngtcp2 folds its pacing deadline into the same expiry — a
/// connection that only re-armed after reading would send one datagram and then wait for the
/// peer to speak.
fn arm_timer<S: Session>(core: &mut Core<S>) -> Poll<()> {
    let Some(deadline) = core.detached.conn.expiry() else {
        core.sleeping = None;
        core.sleeping_until = None;
        return Poll::Pending;
    };
    if core.sleeping_until != Some(deadline) {
        core.sleeping = Some(core.detached.sleep_until(deadline));
        core.sleeping_until = Some(deadline);
    }
    // Cloned out before the mutable borrow of `sleeping` below. The clone is cheap: it is an
    // `Arc` bump on the core's own stable wake target, not a caller's waker.
    let timer_waker = core.timer_waker();
    let mut timer_cx = Context::from_waker(&timer_waker);
    let Some(sleep) = core.sleeping.as_mut() else {
        return Poll::Pending;
    };
    match core::pin::Pin::new(sleep).poll(&mut timer_cx) {
        Poll::Ready(()) => {
            core.sleeping = None;
            core.sleeping_until = None;
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    }
}

/// Tells the endpoint to stop routing here.
///
/// The endpoint cannot work this out for itself — it does not hold the connection and cannot
/// ask whether it is draining — so without this its routing entries outlive the connection.
pub(crate) fn release<S: Session>(core: &mut Core<S>) {
    if !core.released {
        core.released = true;
        core.detached.release();
    }
}

/// What one attempt to hand stream bytes to the transport did.
pub(crate) enum Offered {
    /// The transport took this many bytes. May be zero: the packet may have filled with
    /// control frames instead, and whatever was not accepted must be offered again.
    Accepted(usize),
    /// The transport has something to send but cannot right now. Not "finished".
    Blocked,
    /// There was no room to produce another datagram.
    NoCapacity,
    /// The connection failed while writing.
    Failed,
}

/// Offers one contiguous chunk of stream data to the transport and sends what it produced.
///
/// Capacity is checked *before* taking the offer, never after: a produced datagram cannot be
/// withdrawn, because the connection has already accounted for the stream bytes in it, so
/// offering them again would send them twice and dropping it loses them until a
/// retransmission timer notices.
///
/// Hyperium's `WriteBuf` does not override `chunks_vectored`, so it yields exactly one slice
/// at a time — the frame header first, then the payload. A single-slice vectored write is
/// therefore the whole story here, not a simplification.
pub(crate) fn offer<S: Session>(
    core: &mut Core<S>,
    stream: StreamId,
    chunk: &[u8],
    fin: bool,
    waker: &Waker,
) -> Offered {
    if !core.detached.poll_outbound_capacity(waker) {
        return Offered::NoCapacity;
    }
    let now = core.detached.now();
    let mut datagram = core::mem::take(&mut core.scratch);
    datagram.resize(MAX_DATAGRAM, 0);
    let ranges = [IoSlice::new(chunk)];
    let outcome =
        core.detached
            .conn
            .write_stream_vectored(&mut datagram, stream, &ranges, fin, now);
    match outcome {
        Ok(StreamWrite::Datagram { len, accepted }) => {
            if accepted > chunk.len() {
                datagram.clear();
                core.scratch = datagram;
                core.fail(ConnectionTerminal::internal(
                    "the transport accepted more bytes than were offered",
                ));
                return Offered::Failed;
            }
            if len == 0 {
                datagram.clear();
                core.scratch = datagram;
            } else {
                datagram.truncate(len);
                core.detached.send(datagram);
            }
            Offered::Accepted(accepted)
        }
        Ok(
            StreamWrite::StreamBlocked
            | StreamWrite::ConnectionBlocked
            | StreamWrite::Blocked
            | StreamWrite::Idle,
        ) => {
            datagram.clear();
            core.scratch = datagram;
            Offered::Blocked
        }
        Err(err) => {
            datagram.clear();
            core.scratch = datagram;
            core.fail(ConnectionTerminal::undefined(format!(
                "the transport failed while writing stream data: {err}"
            )));
            Offered::Failed
        }
    }
}

/// Returns flow-control credit for bytes HTTP/3 has consumed.
///
/// Credit is returned on consumption rather than on arrival, deliberately: returning it when
/// bytes are merely buffered would let a peer outrun a reader that is not keeping up. Not
/// calling this at all silently stalls the peer once its window closes, which is why it sits
/// on the read path rather than anywhere optional.
pub(crate) fn extend_credit<S: Session>(core: &mut Core<S>, stream: StreamId, bytes: usize) {
    if bytes == 0 {
        return;
    }
    let bytes = bytes as u64;
    let _ = core.detached.conn.extend_max_stream_offset(stream, bytes);
    core.detached.conn.extend_max_offset(bytes);
    // The MAX_STREAM_DATA and MAX_DATA frames only leave in a datagram.
    produce(core, None);
}

/// Consumes a chunk's worth of a retained buffer.
pub(crate) fn advance(buf: &mut h3::quic::WriteBuf<bytes::Bytes>, accepted: usize) {
    buf.advance(accepted);
}
