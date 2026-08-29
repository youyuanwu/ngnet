//! A [`QuicConnection`] implementation backed by [Quinn](quinn).
//!
//! `ngnet-h3` owns HTTP/3 protocol state but deliberately owns no QUIC implementation.
//! This crate is the adapter between that transport-independent API and an established
//! [`quinn::Connection`]. Endpoint creation, TLS configuration, certificate verification,
//! ALPN negotiation, and socket ownership remain with the caller.
//!
//! Pass [`QuinnBackend::new`] to [`ngnet_h3::http::handshake`] or
//! [`ngnet_h3::http::serve`] after Quinn has completed the QUIC handshake.
//!
//! # What quinn makes easy, and what it does not
//!
//! Easy: `SendStream::poll_write` is public and takes a plain slice, so the write side of an
//! offer maps straight onto it.
//!
//! Not easy: `accept_uni`, `accept_bi`, `open_uni` and `open_bi` are futures that *borrow*
//! the connection, which a poll-shaped trait cannot hold without self-reference. So opening
//! boxes a future owning a cloned `quinn::Connection`, and accepting happens in spawned
//! tasks feeding a channel. That is not a workaround grafted on: quinn is
//! per-stream-async by design, and this is what turning that into one connection-level event
//! stream costs. A callback-driven library such as msquic pays the opposite cost, which is
//! why the trait is shaped the way it is rather than either way.
//!
//! # Lifecycle and ownership rules
//!
//! Each was learned the hard way by the sans-I/O harness and is commented where it applies:
//! a dropped receiving half becomes `STOP_SENDING`; the sending half of an accepted stream
//! must be reachable before the stream is announced; release may be reported on acceptance
//! only because quinn copies; a reader task that exits quietly leaves the driver waiting for
//! an end that will never come; and a stream close must start a new driver event batch after
//! that stream's final data.

#![deny(missing_docs)]
#![deny(unsafe_code)]

mod lifecycle;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;
use std::time::Instant;

use lifecycle::{BatchGate, Streams};
use ngnet_h3::http::quic::Timestamp;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_h3::{ErrorCode, StreamId};
use tokio::sync::{mpsc, oneshot};

/// How much of a stream to read at once.
const READ_CHUNK: usize = 64 * 1024;

/// How far ahead of the layer the reader tasks may run, in bytes.
///
/// The trait requires an implementation to bound its own read-ahead by the credit the layer
/// has extended, *even when the underlying QUIC library manages receive windows itself*.
/// quinn does manage them — it returns credit when a chunk is read — but that governs what
/// the peer may send, not how much this adapter may hold on the layer's behalf. Without a
/// bound here the memory limit moves out of QUIC and into the process, where a fast peer can
/// exhaust it.
const INITIAL_BUDGET: u64 = 256 * 1024;

type OpeningUni = Pin<
    Box<dyn Future<Output = Result<quinn::SendStream, quinn::ConnectionError>> + Send + 'static>,
>;
type OpeningBi = Pin<
    Box<
        dyn Future<Output = Result<(quinn::SendStream, quinn::RecvStream), quinn::ConnectionError>>
            + Send
            + 'static,
    >,
>;
type StopSender = oneshot::Sender<ErrorCode>;
type StopReceiver = oneshot::Receiver<ErrorCode>;

/// Something that happened on a quinn connection.
enum Incoming {
    Data {
        stream: StreamId,
        bytes: bytes::Bytes,
        fin: bool,
    },
    /// A peer-opened bidirectional stream, with the half to answer on.
    Accepted {
        stream: StreamId,
        send: quinn::SendStream,
        stop: StopSender,
    },
    Reset {
        stream: StreamId,
        code: ErrorCode,
    },
    /// This endpoint successfully stopped its receiving direction.
    RecvStopped {
        stream: StreamId,
        code: ErrorCode,
    },
    /// The peer asked this endpoint to stop its sending direction.
    StopSending {
        stream: StreamId,
        code: ErrorCode,
    },
    /// Quinn observed how the peer ended this endpoint's sending direction.
    SendStopped {
        stream: StreamId,
        code: Option<ErrorCode>,
    },
    Closed,
}

/// A [`QuicConnection`] over an established `quinn::Connection`.
pub struct QuinnBackend {
    quic: quinn::Connection,
    /// Handles and terminal state for every stream this endpoint may write to.
    streams: Streams<quinn::SendStream, StopSender>,
    /// Releases owed to the layer, produced by writes and drained by `poll_event`.
    released: VecDeque<(StreamId, u64)>,
    /// Data and lifecycle observations produced by Quinn-owned tasks.
    events: mpsc::UnboundedReceiver<Incoming>,
    /// Held so the channel never closes of its own accord.
    _to_driver: mpsc::UnboundedSender<Incoming>,
    /// How many more bytes the reader tasks may deliver.
    budget: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// An in-progress stream open must survive `Poll::Pending`.
    opening_uni: Option<OpeningUni>,
    /// An in-progress stream open must survive `Poll::Pending`.
    opening_bi: Option<OpeningBi>,
    /// Monotonic origin used by the sans-I/O core.
    started: Instant,
    /// Whether connection shutdown has been observed or locally requested.
    closed: bool,
    /// Whether the single terminal connection event was returned.
    reported_closed: bool,
    /// Forces stream and connection closure to begin a fresh driver event batch.
    batch: BatchGate,
}

/// A quinn operation failed.
#[derive(Debug)]
pub struct QuinnError(String);

impl core::fmt::Display for QuinnError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "quinn: {}", self.0)
    }
}

impl core::error::Error for QuinnError {}

impl QuinnBackend {
    /// Wraps an established connection, spawning the tasks that read from it.
    ///
    /// Both HTTP/3 roles use the same adapter; stream direction and ownership come from Quinn's
    /// stream identifiers rather than from a role flag.
    pub fn new(quic: quinn::Connection) -> Self {
        let (to_driver, events) = mpsc::unbounded_channel();
        let budget = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(INITIAL_BUDGET));

        spawn_acceptor(quic.clone(), to_driver.clone(), budget.clone());

        Self {
            quic,
            streams: Streams::new(),
            released: VecDeque::new(),
            events,
            _to_driver: to_driver,
            budget,
            opening_uni: None,
            opening_bi: None,
            started: Instant::now(),
            closed: false,
            reported_closed: false,
            batch: BatchGate::new(),
        }
    }

    fn fail(error: impl core::fmt::Display) -> QuinnError {
        QuinnError(error.to_string())
    }
}

/// Accepts everything the peer opens, for as long as the connection lives.
fn spawn_acceptor(
    quic: quinn::Connection,
    to_driver: mpsc::UnboundedSender<Incoming>,
    budget: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let uni = quic.clone();
    let uni_sender = to_driver.clone();
    let uni_budget = budget.clone();
    tokio::spawn(async move {
        loop {
            match uni.accept_uni().await {
                Ok(recv) => spawn_reader(recv, None, uni_sender.clone(), uni_budget.clone()),
                Err(_) => {
                    let _ = uni_sender.send(Incoming::Closed);
                    return;
                }
            }
        }
    });

    tokio::spawn(async move {
        loop {
            match quic.accept_bi().await {
                Ok((send, recv)) => {
                    let stream = to_stream_id(send.id());
                    let (stop, receive_stop) = oneshot::channel();
                    spawn_send_observer(send.stopped(), stream, to_driver.clone());
                    // The sending half has to reach the driver *before* the stream is
                    // announced, and it must not be dropped on the way: quinn resets a
                    // stream whose sending half goes away, so a response would be refused
                    // before it could be written.
                    if to_driver
                        .send(Incoming::Accepted { stream, send, stop })
                        .is_err()
                    {
                        return;
                    }
                    spawn_reader(recv, Some(receive_stop), to_driver.clone(), budget.clone());
                }
                Err(_) => {
                    let _ = to_driver.send(Incoming::Closed);
                    return;
                }
            }
        }
    });
}

/// Observes peer STOP_SENDING even when the HTTP/3 layer has no bytes left to offer.
fn spawn_send_observer(
    stopped: impl Future<Output = Result<Option<quinn::VarInt>, quinn::StoppedError>> + Send + 'static,
    stream: StreamId,
    to_driver: mpsc::UnboundedSender<Incoming>,
) {
    tokio::spawn(async move {
        match stopped.await {
            Ok(Some(code)) => {
                let _ = to_driver.send(Incoming::StopSending {
                    stream,
                    code: ErrorCode::new(code.into_inner()),
                });
            }
            Ok(None) => {
                let _ = to_driver.send(Incoming::SendStopped { stream, code: None });
            }
            Err(quinn::StoppedError::ConnectionLost(_)) => {
                let _ = to_driver.send(Incoming::Closed);
            }
            Err(quinn::StoppedError::ZeroRttRejected) => {
                let _ = to_driver.send(Incoming::SendStopped {
                    stream,
                    code: Some(ErrorCode::new(0x102)),
                });
            }
        }
    });
}

/// Reads one stream until it ends, forwarding everything.
fn spawn_reader(
    mut recv: quinn::RecvStream,
    mut stop: Option<StopReceiver>,
    to_driver: mpsc::UnboundedSender<Incoming>,
    budget: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let stream = to_stream_id(recv.id());
    tokio::spawn(async move {
        loop {
            // Read-ahead is bounded by the credit the layer has extended. Without this the
            // channel below is an unbounded buffer a fast peer controls.
            while budget.load(std::sync::atomic::Ordering::Acquire) == 0 {
                if let Some(receiver) = stop.as_mut() {
                    tokio::select! {
                        signal = receiver => {
                            stop = None;
                            match signal {
                                Ok(code) if recv.stop(varint(code)).is_ok() => {
                                    let _ = to_driver.send(Incoming::RecvStopped { stream, code });
                                    return;
                                }
                                _ => continue,
                            }
                        }
                        _ = tokio::task::yield_now() => {}
                    }
                } else {
                    tokio::task::yield_now().await;
                }
                if to_driver.is_closed() {
                    return;
                }
            }

            let read = if let Some(receiver) = stop.as_mut() {
                tokio::select! {
                    signal = receiver => {
                        stop = None;
                        match signal {
                            Ok(code) if recv.stop(varint(code)).is_ok() => {
                                let _ = to_driver.send(Incoming::RecvStopped { stream, code });
                                return;
                            }
                            _ => continue,
                        }
                    }
                    read = recv.read_chunk(READ_CHUNK, true) => read,
                }
            } else {
                recv.read_chunk(READ_CHUNK, true).await
            };

            match read {
                Ok(Some(chunk)) => {
                    let len = chunk.bytes.len() as u64;
                    budget.fetch_sub(
                        len.min(budget.load(std::sync::atomic::Ordering::Acquire)),
                        std::sync::atomic::Ordering::AcqRel,
                    );
                    if to_driver
                        .send(Incoming::Data {
                            stream,
                            bytes: chunk.bytes,
                            fin: false,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => {
                    let _ = to_driver.send(Incoming::Data {
                        stream,
                        bytes: bytes::Bytes::new(),
                        fin: true,
                    });
                    return;
                }
                Err(error) => {
                    // Reported rather than swallowed. A reader that simply exited would
                    // leave the driver waiting for an end-of-stream that is never coming.
                    match error {
                        quinn::ReadError::ConnectionLost(_) => {
                            let _ = to_driver.send(Incoming::Closed);
                        }
                        quinn::ReadError::Reset(code) => {
                            let _ = to_driver.send(Incoming::Reset {
                                stream,
                                code: ErrorCode::new(code.into_inner()),
                            });
                        }
                        _ => {
                            let _ = to_driver.send(Incoming::Reset {
                                stream,
                                code: ErrorCode::new(0x102),
                            });
                        }
                    }
                    return;
                }
            }
        }
    });
}

fn to_stream_id(id: quinn::StreamId) -> StreamId {
    StreamId::new(u64::from(id) as i64).expect("quinn produces valid stream identifiers")
}

impl QuicConnection for QuinnBackend {
    type Error = QuinnError;

    // quinn's `write` copies into its own buffers, so the bytes belong to the application
    // again the moment it returns -- which is what makes reporting release on acceptance
    // sound here, rather than waiting for the peer as a borrowing transport must.
    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        if self.reported_closed {
            self.batch.pending();
            return Poll::Pending;
        }

        loop {
            if self.closed {
                if self.batch.take_boundary() {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                self.streams.clear();
                self.released.clear();
                self.reported_closed = true;
                self.batch.emitted();
                return Poll::Ready(Ok(QuicEvent::Closed { code: None }));
            }

            if let Some(closed) = self.streams.pending_close() {
                let release_owed = self
                    .released
                    .iter()
                    .any(|(stream, _)| *stream == closed.stream);
                if !release_owed {
                    if self.batch.take_boundary() {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    let closed = self.streams.pop_close().expect("the close just observed");
                    self.batch.emitted();
                    return Poll::Ready(Ok(QuicEvent::StreamClosed {
                        stream: closed.stream,
                        rx_code: closed.rx_code,
                        tx_code: closed.tx_code,
                    }));
                }
            }

            // Releases normally go first because they free the layer's buffers. A ready
            // close may pass releases for other streams, however: only bytes accepted on
            // the closing stream have to precede that stream's close.
            if let Some((stream, bytes)) = self.released.pop_front() {
                self.batch.emitted();
                return Poll::Ready(Ok(QuicEvent::Released {
                    stream,
                    bytes,
                    delivered: true,
                }));
            }

            match self.events.poll_recv(cx) {
                Poll::Pending => {
                    self.batch.pending();
                    return Poll::Pending;
                }
                Poll::Ready(None) | Poll::Ready(Some(Incoming::Closed)) => {
                    self.closed = true;
                }
                Poll::Ready(Some(Incoming::Data { stream, bytes, fin })) => {
                    if fin {
                        self.streams.finish_recv(stream, None);
                    }
                    self.batch.emitted();
                    return Poll::Ready(Ok(QuicEvent::Data { stream, bytes, fin }));
                }
                Poll::Ready(Some(Incoming::Accepted { stream, send, stop })) => {
                    self.streams.insert_bidi(stream, send, stop);
                    self.batch.emitted();
                    return Poll::Ready(Ok(QuicEvent::Accepted { stream }));
                }
                Poll::Ready(Some(Incoming::Reset { stream, code })) => {
                    self.streams.finish_recv(stream, Some(code));
                    self.batch.emitted();
                    return Poll::Ready(Ok(QuicEvent::Reset { stream, code }));
                }
                Poll::Ready(Some(Incoming::RecvStopped { stream, code })) => {
                    self.streams.finish_recv(stream, Some(code));
                }
                Poll::Ready(Some(Incoming::StopSending { stream, code })) => {
                    self.streams.finish_send(stream, Some(code));
                    self.batch.emitted();
                    return Poll::Ready(Ok(QuicEvent::StopSending { stream, code }));
                }
                Poll::Ready(Some(Incoming::SendStopped { stream, code })) => {
                    self.streams.finish_send(stream, code);
                }
            }
        }
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        let streams = &mut self.streams;
        let released = &mut self.released;
        let to_driver = self._to_driver.clone();

        while source.write_next(&mut |stream, slices, fin| {
            if streams.send_mut(stream).is_none() {
                return WriteOutcome::Gone;
            }

            // An offer that carries only the end has no bytes to write; finishing the
            // stream *is* the write. Declining it would leave the peer waiting for an
            // end it was never told about.
            let total: usize = slices.iter().map(|s| s.len()).sum();
            if total == 0 {
                if fin && !streams.send_finished(stream) {
                    let result = streams
                        .send_mut(stream)
                        .expect("the send was just observed")
                        .finish();
                    if result.is_err() {
                        return WriteOutcome::Gone;
                    }
                    streams.finish_send(stream, None);
                }
                return WriteOutcome::Accepted(0);
            }

            // quinn writes one slice at a time. Writing the first non-empty one and
            // reporting a short take is correct: the state machine re-offers the rest,
            // and the driver blocks the stream so another gets a turn first.
            let first = slices.iter().find(|slice| !slice.is_empty());
            let Some(first) = first else {
                return WriteOutcome::Accepted(0);
            };

            let write = Pin::new(
                streams
                    .send_mut(stream)
                    .expect("the send was just observed"),
            )
            .poll_write(cx, first);
            match write {
                Poll::Pending => WriteOutcome::Blocked,
                Poll::Ready(Err(quinn::WriteError::Stopped(code))) => {
                    streams.finish_send(stream, Some(ErrorCode::new(code.into_inner())));
                    WriteOutcome::Gone
                }
                Poll::Ready(Err(quinn::WriteError::ConnectionLost(_))) => {
                    let _ = to_driver.send(Incoming::Closed);
                    WriteOutcome::Gone
                }
                Poll::Ready(Err(quinn::WriteError::ClosedStream)) => WriteOutcome::Gone,
                Poll::Ready(Err(quinn::WriteError::ZeroRttRejected)) => {
                    streams.finish_send(stream, Some(ErrorCode::new(0x102)));
                    WriteOutcome::Gone
                }
                Poll::Ready(Ok(written)) => {
                    if written > 0 {
                        // Reported on acceptance, which is sound only because quinn
                        // copied: see `RETAINS_BUFFERS`. A transport that borrowed the
                        // bytes instead would have to wait for the peer.
                        released.push_back((stream, written as u64));
                    }
                    if fin && written == total && !streams.send_finished(stream) {
                        let result = streams
                            .send_mut(stream)
                            .expect("the send was just observed")
                            .finish();
                        if result.is_err() {
                            return WriteOutcome::Gone;
                        }
                        streams.finish_send(stream, None);
                    }
                    WriteOutcome::Accepted(written)
                }
            }
        }) {}

        if self.closed {
            cx.waker().wake_by_ref();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Quinn copies accepted writes into its own send buffers.
        Poll::Ready(Ok(()))
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        // `open_uni` borrows the connection, so it cannot be held across polls without
        // self-reference. `quinn::Connection` is cheap to clone, so the future owns one.
        let opening = self.opening_uni.get_or_insert_with(|| {
            let quic = self.quic.clone();
            Box::pin(async move { quic.open_uni().await })
        });
        match opening.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.opening_uni = None;
                Poll::Ready(Err(Self::fail(error)))
            }
            Poll::Ready(Ok(send)) => {
                self.opening_uni = None;
                let stream = to_stream_id(send.id());
                self.streams.insert_uni(stream, send);
                Poll::Ready(Ok(stream))
            }
        }
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let opening = self.opening_bi.get_or_insert_with(|| {
            let quic = self.quic.clone();
            Box::pin(async move { quic.open_bi().await })
        });
        match opening.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.opening_bi = None;
                Poll::Ready(Err(Self::fail(error)))
            }
            Poll::Ready(Ok((send, recv))) => {
                self.opening_bi = None;
                let stream = to_stream_id(send.id());
                let (stop, receive_stop) = oneshot::channel();
                spawn_send_observer(send.stopped(), stream, self._to_driver.clone());
                self.streams.insert_bidi(stream, send, stop);
                // The receiving half must be read, not dropped: quinn turns a dropped
                // receiving half into STOP_SENDING, so the peer's answer would be reset
                // before it was written.
                spawn_reader(
                    recv,
                    Some(receive_stop),
                    self._to_driver.clone(),
                    self.budget.clone(),
                );
                Poll::Ready(Ok(stream))
            }
        }
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        if self.streams.send_finished(stream) {
            return Ok(());
        }
        let reset = self
            .streams
            .send_mut(stream)
            .map(|send| send.reset(varint(code)));
        if matches!(reset, Some(Ok(()))) {
            self.streams.finish_send(stream, Some(code));
        }
        Ok(())
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        if let Some(stop) = self.streams.take_stop(stream) {
            let _ = stop.send(code);
        }
        Ok(())
    }

    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        // No window update: quinn issues those itself when a chunk is read. What this does
        // is advance the reader tasks' own budget, which is a different limit — see
        // `INITIAL_BUDGET`. The driver reports every consumed byte once for its stream and
        // once for the connection; this adapter has one connection-wide pool, so account for
        // only the connection-level report.
        if stream.is_none() {
            self.budget
                .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        }
        Ok(())
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error> {
        if !self.closed {
            self.closed = true;
            self.quic.close(varint(code), reason);
        }
        Ok(())
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.started.elapsed().as_nanos() as u64)
    }
}

fn varint(code: ErrorCode) -> quinn::VarInt {
    quinn::VarInt::from_u64(code.get()).unwrap_or_else(|_| quinn::VarInt::from_u32(0))
}
