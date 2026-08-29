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
//! # Four things this must get right
//!
//! Each was learned the hard way by the sans-I/O harness and is commented where it applies:
//! a dropped receiving half becomes `STOP_SENDING`; the sending half of an accepted stream
//! must be reachable before the stream is announced; release may be reported on acceptance
//! only because quinn copies; and a reader task that exits quietly leaves the driver waiting
//! for an end that will never come.

#![deny(missing_docs)]
#![deny(unsafe_code)]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::time::Instant;

use ngnet_h3::http::quic::Timestamp;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_h3::{ErrorCode, StreamId};
use tokio::sync::mpsc;

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
    },
    Reset {
        stream: StreamId,
        code: ErrorCode,
    },
    Closed,
}

/// A [`QuicConnection`] over an established `quinn::Connection`.
pub struct QuinnBackend {
    quic: quinn::Connection,
    /// The sending half of every stream this endpoint may write to.
    sends: HashMap<i64, quinn::SendStream>,
    /// Releases owed to the layer, produced by writes and drained by `poll_event`.
    released: Vec<(StreamId, u64)>,
    events: mpsc::UnboundedReceiver<Incoming>,
    /// Held so the channel never closes of its own accord.
    _to_driver: mpsc::UnboundedSender<Incoming>,
    /// Streams whose sending half has been finished.
    finished: Vec<i64>,
    /// How many more bytes the reader tasks may deliver.
    budget: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// An in-progress stream open must survive `Poll::Pending`.
    opening_uni: Option<OpeningUni>,
    /// An in-progress stream open must survive `Poll::Pending`.
    opening_bi: Option<OpeningBi>,
    started: Instant,
    closed: bool,
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
            sends: HashMap::new(),
            released: Vec::new(),
            events,
            _to_driver: to_driver,
            finished: Vec::new(),
            budget,
            opening_uni: None,
            opening_bi: None,
            started: Instant::now(),
            closed: false,
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
                Ok(recv) => spawn_reader(recv, uni_sender.clone(), uni_budget.clone()),
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
                    // The sending half has to reach the driver *before* the stream is
                    // announced, and it must not be dropped on the way: quinn resets a
                    // stream whose sending half goes away, so a response would be refused
                    // before it could be written.
                    if to_driver.send(Incoming::Accepted { stream, send }).is_err() {
                        return;
                    }
                    spawn_reader(recv, to_driver.clone(), budget.clone());
                }
                Err(_) => {
                    let _ = to_driver.send(Incoming::Closed);
                    return;
                }
            }
        }
    });
}

/// Reads one stream until it ends, forwarding everything.
fn spawn_reader(
    mut recv: quinn::RecvStream,
    to_driver: mpsc::UnboundedSender<Incoming>,
    budget: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let stream = to_stream_id(recv.id());
    tokio::spawn(async move {
        loop {
            // Read-ahead is bounded by the credit the layer has extended. Without this the
            // channel below is an unbounded buffer a fast peer controls.
            while budget.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
                if to_driver.is_closed() {
                    return;
                }
            }

            match recv.read_chunk(READ_CHUNK, true).await {
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
                    let code = match &error {
                        quinn::ReadError::Reset(code) => ErrorCode::new(code.into_inner()),
                        _ => ErrorCode::new(0x102),
                    };
                    let _ = to_driver.send(Incoming::Reset { stream, code });
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
        // Releases first: they free memory, and queueing them behind inbound data would
        // hold retained buffers for no reason.
        if let Some((stream, bytes)) = self.released.pop() {
            return Poll::Ready(Ok(QuicEvent::Released {
                stream,
                bytes,
                delivered: true,
            }));
        }

        match self.events.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Ok(QuicEvent::Closed { code: None })),
            Poll::Ready(Some(Incoming::Data { stream, bytes, fin })) => {
                Poll::Ready(Ok(QuicEvent::Data { stream, bytes, fin }))
            }
            Poll::Ready(Some(Incoming::Accepted { stream, send })) => {
                self.sends.insert(stream.get(), send);
                Poll::Ready(Ok(QuicEvent::Accepted { stream }))
            }
            Poll::Ready(Some(Incoming::Reset { stream, code })) => {
                Poll::Ready(Ok(QuicEvent::Reset { stream, code }))
            }
            Poll::Ready(Some(Incoming::Closed)) => {
                self.closed = true;
                Poll::Ready(Ok(QuicEvent::Closed { code: None }))
            }
        }
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        let mut failure = None;

        {
            let sends = &mut self.sends;
            let released = &mut self.released;
            let finished = &mut self.finished;
            let failure = &mut failure;

            while source.write_next(&mut |stream, slices, fin| {
                let Some(send) = sends.get_mut(&stream.get()) else {
                    return WriteOutcome::Gone;
                };

                // An offer that carries only the end has no bytes to write; finishing the
                // stream *is* the write. Declining it would leave the peer waiting for an
                // end it was never told about.
                let total: usize = slices.iter().map(|s| s.len()).sum();
                if total == 0 {
                    if fin && !finished.contains(&stream.get()) {
                        finished.push(stream.get());
                        if send.finish().is_err() {
                            return WriteOutcome::Gone;
                        }
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

                match Pin::new(&mut *send).poll_write(cx, first) {
                    Poll::Pending => WriteOutcome::Blocked,
                    Poll::Ready(Err(_)) => WriteOutcome::Gone,
                    Poll::Ready(Ok(written)) => {
                        if written > 0 {
                            // Reported on acceptance, which is sound only because quinn
                            // copied: see `RETAINS_BUFFERS`. A transport that borrowed the
                            // bytes instead would have to wait for the peer.
                            released.push((stream, written as u64));
                        }
                        if fin && written == total && !finished.contains(&stream.get()) {
                            finished.push(stream.get());
                            if send.finish().is_err() {
                                return WriteOutcome::Gone;
                            }
                        }
                        WriteOutcome::Accepted(written)
                    }
                }
            }) {
                if failure.is_some() {
                    break;
                }
            }
        }

        match failure {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(())),
        }
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
                self.sends.insert(stream.get(), send);
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
                self.sends.insert(stream.get(), send);
                // The receiving half must be read, not dropped: quinn turns a dropped
                // receiving half into STOP_SENDING, so the peer's answer would be reset
                // before it was written.
                spawn_reader(recv, self._to_driver.clone(), self.budget.clone());
                Poll::Ready(Ok(stream))
            }
        }
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        if let Some(send) = self.sends.get_mut(&stream.get()) {
            let _ = send.reset(varint(code));
        }
        Ok(())
    }

    fn stop_sending(&mut self, _stream: StreamId, _code: ErrorCode) -> Result<(), Self::Error> {
        // The receiving halves live in their reader tasks, which own them for the duration.
        // Stopping the peer means dropping the half, and quinn does that itself when the
        // task ends; reaching in from here would need a second channel per stream for a
        // signal the peer will see anyway once this endpoint resets or closes.
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
