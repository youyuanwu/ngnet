//! The transport the HTTP/3 layer runs over.

use core::net::SocketAddr;
use core::task::{Context, Poll, Waker};

use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource};
use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::{ErrorCode, Timestamp as H3Timestamp};
use ngnet_quic::endpoint::{DetachedConnection, Endpoint, Observed};
use ngnet_quic::{
    ApplicationErrorCode, Directionality, ErrorKind as QuicErrorKind, Initiator, Role, StreamId,
    Timestamp, TlsSession,
};

use crate::error::{Error, ErrorKind, Result};
use crate::event::{Recorded, into_event, stream_id};
use crate::{pump, transmit};

/// What the connection's handlers have recorded, waiting to become events.
///
/// Not a lock of its own: the recordings already arrive through the endpoint's shared state,
/// and this only holds what has been translated but not yet handed to the HTTP/3 layer.
#[derive(Default)]
pub(crate) struct Shared {
    pending: std::sync::Mutex<std::collections::VecDeque<Recorded>>,
}

impl Shared {
    fn push(&self, record: Recorded) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(record);
    }

    /// Whether the next record, if any, ends a stream.
    fn peek_kind(&self) -> Option<PeekedKind> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .front()
            .map(|record| PeekedKind {
                ends_a_stream: matches!(
                    record,
                    Recorded::Closed(..) | Recorded::ConnectionClosed(..)
                ),
            })
    }

    fn pop(&self) -> Option<Recorded> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }

    pub(crate) fn record_connection_closed(&self, close: ngnet_quic::CloseError) {
        // Only an application close carries a code the HTTP/3 layer can use. A transport
        // close, an idle timeout or a Retry are all "the connection ended" as far as it is
        // concerned, and inventing a code for them would be a lie about what the peer said.
        let code = match close.reason() {
            ngnet_quic::CloseReason::Application(code) => Some(*code),
            _ => None,
        };
        self.push(Recorded::ConnectionClosed(code));
    }

    /// Records that written bytes are the layer's own again.
    pub(crate) fn record_released(&self, stream: StreamId, bytes: u64) {
        self.push(Recorded::Released(stream, bytes));
    }

    pub(crate) fn record_connection_closed_bare(&self) {
        self.push(Recorded::ConnectionClosed(None));
    }
}

/// A sleep armed for one of the connection's deadlines.
pub(crate) use ngnet_quic::endpoint::Sleep;

/// What the next record is, without taking it.
struct PeekedKind {
    ends_a_stream: bool,
}

impl PeekedKind {
    fn ends_a_stream(&self) -> bool {
        self.ends_a_stream
    }
}

/// Everything about driving the connection that is not the connection itself.
pub(crate) struct State {
    /// Set once the connection has ended, so the pump stops touching it.
    pub(crate) closed: bool,
    /// Whether the end has been reported to the HTTP/3 layer.
    pub(crate) reported_closed: bool,
    /// Whether any event has been handed over since the last time this returned pending.
    ///
    /// The HTTP/3 driver drains events in batches and handles the control-plane ones before
    /// the data ones *within* a batch. A stream ending therefore has to start a batch of its
    /// own: put in the same batch as the last bytes of that stream, the close is processed
    /// first, the stream is released, and the bytes that follow it are read against a stream
    /// the state machine has already forgotten. That is a protocol error, and it happens on
    /// the ordinary path where a response ends -- not an edge case.
    pub(crate) emitted_since_pending: bool,
    /// The armed sleep for the connection's expiry.
    pub(crate) sleeping: Option<Sleep>,
    /// The deadline that sleep is for.
    pub(crate) sleeping_until: Option<Timestamp>,
    /// Wakers waiting for a stream limit to rise.
    pub(crate) limit_wakers: Vec<Waker>,
    /// Streams this end opened, in the order they were opened, awaiting collection.
    pub(crate) opened_bidi: std::collections::VecDeque<StreamId>,
    /// As above, unidirectional.
    pub(crate) opened_uni: std::collections::VecDeque<StreamId>,
}

/// An established QUIC connection, in the shape the HTTP/3 layer runs over.
///
/// Obtained from [`connect`] or [`accept`], both of which drive the handshake first: the
/// abstraction this implements begins with an established connection, and handing over an
/// unfinished one would give the layer something that cannot carry a request.
pub struct NgtcpConnection<S: TlsSession> {
    detached: DetachedConnection<S>,
    shared: Shared,
    state: State,
    local: Initiator,
}

impl<S: TlsSession> NgtcpConnection<S> {
    fn new(detached: DetachedConnection<S>, role: Role) -> Self {
        Self {
            detached,
            shared: Shared::default(),
            state: State {
                closed: false,
                reported_closed: false,
                emitted_since_pending: false,
                sleeping: None,
                sleeping_until: None,
                limit_wakers: Vec::new(),
                opened_bidi: std::collections::VecDeque::new(),
                opened_uni: std::collections::VecDeque::new(),
            },
            local: match role {
                Role::Client => Initiator::Client,
                Role::Server => Initiator::Server,
            },
        }
    }

    /// Where the peer is.
    pub fn remote(&self) -> SocketAddr {
        self.detached.remote
    }

    /// How many inbound datagrams the endpoint dropped for want of room here.
    ///
    /// Non-zero means this connection was not keeping up and the endpoint discarded packets
    /// rather than stalling every other connection on the socket. QUIC recovers from that,
    /// but it is worth knowing.
    pub fn dropped_inbound(&self) -> u64 {
        self.detached.dropped_inbound()
    }

    /// Moves what the endpoint's handlers recorded into this crate's queue.
    fn collect(&mut self) {
        for observed in self.detached.take_observed() {
            match observed {
                Observed::Data(id, bytes, fin) => self.shared.push(Recorded::Data(id, bytes, fin)),
                Observed::Opened(id) => self.shared.push(Recorded::PeerOpened(id)),
                Observed::LocallyOpened(id) => {
                    if id.directionality() == Directionality::Bidirectional {
                        self.state.opened_bidi.push_back(id);
                    } else {
                        self.state.opened_uni.push_back(id);
                    }
                }
                // Acknowledgement is *not* what releases the layer's buffers. See
                // `RETAINS_BUFFERS`: this transport copies, so the buffers are the layer's
                // again as soon as a write is accepted, and release is reported there.
                // Reporting it here as well would count every byte twice, which the state
                // machine rejects -- correctly, since that is the shape of an accounting bug
                // that frees a buffer early.
                Observed::Acked(..) => {}
                Observed::Reset(id, code) => self.shared.push(Recorded::Reset(id, code)),
                Observed::StopSending(id, code) => {
                    self.shared.push(Recorded::StopSending(id, code));
                }
                Observed::Closed(id, reason) => {
                    self.shared
                        .push(Recorded::Closed(id, reason.receiving(), reason.sending()));
                }
                Observed::StreamsExtended(_) => {
                    // Not an event for the layer, but it is what releases an open that was
                    // refused for want of room.
                    for waker in self.state.limit_wakers.drain(..) {
                        waker.wake();
                    }
                }
                _ => {}
            }
        }
    }

    /// Opens a stream, waiting rather than failing when the peer's limit is exhausted.
    fn poll_open(
        &mut self,
        cx: &mut Context<'_>,
        bidi: bool,
    ) -> Poll<Result<H3StreamId>> {
        if let Err(err) = pump::pump(&mut self.detached, &self.shared, &mut self.state, cx) {
            return Poll::Ready(Err(err));
        }
        self.collect();
        if self.state.closed {
            return Poll::Ready(Err(pump::ended()));
        }

        let opened = if bidi {
            self.detached.conn.open_bidi_stream()
        } else {
            self.detached.conn.open_uni_stream()
        };

        match opened {
            Ok(id) => {
                // Whatever the open produced still has to reach the peer.
                if let Err(err) = pump::produce(&mut self.detached, &mut self.state) {
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(stream_id(id)))
            }
            // The peer's limit, not a failure. The layer is content to wait, and has no
            // timeout underneath it -- so the wake this registers is the only thing that
            // will ever release it.
            Err(err) if err.kind() == QuicErrorKind::Blocked => {
                let waker = cx.waker();
                if !self.state.limit_wakers.iter().any(|w| w.will_wake(waker)) {
                    self.state.limit_wakers.push(waker.clone());
                }
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(Error::transport(err))),
        }
    }
}

impl<S: TlsSession> QuicConnection for NgtcpConnection<S> {
    type Error = Error;

    /// This transport copies what it is given.
    ///
    /// `ngnet-quic` stages every accepted write into its own allocation, because ngtcp2
    /// keeps the pointer it was handed until the peer acknowledges it and a borrowed slice
    /// cannot outlive the call. So the HTTP/3 layer's buffers are its own again the moment a
    /// write returns, and release is reported immediately.
    ///
    /// Reporting release on *acknowledgement* instead was considered and rejected. It would
    /// be more truthful — `Released` feeds nghttp3's acknowledgement accounting, and this is
    /// the only transport here with a genuine acknowledgement signal — but the copy already
    /// exists, so deferring would hold every in-flight byte twice for no gain. nghttp3 does
    /// not retransmit; QUIC does, out of the copy.
    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent>> {
        pump::pump(&mut self.detached, &self.shared, &mut self.state, cx)?;
        self.collect();

        while let Some(record) = self.shared.peek_kind() {
            // A stream ending waits for a batch of its own. See `emitted_since_pending`.
            if record.ends_a_stream() && self.state.emitted_since_pending {
                self.state.emitted_since_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let Some(record) = self.shared.pop() else {
                break;
            };
            if let Some(event) = into_event(record, self.local) {
                if matches!(event, QuicEvent::Closed { .. }) {
                    self.state.reported_closed = true;
                }
                self.state.emitted_since_pending = true;
                return Poll::Ready(Ok(event));
            }
        }

        if self.state.closed && !self.state.reported_closed {
            self.state.reported_closed = true;
            self.state.emitted_since_pending = true;
            return Poll::Ready(Ok(QuicEvent::Closed { code: None }));
        }

        // Nothing to report. The timer is polled here because the HTTP/3 driver parks on
        // this call, and nothing else will wake it when a loss probe or an acknowledgement
        // becomes due.
        if pump::poll_timer(&self.detached, &mut self.state, cx).is_ready() {
            cx.waker().wake_by_ref();
        }
        self.state.emitted_since_pending = false;
        Poll::Pending
    }

    fn poll_transmit<Src: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut Src,
    ) -> Poll<Result<()>> {
        pump::pump(&mut self.detached, &self.shared, &mut self.state, cx)?;
        self.collect();
        if self.state.closed {
            return Poll::Ready(Err(pump::ended()));
        }
        transmit::drain(&mut self.detached, &self.shared, &mut self.state, source)?;
        let _ = pump::poll_timer(&self.detached, &mut self.state, cx);
        Poll::Ready(Ok(()))
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<H3StreamId>> {
        self.poll_open(cx, false)
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<H3StreamId>> {
        self.poll_open(cx, true)
    }

    fn reset(&mut self, stream: H3StreamId, code: ErrorCode) -> Result<()> {
        let id = quic_stream(stream);
        self.detached
            .conn
            .reset_stream(id, ApplicationErrorCode::new(code.get()))
            .map_err(Error::transport)?;
        pump::produce(&mut self.detached, &mut self.state)
    }

    fn stop_sending(&mut self, stream: H3StreamId, code: ErrorCode) -> Result<()> {
        let id = quic_stream(stream);
        self.detached
            .conn
            .stop_sending(id, ApplicationErrorCode::new(code.get()))
            .map_err(Error::transport)?;
        pump::produce(&mut self.detached, &mut self.state)
    }

    fn extend_credit(&mut self, stream: Option<H3StreamId>, bytes: u64) -> Result<()> {
        match stream {
            Some(stream) => {
                let id = quic_stream(stream);
                self.detached
                    .conn
                    .extend_max_stream_offset(id, bytes)
                    .map_err(Error::transport)?;
            }
            // The connection window, which is shared across every stream. The layer calls
            // once per level for the same bytes; extending only one stalls the connection
            // once enough total has flowed, late and with nothing to explain it.
            None => self.detached.conn.extend_max_offset(bytes),
        }
        Ok(())
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<()> {
        // The close datagram is produced and queued here, synchronously. The HTTP/3 driver
        // calls this last and then returns, so a close that only recorded an intention would
        // never reach the peer, which would wait out its idle timeout instead.
        let mut buffer = vec![0u8; pump::MAX_DATAGRAM];
        let now = self.detached.now();
        match self.detached.conn.write_connection_close(
            &mut buffer,
            ApplicationErrorCode::new(code.get()),
            reason,
            now,
        ) {
            Ok(len) if len > 0 => self.detached.send(buffer[..len].to_vec()),
            Ok(_) => {}
            Err(err) => return Err(Error::transport(err)),
        }
        self.state.closed = true;
        self.detached.release();
        Ok(())
    }

    fn now(&self) -> H3Timestamp {
        // The endpoint's clock, never one of this crate's own: the endpoint drove this
        // connection's handshake against it, and a second clock with a different origin
        // makes every later timestamp incomparable with the ones already recorded.
        H3Timestamp::from_nanos(self.detached.now().as_nanos())
    }
}

impl<S: TlsSession> Drop for NgtcpConnection<S> {
    fn drop(&mut self) {
        // The endpoint cannot tell that a connection it does not hold has finished, so
        // saying so is this type's job. Without it the routing entries outlive the
        // connection for as long as the endpoint runs.
        self.detached.release();
    }
}

impl<S: TlsSession> core::fmt::Debug for NgtcpConnection<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("NgtcpConnection")
            .field("remote", &self.detached.remote)
            .finish_non_exhaustive()
    }
}

fn quic_stream(stream: H3StreamId) -> StreamId {
    StreamId::new(stream.get()).expect("an HTTP/3 stream identifier is a QUIC one")
}

/// Opens a connection and drives it to establishment, ready for the HTTP/3 client.
///
/// The handshake is the endpoint's, not this crate's: it already knows how, and a caller
/// should not have to. What comes back is established, which is what the HTTP/3 layer
/// requires and what makes the first request work rather than hang.
///
/// # Errors
///
/// Fails if the handshake does not complete, or if the endpoint driver is not running.
pub async fn connect<S: TlsSession>(
    endpoint: &Endpoint<S>,
    remote: SocketAddr,
    server_name: Option<&str>,
) -> Result<NgtcpConnection<S>> {
    let detached = endpoint
        .connect_detached(remote, server_name)
        .await
        .map_err(Error::endpoint)?;
    Ok(NgtcpConnection::new(detached, Role::Client))
}

/// Waits for a connection a peer opened and drives it to establishment, ready for the
/// HTTP/3 server. See [`connect`].
///
/// # Errors
///
/// Fails if the endpoint driver is not running.
pub async fn accept<S: TlsSession>(endpoint: &Endpoint<S>) -> Result<NgtcpConnection<S>> {
    let detached = endpoint.accept_detached().await.map_err(Error::endpoint)?;
    Ok(NgtcpConnection::new(detached, Role::Server))
}

const _: () = {
    // The abstraction imposes no `Send` bound, and this implementation does not add one
    // beyond what its parts already require.
    fn _assert_error_is_boxable<E: Into<Box<dyn core::error::Error + Send + Sync>>>() {}
    fn _check() {
        _assert_error_is_boxable::<Error>();
    }
    let _ = ErrorKind::Transport;
};
