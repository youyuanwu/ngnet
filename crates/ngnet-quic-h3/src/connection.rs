//! The transport the HTTP/3 layer runs over.

use core::net::SocketAddr;
use core::task::{Context, Poll, Waker};

use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource};
use ngnet_h3::{ErrorCode, Timestamp as H3Timestamp};
use ngnet_quic::endpoint::{DetachedConnection, Endpoint, Observed};
use ngnet_quic::{
    ApplicationErrorCode, Directionality, ErrorKind as QuicErrorKind, Initiator, Role, Session,
    StreamId, Timestamp,
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
    /// Coarse backup sleep for an imminent deadline whose ordinary wake could be missed.
    pub(crate) fallback_sleeping: Option<Sleep>,
    /// Original connection deadline protected by `fallback_sleeping`.
    pub(crate) fallback_for: Option<Timestamp>,
    /// Whether the latest stream drain stopped on transport/pacing block without progress.
    pub(crate) timer_fallback_needed: bool,
    /// Whether the previous pass parked on a full outbound queue.
    #[cfg(feature = "diagnostics")]
    pub(crate) capacity_parked: bool,
    /// Whether the connection future most recently returned `Pending`.
    #[cfg(feature = "diagnostics")]
    pub(crate) idle_parked: bool,
    /// Wakers waiting for a stream limit to rise.
    pub(crate) limit_wakers: Vec<Waker>,
    /// Streams this end opened, in the order they were opened, awaiting collection.
    pub(crate) opened_bidi: std::collections::VecDeque<StreamId>,
    /// As above, unidirectional.
    pub(crate) opened_uni: std::collections::VecDeque<StreamId>,
    /// A datagram buffer reused across passes.
    ///
    /// Every datagram this crate produces is handed to the endpoint's queue, which takes
    /// ownership and may hold it across passes -- so one owned allocation per datagram is
    /// forced and cannot be avoided. What this buffer avoids is a *second* allocation: the
    /// connection writes each datagram directly into an owned buffer that is then handed
    /// over as itself, rather than into a scratch that is copied out. The one buffer that a
    /// pass does not send -- the one the final "nothing more to write" probe wrote into --
    /// is kept here and reused next pass, so a settled connection's pass allocates nothing
    /// and a pass producing `n` datagrams allocates exactly `n`.
    pub(crate) scratch: Vec<u8>,
}

/// An established QUIC connection, in the shape the HTTP/3 layer runs over.
///
/// Obtained from [`connect`] or [`accept`], both of which drive the handshake first: the
/// abstraction this implements begins with an established connection, and handing over an
/// unfinished one would give the layer something that cannot carry a request.
pub struct NgtcpConnection<S: Session> {
    detached: DetachedConnection<S>,
    shared: Shared,
    state: State,
    local: Initiator,
}

impl<S: Session> NgtcpConnection<S> {
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
                fallback_sleeping: None,
                fallback_for: None,
                timer_fallback_needed: false,
                #[cfg(feature = "diagnostics")]
                capacity_parked: false,
                #[cfg(feature = "diagnostics")]
                idle_parked: false,
                limit_wakers: Vec::new(),
                opened_bidi: std::collections::VecDeque::new(),
                opened_uni: std::collections::VecDeque::new(),
                // Sized once here, off any counted path: a connection is built after its
                // handshake, so this allocation never falls inside a send pass.
                scratch: vec![0u8; pump::MAX_DATAGRAM],
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

    /// Reads whatever the endpoint has routed here, and nothing more.
    ///
    /// Not a supported API: it exists so an allocation-counting test can leave the
    /// connection owing an acknowledgement without also producing it, so the produce pass it
    /// then measures starts from a known debt. This is the read half of [`pump`](pump::pump)
    /// with the timer and the produce step removed.
    #[doc(hidden)]
    pub fn intake_for_test(&mut self) -> Result<()> {
        let now = self.detached.now();
        while let Some(datagram) = self.detached.next_inbound() {
            self.detached
                .conn
                .read_pkt(&datagram, now)
                .map_err(Error::transport)?;
        }
        Ok(())
    }

    /// Runs a single produce pass and reports how many datagrams it queued.
    ///
    /// Not a supported API: it exists so an allocation-counting test can measure the produce
    /// pass on its own — the send path that owes acknowledgements and probes, and stages no
    /// stream data, so the only allocation it can force is the one owned buffer per datagram
    /// the endpoint's queue takes ownership of. Marked hidden and carrying no compatibility
    /// promise, in the same spirit as `ngnet-quic`'s `endpoint::testing`.
    #[doc(hidden)]
    pub fn produce_pass_for_test(&mut self) -> Result<usize> {
        let before = self.detached.outbound_len_for_test();
        pump::produce(&mut self.detached, &mut self.state, None)?;
        Ok(self.detached.outbound_len_for_test() - before)
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
                    // ngtcp2 never restores peer stream credit automatically. A completed
                    // peer-opened stream frees one concurrent-stream slot, so grant it back
                    // here before the peer reaches the advertised lifetime total and stalls.
                    if id.initiator() != self.local {
                        match id.directionality() {
                            Directionality::Bidirectional => {
                                self.detached.conn.extend_max_streams_bidi(1);
                            }
                            Directionality::Unidirectional => {
                                self.detached.conn.extend_max_streams_uni(1);
                            }
                        }
                    }
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
    fn poll_open(&mut self, cx: &mut Context<'_>, bidi: bool) -> Poll<Result<H3StreamId>> {
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
                if let Err(err) =
                    pump::produce(&mut self.detached, &mut self.state, Some(cx.waker()))
                {
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

impl<S: Session> QuicConnection for NgtcpConnection<S> {
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
        #[cfg(feature = "diagnostics")]
        if core::mem::take(&mut self.state.idle_parked) {
            ngnet_quic::diagnostics::record_driver_wake(
                self.detached.conn.diagnostic_id(),
                self.detached.conn.role(),
            );
        }
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
        #[cfg(feature = "diagnostics")]
        {
            self.state.idle_parked = true;
            ngnet_quic::diagnostics::record_park(
                self.detached.conn.diagnostic_id(),
                self.detached.conn.role(),
            );
        }
        self.state.emitted_since_pending = false;
        Poll::Pending
    }

    fn poll_transmit<Src: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut Src,
    ) -> Poll<Result<()>> {
        #[cfg(feature = "diagnostics")]
        if core::mem::take(&mut self.state.idle_parked) {
            ngnet_quic::diagnostics::record_driver_wake(
                self.detached.conn.diagnostic_id(),
                self.detached.conn.role(),
            );
        }
        pump::pump(&mut self.detached, &self.shared, &mut self.state, cx)?;
        self.collect();
        if self.state.closed {
            return Poll::Ready(Err(pump::ended()));
        }
        transmit::drain(
            &mut self.detached,
            &self.shared,
            &mut self.state,
            source,
            cx,
        )?;
        let _ = pump::poll_timer(&self.detached, &mut self.state, cx);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Every parkable operation starts by producing the datagrams ngtcp2 currently
        // owes and handing them to the endpoint. Accepted stream bytes therefore leave no
        // join-owned output for a later driver pass.
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
        pump::produce(&mut self.detached, &mut self.state, None)
    }

    fn stop_sending(&mut self, stream: H3StreamId, code: ErrorCode) -> Result<()> {
        let id = quic_stream(stream);
        self.detached
            .conn
            .stop_sending(id, ApplicationErrorCode::new(code.get()))
            .map_err(Error::transport)?;
        pump::produce(&mut self.detached, &mut self.state, None)
    }

    fn extend_credit(&mut self, stream: Option<H3StreamId>, bytes: u64) -> Result<()> {
        #[cfg(feature = "diagnostics")]
        let role = self.detached.conn.role();
        match stream {
            Some(stream) => {
                let id = quic_stream(stream);
                self.detached
                    .conn
                    .extend_max_stream_offset(id, bytes)
                    .map_err(Error::transport)?;
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_credit(role, true, bytes);
            }
            // The connection window, which is shared across every stream. The layer calls
            // once per level for the same bytes; extending only one stalls the connection
            // once enough total has flowed, late and with nothing to explain it.
            None => {
                self.detached.conn.extend_max_offset(bytes);
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_credit(role, false, bytes);
            }
        }
        Ok(())
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<()> {
        if self.state.closed {
            self.detached.release();
            return Ok(());
        }

        // The close datagram is produced and queued here, synchronously. The HTTP/3 driver
        // calls this last and then returns, so a close that only recorded an intention would
        // never reach the peer, which would wait out its idle timeout instead.
        //
        // Written straight into the buffer that is handed over, rather than into a scratch
        // and copied: the endpoint's queue takes ownership, so this one allocation is forced
        // and the copy that used to sit beside it is not.
        let mut datagram = core::mem::take(&mut self.state.scratch);
        datagram.resize(pump::MAX_DATAGRAM, 0);
        let now = self.detached.now();
        match self.detached.conn.write_connection_close(
            &mut datagram,
            ApplicationErrorCode::new(code.get()),
            reason,
            now,
        ) {
            Ok(len) if len > 0 => {
                datagram.truncate(len);
                self.detached.send_close(datagram);
            }
            Ok(_) => {
                datagram.clear();
                self.state.scratch = datagram;
            }
            Err(err) => {
                datagram.clear();
                self.state.scratch = datagram;
                return Err(Error::transport(err));
            }
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

impl<S: Session> Drop for NgtcpConnection<S> {
    fn drop(&mut self) {
        // The endpoint cannot tell that a connection it does not hold has finished, so
        // saying so is this type's job. Without it the routing entries outlive the
        // connection for as long as the endpoint runs.
        self.detached.release();
    }
}

impl<S: Session> core::fmt::Debug for NgtcpConnection<S> {
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
pub async fn connect<S: Session>(
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
pub async fn accept<S: Session>(endpoint: &Endpoint<S>) -> Result<NgtcpConnection<S>> {
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
