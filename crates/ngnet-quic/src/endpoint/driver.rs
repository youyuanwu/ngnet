//! The loop that makes everything happen.
//!
//! # What one pass does, and why in this order
//!
//! 1. **Drain commands** from the handles, bounded, so a caller's writes reach ngtcp2
//!    before this pass produces packets rather than after.
//! 2. **Read datagrams** from the socket, bounded by
//!    [`Config::datagrams_per_pass`](super::Config), routing each to its connection.
//! 3. **Service expired timers**, since a datagram may have made a timer fire and a timer
//!    may produce something to send.
//! 4. **Write**, draining every connection that has anything until it says it has nothing.
//! 5. **Rearm** from the earliest [`Conn::expiry`](crate::Conn::expiry) across all
//!    connections.
//!
//! Steps 2 and 4 are bounded and step 5 is not skippable. Between them those two facts are
//! the whole liveness argument: a connection cannot monopolise a pass, and no pass ends
//! without a timer armed for whatever wants attention next.
//!
//! # The two mistakes this shape exists to avoid
//!
//! **Not rearming after a write pass.** ngtcp2 paces its sending, and reports the pacing
//! deadline through the same `expiry()` as everything else
//! (`deps/ngtcp2/lib/ngtcp2_conn.c:11387`). A driver that only rearms after reading will
//! send one datagram and then sleep until the peer says something — which for a bulk
//! transfer is never, and which looks like a hang rather than a slow connection.
//!
//! **Calling `update_pkt_tx_time` here.** It is tempting, since this is where the pacing is
//! respected. The core's write paths already call it (`src/packet.rs:168-175`,
//! `src/stream_io.rs:177-184`), and calling it again would push the deadline forward twice
//! per packet, halving the send rate for no visible reason.

use core::net::SocketAddr;
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::sync::Arc;

use crate::conn::Conn;
use crate::error::ApplicationErrorCode;
use crate::handlers::Handlers;
use crate::packet::{ExpiryOutcome, ReadOutcome, WriteOutcome};
use crate::stream::StreamId;
use crate::stream_io::StreamWrite;
use crate::time::Timestamp;
use crate::tls::{Role, TlsBackend, TlsSession};

use super::clock::Clock;
use super::config::Config;
use super::error::{Error, ErrorKind};
use super::shared::{Command, ConnectionShared, Observed};
use super::socket::{AsyncUdpSocket, Sent};

/// Makes an entropy source for a new connection.
///
/// `Send` on the factory itself, not just on what it produces. The core already requires an
/// entropy source to be `Send` -- a `Conn` is `Send` and owns one -- so a factory that was
/// not would have to capture thread-bound state in order to produce values that are not,
/// which is a shape nothing wants. Requiring it here is what lets the driver's own `Send`
/// follow from its socket and clock rather than being blocked by this.
pub(crate) type EntropyFactory =
    Box<dyn Fn() -> Box<dyn crate::rand::EntropySource + Send> + Send>;

/// The largest datagram this endpoint will read or write.
///
/// A receive buffer must be big enough for anything a peer may legitimately send, and QUIC
/// permits up to the path MTU. The *send* side is sized from what a connection reports it
/// may currently emit; this is only the ceiling on the buffer, which is capacity rather
/// than permission.
pub(crate) const MAX_DATAGRAM: usize = 1500;

/// One connection the driver owns, with the state its handle shares.
pub(crate) struct Tracked<S: TlsSession> {
    /// The connection. `'static` because its handlers capture an `Arc` rather than
    /// borrowing anything the driver owns — see [`super::shared`].
    pub(crate) conn: Conn<'static, S>,
    /// What the handle reads and writes.
    pub(crate) shared: Arc<ConnectionShared>,
    /// Where its datagrams go.
    pub(crate) remote: SocketAddr,
    /// A datagram the socket refused, to be offered again.
    ///
    /// Holding it here rather than dropping it is what stops a would-blocked send from
    /// silently losing a packet.
    pub(crate) pending: Option<Vec<u8>>,
    /// The connection-close datagram, kept for the closing period.
    ///
    /// `write_connection_close` returns nothing once the connection is in its closing
    /// period, so a close that has to be retransmitted cannot be regenerated — it has to
    /// have been kept. ngtcp2's own server does the same.
    pub(crate) close_datagram: Option<Vec<u8>>,
    /// Whether this connection has been reported finished.
    pub(crate) finished: bool,
}

/// Builds the handler set a driven connection uses.
///
/// Every closure captures an `Arc` and nothing else, which is what makes the resulting
/// `Conn` `'static` and satisfies the `Send` bound the core requires.
pub(crate) fn handlers_for(shared: &Arc<ConnectionShared>) -> Handlers<'static> {
    let data = Arc::clone(shared);
    let opened = Arc::clone(shared);
    let closed = Arc::clone(shared);
    let reset = Arc::clone(shared);
    let stop = Arc::clone(shared);
    let acked = Arc::clone(shared);
    let established = Arc::clone(shared);
    let minted = Arc::clone(shared);
    let retired = Arc::clone(shared);

    Handlers::new()
        .on_stream_data(move |id, bytes, fin| {
            data.observe(Observed::Data(id, bytes.to_vec(), fin));
        })
        .on_stream_open(move |id| opened.observe(Observed::Opened(id)))
        .on_stream_close(move |id, reason| closed.observe(Observed::Closed(id, reason)))
        .on_stream_reset(move |id, code| reset.observe(Observed::Reset(id, code)))
        .on_stop_sending(move |id, code| stop.observe(Observed::StopSending(id, code)))
        .on_acked_stream_data(move |id, len| acked.observe(Observed::Acked(id, len)))
        .on_handshake_completed(move || established.observe(Observed::HandshakeCompleted))
        .on_new_connection_id(move |cid| minted.observe(Observed::IdMinted(*cid)))
        .on_remove_connection_id(move |cid| retired.observe(Observed::IdRetired(*cid)))
}

/// Everything the driver owns.
pub(crate) struct Inner<Sock, Clk, B>
where
    Sock: AsyncUdpSocket,
    Clk: Clock,
    B: TlsBackend,
{
    pub(crate) socket: Sock,
    pub(crate) clock: Clk,
    pub(crate) backend: B,
    pub(crate) config: Config,
    /// Makes an entropy source for each new connection.
    ///
    /// Supplied by the caller for the same reason the clock is: this crate reads no clock
    /// and owns no random number generator, and QUIC needs cryptographically secure
    /// randomness for connection identifiers and stateless reset tokens. Picking one here
    /// would be picking it on the caller's behalf, and picking a weak one would be a
    /// security defect nothing in the API would reveal.
    pub(crate) entropy: EntropyFactory,
    /// Connections, keyed by an index that never repeats.
    pub(crate) connections: HashMap<u64, Tracked<B::Session>>,
    /// Which connection each identifier routes to.
    pub(crate) routes: HashMap<Vec<u8>, u64>,
    /// The next connection index.
    pub(crate) next_index: u64,
    /// A receive buffer, reused so a pass allocates nothing.
    pub(crate) buffer: Vec<u8>,
    /// Whether this endpoint accepts connections it did not initiate.
    pub(crate) accepts: bool,
    /// Datagrams that belong to no connection -- Retry, Version Negotiation, stateless
    /// reset. They cannot be queued on a connection because the whole point of each is that
    /// there is not one.
    pub(crate) outbox: std::collections::VecDeque<(SocketAddr, Vec<u8>)>,
    /// How address validation is configured, if at all.
    #[cfg(feature = "tls-ossl")]
    pub(crate) validation: Option<super::validate::Validation>,
    /// The armed sleep, if any.
    pub(crate) sleeping: Option<Clk::Sleep>,
    /// The deadline the armed sleep is for.
    pub(crate) sleeping_until: Option<Timestamp>,
}

impl<Sock, Clk, B> Inner<Sock, Clk, B>
where
    Sock: AsyncUdpSocket,
    Clk: Clock,
    B: TlsBackend,
{
    /// Registers a connection under every identifier it currently answers to.
    pub(crate) fn install_routes(&mut self, index: u64) {
        let Some(tracked) = self.connections.get(&index) else {
            return;
        };
        let mut identifiers: Vec<Vec<u8>> = tracked
            .conn
            .scids()
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();
        identifiers.push(tracked.conn.scid().as_bytes().to_vec());
        for id in identifiers {
            self.routes.insert(id, index);
        }
    }

    /// Finds the connection a datagram is addressed to.
    ///
    /// By connection identifier, never by source address: QUIC connections survive an
    /// address change, and a table keyed on the address would lose a connection the moment
    /// a NAT rebound. ngtcp2's own server routes the same way.
    pub(crate) fn route(&self, datagram: &[u8]) -> Option<u64> {
        let inspection = crate::accept::inspect(datagram, crate::cid::DEFAULT_LEN).ok()?;
        let dcid = match inspection {
            crate::accept::Inspection::Supported { dcid, .. }
            | crate::accept::Inspection::UnsupportedVersion { dcid, .. }
            | crate::accept::Inspection::ShortHeader { dcid } => dcid,
        };
        self.routes.get(dcid.as_bytes()).copied()
    }

    /// Feeds a datagram to a connection and acts on what its handlers saw.
    pub(crate) fn deliver(&mut self, index: u64, datagram: &[u8]) {
        let now = self.clock.now();
        let Some(tracked) = self.connections.get_mut(&index) else {
            return;
        };
        let outcome = tracked.conn.read_pkt(datagram, now);
        let shared = Arc::clone(&tracked.shared);

        match outcome {
            Ok(ReadOutcome::Processed | ReadOutcome::SendRetry | ReadOutcome::DropSilently) => {}
            Ok(ReadOutcome::Draining | ReadOutcome::Closing) => {
                // The peer closed. What it *said* is on the connection, and is the only
                // application-level explanation available.
                let close = tracked.conn.close_error();
                shared.fail_with_close(close);
            }
            Err(err) => {
                shared.fail(Error::from(err));
            }
        }
        self.absorb(index);
    }

    /// Drains what a connection's handlers recorded and acts on it.
    pub(crate) fn absorb(&mut self, index: u64) {
        let Some(tracked) = self.connections.get(&index) else {
            return;
        };
        let shared = Arc::clone(&tracked.shared);
        let observed = shared.take_observed();
        if observed.is_empty() {
            return;
        }

        let mut new_routes = Vec::new();
        let mut dead_routes = Vec::new();
        let mut established = false;
        let mut credit = 0u64;

        for event in &observed {
            match event {
                Observed::IdMinted(cid) => new_routes.push(cid.as_bytes().to_vec()),
                Observed::IdRetired(cid) => dead_routes.push(cid.as_bytes().to_vec()),
                Observed::HandshakeCompleted => established = true,
                Observed::Data(_, bytes, _) => credit += bytes.len() as u64,
                _ => {}
            }
        }

        for id in new_routes {
            self.routes.insert(id, index);
        }
        for id in dead_routes {
            self.routes.remove(&id);
        }

        if let Some(tracked) = self.connections.get_mut(&index) {
            // Flow control. Reading is what earns credit back, and both levels must be
            // extended: the connection window is shared across every stream, so extending
            // only the stream level stalls the connection once enough total bytes have
            // flowed -- late, and with no error to explain it.
            if credit > 0 {
                for event in &observed {
                    if let Observed::Data(stream, bytes, _) = event {
                        let _ = tracked
                            .conn
                            .extend_max_stream_offset(*stream, bytes.len() as u64);
                    }
                }
                tracked.conn.extend_max_offset(credit);
            }
            tracked
                .shared
                .set_retained(tracked.conn.retained_bytes() as u64);
        }

        if established {
            shared.mark_established();
        }

        // Put back what the handle still needs to see. The driver consumes the routing and
        // flow-control events; everything else belongs to the caller.
        {
            let mut inner = shared.lock();
            for event in observed {
                match event {
                    Observed::IdMinted(_) | Observed::IdRetired(_) => {}
                    other => inner.observed.push(other),
                }
            }
        }
        shared.wake_all();
    }

    /// Runs whatever a handle asked for on one connection.
    pub(crate) fn apply_commands(&mut self, index: u64) {
        let Some(tracked) = self.connections.get(&index) else {
            return;
        };
        let shared = Arc::clone(&tracked.shared);
        for command in shared.take_commands() {
            let Some(tracked) = self.connections.get_mut(&index) else {
                return;
            };
            match command {
                Command::OpenStream { bidi } => {
                    let opened = if bidi {
                        tracked.conn.open_bidi_stream()
                    } else {
                        tracked.conn.open_uni_stream()
                    };
                    match opened {
                        Ok(id) => {
                            shared.observe(Observed::Opened(id));
                            shared.wake_all();
                        }
                        // Running out of stream credit is an ordinary condition in a
                        // working connection, not a failure of it: the peer advertised a
                        // limit and this endpoint has reached it. The request waits for the
                        // peer to raise it rather than tearing the connection down, which
                        // is what a caller means by "open a stream".
                        Err(err) if err.kind() == crate::ErrorKind::Blocked => {
                            shared.push(Command::OpenStream { bidi });
                        }
                        Err(err) => shared.fail(Error::from(err)),
                    }
                }
                Command::Write { stream, data, fin } => {
                    self.write_stream(index, stream, &data, fin);
                }
                Command::Reset(stream, code) => {
                    let _ = tracked.conn.reset_stream(stream, code);
                }
                Command::StopSending(stream, code) => {
                    let _ = tracked.conn.stop_sending(stream, code);
                }
                Command::Close(code, reason) => {
                    self.close_connection(index, code, &reason);
                }
            }
        }
    }

    /// Writes stream data, in chunks a single packet can carry.
    ///
    /// Chunked deliberately. The core copies whatever it is offered and holds the copy
    /// until the peer acknowledges it, so offering a whole large payload on every attempt
    /// would recopy the remainder for every datagram produced. One packet's worth per
    /// offer keeps that bounded.
    pub(crate) fn write_stream(&mut self, index: u64, stream: StreamId, data: &[u8], fin: bool) {
        let now = self.clock.now();
        let Some(tracked) = self.connections.get_mut(&index) else {
            return;
        };

        // One datagram is in flight at a time per connection, so if the previous one has
        // not been handed to the socket yet, this write waits its turn rather than
        // overwriting it -- which would be a silently dropped packet.
        if tracked.pending.is_some() {
            tracked.shared.push(Command::Write {
                stream,
                data: data.to_vec(),
                fin,
            });
            return;
        }

        // Offer at most one packet's worth. The core copies whatever it accepts and holds
        // the copy until the peer acknowledges it, so offering a whole large payload on
        // every attempt would recopy the remainder for every datagram produced.
        let permitted = tracked.conn.max_tx_udp_payload_size().max(1);
        let chunk_len = data.len().min(permitted);
        // The end-of-stream flag belongs on the *last* write, never the first: setting it
        // early closes the write side, and the next attempt is refused with
        // `STREAM_SHUT_WR`.
        let last = fin && chunk_len == data.len();

        let mut datagram = vec![0u8; MAX_DATAGRAM];
        let outcome = tracked
            .conn
            .write_stream(&mut datagram, stream, &data[..chunk_len], last, now);

        match outcome {
            Ok(StreamWrite::Datagram { len, accepted }) => {
                datagram.truncate(len);
                tracked.pending = Some(datagram);
                tracked
                    .shared
                    .set_retained(tracked.conn.retained_bytes() as u64);
                // Whatever was not accepted -- which may be everything, since a packet can
                // fill with control frames instead -- goes back on the queue.
                if accepted < data.len() {
                    tracked.shared.push(Command::Write {
                        stream,
                        data: data[accepted..].to_vec(),
                        fin,
                    });
                }
            }
            Ok(StreamWrite::StreamBlocked
            | StreamWrite::ConnectionBlocked
            | StreamWrite::Blocked
            | StreamWrite::Idle) => {
                // Not an error and not the end. Requeue and try again once credit or the
                // congestion window allows; treating any of these as "finished" is the
                // classic QUIC stall.
                tracked.shared.push(Command::Write {
                    stream,
                    data: data.to_vec(),
                    fin,
                });
            }
            Err(err) => {
                let shared = Arc::clone(&tracked.shared);
                shared.fail(Error::from(err));
            }
        }
    }

    /// Writes a connection close and keeps it for the closing period.
    pub(crate) fn close_connection(
        &mut self,
        index: u64,
        code: ApplicationErrorCode,
        reason: &[u8],
    ) {
        let now = self.clock.now();
        let Some(tracked) = self.connections.get_mut(&index) else {
            return;
        };
        let mut datagram = vec![0u8; MAX_DATAGRAM];
        if let Ok(len) = tracked
            .conn
            .write_connection_close(&mut datagram, code, reason, now)
            && len > 0
        {
            datagram.truncate(len);
            tracked.close_datagram = Some(datagram.clone());
            tracked.pending = Some(datagram);
        }
        tracked
            .shared
            .fail(Error::new(ErrorKind::LocallyClosed, "closed by this endpoint"));
    }

    /// Produces the next datagram a connection wants to send, if any.
    pub(crate) fn next_datagram(&mut self, index: u64) -> Option<Vec<u8>> {
        let now = self.clock.now();
        let tracked = self.connections.get_mut(&index)?;

        if let Some(pending) = tracked.pending.take() {
            return Some(pending);
        }

        let mut datagram = vec![0u8; MAX_DATAGRAM];
        match tracked.conn.write_pkt(&mut datagram, now) {
            Ok(WriteOutcome::Datagram { len }) => {
                datagram.truncate(len);
                Some(datagram)
            }
            Ok(WriteOutcome::Blocked | WriteOutcome::Idle) => None,
            Err(err) => {
                let shared = Arc::clone(&tracked.shared);
                shared.fail(Error::from(err));
                None
            }
        }
    }

    /// Services a connection whose timer has fired.
    pub(crate) fn handle_expiry(&mut self, index: u64) {
        let now = self.clock.now();
        let Some(tracked) = self.connections.get_mut(&index) else {
            return;
        };
        if tracked.conn.expiry().is_none_or(|at| at > now) {
            return;
        }
        let shared = Arc::clone(&tracked.shared);
        match tracked.conn.handle_expiry(now) {
            Ok(ExpiryOutcome::Handled) => {}
            Ok(ExpiryOutcome::IdleClose) => {
                let close = tracked.conn.close_error();
                shared.fail_with_close(close);
            }
            Ok(ExpiryOutcome::Terminal) => {
                shared.fail(Error::new(
                    ErrorKind::Transport,
                    "the connection reached a terminal state",
                ));
            }
            Err(err) => {
                // A handshake that ran out of time is not a transport failure: nothing
                // refused anything, and a caller may reasonably retry. ngtcp2 reports it
                // only from here, which is why the distinction is drawn at this call site
                // rather than in the general mapping.
                let timed_out = err
                    .native_code()
                    .is_some_and(|code| code.get() == crate::raw::NGTCP2_ERR_HANDSHAKE_TIMEOUT);
                if timed_out {
                    shared.fail(Error::new(
                        ErrorKind::HandshakeTimeout,
                        "the handshake did not complete in time",
                    ));
                } else {
                    shared.fail(Error::from(err));
                }
            }
        }
        self.absorb(index);
    }

    /// The earliest deadline across every connection.
    pub(crate) fn earliest_expiry(&self) -> Option<Timestamp> {
        self.connections
            .values()
            .filter_map(|t| t.conn.expiry())
            .min()
    }

    /// Removes connections that have finished and released their identifiers.
    pub(crate) fn evict(&mut self) {
        let mut dead = Vec::new();
        for (index, tracked) in &self.connections {
            let done = tracked.shared.is_closed()
                && tracked.pending.is_none()
                && (tracked.conn.in_draining_period() || tracked.finished);
            if done {
                dead.push(*index);
            }
        }
        for index in dead {
            self.connections.remove(&index);
            // Every identifier that routed here goes with it, or a later datagram would be
            // delivered to a connection that no longer exists.
            self.routes.retain(|_, target| *target != index);
        }
    }

    /// Builds a connection this endpoint initiated.
    pub(crate) fn dial(
        &mut self,
        remote: SocketAddr,
        server_name: Option<&str>,
        shared: Arc<ConnectionShared>,
    ) -> Result<u64, Error> {
        let local = self
            .socket
            .local_addr()
            .map_err(|err| Error::new(ErrorKind::Socket, "the socket has no address").with_source(SocketError(err.to_string())))?;

        let session = self
            .backend
            .new_session(Role::Client, server_name)
            .map_err(Error::from)?;

        let now = self.clock.now();
        let conn = crate::conn::ConnBuilder::new(
            Role::Client,
            self.config.settings(now),
            self.config.transport_params(),
            (self.entropy)(),
            session,
            local,
            remote,
        )
        .build(handlers_for(&shared))
        .map_err(Error::from)?;

        let index = self.next_index;
        self.next_index += 1;
        self.connections.insert(
            index,
            Tracked {
                conn,
                shared,
                remote,
                pending: None,
                close_datagram: None,
                finished: false,
            },
        );
        self.install_routes(index);
        Ok(index)
    }

    /// Builds a connection a peer initiated.
    pub(crate) fn accept(
        &mut self,
        remote: SocketAddr,
        packet: &crate::accept::InitialPacket,
        original_dcid: &crate::cid::ConnectionId,
        retried: bool,
        shared: Arc<ConnectionShared>,
    ) -> Result<u64, Error> {
        let local = self
            .socket
            .local_addr()
            .map_err(|err| Error::new(ErrorKind::Socket, "the socket has no address").with_source(SocketError(err.to_string())))?;

        let session = self
            .backend
            .new_session(Role::Server, None)
            .map_err(Error::from)?;

        let now = self.clock.now();

        // A server cannot be built without the identifier the client first addressed:
        // ngtcp2 asserts on it, and the assertion is compiled out of release builds.
        let mut params = self.config.transport_params().original_dcid(original_dcid);
        let mut settings = self.config.settings(now);
        if retried {
            // Both halves are required after a Retry and neither errors when omitted. The
            // client verifies that the identifier it was told to address really came from a
            // Retry this server sent, so without `retry_scid` the handshake never
            // completes -- indistinguishable from an unreachable server.
            params = params.retry_scid(&packet.dcid);
            settings = settings.validated_token(packet.token.bytes(), crate::TokenKind::Retry);
        }

        let conn = crate::conn::ConnBuilder::new(
            Role::Server,
            settings,
            params,
            (self.entropy)(),
            session,
            local,
            remote,
        )
        .dcid(packet.scid)
        .build(handlers_for(&shared))
        .map_err(Error::from)?;

        let index = self.next_index;
        self.next_index += 1;
        self.connections.insert(
            index,
            Tracked {
                conn,
                shared,
                remote,
                pending: None,
                close_datagram: None,
                finished: false,
            },
        );
        self.install_routes(index);
        // The identifier the client addressed also routes here until the handshake gives
        // the connection its own: the client's next packet still carries it.
        self.routes.insert(packet.dcid.as_bytes().to_vec(), index);
        Ok(index)
    }

    /// Arms the sleep for the earliest deadline, if there is one.
    ///
    /// Returns [`Poll::Ready`] when a deadline has been reached, which is the driver's
    /// signal to service timers. Rearming after *every* pass — including a pass that only
    /// wrote — is what keeps a paced connection sending: ngtcp2 reports its pacing deadline
    /// through the same `expiry()` as everything else, so a driver that rearms only after
    /// reading sends one datagram and then sleeps until the peer speaks.
    pub(crate) fn rearm(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(deadline) = self.earliest_expiry() else {
            self.sleeping = None;
            self.sleeping_until = None;
            return Poll::Pending;
        };

        if self.sleeping_until != Some(deadline) {
            self.sleeping = Some(self.clock.sleep_until(deadline));
            self.sleeping_until = Some(deadline);
        }

        let Some(sleep) = self.sleeping.as_mut() else {
            return Poll::Pending;
        };
        // `Clock::Sleep` is `Unpin`, which is what lets this be written without `unsafe` --
        // the reason that bound is on the trait.
        match core::pin::Pin::new(sleep).poll(cx) {
            Poll::Ready(()) => {
                self.sleeping = None;
                self.sleeping_until = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Sends whatever every connection has to send.
    pub(crate) fn flush(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        // Connectionless answers first. They are small, they are replies to something that
        // already arrived, and holding them behind a bulk transfer would make a Retry
        // arrive after the client had given up.
        while let Some((destination, datagram)) = self.outbox.pop_front() {
            match self.socket.poll_send(cx, destination, &datagram) {
                Poll::Ready(Ok(Sent::Complete)) => {}
                Poll::Ready(Ok(Sent::WouldBlock)) | Poll::Pending => {
                    self.outbox.push_front((destination, datagram));
                    break;
                }
                Poll::Ready(Err(err)) => {
                    return Err(Error::new(ErrorKind::Socket, "the socket failed")
                        .with_source(SocketError(err.to_string())));
                }
            }
        }

        let indices: Vec<u64> = self.connections.keys().copied().collect();
        for index in indices {
            while let Some(datagram) = self.next_datagram(index) {
                let Some(tracked) = self.connections.get(&index) else {
                    break;
                };
                let remote = tracked.remote;
                match self.socket.poll_send(cx, remote, &datagram) {
                    Poll::Ready(Ok(Sent::Complete)) => {}
                    Poll::Ready(Ok(Sent::WouldBlock)) | Poll::Pending => {
                        // Not sent. Keeping it is what stops a busy socket from silently
                        // losing packets, which QUIC would recover from slowly enough to
                        // look like a hang.
                        if let Some(tracked) = self.connections.get_mut(&index) {
                            tracked.pending = Some(datagram);
                        }
                        break;
                    }
                    Poll::Ready(Err(err)) => {
                        return Err(Error::new(ErrorKind::Socket, "the socket failed")
                            .with_source(SocketError(err.to_string())));
                    }
                }
            }
        }
        Ok(())
    }

    /// Fails every connection, which is what a dead socket or a dropped driver means.
    pub(crate) fn fail_all(&mut self, make: impl Fn() -> Error) {
        for tracked in self.connections.values() {
            tracked.shared.fail(make());
        }
    }

    /// Wakes every connection's handle.
    pub(crate) fn wake_all(&self) {
        for tracked in self.connections.values() {
            tracked.shared.wake_all();
        }
    }

    /// Whether anything is waiting to be sent.
    pub(crate) fn has_pending(&self) -> bool {
        !self.outbox.is_empty() || self.connections.values().any(|t| t.pending.is_some())
    }


}

/// A socket failure, rendered as text so it can cross the error boundary.
///
/// [`AsyncUdpSocket::Error`] is only `Debug + Display`, deliberately — requiring
/// `std::error::Error` would exclude error types that do not implement it, for no benefit
/// beyond a nicer chain. Rendering it here keeps what it said.
#[derive(Debug)]
pub(crate) struct SocketError(pub(crate) String);

impl core::fmt::Display for SocketError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl core::error::Error for SocketError {}
