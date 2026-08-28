//! The loop that moves bytes between a QUIC backend and the state machine.
//!
//! # One pass
//!
//! Every pass does the same things in the same order, and the order is load-bearing:
//!
//! 1. **Take transport events**, up to a bound. Control-plane news — releases, resets,
//!    stop-sending, stream closes — is applied before bulk data, so a flood of body bytes
//!    cannot delay a reset. The bound is what stops a fast stream starving everything else.
//! 2. **Feed received bytes** to the state machine and extend receive credit by what it
//!    reports. Body payload is *not* credited here; the caller credits it as it reads,
//!    because only the caller knows when it has finished with the bytes.
//! 3. **Settle finished bodies**, so a body that failed during the last pass's transmit
//!    becomes a queued reset here, ahead of everything below that can wait. Such a stream
//!    carries no end-of-stream marker by design, so a pass that stopped short of this —
//!    on a stream the transport has not opened yet, or at the park — would leave the peer
//!    holding a message that neither ended nor was abandoned.
//! 4. **Drain deferred credit.** Credit for a QPACK-blocked stream arrives late and exactly
//!    once, and a connection that drops it under-credits the peer by degrees until it
//!    stalls.
//! 5. **Apply releases**, skipping streams already closed. Closing released that stream's
//!    buffers, so applying a release afterwards would report more acknowledged than was ever
//!    written.
//! 6. **Drain transport actions** the state machine asked for.
//! 7. **Advance the role** — submit queued requests, or poll handlers.
//! 8. **Transmit**, by handing the backend a source it pulls from.
//! 9. **Close finished streams**, which is one of only three things that release a buffer.
//! 10. **Park**, if and only if nothing above could make progress.
//!
//! # The write side is pulled, not pushed
//!
//! The backend calls [`Offers::write_next`] when it has room. That is what makes the
//! `SendGuard` contract keepable: acquiring the offer, handing it to the transport and
//! disposing of it all happen inside one function the guard cannot escape, so there is no
//! path — including `?` and early return — on which one is dropped without a verdict.

use core::future::poll_fn;
use core::task::Poll;
use std::collections::{HashSet, VecDeque};
use std::io::IoSlice;
use std::sync::Arc;

use bytes::Bytes;

use super::config::Config;
use super::error::{Error, ErrorKind, Result};
use super::events::{Events, Observation};
use super::head;
use super::quic::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use super::shared::{Registry, Shared, SharedWork, TransportAction};
use crate::conn::{Conn, ConnBuilder, Role as CoreRole};
use crate::error::ErrorCode;
use crate::handlers::{FieldSection, Shutdown};
use crate::settings::Settings;
use crate::stream::StreamId;

/// How many recently-closed streams are remembered, for discarding late releases.
///
/// A release can only arrive for a stream the transport still knows about, so a tombstone
/// need not outlive the transport's own accounting for it. This is generous enough that it
/// never expires one that matters and small enough that the list cannot grow without bound.
const CLOSED_TOMBSTONES: usize = 1024;

/// Recently closed streams, indexed for membership and ordered for bounded eviction.
///
/// `members` and `order` contain exactly the same identifiers after every operation.
/// Keeping insertion and eviction here makes it impossible for a caller to update only one
/// half of that invariant.
struct ClosedStreams {
    members: HashSet<StreamId>,
    order: VecDeque<StreamId>,
}

impl ClosedStreams {
    fn new() -> Self {
        Self {
            members: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn contains(&self, stream: StreamId) -> bool {
        self.members.contains(&stream)
    }

    /// Records a distinct close and says whether its side effects should run.
    fn insert(&mut self, stream: StreamId) -> bool {
        if !self.members.insert(stream) {
            return false;
        }

        self.order.push_back(stream);
        while self.order.len() > CLOSED_TOMBSTONES {
            let oldest = self
                .order
                .pop_front()
                .expect("an over-capacity closed-stream queue is not empty");
            let removed = self.members.remove(&oldest);
            debug_assert!(removed, "closed-stream membership and order diverged");
        }
        true
    }
}

/// `H3_REQUEST_CANCELLED`, the code an abandoned exchange carries.
pub(crate) const REQUEST_CANCELLED: u64 = 0x10c;
/// `H3_NO_ERROR`, for an orderly close.
pub(crate) const NO_ERROR: u64 = 0x100;

/// What differs between a client and a server.
///
/// Everything else — reads, writes, credit, releases, teardown — is the same code at both
/// ends rather than the same idea written twice.
pub(crate) trait Role {
    /// Whether the role has work waiting on a stream it does not yet have.
    ///
    /// Stream identifiers are the *transport's* to allocate, not this layer's: quinn will
    /// only write to a stream it opened, and a QUIC library that let an application invent
    /// identifiers would be one that could not enforce its own concurrency limits. So the
    /// driver asks the backend and hands the result over.
    fn needs_stream(&self) -> bool {
        false
    }

    /// Hands the role a stream the backend just opened.
    fn give_stream(&mut self, _stream: StreamId) {}

    /// Whether an exchange can appear on a stream this endpoint never opened.
    ///
    /// A server learns of a request when its head arrives, so a stream it has never heard
    /// of is the ordinary state of one whose head is a few entries further down the batch
    /// being applied. A client opens every stream it uses and registers it as it does, so
    /// a stream unknown to a client is one that will stay unknown.
    const ACCEPTS_STREAMS: bool = false;

    /// Submits whatever the role has queued.
    fn advance(&mut self, conn: &mut Conn<Events>, events: &mut Events) -> Result<()>;

    /// Acts on bodies that have finished since the last pass.
    ///
    /// Separate from [`advance`](Self::advance), which also does this, because a body that
    /// failed leaves its stream deferred with no end marker on it and the reset is the
    /// only thing that will ever tell the peer so. `advance` is too late to be the first
    /// to notice: the pass can wait for a stream the transport has not opened, or decide
    /// the connection is finished, before it ever gets there. Run early it is one extra
    /// scan of a short list; run only late it is a stream that neither ends nor resets.
    fn settle(&mut self, conn: &mut Conn<Events>) -> Result<()>;

    /// A complete header section arrived on a stream.
    fn head(
        &mut self,
        conn: &mut Conn<Events>,
        events: &mut Events,
        stream: StreamId,
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()>;

    /// The state machine has finished with a stream.
    fn closed(&mut self, stream: StreamId);

    /// Whether the role has work that cannot wait.
    fn busy(&self) -> bool;

    /// Whether the role has nothing left to do and never will.
    fn done(&self) -> bool;

    /// Fails everything the role is holding, for teardown.
    fn abandon(&mut self);
}

/// Fails every exchange when the driver goes away, however it goes away.
///
/// A guard rather than a tidy-up at the end of the loop, because the loop can exit by `?`,
/// by returning, or by being dropped mid-poll, and all three must leave callers with an
/// error rather than a future that never resolves.
///
/// **Constructed before the future is, not inside it.** An `async fn` body does not run
/// until the future is first polled, so a guard built inside one would not exist for a
/// driver that was created and then dropped without ever being polled — which is precisely
/// the case the guarantee is for, and the one the `#[must_use]` on
/// [`Connection`](super::connection::Connection) is warning about. Built here and moved in,
/// it is dropped with the future whether that future ever ran or not.
pub(crate) struct DriverGuard<R: Role> {
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    role: R,
}

impl<R: Role> DriverGuard<R> {
    pub(crate) fn new(shared: Arc<Shared>, registry: Arc<Registry>, role: R) -> Self {
        Self {
            shared,
            registry,
            role,
        }
    }
}

#[cfg(test)]
#[path = "driver/closed_streams.rs"]
mod closed_streams;
#[cfg(test)]
#[path = "driver/shared_snapshot.rs"]
mod shared_snapshot_tests;

impl<R: Role> Drop for DriverGuard<R> {
    fn drop(&mut self) {
        self.shared.set_gone();
        for entry in self.registry.take_all() {
            if let Some(slot) = &entry.slot {
                slot.fail(Error::new(ErrorKind::Closed, "the connection went away"));
            }
            entry
                .incoming
                .fail(Error::new(ErrorKind::Closed, "the connection went away"));
        }
        self.role.abandon();
    }
}

/// The layer's side of a write, handed to the backend to pull from.
struct Offers<'a> {
    conn: &'a mut Conn<Events>,
    events: &'a mut Events,
    /// Streams blocked during this pass, to be unblocked before giving up.
    blocked: &'a mut Vec<StreamId>,
    /// A failure the backend has no way to represent, collected by the driver afterwards.
    failure: Option<Error>,
    /// Whether the unblock-and-retry has already happened this pass.
    retried: bool,
}

impl StreamSource for Offers<'_> {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        loop {
            if self.failure.is_some() {
                return false;
            }

            let offer = match self.conn.writev_stream(self.events) {
                Ok(offer) => offer,
                Err(error) => {
                    self.failure = Some(error.into());
                    return false;
                }
            };

            let Some(guard) = offer else {
                // Nothing offerable. A stream blocked earlier in this pass will never be
                // offered again on its own — nghttp3 unschedules a blocked stream and only
                // an explicit unblock puts it back — so the one thing left to try is
                // putting them all back and asking once more.
                if self.retried || self.blocked.is_empty() {
                    return false;
                }
                self.retried = true;
                let blocked = core::mem::take(self.blocked);
                for stream in blocked {
                    if let Err(error) = self.conn.unblock_stream(stream) {
                        self.failure = Some(error.into());
                        return false;
                    }
                }
                continue;
            };

            let stream = guard.stream();
            let fin = guard.fin();
            let offered = guard.len();
            let outcome = write(stream, guard.slices(), fin);

            // Every arm disposes of the guard exactly once, and the difference between
            // committing and abandoning is the whole game.
            //
            // `commit(n)` says "n bytes of this offer are gone for good"; the rest, if any,
            // is re-offered. `commit(0)` therefore does *not* mean "I took nothing" — it
            // consumes the offer having taken nothing, and on an offer carrying `fin` that
            // tells the state machine the stream ended. `abandon` is the one that means
            // "not now, ask me again", and it is what congestion calls for.
            let disposal = match outcome {
                // Nothing taken. The same bytes must be offered again, so the transaction
                // is abandoned rather than committed at zero.
                WriteOutcome::Blocked | WriteOutcome::Accepted(0) if offered > 0 => {
                    guard.abandon();
                    self.blocked.push(stream);
                    (Ok(()), true, false)
                }
                // An offer with `fin` and no bytes: ending the stream *is* the write, and a
                // transport that declined it leaves the peer waiting for an end that never
                // comes. Abandoned, so it is offered again rather than reported as sent.
                WriteOutcome::Blocked => {
                    guard.abandon();
                    self.blocked.push(stream);
                    (Ok(()), true, false)
                }
                WriteOutcome::Accepted(taken) => {
                    let taken = taken.min(offered);
                    // A short write is backpressure: this stream steps aside so another
                    // gets a turn, rather than being re-offered ahead of everything else.
                    let short = taken < offered;
                    let result = guard.commit(taken);
                    if short {
                        self.blocked.push(stream);
                    }
                    (result, short, false)
                }
                WriteOutcome::Gone => {
                    guard.abandon();
                    (Ok(()), false, true)
                }
            };

            let (result, block, gone) = disposal;
            if let Err(error) = result {
                self.failure = Some(error.into());
                return false;
            }
            if block && let Err(error) = self.conn.block_stream(stream) {
                self.failure = Some(error.into());
                return false;
            }
            if gone && let Err(error) = self.conn.shutdown_stream_write(stream) {
                self.failure = Some(error.into());
                return false;
            }
            return true;
        }
    }
}

/// Everything one connection's driver needs.
pub(crate) struct Driver<Q> {
    pub(crate) backend: Q,
    pub(crate) conn: Conn<Events>,
    pub(crate) events: Events,
    pub(crate) shared: Arc<Shared>,
    pub(crate) registry: Arc<Registry>,
    pub(crate) config: Config,
    /// Streams the state machine has finished with, so a late release is dropped rather
    /// than reported as more acknowledgement than was ever written.
    ///
    /// Bounded: a tombstone only has to outlive the releases still in flight for its stream,
    /// and the transport cannot report release for a stream it has already closed. Keeping
    /// one per exchange for the life of the connection would turn ordinary traffic into an
    /// unbounded allocation, so the oldest are dropped once there are more than a
    /// connection could plausibly have in flight.
    closed: ClosedStreams,
    /// Streams blocked by congestion, reused across passes to avoid reallocating.
    blocked: Vec<StreamId>,
    /// Unidirectional streams opened so far, kept across a `Pending`.
    ///
    /// Opening three streams is not one atomic act: a transport may hand over the first and
    /// then make the second wait on the peer's stream limit. Rebuilding the list from
    /// scratch on the next poll would open a fresh stream each time, spending the peer's
    /// allowance without ever binding a control stream.
    opened: Vec<StreamId>,
    /// One event taken while deciding whether to park, held for the next pass.
    ///
    /// The park has to answer "is there anything to do", and for the transport the only way
    /// to ask is to take an event. Throwing it away to answer the question would lose it, so
    /// it is kept here and consumed first next time round.
    pushback: Option<QuicEvent>,
    /// A transport failure raised while parking, likewise held rather than dropped.
    ///
    /// The trait promises nothing about an error repeating, and a source that reports one
    /// only once would otherwise have it swallowed -- turning a transport failure into a
    /// hang, or into a connection that looks as though it closed cleanly.
    pushback_error: Option<Error>,
    bound: bool,
    peer_gone: bool,
    /// The peer's limits, once it has stated them.
    peer_settings: Option<crate::handlers::PeerSettings>,
    /// Handle and callback work, drained once and reused across driver passes.
    shared_work: SharedWork,
}

impl<Q: QuicConnection> Driver<Q> {
    pub(crate) fn new(
        backend: Q,
        conn: Conn<Events>,
        shared: Arc<Shared>,
        registry: Arc<Registry>,
        config: Config,
    ) -> Self {
        Self {
            backend,
            conn,
            events: Events::default(),
            shared,
            registry,
            config,
            closed: ClosedStreams::new(),
            blocked: Vec::new(),
            opened: Vec::new(),
            pushback: None,
            pushback_error: None,
            bound: false,
            peer_gone: false,
            peer_settings: None,
            shared_work: SharedWork::new(),
        }
    }

    /// Preserves a transport operation's `Pending` after discharging buffered output.
    ///
    /// The operation which returned `Pending` already registered the wake for its own
    /// readiness. A transport whose flush also parks registers the same task for write
    /// readiness. Only a flush failure changes the pending operation's result.
    fn flush_before_pending<T>(
        backend: &mut Q,
        cx: &mut core::task::Context<'_>,
    ) -> Poll<Result<T>> {
        match backend.poll_flush(cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(Self::transport(error))),
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
        }
    }

    /// Turns a backend failure into the layer's error type.
    fn transport(error: Q::Error) -> Error {
        Error::with_source(ErrorKind::Transport, "the QUIC backend failed", error)
    }
}

/// Builds the state machine with handlers that record into [`Events`].
///
/// The handlers do as little as possible. Each runs inside an FFI call, where the backend is
/// unreachable and where a panic would cross a C frame and abort the process, so they
/// accumulate and the driver acts afterwards.
pub(crate) fn build_conn(
    role: CoreRole,
    config: &Config,
    shared: &Arc<Shared>,
) -> Result<Conn<Events>> {
    let settings = Settings::default()
        .max_field_section_size(config.max_field_section_size)
        .qpack_max_dtable_capacity(config.qpack_max_dtable_capacity)
        .qpack_blocked_streams(config.qpack_blocked_streams);

    let for_stop = Arc::clone(shared);
    let for_reset = Arc::clone(shared);

    let conn = ConnBuilder::<Events>::new(role)
        .settings(settings)
        .on_section_begin(|events, stream, section| events.begin_section(stream, section))
        .on_field(|events, stream, _section, _token, name, value| {
            events.push_field(stream, name, value);
            crate::handlers::FieldAction::Continue
        })
        .on_section_end(|events, stream, section| events.end_section(stream, section))
        .on_data(|events, stream, chunk| events.push_data(stream, chunk))
        .on_end_stream(|events, stream| events.push_end(stream))
        .on_stream_close(|events, stream, closed| events.push_closed(stream, closed))
        .on_stop_sending(move |_events, stream, code| {
            // An instruction to the QUIC layer, not news from it. It cannot be performed
            // here — the backend is out of scope inside a handler — so it is queued.
            for_stop.push_action(TransportAction::StopSending { stream, code });
        })
        .on_reset_stream(move |_events, stream, code| {
            for_reset.push_action(TransportAction::Reset { stream, code });
        })
        .on_shutdown(|events, shutdown| events.push_shutdown(shutdown))
        .on_peer_settings(|events, settings| events.push_settings(settings))
        .build()?;

    Ok(conn)
}

impl<Q: QuicConnection> Driver<Q> {
    /// Opens and binds the three unidirectional streams HTTP/3 needs before anything else.
    fn poll_bind(&mut self, cx: &mut core::task::Context<'_>) -> Poll<Result<()>> {
        if self.bound {
            return Poll::Ready(Ok(()));
        }
        while self.opened.len() < 3 {
            match self.backend.poll_open_uni(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(Self::transport(error))),
                Poll::Ready(Ok(stream)) => self.opened.push(stream),
            }
        }
        let ids = &self.opened;
        if let Err(error) = self.conn.bind_control_stream(ids[0]) {
            return Poll::Ready(Err(error.into()));
        }
        if let Err(error) = self.conn.bind_qpack_streams(ids[1], ids[2]) {
            return Poll::Ready(Err(error.into()));
        }
        self.bound = true;
        Poll::Ready(Ok(()))
    }

    /// Takes up to the configured number of transport events.
    fn take_events(&mut self, cx: &mut core::task::Context<'_>) -> Result<Vec<QuicEvent>> {
        if let Some(error) = self.pushback_error.take() {
            return Err(error);
        }
        let mut taken = Vec::new();
        if let Some(event) = self.pushback.take() {
            taken.push(event);
        }
        for _ in taken.len()..self.config.events_per_pass {
            match self.backend.poll_event(cx) {
                Poll::Pending => break,
                Poll::Ready(Err(error)) => return Err(Self::transport(error)),
                Poll::Ready(Ok(event)) => {
                    let last = matches!(event, QuicEvent::Closed { .. });
                    taken.push(event);
                    if last {
                        break;
                    }
                }
            }
        }
        Ok(taken)
    }

    /// Applies one pass of transport events, control-plane news first.
    ///
    /// Answers with the resets that named a stream this endpoint had never heard of, which
    /// the caller must apply again once this pass has dispatched what it read — see the
    /// note on `unheard` below for why they cannot be applied here.
    fn apply_events<R: Role>(
        &mut self,
        events: Vec<QuicEvent>,
        role: &mut R,
    ) -> Result<Vec<(StreamId, ErrorCode)>> {
        // Two sweeps rather than one: a reset behind a megabyte of body data must not wait
        // for the body to be parsed before it is acted on.
        let mut data = Vec::new();
        // Resets that named a stream this endpoint had never heard of, and could still hear
        // of before the pass is out. Acting on the control plane first means a reset can
        // arrive in the same batch as the head it follows, and there was nothing to fail
        // when it did. Left there, the head goes on to open an exchange the peer has
        // already abandoned: a server would hand its handler a request body that will never
        // end and never fail, and the handler would read it forever.
        //
        // So they are kept and handed back rather than dropped. Not replayed at the foot of
        // this function, though: reading a head only *records* it, and the exchange it opens
        // does not exist until the pass dispatches what it recorded, which is well after
        // this. They are applied there instead.
        //
        // Same batch is the whole of it. A reset whose head arrives in some *later* batch is
        // still dropped, which would be the same hang — but a transport that has reported a
        // stream reset does not go on to deliver more of that stream: ngtcp2 terminates the
        // receiving side and discards what follows, and this layer has just shut the read
        // side down as well. If a transport is ever found that does, the durable answer is a
        // short-lived record of reset streams consulted wherever a head opens an exchange,
        // not a longer-lived version of this vector.
        let mut unheard = Vec::new();
        for event in events {
            match event {
                QuicEvent::Data { .. } => data.push(event),
                QuicEvent::Accepted { stream } => {
                    // A server notices a peer-opened stream when its head arrives; nothing
                    // to do here beyond letting it exist.
                    let _ = stream;
                }
                QuicEvent::Released {
                    stream,
                    bytes,
                    delivered,
                } => self.apply_release(stream, bytes, delivered)?,
                QuicEvent::StopSending { stream, code } => {
                    // The peer acting. Fed *into* the state machine, unlike the handler of
                    // the same name, which is the state machine asking us to act.
                    self.conn.shutdown_stream_write(stream).ok();
                    let _ = code;
                }
                QuicEvent::Reset { stream, code } => {
                    self.conn.shutdown_stream_read(stream).ok();
                    // Only where an unknown stream can still become an exchange. A client
                    // registers its streams as it opens them, so replaying a reset it could
                    // not place would let a peer cancel a request the client submits later
                    // in this very pass — widening the fix into a way to lose an exchange
                    // that was never abandoned.
                    if !self.fail_stream(stream, code, role) && R::ACCEPTS_STREAMS {
                        unheard.push((stream, code));
                    }
                }
                QuicEvent::StreamClosed {
                    stream,
                    rx_code,
                    tx_code,
                } => {
                    let closed = crate::handlers::StreamClosed {
                        receiving: rx_code,
                        sending: tx_code,
                    };
                    self.close_stream(stream, closed, role)?;
                }
                QuicEvent::Closed { .. } => self.peer_gone = true,
            }
        }

        for event in data {
            if let QuicEvent::Data { stream, bytes, fin } = event {
                self.read(stream, bytes, fin)?;
            }
        }
        Ok(unheard)
    }

    /// Feeds received bytes to the state machine and extends receive credit.
    fn read(&mut self, stream: StreamId, bytes: Bytes, fin: bool) -> Result<()> {
        let now = self.backend.now();
        // Callbacks retain checked ranges only. Once the FFI call returns, a unique parent can
        // move into a whole-body observation without first becoming shared.
        self.events.begin_inbound(&bytes);
        let credit = self
            .conn
            .read_stream(stream, &bytes, fin, now, &mut self.events);
        self.events.finish_inbound(bytes);
        let credit = credit?;

        self.extend(Some(stream), credit.bytes())?;
        Ok(())
    }

    /// Extends receive credit at both levels.
    ///
    /// Twice for the same bytes, deliberately: stream credit does not imply connection
    /// credit, and a backend needing only one can ignore the other.
    fn extend(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some(stream) = stream {
            self.backend
                .extend_credit(Some(stream), bytes)
                .map_err(Self::transport)?;
        }
        self.backend
            .extend_credit(None, bytes)
            .map_err(Self::transport)
    }

    /// Reports acknowledged bytes, which is the only thing that releases a retained buffer.
    fn apply_release(&mut self, stream: StreamId, bytes: u64, delivered: bool) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if self.closed.contains(stream) {
            // Closing already released this stream's buffers. Reporting acknowledgement
            // afterwards would claim more was acknowledged than was ever written, which the
            // state machine rejects — correctly, since that is exactly the shape of an
            // accounting bug that frees a buffer early.
            return Ok(());
        }
        if !delivered {
            // The buffer is the transport's to hand back but the data never arrived, so it
            // must not be reported as acknowledged: that would claim more reached the peer
            // than ever did, and the state machine's offset accounting would then release a
            // buffer on the strength of it.
            //
            // Nothing else releases it either, so it is held until the stream closes. That
            // errs in the safe direction — holding too long rather than freeing too early —
            // but it *is* holding: a transport that cancels many sends on a long-lived
            // connection accumulates them until those streams end. Recorded in
            // `docs/h3/pending-work.md` rather than glossed.
            return Ok(());
        }
        self.conn
            .add_ack_offset(stream, bytes, &mut self.events)
            .map_err(Into::into)
    }

    /// Fails one exchange without disturbing the rest of the connection, and reports
    /// whether there was one to fail.
    ///
    /// A `false` answer is not "nothing happened": it means the stream is unknown to this
    /// endpoint *so far*, which on a server is the ordinary state of a request stream whose
    /// head is still sitting in the same batch of events, a few entries further down.
    fn fail_stream<R: Role>(&mut self, stream: StreamId, code: ErrorCode, role: &mut R) -> bool {
        let known = if let Some(entry) = self.registry.remove(stream) {
            let error =
                Error::new(ErrorKind::Stream, "the peer reset this exchange").with_code(code);
            if let Some(slot) = &entry.slot {
                slot.fail(Error::new(
                    ErrorKind::Stream,
                    "the peer reset this exchange",
                ));
            }
            entry.incoming.fail(error);
            true
        } else {
            false
        };
        role.closed(stream);
        known
    }

    /// Fails every exchange at or above a `GOAWAY` cut-off, retriably.
    fn refuse_from<R: Role>(&mut self, cutoff: StreamId, role: &mut R) {
        let above: Vec<StreamId> = self
            .registry
            .streams()
            .into_iter()
            .filter(|stream| stream.get() >= cutoff.get())
            .collect();
        for stream in above {
            if let Some(entry) = self.registry.remove(stream) {
                if let Some(slot) = &entry.slot {
                    slot.fail(Error::new(
                        ErrorKind::Refused,
                        "the peer went away before it looked at this exchange",
                    ));
                }
                entry.incoming.fail(Error::new(
                    ErrorKind::Refused,
                    "the peer went away before it looked at this exchange",
                ));
            }
            role.closed(stream);
        }
    }

    /// Releases everything the state machine holds for a stream.
    fn close_stream<R: Role>(
        &mut self,
        stream: StreamId,
        closed: crate::handlers::StreamClosed,
        role: &mut R,
    ) -> Result<()> {
        if !self.closed.insert(stream) {
            return Ok(());
        }
        let event_checkpoint = self.events.observed.len();
        self.conn
            .close_stream_with(stream, closed, &mut self.events)
            .ok();
        // `close_stream_with` may fire the state-machine close callback. This close has
        // already been applied here, so do not queue its observation for a second pass:
        // a large transport batch can evict the bounded release tombstone before the
        // driver gets round to dispatching those observations. Discard unconditionally
        // because an error does not promise that no callback ran before it.
        self.events.discard_closed_since(event_checkpoint, stream);
        if let Some(entry) = self.registry.remove(stream) {
            entry.incoming.finish();
            if let Some(slot) = &entry.slot
                && !slot.is_settled()
            {
                slot.fail(Error::new(
                    ErrorKind::Stream,
                    "the exchange ended before a response arrived",
                ));
            }
        }
        role.closed(stream);
        Ok(())
    }
}

/// Dispatches what the state machine observed during a pass.
pub(crate) fn dispatch<Q: QuicConnection, R: Role>(
    driver: &mut Driver<Q>,
    role: &mut R,
    observed: Vec<Observation>,
) -> Result<()> {
    for observation in observed {
        match observation {
            Observation::Head {
                stream,
                section,
                fields,
            } => match section {
                FieldSection::Headers => {
                    let mut events = core::mem::take(&mut driver.events);
                    let result = role.head(&mut driver.conn, &mut events, stream, &fields);
                    driver.events = events;
                    result?;
                }
                FieldSection::Trailers => {
                    let trailers = head::trailers(&fields)?;
                    if let Some(incoming) = driver.registry.incoming(stream) {
                        incoming.set_trailers(trailers);
                    }
                }
            },
            Observation::Data { stream, bytes } => {
                if let Some(incoming) = driver.registry.incoming(stream) {
                    incoming.push(bytes.into_ready());
                }
            }
            Observation::End { stream } => {
                if let Some(incoming) = driver.registry.incoming(stream) {
                    incoming.finish();
                }
            }
            Observation::Closed { stream, closed } => {
                driver.close_stream(stream, closed, role)?;
            }
            Observation::Shutdown(shutdown) => {
                // The peer will begin no new exchanges, so nothing further is submitted.
                driver.shared.set_refusing();
                if let Shutdown::NoStreamsFrom(cutoff) = shutdown {
                    // Everything at or above the cut-off was never looked at, which is
                    // exactly the condition that makes a retry safe: it cannot duplicate a
                    // side effect the peer never performed. Failing them here rather than
                    // waiting for the connection to end is what lets a caller retry
                    // promptly.
                    driver.refuse_from(cutoff, role);
                }
            }
            Observation::Settings(settings) => {
                driver.peer_settings = Some(settings);
            }
        }
    }
    Ok(())
}

/// Runs one connection to completion.
pub(crate) async fn run<Q, R>(mut driver: Driver<Q>, mut guard: DriverGuard<R>) -> Result<()>
where
    Q: QuicConnection,
    R: Role,
{
    let shared = Arc::clone(&driver.shared);
    let registry = Arc::clone(&driver.registry);

    loop {
        // 0. Bind, once, before anything else can happen.
        poll_fn(|cx| {
            let bound = driver.poll_bind(cx);
            if bound.is_pending() {
                Driver::<Q>::flush_before_pending(&mut driver.backend, cx)
            } else {
                bound
            }
        })
        .await?;

        // 1-2. Transport events, control-plane first, then reads.
        let events = poll_fn(|cx| Poll::Ready(driver.take_events(cx))).await?;
        let had_events = !events.is_empty();
        let unheard = driver.apply_events(events, &mut guard.role)?;

        // 3. Bodies that finished during the last pass, read before anything below can
        // wait on the transport or on the peer. A failed body's stream is suspended with
        // no end marker on it, and the reset queued here is the only thing that will ever
        // tell the peer the message was abandoned; it is drained onto the transport a few
        // lines down, in this same pass.
        guard.role.settle(&mut driver.conn)?;

        // Everything present after settling belongs to this pass. Work queued while this
        // snapshot is processed remains in Shared for the next pass.
        shared.drain_work(&mut driver.shared_work);

        // 4. Credit that arrived late, for streams that were QPACK-blocked.
        let deferred = driver.conn.take_deferred_credit();
        for (stream, bytes) in deferred {
            driver.extend(Some(stream), bytes)?;
        }

        // Credit the caller returned by reading.
        let mut credit = core::mem::take(&mut driver.shared_work.credit);
        for (stream, bytes) in credit.drain(..) {
            driver.extend(Some(stream), bytes)?;
        }
        driver.shared_work.credit = credit;

        // 6. Actions the state machine asked the transport to take.
        let mut actions = core::mem::take(&mut driver.shared_work.actions);
        for action in actions.drain(..) {
            match action {
                TransportAction::Reset { stream, code } => {
                    driver
                        .backend
                        .reset(stream, code)
                        .map_err(Driver::<Q>::transport)?;
                }
                TransportAction::StopSending { stream, code } => {
                    driver
                        .backend
                        .stop_sending(stream, code)
                        .map_err(Driver::<Q>::transport)?;
                }
            }
        }
        driver.shared_work.actions = actions;

        // Resets a caller asked for, by dropping a response future or an unread body, and
        // resets owed by a body that failed.
        let mut resets = core::mem::take(&mut driver.shared_work.resets);
        for (stream, code) in resets.drain(..) {
            driver.conn.shutdown_stream_read(stream).ok();
            // Every caller of this queue is abandoning its own send side, and saying so
            // sets `SHUT_WR` and unschedules the stream, which is what stops nghttp3
            // offering write turns to a stream that will never produce bytes again — a
            // failed body's stream is suspended, so it would otherwise be asked
            // indefinitely. It does not close the stream or release its buffers; only a
            // stream close does that, and the transport reports none for a reset this end
            // issued.
            driver.conn.shutdown_stream_write(stream).ok();
            driver
                .backend
                .reset(stream, code)
                .map_err(Driver::<Q>::transport)?;
            driver
                .backend
                .stop_sending(stream, code)
                .map_err(Driver::<Q>::transport)?;
        }
        driver.shared_work.resets = resets;

        // Bodies that deferred and have since been woken.
        let mut ready = core::mem::take(&mut driver.shared_work.ready);
        for stream in ready.drain(..) {
            driver.conn.resume_stream(stream).ok();
        }
        driver.shared_work.ready = ready;

        if core::mem::take(&mut driver.shared_work.shutdown) {
            driver.conn.shutdown().ok();
        }
        driver.shared_work.bound_retained_capacity();

        // Request streams are opened by the transport, one per queued request, before the
        // role can submit anything onto them.
        while guard.role.needs_stream() {
            match poll_fn(|cx| match driver.backend.poll_open_bi(cx) {
                Poll::Pending => Driver::<Q>::flush_before_pending(&mut driver.backend, cx),
                Poll::Ready(Ok(stream)) => Poll::Ready(Ok(stream)),
                Poll::Ready(Err(error)) => Poll::Ready(Err(Driver::<Q>::transport(error))),
            })
            .await
            {
                Ok(stream) => guard.role.give_stream(stream),
                Err(error) => return Err(error),
            }
        }

        // 7. Submit whatever the role has queued.
        {
            let mut events = core::mem::take(&mut driver.events);
            let result = guard.role.advance(&mut driver.conn, &mut events);
            driver.events = events;
            result?;
        }

        // Dispatch everything the handlers recorded during this pass.
        let observed = driver.events.drain();
        if !observed.is_empty() {
            dispatch(&mut driver, &mut guard.role, observed)?;
        }

        // A reset that arrived alongside the head it was about, now that the head has
        // opened the exchange it names. This is the second attempt at it: the first, in the
        // event sweep, had nothing to fail, and dropping it there would leave a server's
        // handler reading a request body that can no longer end or fail.
        for (stream, code) in unheard {
            driver.fail_stream(stream, code, &mut guard.role);
        }

        // 8. Transmit.
        let mut blocked = core::mem::take(&mut driver.blocked);
        let failure = {
            let mut offers = Offers {
                conn: &mut driver.conn,
                events: &mut driver.events,
                blocked: &mut blocked,
                failure: None,
                retried: false,
            };
            let outcome = poll_fn(|cx| match driver.backend.poll_transmit(cx, &mut offers) {
                Poll::Pending => Driver::<Q>::flush_before_pending(&mut driver.backend, cx),
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(Driver::<Q>::transport(error))),
            })
            .await;
            let failure = offers.failure.take();
            match outcome {
                Err(error) => Some(error),
                Ok(()) => failure,
            }
        };
        driver.blocked = blocked;
        if let Some(error) = failure {
            return Err(error);
        }

        // Serialising can fire handlers of its own.
        let observed = driver.events.drain();
        if !observed.is_empty() {
            dispatch(&mut driver, &mut guard.role, observed)?;
        }

        // 10. Are we finished, and if not, is there anything to do?
        if driver.peer_gone {
            return Ok(());
        }
        if guard.role.done() && registry.is_empty() {
            if shared.work_pending_for_completion() {
                continue;
            }
            driver
                .backend
                .close(ErrorCode::new(NO_ERROR), b"")
                .map_err(Driver::<Q>::transport)?;
            return Ok(());
        }

        let idle = !had_events
            && !guard.role.busy()
            && driver.events.is_empty()
            && !(guard.role.done() && registry.is_empty())
            && !shared.work_pending_for_idle();

        if idle {
            // Being woken at all is the signal that a congested stream may be writable
            // again. The transport registered for that when `poll_write` answered `Pending`,
            // and it wakes this same task -- but a writability wake produces no *event*, so
            // a park that only ever asked `poll_event` would find nothing, park again, and
            // never retry the write. Once anything has woken us, a pass with blocked streams
            // is worth running.
            let mut parked = false;
            poll_fn(|cx| {
                if parked && !driver.blocked.is_empty() {
                    return Poll::Ready(());
                }
                parked = true;
                // Re-checked under the waker: work may have arrived between the decision
                // above and registering here, and missing it is a hang rather than a delay.
                if shared.refresh_driver_and_work_pending(cx.waker()) || guard.role.busy() {
                    return Poll::Ready(());
                }
                // Asking the transport whether it has anything means taking it: there is
                // no peek. So it is taken and kept for the next pass rather than dropped,
                // which is the difference between a slow connection and a lost response.
                match driver.backend.poll_event(cx) {
                    Poll::Pending => match driver.backend.poll_flush(cx) {
                        Poll::Ready(Err(error)) => {
                            driver.pushback_error = Some(Driver::<Q>::transport(error));
                            Poll::Ready(())
                        }
                        Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
                    },
                    Poll::Ready(Ok(event)) => {
                        driver.pushback = Some(event);
                        Poll::Ready(())
                    }
                    // An error is not lost either: it is kept and reported by the next
                    // `take_events`, because the trait promises nothing about a transport
                    // raising the same failure twice.
                    Poll::Ready(Err(error)) => {
                        driver.pushback_error = Some(Driver::<Q>::transport(error));
                        Poll::Ready(())
                    }
                }
            })
            .await;
        }
    }
}
