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
//! 3. **Drain deferred credit.** Credit for a QPACK-blocked stream arrives late and exactly
//!    once, and a connection that drops it under-credits the peer by degrees until it
//!    stalls.
//! 4. **Apply releases**, skipping streams already closed. Closing released that stream's
//!    buffers, so applying a release afterwards would report more acknowledged than was ever
//!    written.
//! 5. **Drain transport actions** the state machine asked for.
//! 6. **Advance the role** — submit queued requests, or poll handlers.
//! 7. **Transmit**, by handing the backend a source it pulls from.
//! 8. **Close finished streams**, which is one of only three things that release a buffer.
//! 9. **Park**, if and only if nothing above could make progress.
//!
//! # The write side is pulled, not pushed
//!
//! The backend calls [`Offers::write_next`] when it has room. That is what makes the
//! `SendGuard` contract keepable: acquiring the offer, handing it to the transport and
//! disposing of it all happen inside one function the guard cannot escape, so there is no
//! path — including `?` and early return — on which one is dropped without a verdict.

use core::future::poll_fn;
use core::task::Poll;
use std::io::IoSlice;
use std::sync::Arc;

use bytes::Bytes;

use super::config::Config;
use super::error::{Error, ErrorKind, Result};
use super::events::{Events, Observation};
use super::head;
use super::quic::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use super::shared::{Registry, Shared, TransportAction};
use crate::conn::{Conn, ConnBuilder, Role as CoreRole};
use crate::error::ErrorCode;
use crate::handlers::{FieldSection, Shutdown};
use crate::settings::Settings;
use crate::stream::StreamId;

/// `H3_REQUEST_CANCELLED`, the code an abandoned exchange carries.
pub(crate) const REQUEST_CANCELLED: u64 = 0x10c;
/// `H3_NO_ERROR`, for an orderly close.
pub(crate) const NO_ERROR: u64 = 0x100;

/// What differs between a client and a server.
///
/// Everything else — reads, writes, credit, releases, teardown — is the same code at both
/// ends rather than the same idea written twice.
pub(crate) trait Role {
    /// Submits whatever the role has queued.
    fn advance(&mut self, conn: &mut Conn<Events>, events: &mut Events) -> Result<()>;

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

            // Every arm below disposes of the guard exactly once. The empty-final-write row
            // is the one that bites: `commit(0)` on an offer carrying `fin` and no bytes
            // tells the state machine the stream ended, so a transport that merely declined
            // it would leave the peer waiting for an end that was never sent.
            let disposal = match outcome {
                WriteOutcome::Accepted(taken) => {
                    let taken = taken.min(offered);
                    let short = taken < offered;
                    let result = guard.commit(taken);
                    if short {
                        self.blocked.push(stream);
                    }
                    (result, short, false)
                }
                WriteOutcome::Blocked => {
                    if offered == 0 && fin {
                        guard.abandon();
                        self.blocked.push(stream);
                        (Ok(()), true, false)
                    } else {
                        let result = guard.commit(0);
                        self.blocked.push(stream);
                        (result, true, false)
                    }
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
    closed: Vec<StreamId>,
    /// Streams blocked by congestion, reused across passes to avoid reallocating.
    blocked: Vec<StreamId>,
    /// One event taken while deciding whether to park, held for the next pass.
    ///
    /// The park has to answer "is there anything to do", and for the transport the only way
    /// to ask is to take an event. Throwing it away to answer the question would lose it, so
    /// it is kept here and consumed first next time round.
    pushback: Option<QuicEvent>,
    bound: bool,
    peer_gone: bool,
    /// The peer's limits, once it has stated them.
    peer_settings: Option<crate::handlers::PeerSettings>,
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
            closed: Vec::new(),
            blocked: Vec::new(),
            pushback: None,
            bound: false,
            peer_gone: false,
            peer_settings: None,
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
        let mut ids = [StreamId::new(0).expect("zero is a valid identifier"); 3];
        for slot in &mut ids {
            match self.backend.poll_open_uni(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(Self::transport(error))),
                Poll::Ready(Ok(stream)) => *slot = stream,
            }
        }
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
    fn apply_events<R: Role>(&mut self, events: Vec<QuicEvent>, role: &mut R) -> Result<()> {
        // Two sweeps rather than one: a reset behind a megabyte of body data must not wait
        // for the body to be parsed before it is acted on.
        let mut data = Vec::new();
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
                    self.fail_stream(stream, code, role);
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
        Ok(())
    }

    /// Feeds received bytes to the state machine and extends receive credit.
    fn read(&mut self, stream: StreamId, bytes: Bytes, fin: bool) -> Result<()> {
        let now = self.backend.now();
        // Lent for the duration of the call so delivery can take refcounted views of it.
        self.events.set_inbound(Some(bytes.clone()));
        let credit = self
            .conn
            .read_stream(stream, &bytes, fin, now, &mut self.events);
        self.events.set_inbound(None);
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
        if self.closed.contains(&stream) {
            // Closing already released this stream's buffers. Reporting acknowledgement
            // afterwards would claim more was acknowledged than was ever written, which the
            // state machine rejects — correctly, since that is exactly the shape of an
            // accounting bug that frees a buffer early.
            return Ok(());
        }
        if !delivered {
            // The buffer is ours again but the data never arrived. Freeing it is right;
            // reporting it as acknowledged would be a lie the peer could not corroborate.
            return Ok(());
        }
        self.conn
            .add_ack_offset(stream, bytes, &mut self.events)
            .map_err(Into::into)
    }

    /// Fails one exchange without disturbing the rest of the connection.
    fn fail_stream<R: Role>(&mut self, stream: StreamId, code: ErrorCode, role: &mut R) {
        if let Some(entry) = self.registry.remove(stream) {
            let error =
                Error::new(ErrorKind::Stream, "the peer reset this exchange").with_code(code);
            if let Some(slot) = &entry.slot {
                slot.fail(Error::new(
                    ErrorKind::Stream,
                    "the peer reset this exchange",
                ));
            }
            entry.incoming.fail(error);
        }
        role.closed(stream);
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
        if self.closed.contains(&stream) {
            return Ok(());
        }
        self.closed.push(stream);
        self.conn
            .close_stream_with(stream, closed, &mut self.events)
            .ok();
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
                    incoming.push(bytes);
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

    let mut ready = Vec::new();
    let mut resets = Vec::new();
    let mut credit = Vec::new();
    let mut actions = Vec::new();

    loop {
        // 0. Bind, once, before anything else can happen.
        poll_fn(|cx| driver.poll_bind(cx)).await?;

        // 1-2. Transport events, control-plane first, then reads.
        let events = poll_fn(|cx| Poll::Ready(driver.take_events(cx))).await?;
        let had_events = !events.is_empty();
        driver.apply_events(events, &mut guard.role)?;

        // 3. Credit that arrived late, for streams that were QPACK-blocked.
        let deferred = driver.conn.take_deferred_credit();
        for (stream, bytes) in deferred {
            driver.extend(Some(stream), bytes)?;
        }

        // Credit the caller returned by reading.
        credit.clear();
        shared.take_credit(&mut credit);
        for (stream, bytes) in credit.drain(..) {
            driver.extend(Some(stream), bytes)?;
        }

        // 5. Actions the state machine asked the transport to take.
        actions.clear();
        shared.take_actions(&mut actions);
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

        // Resets a caller asked for, by dropping a response future or an unread body.
        resets.clear();
        shared.take_resets(&mut resets);
        for (stream, code) in resets.drain(..) {
            driver.conn.shutdown_stream_read(stream).ok();
            driver
                .backend
                .reset(stream, code)
                .map_err(Driver::<Q>::transport)?;
            driver
                .backend
                .stop_sending(stream, code)
                .map_err(Driver::<Q>::transport)?;
        }

        // Bodies that deferred and have since been woken.
        ready.clear();
        shared.take_ready(&mut ready);
        for stream in ready.drain(..) {
            driver.conn.resume_stream(stream).ok();
        }

        if shared.take_shutdown() {
            driver.conn.shutdown().ok();
        }

        // 6. Submit whatever the role has queued.
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

        // 7. Transmit.
        let mut blocked = core::mem::take(&mut driver.blocked);
        let failure = {
            let mut offers = Offers {
                conn: &mut driver.conn,
                events: &mut driver.events,
                blocked: &mut blocked,
                failure: None,
                retried: false,
            };
            let outcome = poll_fn(|cx| driver.backend.poll_transmit(cx, &mut offers)).await;
            let failure = offers.failure.take();
            match outcome {
                Err(error) => Some(Driver::<Q>::transport(error)),
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

        // 9. Are we finished, and if not, is there anything to do?
        if driver.peer_gone {
            return Ok(());
        }
        if guard.role.done() && registry.is_empty() {
            driver
                .backend
                .close(ErrorCode::new(NO_ERROR), b"")
                .map_err(Driver::<Q>::transport)?;
            return Ok(());
        }

        let idle = !had_events
            && !guard.role.busy()
            && shared.ready_len() == 0
            && !shared.resets_pending()
            && !shared.credit_pending()
            && !shared.actions_pending()
            && !shared.shutdown_pending()
            && driver.events.is_empty()
            && !(guard.role.done() && registry.is_empty());

        if idle {
            poll_fn(|cx| {
                shared.refresh_driver(cx.waker());
                // Re-checked under the waker: work may have arrived between the decision
                // above and registering here, and missing it is a hang rather than a delay.
                if shared.ready_len() > 0
                    || shared.resets_pending()
                    || shared.credit_pending()
                    || shared.actions_pending()
                    || shared.shutdown_pending()
                    || guard.role.busy()
                {
                    return Poll::Ready(());
                }
                // Asking the transport whether it has anything means taking it: there is
                // no peek. So it is taken and kept for the next pass rather than dropped,
                // which is the difference between a slow connection and a lost response.
                match driver.backend.poll_event(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(event)) => {
                        driver.pushback = Some(event);
                        Poll::Ready(())
                    }
                    // An error is not lost either: parking would strand it, so the pass is
                    // resumed and `take_events` reports it.
                    Poll::Ready(Err(_)) => Poll::Ready(()),
                }
            })
            .await;
        }
    }
}
