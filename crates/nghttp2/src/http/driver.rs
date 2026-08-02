//! The connection driver: the one place the session is touched.
//!
//! # Shape
//!
//! The driver is an `async` block rather than a hand-written [`Future`]. That is a
//! constraint, not a preference: the transport traits return `impl Future`, whose types
//! cannot be named, so a hand-written `poll` could only hold an in-flight read or write
//! across calls by boxing it — which allocates on every operation, or demands
//! `Box<dyn Future + Send>` and reintroduces the very `Send` bound the transport traits
//! exist without. An `async` block holds them in its own generated state instead.
//!
//! # Why one half owns the session
//!
//! Reading and writing must proceed at once, or a large upload stalls every download
//! behind it. But both directions want the session, and the session is `!Sync`.
//!
//! The split here is by *responsibility* rather than by direction. The reading half only
//! moves octets: it reads into a pooled buffer and posts it. The session half does
//! everything else — drains commands, resumes deferred bodies, feeds received octets to
//! the session, and writes out whatever the session produces. Only the session half ever
//! touches the session, so no lock and no shared mutability is needed for it, and a read
//! stays in flight across the whole of a write.
//!
//! # One pass
//!
//! Refresh the driver's waker slot, drain the command queue, drain the ready set and
//! resume those streams, feed everything the reading half posted, dispatch what the
//! handlers observed, write out what the session produced, then park. Every wake — from a
//! handle, from a body, from the reading half — starts another pass.
//!
//! [`Future`]: core::future::Future

use core::future::Future;
use core::task::Poll;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::BytesMut;
use http_body::Body;

use crate::{
    ErrorCode, FrameType, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

use super::body::outgoing::Outgoing;
use super::error::{Error, ErrorKind, Result};
use super::head;
use super::shared;
use super::shared::{Incoming, Registry, Shared};
use super::transport::{Transport, TransportRead, TransportWrite};
use super::waker::StreamWaker;

/// Receive windows are replenished by this layer, not by libnghttp2.
///
/// Automatic replenishment credits the peer the moment octets arrive, which is the
/// opposite of backpressure: a slow reader would still invite the peer to send more.
/// Reporting consumption explicitly is what lets the window track what the application
/// has actually taken.
pub(crate) const MANUAL_FLOW_CONTROL: bool = true;

/// How much a single read may take in.
const READ_BUFFER: usize = 16 * 1024;

/// How many filled buffers may wait to be fed before reading pauses.
///
/// Without a bound the reading half would keep pulling octets in as fast as the peer
/// produced them, since it never has to wait for the session half.
const READ_AHEAD: usize = 2;

/// A header block as it arrives: names and values, in order, uninterpreted.
pub(crate) type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// Where a delivered chunk lies inside the buffer `Session::recv` was handed.
#[derive(Debug, Clone, Copy)]
struct Span {
    offset: usize,
    len: usize,
}

/// One thing the handlers observed.
///
/// A single ordered list rather than a bucket per kind, because the order *is* the
/// message. Payload dispatched before its head would have nowhere to go, and payload
/// dispatched after the end of the message would arrive after the caller had been told
/// there was none.
#[derive(Debug)]
enum Event {
    /// A response head completed.
    Head { stream: i32, fields: Fields },
    /// A trailing header block completed.
    Trailers { stream: i32, fields: Fields },
    /// Payload arrived.
    ///
    /// Recorded as an extent of the buffer being read from, and resolved into a
    /// refcounted view of that same buffer once the call returns: the handler is handed
    /// the chunk but not the buffer it lies in, so it cannot take the view itself.
    Data {
        stream: i32,
        span: Option<Span>,
        data: bytes::Bytes,
    },
    /// The peer ended the message.
    End { stream: i32 },
    /// The peer is going away, and will begin nothing above the stream it named.
    Goaway { last_stream: i32, code: ErrorCode },
    /// The stream closed.
    Close {
        stream: i32,
        code: ErrorCode,
        /// What the outgoing body reported, if the stream ended because it failed.
        failure: Option<crate::BodyError>,
    },
}

/// What the session's handlers observed, accumulated for the driver to act on.
///
/// Handlers run inside `Session::send` and `Session::recv` and are handed only this, so
/// they cannot reach the session they are running inside. Everything that needs the
/// session — replenishing a window, resetting a stream — is therefore recorded here and
/// done afterwards.
#[derive(Debug, Default)]
pub(crate) struct Events {
    /// Header blocks still arriving, by stream.
    open: std::collections::BTreeMap<i32, Fields>,
    /// What happened, in the order it happened.
    list: Vec<Event>,
    /// The address range of the buffer currently being read from.
    ///
    /// Plain addresses rather than a borrow, because this outlives any one call and a
    /// lifetime here would infect the session's type. Nothing is ever dereferenced
    /// through them — they are only compared, to learn where a chunk sits.
    base: usize,
    limit: usize,
}

/// Turns the extents recorded during one `recv` into refcounted views of its buffer.
///
/// This is the zero-copy step: `Bytes::slice` over the very buffer the octets were read
/// into, so a chunk the caller keeps is the same memory rather than a copy of it.
fn resolve(events: &mut Events, buffer: &bytes::Bytes, from: usize) {
    for event in &mut events.list[from..] {
        if let Event::Data { span, data, .. } = event {
            if let Some(span) = span.take() {
                *data = buffer.slice(span.offset..span.offset + span.len);
            }
        }
    }
}

/// Builds the session a client connection runs on.
///
/// Kept separate from [`run`] so the flow-control choice above can be asserted against a
/// real session rather than read off a constant.
pub(crate) fn client_session() -> crate::Result<Session<Events>> {
    observing(SessionBuilder::<Events>::client()).build()
}

/// Builds the session a server connection runs on.
pub(crate) fn server_session() -> crate::Result<Session<Events>> {
    observing(SessionBuilder::<Events>::server()).build()
}

/// Wires the handlers that record what arrived.
///
/// The same at both ends: what a header block *means* is the role's business, and is
/// decided when the recorded events are dispatched. Splitting the wiring by role would
/// mean maintaining the aliasing rules and the ordering rules twice for no difference.
fn observing(builder: SessionBuilder<Events>) -> SessionBuilder<Events> {
    builder
        .on_begin_headers(|events: &mut Events, frame| {
            // Requests, responses and trailers all open a block that has to be collected;
            // which one it was is read off the frame again when the block completes. A
            // client never receives a request and a server never receives a response, so
            // accepting all three costs nothing and keeps this end-agnostic.
            let opens_a_block = matches!(
                frame.category(),
                Some(HeaderCategory::Request | HeaderCategory::Response | HeaderCategory::Trailing)
            );
            if opens_a_block {
                events.open.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|events: &mut Events, frame, name: &[u8], value: &[u8]| {
            if let Some(fields) = events.open.get_mut(&frame.stream_id().get()) {
                fields.push((name.to_vec(), value.to_vec()));
            }
            HeaderAction::Continue
        })
        .on_frame(|events: &mut Events, frame| {
            let stream = frame.stream_id().get();
            if frame.kind() == FrameType::HEADERS && frame.is_end_headers() {
                if let Some(fields) = events.open.remove(&stream) {
                    events.list.push(if frame.is_trailers() {
                        Event::Trailers { stream, fields }
                    } else {
                        Event::Head { stream, fields }
                    });
                }
            }
            // Recorded from the flag rather than from stream closure: a stream stays open
            // until both directions have finished, and the message ends first.
            if stream > 0 && frame.is_end_stream() {
                events.list.push(Event::End { stream });
            }

            if let Some(goaway) = frame.goaway() {
                events.list.push(Event::Goaway {
                    last_stream: goaway.last_stream_id().get(),
                    code: goaway.code(),
                });
            }
        })
        .on_data_chunk(|events: &mut Events, stream, chunk: &[u8]| {
            let address = chunk.as_ptr() as usize;
            // Chunks are delivered as views of the buffer handed to `recv`. Checked
            // rather than assumed: a chunk from anywhere else is still correct data and
            // is copied, so an implementation detail changing underneath costs a copy
            // instead of producing wrong bytes.
            let aliases = address >= events.base
                && address.saturating_add(chunk.len()) <= events.limit
                && !chunk.is_empty();
            events.list.push(Event::Data {
                stream: stream.get(),
                span: aliases.then(|| Span {
                    offset: address - events.base,
                    len: chunk.len(),
                }),
                data: if aliases {
                    bytes::Bytes::new()
                } else {
                    bytes::Bytes::copy_from_slice(chunk)
                },
            });
        })
        .on_stream_close(|events: &mut Events, stream, code, failure| {
            events.open.remove(&stream.get());
            events.list.push(Event::Close {
                stream: stream.get(),
                code,
                failure,
            });
        })
        .manual_flow_control(MANUAL_FLOW_CONTROL)
}

/// What a connection does that depends on which end of it this is.
///
/// Everything else about a driver pass — reading, feeding, flushing, crediting, parking —
/// is the same at both ends, and duplicating it would mean maintaining the park predicate
/// and the buffer discipline twice. What genuinely differs is small: where work comes from
/// (a handle's queue, or a handler that has finished), what a completed header block
/// means (an answer arriving, or a request to serve), and when there is nothing left.
pub(crate) trait Role {
    /// Runs whatever is waiting that is not I/O. Called at the top of every pass, before
    /// anything received is fed in.
    fn advance(&mut self, session: &mut Session<Events>) -> Result<()>;

    /// A complete header block arrived on `stream`.
    ///
    /// `incoming` is where that message's payload will be delivered; the role decides who
    /// reads it.
    fn head(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        fields: &[(Vec<u8>, Vec<u8>)],
        incoming: &Arc<Incoming>,
    ) -> Result<()>;

    /// `stream` has closed, and is about to leave the registry.
    fn closed(&mut self, stream: i32);

    /// Whether this end opened `stream`.
    ///
    /// A `GOAWAY` names the last stream *its sender* acted on, so what it abandons is the
    /// work the receiver started — a client's requests, or a server's pushes. A server
    /// reading a client's ordinary `GOAWAY(0)` must not read it as "discard every request
    /// in flight", which is what a role-agnostic reading would say.
    fn started(&self, stream: i32) -> bool;

    /// Fails everything still waiting, because the driver is going away.
    fn abandon(&mut self);

    /// A handle to this role's "is there anything to do?" state.
    ///
    /// Taken once and consulted from inside the park predicate, which is held across an
    /// `await`: borrowing the role itself there would make the whole driver non-`Send` and
    /// defeat the auto-trait inference the transport traits are shaped around. It has to
    /// be *live* rather than a snapshot — a value read before the park would still say
    /// "nothing to do" after the wake that had something to do, and the connection would
    /// sleep through it.
    fn signals(&self) -> Signals;
}

/// A role's readiness, consultable without borrowing the role.
///
/// The closures capture the role's own shared state, so every call sees the present
/// moment.
pub(crate) struct Signals {
    busy: Box<dyn Fn() -> bool + Send + Sync>,
    done: Box<dyn Fn() -> bool + Send + Sync>,
}

impl Signals {
    pub(crate) fn new(
        busy: impl Fn() -> bool + Send + Sync + 'static,
        done: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            busy: Box::new(busy),
            done: Box::new(done),
        }
    }

    /// Whether something is waiting that parking would sit on top of.
    fn busy(&self) -> bool {
        (self.busy)()
    }

    /// Whether nothing further can arrive, so the connection may end.
    fn done(&self) -> bool {
        (self.done)()
    }
}

/// Fails everything still waiting when the driver goes away.
///
/// Taken as an argument to [`run`] rather than created inside it, because an `async fn`
/// stores its arguments in the future the moment it is called. A driver that is dropped
/// without ever being polled therefore still runs this, which is what makes "dropping the
/// connection resolves every pending request" true rather than nearly true.
///
/// It owns the role for the same reason: whatever the role is holding — queued commands, a
/// handler part-way through — is exactly what has to be given up when the driver does.
pub(crate) struct DriverGuard<R: Role> {
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    role: R,
}

impl<R: Role> DriverGuard<R> {
    pub(crate) const fn new(shared: Arc<Shared>, registry: Arc<Registry>, role: R) -> Self {
        Self {
            shared,
            registry,
            role,
        }
    }
}

impl<R: Role> Drop for DriverGuard<R> {
    fn drop(&mut self) {
        // Marked gone first, so a handle racing this sees a closed connection rather than
        // enqueueing a command nothing will ever drain.
        self.shared.set_gone();
        for entry in self.registry.take_all() {
            if let Some(slot) = &entry.slot {
                slot.fail(Error::closed());
            }
            entry.incoming.fail(Error::closed());
        }
        self.role.abandon();
    }
}

/// What the reading half has learned about the far end.
#[derive(Debug, Default)]
struct Intake {
    /// The peer stopped sending, cleanly or otherwise.
    finished: bool,
    failure: Option<std::io::Error>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Read buffers waiting to go back to the pool, and the pool itself.
///
/// # The reuse rule
///
/// Received payload is handed to callers as refcounted views of the buffer it was read
/// into, so a buffer must not be read into again while any of those views is still alive
/// — doing so would rewrite octets a caller is holding. A buffer therefore goes to
/// `holding` after it is fed, and moves to `pool` only once every view derived from it
/// has been dropped, which [`bytes::Bytes::try_into_mut`] reports by succeeding.
///
/// Nothing is leaked when a caller keeps a chunk forever: the memory is retained by the
/// chunk regardless, and the entry here is a pointer's worth of bookkeeping that
/// `HOLDING_LIMIT` bounds. A buffer dropped from `holding` is simply not reused; it is
/// freed when its last chunk is.
struct Buffers {
    pool: Mutex<Vec<BytesMut>>,
    holding: Mutex<Vec<bytes::Bytes>>,
}

/// How many spare read buffers to keep. One per outstanding read, plus room for a buffer
/// coming back while another goes out.
const POOL_LIMIT: usize = READ_AHEAD + 2;

/// How many buffers to keep watching for reuse before giving up on them.
const HOLDING_LIMIT: usize = 16;

impl Buffers {
    fn new() -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            holding: Mutex::new(Vec::new()),
        }
    }

    /// A buffer to read into: a recycled one if any is free, otherwise a fresh one.
    fn take(&self) -> BytesMut {
        let mut buf = lock(&self.pool)
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(READ_BUFFER));
        buf.clear();
        buf
    }

    /// Offers a fed buffer back, and reclaims any earlier one that has come free.
    fn release(&self, buffer: bytes::Bytes) {
        lock(&self.holding).push(buffer);
        self.sweep();
    }

    fn sweep(&self) {
        let waiting: Vec<bytes::Bytes> = core::mem::take(&mut *lock(&self.holding));
        let mut still = Vec::with_capacity(waiting.len());

        for buffer in waiting {
            match buffer.try_into_mut() {
                Ok(mut buf) => {
                    let mut pool = lock(&self.pool);
                    if pool.len() < POOL_LIMIT {
                        buf.clear();
                        pool.push(buf);
                    }
                }
                Err(buffer) => still.push(buffer),
            }
        }

        // Oldest first, so what is dropped is what has been held longest.
        let excess = still.len().saturating_sub(HOLDING_LIMIT);
        *lock(&self.holding) = still.split_off(excess);
    }
}

/// Runs the connection until the peer goes away or nothing is left to do.
pub(crate) async fn run<T, R>(
    transport: T,
    mut session: Session<Events>,
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    mut guard: DriverGuard<R>,
) -> Result<()>
where
    T: Transport,
    R: Role,
{
    let (mut reader, mut writer) = transport.split();

    // Locals of this future, borrowed by both halves. Mutexes rather than a `RefCell`
    // because `&RefCell<T>` is never `Sync`, which would make the whole driver non-`Send`
    // and defeat the auto-trait inference the transport traits are shaped around. The
    // locks are never contended — the halves are polled one at a time on one task.
    let inbox = Mutex::new(VecDeque::<BytesMut>::new());
    let buffers = Buffers::new();
    let intake = Mutex::new(Intake::default());

    let reading = async {
        loop {
            // Pause while the session half is behind. The wake that releases this comes
            // from the session half once it has drained the inbox.
            core::future::poll_fn(|_cx| {
                if lock(&inbox).len() >= READ_AHEAD {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;

            let buf = buffers.take();

            let (result, buf) = reader.read(buf).await;
            match result {
                Ok(0) => {
                    lock(&intake).finished = true;
                    shared.wake_driver();
                    return;
                }
                Ok(_) => {
                    lock(&inbox).push_back(buf);
                    shared.wake_driver();
                }
                Err(failure) => {
                    let mut intake = lock(&intake);
                    intake.finished = true;
                    intake.failure = Some(failure);
                    drop(intake);
                    shared.wake_driver();
                    return;
                }
            }
        }
    };

    let signals = guard.role.signals();

    let driving = async {
        let mut events = Events::default();

        loop {
            buffers.sweep();

            guard.role.advance(&mut session)?;

            // Hand back the window the application has finished with. Done before
            // anything else touches the session, so the `WINDOW_UPDATE` this queues goes
            // out in the same pass rather than waiting for the next wake.
            for (stream, len) in shared.take_credits() {
                session.consume(StreamId::new(stream), len)?;
            }

            // A caller that dropped a request or an unread response body has asked for the
            // stream to stop. Honoured here rather than later: until it is, the peer is
            // still sending a body nobody will read.
            for (stream, code) in shared.take_resets() {
                if session.stream_is_open(StreamId::new(stream)) {
                    session.reset_stream(StreamId::new(stream), code)?;
                }
            }

            if let Some((last_stream, code)) = shared.take_shutdown() {
                session.shutdown(StreamId::new(last_stream), code)?;
            }

            for stream in shared.take_ready() {
                match session.resume_body(StreamId::new(stream)) {
                    Ok(()) => {}
                    // A readiness note that arrived after its stream closed, or after the
                    // body already finished. Benign, and swallowed only here: the same
                    // error from any other call still fails the connection.
                    Err(error) if error.kind() == crate::ErrorKind::InvalidInput => {}
                    Err(error) => return Err(Error::from(error)),
                }
            }

            let mut fed = false;
            loop {
                let next = lock(&inbox).pop_front();
                let Some(buf) = next else { break };

                // Frozen before it is read from, so the chunks the handlers see can be
                // recovered as refcounted views of this very buffer rather than copies.
                let buf = buf.freeze();
                let mark = events.list.len();
                events.base = buf.as_ptr() as usize;
                events.limit = events.base + buf.len();

                let outcome = session.recv(&buf, &mut events);

                events.base = 0;
                events.limit = 0;
                outcome?;

                resolve(&mut events, &buf, mark);
                buffers.release(buf);
                fed = true;
            }
            if fed {
                // The reading half may be parked on the read-ahead bound; there is room
                // now.
                shared.wake_driver();
            }

            dispatch(
                &mut session,
                &mut events,
                &registry,
                &shared,
                &mut guard.role,
            )?;
            flush(&mut session, &mut writer, &mut events).await?;
            // A body announces its trailers while it is being serialised, so they can only
            // be submitted once that pass is over — and then written by a second one.
            if submit_trailers(&mut session, &shared, &registry)? {
                flush(&mut session, &mut writer, &mut events).await?;
            }
            // Serialising fires the stream-close handler, so what it observed is
            // dispatched too rather than waiting for the next pass.
            dispatch(
                &mut session,
                &mut events,
                &registry,
                &shared,
                &mut guard.role,
            )?;

            if lock(&intake).finished && lock(&inbox).is_empty() {
                if let Some(failure) = lock(&intake).failure.take() {
                    return Err(Error::from(failure));
                }
                if session.mid_frame() {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "the peer stopped sending part-way through a frame",
                    ));
                }
                return Ok(());
            }

            if signals.done() && registry.is_empty() && !session.want_write() {
                return Ok(());
            }

            // Read before the park rather than inside it: `&Session` is not `Send`, and a
            // shared borrow captured by the closure would be held across the `await`,
            // making the whole driver non-`Send` and defeating the auto-trait inference
            // the transport traits are shaped around. A snapshot is enough — nothing can
            // make the session want to write again except a command, a resume or received
            // octets, and the predicate already watches all three.
            let wants_write = session.want_write();

            core::future::poll_fn(|_cx| {
                // `wants_write` is redundant today — every path above flushes before
                // reaching here — but not structurally so: a later phase that touches the
                // session after the flush would leave octets queued behind a park that no
                // wake is coming for.
                let idle = !signals.busy()
                    && shared.ready_len() == 0
                    && shared.credits_len() == 0
                    && !shared.resets_pending()
                    && !shared.shutdown_pending()
                    && !shared.trailers_pending()
                    && lock(&inbox).is_empty()
                    && !lock(&intake).finished
                    && !wants_write
                    && !(signals.done() && registry.is_empty());
                if idle { Poll::Pending } else { Poll::Ready(()) }
            })
            .await;
        }
    };

    let outcome = poll_both(&shared, reading, driving).await;
    drop(guard);
    outcome
}

/// Polls both halves on one task, finishing when the session half does.
///
/// The reading half's completion is noted but does not end the driver: the octets it
/// already posted still have to be fed, and the exchanges they complete still have to be
/// answered. It is the session half that decides when there is nothing left.
///
/// This is also the single place the driver's waker slot is refreshed, which is what makes
/// "refreshed every poll, never captured at submission" a property of one line rather than
/// a convention spread across the file.
async fn poll_both<R, D>(shared: &Shared, reading: R, driving: D) -> D::Output
where
    R: Future<Output = ()>,
    D: Future,
{
    let mut reading = core::pin::pin!(reading);
    let mut driving = core::pin::pin!(driving);
    let mut read_done = false;

    core::future::poll_fn(move |cx| {
        shared.refresh_driver(cx.waker());
        if !read_done && reading.as_mut().poll(cx).is_ready() {
            read_done = true;
        }
        driving.as_mut().poll(cx)
    })
    .await
}

/// Wraps a caller's body for the session, with the waker that will resume it.
///
/// Both ends of a connection send bodies and both need the same three things tied
/// together: the bridge, a waker naming the stream, and a liveness token that retires
/// every waker when the stream goes. The identifier does not exist until submission
/// returns one, so the caller binds it — see [`StreamWaker`] for why the cycle is cut that
/// way rather than avoided.
pub(crate) fn outgoing_body<B>(
    shared: &Arc<Shared>,
    liveness: std::sync::Weak<()>,
    body: B,
) -> (Outgoing<B>, Arc<StreamWaker>)
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let waker = Arc::new(StreamWaker::new(Arc::clone(shared), liveness));
    let source = Outgoing::new(body, Arc::clone(&waker), Arc::clone(shared));
    (source, waker)
}

/// Acts on everything the handlers observed, in the order they observed it.
fn dispatch<R: Role>(
    session: &mut Session<Events>,
    events: &mut Events,
    registry: &Registry,
    shared: &Shared,
    role: &mut R,
) -> Result<()> {
    for event in core::mem::take(&mut events.list) {
        match event {
            Event::Head { stream, fields } => {
                // A client made this stream and registered it at submission; a server
                // learns of it here. Either way its payload needs somewhere to go before
                // the role decides who reads it.
                let incoming = match registry.incoming(stream) {
                    Some(incoming) => incoming,
                    None => {
                        let incoming = Arc::new(Incoming::default());
                        registry.insert(stream, None, Arc::clone(&incoming), Arc::new(()));
                        incoming
                    }
                };
                role.head(session, stream, &fields, &incoming)?;
            }

            Event::Trailers { stream, fields } => {
                let Some(incoming) = registry.incoming(stream) else {
                    continue;
                };
                match head::trailers(&fields) {
                    Ok(trailers) => incoming.set_trailers(trailers),
                    Err(error) => {
                        incoming.fail(error);
                        session.reset_stream(StreamId::new(stream), ErrorCode::PROTOCOL_ERROR)?;
                    }
                }
            }

            Event::Data { stream, data, .. } => {
                let len = data.len();
                // Nothing will read payload for a stream nobody is tracking, or one whose
                // body has been dropped, so its window capacity is returned at once rather
                // than being held against a reader that will never arrive.
                let unwanted = registry
                    .incoming(stream)
                    .map_or(len, |incoming| incoming.push(data));
                if unwanted > 0 {
                    session.consume(StreamId::new(stream), unwanted)?;
                }
            }

            Event::End { stream } => {
                if let Some(incoming) = registry.incoming(stream) {
                    incoming.finish();
                }
            }

            Event::Goaway { last_stream, code } => {
                // Nothing new may be started, whatever the code says. A `GOAWAY` carrying
                // NO_ERROR is an orderly wind-down and one carrying anything else is a
                // fault, but neither leaves room for another request.
                shared.set_refusing();

                // Everything *this end started* above the stream the peer named was never
                // begun, which is the one failure a caller may retry without knowing
                // anything else about the request. Streams the peer started are its own
                // business and are unaffected: a server told its client is going away still
                // owes that client the responses it is working on.
                let abandoned: Vec<i32> = registry
                    .above(last_stream)
                    .into_iter()
                    .filter(|stream| role.started(*stream))
                    .collect();
                for stream in abandoned {
                    let Some(entry) = registry.remove(stream) else {
                        continue;
                    };
                    role.closed(stream);
                    if let Some(slot) = &entry.slot {
                        slot.fail(Error::refused().because(code));
                    }
                    entry.incoming.fail(Error::refused().because(code));
                }
            }

            Event::Close {
                stream,
                code,
                failure,
            } => {
                let Some(entry) = registry.remove(stream) else {
                    continue;
                };
                role.closed(stream);

                let reported = failure.map(recover);
                let unanswered = entry.slot.as_ref().filter(|slot| !slot.is_settled());
                if let Some(slot) = unanswered {
                    slot.fail(reported.unwrap_or_else(|| {
                        if code == ErrorCode::NO_ERROR {
                            Error::new(
                                ErrorKind::Stream,
                                "the stream closed before a response head arrived",
                            )
                        } else {
                            Error::new(ErrorKind::Stream, "the peer reset the stream").because(code)
                        }
                    }));
                } else if let Some(reported) = reported {
                    // The response head was already delivered, so the only channel left
                    // to the caller is the body it is holding. A request body that failed
                    // after its response began is still the caller's to hear about.
                    entry.incoming.fail(reported);
                }
                // A no-op once the message has ended: what is still queued is the whole
                // of it, and the caller is entitled to read it after the stream is gone.
                entry.incoming.fail(shared::truncated());
            }
        }
    }

    Ok(())
}

/// Recovers the error an outgoing body reported.
///
/// The session parks whatever a body handed to [`BodyOutcome::Fail`] and gives it back at
/// stream close, by value. Everything this crate puts in there is one of its own errors,
/// so the caller sees the cause they produced rather than a printed rendering of it. A box
/// from anywhere else — a body source submitted directly against the sans-I/O layer — is
/// carried as a source instead of being discarded.
///
/// [`BodyOutcome::Fail`]: crate::BodyOutcome::Fail
fn recover(failure: crate::BodyError) -> Error {
    match failure.downcast::<Error>() {
        Ok(error) => *error,
        Err(other) => Error::with_source(
            ErrorKind::Body,
            "the outgoing body reported an error",
            // `BodyError` is only `Send`; the taxonomy's source is `Send + Sync`, so the
            // rendering is carried rather than the box.
            other.to_string(),
        ),
    }
}

/// Submits every trailing block an outgoing body left behind.
///
/// Deliberately after the send pass. A body announces trailers while it is being
/// serialised, and they only become legal once the message they follow has gone out —
/// which is the very call the announcement came from.
///
/// Returns whether anything was submitted, since submitting makes the session want to
/// write again.
fn submit_trailers(
    session: &mut Session<Events>,
    shared: &Shared,
    registry: &Registry,
) -> Result<bool> {
    let mut submitted = false;

    for (stream, trailers) in shared.take_trailers() {
        // The stream may have been reset between the announcement and here, in which case
        // the trailer window closed with it. Checked rather than caught: submitting to a
        // stream that cannot carry trailers is a caller error, and it would be
        // indistinguishable from a real one.
        if !session.trailers_ready(StreamId::new(stream)) {
            continue;
        }

        let fields = match head::trailer_fields(&trailers) {
            Ok(fields) => fields,
            Err(error) => {
                // A trailing block this crate cannot encode is one message's problem, not
                // the connection's. The receive side treats the mirror image the same way,
                // and the asymmetry would otherwise be that a caller's own bad trailer is
                // more destructive than a peer's.
                fail_stream(registry, stream, error);
                session.reset_stream(StreamId::new(stream), ErrorCode::INTERNAL_ERROR)?;
                continue;
            }
        };

        session.submit_trailer(StreamId::new(stream), &fields.views())?;
        submitted = true;
    }

    Ok(submitted)
}

/// Tells whoever is waiting on `stream` why it is going away.
///
/// The response future if it has not been answered, and the receiving body otherwise —
/// which is the only channel left once a head has been delivered.
fn fail_stream(registry: &Registry, stream: i32, error: Error) {
    let Some(slot) = registry.slot(stream) else {
        return;
    };
    if slot.is_settled() {
        if let Some(incoming) = registry.incoming(stream) {
            incoming.fail(error);
        }
    } else {
        slot.fail(error);
    }
}

/// Writes out everything the session currently has to say.
///
/// Which of the two strategies runs is the transport's choice, because they cannot be
/// combined: the session invalidates each block when the next is asked for, so blocks can
/// only be gathered into one write by copying them.
async fn flush<W: TransportWrite>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
) -> Result<()> {
    if writer.writes_borrowed() {
        while let Some(block) = session.send(events)? {
            let mut offset = 0;
            while offset < block.len() {
                let written = writer.write_borrowed(&block[offset..]).await?;
                if written == 0 {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "the transport accepted no octets and reported no error",
                    ));
                }
                offset += written;
            }
        }
        return Ok(());
    }

    let mut out = BytesMut::new();
    while let Some(block) = session.send(events)? {
        out.extend_from_slice(block);
    }
    let mut pending = out.freeze();
    while !pending.is_empty() {
        let (result, returned) = writer.write(pending).await;
        let written = result?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::Transport,
                "the transport accepted no octets and reported no error",
            ));
        }
        pending = returned.slice(written..);
    }
    Ok(())
}
