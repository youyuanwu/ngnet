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

use bytes::{Bytes, BytesMut};
use http_body::Body;

use crate::settings::Setting;
use crate::{
    ErrorCode, FrameType, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

use super::body::outgoing::Outgoing;
use super::config::Config;
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

/// The block size at which the vectored path stops copying and starts gathering.
///
/// A session block below this is appended to a buffer the driver owns; one at or above it
/// is handed to the socket as its own region, uncopied. The value separates two populations
/// rather than cutting through one: a multiplexed pass is control and header frames of a
/// few dozen octets, and a body pass is `DATA` frames of a little over 16 KiB, with nothing
/// measured in between. So anything from about 64 to 16,384 would behave identically on
/// real traffic, and the choice within that range is a tuning decision rather than a
/// correctness one — 256 is what the ecosystem's other HTTP/2 implementation uses when
/// gathering is available, and measurement here agreed.
const VECTORED_THRESHOLD: usize = 256;

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
pub(crate) fn client_session(config: &Config) -> crate::Result<Session<Events>> {
    configured(observing(SessionBuilder::<Events>::client()), config).build()
}

/// Builds the session a server connection runs on.
pub(crate) fn server_session(config: &Config) -> crate::Result<Session<Events>> {
    configured(observing(SessionBuilder::<Events>::server()), config).build()
}

/// Advertises this crate's connection limits in the initial `SETTINGS` frame.
///
/// libnghttp2's own defaults for these two are effectively unlimited, so without this a
/// peer could open unbounded streams and force unbounded header copies; see [`Config`] for
/// why the built-in values are what they are.
fn configured(builder: SessionBuilder<Events>, config: &Config) -> SessionBuilder<Events> {
    builder
        .setting(Setting::MaxConcurrentStreams(config.concurrency()))
        .setting(Setting::MaxHeaderListSize(config.header_list_size()))
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

    /// A header block that libnghttp2 categorised as trailers arrived on `stream`.
    ///
    /// The default is to attach them to the message being received, which is what a
    /// server does with a request's trailers. The client overrides this: libnghttp2
    /// reports the final response that follows a `1xx` under the same category as genuine
    /// trailers, so that end has to disambiguate the two.
    fn trailers(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        fields: &[(Vec<u8>, Vec<u8>)],
        incoming: &Arc<Incoming>,
    ) -> Result<()> {
        deliver_trailers(session, stream, fields, incoming)
    }

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
    /// Where the pool's size is reported, so a test can watch it settle. Nothing in the
    /// driver reads it back.
    gauge: Arc<Shared>,
}

/// How many spare read buffers to keep. One per outstanding read, plus room for a buffer
/// coming back while another goes out.
const POOL_LIMIT: usize = READ_AHEAD + 2;

/// How many buffers to keep watching for reuse before giving up on them.
const HOLDING_LIMIT: usize = 16;

/// A read buffer whose shared representation has already been forced.
///
/// A `BytesMut` is first backed by a plain allocation; the first time a `Bytes` derived
/// from it is cloned or sliced, it promotes to a reference-counted representation, and that
/// promotion allocates. Forcing it once here — `split_off(0)` yields the whole buffer in
/// the shared representation — means every later slice only bumps a refcount, and because
/// reclaiming the buffer keeps that representation, the cost is paid once for the buffer's
/// whole life rather than once per reuse. Without it, handing a streamed body's chunks to
/// the caller as owned `Bytes` would allocate on every driver pass, which the steady-state
/// allocation harness exists to forbid.
fn new_read_buffer() -> BytesMut {
    let mut buf = BytesMut::with_capacity(READ_BUFFER);
    buf.split_off(0)
}

impl Buffers {
    fn new(gauge: Arc<Shared>) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            holding: Mutex::new(Vec::new()),
            gauge,
        }
    }

    /// A buffer to read into: a recycled one if any is free, otherwise a fresh one.
    fn take(&self) -> BytesMut {
        let (buf, size) = {
            let mut pool = lock(&self.pool);
            let buf = pool.pop();
            (buf, pool.len())
        };
        self.gauge.note_pool_size(size);
        let mut buf = buf.unwrap_or_else(new_read_buffer);
        buf.clear();
        buf
    }

    /// Offers a fed buffer back, and reclaims any earlier one that has come free.
    fn release(&self, buffer: bytes::Bytes) {
        lock(&self.holding).push(buffer);
        self.sweep();
    }

    fn sweep(&self) {
        let mut holding = lock(&self.holding);
        let mut pool = lock(&self.pool);

        // Partitioned in place rather than into a fresh vector: this runs on every pass,
        // often several times, so a scratch allocation here would be a per-pass cost on
        // the steady-state path the whole design is trying to keep free of them. Each
        // buffer is lifted out against an empty `Bytes` placeholder — which owns nothing
        // and allocates nothing — reclaimed if its last view has been dropped, and kept
        // by compacting it toward the front otherwise.
        let mut kept = 0;
        for index in 0..holding.len() {
            let buffer = core::mem::replace(&mut holding[index], bytes::Bytes::new());
            match buffer.try_into_mut() {
                Ok(mut buf) => {
                    if pool.len() < POOL_LIMIT {
                        buf.clear();
                        pool.push(buf);
                    }
                }
                Err(buffer) => {
                    holding[kept] = buffer;
                    kept += 1;
                }
            }
        }
        holding.truncate(kept);

        let size = pool.len();
        drop(pool);
        self.gauge.note_pool_size(size);

        // Oldest first, so what is dropped is what has been held longest. `drain` keeps the
        // vector's capacity, so bounding the set costs no allocation either.
        let excess = holding.len().saturating_sub(HOLDING_LIMIT);
        if excess > 0 {
            holding.drain(..excess);
        }
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
    let buffers = Buffers::new(Arc::clone(&shared));
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

            // The transport appends into the buffer's spare capacity, so its length before
            // the read is where the fresh octets begin. The returned count is only an EOF
            // indicator (see `TransportRead::read`); the octets themselves are read off the
            // buffer by how much it grew. This asserts the two agree so an adapter that
            // reads but forgets to append cannot pass silently in debug builds.
            let filled = buf.len();

            let (result, buf) = reader.read(buf).await;
            match result {
                Ok(0) => {
                    lock(&intake).finished = true;
                    shared.wake_driver();
                    return;
                }
                Ok(count) => {
                    debug_assert_eq!(
                        buf.len(),
                        filled + count,
                        "a transport read must append its reported octets, growing the buffer by \
                         exactly that many",
                    );
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
        // Reused across passes so draining the command sets costs no allocation on the
        // steady-state path — the shared collections keep their capacity when drained, and
        // so do these. The steady-state allocation harness is what holds this to account.
        let mut credited: Vec<(i32, usize)> = Vec::new();
        let mut to_reset: Vec<(i32, crate::ErrorCode)> = Vec::new();
        let mut to_resume: Vec<i32> = Vec::new();
        // The vectored path's accumulation buffer, reused across passes exactly as the
        // collections above are: cleared rather than reallocated, so it stops allocating
        // once it has grown to the size a pass needs. Untouched on the other two paths.
        let mut gathered = BytesMut::new();
        // The owned path's coalescing buffer, reused for the same reason. It reaches the
        // transport as an owned `Bytes`, which would ordinarily consume the allocation and
        // force the next pass to start from nothing; `flush` hands it over with
        // `split().freeze()` instead, so the capacity comes back here once the transport has
        // dropped what it was given. See the note there — the reuse only holds because of
        // that pairing, and writing `freeze()` would silently restore a per-pass allocation.
        let mut coalesced = BytesMut::new();

        loop {
            buffers.sweep();

            guard.role.advance(&mut session)?;

            // Hand back the window the application has finished with. Done before
            // anything else touches the session, so the `WINDOW_UPDATE` this queues goes
            // out in the same pass rather than waiting for the next wake.
            shared.take_credits_into(&mut credited);
            for (stream, len) in &credited {
                session.consume(StreamId::new(*stream), *len)?;
            }

            // A caller that dropped a request or an unread response body has asked for the
            // stream to stop. Honoured here rather than later: until it is, the peer is
            // still sending a body nobody will read.
            shared.take_resets_into(&mut to_reset);
            for (stream, code) in &to_reset {
                if session.stream_is_open(StreamId::new(*stream)) {
                    session.reset_stream(StreamId::new(*stream), *code)?;
                }
            }

            if let Some((last_stream, code)) = shared.take_shutdown() {
                session.shutdown(StreamId::new(last_stream), code)?;
            }

            shared.take_ready_into(&mut to_resume);
            for stream in &to_resume {
                match session.resume_body(StreamId::new(*stream)) {
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
            flush(
                &mut session,
                &mut writer,
                &mut events,
                &mut gathered,
                &mut coalesced,
            )
            .await?;
            // A body announces its trailers while it is being serialised, so they can only
            // be submitted once that pass is over — and then written by a second one.
            if submit_trailers(&mut session, &shared, &registry)? {
                flush(
                    &mut session,
                    &mut writer,
                    &mut events,
                    &mut gathered,
                    &mut coalesced,
                )
                .await?;
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

            // Everything this pass produced has been written; commit it to the peer-visible
            // stream before the pass can end in a park or a return. A buffering transport
            // holds octets until this call, so skipping it before awaiting the peer — or
            // before finishing — would strand the request behind a buffer and hang the
            // connection. The honest cases (a raw socket, the in-memory duplex) flush
            // nothing here.
            writer.commit().await?;

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

/// Attaches a received trailer block to the message it belongs to.
///
/// The default handling of trailers, shared by the `Role` trait's default `trailers` and
/// the client's fallback once it has ruled out a post-`1xx` final response.
pub(super) fn deliver_trailers(
    session: &mut Session<Events>,
    stream: i32,
    fields: &[(Vec<u8>, Vec<u8>)],
    incoming: &Arc<Incoming>,
) -> Result<()> {
    match head::trailers(fields) {
        Ok(trailers) => incoming.set_trailers(trailers),
        Err(error) => {
            incoming.fail(error);
            session.reset_stream(StreamId::new(stream), ErrorCode::PROTOCOL_ERROR)?;
        }
    }
    Ok(())
}

/// Acts on everything the handlers observed, in the order they observed it.
fn dispatch<R: Role>(
    session: &mut Session<Events>,
    events: &mut Events,
    registry: &Registry,
    shared: &Shared,
    role: &mut R,
) -> Result<()> {
    for event in events.list.drain(..) {
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
                role.trailers(session, stream, &fields, &incoming)?;
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
/// Which strategy runs is the transport's choice, expressed once through
/// [`TransportWrite::write_vectored`] or [`TransportWrite::write_borrowed`]: the first that
/// offers itself wins, and the choice is read once and held for the rest of the pass.
///
/// * **Vectored.** Blocks below [`VECTORED_THRESHOLD`] accumulate into `gathered`, a buffer
///   the driver owns and reuses across passes; a block at or above it is handed to the
///   socket beside whatever has accumulated, in one gathering call, and never copied. A
///   pass of only small blocks therefore costs one write, and a pass of large ones costs
///   one per block with nothing copied.
/// * **Borrowed.** Each block is written on its own, uncopied: one write per block.
/// * **Owned.** Every block is copied into `out`, a second driver-owned buffer reused the
///   same way, and sent in a single write. The copy is unavoidable here — the transport
///   takes ownership — but the buffer behind it is not reallocated per pass.
///
/// Several session blocks cannot be gathered *with each other*, because the session
/// invalidates each block when the next is asked for and [`Session::send`] enforces that by
/// borrowing the session for as long as the block lives. That is the whole of the
/// constraint: one block gathers perfectly well with memory the driver already owns, which
/// is why the vectored strategy needs at most two regions and never more.
///
/// This does not commit the octets to the peer; [`TransportWrite::commit`] does, and the
/// caller runs it once the pass is fully drained.
async fn flush<W: TransportWrite>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    gathered: &mut BytesMut,
    out: &mut BytesMut,
) -> Result<()> {
    let mut coalescing = false;
    // The election. Reading it costs one constructed future that is immediately dropped
    // without being polled, which the trait's contract requires transports to tolerate.
    let vectored = writer.write_vectored(&[]).is_some();
    // Whatever a previous pass left behind has already been written or copied; starting
    // from empty means no error path can leak a remainder into the next pass. Both buffers
    // keep their capacity across the clear, which is what makes the steady state free of
    // allocation on the vectored and owned paths alike.
    gathered.clear();
    out.clear();

    while let Some(block) = session.send(events)? {
        if coalescing {
            out.extend_from_slice(block);
            continue;
        }
        if vectored {
            if block.len() < VECTORED_THRESHOLD {
                gathered.extend_from_slice(block);
                continue;
            }
            // Big enough to be worth a syscall of its own, so it goes out uncopied, with
            // the accumulation riding along as the first region rather than as a second
            // write.
            match write_gathering(writer, gathered, block).await? {
                Gathered::All => gathered.clear(),
                Gathered::Declined { done } => {
                    // The transport has reneged on its own election, which the contract
                    // forbids. Failing the connection over it would be a worse answer than
                    // paying the copy: the remainder joins the coalescing buffer, in order,
                    // and the pass finishes on the owned path.
                    if done < gathered.len() {
                        out.extend_from_slice(&gathered[done..]);
                        out.extend_from_slice(block);
                    } else {
                        out.extend_from_slice(&block[done - gathered.len()..]);
                    }
                    gathered.clear();
                    coalescing = true;
                }
            }
            continue;
        }
        let mut offset = 0;
        while offset < block.len() {
            match writer.write_borrowed(&block[offset..]) {
                Some(write) => {
                    let written = write.await?;
                    if written == 0 {
                        return Err(Error::new(
                            ErrorKind::Transport,
                            "the transport accepted no octets and reported no error",
                        ));
                    }
                    offset += written;
                }
                // The transport does not lend the fast path; from here on the pass is
                // coalesced into one owned write, this block included.
                None => break,
            }
        }
        if offset < block.len() {
            coalescing = true;
            out.extend_from_slice(&block[offset..]);
        }
    }

    // Small blocks with no large one behind them: the common multiplexed pass, and the one
    // this strategy exists for. One write for the lot.
    if !gathered.is_empty() {
        match write_gathering(writer, gathered, &[]).await? {
            Gathered::All => {}
            Gathered::Declined { done } => out.extend_from_slice(&gathered[done..]),
        }
        gathered.clear();
    }

    // `split` rather than `freeze`: the transport takes ownership of what it is handed, so
    // freezing `out` itself would give the allocation away and leave the next pass to build
    // a new one — which is exactly what this path used to do, at four allocations on a plain
    // upload and twelve on a multiplexed pass. Splitting hands over the octets while leaving
    // the allocation here, and `bytes` returns the capacity once the transport has dropped
    // its handle, so the steady state allocates nothing. The reclaim depends on that handle
    // actually being dropped before the next pass; a transport that retained it would simply
    // cost an allocation again rather than misbehave.
    //
    // The empty case is taken first and is not merely an optimisation of the common path —
    // it keeps this buffer's cost off the two strategies that never fill it. Once `out` has
    // been split even once it is `KIND_ARC`, and from then on `split` is an atomic increment
    // and the dropped handle an atomic decrement. Paying two atomics per pass to hand over
    // nothing would tax the vectored and borrowed paths for a buffer they do not use.
    let mut pending = if out.is_empty() {
        Bytes::new()
    } else {
        out.split().freeze()
    };
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

/// How a gathering write ended.
enum Gathered {
    /// Every octet offered was accepted.
    All,
    /// The transport withdrew the path partway, having accepted `done` octets of the
    /// logical concatenation.
    Declined { done: usize },
}

/// Writes `head` immediately followed by `tail`, as one gathering operation.
///
/// Retries the remainder on a short write, which is why the two regions are recomputed each
/// time round rather than advanced in place. Three cases arise and the middle one is the
/// one worth naming: when the accepted prefix lands *exactly* on the boundary between the
/// regions, what remains is the second region alone — offering it beside a now-empty first
/// region would hand the transport a zero-length `IoSlice`, which the contract promises
/// never to do and which some transports would count as a region for nothing.
async fn write_gathering<W: TransportWrite>(
    writer: &mut W,
    head: &[u8],
    tail: &[u8],
) -> Result<Gathered> {
    let total = head.len() + tail.len();
    let mut done = 0;
    while done < total {
        let (first, second): (&[u8], &[u8]) = if done < head.len() {
            (&head[done..], tail)
        } else {
            (&tail[done - head.len()..], &[])
        };
        // `first` is never empty: the loop condition guarantees octets remain, and both
        // arms slice from a position strictly inside their own region.
        let both = [std::io::IoSlice::new(first), std::io::IoSlice::new(second)];
        let regions = if second.is_empty() {
            &both[..1]
        } else {
            &both[..]
        };
        match writer.write_vectored(regions) {
            Some(write) => {
                let written = write.await?;
                if written == 0 {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "the transport accepted no octets and reported no error",
                    ));
                }
                done += written;
            }
            None => return Ok(Gathered::Declined { done }),
        }
    }
    Ok(Gathered::All)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    const MAX_CONCURRENT_STREAMS_ID: u16 = 0x03;
    const MAX_HEADER_LIST_SIZE_ID: u16 = 0x06;

    fn drain(session: &mut Session<Events>) -> Vec<u8> {
        let mut events = Events::default();
        let mut wire = Vec::new();
        while let Some(block) = session.send(&mut events).expect("send failed") {
            wire.extend_from_slice(block);
        }
        wire
    }

    /// Parses the SETTINGS payload following an optional preface into (id, value) pairs.
    fn settings_entries(wire: &[u8], skip_preface: bool) -> Vec<(u16, u32)> {
        let start = if skip_preface { CLIENT_MAGIC.len() } else { 0 } + 9;
        wire[start..]
            .chunks_exact(6)
            .map(|c| {
                (
                    u16::from_be_bytes([c[0], c[1]]),
                    u32::from_be_bytes([c[2], c[3], c[4], c[5]]),
                )
            })
            .collect()
    }

    #[test]
    fn a_client_advertises_its_concurrency_and_header_limits() {
        let config = Config::default();
        let mut session = client_session(&config).unwrap();
        let entries = settings_entries(&drain(&mut session), true);

        assert!(
            entries.contains(&(MAX_CONCURRENT_STREAMS_ID, config.concurrency())),
            "the initial SETTINGS must carry the concurrency cap, got {entries:?}"
        );
        assert!(
            entries.contains(&(MAX_HEADER_LIST_SIZE_ID, config.header_list_size())),
            "the initial SETTINGS must carry the header-list limit, got {entries:?}"
        );
    }

    #[test]
    fn a_server_advertises_its_concurrency_and_header_limits() {
        let config = Config::default();
        let mut session = server_session(&config).unwrap();
        let entries = settings_entries(&drain(&mut session), false);

        assert!(
            entries.contains(&(MAX_CONCURRENT_STREAMS_ID, config.concurrency())),
            "the initial SETTINGS must carry the concurrency cap, got {entries:?}"
        );
        assert!(
            entries.contains(&(MAX_HEADER_LIST_SIZE_ID, config.header_list_size())),
            "the initial SETTINGS must carry the header-list limit, got {entries:?}"
        );
    }

    #[test]
    fn a_caller_can_override_the_advertised_limits() {
        let config = Config::default()
            .max_concurrent_streams(7)
            .max_header_list_size(9000);
        let mut session = client_session(&config).unwrap();
        let entries = settings_entries(&drain(&mut session), true);

        assert!(
            entries.contains(&(MAX_CONCURRENT_STREAMS_ID, 7)),
            "an overridden concurrency cap must be the one advertised, got {entries:?}"
        );
        assert!(
            entries.contains(&(MAX_HEADER_LIST_SIZE_ID, 9000)),
            "an overridden header-list limit must be the one advertised, got {entries:?}"
        );
    }
}
