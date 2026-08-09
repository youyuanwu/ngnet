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
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use bytes::{Bytes, BytesMut};
use http_body::Body;

use crate::settings::Setting;
use crate::state::SendRecord;
use crate::{
    ErrorCode, FrameType, Header, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

use super::body::outgoing::Outgoing;
use super::body::shared::SharedOutgoing;
use super::config::Config;
use super::error::{Error, ErrorKind, Result};
use super::head;
use super::shared;
use super::shared::{Incoming, Registry, Shared};
use super::transport::{
    BorrowedWrite, Completion, CompletionModel, Drains, Pass, Readiness, ReadinessModel,
    RegionWrite, Transport, TransportRead, TransportWrite,
};
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

/// The most descriptors a gathering write's region list may hold before the driver writes
/// what it has and begins a new list.
///
/// A no-copy pass is a handful of regions in practice — the measured pass carries at most
/// four `DATA` frames beside one accumulated control run, which this design splits into a
/// header region and a payload region each, so about nine regions. What holds it there is
/// flow control: a peer's initial 64 KiB window admits about four 16 KiB frames per pass.
/// But `SETTINGS_INITIAL_WINDOW_SIZE` is the peer's to choose, and a peer advertising a large
/// window could have libnghttp2 serialise many `DATA` frames in a single `send_into`, each
/// depositing a record. This cap is the guard rail against exactly that: it is far below
/// Linux's `IOV_MAX` of 1024 and far above the measured nine, so under a default window it
/// never binds, and under a window large enough to reach it the list is flushed and restarted
/// rather than overrunning. It is a bound that always holds, not a case that never arises —
/// `http_vectored.rs::a_pass_driven_past_the_region_cap_holds_the_bound_and_stays_correct`
/// drives a peer-advertised window big enough to bind it several times in one pass. The stack
/// array a write materialises into is one longer than this, because a live session block may
/// ride as the trailing region of a list that is already this full (design decisions D6
/// and D9).
const MAX_REGIONS: usize = 64;

/// A lifetime-free description of one region of a gathering write.
///
/// The region list is retained across [`send_into`](Session::send_into) calls and across
/// passes, exactly as the driver's buffers are — and [`send_into`] returns a block
/// borrowing the session that the next call invalidates, so nothing the list holds may
/// borrow the session. Two further borrows rule out a retained `Vec<IoSlice>`: the regions
/// point into the accumulation buffer and into the record sink, and both are appended to
/// during the same loop, so a list of live slices cannot coexist with buffers being grown.
/// The list therefore holds indices, which own no borrow at all, and the slices are
/// recovered at write time when every borrow is shared and nothing is being mutated (design
/// decision D9).
enum Region {
    /// A run of the accumulation buffer, `gathered[start..end]` — a run of small blocks
    /// between two records, gathered into a single region.
    Gathered { start: usize, end: usize },
    /// The nine-octet frame header of the record at this index in the sink.
    Header(usize),
    /// The handed-over payload of the record at this index in the sink.
    Payload(usize),
}

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
        if let Event::Data { span, data, .. } = event
            && let Some(span) = span.take()
        {
            *data = buffer.slice(span.offset..span.offset + span.len);
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
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && let Some(fields) = events.open.remove(&stream)
            {
                events.list.push(if frame.is_trailers() {
                    Event::Trailers { stream, fields }
                } else {
                    Event::Head { stream, fields }
                });
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

    // The one capability this layer reads, read once, here, before a single octet is
    // written. `is_write_vectored` answers whether the writer's gathering operation reaches
    // a real scatter-gather call or the provided emulation's loop; `true` routes every pass
    // of this connection's life through the gathered drain, `false` through the coalesced
    // one. It is asked once rather than per pass because it is a property of the underlying
    // I/O and cannot change while the connection is up — the same reason tokio's
    // `AsyncWrite::is_write_vectored` is a plain `&self -> bool`, answerable without I/O.
    //
    // Two earlier revisions each got half of this. The first asked the writer a `gathers()`
    // question here and threaded the answer through every drain — right about *where* to ask,
    // wrong in that the question defaulted to `true`, so a transport that had never thought
    // about gathering was reported as gathering well. The second removed the question
    // entirely and took the answer from the caller's `Config` instead, on the reasoning that
    // how many writes a pass becomes is a tuning decision belonging to the layer that owns
    // the accumulation buffer. That reasoning does not survive: the decision does not turn on
    // what this layer owns, it turns on whether the *transport's* `write_vectored` is real,
    // which this layer cannot see and the caller generally does not know. Asking the
    // transport puts the question to the only party that can answer it, and the `false`
    // default makes silence mean the conservative answer instead of the optimistic one.
    let vectored = writer.is_write_vectored();

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
        // The write-side scratch, retained across passes so the steady state allocates
        // nothing — every buffer keeps its capacity when cleared. Grouped into one value
        // both to keep [`flush`]'s signature small and because the buffers share a lifetime
        // and a purpose; the field comments record what each strategy uses.
        let mut write = WriteBuffers::new();

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
            flush(&mut session, &mut writer, &mut events, &mut write, vectored).await?;
            // A body announces its trailers while it is being serialised, so they can only
            // be submitted once that pass is over — and then written by a second one.
            if submit_trailers(&mut session, &shared, &registry)? {
                flush(&mut session, &mut writer, &mut events, &mut write, vectored).await?;
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

/// Wraps a caller's body for the session as a *no-copy* source, with its resume waker.
///
/// The no-copy counterpart of [`outgoing_body`]: it builds a [`SharedOutgoing`] instead of
/// an [`Outgoing`], so the body's own [`bytes::Bytes`] is handed to the session rather than
/// copied into libnghttp2's serialisation buffer — the memset of that buffer and the
/// source-side copy into it are both gone. On the two readiness strategies the payload then
/// reaches the transport uncopied as well — as its own region of a gathering write on the
/// vectored path, as a write of its own on the borrowed one; the owned strategy still
/// coalesces it once, which is inherent to a transport that takes ownership of what it is
/// handed. The waker and liveness plumbing are identical, because deferral and resumption
/// work the same way whichever kind of source is underneath.
pub(crate) fn shared_outgoing_body<B>(
    shared: &Arc<Shared>,
    liveness: Weak<()>,
    body: B,
) -> (SharedOutgoing<B>, Arc<StreamWaker>)
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let waker = Arc::new(StreamWaker::new(Arc::clone(shared), liveness));
    let source = SharedOutgoing::new(body, Arc::clone(&waker), Arc::clone(shared));
    (source, waker)
}

/// How a connection submits the bodies it carries: by copying, or by handing them over.
///
/// A connection is wholly one or wholly the other — the choice is fixed when it is built,
/// not per request (design decision D7 and FR-012) — so it is a type parameter on the
/// role rather than a runtime branch. Both implementations are zero-sized; the trait
/// carries no state, only the two submission acts that differ between the push and no-copy
/// paths. Everything else a role does is identical whichever body plan it holds, which is
/// why the plan reaches no further than these two methods.
///
/// This is entirely crate-internal: the roles that carry it are private, so widening a
/// role from `Role<B>` to `Role<B, P>` is invisible to callers. The public opt-in is the
/// four `*_shared*` entry points, each of which simply fixes `P` to [`SharedBodies`].
pub(crate) trait BodyPlan<B>: Send + 'static
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Submits `body` as a request, returning the assigned stream and its resume waker.
    fn submit_request(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        views: &[Header<'_>],
        body: B,
    ) -> Result<(StreamId, Arc<StreamWaker>)>;

    /// Submits `body` as a response on an already-open stream, returning its resume waker.
    fn submit_response(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        stream: StreamId,
        views: &[Header<'_>],
        body: B,
    ) -> Result<Arc<StreamWaker>>;
}

/// The push body plan: each chunk is copied into libnghttp2's frame buffer.
///
/// The plan every connection uses today, and every one that does not opt in to no-copy.
pub(crate) struct PushBodies;

impl<B> BodyPlan<B> for PushBodies
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn submit_request(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        views: &[Header<'_>],
        body: B,
    ) -> Result<(StreamId, Arc<StreamWaker>)> {
        let (source, waker) = outgoing_body(shared, liveness, body);
        let stream = session.submit_request_with_body(views, source)?;
        Ok((stream, waker))
    }

    fn submit_response(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        stream: StreamId,
        views: &[Header<'_>],
        body: B,
    ) -> Result<Arc<StreamWaker>> {
        let (source, waker) = outgoing_body(shared, liveness, body);
        session.submit_response_with_body(stream, views, source)?;
        Ok(waker)
    }
}

/// The no-copy body plan: each chunk is handed over as the caller's own [`bytes::Bytes`].
///
/// Bounded on `B::Data = Bytes`, which is what makes the hand-over possible — the payload
/// libnghttp2 would otherwise copy is already a reference-counted buffer the crate can
/// give away. This bound is why no-copy is a parallel set of entry points rather than a
/// widening of the existing ones (design decision D1).
pub(crate) struct SharedBodies;

impl<B> BodyPlan<B> for SharedBodies
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn submit_request(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        views: &[Header<'_>],
        body: B,
    ) -> Result<(StreamId, Arc<StreamWaker>)> {
        let (source, waker) = shared_outgoing_body(shared, liveness, body);
        let stream = session.submit_request_with_shared_body(views, source)?;
        Ok((stream, waker))
    }

    fn submit_response(
        shared: &Arc<Shared>,
        session: &mut Session<Events>,
        liveness: Weak<()>,
        stream: StreamId,
        views: &[Header<'_>],
        body: B,
    ) -> Result<Arc<StreamWaker>> {
        let (source, waker) = shared_outgoing_body(shared, liveness, body);
        session.submit_response_with_shared_body(stream, views, source)?;
        Ok(waker)
    }
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

/// The driver's write-side scratch, retained across passes and reused so the steady state
/// allocates nothing. Held by [`run`] and handed to [`flush`] as one value; each field is
/// used by the strategy its comment names, and untouched by the others.
pub(crate) struct WriteBuffers {
    /// The vectored path's accumulation buffer: runs of small blocks land here, cleared
    /// rather than reallocated each pass.
    gathered: BytesMut,
    /// The owned path's coalescing buffer. It reaches the transport as an owned `Bytes`,
    /// which would ordinarily consume the allocation; `flush` hands it over with
    /// `split().freeze()` so `bytes` reclaims the capacity once the transport drops its
    /// handle. Writing `freeze()` in its place would silently restore a per-pass allocation.
    coalesced: BytesMut,
    /// The no-copy record sink. `send_into` appends the header/payload records the send
    /// callback deposited, and the flush drains it — in order, ahead of each call's block —
    /// after every call. Empty on a push-model connection.
    send_records: Vec<SendRecord>,
    /// The vectored path's lifetime-free descriptor list (design decision D9). It survives
    /// across `send_into` calls even though each invalidates the last block.
    regions: Vec<Region>,
    /// The owned-region path's coalescing-and-header buffer. Runs of session blocks — all of
    /// them, since that path has no size threshold — and each minted nine-octet frame header are split-frozen out of it into owned `Bytes` at
    /// every region boundary; `bytes` reclaims its capacity across passes exactly as
    /// `coalesced`'s is reclaimed, since [`RegionWrite::write_regions`] returns the list
    /// and the transport drops the frozen slices.
    minted: BytesMut,
    /// The owned-region path's region list, handed to [`RegionWrite::write_regions`] and
    /// taken back for reuse.
    owned: Vec<Bytes>,
}

impl WriteBuffers {
    fn new() -> Self {
        Self {
            gathered: BytesMut::new(),
            coalesced: BytesMut::new(),
            send_records: Vec::new(),
            regions: Vec::new(),
            minted: BytesMut::new(),
            owned: Vec::new(),
        }
    }
}

/// The retained fast-path state a gathering flush works over: the accumulation buffer for
/// runs of small blocks, the no-copy record sink, the descriptor list, and the two cursors
/// that track how much of each the region list has already named. Bundled into one value so
/// the flush helpers take a single handle rather than a fistful of buffers — and so the
/// cursor reset that every flush shares lives in one place, [`flush_regions`], rather than at
/// each of its call sites.
struct Gather<'a> {
    /// The vectored path's accumulation buffer. Runs of small blocks land here and become
    /// one `Region::Gathered` apiece.
    gathered: &'a mut BytesMut,
    /// The no-copy record sink. Each entry is offered as a header region plus a payload
    /// region, the payload in the caller's own memory.
    send_records: &'a mut Vec<SendRecord>,
    /// The lifetime-free descriptor list of design decision D9.
    regions: &'a mut Vec<Region>,
    /// The start of the not-yet-closed run of `gathered`: octets appended since the last
    /// `Region::Gathered` was cut, waiting to become one region.
    run_start: usize,
    /// How many records in the sink the region list already names. New records a `send_into`
    /// call deposits sit at `send_records[regioned..]`, waiting to be turned into regions.
    regioned: usize,
}

/// Writes out everything the session currently has to say.
///
/// A thin dispatcher over two independent choices. The writer's
/// [`Model`](TransportWrite::Model) resolves, through [`Drains`], to *which family* of drain
/// runs — borrowed regions or owned ones — and that is settled at compile time. `vectored`
/// chooses *within* the family, and is the writer's own answer to
/// [`TransportWrite::is_write_vectored`], resolved once when the driver split the transport
/// and unchanged for the connection's life. The writer is not re-probed here, and there is no
/// precedence to arbitrate: the two choices are orthogonal.
///
/// This does not commit the octets to the peer; [`TransportWrite::commit`] does, and the
/// caller runs it once the pass is fully drained.
async fn flush<W: TransportWrite>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    buffers: &mut WriteBuffers,
    vectored: bool,
) -> Result<()> {
    <W::Model as Drains<W>>::drain(
        writer,
        vectored,
        Pass {
            inner: PassInner {
                session,
                events,
                buffers,
            },
        },
    )
    .await
}

/// The driver-side half of [`Pass`]: the session and the write scratch a drain touches, held
/// as disjoint `&mut` fields.
///
/// Named in [`Pass`]'s definition in the transport module and constructed only here. The
/// fields are reached as paths (`pass.inner.session`) and never through accessors, so a drain
/// loop keeps its disjoint borrows of the session and of the several buffers at once — an
/// accessor would collapse them into one borrow and the loop would stop compiling.
pub(crate) struct PassInner<'a> {
    /// The session, which every drain serialises from.
    pub(crate) session: &'a mut Session<Events>,
    /// The event scratch [`Session::send_into`] needs.
    pub(crate) events: &'a mut Events,
    /// The retained write buffers; each strategy uses the subset it needs.
    pub(crate) buffers: &'a mut WriteBuffers,
}

impl<W: BorrowedWrite + ?Sized> Drains<W> for Readiness
where
    W::Model: ReadinessModel,
{
    fn drain<'a>(
        writer: &'a mut W,
        vectored: bool,
        pass: Pass<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        let PassInner {
            session,
            events,
            buffers,
        } = pass.inner;
        async move {
            if vectored {
                // The writer declared its `write_vectored` reaches a real scatter-gather
                // call, so hand it the region list: small blocks already collapsed into one
                // accumulation run, large blocks and handed-over payloads riding uncopied.
                flush_readiness(session, writer, events, buffers).await
            } else {
                // It declared otherwise, so its `write_vectored` is the provided emulation —
                // a loop of one borrowed write per region. Coalescing pays a copy of every
                // octet to make the pass one region, and one write, instead.
                flush_coalesced_borrowed(session, writer, events, buffers).await
            }
        }
    }
}

impl<W: RegionWrite + ?Sized> Drains<W> for Completion
where
    W::Model: CompletionModel,
{
    fn drain<'a>(
        writer: &'a mut W,
        vectored: bool,
        pass: Pass<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        let PassInner {
            session,
            events,
            buffers,
        } = pass.inner;
        async move {
            if vectored {
                // A native region submission: one owned vectored write for the whole pass.
                flush_owned(
                    session,
                    writer,
                    events,
                    &mut buffers.minted,
                    &mut buffers.owned,
                    &mut buffers.send_records,
                )
                .await
            } else {
                // `write_regions` is the provided emulation, one owned write per region.
                // Coalescing mints a single owned buffer for the pass instead.
                flush_coalesced_owned(session, writer, events, buffers).await
            }
        }
    }
}

/// Accumulates a whole pass into one driver-owned buffer, ready to be written.
///
/// The shared half of the [`Coalesced`] strategy, which both models take: records the send
/// callback deposited are coalesced into `out` ahead of each call's block, so that when this
/// returns `out` holds every octet of the pass in wire order. Handing `out` to the transport
/// is the half that differs, because *how* a buffer reaches a transport is exactly what the
/// I/O model settles — see [`flush_coalesced_borrowed`] and [`flush_coalesced_owned`].
///
/// `out` (the `coalesced` buffer) is reused across passes, so the steady state allocates
/// nothing here.
fn accumulate_coalesced(
    session: &mut Session<Events>,
    events: &mut Events,
    send_records: &mut Vec<SendRecord>,
    out: &mut BytesMut,
) -> Result<()> {
    // Whatever a previous pass left has already been written; the clear keeps the capacity.
    // `send_records` is drained to empty every pass, so it already starts a pass empty.
    out.clear();

    loop {
        let block = match session.send_into(events, send_records) {
            Ok(block) => block,
            Err(error) => {
                // `send_into` appends whatever the send callback recorded *before* it reports
                // a failure, so records can be sitting in the sink on this path. Dropping them
                // keeps the drain-to-empty invariant of design decision D3 true on the error
                // exit: releasing this crate's handle to the caller's `Bytes` is what a
                // torn-down connection should do.
                send_records.clear();
                return Err(error.into());
            }
        };

        // Records deposited during this call belong on the wire before the block it returns
        // and after everything earlier calls produced. Drained after every call — the final
        // one returning `None` included — because libnghttp2's no-copy branch can deposit a
        // record without contributing octets to the returned block.
        for record in send_records.drain(..) {
            out.extend_from_slice(&record.header);
            out.extend_from_slice(&record.payload);
        }

        let Some(block) = block else { break };
        out.extend_from_slice(block);
    }

    debug_assert!(
        send_records.is_empty(),
        "accumulate_coalesced left records in the sink; they would outlive the pass and be lost"
    );
    Ok(())
}

/// Drains a pass on the [`Coalesced`] strategy over the readiness model.
///
/// Coalesce into the driver's own buffer, then lend it. Nothing is transferred: the driver
/// already owns every octet, and a readiness transport only ever borrows, so the buffer goes
/// out as a slice and the driver keeps it.
///
/// This used to hand over an owned `Bytes` split off the same buffer, because the owned write
/// was the one primitive both models shared and this drain serves both. That transfer was
/// manufactured for a recipient that immediately took a reference to what it had just been
/// given — `TokioWriter::write` took `buf` and called `self.half.write(&buf)` — and it cost
/// two atomics a pass to set up and tear down the shared-buffer bookkeeping. Splitting the
/// primitive by model removed the recipient, so it removed the transfer.
///
/// There is no empty-buffer guard here, unlike the owned drain below. A length-bounded loop
/// over an empty slice is already a no-op, and lending costs nothing to set up; the guard on
/// the owned path exists to dodge atomics that only ownership transfer incurs.
async fn flush_coalesced_borrowed<W: BorrowedWrite + ?Sized>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    buffers: &mut WriteBuffers,
) -> Result<()>
where
    W::Model: ReadinessModel,
{
    let out = &mut buffers.coalesced;
    accumulate_coalesced(session, events, &mut buffers.send_records, out)?;

    let mut offset = 0;
    while offset < out.len() {
        let written = writer.write_borrowed(&out[offset..]).await?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::Transport,
                "the transport accepted no octets and reported no error",
            ));
        }
        offset += written;
    }
    Ok(())
}

/// Drains a pass on the [`Coalesced`] strategy over the completion model.
///
/// Coalesce into the driver's own buffer, then hand it over. The transfer is real here: a
/// completion transport keeps the buffer until the operation finishes, so it must own one.
///
/// `split` rather than `freeze`: freezing `out` itself would give the allocation away and
/// leave the next pass to build a new one. Splitting hands over the octets while leaving the
/// allocation here, and `bytes` returns the capacity once the transport has dropped its
/// handle, so the steady state allocates nothing. The empty case is taken first so a pass
/// with nothing to say costs no atomics on `out` — a guard that matters only on this path,
/// because only this path transfers ownership.
async fn flush_coalesced_owned<W: RegionWrite + ?Sized>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    buffers: &mut WriteBuffers,
) -> Result<()>
where
    W::Model: CompletionModel,
{
    let out = &mut buffers.coalesced;
    accumulate_coalesced(session, events, &mut buffers.send_records, out)?;

    let mut pending = if out.is_empty() {
        Bytes::new()
    } else {
        out.split().freeze()
    };
    while !pending.is_empty() {
        let (result, returned) = writer.write_owned(pending).await;
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

/// Drains a pass on the readiness model when the writer declared efficient gathering.
///
/// There is one loop and one shape. Earlier revisions had two, because a transport could
/// decline to gather; it cannot any more — [`BorrowedWrite::write_vectored`] is always
/// available, natively or by the provided default — so the branch and the marker type that
/// selected it are gone.
///
/// Blocks below [`VECTORED_THRESHOLD`] accumulate into `gathered`, a
/// driver-owned buffer reused across passes; a block at or above it is handed to the socket
/// beside whatever has accumulated, in one gathering call, uncopied. A handed-over payload
/// does not force a copy either: its frame header and its octets ride as their own regions of
/// the same gathering write, so a run of small blocks, a record and a large block leave in one
/// `writev` with nothing copied, and a pass of only small blocks still costs one write.
///
/// Several session blocks cannot be gathered *with each other*, because the session
/// invalidates each block when the next is asked for and [`Session::send`] enforces that by
/// borrowing the session for as long as the block lives. That constraint is why the region
/// list holds lifetime-free [`Region`] descriptors rather than borrowed slices, and why a
/// large session block is written the instant it arrives — riding as the trailing region of a
/// gathering write over the descriptors accumulated so far — never stored (design decision
/// D9). A pass carrying no records offers at most two regions, one accumulated run beside one
/// live block; only records grow the list, up to [`MAX_REGIONS`].
///
/// This does not commit the octets to the peer; [`TransportWrite::commit`] does, and the
/// caller runs it once the pass is fully drained.
async fn flush_readiness<W: BorrowedWrite + ?Sized>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    buffers: &mut WriteBuffers,
) -> Result<()>
where
    W::Model: ReadinessModel,
{
    // Disjoint borrows of the buffers the readiness strategies use. `coalesced`, `minted` and
    // `owned` belong to the other strategies and are untouched here.
    let gathered = &mut buffers.gathered;
    let send_records = &mut buffers.send_records;
    let regions = &mut buffers.regions;
    // Whatever a previous pass left behind has already been written; starting from empty means
    // no error path can leak a remainder into the next pass. Every buffer keeps its capacity
    // across the clear, which is what makes the steady state free of allocation.
    gathered.clear();
    regions.clear();
    // The retained buffers gathered into one handle alongside the two cursors that start a
    // pass at zero. `send_records` is not cleared here: it is drained to empty at every flush
    // and on the error exit, so it already starts a pass empty.
    let mut g = Gather {
        gathered,
        send_records,
        regions,
        run_start: 0,
        regioned: 0,
    };

    loop {
        let block = match session.send_into(events, g.send_records) {
            Ok(block) => block,
            Err(error) => {
                // `send_into` appends whatever the send callback recorded *before* it reports
                // a failure, so records can be sitting in the sink on this path. Dropping them
                // — and the descriptors that name them — here keeps the drain-to-empty
                // invariant true on the error exit: the payloads they hold are the caller's
                // own `Bytes`, and releasing this crate's handle to them is what a torn-down
                // connection should do (design decision D3).
                g.send_records.clear();
                g.regions.clear();
                return Err(error.into());
            }
        };

        // Records the send callback deposited during this call belong on the wire *before* the
        // block the call returns, and after everything earlier calls produced. They become
        // regions of a gathering write, offered in the caller's own memory and never copied.
        //
        // Processing after *every* call — the final one that returns `None` included — is
        // required: libnghttp2's no-copy branch can deposit a record without contributing any
        // octets to the returned block, so a record can ride along with a `None`.
        if g.send_records.len() > g.regioned {
            // Close the open run of small blocks so the records that follow are ordered after
            // it. The invariant `regions.len() + open_run <= MAX_REGIONS` keeps a slot free for
            // this cut, so it never breaches the array; the guard is a defensive backstop that
            // flushes first should that invariant ever fail to hold, and flushing writes and
            // closes the run itself.
            if g.gathered.len() > g.run_start && g.regions.len() >= MAX_REGIONS {
                flush_regions(writer, &mut g, &[]).await?;
            }
            if g.gathered.len() > g.run_start {
                g.regions.push(Region::Gathered {
                    start: g.run_start,
                    end: g.gathered.len(),
                });
                g.run_start = g.gathered.len();
            }
            // Each record becomes a header region and a payload region. The list is flushed
            // whenever another pair would carry it past `MAX_REGIONS`, so a single call framing
            // more `DATA` than the cap admits still writes within the bound rather than
            // overrunning the materialisation array.
            while g.regioned < g.send_records.len() {
                if g.regions.len() + 2 > MAX_REGIONS {
                    flush_regions(writer, &mut g, &[]).await?;
                }
                g.regions.push(Region::Header(g.regioned));
                g.regions.push(Region::Payload(g.regioned));
                g.regioned += 1;
            }
        }

        let Some(block) = block else { break };

        if block.len() < VECTORED_THRESHOLD {
            // A small block extends the open run. If it would open a *fresh* run while the
            // list is already full, the run has no slot to close into, so flush first —
            // the one case the `regions.len() + open_run` invariant cannot absorb, since
            // opening a run raises `open_run` from zero to one.
            if g.gathered.len() == g.run_start && g.regions.len() >= MAX_REGIONS {
                flush_regions(writer, &mut g, &[]).await?;
            }
            g.gathered.extend_from_slice(block);
            continue;
        }
        // Big enough to be worth a syscall of its own, so it goes out uncopied, as the
        // trailing region of a gathering write over everything accumulated before it.
        flush_regions(writer, &mut g, block).await?;
    }

    // The tail of the list: an accumulated run with no large block behind it (the common
    // multiplexed pass), or records with no trailing block. One gathering write for the lot.
    if !g.regions.is_empty() || g.gathered.len() > g.run_start {
        flush_regions(writer, &mut g, &[]).await?;
    }

    // The sink and the region list were drained at every flush above — the final one included
    // — so both are empty on the success path exactly as on the error path that returns early.
    // This is the drain-to-empty invariant of D3, which is what makes it safe that
    // libnghttp2's `want_write` knows nothing about the records: a record, and the descriptor
    // that names it, can never outlive the pass that produced it.
    debug_assert!(
        g.send_records.is_empty(),
        "flush left records in the sink; they would outlive the pass and be lost"
    );
    debug_assert!(
        g.regions.is_empty(),
        "flush left descriptors in the region list; they would name a stale pass"
    );
    Ok(())
}

/// Writes the accumulated region list, optionally with a live session block as its trailing
/// region, then clears the octets it named.
///
/// This is the one place the lifetime-free descriptors of [`Region`] become slices: every
/// borrow it takes — of `gathered`, of the record sink, of `tail` — is shared, and nothing
/// is mutated until the write has returned, which is exactly what design decision D9 needs to
/// compile. The slices are materialised into a stack array, so no heap traffic is added and
/// `http_zero_alloc.rs` is not tripped.
///
/// The write is always the gathering one. Whether it reaches a real `writev` or the loop in
/// [`BorrowedWrite::write_vectored`]'s provided default is the transport's business, invisible
/// here and identical in the octets it produces.
///
/// `tail` is the live session block to write after the list, or empty for a flush that has no
/// block behind it. `g.regioned` is how many records the list names, drained on success so
/// their payload handles are released the moment they reach the transport; the two cursors are
/// reset here so every call site starts the next run and batch from zero. A transport failure
/// disposes of the *whole* sink instead and returns the error, so the drain-to-empty invariant
/// holds on that exit too.
async fn flush_regions<W: BorrowedWrite + ?Sized>(
    writer: &mut W,
    g: &mut Gather<'_>,
    tail: &[u8],
) -> Result<()>
where
    W::Model: ReadinessModel,
{
    // Close the open run of small blocks, if any accumulated since the last cut, so it is
    // ordered ahead of `tail`. Done here rather than at the call sites so every flush path
    // shares one closing rule.
    if g.gathered.len() > g.run_start {
        g.regions.push(Region::Gathered {
            start: g.run_start,
            end: g.gathered.len(),
        });
    }

    // The materialisation array of D9. `MAX_REGIONS + 1` entries: the list is capped at
    // `MAX_REGIONS`, and one live block may ride as the trailing region. `IoSlice` is `Copy`,
    // so the array initialises from an empty slice and is filled in place.
    let outcome = {
        let mut slots = [std::io::IoSlice::new(&[]); MAX_REGIONS + 1];
        let count = materialise(g.gathered, g.send_records, g.regions, tail, &mut slots);
        // Deliberately not `?`: a failure here must still fall through to the disposal below,
        // so the write result is carried out of the borrowing scope rather than propagated
        // from inside it.
        write_gathering(writer, &slots[..count]).await
    };

    if let Err(error) = outcome {
        // A transport failure tears the connection down, so no later pass will consume what
        // the sink holds. Everything goes — the records this write named *and* any later batch
        // queued behind them — rather than the `regioned` prefix the success path disposes of,
        // because there is no longer a "rest of the pass" for the remainder to belong to.
        // Releasing this crate's handle to the caller's `Bytes` here is what design decision
        // D3's drain-to-empty invariant asks for on the failure path, and it is what stops a
        // retained sink from re-emitting a broken pass's frames should the driver be polled
        // again.
        g.gathered.clear();
        g.send_records.clear();
        g.regions.clear();
        g.run_start = 0;
        g.regioned = 0;
        return Err(error);
    }

    // Everything the list named has been written. Clearing the run buffer and the list,
    // releasing exactly the records the list named, and resetting the cursors is the disposal
    // the D3 drain-to-empty invariant requires; the records beyond `regioned` are a later
    // batch and stay put.
    g.gathered.clear();
    g.send_records.drain(0..g.regioned);
    g.regions.clear();
    g.run_start = 0;
    g.regioned = 0;
    Ok(())
}

/// Fills `slots` with the slices the region list names, followed by `tail` if it is not
/// empty, and returns how many entries it wrote.
///
/// Zero-length regions are skipped, never offered: a zero-length final `DATA` frame
/// contributes a header region but an empty payload, and the transport contract promises no
/// empty region ever reaches it. A header is always nine octets and an accumulated run is
/// never empty by construction, so only a payload can be skipped.
fn materialise<'a>(
    gathered: &'a [u8],
    send_records: &'a [SendRecord],
    regions: &[Region],
    tail: &'a [u8],
    slots: &mut [std::io::IoSlice<'a>],
) -> usize {
    // The caller sizes `slots` at `MAX_REGIONS + 1` and the driver holds
    // `regions.len() + open_run <= MAX_REGIONS`, so the writes below are always in bounds.
    // That invariant lives in the caller, though, and this function is where it would be
    // violated — a `slots[count]` overrun is a panic in the middle of a write, with a
    // half-materialised list. Assert it here, where the bound is used, so a future change to
    // the cap or to the pre-flush arithmetic fails loudly in test builds rather than
    // depending on a reader connecting two distant pieces of code.
    debug_assert!(
        regions.len() + usize::from(!tail.is_empty()) <= slots.len(),
        "materialisation array too small: {} regions plus {} tail into {} slots",
        regions.len(),
        usize::from(!tail.is_empty()),
        slots.len(),
    );
    let mut count = 0;
    for region in regions {
        let slice: &[u8] = match *region {
            Region::Gathered { start, end } => &gathered[start..end],
            Region::Header(index) => &send_records[index].header,
            Region::Payload(index) => &send_records[index].payload,
        };
        if slice.is_empty() {
            continue;
        }
        slots[count] = std::io::IoSlice::new(slice);
        count += 1;
    }
    if !tail.is_empty() {
        slots[count] = std::io::IoSlice::new(tail);
        count += 1;
    }
    count
}

/// Writes `slices` as one gathering operation through `V`, retrying the remainder on a short
/// write.
///
/// The regions are recomputed each time round rather than advanced in place, which is what
/// lets a partial write resume cleanly: the already-written prefix is skipped and the first
/// region it landed inside is sliced from where it stopped. The case worth naming is a prefix
/// landing *exactly* on a region boundary — the finished region is then dropped entirely
/// rather than offered as a now-empty `IoSlice`, which the contract promises never to do and
/// which some transports would count as a region for nothing. `slices` carries no empty entry
/// to begin with, since [`materialise`] skipped them.
///
/// This loop is the *only* short-write authority on the readiness path. The emulating default
/// behind [`BorrowedWrite::write_vectored`] deliberately does not retry: it stops at the first
/// short region and reports the running total, and this rebuilds the offer and calls it again.
/// One authority rather than two nested ones is what keeps the accounting checkable — and it
/// is why a transport that moves only the first region of every offer still delivers every
/// octet, in order, at the cost of one call per region.
async fn write_gathering<W: BorrowedWrite + ?Sized>(
    writer: &mut W,
    slices: &[std::io::IoSlice<'_>],
) -> Result<()>
where
    W::Model: ReadinessModel,
{
    let total: usize = slices.iter().map(|slice| slice.len()).sum();
    let mut done = 0;
    while done < total {
        // Rebuild the offer from the octets still outstanding. Fresh each round on the stack,
        // so nothing is retained and nothing allocated.
        let mut offer = [std::io::IoSlice::new(&[]); MAX_REGIONS + 1];
        let mut count = 0;
        let mut skip = done;
        for slice in slices {
            let len = slice.len();
            if skip >= len {
                skip -= len;
                continue;
            }
            offer[count] = std::io::IoSlice::new(&slice[skip..]);
            skip = 0;
            count += 1;
        }
        let written = writer.write_vectored(&offer[..count]).await?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::Transport,
                "the transport accepted no octets and reported no error",
            ));
        }
        done += written;
    }
    Ok(())
}

/// Drains a pass on the owned-region strategy: the completion-transport counterpart of
/// [`flush`]'s vectored path.
///
/// A completion transport owns its buffers, so unlike the vectored path this cannot hand the
/// socket a borrowed session block: everything it offers must be owned. It builds `owned`, a
/// list of [`Bytes`], where every session block is coalesced into `minted` and
/// split-frozen at each region boundary — there is no size threshold, unlike the vectored
/// path, because a borrowed block cannot be owned without a copy — each frame header is
/// minted into `minted` and frozen
/// the same way, and each handed-over payload rides as its own region in the caller's own
/// memory, uncopied. The whole list goes to [`RegionWrite::write_regions`], which reaches
/// a single `writev`.
///
/// The payload is never copied; the session's own small blocks are, exactly as the vectored
/// path copies them into its accumulation buffer, because a borrow of the session cannot be
/// owned without one. Both `minted` and `owned` are retained across passes: `write_regions`
/// returns the `Vec` and the transport drops the frozen slices, so `bytes` reclaims `minted`'s
/// capacity and the steady state allocates nothing.
///
/// Ordering matches [`flush`]: records the send callback deposited during a `send_into` call
/// belong on the wire before that call's returned block and after everything earlier calls
/// produced, so the open run of coalesced blocks is closed ahead of the records that follow
/// it. [`MAX_REGIONS`] bounds the list exactly as it bounds the vectored one — the list is
/// flushed and restarted before it would exceed the cap — since [`crate::http`]'s region
/// bound is transport-independent.
async fn flush_owned<W: RegionWrite + ?Sized>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
    minted: &mut BytesMut,
    owned: &mut Vec<Bytes>,
    send_records: &mut Vec<SendRecord>,
) -> Result<()>
where
    W::Model: CompletionModel,
{
    // Whatever a previous pass left has already been written and dropped; both buffers keep
    // their capacity across the clear. `send_records` is drained to empty at every write and
    // on the error exit, so it already starts empty.
    minted.clear();
    owned.clear();
    // How many leading records in the sink `owned` already names as regions. Records a
    // `send_into` call deposits sit past this; a write drains the prefix and resets it.
    let mut regioned = 0usize;

    loop {
        let block = match session.send_into(events, send_records) {
            Ok(block) => block,
            Err(error) => {
                // `send_into` appends whatever the send callback recorded before it reports a
                // failure, so records can be sitting in the sink here. Dropping them keeps the
                // drain-to-empty invariant of design decision D3 true on the error exit:
                // releasing this crate's handle to the caller's `Bytes` is exactly what a
                // torn-down connection should do.
                send_records.clear();
                owned.clear();
                return Err(error.into());
            }
        };

        // Records deposited during this call go on the wire before the block it returns, and
        // after everything earlier calls produced. Processed after every call — the final one
        // returning `None` included — because libnghttp2's no-copy branch can deposit a record
        // without contributing octets to the returned block, so a record can ride with a `None`.
        if send_records.len() > regioned {
            // Close the open run of coalesced blocks so the records that follow are ordered
            // after it. If the list is already at the cap there is no slot for the run region,
            // so write first; a write empties the list and drains the records it named.
            if !minted.is_empty() {
                if owned.len() >= MAX_REGIONS {
                    write_owned(writer, owned, send_records, &mut regioned).await?;
                }
                owned.push(minted.split().freeze());
            }
            while regioned < send_records.len() {
                // Each record becomes a header region and, unless it is empty, a payload
                // region. Write first whenever the pair would carry the list past the cap, so
                // a single call framing more `DATA` than the cap admits still writes within
                // the bound. Two slots are reserved even for an empty payload: the check is on
                // the worst case, which keeps it a single rule.
                if owned.len() + 2 > MAX_REGIONS {
                    write_owned(writer, owned, send_records, &mut regioned).await?;
                }
                // `header` is a `[u8; 9]` (`Copy`), taken by value so no borrow of the sink is
                // held across the `minted` mutation; `payload` is a refcount bump, not a copy.
                let header = send_records[regioned].header;
                let payload = send_records[regioned].payload.clone();
                minted.extend_from_slice(&header);
                owned.push(minted.split().freeze());
                // A zero-length final `DATA` frame contributes a header but an empty payload,
                // and the contract promises no empty region ever reaches the transport.
                if !payload.is_empty() {
                    owned.push(payload);
                }
                regioned += 1;
            }
        }

        let Some(block) = block else { break };

        // Coalesce the block into the open run. It cannot ride uncopied as the vectored path's
        // large blocks do — the transport owns what it is handed — so there is no
        // `VECTORED_THRESHOLD` split here: every block, large or small, joins `minted`. If the
        // list is at the cap and no run is open, opening one would leave its eventual region no
        // slot, so write first.
        if minted.is_empty() && owned.len() >= MAX_REGIONS {
            write_owned(writer, owned, send_records, &mut regioned).await?;
        }
        minted.extend_from_slice(block);
    }

    // The tail: a final open run with no records behind it (the common case), or records with
    // no trailing block. One gathering write for the lot. The run-close guards the cap exactly
    // as the mid-pass one does — an earlier record batch may have filled the list to the cap
    // while a later small block left a run still open.
    if !minted.is_empty() {
        if owned.len() >= MAX_REGIONS {
            write_owned(writer, owned, send_records, &mut regioned).await?;
        }
        owned.push(minted.split().freeze());
    }
    if !owned.is_empty() {
        write_owned(writer, owned, send_records, &mut regioned).await?;
    }

    // The sink was drained at every write above, so it is empty on the success path exactly as
    // on the error path — the drain-to-empty invariant of design decision D3, which is what
    // makes it safe that libnghttp2's `want_write` knows nothing about the records.
    debug_assert!(
        send_records.is_empty(),
        "flush_owned left records in the sink; they would outlive the pass and be lost"
    );
    debug_assert!(
        owned.is_empty(),
        "flush_owned left regions in the list; they would name a stale pass"
    );
    Ok(())
}

/// Hands `owned` to [`RegionWrite::write_regions`] as one gathering write, retrying the
/// remainder on a short write, then disposes of exactly the records it named.
///
/// The list is moved in and taken back out — the `Vec` returns so its allocation is reused,
/// and the buffer is restored to `*owned` on every exit — which is the ownership round-trip a
/// completion API needs. A short write is resumed without a copy: the fully written regions
/// are dropped from the front and the first partial one is [`Bytes::advance`]d, both free
/// since [`Bytes`] is a view, and the remainder is offered again. An accepted write of zero
/// octets is an error, exactly as on every other strategy.
///
/// On success the `regioned` leading records are drained from the sink — releasing this
/// crate's handle to the caller's payload `Bytes` the moment they reach the transport — and
/// the cursor is reset. A transport failure disposes of the *whole* sink instead: the
/// connection is torn down, so no later pass will consume what remains, and design decision
/// D3's drain-to-empty invariant asks for the handles to be released here.
async fn write_owned<W: RegionWrite + ?Sized>(
    writer: &mut W,
    owned: &mut Vec<Bytes>,
    send_records: &mut Vec<SendRecord>,
    regioned: &mut usize,
) -> Result<()>
where
    W::Model: CompletionModel,
{
    // Move the list out for the by-value write. The emptied handle keeps its capacity, and it
    // — or the list `write_regions` returns — is restored to `*owned` before every return.
    let mut regions = core::mem::take(owned);
    loop {
        let total: usize = regions.iter().map(Bytes::len).sum();
        let (result, returned) = writer.write_regions(regions).await;
        regions = returned;
        let written = match result {
            Ok(written) => written,
            Err(error) => {
                regions.clear();
                *owned = regions;
                send_records.clear();
                *regioned = 0;
                return Err(error.into());
            }
        };
        if written == 0 {
            regions.clear();
            *owned = regions;
            send_records.clear();
            *regioned = 0;
            return Err(Error::new(
                ErrorKind::Transport,
                "the transport accepted no octets and reported no error",
            ));
        }
        if written >= total {
            break;
        }
        // Drop the regions the write consumed whole and advance the first it landed inside.
        let mut consumed = written;
        let mut whole = 0;
        for region in &mut regions {
            if consumed >= region.len() {
                consumed -= region.len();
                whole += 1;
            } else {
                bytes::Buf::advance(region, consumed);
                break;
            }
        }
        regions.drain(0..whole);
    }
    regions.clear();
    *owned = regions;
    send_records.drain(0..*regioned);
    *regioned = 0;
    Ok(())
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

    /// Octets whose liveness is observable through a strong count: while any handle to the
    /// payload lives, the count is two — the test's `alive` and this owner's clone — and it
    /// falls to one the instant the last handle is dropped.
    struct Witness {
        data: Vec<u8>,
        _alive: std::sync::Arc<()>,
    }

    impl AsRef<[u8]> for Witness {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    /// Builds the sink and region list a gathering flush holds at the moment it hands a `DATA`
    /// frame to the transport: one record named by a header region and a payload region, and
    /// behind it a second record the list does *not* name.
    ///
    /// The queued successor is what makes the disposal check discriminating. On the success
    /// path [`flush_regions`] drains only the `regioned` prefix, because the records behind it
    /// are a later batch of the same pass and still have a write coming; on the failure path
    /// there is no later batch, so the whole sink must go. With a single record those two
    /// rules are indistinguishable — `drain(0..1)` and `clear()` do the same thing — and a
    /// regression that disposed of only the prefix would pass unnoticed. Only the whole-sink
    /// rule releases this successor.
    fn one_record_and_a_queued_successor(
        payload: Bytes,
        queued: Bytes,
    ) -> (Vec<SendRecord>, Vec<Region>) {
        (
            vec![
                SendRecord {
                    header: [0u8; 9],
                    payload,
                },
                SendRecord {
                    header: [1u8; 9],
                    payload: queued,
                },
            ],
            vec![Region::Header(0), Region::Payload(0)],
        )
    }

    /// Design decision D3's drain-to-empty invariant, on the failure exit of the fast paths.
    ///
    /// This is a direct check because it cannot be an indirect one. A fast-path write failure
    /// surfaces from [`flush_regions`] and tears the whole connection down, and teardown drops
    /// the driver's record sink regardless of whether [`flush_regions`] disposed of it first —
    /// so from outside the async API the two are indistinguishable, and a `strong_count`
    /// assertion at the end of a broken exchange (as the sibling integration tests make) holds
    /// whether or not the disposal ran. The property the fix restores is that the sink is
    /// *already* empty the moment the error leaves [`flush_regions`], which only a check on the
    /// sink itself, here, can witness. Put the early `?` back on the fast-path write and this
    /// is the test that fails.
    #[test]
    fn a_failing_vectored_write_drains_the_record_sink_before_returning() {
        use crate::http::testing::{block_on, failing_vectored};

        let alive = std::sync::Arc::new(());
        let witness = |fill: u8| {
            Bytes::from_owner(Witness {
                data: vec![fill; 4096],
                _alive: std::sync::Arc::clone(&alive),
            })
        };
        // Two payloads share one witness: the record the region list names, and a record
        // queued behind it that the list does not. Both must be released, which is what
        // separates whole-sink disposal from prefix disposal.
        let (payload, queued) = (witness(7), witness(8));
        assert_eq!(
            std::sync::Arc::strong_count(&alive),
            3,
            "the witness should see both payloads held before the flush"
        );

        let (mut send_records, mut regions) = one_record_and_a_queued_successor(payload, queued);
        let mut gathered = BytesMut::new();
        let mut g = Gather {
            gathered: &mut gathered,
            send_records: &mut send_records,
            regions: &mut regions,
            run_start: 0,
            regioned: 1,
        };

        // A transport that gathers natively and fails its first write, so the error arrives
        // from inside `flush_regions`' gathering write with the whole offer in one call.
        let (failing, _peer) = failing_vectored(1, false);
        let (_reader, mut writer) = failing.split();

        let outcome = block_on(flush_regions(&mut writer, &mut g, &[]));
        assert!(
            outcome.is_err(),
            "a failing gathering write must surface the transport error"
        );
        assert!(
            g.send_records.is_empty(),
            "flush_regions left a record in the sink after a failing vectored write"
        );
        assert!(
            g.regions.is_empty(),
            "flush_regions left descriptors in the list after a failing vectored write"
        );
        assert_eq!(
            std::sync::Arc::strong_count(&alive),
            1,
            "the failing write's disposal did not release every caller payload the sink held; \
             a queued record behind the flushed prefix was left holding one"
        );
    }

    /// The emulating twin of the check above: the same invariant when the gathering write is
    /// the provided default rather than a native `writev`, so the failure surfaces from inside
    /// the emulation loop and part-way through the offer. The record is named by two regions,
    /// and emulation writes one region per call, so failing the *second* call leaves the
    /// record whole in the sink at the moment the error appears — the case whole-sink disposal
    /// has to cover and prefix disposal would miss.
    #[test]
    fn a_failing_borrowed_write_drains_the_record_sink_before_returning() {
        use crate::http::testing::{block_on, failing_borrowed};

        let alive = std::sync::Arc::new(());
        let witness = |fill: u8| {
            Bytes::from_owner(Witness {
                data: vec![fill; 4096],
                _alive: std::sync::Arc::clone(&alive),
            })
        };
        let (payload, queued) = (witness(7), witness(8));
        assert_eq!(
            std::sync::Arc::strong_count(&alive),
            3,
            "both payloads held before the flush"
        );

        let (mut send_records, mut regions) = one_record_and_a_queued_successor(payload, queued);
        let mut gathered = BytesMut::new();
        let mut g = Gather {
            gathered: &mut gathered,
            send_records: &mut send_records,
            regions: &mut regions,
            run_start: 0,
            regioned: 1,
        };

        // Emulation writes the header region first and the payload region second, so the
        // second write is the one that fails — the record is still whole in the sink when it
        // does. This writer overrides nothing, so `write_vectored` is the provided default and
        // every region reaches the transport through `write_borrowed`.
        let (failing, _peer) = failing_borrowed(2, false);
        let (_reader, mut writer) = failing.split();

        let outcome = block_on(flush_regions(&mut writer, &mut g, &[]));
        assert!(
            outcome.is_err(),
            "a failing borrowed write inside the emulation loop must surface the transport \
             error"
        );
        assert!(
            g.send_records.is_empty(),
            "flush_regions left a record in the sink after a failing borrowed write"
        );
        assert!(
            g.regions.is_empty(),
            "flush_regions left descriptors in the list after a failing borrowed write"
        );
        assert_eq!(
            std::sync::Arc::strong_count(&alive),
            1,
            "the failing write's disposal did not release every caller payload the sink held; \
             a queued record behind the flushed prefix was left holding one"
        );
    }
}
