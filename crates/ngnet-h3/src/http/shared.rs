//! State shared between a connection's handles and its driver.
//!
//! A handle and a driver live on different tasks and neither owns the other. Everything they
//! say to each other passes through here: a request to submit, a stream to abandon, a body
//! that has more to give, a connection that has gone away.
//!
//! # Why one mutex rather than several
//!
//! Every field below is touched once per driver pass, together, under the same conditions.
//! Splitting them would mean several uncontended acquisitions where there is now one, and
//! would make the park decision — which reads all of them — a sequence of separate reads
//! that can each go stale before the next. One lock held briefly is both faster and easier
//! to reason about.
//!
//! The command queue is the deliberate exception. It names the caller's body type, and
//! putting it in the same structure would infect the waker — which every stream holds — with
//! that parameter for no benefit.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Waker;

use bytes::Bytes;

use super::error::{Error, ErrorKind};
use crate::error::ErrorCode;
use crate::stream::StreamId;

/// Acquires a lock, recovering from a poisoned one.
///
/// A panic while this lock was held cannot leave the protected state inconsistent: every
/// method below either completes its update or does nothing, and none can panic partway.
/// Propagating the poison instead would turn one task's panic into every task's panic.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Something the driver must ask the QUIC backend to do.
///
/// These come from nghttp3, which asks for a stream to be reset or for the peer to be told
/// to stop sending. They cannot be performed where they arise: a handler receives only the
/// context, and at that moment the backend is either mutably borrowed by the transmit path
/// or not in scope at all. So they are queued here and drained by the driver between calls.
///
/// This is a different direction from [`crate::http::QuicEvent::Reset`] and
/// [`crate::http::QuicEvent::StopSending`], which are the *peer* acting and are fed *into*
/// nghttp3. Conflating the two is the mistake to avoid: one is an instruction to the
/// transport, the other is news from it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TransportAction {
    /// Abandon what is left to send on this stream.
    Reset { stream: StreamId, code: ErrorCode },
    /// Ask the peer to stop sending on this stream.
    StopSending { stream: StreamId, code: ErrorCode },
}

/// The mutable state a handle and a driver share.
pub(crate) struct Shared {
    inner: Mutex<Inner>,
    /// Read on every park decision and written rarely, so it is worth keeping out of the
    /// lock: a handle asking "is this connection still alive" should not contend with the
    /// driver.
    gone: AtomicBool,
    refusing: AtomicBool,
}

#[derive(Default)]
struct Inner {
    /// The driver's waker, refreshed on every poll.
    driver: Option<Waker>,
    /// Streams whose outgoing body deferred and has since been woken.
    ready: Vec<StreamId>,
    /// Streams to abandon, with the code to abandon them with.
    resets: Vec<(StreamId, ErrorCode)>,
    /// Receive credit the caller has consumed and the peer may be given back.
    credit: HashMap<StreamId, u64>,
    /// Whether a caller has asked for a graceful shutdown.
    shutdown: bool,
    /// Transport actions nghttp3 has asked for.
    actions: Vec<TransportAction>,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            gone: AtomicBool::new(false),
            refusing: AtomicBool::new(false),
        }
    }

    /// Records where to wake the driver from.
    pub(crate) fn refresh_driver(&self, waker: &Waker) {
        let mut inner = lock(&self.inner);
        match &inner.driver {
            Some(existing) if existing.will_wake(waker) => {}
            _ => inner.driver = Some(waker.clone()),
        }
    }

    /// Wakes the driver, if it is parked.
    ///
    /// The waker is cloned out and woken outside the lock, so waking cannot re-enter a
    /// method that wants it.
    pub(crate) fn wake_driver(&self) {
        let waker = lock(&self.inner).driver.clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Notes that a stream's body has more to give.
    ///
    /// Returns whether the note was new, so a caller can skip the wake when it was not.
    pub(crate) fn mark_ready(&self, stream: StreamId) -> bool {
        let mut inner = lock(&self.inner);
        if inner.ready.contains(&stream) {
            return false;
        }
        inner.ready.push(stream);
        true
    }

    pub(crate) fn take_ready(&self, into: &mut Vec<StreamId>) {
        into.append(&mut lock(&self.inner).ready);
    }

    pub(crate) fn ready_len(&self) -> usize {
        lock(&self.inner).ready.len()
    }

    /// Asks for a stream to be abandoned.
    pub(crate) fn reset(&self, stream: StreamId, code: ErrorCode) {
        let mut inner = lock(&self.inner);
        if !inner.resets.iter().any(|(s, _)| *s == stream) {
            inner.resets.push((stream, code));
        }
    }

    pub(crate) fn take_resets(&self, into: &mut Vec<(StreamId, ErrorCode)>) {
        into.append(&mut lock(&self.inner).resets);
    }

    pub(crate) fn resets_pending(&self) -> bool {
        !lock(&self.inner).resets.is_empty()
    }

    /// Records receive credit a caller has consumed.
    pub(crate) fn credit(&self, stream: StreamId, bytes: u64) {
        *lock(&self.inner).credit.entry(stream).or_default() += bytes;
    }

    pub(crate) fn take_credit(&self, into: &mut Vec<(StreamId, u64)>) {
        into.extend(lock(&self.inner).credit.drain());
    }

    pub(crate) fn credit_pending(&self) -> bool {
        !lock(&self.inner).credit.is_empty()
    }

    /// Queues a transport action nghttp3 asked for.
    pub(crate) fn push_action(&self, action: TransportAction) {
        lock(&self.inner).actions.push(action);
    }

    pub(crate) fn take_actions(&self, into: &mut Vec<TransportAction>) {
        into.append(&mut lock(&self.inner).actions);
    }

    pub(crate) fn actions_pending(&self) -> bool {
        !lock(&self.inner).actions.is_empty()
    }

    /// Asks for a graceful shutdown.
    pub(crate) fn request_shutdown(&self) {
        lock(&self.inner).shutdown = true;
        self.refusing.store(true, Ordering::Release);
    }

    pub(crate) fn take_shutdown(&self) -> bool {
        core::mem::replace(&mut lock(&self.inner).shutdown, false)
    }

    pub(crate) fn shutdown_pending(&self) -> bool {
        lock(&self.inner).shutdown
    }

    /// Marks the connection as refusing new exchanges.
    pub(crate) fn set_refusing(&self) {
        self.refusing.store(true, Ordering::Release);
    }

    pub(crate) fn is_refusing(&self) -> bool {
        self.refusing.load(Ordering::Acquire)
    }

    /// Marks the connection as gone, so handles fail immediately rather than queueing.
    pub(crate) fn set_gone(&self) {
        self.gone.store(true, Ordering::Release);
        self.refusing.store(true, Ordering::Release);
    }

    pub(crate) fn is_gone(&self) -> bool {
        self.gone.load(Ordering::Acquire)
    }
}

/// A stream identifier no request can have, meaning "not submitted yet".
const UNBOUND: i64 = -1;

/// Where a response is delivered once it arrives.
///
/// One per submitted request. The future holds one end and the driver the other, and either
/// may reach it first — a response can arrive before the caller polls, and a caller can drop
/// the future before a response arrives.
pub(crate) struct Slot {
    state: Mutex<SlotState>,
    /// The stream this request was put on, once the driver has chosen one.
    ///
    /// Not known when the slot is created: a handle may submit from any task, and which
    /// stream identifier the request gets is the driver's decision, made later. The future
    /// needs it for two things it can only do afterwards — naming the body's stream so
    /// reading it returns credit to the right place, and abandoning the right exchange if
    /// the caller gives up.
    stream: AtomicI64,
}

#[derive(Default)]
struct SlotState {
    outcome: Option<Result<http::Response<()>, Error>>,
    waker: Option<Waker>,
    settled: bool,
}

impl Slot {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            stream: AtomicI64::new(UNBOUND),
        }
    }

    /// Records which stream the driver put this request on.
    pub(crate) fn bind(&self, stream: StreamId) {
        self.stream.store(stream.get(), Ordering::Release);
    }

    /// Which stream this request went out on, if it has gone out.
    pub(crate) fn stream(&self) -> Option<StreamId> {
        let raw = self.stream.load(Ordering::Acquire);
        if raw == UNBOUND {
            return None;
        }
        StreamId::new(raw).ok()
    }

    /// Delivers a response head.
    ///
    /// Ignored once the slot has settled: an informational response does not settle it, so a
    /// stream can legitimately produce a second head, but a *third* delivery would be the
    /// peer misbehaving and is dropped rather than trusted.
    pub(crate) fn complete(&self, response: http::Response<()>) {
        let mut state = lock(&self.state);
        if state.settled {
            return;
        }
        state.settled = true;
        state.outcome = Some(Ok(response));
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Fails the exchange.
    pub(crate) fn fail(&self, error: Error) {
        let mut state = lock(&self.state);
        if state.settled {
            return;
        }
        state.settled = true;
        state.outcome = Some(Err(error));
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Whether an outcome has been recorded.
    pub(crate) fn is_settled(&self) -> bool {
        lock(&self.state).settled
    }

    /// Takes the outcome, if there is one, recording where to wake if there is not.
    pub(crate) fn poll(&self, waker: &Waker) -> Option<Result<http::Response<()>, Error>> {
        let mut state = lock(&self.state);
        if let Some(outcome) = state.outcome.take() {
            return Some(outcome);
        }
        state.waker = Some(waker.clone());
        None
    }
}

/// The receiving side of one exchange.
///
/// Body chunks accumulate here as the driver reads them and are taken by whoever holds the
/// [`IncomingBody`](super::body::IncomingBody).
pub(crate) struct Incoming {
    state: Mutex<IncomingState>,
}

#[derive(Default)]
struct IncomingState {
    chunks: VecDeque<Bytes>,
    trailers: Option<http::HeaderMap>,
    finished: bool,
    failure: Option<Error>,
    waker: Option<Waker>,
    /// Set when the reader has gone away, so the driver stops accumulating.
    abandoned: bool,
}

/// What a reader found waiting.
pub(crate) enum Received {
    Data(Bytes),
    Trailers(http::HeaderMap),
    Finished,
    Failed(Error),
    Nothing,
}

impl Incoming {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(IncomingState::default()),
        }
    }

    /// Adds received body bytes.
    pub(crate) fn push(&self, chunk: Bytes) {
        let mut state = lock(&self.state);
        if state.abandoned || chunk.is_empty() {
            return;
        }
        state.chunks.push_back(chunk);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Records trailers, which are delivered after every body chunk.
    pub(crate) fn set_trailers(&self, trailers: http::HeaderMap) {
        let mut state = lock(&self.state);
        if state.abandoned {
            return;
        }
        state.trailers = Some(trailers);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Marks the message complete.
    pub(crate) fn finish(&self) {
        let mut state = lock(&self.state);
        state.finished = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Fails the message.
    pub(crate) fn fail(&self, error: Error) {
        let mut state = lock(&self.state);
        if state.failure.is_none() && !state.finished {
            state.failure = Some(error);
        }
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Whether every byte has arrived.
    pub(crate) fn is_finished(&self) -> bool {
        lock(&self.state).finished
    }

    /// Takes whatever is waiting, recording where to wake if nothing is.
    pub(crate) fn poll(&self, waker: &Waker) -> Received {
        let mut state = lock(&self.state);
        if let Some(chunk) = state.chunks.pop_front() {
            return Received::Data(chunk);
        }
        if let Some(trailers) = state.trailers.take() {
            return Received::Trailers(trailers);
        }
        if let Some(error) = state.failure.take() {
            return Received::Failed(error);
        }
        if state.finished {
            return Received::Finished;
        }
        state.waker = Some(waker.clone());
        Received::Nothing
    }

    /// Abandons the reading side, returning how many unread bytes were dropped.
    ///
    /// The count is credited back to the peer: it consumed flow control to send bytes
    /// nobody read, and withholding the credit would shrink the window for the rest of the
    /// connection's life.
    pub(crate) fn abandon(&self) -> u64 {
        let mut state = lock(&self.state);
        state.abandoned = true;
        let unread: u64 = state.chunks.iter().map(|chunk| chunk.len() as u64).sum();
        state.chunks.clear();
        unread
    }
}

/// One exchange's entry in the driver's registry.
pub(crate) struct Entry {
    /// Where the response goes. Absent on a server, which answers rather than waits.
    pub(crate) slot: Option<Arc<Slot>>,
    pub(crate) incoming: Arc<Incoming>,
    /// Dropped when the entry is removed, which is what makes a stale waker inert.
    ///
    /// Never read. Its whole job is to stop existing.
    pub(crate) _liveness: Arc<()>,
}

/// Every exchange in flight, by stream.
pub(crate) struct Registry {
    entries: Mutex<BTreeMap<StreamId, Entry>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn insert(&self, stream: StreamId, entry: Entry) {
        lock(&self.entries).insert(stream, entry);
    }

    pub(crate) fn slot(&self, stream: StreamId) -> Option<Arc<Slot>> {
        lock(&self.entries)
            .get(&stream)
            .and_then(|entry| entry.slot.clone())
    }

    pub(crate) fn incoming(&self, stream: StreamId) -> Option<Arc<Incoming>> {
        lock(&self.entries)
            .get(&stream)
            .map(|entry| Arc::clone(&entry.incoming))
    }

    /// Every stream currently in flight.
    pub(crate) fn streams(&self) -> Vec<StreamId> {
        lock(&self.entries).keys().copied().collect()
    }

    pub(crate) fn remove(&self, stream: StreamId) -> Option<Entry> {
        lock(&self.entries).remove(&stream)
    }

    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.entries).is_empty()
    }

    /// Empties the registry, for teardown.
    pub(crate) fn take_all(&self) -> Vec<Entry> {
        core::mem::take(&mut *lock(&self.entries))
            .into_values()
            .collect()
    }
}

/// A request a handle has submitted and the driver has not yet picked up.
pub(crate) struct Command<B> {
    pub(crate) request: http::Request<B>,
    pub(crate) slot: Arc<Slot>,
    pub(crate) incoming: Arc<Incoming>,
}

/// The queue between a handle and its driver.
///
/// Separate from [`Shared`] because it names the body type, and putting it there would
/// infect every waker with a parameter none of them needs.
pub(crate) struct Queue<B> {
    commands: Mutex<VecDeque<Command<B>>>,
}

impl<B> Queue<B> {
    pub(crate) fn new() -> Self {
        Self {
            commands: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn push(&self, command: Command<B>) {
        lock(&self.commands).push_back(command);
    }

    pub(crate) fn pop(&self) -> Option<Command<B>> {
        lock(&self.commands).pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.commands).is_empty()
    }

    /// Fails everything queued, for teardown.
    pub(crate) fn abandon(&self) {
        for command in lock(&self.commands).drain(..) {
            command.slot.fail(Error::new(
                ErrorKind::Closed,
                "the connection went away before this request was sent",
            ));
        }
    }
}
