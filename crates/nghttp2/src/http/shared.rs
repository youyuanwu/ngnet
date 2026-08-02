//! State that handles, bodies and the driver all reach.
//!
//! The session lives inside the driver and is deliberately not `Sync`, so nothing outside
//! the driver may touch it. Everything that wants the session's attention — a handle
//! submitting a request, a body announcing it has more to give — leaves a note here and
//! wakes the driver, which acts on it during its next pass.
//!
//! # Lock discipline
//!
//! Every lock in this module is a *leaf*: it is taken, a small amount of state is moved in
//! or out, and it is released before anything else is called. The driver never holds one
//! across a call into the session.
//!
//! That rule is load-bearing rather than tidy. A body may wake its own waker from inside
//! [`BodySource::fill`](crate::BodySource::fill), which runs re-entrantly inside
//! `Session::send` — so waking must never need a lock the driver is already holding.
//! Waking the driver itself is done *after* releasing the lock, for the same reason.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use bytes::Bytes;
use http_body::Frame;

use super::error::{Error, ErrorKind, Result};

/// The de-duplicating ready set, the driver's waker, and the connection's liveness.
///
/// Carries no type parameter, because [`super::waker::StreamWaker`] holds one and
/// [`std::task::Wake`] requires `Send + Sync`. Keeping the body type out of here means a
/// non-`Send` body makes the *handle* non-`Send` by inference without making the waker
/// impossible to write.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Streams whose bodies have announced they are ready to be asked again.
    ///
    /// A set rather than a queue: a stream woken five times before the driver next runs
    /// still needs resuming exactly once.
    ready: BTreeSet<i32>,
    /// The driver task's waker, replaced whenever the runtime hands over a different one.
    driver: Option<Waker>,
    /// Receive-window capacity the application has finished with, by stream, waiting to
    /// be handed back to the peer.
    ///
    /// Accumulated per stream rather than queued per chunk, so a caller reading a body in
    /// small pieces produces one `WINDOW_UPDATE` per driver pass instead of one per read.
    credits: BTreeMap<i32, usize>,
    /// Trailing header blocks an outgoing body produced, waiting to be submitted.
    ///
    /// Left here rather than submitted on the spot because they are produced from inside
    /// `Session::send`, where the session cannot be reached — and could not accept them
    /// anyway until the body that announced them has finished being serialised.
    trailers: Vec<(i32, http::HeaderMap)>,
    /// Streams to reset, with the code to reset them under.
    ///
    /// Written by a dropped request or a dropped response body, both of which are on the
    /// caller's task rather than the driver's, and neither of which can reach the session.
    resets: Vec<(i32, crate::ErrorCode)>,
    /// A graceful shutdown the caller asked for and the driver has not yet sent, as the
    /// last stream to honour and the code to give.
    shutdown: Option<(i32, crate::ErrorCode)>,
    /// Set once nothing new may be started: the caller asked to shut down, or the peer
    /// said it was going away.
    refusing: bool,
    /// The most octets any one outgoing body has ever held back at once, as chunks.
    ///
    /// The send path retains at most one unconsumed chunk per stream, and this is the
    /// hook that claim is asserted against rather than read off the source. Never reset:
    /// a high-water mark that could be cleared would not be evidence of anything.
    buffered_high_water: usize,
    /// Set once the driver is gone, so nothing waits for an answer that cannot come.
    gone: bool,
}

impl Shared {
    /// Records the waker the driver is currently being polled with.
    ///
    /// Called at the top of every driver poll. A waker captured once at submission would
    /// go stale the first time the runtime moved or re-polled the driver, so the slot is
    /// refreshed rather than filled — and [`Waker::will_wake`] keeps the common case, the
    /// same waker every time, from doing any work.
    pub(crate) fn refresh_driver(&self, waker: &Waker) {
        let mut inner = self.lock();
        match &inner.driver {
            Some(current) if current.will_wake(waker) => {}
            _ => inner.driver = Some(waker.clone()),
        }
    }

    /// Asks the driver to run another pass.
    pub(crate) fn wake_driver(&self) {
        // Taken out under the lock and woken outside it: a waker may do arbitrary work,
        // including re-entering this type, and must not do so while the lock is held.
        let waker = self.lock().driver.clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Notes that `stream`'s body is ready to be consulted again.
    ///
    /// Returns whether the note was kept. A wake whose stream is no longer live is
    /// discarded here, which is what bounds the set by the number of live streams rather
    /// than by the number of wakers that were ever handed out.
    pub(crate) fn mark_ready(&self, stream: i32, liveness: &Weak<()>) -> bool {
        if liveness.upgrade().is_none() {
            return false;
        }
        if stream <= 0 {
            // The stream identifier is assigned after submission; a waker fired before
            // then has nothing to name.
            return false;
        }
        self.lock().ready.insert(stream);
        true
    }

    /// Takes the streams to resume, leaving the set empty.
    pub(crate) fn take_ready(&self) -> Vec<i32> {
        let mut inner = self.lock();
        core::mem::take(&mut inner.ready).into_iter().collect()
    }

    /// How many streams are waiting to be resumed.
    pub(crate) fn ready_len(&self) -> usize {
        self.lock().ready.len()
    }

    /// Notes that `len` octets received on `stream` have been consumed by the application.
    ///
    /// Reporting consumption is what re-opens the receive window, so this is the whole of
    /// the crate's backpressure: a body nobody reads produces no credit, the window
    /// closes, and the peer stops. The driver hands the total to the session on its next
    /// pass — nothing here touches the session, which lives in the driver and is not
    /// shareable.
    pub(crate) fn credit(&self, stream: i32, len: usize) {
        if len == 0 {
            return;
        }
        *self.lock().credits.entry(stream).or_default() += len;
    }

    /// Takes the consumption to report, leaving nothing behind.
    pub(crate) fn take_credits(&self) -> Vec<(i32, usize)> {
        let mut inner = self.lock();
        core::mem::take(&mut inner.credits).into_iter().collect()
    }

    /// How many streams have consumption waiting to be reported.
    pub(crate) fn credits_len(&self) -> usize {
        self.lock().credits.len()
    }

    /// Records a trailing header block an outgoing body produced.
    ///
    /// Called from inside [`BodySource::fill`](crate::BodySource::fill), which runs
    /// re-entrantly inside `Session::send`. A leaf lock, taken and released without
    /// calling anything: the driver is inside the session at this moment and must not be
    /// made to wait on it.
    pub(crate) fn stash_trailers(&self, stream: i32, trailers: http::HeaderMap) {
        self.lock().trailers.push((stream, trailers));
    }

    /// Takes the trailing blocks to submit, leaving none behind.
    pub(crate) fn take_trailers(&self) -> Vec<(i32, http::HeaderMap)> {
        core::mem::take(&mut self.lock().trailers)
    }

    /// Whether any trailing block is waiting to be submitted.
    pub(crate) fn trailers_pending(&self) -> bool {
        !self.lock().trailers.is_empty()
    }

    /// Asks the driver to reset `stream`.
    ///
    /// The session lives in the driver, so a caller dropping a request cannot reset
    /// anything itself. Leaving a note and waking is the whole of it — and the note has to
    /// be honoured promptly, because until it is the peer is still sending a body nobody
    /// will read.
    pub(crate) fn reset(&self, stream: i32, code: crate::ErrorCode) {
        if stream <= 0 {
            return;
        }
        self.lock().resets.push((stream, code));
    }

    /// Takes the streams to reset, leaving none behind.
    pub(crate) fn take_resets(&self) -> Vec<(i32, crate::ErrorCode)> {
        core::mem::take(&mut self.lock().resets)
    }

    /// Whether any stream is waiting to be reset.
    pub(crate) fn resets_pending(&self) -> bool {
        !self.lock().resets.is_empty()
    }

    /// Asks the driver to stop accepting new exchanges, letting current ones finish.
    ///
    /// `last_stream` is what the `GOAWAY` will name, and it means the last stream *the
    /// peer opened* that this end will honour — not the last one this end opened. A client
    /// that accepts no pushed streams has opened nothing on the peer's behalf, so it says
    /// zero; naming one of its own requests is rejected outright by libnghttp2, which is
    /// the protocol saying the same thing.
    pub(crate) fn request_shutdown(&self, last_stream: i32, code: crate::ErrorCode) {
        let mut inner = self.lock();
        inner.refusing = true;
        inner.shutdown = Some((last_stream, code));
    }

    /// Takes the shutdown to send, if one was asked for and not yet sent.
    pub(crate) fn take_shutdown(&self) -> Option<(i32, crate::ErrorCode)> {
        self.lock().shutdown.take()
    }

    /// Whether a shutdown is waiting to be sent.
    pub(crate) fn shutdown_pending(&self) -> bool {
        self.lock().shutdown.is_some()
    }

    /// Records that nothing new may be started on this connection.
    ///
    /// Set by a caller's shutdown and by a peer's `GOAWAY` alike: from a handle's point of
    /// view the two are the same fact, and the difference is only in what the exchanges
    /// already in flight are told.
    pub(crate) fn set_refusing(&self) {
        self.lock().refusing = true;
    }

    /// Whether new exchanges are being refused.
    pub(crate) fn is_refusing(&self) -> bool {
        self.lock().refusing
    }

    /// Notes how many chunks an outgoing body is holding back.
    pub(crate) fn note_buffered(&self, chunks: usize) {
        let mut inner = self.lock();
        inner.buffered_high_water = inner.buffered_high_water.max(chunks);
    }

    /// The most chunks any outgoing body has held back at once.
    pub(crate) fn buffered_high_water(&self) -> usize {
        self.lock().buffered_high_water
    }

    /// Records that the driver has stopped, and wakes anything waiting on it.
    pub(crate) fn set_gone(&self) {
        self.lock().gone = true;
    }

    /// Whether the driver has stopped.
    pub(crate) fn is_gone(&self) -> bool {
        self.lock().gone
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock here would mean a panic escaped one of these very short
        // critical sections, none of which call out into caller code. Recovering the
        // guard is more useful than turning an unrelated panic into a second one.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Proof that at least one handle still exists.
///
/// The driver holds a [`Weak`] to this. When the strong count reaches zero no further
/// request can ever arrive, which is what lets an idle connection finish instead of
/// waiting forever — and dropping the last one wakes the driver so it notices.
#[derive(Debug)]
pub(crate) struct HandleToken {
    shared: Arc<Shared>,
}

impl HandleToken {
    pub(crate) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl Drop for HandleToken {
    fn drop(&mut self) {
        self.shared.wake_driver();
    }
}

/// Something a handle wants the driver to do with the session.
pub(crate) enum Command<B> {
    /// Submit this request and answer through the attached slot.
    SendRequest {
        request: http::Request<B>,
        slot: Arc<Slot>,
    },
}

/// The queue commands travel on.
///
/// Separate from [`Shared`] because it is the only part that names the body type. A
/// non-`Send` body therefore infects the handle, as it should, without reaching the waker.
pub(crate) struct Queue<B> {
    commands: Mutex<VecDeque<Command<B>>>,
}

impl<B> Default for Queue<B> {
    fn default() -> Self {
        Self {
            commands: Mutex::new(VecDeque::new()),
        }
    }
}

impl<B> Queue<B> {
    pub(crate) fn push(&self, command: Command<B>) {
        self.lock().push_back(command);
    }

    pub(crate) fn drain(&self) -> Vec<Command<B>> {
        self.lock().drain(..).collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Command<B>>> {
        self.commands
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Where one exchange's answer is delivered.
///
/// The driver fills it; the response future reads it. Both ends are reachable from
/// different tasks, so it carries its own lock rather than relying on the driver's.
#[derive(Debug, Default)]
pub(crate) struct Slot {
    state: Mutex<SlotState>,
}

#[derive(Debug, Default)]
struct SlotState {
    head: Option<http::Response<super::body::IncomingBody>>,
    error: Option<Error>,
    waker: Option<Waker>,
    settled: bool,
    /// Filled once the request has been submitted and a stream exists to name.
    ///
    /// A response future that is dropped before its answer arrives has to reset that
    /// stream, and this is the only place it can learn which one — it holds a slot, not a
    /// connection.
    stream: i32,
}

impl Slot {
    /// Delivers a response head.
    pub(crate) fn complete(&self, head: http::Response<super::body::IncomingBody>) {
        let waker = {
            let mut state = self.lock();
            if state.settled {
                return;
            }
            state.settled = true;
            state.head = Some(head);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Delivers a failure, unless an answer has already been delivered.
    ///
    /// Idempotent on purpose: a stream that closed with an error after its head arrived
    /// has already been answered, and teardown must not overwrite that.
    pub(crate) fn fail(&self, error: Error) {
        let waker = {
            let mut state = self.lock();
            if state.settled {
                return;
            }
            state.settled = true;
            state.error = Some(error);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether an answer has been delivered.
    pub(crate) fn is_settled(&self) -> bool {
        self.lock().settled
    }

    /// Names the stream this exchange was given.
    pub(crate) fn bind(&self, stream: i32) {
        self.lock().stream = stream;
    }

    /// The stream still owed an answer, if this exchange has one and is unsettled.
    pub(crate) fn unsettled_stream(&self) -> Option<i32> {
        let state = self.lock();
        (!state.settled && state.stream > 0).then_some(state.stream)
    }

    pub(crate) fn poll(
        &self,
        waker: &Waker,
    ) -> Poll<Result<http::Response<super::body::IncomingBody>>> {
        let mut state = self.lock();
        if let Some(head) = state.head.take() {
            return Poll::Ready(Ok(head));
        }
        if let Some(error) = state.error.take() {
            return Poll::Ready(Err(error));
        }
        match &state.waker {
            Some(current) if current.will_wake(waker) => {}
            _ => state.waker = Some(waker.clone()),
        }
        Poll::Pending
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// What one stream has received, waiting for its body to be read.
///
/// The driver fills it as frames arrive; [`IncomingBody`](super::body::IncomingBody)
/// empties it. Nothing here credits the peer — that is the body's doing, and only when the
/// application has actually taken a chunk, which is what makes the window track
/// consumption rather than arrival.
#[derive(Debug, Default)]
pub(crate) struct Incoming {
    state: Mutex<IncomingState>,
}

#[derive(Debug, Default)]
struct IncomingState {
    /// Payload as delivered, each chunk a refcounted view of a driver read buffer.
    chunks: VecDeque<Bytes>,
    /// Octets sitting in `chunks`, so a body that is dropped can return the window in one
    /// call rather than by walking what it never read.
    queued: usize,
    trailers: Option<http::HeaderMap>,
    /// The peer ended the message.
    finished: bool,
    error: Option<Error>,
    waker: Option<Waker>,
    /// The receiving body was dropped, so nothing will ever read what arrives next.
    abandoned: bool,
}

impl Incoming {
    /// Queues a received chunk, waking whoever is reading.
    ///
    /// Returns octets the driver should hand straight back to the peer — non-zero only
    /// when the body has been dropped, in which case holding the window shut for a chunk
    /// nobody will read would stall the whole connection.
    pub(crate) fn push(&self, chunk: Bytes) -> usize {
        let waker = {
            let mut state = self.lock();
            if state.abandoned {
                return chunk.len();
            }
            state.queued += chunk.len();
            state.chunks.push_back(chunk);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        0
    }

    /// Records a trailing header block.
    pub(crate) fn set_trailers(&self, trailers: http::HeaderMap) {
        let waker = {
            let mut state = self.lock();
            state.trailers = Some(trailers);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Marks the end of the message.
    pub(crate) fn finish(&self) {
        let waker = {
            let mut state = self.lock();
            state.finished = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Reports a failure, unless the message already ended.
    ///
    /// A stream that closed after its body was complete has nothing to report: the octets
    /// still queued are the whole message and the caller is entitled to read them.
    pub(crate) fn fail(&self, error: Error) {
        let waker = {
            let mut state = self.lock();
            if state.finished || state.error.is_some() {
                return;
            }
            state.error = Some(error);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether the peer finished sending this message, read or not.
    pub(crate) fn is_finished(&self) -> bool {
        let state = self.lock();
        state.finished || state.error.is_some()
    }

    /// Whether the message has ended and nothing is left to read.
    pub(crate) fn is_end_stream(&self) -> bool {
        let state = self.lock();
        state.finished
            && state.chunks.is_empty()
            && state.trailers.is_none()
            && state.error.is_none()
    }

    /// Takes the next frame, or registers `waker` to be told when there is one.
    pub(crate) fn poll_frame(&self, waker: &Waker) -> Poll<Option<Result<Frame<Bytes>>>> {
        let mut state = self.lock();

        if let Some(chunk) = state.chunks.pop_front() {
            state.queued -= chunk.len();
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        if let Some(error) = state.error.take() {
            return Poll::Ready(Some(Err(error)));
        }
        // Trailers last: they close the message, and anything still queued precedes them.
        if let Some(trailers) = state.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        if state.finished {
            return Poll::Ready(None);
        }

        match &state.waker {
            Some(current) if current.will_wake(waker) => {}
            _ => state.waker = Some(waker.clone()),
        }
        Poll::Pending
    }

    /// Gives up on anything unread, reporting how much window it holds.
    ///
    /// Called when the receiving body is dropped. The octets it never read still count
    /// against the connection's receive window, so they have to be handed back or the
    /// connection would slowly throttle itself to a halt.
    pub(crate) fn abandon(&self) -> usize {
        let mut state = self.lock();
        state.abandoned = true;
        state.chunks.clear();
        state.trailers = None;
        core::mem::take(&mut state.queued)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, IncomingState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The failure a stream that closed without ending its message reports.
pub(crate) fn truncated() -> Error {
    Error::new(
        ErrorKind::Stream,
        "the stream closed before the message body ended",
    )
}

/// What the driver knows about one live stream.
#[derive(Debug)]
pub(crate) struct Entry {
    /// Where this exchange's answer goes, for a client.
    ///
    /// A server has no response future to settle — its answer goes out on the wire, not
    /// back to a caller — so there is deliberately nothing here for it to fill.
    pub(crate) slot: Option<Arc<Slot>>,
    /// Where this exchange's received payload goes.
    ///
    /// Created with the stream rather than with the response head, so a peer that sends
    /// payload before anything has looked for it still has somewhere to put it.
    pub(crate) incoming: Arc<Incoming>,
    /// Proof that the stream is still live.
    ///
    /// The only strong handle, so removing the entry is what makes every waker that ever
    /// named this stream inert. Read only to hand out [`Weak`] copies of it — its
    /// existence, not its contents, is the signal.
    liveness: Arc<()>,
}

/// The streams currently in flight.
///
/// Shared with [`super::driver::DriverGuard`] rather than kept as a driver local, because
/// a driver future that is dropped — including one that was never polled — must still be
/// able to tell everyone waiting on it that no answer is coming.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    streams: Mutex<std::collections::BTreeMap<i32, Entry>>,
}

impl Registry {
    pub(crate) fn insert(
        &self,
        stream: i32,
        slot: Option<Arc<Slot>>,
        incoming: Arc<Incoming>,
        liveness: Arc<()>,
    ) {
        self.lock().insert(
            stream,
            Entry {
                slot,
                incoming,
                liveness,
            },
        );
    }

    /// The slot for a live stream, if this end of the connection has one.
    pub(crate) fn slot(&self, stream: i32) -> Option<Arc<Slot>> {
        self.lock()
            .get(&stream)
            .and_then(|entry| entry.slot.clone())
    }

    /// The receive queue for a live stream.
    pub(crate) fn incoming(&self, stream: i32) -> Option<Arc<Incoming>> {
        self.lock()
            .get(&stream)
            .map(|entry| Arc::clone(&entry.incoming))
    }

    /// A handle that stops resolving once `stream` leaves the registry.
    ///
    /// What a waker for this stream is gated on. Handed out rather than created afresh,
    /// so every waker naming a stream is retired by the one act of removing it.
    pub(crate) fn liveness(&self, stream: i32) -> Option<Weak<()>> {
        self.lock()
            .get(&stream)
            .map(|entry| Arc::downgrade(&entry.liveness))
    }

    /// Forgets a stream, retiring every waker that named it.
    pub(crate) fn remove(&self, stream: i32) -> Option<Entry> {
        self.lock().remove(&stream)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// The live streams above `limit`, which a `GOAWAY` says were never begun.
    pub(crate) fn above(&self, limit: i32) -> Vec<i32> {
        self.lock()
            .keys()
            .copied()
            .filter(|stream| *stream > limit)
            .collect()
    }

    /// Empties the registry, handing back everything that was in it.
    pub(crate) fn take_all(&self) -> Vec<Entry> {
        core::mem::take(&mut *self.lock()).into_values().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<i32, Entry>> {
        self.streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A waker that records nothing. The accounting below is what is under test, not the
    /// notification.
    fn nowhere() -> Waker {
        struct Silent;
        impl std::task::Wake for Silent {
            fn wake(self: Arc<Self>) {}
        }
        Waker::from(Arc::new(Silent))
    }

    fn taken(incoming: &Incoming) -> Option<usize> {
        match incoming.poll_frame(&nowhere()) {
            Poll::Ready(Some(Ok(frame))) => frame.data_ref().map(bytes::Bytes::len),
            _ => None,
        }
    }

    /// Every received octet must be accounted for exactly once: once by being read, or
    /// once by being abandoned, never both and never neither. Getting this wrong does not
    /// corrupt anything — it quietly throttles the connection, and the symptom shows up
    /// far from the cause, which is why it is pinned here rather than left to inference.
    #[test]
    fn octets_read_are_not_also_octets_abandoned() {
        let incoming = Incoming::default();
        assert_eq!(incoming.push(Bytes::from_static(b"first")), 0);
        assert_eq!(incoming.push(Bytes::from_static(b"second")), 0);

        assert_eq!(taken(&incoming), Some(5));
        // The five already handed over are the caller's to credit; only the six left are
        // the abandoning body's.
        assert_eq!(incoming.abandon(), 6);
    }

    #[test]
    fn everything_read_leaves_nothing_to_abandon() {
        let incoming = Incoming::default();
        assert_eq!(incoming.push(Bytes::from_static(b"payload")), 0);
        assert_eq!(taken(&incoming), Some(7));
        assert_eq!(incoming.abandon(), 0);
    }

    #[test]
    fn arrivals_after_a_body_is_dropped_are_credited_at_once() {
        let incoming = Incoming::default();
        assert_eq!(incoming.abandon(), 0);
        // Nothing will ever read this, so the driver is told to hand the window straight
        // back rather than let it accumulate behind a reader that does not exist.
        assert_eq!(incoming.push(Bytes::from_static(b"unwanted")), 8);
    }

    #[test]
    fn a_finished_message_is_not_retroactively_failed() {
        let incoming = Incoming::default();
        incoming.push(Bytes::from_static(b"body"));
        incoming.finish();
        incoming.fail(truncated());

        assert_eq!(taken(&incoming), Some(4));
        assert!(matches!(incoming.poll_frame(&nowhere()), Poll::Ready(None)));
        assert!(incoming.is_end_stream());
    }

    #[test]
    fn trailers_are_delivered_after_the_payload_that_precedes_them() {
        let incoming = Incoming::default();
        incoming.push(Bytes::from_static(b"body"));
        incoming.set_trailers(http::HeaderMap::new());
        incoming.finish();

        assert_eq!(taken(&incoming), Some(4));
        let trailing = incoming.poll_frame(&nowhere());
        assert!(
            matches!(&trailing, Poll::Ready(Some(Ok(frame))) if frame.trailers_ref().is_some()),
            "trailers overtook the payload they follow",
        );
        assert!(matches!(incoming.poll_frame(&nowhere()), Poll::Ready(None)));
    }
}
