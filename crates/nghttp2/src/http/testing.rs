//! Scaffolding for exercising the async layer, public but hidden.
//!
//! The crate takes no dev-dependencies by design, and integration tests are separate
//! crates that cannot reach `cfg(test)` items — so the machinery the tests need lives
//! here, marked `#[doc(hidden)]` to keep it out of the documented surface. It is not a
//! supported API and carries no compatibility promise.

use core::future::Future;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use bytes::{Bytes, BytesMut};

use super::transport::{Transport, TransportRead, TransportWrite};

/// The ecosystem crates the async layer is built on, re-exported for tests.
///
/// Integration tests are separate crates and can only reach what this one exposes. The
/// alternative would be dev-dependencies, which the crate deliberately does without.
pub use ::bytes as bytes_crate;
/// See [`bytes_crate`].
pub use ::http as http_crate;
/// See [`bytes_crate`].
pub use ::http_body as http_body_crate;

/// Wakes a parked [`block_on`].
struct Unparker {
    woken: Mutex<bool>,
    signal: Condvar,
}

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().expect("wake flag") = true;
        self.signal.notify_one();
    }
}

/// Drives a future to completion on the calling thread.
///
/// A real waker rather than a no-op one, so a future that returns `Pending` genuinely
/// waits instead of being polled in a spin — which matters here, since several of the
/// properties under test are about *not* being polled.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let unparker = Arc::new(Unparker {
        woken: Mutex::new(false),
        signal: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&unparker));
    let mut context = Context::from_waker(&waker);

    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }

        let mut woken = unparker.woken.lock().expect("wake flag");
        while !*woken {
            woken = unparker.signal.wait(woken).expect("waiting for a wake");
        }
        *woken = false;
    }
}

/// One direction of an in-memory connection.
#[derive(Debug, Default)]
struct Pipe {
    bytes: VecDeque<u8>,
    closed: bool,
    waker: Option<Waker>,
}

impl Pipe {
    /// Appends `data`, handing back whoever was waiting for it.
    ///
    /// The waker is returned rather than invoked, so the caller can release the pipe's
    /// lock before waking. Waking under a lock is the shape a deadlock takes when a woken
    /// task reaches straight back for the same lock, and scaffolding that models a
    /// transport should not be the one place that rule is broken.
    #[must_use]
    fn put(&mut self, data: &[u8]) -> Option<Waker> {
        self.bytes.extend(data.iter().copied());
        self.waker.take()
    }

    /// Marks the end of the stream, handing back whoever was waiting.
    #[must_use]
    fn close(&mut self) -> Option<Waker> {
        self.closed = true;
        self.waker.take()
    }
}

/// Runs a pipe operation and wakes the waiter, if any, outside the pipe's lock.
fn notifying(pipe: &Mutex<Pipe>, act: impl FnOnce(&mut Pipe) -> Option<Waker>) {
    let waker = act(&mut pipe
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner));
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// A transport wired directly to a peer, with no socket in between.
///
/// Reading blocks until the peer writes, so a test that deadlocks fails by hanging rather
/// than by silently reading zero and treating it as a clean close.
#[derive(Debug)]
pub struct Duplex {
    incoming: Arc<Mutex<Pipe>>,
    outgoing: Arc<Mutex<Pipe>>,
    /// Set when this transport overrides the borrowed-write path, so both drain
    /// strategies can be exercised against the same in-memory plumbing.
    borrowed_writes: bool,
    writes: Arc<Mutex<usize>>,
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
}

/// Creates a connected pair.
///
/// `borrowed_writes` selects which write path each side advertises, so a test can cover
/// the coalescing and zero-copy strategies without a second transport implementation.
pub fn duplex(borrowed_writes: bool) -> (Duplex, Duplex) {
    let one = Arc::new(Mutex::new(Pipe::default()));
    let two = Arc::new(Mutex::new(Pipe::default()));

    (
        Duplex {
            incoming: Arc::clone(&one),
            outgoing: Arc::clone(&two),
            borrowed_writes,
            writes: Arc::new(Mutex::new(0)),
            reads: Arc::new(Mutex::new(Vec::new())),
        },
        Duplex {
            incoming: two,
            outgoing: one,
            borrowed_writes,
            writes: Arc::new(Mutex::new(0)),
            reads: Arc::new(Mutex::new(Vec::new())),
        },
    )
}

impl Duplex {
    /// How many writes this half has issued.
    pub fn writes(&self) -> usize {
        *self.writes.lock().expect("write count")
    }

    /// A handle that keeps observing the write count after the transport is split.
    ///
    /// [`Transport::split`] consumes the transport, so a test driving a connection can no
    /// longer reach it — but the per-pass write counts are exactly what the later phases
    /// must assert. Taking a handle first is how that count stays observable.
    pub fn write_counter(&self) -> WriteCounter {
        WriteCounter {
            writes: Arc::clone(&self.writes),
        }
    }

    /// A handle that keeps observing the read buffers after the transport is split.
    ///
    /// The buffers the driver reads into are its own business and reach nothing else —
    /// except the transport, which is handed each one. That makes this the only vantage
    /// point from which "the octets a caller was given are the octets that were read" can
    /// be checked rather than assumed.
    pub fn buffer_log(&self) -> BufferLog {
        BufferLog {
            reads: Arc::clone(&self.reads),
        }
    }

    /// Signals end of stream to the peer.
    pub fn close(&self) {
        notifying(&self.outgoing, Pipe::close);
    }
}

/// Where each read put its octets, in the order the reads happened.
#[derive(Debug, Clone)]
pub struct BufferLog {
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl BufferLog {
    /// Every filled region so far, as `(address, length)`.
    pub fn regions(&self) -> Vec<(usize, usize)> {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// How many reads have happened, for marking a point in time.
    pub fn reads(&self) -> usize {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether `chunk` lies inside a region that was read into.
    pub fn holds(&self, chunk: &[u8]) -> bool {
        let start = chunk.as_ptr() as usize;
        let end = start + chunk.len();
        self.regions()
            .into_iter()
            .any(|(base, len)| start >= base && end <= base + len)
    }

    /// Whether any read from `mark` onwards wrote over the region `chunk` occupies.
    pub fn overwrote(&self, chunk: &[u8], mark: usize) -> bool {
        let start = chunk.as_ptr() as usize;
        let end = start + chunk.len();
        self.regions()
            .into_iter()
            .skip(mark)
            .any(|(base, len)| start < base + len && base < end)
    }

    /// How many reads landed in a buffer an earlier read had already used.
    ///
    /// Without this, "no read overwrote the chunk you were holding" could pass simply
    /// because no buffer was ever reused at all — a pool that recycles nothing satisfies
    /// the letter of the rule and none of its point.
    pub fn reuses(&self) -> usize {
        let regions = self.regions();
        let distinct: std::collections::BTreeSet<usize> =
            regions.iter().map(|(base, _)| *base).collect();
        regions.len() - distinct.len()
    }
}

/// Observes how many writes a transport has issued, across a split.
#[derive(Debug, Clone)]
pub struct WriteCounter {
    writes: Arc<Mutex<usize>>,
}

impl WriteCounter {
    /// Writes issued so far.
    pub fn get(&self) -> usize {
        *self.writes.lock().expect("write count")
    }

    /// Resets the count, so a test can measure one driver pass at a time.
    pub fn reset(&self) {
        *self.writes.lock().expect("write count") = 0;
    }
}

/// The reading half of a [`Duplex`].
#[derive(Debug)]
pub struct DuplexReader {
    incoming: Arc<Mutex<Pipe>>,
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
}

/// The writing half of a [`Duplex`].
#[derive(Debug)]
pub struct DuplexWriter {
    outgoing: Arc<Mutex<Pipe>>,
    borrowed_writes: bool,
    writes: Arc<Mutex<usize>>,
}

impl Transport for Duplex {
    type Reader = DuplexReader;
    type Writer = DuplexWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            DuplexReader {
                incoming: self.incoming,
                reads: self.reads,
            },
            DuplexWriter {
                outgoing: self.outgoing,
                borrowed_writes: self.borrowed_writes,
                writes: self.writes,
            },
        )
    }
}

impl TransportRead for DuplexReader {
    fn read(&mut self, mut buf: BytesMut) -> impl Future<Output = (io::Result<usize>, BytesMut)> {
        let incoming = Arc::clone(&self.incoming);
        let reads = Arc::clone(&self.reads);
        async move {
            // Wait for something to read, or for the peer to close. Parking here rather
            // than returning zero is deliberate: a test that deadlocks should hang and
            // fail, not quietly look like a clean shutdown.
            let available = core::future::poll_fn(|cx: &mut Context<'_>| {
                let mut pipe = incoming.lock().expect("incoming pipe");
                if pipe.bytes.is_empty() {
                    if pipe.closed {
                        return Poll::Ready(0usize);
                    }
                    pipe.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(pipe.bytes.len())
            })
            .await;

            if available == 0 {
                return (Ok(0), buf);
            }

            let room = buf.capacity().saturating_sub(buf.len()).max(1);
            let take = available.min(room);
            let chunk: Vec<u8> = incoming
                .lock()
                .expect("incoming pipe")
                .bytes
                .drain(..take)
                .collect();
            buf.extend_from_slice(&chunk);
            // Recorded after the fill, so the region named is exactly the one the octets
            // landed in.
            reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((buf.as_ptr() as usize, buf.len()));
            (Ok(take), buf)
        }
    }
}

impl DuplexWriter {
    /// Writes issued by this half.
    pub fn writes(&self) -> usize {
        *self.writes.lock().expect("write count")
    }
}

impl Drop for DuplexWriter {
    /// Closing on drop is what lets a test model a peer hanging up, and is what a real
    /// socket does.
    fn drop(&mut self) {
        notifying(&self.outgoing, Pipe::close);
    }
}

impl TransportWrite for DuplexWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
        *self.writes.lock().expect("write count") += 1;
        notifying(&self.outgoing, |pipe| pipe.put(&buf));
        let written = buf.len();
        core::future::ready((Ok(written), buf))
    }

    fn write_borrowed(&mut self, data: &[u8]) -> impl Future<Output = io::Result<usize>> {
        *self.writes.lock().expect("write count") += 1;
        notifying(&self.outgoing, |pipe| pipe.put(data));
        core::future::ready(Ok(data.len()))
    }

    fn writes_borrowed(&self) -> bool {
        self.borrowed_writes
    }
}

/// Polls `background` alongside `main`, finishing when `main` does.
///
/// Everything an asynchronous connection does needs at least two things running at once —
/// the driver and whatever is awaiting it — and often three, with a peer as well. Nesting
/// these gives that without a runtime, and without spawning: the properties under test are
/// about what happens on one task, and putting them on one task is how they stay
/// observable.
pub async fn alongside<M: Future, B: Future>(main: M, background: B) -> M::Output {
    let mut main = core::pin::pin!(main);
    let mut background = core::pin::pin!(background);
    let mut finished = false;

    core::future::poll_fn(move |cx| {
        // Background first: the driver should have moved whatever it can before the thing
        // waiting on it looks again.
        if !finished && background.as_mut().poll(cx).is_ready() {
            finished = true;
        }
        main.as_mut().poll(cx)
    })
    .await
}

/// Whether the session the client driver builds reports receive consumption itself.
///
/// [`Session::consume`](crate::Session::consume) is rejected outright on a session that
/// replenishes windows automatically, so a successful call is proof that this one does
/// not — asserted against a real session rather than read off a constant.
pub fn client_session_has_manual_flow_control() -> bool {
    let mut session = super::driver::client_session().expect("building a client session");
    session.consume(crate::StreamId::new(1), 0).is_ok()
}

/// A body with nothing in it.
#[derive(Debug, Default, Clone, Copy)]
pub struct Empty;

impl http_body::Body for Empty {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        true
    }
}

/// A body already held in memory.
#[derive(Debug)]
pub struct Full {
    data: Option<Bytes>,
}

impl Full {
    /// A body consisting of exactly these octets.
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self {
            data: Some(data.into()),
        }
    }
}

impl http_body::Body for Full {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(
            self.data
                .take()
                .map(|data| Ok(http_body::Frame::data(data))),
        )
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }
}

/// The state a [`Scripted`] body and its handle share.
#[derive(Debug, Default)]
struct Script {
    chunks: Mutex<VecDeque<Bytes>>,
    trailers: Mutex<Option<http::HeaderMap>>,
    failure: Mutex<Option<&'static str>>,
    finished: Mutex<bool>,
    waker: Mutex<Option<Waker>>,
    consultations: AtomicUsize,
}

impl Script {
    fn signal(&self) {
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// What a [`Scripted`] body reports when told to fail.
#[derive(Debug)]
pub struct ScriptError(&'static str);

impl core::fmt::Display for ScriptError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ScriptError {}

/// A body that answers only when told to, and counts how often it is asked.
///
/// This is the instrument the deferral proof is made with: "the body is never consulted
/// without an intervening wake" is only assertable against a body that never becomes ready
/// on its own.
#[derive(Debug)]
pub struct Scripted {
    script: Arc<Script>,
}

/// Drives a [`Scripted`] body from the test.
#[derive(Debug, Clone)]
pub struct ScriptHandle {
    script: Arc<Script>,
}

/// A body under test control, and the handle that controls it.
pub fn scripted() -> (Scripted, ScriptHandle) {
    let script = Arc::new(Script::default());
    (
        Scripted {
            script: Arc::clone(&script),
        },
        ScriptHandle { script },
    )
}

impl ScriptHandle {
    /// How many times the body has been asked for content.
    pub fn consultations(&self) -> usize {
        self.script.consultations.load(Ordering::Acquire)
    }

    /// Whether the body is parked, having registered a waker and answered `Pending`.
    pub fn is_deferred(&self) -> bool {
        self.script
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Makes octets available and wakes the body.
    pub fn send(&self, data: impl Into<Bytes>) {
        self.script
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(data.into());
        self.script.signal();
    }

    /// Ends the body and wakes it.
    pub fn finish(&self) {
        *self
            .script
            .finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.script.signal();
    }

    /// Wakes the body without making anything available.
    ///
    /// A permitted thing for a real body to do, and the case a driver must survive: the
    /// only correct response is to consult the body once more and let it defer again.
    pub fn wake_spuriously(&self) {
        self.script.signal();
    }

    /// Ends the body with a trailing header block, and wakes it.
    ///
    /// Delivered after everything already queued, which is the order `http_body` requires
    /// and the order the wire requires.
    pub fn finish_with_trailers(&self, trailers: http::HeaderMap) {
        *self
            .script
            .trailers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(trailers);
        self.finish();
    }

    /// Makes the body report a failure the next time it is asked, and wakes it.
    pub fn fail(&self, detail: &'static str) {
        *self
            .script
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail);
        self.script.signal();
    }
}

impl http_body::Body for Scripted {
    type Data = Bytes;
    type Error = ScriptError;

    fn poll_frame(
        self: core::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, ScriptError>>> {
        self.script.consultations.fetch_add(1, Ordering::AcqRel);

        let next = self
            .script
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front();
        if let Some(data) = next {
            return Poll::Ready(Some(Ok(http_body::Frame::data(data))));
        }

        let failure = self
            .script
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(detail) = failure {
            return Poll::Ready(Some(Err(ScriptError(detail))));
        }

        let trailers = self
            .script
            .trailers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(trailers) = trailers {
            return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }

        if *self
            .script
            .finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return Poll::Ready(None);
        }

        *self
            .script
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// Runs a sans-I/O session over a transport, as the peer of the connection under test.
///
/// `step` runs until it has nothing more to add, then whatever the session produced is
/// written, then the peer waits for more input. Returns when the peer stops sending.
///
/// The step is re-run after each write rather than once per pass because some things only
/// become legal once the previous thing has gone out — trailers, for one, cannot be
/// submitted until the body they follow has been serialised. A peer that stepped only once
/// per read would sit on them until the connection under test happened to send something,
/// which is a property of this scaffolding rather than of HTTP/2.
pub async fn serve<T: Transport, C>(
    transport: T,
    mut session: crate::Session<C>,
    context: &mut C,
    mut step: impl FnMut(&mut crate::Session<C>, &mut C),
) -> io::Result<()> {
    let (mut reader, mut writer) = transport.split();

    loop {
        loop {
            step(&mut session, context);

            let mut out = BytesMut::new();
            while let Some(block) = session.send(context).expect("serialising") {
                out.extend_from_slice(block);
            }
            if out.is_empty() {
                break;
            }
            let (result, _returned) = writer.write(out.freeze()).await;
            result?;
        }

        let (result, buf) = reader.read(BytesMut::with_capacity(16 * 1024)).await;
        if result? == 0 {
            return Ok(());
        }
        session.recv(&buf, context).expect("receiving");
    }
}

/// The most chunks any one outgoing body on this connection has held back at once.
///
/// The send path retains at most one unconsumed chunk per stream. This is the named hook
/// that claim is asserted against — a property proven by inspection is a property that
/// stops being true the first time someone edits the file without reading the comment.
pub fn buffered_chunks<B>(handle: &super::client::SendRequest<B>) -> usize {
    handle.buffered_chunks()
}

/// How many streams a connection is holding wakes for.
///
/// Exposed as a free function rather than a method so the property stays testable without
/// widening the connection's public surface.
pub fn pending_wakes<B>(handle: &super::client::SendRequest<B>) -> usize {
    handle.pending_wakes()
}

impl ScriptHandle {
    /// A clone of the waker the body was last handed, if it is parked.
    ///
    /// Taking a copy is what makes a *stale* waker testable: a real body may clone the
    /// waker it is given and invoke it long after its stream has gone, and the driver's
    /// bound on the ready set has to hold when it does.
    pub fn stale_waker(&self) -> Option<Waker> {
        self.script
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
