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
    /// Which of the three drain strategies this half advertises, so all of them can be
    /// exercised against the same in-memory plumbing.
    shape: WriteShape,
    writes: Arc<Mutex<usize>>,
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
    vectored: Arc<Mutex<VectoredRecord>>,
    limits: Arc<Mutex<VecDeque<usize>>>,
    decline_after: Arc<Mutex<Option<usize>>>,
    elections: Arc<Mutex<ElectionRecord>>,
}

/// How often each write-strategy election was consulted, and the owned-region write taken.
///
/// The election probes are the driver's per-pass strategy choice, distinct from the writes
/// that follow it: a test that wants to pin *precedence* — the vectored election is read
/// first, the owned-region one only when it declines — or *once-per-pass* — the choice is
/// read once a pass and never once per write — needs to count the consultations, not the
/// writes. [`VectoredRecord`] already counts the writes; this counts the choosing.
#[derive(Debug, Default)]
struct ElectionRecord {
    /// Times the vectored election was probed: one construct-and-drop `write_vectored(&[])`
    /// per flush pass, so this is the pass count as the driver sees it.
    vectored_probes: usize,
    /// Times [`TransportWrite::gathers_owned_regions`] was read. Zero when the vectored
    /// election took precedence, since the driver never consults it after vectored wins.
    owned_region_elections: usize,
    /// Times [`TransportWrite::write_regions`] actually ran — the owned-region *write*, not
    /// its election. Retries within a pass count, since each is a real call to the transport.
    region_writes: usize,
}

/// Which write strategy a [`Duplex`] half offers the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteShape {
    /// Overrides nothing, so the driver coalesces a pass into one owned write.
    Owned,
    /// Overrides the borrowed path: one write per session block, nothing copied.
    Borrowed,
    /// Overrides the vectored path: small blocks gathered with a driver-owned buffer.
    Vectored,
    /// Overrides both fast paths, so the driver's precedence rule has something to decide.
    Both,
    /// Overrides the owned-region path: the completion strategy, a gathering write over an
    /// owned `Vec<Bytes>`. Payloads ride uncopied in the caller's own memory.
    OwnedRegions,
    /// Overrides the vectored path *and* the owned-region one, so the driver's precedence
    /// rule — read the vectored election first, take the owned-region path only when it
    /// declines — has something to arbitrate over a driven connection rather than only at the
    /// trait surface.
    VectoredAndOwnedRegions,
}

impl WriteShape {
    const fn offers_borrowed(self) -> bool {
        matches!(self, Self::Borrowed | Self::Both)
    }

    const fn offers_vectored(self) -> bool {
        matches!(
            self,
            Self::Vectored | Self::Both | Self::VectoredAndOwnedRegions
        )
    }

    const fn offers_owned_regions(self) -> bool {
        matches!(self, Self::OwnedRegions | Self::VectoredAndOwnedRegions)
    }
}

/// What a vectored duplex half saw, recorded as the writes actually happened.
#[derive(Debug, Default)]
struct VectoredRecord {
    /// The region lengths of each polled call, in order.
    calls: Vec<Vec<usize>>,
    /// The base address of each region of each polled call, in the same shape as
    /// [`calls`](Self::calls).
    ///
    /// Recorded so a test can ask *where* the octets a write gathered came from, not only
    /// how many there were. Phase 2 records these addresses; the two-sided coverage
    /// assertion that pins a caller's chunk to an untouched region is Phase 3's (design
    /// decision D8). An address is only meaningful for the instant of the call that logged
    /// it — the buffer behind it may be reused afterwards — so a reader must compare it
    /// against ranges captured at the same time, never dereference it.
    bases: Vec<Vec<usize>>,
    /// Every octet handed over, concatenated in the order it was offered.
    octets: Vec<u8>,
    /// Calls that re-offered the remainder of a short write, rather than new octets.
    retries: usize,
    /// Whether the previous call was short, which makes the next one a retry.
    last_was_short: bool,
}
/// Creates a connected pair.
///
/// `borrowed_writes` selects which write path each side advertises, so a test can cover
/// the coalescing and zero-copy strategies without a second transport implementation. For
/// the vectored strategy, see [`duplex_vectored`].
pub fn duplex(borrowed_writes: bool) -> (Duplex, Duplex) {
    let shape = if borrowed_writes {
        WriteShape::Borrowed
    } else {
        WriteShape::Owned
    };
    pair(shape)
}

/// Creates a connected pair whose halves elect the vectored write path.
///
/// Separate from [`duplex`] rather than another argument to it: the boolean there names a
/// choice between two strategies at around seventy-five call sites, and rewriting all of
/// them to say "not vectored" would be a large diff that could only lose information.
///
/// A half made this way records what it was offered — see [`Duplex::vectored_log`] — and can
/// be told to accept only a prefix of each call, see [`Duplex::accept_at_most`], which is how
/// short writes are driven deterministically rather than hoped for.
pub fn duplex_vectored() -> (Duplex, Duplex) {
    pair(WriteShape::Vectored)
}

/// Creates a connected pair offering **both** fast paths.
///
/// The driver's precedence rule — vectored wins — is only observable against a transport
/// that genuinely offers both, since with either alone there is nothing to arbitrate.
pub fn duplex_offering_both() -> (Duplex, Duplex) {
    pair(WriteShape::Both)
}

/// Creates a connected pair whose halves elect the owned-region (completion) write path.
///
/// A half made this way reports `gathers_owned_regions`, receives an owned `Vec<Bytes>` at
/// each gathering write, and records it through the same [`VectoredLog`] the vectored shape
/// uses — so the pointer-coverage assertion of design decision D8 sees the completion path
/// too. It honours [`Duplex::accept_at_most`] the same way, so owned-region short writes are
/// driven deterministically rather than hoped for.
pub fn duplex_owned_regions() -> (Duplex, Duplex) {
    pair(WriteShape::OwnedRegions)
}

/// Creates a connected pair offering **both** the vectored and the owned-region paths.
///
/// Separate from [`duplex_offering_both`] — which offers the *borrowed* and vectored paths —
/// because the precedence this one arbitrates is the other pairing: a completion transport
/// that owns its buffers yet also advertises the readiness vectored path. The driver's rule
/// is that vectored is read first and the owned-region election consulted only when it
/// declines, so a half made this way must have its regions carried by the vectored path and
/// never see [`TransportWrite::write_regions`]. Watch which election the driver consulted, and
/// how often, through [`Duplex::election_log`]; watch what the vectored path gathered through
/// [`Duplex::vectored_log`].
pub fn duplex_vectored_and_owned_regions() -> (Duplex, Duplex) {
    pair(WriteShape::VectoredAndOwnedRegions)
}

fn pair(shape: WriteShape) -> (Duplex, Duplex) {
    let one = Arc::new(Mutex::new(Pipe::default()));
    let two = Arc::new(Mutex::new(Pipe::default()));

    (
        Duplex {
            incoming: Arc::clone(&one),
            outgoing: Arc::clone(&two),
            shape,
            writes: Arc::new(Mutex::new(0)),
            reads: Arc::new(Mutex::new(Vec::new())),
            vectored: Arc::new(Mutex::new(VectoredRecord::default())),
            limits: Arc::new(Mutex::new(VecDeque::new())),
            decline_after: Arc::new(Mutex::new(None)),
            elections: Arc::new(Mutex::new(ElectionRecord::default())),
        },
        Duplex {
            incoming: two,
            outgoing: one,
            shape,
            writes: Arc::new(Mutex::new(0)),
            reads: Arc::new(Mutex::new(Vec::new())),
            vectored: Arc::new(Mutex::new(VectoredRecord::default())),
            limits: Arc::new(Mutex::new(VecDeque::new())),
            decline_after: Arc::new(Mutex::new(None)),
            elections: Arc::new(Mutex::new(ElectionRecord::default())),
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

    /// A handle that keeps observing the vectored writes after the transport is split.
    ///
    /// Populated by the vectored shape, which logs each gathering call, by the borrowed
    /// shape, which logs each uncopied `write_borrowed` as a single-region call, and by the
    /// owned-region shape, which logs each `write_regions` call the same way the vectored one
    /// does — so the pointer-coverage assertion can see all three. Empty on the owned shape,
    /// which reaches no fast path and is coalesced through `write`.
    pub fn vectored_log(&self) -> VectoredLog {
        VectoredLog {
            record: Arc::clone(&self.vectored),
        }
    }

    /// A handle that keeps observing the write-strategy elections after the transport is
    /// split.
    ///
    /// [`Transport::split`] consumes the transport, so a test driving a connection can no
    /// longer reach it — but which election the driver consulted, and how often, is exactly
    /// what a precedence or once-per-pass assertion turns on. Taking a handle first is how
    /// that stays observable. See [`ElectionLog`] for what each count means.
    pub fn election_log(&self) -> ElectionLog {
        ElectionLog {
            record: Arc::clone(&self.elections),
        }
    }

    /// Caps how many octets each subsequent vectored call accepts.
    ///
    /// One cap per call, consumed in order; once they run out, every call accepts
    /// everything it is offered. A cap of zero has the transport report a successful write
    /// of nothing, which the driver must treat as an error rather than spin on.
    ///
    /// This is how partial writes get driven deterministically. A real socket short-writes
    /// when it feels like it, which is untestable; naming the prefix makes the interesting
    /// cases — a cut inside the first region, a cut exactly on the boundary between the two,
    /// a cut one octet from the end — reachable on purpose.
    pub fn accept_at_most(&self, caps: impl IntoIterator<Item = usize>) {
        let mut limits = self.limits.lock().expect("write limits");
        limits.clear();
        limits.extend(caps);
    }

    /// Has this half stop offering the vectored path once it has performed `writes` of them.
    ///
    /// Models a transport violating the contract: the election is meant to be a fixed
    /// property, read once per pass and held, but nothing in the signature stops a later
    /// call returning `None`. The driver must survive that by falling back to coalescing —
    /// paying the copy it was avoiding, but neither losing an octet nor panicking — and
    /// this is how that branch gets driven. Left alone, the vectored path is offered
    /// forever.
    ///
    /// Counted against writes actually performed, not futures constructed, and never
    /// against the driver's region-less election probe — which is why a limit of zero is
    /// meaningful rather than degenerate: the path is elected, and then the very first real
    /// call is refused, with the driver's accumulation buffer still holding everything the
    /// pass has gathered so far.
    pub fn decline_vectored_after(&self, writes: usize) {
        *self.decline_after.lock().expect("decline limit") = Some(writes);
    }

    /// Signals end of stream to the peer.
    pub fn close(&self) {
        notifying(&self.outgoing, Pipe::close);
    }
}

/// What a vectored transport half was offered, and accepted.
#[derive(Debug, Clone)]
pub struct VectoredLog {
    record: Arc<Mutex<VectoredRecord>>,
}

/// Which write-strategy elections a transport half was asked for, and how often.
///
/// Distinct from [`VectoredLog`], which records the *writes*: this records the *choosing*
/// that precedes them. The two answer different questions — "what went on the wire" versus
/// "which strategy the driver picked, and whether it picked once a pass" — so the counts live
/// apart rather than being teased out of one log after the fact.
#[derive(Debug, Clone)]
pub struct ElectionLog {
    record: Arc<Mutex<ElectionRecord>>,
}

impl ElectionLog {
    /// Times the vectored election was probed — one construct-and-drop `write_vectored(&[])`
    /// per flush pass, so this is the number of passes the driver ran, counted where it makes
    /// its per-pass strategy choice rather than inferred from the writes that followed.
    pub fn vectored_probes(&self) -> usize {
        self.record.lock().expect("election record").vectored_probes
    }

    /// Times [`TransportWrite::gathers_owned_regions`] was read. Zero when the vectored
    /// election took precedence, since the driver stops at the first election that offers
    /// itself and never consults this one after vectored wins — which is what makes a count of
    /// zero here, over a transport that advertises *both*, a proof of precedence rather than
    /// of the owned-region path merely going unused.
    pub fn owned_region_elections(&self) -> usize {
        self.record
            .lock()
            .expect("election record")
            .owned_region_elections
    }

    /// Times [`TransportWrite::write_regions`] actually ran — the owned-region *write*, retries
    /// included. Zero alongside a positive [`vectored_probes`](ElectionLog::vectored_probes)
    /// over a both-advertising transport says the vectored path carried the traffic and the
    /// owned-region write was never taken.
    pub fn region_writes(&self) -> usize {
        self.record.lock().expect("election record").region_writes
    }
}

impl VectoredLog {
    /// The region lengths of each polled call, in order.
    ///
    /// One entry per call, so the length of this is the number of writes and each inner
    /// vector's length is how many regions that write gathered.
    pub fn calls(&self) -> Vec<Vec<usize>> {
        self.record.lock().expect("vectored record").calls.clone()
    }

    /// Every octet handed over, concatenated in the order it was offered.
    ///
    /// The point of comparison for "the vectored path puts the same octets on the wire, in
    /// the same order, as the coalescing path would".
    pub fn octets(&self) -> Vec<u8> {
        self.record.lock().expect("vectored record").octets.clone()
    }

    /// Calls that re-offered the remainder of a short write rather than new octets.
    ///
    /// Counted apart from the calls in [`calls`](VectoredLog::calls) so that a bound on
    /// writes per pass can exclude retries without having to reconstruct which was which.
    pub fn retries(&self) -> usize {
        self.record.lock().expect("vectored record").retries
    }

    /// The base address of each region of each polled call, shaped like
    /// [`calls`](VectoredLog::calls).
    ///
    /// Lets a test see *where* a gathered write's octets came from — the driver's own
    /// coalescing buffer, or a caller's `Bytes` handed over uncopied. Phase 2 records these
    /// so Phase 3 can assert that a no-copy chunk's address falls inside a region the
    /// transport was offered and outside the driver's buffer; on its own an address here
    /// proves nothing and must never be dereferenced, since the buffer behind it may have
    /// been reused by the time it is read.
    pub fn bases(&self) -> Vec<Vec<usize>> {
        self.record.lock().expect("vectored record").bases.clone()
    }

    /// Forgets everything so far, so a test can measure one driver pass at a time.
    pub fn reset(&self) {
        *self.record.lock().expect("vectored record") = VectoredRecord::default();
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
    shape: WriteShape,
    writes: Arc<Mutex<usize>>,
    vectored: Arc<Mutex<VectoredRecord>>,
    limits: Arc<Mutex<VecDeque<usize>>>,
    decline_after: Arc<Mutex<Option<usize>>>,
    elections: Arc<Mutex<ElectionRecord>>,
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
                shape: self.shape,
                writes: self.writes,
                vectored: self.vectored,
                limits: self.limits,
                decline_after: self.decline_after,
                elections: self.elections,
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

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // Which shape this duplex takes is fixed at construction: only the borrowed variant
        // elects the zero-copy path, the owned one declines it and is coalesced through
        // `write`. Returning the write here — rather than a separate flag — is the whole
        // decision.
        if !self.shape.offers_borrowed() {
            return None;
        }
        *self.writes.lock().expect("write count") += 1;
        // Record where these octets came from, exactly as the vectored path does, so the
        // two-sided pointer-coverage assertion (design decision D8) can pin a handed-over
        // payload to the caller's own memory on the borrowed strategy too — not only on the
        // vectored one. One borrowed write is one region, so it is logged as a single-region
        // call. The address is meaningful only for the instant of the call, as the vectored
        // log's own note explains.
        {
            let mut record = self.vectored.lock().expect("vectored record");
            record.calls.push(vec![data.len()]);
            record.bases.push(vec![data.as_ptr() as usize]);
            record.octets.extend_from_slice(data);
        }
        notifying(&self.outgoing, |pipe| pipe.put(data));
        Some(core::future::ready(Ok(data.len())))
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        if regions.is_empty() {
            // The driver's per-pass vectored election: a construct-and-drop probe with no
            // regions. Counted here — at the one place per pass it happens, and recognisable
            // as the only call with no regions — so a test can pin that the election is read
            // once a pass and not once per write, independent of the regions later offered.
            // Counted before the `offers_vectored` gate so a declining shape's probe still
            // registers as a pass, which is what makes this the pass count for any shape.
            self.elections
                .lock()
                .expect("election record")
                .vectored_probes += 1;
        }
        if !self.shape.offers_vectored() {
            return None;
        }
        // The contract-violation knob. Two things keep it from swallowing the election
        // itself. It is read against writes *performed* rather than futures built, so a
        // constructed-and-dropped probe never spends one; and the probe is recognisable —
        // it is the only call with no regions at all — so a limit of zero declines the
        // first real write while still electing the path, which is the case that would
        // otherwise be unreachable.
        if !regions.is_empty() {
            let performed = self.vectored.lock().expect("vectored record").calls.len();
            match *self.decline_after.lock().expect("decline limit") {
                Some(limit) if performed >= limit => return None,
                _ => {}
            }
        }
        // Note what is *not* here: nothing is recorded, and no octet moves. The driver
        // elects a strategy by building one of these and dropping it without polling, so
        // any effect at construction time would be an effect that never happened. All of it
        // lives in `poll` below. The borrowed path above can afford to be laxer because
        // nothing probes it with an empty slice.
        Some(DuplexVectoredWrite {
            regions,
            outgoing: Arc::clone(&self.outgoing),
            writes: Arc::clone(&self.writes),
            record: Arc::clone(&self.vectored),
            limits: Arc::clone(&self.limits),
        })
    }

    fn gathers_owned_regions(&self) -> bool {
        // The completion election, fixed at construction exactly as the readiness ones are.
        // Only the owned-region shapes advertise it; every other shape leaves the default
        // `false` in place, so the driver never offers them a `Vec<Bytes>`. Counting each read
        // — through interior mutability, since the trait takes `&self` — lets a test see
        // whether the driver consulted this election at all: over a transport that also offers
        // the vectored path, a count of zero is proof the vectored election took precedence.
        self.elections
            .lock()
            .expect("election record")
            .owned_region_elections += 1;
        self.shape.offers_owned_regions()
    }

    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<Bytes>)> {
        // The owned counterpart of `DuplexVectoredWrite::poll`. It runs eagerly rather than
        // as an inert future because there is nothing to probe: the election is a plain bool,
        // so the driver never constructs one of these speculatively. The logging, cap
        // handling, and retry accounting mirror the vectored path so one `VectoredLog` covers
        // both — see that path for why each piece is shaped the way it is.
        //
        // The owned-region *write* is counted apart from its election: retries included, since
        // each is a real call the transport served, which is what lets a test show the write
        // ran more often than the once-per-pass election that chose it.
        self.elections
            .lock()
            .expect("election record")
            .region_writes += 1;
        let offered: usize = regions.iter().map(Bytes::len).sum();
        let cap = self
            .limits
            .lock()
            .expect("write limits")
            .pop_front()
            .unwrap_or(offered);
        let accepted = cap.min(offered);

        let mut record = self.vectored.lock().expect("vectored record");
        if record.last_was_short {
            record.retries += 1;
        } else {
            *self.writes.lock().expect("write count") += 1;
        }
        record.calls.push(regions.iter().map(Bytes::len).collect());
        record.bases.push(
            regions
                .iter()
                .map(|region| region.as_ptr() as usize)
                .collect(),
        );
        record.last_was_short = accepted < offered;

        let mut remaining = accepted;
        for region in &regions {
            if remaining == 0 {
                break;
            }
            let take = remaining.min(region.len());
            record.octets.extend_from_slice(&region[..take]);
            notifying(&self.outgoing, |pipe| pipe.put(&region[..take]));
            remaining -= take;
        }
        drop(record);

        core::future::ready((Ok(accepted), regions))
    }
}

/// The write a vectored [`DuplexWriter`] hands back — inert until polled.
#[derive(Debug)]
struct DuplexVectoredWrite<'w> {
    regions: &'w [io::IoSlice<'w>],
    outgoing: Arc<Mutex<Pipe>>,
    writes: Arc<Mutex<usize>>,
    record: Arc<Mutex<VectoredRecord>>,
    limits: Arc<Mutex<VecDeque<usize>>>,
}

impl Future for DuplexVectoredWrite<'_> {
    type Output = io::Result<usize>;

    fn poll(self: core::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let offered: usize = me.regions.iter().map(|region| region.len()).sum();
        let cap = me
            .limits
            .lock()
            .expect("write limits")
            .pop_front()
            .unwrap_or(offered);
        let accepted = cap.min(offered);

        let mut record = me.record.lock().expect("vectored record");
        // A call that follows a short one is re-offering octets already counted, so it is a
        // retry rather than another logical write. Keeping the two apart is what lets a test
        // bound writes per pass without first having to work out which was which.
        if record.last_was_short {
            record.retries += 1;
        } else {
            *me.writes.lock().expect("write count") += 1;
        }
        record
            .calls
            .push(me.regions.iter().map(|region| region.len()).collect());
        record.bases.push(
            me.regions
                .iter()
                .map(|region| region.as_ptr() as usize)
                .collect(),
        );
        record.last_was_short = accepted < offered;

        let mut remaining = accepted;
        for region in me.regions {
            if remaining == 0 {
                break;
            }
            let take = remaining.min(region.len());
            record.octets.extend_from_slice(&region[..take]);
            notifying(&me.outgoing, |pipe| pipe.put(&region[..take]));
            remaining -= take;
        }

        Poll::Ready(Ok(accepted))
    }
}

/// A transport that works for a while and then does not.
///
/// Reads and writes pass through to a [`Duplex`] until the countdown reaches zero, after
/// which every operation reports the same error. That is what makes "this failure came
/// from the transport, not from the protocol" assertable: the same exchange either
/// completes or fails, and which one is the caller's choice.
#[derive(Debug)]
pub struct Failing {
    inner: Duplex,
    /// Operations left before the failure. Shared so both halves count against one budget.
    countdown: Arc<AtomicUsize>,
    /// Whether it is reads or writes that fail.
    on_read: bool,
}

/// A transport that fails after `after` operations, and its unbroken peer.
///
/// `on_read` chooses the direction that breaks, since a socket may fail either way and the
/// two reach the driver through different paths.
pub fn failing(after: usize, on_read: bool) -> (Failing, Duplex) {
    over(duplex(false), after, on_read)
}

/// A [`Failing`] whose unbroken half elects the **vectored** write path, and its peer.
///
/// Separate from [`failing`] rather than another argument to it, for the same reason
/// [`duplex_vectored`] is separate from [`duplex`]: the shape a transport advertises is a
/// property of the transport, so it belongs in the constructor's name, not in a boolean
/// threaded through every call site. It exists because [`failing`] alone can only break the
/// owned/coalescing path — the fast paths run through the driver's `flush_regions`, which
/// `failing` never reaches — so a transport that fails *while* electing the vectored strategy
/// is the only way to drive a transport error through that code. Observe what it gathered
/// through [`Failing::vectored_log`].
pub fn failing_vectored(after: usize, on_read: bool) -> (Failing, Duplex) {
    over(duplex_vectored(), after, on_read)
}

/// A [`Failing`] whose unbroken half elects the **borrowed** (zero-copy) write path, and its
/// peer.
///
/// The borrowed counterpart to [`failing_vectored`], and separate from [`failing`] for the
/// same reason: the borrowed strategy also runs through `flush_regions`, so a transport that
/// fails while lending the borrowed path is the second way — beside the vectored one — to
/// drive a transport error through that code. Where the vectored path fails on one gathered
/// write of many regions, the borrowed path writes one region per call, so a failure lands on
/// a single named region; [`Failing::vectored_log`] records each as a single-region write.
pub fn failing_borrowed(after: usize, on_read: bool) -> (Failing, Duplex) {
    over(duplex(true), after, on_read)
}

/// Builds a [`Failing`] over the already-made duplex pair, arming the first half.
///
/// The three `failing*` constructors differ only in which strategy their duplex advertises,
/// so the countdown wiring lives here once rather than being repeated three times.
fn over((one, two): (Duplex, Duplex), after: usize, on_read: bool) -> (Failing, Duplex) {
    (
        Failing {
            inner: one,
            countdown: Arc::new(AtomicUsize::new(after)),
            on_read,
        },
        two,
    )
}

impl Failing {
    /// A handle that keeps observing the unbroken half's vectored writes after the transport
    /// is split.
    ///
    /// [`Transport::split`] consumes the transport, so a test driving a failing connection
    /// must take this before handing the transport to the driver. Populated exactly as
    /// [`Duplex::vectored_log`] is — by the vectored shape logging each gathering call and the
    /// borrowed shape logging each uncopied write as a single region — which is how a test can
    /// prove the failing write really was carrying payload regions rather than a bare
    /// handshake block.
    pub fn vectored_log(&self) -> VectoredLog {
        self.inner.vectored_log()
    }
}

/// The reading half of a [`Failing`].
#[derive(Debug)]
pub struct FailingReader {
    inner: DuplexReader,
    countdown: Arc<AtomicUsize>,
    armed: bool,
}

/// The writing half of a [`Failing`].
#[derive(Debug)]
pub struct FailingWriter {
    inner: DuplexWriter,
    countdown: Arc<AtomicUsize>,
    armed: bool,
}

/// Counts one operation down, reporting whether this is the one that fails.
fn spent(countdown: &AtomicUsize, armed: bool) -> bool {
    if !armed {
        return false;
    }
    // Saturating, so every operation after the first failure fails too — a transport that
    // recovered on its own would be a different thing to test.
    let left = countdown.load(Ordering::Acquire);
    if left == 0 {
        return true;
    }
    countdown.store(left - 1, Ordering::Release);
    left == 1
}

fn broken() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        "the scripted transport failed",
    )
}

impl Transport for Failing {
    type Reader = FailingReader;
    type Writer = FailingWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.inner.split();
        (
            FailingReader {
                inner: reader,
                countdown: Arc::clone(&self.countdown),
                armed: self.on_read,
            },
            FailingWriter {
                inner: writer,
                countdown: self.countdown,
                armed: !self.on_read,
            },
        )
    }
}

impl TransportRead for FailingReader {
    fn read(&mut self, buf: BytesMut) -> impl Future<Output = (io::Result<usize>, BytesMut)> {
        let failed = spent(&self.countdown, self.armed);
        let inner = self.inner.read(buf);
        async move {
            let (result, buf) = inner.await;
            if failed {
                (Err(broken()), buf)
            } else {
                (result, buf)
            }
        }
    }
}

impl TransportWrite for FailingWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
        let failed = spent(&self.countdown, self.armed);
        let inner = self.inner.write(buf);
        async move {
            let (result, buf) = inner.await;
            if failed {
                (Err(broken()), buf)
            } else {
                (result, buf)
            }
        }
    }

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // Election passes straight through: whether the borrowed path is offered is the inner
        // duplex's shape to decide, so `None` from it is `None` here and the driver takes
        // another path. Note that the inner duplex performs a borrowed write *at
        // construction* and returns a ready future — long-standing behaviour the driver
        // accommodates by never probing this election speculatively, unlike the vectored one.
        // So it is only the countdown that is deferred into the returned future here, and the
        // reason is narrower than on the vectored path: it keeps the accounting in one place
        // across both, so a countdown reads the same number of operations either way.
        let countdown = Arc::clone(&self.countdown);
        let armed = self.armed;
        let inner = self.inner.write_borrowed(data)?;
        Some(async move {
            let failed = spent(&countdown, armed);
            let written = inner.await;
            if failed { Err(broken()) } else { written }
        })
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // As with the borrowed path, the election is the inner duplex's to make and the
        // countdown is spent only when the write is actually performed. This matters more
        // here: the driver probes the vectored election by building this future with an empty
        // region list and dropping it unpolled, so spending at construction would charge an
        // operation for a write that never happened and shift the failure onto the wrong one.
        let countdown = Arc::clone(&self.countdown);
        let armed = self.armed;
        let inner = self.inner.write_vectored(regions)?;
        Some(async move {
            let failed = spent(&countdown, armed);
            let written = inner.await;
            if failed { Err(broken()) } else { written }
        })
    }
}

/// A transport whose writes are invisible to the peer until [`TransportWrite::commit`].
///
/// This is the shape of a `tokio::io::BufWriter`/`BufStream`: `write` only fills a
/// user-space buffer, and the octets reach the peer solely when that buffer is flushed.
/// It exists to pin the driver's flush contract — that the driver commits produced octets
/// before it ever awaits the peer. Drop the driver's [`TransportWrite::commit`] call and
/// an exchange over this transport hangs, because the request never leaves the buffer.
///
/// The reading half and the peer are an ordinary [`Duplex`]; only the writing half buffers.
#[derive(Debug)]
pub struct Buffering {
    inner: Duplex,
}

/// A [`Buffering`] transport and its ordinary [`Duplex`] peer.
pub fn buffering() -> (Buffering, Duplex) {
    let (one, two) = duplex(false);
    (Buffering { inner: one }, two)
}

/// The writing half of a [`Buffering`] transport.
#[derive(Debug)]
pub struct BufferingWriter {
    inner: DuplexWriter,
    /// Octets written but not yet flushed to the peer — the user-space buffer a
    /// `BufWriter` would keep.
    buffer: Vec<u8>,
}

impl Transport for Buffering {
    type Reader = DuplexReader;
    type Writer = BufferingWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.inner.split();
        (
            reader,
            BufferingWriter {
                inner: writer,
                buffer: Vec::new(),
            },
        )
    }
}

impl BufferingWriter {
    /// How many octets are buffered but not yet flushed to the peer.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

impl TransportWrite for BufferingWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
        // Buffered, not sent: the octets stay here until `commit`, exactly as a buffering
        // wrapper's `write` fills its buffer without touching the socket.
        self.buffer.extend_from_slice(&buf);
        let written = buf.len();
        core::future::ready((Ok(written), buf))
    }

    // `write_borrowed` is deliberately left at its default: this transport exercises the
    // owned/coalesced path, and the point it makes is about `commit`, not about borrowing.

    async fn commit(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let data = core::mem::take(&mut self.buffer);
        let (result, _buf) = self.inner.write(Bytes::from(data)).await;
        result.map(|_| ())
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
    let mut session = super::driver::client_session(&super::config::Config::default())
        .expect("building a client session");
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

/// The read-buffer pool's current size, and the largest it has ever reached.
///
/// The pool lives inside the driver future and reaches nothing else, so its settling to a
/// fixed size is a claim that can only be observed through a gauge — these two hand that
/// gauge to a test without widening the public surface.
pub fn pool_size<B>(handle: &super::client::SendRequest<B>) -> usize {
    handle.pool_size()
}

/// See [`pool_size`].
pub fn pool_high_water<B>(handle: &super::client::SendRequest<B>) -> usize {
    handle.pool_high_water()
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
