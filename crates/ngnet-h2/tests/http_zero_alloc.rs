//! Steady-state driver passes allocate nothing attributable to the crate (Spec SC-017),
//! and the two write strategies drain the session as their contract promises (SC-019).
//!
//! The sans-I/O sibling of this file, `zero_alloc.rs`, proves the receive path allocates
//! nothing by handing a handler borrowed slices. The async layer cannot stop there: a body
//! chunk outlives the `recv` call that produced it — the caller polls for it later — so it
//! reaches the caller as an owned `Bytes`, and the driver's work is spread across the
//! suspension points of a future rather than confined to one call. That is why this harness
//! exists separately and why `zero_alloc.rs`'s `count_allocations(impl FnOnce())` cannot be
//! reused (see its comments at lines ~17 and ~66): the counter is a thread-local, so the
//! future must be polled on the very thread that armed it, and the measured window must
//! cover whole driver passes rather than a single call.
//!
//! # What is measured, and what is deliberately excluded
//!
//! The window covers *steady state*: a connection is driven through dozens of identical
//! passes to warm up — session setup, the stream's registry entry, the read-buffer pool's
//! growth all happen here — and only then is the counter armed for further identical
//! passes. Per-stream setup is therefore excluded by construction: it occurs once, before
//! the window, not inside it. This is the honest reading of "steady-state frame processing
//! allocates nothing": the recurring cost of moving frames, not the one-off cost of
//! standing a stream up.
//!
//! # The counting allocator is duplicated on purpose
//!
//! A `#[global_allocator]` is a per-binary choice, so this file carries its own copy of the
//! `Counting` allocator and thread-local cells rather than sharing `zero_alloc.rs`'s. The
//! duplication is intended: there is no way for two integration-test binaries to share one.
//! Note also that libnghttp2 allocates through C `malloc`/`free`, which never reaches this
//! allocator — so what it counts is precisely this crate's own Rust allocations, which is
//! exactly the attribution SC-017 asks for.
//!
//! # Answered: which drain strategy should `TokioWriter` elect?
//!
//! This section was written as an open question — whether the borrowed path was the right
//! default for `TokioWriter` — and deferred, because the two shapes then available each
//! bought one virtue at the other's expense and the harness could not say which mattered
//! more. A third shape settles it, and this section now records the answer rather than the
//! question.
//!
//! Measured here, per steady-state pass, for each write shape and each of two workloads: a
//! client upload, whose blocks are all full-sized `DATA` frames, and a multiplexed trickle
//! of eight long-lived streams, whose blocks are all small. The two are not variations on
//! one measurement — they are the two ends of the traffic this library sees, and the shapes
//! rank differently on each, which is exactly why one of them alone would have answered the
//! question wrongly.
//!
//! A *shape* is no longer one thing the transport declares. It is a pair: what the transport
//! can do — gather natively, or only through `write_vectored`'s looping default — and what the
//! h2 layer decided, through [`WritePolicy`]. The rows below name both.
//!
//! The allocation and write columns are asserted by the tests named in the final column;
//! the parenthesised counts are illustrative — they are the values observed at the default
//! 64 KiB window and eight streams, but the tests pin the *properties* rather than those
//! incidental numbers, which move with the window size, the stream count and `bytes`'
//! growth policy.
//!
//! | shape (transport × policy) | upload: allocations / writes | multiplexed: allocations / writes | pinned by |
//! |-------|------------------------------|-----------------------------------|-----------|
//! | `CoalescedShape` — any readiness transport, gathering **off** | `0` / `1` | `0` / `1` | `the_owned_write_path_coalesces_a_pass_into_one_write`, `the_owned_write_path_reuses_its_coalescing_buffer` |
//! | `EmulatedShape` — emulating transport, gathering on | `0` / one per frame (4) | `0` / `1` | `steady_state_send_allocates_nothing_on_the_borrowed_path`, `emulated_gathering_costs_no_more_writes_than_native_on_an_upload` |
//! | `GatheredShape` — natively-gathering transport, gathering on | `0` / one per frame (4) | `0` / `1` | `steady_state_send_allocates_nothing_on_the_vectored_path`, `steady_state_multiplexed_send_allocates_nothing_on_the_vectored_path`, `a_multiplexed_pass_costs_one_write_natively_and_under_emulation_alike`, `the_vectored_write_path_writes_once_per_large_block_and_no_more` |
//! | `RegionShape` — completion transport | `0` / `1` | — | `the_owned_region_write_path_coalesces_a_pass_into_one_write`, `steady_state_send_allocates_nothing_on_the_owned_region_path` |
//!
//! # The emulated row used to read `513`
//!
//! Before the write policy moved to the h2 layer there was a fourth *drain*, `PerRegion`,
//! which wrote each session block on its own — 513 writes on the multiplexed pass. It is gone,
//! and with it that number. What replaced it is not a cheaper drain but the absence of one:
//! the driver accumulates sub-threshold blocks into a single region *before* any write
//! happens, so a transport that can only emulate gathering is handed one region and loops
//! once. Its cost is set by the regions the driver offers, never by the blocks the session
//! produced, which is the entire reason mandatory gathering is affordable.
//!
//! The `GatheredShape` and `RegionShape` rows are unchanged from before that move, deliberately
//! and to the digit: a real `TcpStream` and a completion transport must not pay anything for a
//! decision that was taken away from them.
//!
//! The owned-region row carries no multiplexed column: this file's push-model workload never
//! hands over a payload, so the completion path coalesces every block into its minting
//! buffer and looks exactly like the coalesced shape from here — one write per pass, no
//! allocation. Its distinguishing property, that a *handed-over* payload rides uncopied in
//! its own region, needs a shared body to exercise and is proven in `http_shared_body.rs`.
//!
//! All four shapes reach zero steady-state allocation, but they arrived there at different
//! times and for different reasons, and the history is worth keeping because the table above
//! was once the argument for a design decision.
//!
//! The owned shape used to allocate every pass — four times on an upload, twelve on a
//! multiplexed pass — and that recurring cost was the stated reason the tokio adapter took
//! the borrowed path instead. It turned out not to be inherent. `flush` was building its
//! coalescing buffer as a local and handing the whole allocation to the transport with
//! `freeze()`, so each pass started from nothing; hoisting the buffer and handing over
//! `split().freeze()` lets `bytes` reclaim the capacity, and the cost simply went away. What
//! remains inherent to that shape is the *copy* of every outgoing octet, which the transport
//! taking ownership genuinely requires.
//!
//! So the column that still separates the three is the write count, and it is a syscall
//! count — which is what the benchmarks in `crates/ngnet-h2-bench` measure as the dominant
//! cost on a real socket. The borrowed shape pays one per block: four on an upload, and 513
//! on multiplexed traffic.
//!
//! The gathering shape does not make that trade. It costs no allocation on either workload
//! and the *lower* of the two write counts on both, because the two things being traded were
//! never actually in tension: a pass needs one block from the session live at a time, and a
//! gathering write can carry that block beside memory the driver already owns. So the small
//! blocks accumulate into a buffer that is reused pass after pass — which is why the
//! allocation column stays at zero, and what
//! `steady_state_multiplexed_send_allocates_nothing_on_the_vectored_path` exists to pin —
//! and a large block rides out beside that accumulation without being copied.
//!
//! So the answer to the question this section used to ask is: neither of the two shapes it
//! was choosing between. `TokioWriter` elects the gathering path, and falls back to the
//! borrowed one only where `is_write_vectored()` reports that the underlying stream would
//! emulate `writev` by copying — in which case the emulation would reintroduce exactly the
//! copy the strategy exists to avoid, and one write per block is the better bargain.
//!
//! The crate's headline commitment — steady-state zero allocation — was once reachable only
//! on the borrowed path, and that fact was the argument for the tokio adapter taking it. It
//! is now reachable on all three, the owned path included: what remained there was never the
//! allocation but the *copy* of every outgoing octet, which a transport taking ownership
//! genuinely requires and which no reuse can remove.
//!
#![cfg(feature = "http")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Wake;

use core::future::{Future, poll_fn};
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};

use http_body::Body;
use ngnet_h2::http::testing::{
    Empty, Full, bytes_crate as bytes, http_body_crate as http_body, http_crate as http,
    pool_high_water, pool_size,
};
use ngnet_h2::http::transport::{
    BorrowedWrite, Completion, Readiness, RegionWrite, Transport, TransportRead, TransportWrite,
};
use ngnet_h2::http::{Config, WritePolicy};

/// A recording transport that gathers natively — the fast path a real `TcpStream` takes.
struct Native;

/// A recording transport that reaches gathering only through `write_vectored`'s provided
/// default, which loops the offer through `write_borrowed` one region at a time.
struct Emul;

/// A recording completion transport, which writes owned regions.
struct Owned;

/// A shape a measured run uses: a transport behaviour paired with the [`WritePolicy`] the h2
/// layer is configured with.
///
/// Under the strategy-election design one marker named both, because the transport declared
/// the drain. It no longer does — the transport declares only its I/O model and whether it
/// gathers natively, and the policy is the h2 layer's — so a shape is a pair, and this trait
/// is what lets a run still be written as one type parameter.
trait Shape {
    /// The transport behaviour marker the run is measured over.
    type Half;
    /// The policy the connection is configured with.
    const POLICY: WritePolicy;
}

/// Native transport, gathering on: one `write_vectored` per pass.
struct GatheredShape;
/// Native transport, gathering off: one owned `write` per pass, every octet copied.
struct CoalescedShape;
/// Emulating transport, gathering on: the default's loop, one `write_borrowed` per region.
struct EmulatedShape;
/// Completion transport: one `write_regions` per pass.
struct RegionShape;

impl Shape for GatheredShape {
    type Half = Native;
    const POLICY: WritePolicy = WritePolicy::Gathered;
}

impl Shape for CoalescedShape {
    type Half = Native;
    const POLICY: WritePolicy = WritePolicy::Coalesced;
}

impl Shape for EmulatedShape {
    type Half = Emul;
    const POLICY: WritePolicy = WritePolicy::Gathered;
}

impl Shape for RegionShape {
    type Half = Owned;
    const POLICY: WritePolicy = WritePolicy::Gathered;
}
use ngnet_h2::{
    BytesBody, FrameType, Header, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

// The counting allocator, duplicated from `zero_alloc.rs` on purpose: a `#[global_allocator]`
// is a per-binary choice and cannot be shared between integration-test binaries. Counting is
// per-thread, armed explicitly, so a sibling test running in parallel cannot charge its
// allocations to this one's window. `Cell` with a const initialiser needs no drop glue, so
// reaching it from inside the allocator cannot recurse into allocation.
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

fn record_allocation() {
    // `try_with`, not `with`: during thread teardown the thread-local may be gone, and an
    // allocation then must not panic.
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        }
    });
}

// SAFETY: every method forwards to the system allocator unchanged; the counter is
// incidental and never affects the returned pointer.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn arm() {
    ALLOCATIONS.with(|c| c.set(0));
    COUNTING.with(|c| c.set(true));
}

fn disarm() -> usize {
    COUNTING.with(|c| c.set(false));
    ALLOCATIONS.with(Cell::get)
}

/// A waker that records nothing but its own firing.
///
/// The driver is stepped by hand, one poll at a time, so nothing here needs to reschedule
/// it; the waker exists only because `poll` demands one. A no-op waker would do, but a real
/// `Wake` implementation keeps the type honest.
struct Flag(std::sync::atomic::AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// One poll of a future on this thread.
///
/// This is the whole reason the harness is bespoke: the allocation counter is a
/// thread-local, so the future has to be polled on the thread that armed it. A
/// thread-spawning executor would poll it elsewhere and the counter would follow the wrong
/// thread — the trap `zero_alloc.rs` documents.
fn step<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

// ----- an allocation-free in-memory transport -----
//
// The `Duplex` in `testing.rs` would serve, but it allocates as it grows its pipes, which
// would show up in the very window this harness arms. This one pre-reserves both pipes and
// never grows them, so the only allocations the counter sees are the driver's own.

#[derive(Default)]
struct Pipe {
    buf: VecDeque<u8>,
    closed: bool,
}

type Wire = Rc<RefCell<Pipe>>;

/// A tally kept beside the writer, since splitting the transport moves the writer out of
/// reach yet its per-pass write count is exactly what SC-019 asserts on.
#[derive(Default)]
struct Meter {
    writes: usize,
    bytes: usize,
}

/// Which of the four drain strategies a recording transport advertises, expressed as the
/// writer's strategy marker.
///
/// The strategy is fixed when the transport is built, exactly as a real transport's is: a
/// writer carries exactly one strategy in its type, so which path the driver takes is settled
/// by the marker `S` at construction rather than by a run-time flag. [`Coalesced`] overrides
/// nothing and coalesces each pass into one owned write; [`PerRegion`] overrides
/// [`BorrowedWrite`] for one write per session block; [`Gathering`] overrides [`VectoredWrite`]
/// to gather small blocks with the driver's own buffer; [`OwnedRegions`] overrides
/// [`RegionWrite`] for the completion strategy — one gathering write over an owned
/// `Vec<Bytes>`.
struct Recording<S> {
    inbound: Wire,
    outbound: Wire,
    meter: Rc<RefCell<Meter>>,
    _marker: PhantomData<S>,
}

struct RecReader {
    inbound: Wire,
}

struct RecWriter<S> {
    outbound: Wire,
    meter: Rc<RefCell<Meter>>,
    _marker: PhantomData<S>,
}

impl<S> RecWriter<S> {
    fn record(&self, data: &[u8]) {
        let mut meter = self.meter.borrow_mut();
        meter.writes += 1;
        meter.bytes += data.len();
        drop(meter);
        self.outbound.borrow_mut().buf.extend(data.iter().copied());
    }

    /// The shared body of every strategy's [`write`](TransportWrite::write): one coalesced
    /// owned write, metered and delivered.
    async fn do_write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        self.record(&buf);
        let written = buf.len();
        (Ok(written), buf)
    }

    /// The shared body of the borrowed path: one metered write per block, nothing copied.
    ///
    /// A real fallback rather than a stub: the [`Gathering`] writer supplies it too, since
    /// [`VectoredWrite`] requires [`BorrowedWrite`] and the driver writes here when a stream
    /// does not really scatter-gather. In these tests `gathers` is left at its `true` default,
    /// so the [`Gathering`] writer never reaches this — but it must still be live.
    fn do_write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        self.record(data);
        core::future::ready(Ok(data.len()))
    }

    /// The shared body of the vectored path: an inert future that meters and delivers when
    /// polled.
    ///
    /// Nothing is recorded at construction, and no octet moves: the driver may build one of
    /// these speculatively and drop it unpolled, so recording here would charge a phantom
    /// write and a phantom pile of octets on every pass. An `async` block is inert until
    /// polled, which is exactly the property needed.
    async fn do_write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> io::Result<usize> {
        let mut written = 0;
        for region in regions {
            written += region.len();
        }
        // One call, one write, however many regions it gathered: the whole point of the
        // strategy is that the gathering is free and the syscall is what costs.
        let mut meter = self.meter.borrow_mut();
        meter.writes += 1;
        meter.bytes += written;
        drop(meter);
        let mut outbound = self.outbound.borrow_mut();
        for region in regions {
            outbound.buf.extend(region.iter().copied());
        }
        drop(outbound);
        Ok(written)
    }

    /// The shared body of the owned-region path: one metered write over the owned list.
    ///
    /// The ownership round-trip a completion API needs is visible here: the `Vec` comes in and
    /// goes back out, so the driver can reuse its allocation.
    async fn do_write_regions(&mut self, regions: Vec<Bytes>) -> (io::Result<usize>, Vec<Bytes>) {
        let written: usize = regions.iter().map(Bytes::len).sum();
        let mut meter = self.meter.borrow_mut();
        meter.writes += 1;
        meter.bytes += written;
        drop(meter);
        let mut outbound = self.outbound.borrow_mut();
        for region in &regions {
            outbound.buf.extend(region.iter().copied());
        }
        drop(outbound);
        (Ok(written), regions)
    }
}

/// Emits the `Transport` impl for a [`Recording`] over one strategy marker.
///
/// One impl per marker rather than a blanket one over `S`: a blanket impl cannot name a
/// concrete `Writer` that is itself `TransportWrite` for a generic `S`, the same limitation
/// the crate's own testing duplex works around.
macro_rules! recording_transport {
    ($marker:ty) => {
        impl Transport for Recording<$marker> {
            type Reader = RecReader;
            type Writer = RecWriter<$marker>;

            fn split(self) -> (RecReader, RecWriter<$marker>) {
                (
                    RecReader {
                        inbound: self.inbound,
                    },
                    RecWriter {
                        outbound: self.outbound,
                        meter: self.meter,
                        _marker: PhantomData,
                    },
                )
            }
        }
    };
}

recording_transport!(Native);
recording_transport!(Emul);
recording_transport!(Owned);

impl TransportRead for RecReader {
    async fn read(&mut self, mut buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        let inbound = Rc::clone(&self.inbound);
        let available = poll_fn(|_cx: &mut Context<'_>| {
            let pipe = inbound.borrow();
            if pipe.buf.is_empty() {
                if pipe.closed {
                    return Poll::Ready(0usize);
                }
                return Poll::Pending;
            }
            Poll::Ready(pipe.buf.len())
        })
        .await;
        if available == 0 {
            return (Ok(0), buf);
        }
        let room = buf.capacity().saturating_sub(buf.len());
        let take = available.min(room.max(1));
        buf.extend(inbound.borrow_mut().buf.drain(..take));
        (Ok(take), buf)
    }
}

/// Emits the `TransportWrite` impl for a [`RecWriter`] over one strategy marker.
macro_rules! recording_transport_write {
    ($marker:ty, $model:ty) => {
        impl TransportWrite for RecWriter<$marker> {
            type Model = $model;

            fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
                self.do_write(buf)
            }
        }
    };
}

recording_transport_write!(Native, Readiness);
recording_transport_write!(Emul, Readiness);
recording_transport_write!(Owned, Completion);

/// Emits the `BorrowedWrite` impl for the readiness behaviours.
///
/// Emitted for both, and for `Emul` this is the *whole* impl: it takes `write_vectored`'s
/// provided default, so its gathering writes loop back through this method one region at a
/// time. `Native` overrides the default below.
macro_rules! recording_borrowed_write {
    ($marker:ty) => {
        impl BorrowedWrite for RecWriter<$marker> {
            fn write_borrowed<'w>(
                &'w mut self,
                data: &'w [u8],
            ) -> impl Future<Output = io::Result<usize>> + 'w {
                self.do_write_borrowed(data)
            }
        }
    };
}

recording_borrowed_write!(Emul);

/// The natively-gathering writer, spelled out rather than macro-emitted because it is the one
/// that overrides `write_vectored`. Deleting that override would not fail to compile — it
/// would silently fall back to the loop — so the tests below have to catch it instead, which
/// is what `the_native_override_costs_fewer_writes_than_emulation` is for.
impl BorrowedWrite for RecWriter<Native> {
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        self.do_write_borrowed(data)
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        self.do_write_vectored(regions)
    }
}

impl RegionWrite for RecWriter<Owned> {
    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<Bytes>)> {
        self.do_write_regions(regions)
    }
}

// ----- a peer that answers, driven by hand -----

#[derive(Default)]
struct PeerCtx {
    pending: Vec<i32>,
}

fn peer_session() -> Session<PeerCtx> {
    SessionBuilder::<PeerCtx>::server()
        .on_header(|_c: &mut PeerCtx, _f, _n: &[u8], _v: &[u8]| HeaderAction::Continue)
        .on_frame(|c: &mut PeerCtx, frame| {
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && frame.category() == Some(HeaderCategory::Request)
            {
                c.pending.push(frame.stream_id().get());
            }
        })
        .build()
        .expect("peer session")
}

/// Feeds the client's octets to the peer, answers any new request with a body, and posts
/// the peer's octets back.
///
/// Runs outside the armed window, so its own allocations never reach the counter. The peer
/// is stepped by hand rather than by [`ngnet_h2::http::testing::serve`] because the counter
/// needs the client polled on this thread between exchanges, not run to completion.
fn pump_peer(peer: &mut Session<PeerCtx>, ctx: &mut PeerCtx, c2s: &Wire, s2c: &Wire, body: usize) {
    let input: Vec<u8> = c2s.borrow_mut().buf.drain(..).collect();
    if !input.is_empty() {
        peer.recv(&input, ctx).expect("peer recv");
    }
    let pending: Vec<i32> = core::mem::take(&mut ctx.pending);
    for stream in pending {
        peer.submit_response_with_body(
            StreamId::new(stream),
            &[Header::new(":status", "200")],
            BytesBody::new(vec![b'x'; body]),
        )
        .expect("submit response");
    }
    let mut out = s2c.borrow_mut();
    while let Some(block) = peer.send(ctx).expect("peer send") {
        out.buf.extend(block.iter().copied());
    }
}

/// Feeds the client's octets to the peer and posts the peer's octets back, but sends no
/// response body — the upload measurement wants nothing but flow-control credit travelling
/// back, so the peer's auto-generated `WINDOW_UPDATE`s are all that returns.
fn pump_absorb(peer: &mut Session<PeerCtx>, ctx: &mut PeerCtx, c2s: &Wire, s2c: &Wire) {
    let input: Vec<u8> = c2s.borrow_mut().buf.drain(..).collect();
    if !input.is_empty() {
        peer.recv(&input, ctx).expect("peer recv");
    }
    ctx.pending.clear();
    let mut out = s2c.borrow_mut();
    while let Some(block) = peer.send(ctx).expect("peer send") {
        out.buf.extend(block.iter().copied());
    }
}

async fn next_frame(
    body: &mut ngnet_h2::http::IncomingBody,
) -> Option<Result<http_body::Frame<Bytes>, ngnet_h2::http::Error>> {
    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

fn wire(capacity: usize) -> Wire {
    let pipe = Rc::new(RefCell::new(Pipe::default()));
    pipe.borrow_mut().buf.reserve(capacity);
    pipe
}

const WARMUP: usize = 40;
const MEASURE: usize = 20;
/// HTTP/2's default maximum frame payload. Body larger than this spans several DATA frames,
/// which is what makes "one write however many frames are pending" a claim with teeth.
const MAX_FRAME: usize = 16 * 1024;

/// One measured pass of a steady-state receive: warm the connection up, then per pass return
/// how many allocations the connection's poll and the body drainer's poll each cost.
struct Receive {
    connection: Vec<usize>,
    drainer: Vec<usize>,
    pool: usize,
    pool_high_water: usize,
    high_water_before: usize,
}

fn run_receive() -> Receive {
    let c2s = wire(1 << 20);
    let s2c = wire(1 << 20);
    let transport = Recording::<Native> {
        inbound: Rc::clone(&s2c),
        outbound: Rc::clone(&c2s),
        meter: Rc::new(RefCell::new(Meter::default())),
        _marker: PhantomData,
    };

    let (requests, connection) =
        ngnet_h2::http::handshake::<Recording<Native>, Empty>(transport).expect("handshake");

    let mut peer = peer_session();
    let mut ctx = PeerCtx::default();

    let response = requests.send_request(
        http::Request::builder()
            .uri("http://example.test/")
            .body(Empty)
            .expect("request"),
    );

    let flag = Arc::new(Flag(std::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));

    // The response body is one long stream; each pass processes DATA frames identically, so
    // the measured window is pure steady-state frame processing with the stream's setup left
    // behind in the warm-up.
    const BODY: usize = 8 << 20;
    let drainer = async move {
        let mut body = response.await.expect("response").into_body();
        while let Some(frame) = next_frame(&mut body).await {
            frame.expect("body frame");
        }
    };

    let mut connection = core::pin::pin!(connection);
    let mut drainer = core::pin::pin!(drainer);

    for _ in 0..WARMUP {
        pump_peer(&mut peer, &mut ctx, &c2s, &s2c, BODY);
        let _ = step(connection.as_mut(), &waker);
        let _ = step(drainer.as_mut(), &waker);
    }

    let high_water_before = pool_high_water(&requests);
    let mut result = Receive {
        connection: Vec::with_capacity(MEASURE),
        drainer: Vec::with_capacity(MEASURE),
        pool: 0,
        pool_high_water: 0,
        high_water_before,
    };
    for _ in 0..MEASURE {
        pump_peer(&mut peer, &mut ctx, &c2s, &s2c, BODY);
        arm();
        let _ = step(connection.as_mut(), &waker);
        result.connection.push(disarm());
        arm();
        let _ = step(drainer.as_mut(), &waker);
        result.drainer.push(disarm());
    }
    result.pool = pool_size(&requests);
    result.pool_high_water = pool_high_water(&requests);
    result
}

/// The per-pass measurements of a steady-state client upload on one write shape.
struct Send {
    allocations: Vec<usize>,
    writes: Vec<usize>,
    bytes: Vec<usize>,
}

fn run_send<S>() -> Send
where
    S: Shape,
    Recording<S::Half>: Transport,
{
    let c2s = wire(1 << 22);
    let s2c = wire(1 << 20);
    let meter = Rc::new(RefCell::new(Meter::default()));
    let transport = Recording::<S::Half> {
        inbound: Rc::clone(&s2c),
        outbound: Rc::clone(&c2s),
        meter: Rc::clone(&meter),
        _marker: PhantomData,
    };

    // The drain is the h2 layer's choice now, so it arrives through `Config` rather than
    // being read off the transport the run was built over.
    let (requests, connection) = ngnet_h2::http::handshake_with::<Recording<S::Half>, Full>(
        transport,
        Config::default().write_policy(S::POLICY),
    )
    .expect("handshake");

    let mut peer = peer_session();
    let mut ctx = PeerCtx::default();

    // A body far larger than the flow-control window, so every measured pass has more to
    // send than one window admits and the send path stays busy throughout.
    const BODY: usize = 32 << 20;
    let response = requests.send_request(
        http::Request::builder()
            .uri("http://example.test/")
            .body(Full::new(vec![b'x'; BODY]))
            .expect("request"),
    );

    let flag = Arc::new(Flag(std::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));

    let drainer = async move {
        let _ = response.await;
    };

    let mut connection = core::pin::pin!(connection);
    let mut drainer = core::pin::pin!(drainer);

    for _ in 0..WARMUP {
        pump_absorb(&mut peer, &mut ctx, &c2s, &s2c);
        let _ = step(connection.as_mut(), &waker);
        let _ = step(drainer.as_mut(), &waker);
    }

    let mut result = Send {
        allocations: Vec::with_capacity(MEASURE),
        writes: Vec::with_capacity(MEASURE),
        bytes: Vec::with_capacity(MEASURE),
    };
    for _ in 0..MEASURE {
        pump_absorb(&mut peer, &mut ctx, &c2s, &s2c);
        *meter.borrow_mut() = Meter::default();
        arm();
        let _ = step(connection.as_mut(), &waker);
        result.allocations.push(disarm());
        let snapshot = meter.borrow();
        result.writes.push(snapshot.writes);
        result.bytes.push(snapshot.bytes);
        drop(snapshot);
        let _ = step(drainer.as_mut(), &waker);
    }
    result
}

#[test]
fn steady_state_receive_allocates_nothing() {
    let measured = run_receive();

    assert!(
        measured.connection.iter().all(|&count| count == 0),
        "a steady-state receive pass must allocate nothing attributable to the crate, saw {:?}",
        measured.connection,
    );
    assert!(
        measured.drainer.iter().all(|&count| count == 0),
        "draining a body chunk across suspension points must allocate nothing, saw {:?}",
        measured.drainer,
    );
}

#[test]
fn the_read_buffer_pool_settles_to_a_fixed_size() {
    let measured = run_receive();

    assert_eq!(
        measured.pool, measured.pool_high_water,
        "a settled pool holds as many buffers as it ever has: nothing was dropped and left \
         to be re-grown",
    );
    assert_eq!(
        measured.high_water_before, measured.pool_high_water,
        "the pool must reach its high-water mark during warm-up and not grow once measurement \
         begins: a larger mark afterwards means a buffer was grown inside the window, which is \
         an allocation. Comparing the pre-window mark against the final one — rather than a \
         running max against itself — is what makes this catch that growth.",
    );
    assert!(
        measured.pool > 0,
        "a connection that has processed a whole streamed body must be recycling buffers, \
         not standing a fresh one up each pass",
    );
}

#[test]
fn steady_state_send_allocates_nothing_on_the_borrowed_path() {
    let measured = run_send::<EmulatedShape>();

    assert!(
        measured.allocations.iter().all(|&count| count == 0),
        "a steady-state send pass on the borrowed path must allocate nothing, saw {:?}",
        measured.allocations,
    );
}

#[test]
fn the_owned_write_path_coalesces_a_pass_into_one_write() {
    let measured = run_send::<CoalescedShape>();

    assert!(
        measured.writes.iter().all(|&count| count == 1),
        "the owned path issues exactly one write per pass however many frames are pending, \
         saw {:?}",
        measured.writes,
    );
    // Without this the single-write claim would be vacuous: one write is only remarkable if
    // there was more than one frame's worth to write.
    assert!(
        measured.bytes.iter().all(|&bytes| bytes > MAX_FRAME),
        "each measured pass must carry more than one frame, so the single write genuinely \
         coalesced several, saw byte counts {:?}",
        measured.bytes,
    );
}

#[test]
fn emulated_gathering_costs_no_more_writes_than_native_on_an_upload() {
    // The second half of the affordability argument, on the workload the multiplexed test
    // does not cover. An upload's blocks are full-sized `DATA` frames, so each takes the
    // large-block path and the pass is several writes rather than one — on *both* shapes.
    //
    // **This test replaced `the_borrowed_write_path_writes_each_block_separately`.** That
    // name described the old `PerRegion` drain, which is gone: there is no longer a drain that
    // writes blocks separately, only a transport that loops over whatever regions the driver
    // offers. Since the driver offers one region per large block either way, emulation lands
    // on the same count as `writev`, and the comparison worth pinning is that equality rather
    // than a difference.
    let native = run_send::<GatheredShape>();
    let emulated = run_send::<EmulatedShape>();
    let coalesced = run_send::<CoalescedShape>();

    // Same traffic, driven identically: the shapes differ only in how they drain it.
    assert_eq!(
        emulated.bytes, coalesced.bytes,
        "the shapes were not compared on the same traffic",
    );
    assert_eq!(
        emulated.bytes, native.bytes,
        "the shapes were not compared on the same traffic",
    );
    assert_eq!(
        emulated.writes, native.writes,
        "emulated gathering cost a different number of writes than native gathering on the \
         copying upload path, where the driver offers one region per large block; if these \
         diverge, the driver stopped accumulating and emulation is no longer bounded by the \
         offer",
    );
    assert!(
        emulated.writes.iter().all(|&count| count > 1),
        "an upload of several full-sized frames must cost several writes, or the comparison \
         above is between two trivial counts, saw {:?}",
        emulated.writes,
    );
    // A fixed cost of the traffic, not a growing one.
    let first = emulated.writes[0];
    assert!(
        emulated.writes.iter().all(|&count| count == first),
        "one write per frame is a fixed cost of the traffic, not a growing one, saw {:?}",
        emulated.writes,
    );
    // And turning gathering off really is the cheaper shape by syscall count here — the
    // honest cost of mandatory gathering, pinned rather than hidden.
    assert!(
        coalesced.writes.iter().all(|&count| count == 1),
        "with gathering off the pass coalesces into one write, saw {:?}",
        coalesced.writes,
    );
}

#[test]
fn the_owned_write_path_reuses_its_coalescing_buffer() {
    // This test used to assert the opposite — that the owned path allocates on every pass —
    // and that assertion was the stated justification for the tokio adapter preferring the
    // borrowed path. It was true, but it was never *inherent*: the cost came from `flush`
    // building its coalescing buffer as a local and handing the whole allocation away with
    // `freeze()`, so every pass began from nothing. Hoisting the buffer beside the gathering
    // one and handing the octets over with `split().freeze()` instead lets `bytes` reclaim
    // the capacity once the transport drops its handle, and the recurring cost disappears.
    //
    // What is pinned here is therefore the reuse, not a count. The owned path still copies
    // every outgoing octet — that is inherent, because the transport takes ownership — but it
    // must not reallocate to do so. A regression to `freeze()`, or to declaring the buffer
    // inside `flush`, would restore the per-pass allocation and fail here.
    let owned = run_send::<CoalescedShape>();
    let borrowed = run_send::<EmulatedShape>();

    assert!(
        owned.allocations.iter().all(|&count| count == 0),
        "the owned path must reuse its coalescing buffer rather than rebuild it per pass, \
         saw {:?}",
        owned.allocations,
    );
    // Kept as the control it always was: identical traffic, and the borrowed path — which
    // never had a coalescing buffer to reuse — is still zero. Without this, a change that
    // broke the measurement itself could pass the assertion above vacuously.
    assert!(
        borrowed.allocations.iter().all(|&count| count == 0),
        "the borrowed path carries the same traffic for no allocation, saw {:?}",
        borrowed.allocations,
    );
    // The two paths agree on allocation, so the thing that still separates them is the write
    // count. Asserted here so the comparison this test used to draw is not simply lost.
    assert!(
        owned.writes.iter().all(|&count| count == 1),
        "with gathering off the pass still coalesces into one write, saw {:?}",
        owned.writes,
    );
    assert!(
        borrowed.writes.iter().all(|&count| count > 1),
        "with gathering on an upload still pays one write per large block, saw {:?}",
        borrowed.writes,
    );
}

#[test]
fn the_owned_region_write_path_coalesces_a_pass_into_one_write() {
    // The completion strategy on push-model traffic: with no handed-over payload to ride
    // uncopied, every block is coalesced into the minting buffer and the whole pass reaches
    // the transport as a single owned region. So it looks exactly like the owned path from
    // the write-count side — one gathering write per pass — which is what this pins.
    let measured = run_send::<RegionShape>();

    assert!(
        measured.writes.iter().all(|&count| count == 1),
        "the owned-region path issues exactly one gathering write per pass however many \
         frames are pending, saw {:?}",
        measured.writes,
    );
    assert!(
        measured.bytes.iter().all(|&bytes| bytes > MAX_FRAME),
        "each measured pass must carry more than one frame, so the single write genuinely \
         coalesced several, saw byte counts {:?}",
        measured.bytes,
    );
}

#[test]
fn steady_state_send_allocates_nothing_on_the_owned_region_path() {
    // The completion counterpart of `the_owned_write_path_reuses_its_coalescing_buffer`: the
    // owned-region path mints each region by `split().freeze()`ing the minting buffer, and
    // `write_regions` returns the `Vec` so the transport's frozen handles are dropped before
    // the next pass. That leaves the minting buffer the unique owner of its allocation, so
    // `bytes` reclaims the capacity and the steady state allocates nothing. A regression to
    // `freeze()` in place of `split().freeze()`, or to building the buffers inside `flush`,
    // would restore a per-pass allocation and fail here.
    let measured = run_send::<RegionShape>();

    assert!(
        measured.allocations.iter().all(|&count| count == 0),
        "a steady-state send pass on the owned-region path must allocate nothing, saw {:?}",
        measured.allocations,
    );
}

// ----- the gathering path: one write for a multiplexed pass, and still no allocation -----
//
// The upload workload above is the wrong shape to prove what the gathering path is for. Its
// steady-state passes are nothing but full-sized DATA frames, every one of them over the
// driver's threshold, so the accumulation buffer is filled and drained within a single
// block's handling and its reuse across passes is never exercised at all. What the strategy
// exists for is the opposite traffic: a handful of streams each contributing a few dozen
// octets, which today costs one write per stream.
//
// The obvious way to produce that traffic — many short requests — would be a trap. Standing
// a stream up allocates, which this file says in its opening paragraphs and excludes from
// the claim by construction; an arm built on request churn would fail for that reason and
// say nothing whatever about the buffer. So the streams here are opened during warm-up and
// never closed, and each contributes one sub-threshold chunk at a time forever.

/// How many long-lived streams the multiplexed arm keeps open.
const STREAMS: usize = 8;

/// How much each of them contributes per chunk. Well below the driver's 256-octet threshold,
/// so every block it produces accumulates rather than being gathered on its own.
const TRICKLE: usize = 64;

/// A body that never ends and never blocks, handing over the same octets again and again.
///
/// `Bytes::slice` is a refcount bump over memory allocated once when the body was built, so
/// producing a chunk costs nothing the measured window can see. That matters more than it
/// looks: a body that allocated per chunk would charge the harness for its own scaffolding
/// and the assertion would be about this file rather than about the driver.
struct Trickle {
    source: Bytes,
}

impl Body for Trickle {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(Some(Ok(http_body::Frame::data(
            self.source.slice(..TRICKLE),
        ))))
    }

    fn is_end_stream(&self) -> bool {
        false
    }
}

/// Drives several long-lived streams that each trickle, and measures the same two things
/// per pass as [`run_send`]: what the connection allocated, and what it cost in writes.
fn run_multiplexed<S>() -> Send
where
    S: Shape,
    Recording<S::Half>: Transport,
{
    let c2s = wire(1 << 22);
    let s2c = wire(1 << 20);
    let meter = Rc::new(RefCell::new(Meter::default()));
    let transport = Recording::<S::Half> {
        inbound: Rc::clone(&s2c),
        outbound: Rc::clone(&c2s),
        meter: Rc::clone(&meter),
        _marker: PhantomData,
    };

    // The drain is the h2 layer's choice now, so it arrives through `Config` rather than
    // being read off the transport the run was built over.
    let (requests, connection) = ngnet_h2::http::handshake_with::<Recording<S::Half>, Trickle>(
        transport,
        Config::default().write_policy(S::POLICY),
    )
    .expect("handshake");

    let mut peer = peer_session();
    let mut ctx = PeerCtx::default();

    let source = Bytes::from(vec![b'x'; TRICKLE]);
    let responses: Vec<_> = (0..STREAMS)
        .map(|index| {
            requests.send_request(
                http::Request::builder()
                    .method(http::Method::POST)
                    .uri(format!("http://example.test/{index}"))
                    .body(Trickle {
                        source: source.clone(),
                    })
                    .expect("request"),
            )
        })
        .collect();

    let flag = Arc::new(Flag(std::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));

    // The peer never answers, so none of these ever complete; they are held and polled only
    // because dropping one would reset its stream, and a reset stream is exactly the churn
    // this arm is built to avoid.
    let drainer = async move {
        for response in responses {
            let _ = response.await;
        }
    };

    let mut connection = core::pin::pin!(connection);
    let mut drainer = core::pin::pin!(drainer);

    for _ in 0..WARMUP {
        pump_absorb(&mut peer, &mut ctx, &c2s, &s2c);
        let _ = step(connection.as_mut(), &waker);
        let _ = step(drainer.as_mut(), &waker);
    }

    let mut result = Send {
        allocations: Vec::with_capacity(MEASURE),
        writes: Vec::with_capacity(MEASURE),
        bytes: Vec::with_capacity(MEASURE),
    };
    for _ in 0..MEASURE {
        pump_absorb(&mut peer, &mut ctx, &c2s, &s2c);
        *meter.borrow_mut() = Meter::default();
        arm();
        let _ = step(connection.as_mut(), &waker);
        result.allocations.push(disarm());
        let snapshot = meter.borrow();
        result.writes.push(snapshot.writes);
        result.bytes.push(snapshot.bytes);
        drop(snapshot);
        let _ = step(drainer.as_mut(), &waker);
    }
    result
}

#[test]
fn steady_state_send_allocates_nothing_on_the_vectored_path() {
    // SC-015. The gathering path buys its single write with an accumulation buffer, and a
    // buffer that were reallocated each pass would have traded one recurring cost for
    // another. It is not: the driver holds it across passes and clears rather than drops it,
    // so once it has grown to what a pass needs it stops allocating entirely.
    let measured = run_send::<GatheredShape>();

    assert!(
        measured.allocations.iter().all(|&count| count == 0),
        "a steady-state send pass on the gathering path must allocate nothing, saw {:?}",
        measured.allocations,
    );
}

#[test]
fn steady_state_multiplexed_send_allocates_nothing_on_the_vectored_path() {
    // The claim that matters, and the one the body workload cannot make: passes whose blocks
    // all land *in* the accumulation buffer, so its reuse from one pass to the next is what
    // is being measured rather than its emptiness.
    let measured = run_multiplexed::<GatheredShape>();

    assert!(
        measured.allocations.iter().all(|&count| count == 0),
        "a steady-state multiplexed pass on the gathering path must allocate nothing, saw \
         {:?}",
        measured.allocations,
    );
    // Without this the claim would be vacuous: a buffer that is never written cannot be
    // shown not to reallocate.
    assert!(
        measured
            .bytes
            .iter()
            .all(|&bytes| bytes > TRICKLE * STREAMS),
        "each measured pass must carry more than one chunk from every stream, so the \
         accumulation is genuinely being filled, saw byte counts {:?}",
        measured.bytes,
    );
}

#[test]
fn a_multiplexed_pass_costs_one_write_natively_and_under_emulation_alike() {
    // The syscall property the whole change exists to deliver, measured against the traffic
    // it was measured to matter for. Every block a trickling stream produces is below the
    // threshold, so the pass accumulates all of them into a single region and pays for
    // exactly one write.
    //
    // **This assertion changed.** It used to read `borrowed.writes > STREAMS` — one write per
    // block, 513 of them — because the borrowed drain wrote region by region *without
    // accumulating first*. There is no such drain any more: accumulation is unconditional and
    // happens in the driver before any write, so a transport that only emulates gathering is
    // handed the same one-region offer the natively-gathering one is, and its loop runs
    // exactly once. That is the mechanism, and it is the whole argument that mandatory
    // gathering is affordable: emulation's cost is set by how many regions the driver
    // *offers*, not by how many blocks the session produced.
    let native = run_multiplexed::<GatheredShape>();
    let emulated = run_multiplexed::<EmulatedShape>();

    assert_eq!(
        native.bytes, emulated.bytes,
        "the two shapes were not compared on the same traffic",
    );
    assert!(
        native.writes.iter().all(|&count| count == 1),
        "a multiplexed pass of sub-threshold blocks costs one write however many streams \
         contributed, saw {:?}",
        native.writes,
    );
    assert_eq!(
        emulated.writes, native.writes,
        "emulated gathering cost more writes than native gathering on a pass the driver \
         accumulated into one region; if this fails, accumulation stopped happening before \
         the write and the emulation argument no longer holds",
    );
    // Not vacuous: the single write really did carry every stream's contribution, so "one
    // write" is a collapse of many blocks rather than a pass that had nothing to send.
    assert!(
        native.bytes.iter().all(|&bytes| bytes > TRICKLE * STREAMS),
        "each measured pass must carry more than one chunk from every stream for the \
         one-write claim to mean anything, saw {:?}",
        native.bytes,
    );
}

#[test]
fn the_vectored_write_path_writes_once_per_large_block_and_no_more() {
    // The other half of the strategy: a block big enough to be worth a syscall of its own
    // goes out uncopied, beside whatever has accumulated ahead of it rather than after it.
    // So an upload costs one write per DATA frame — the same count the borrowed path pays,
    // and for the same reason, but with the frame's header and any control frames riding
    // along instead of costing writes of their own.
    let vectored = run_send::<GatheredShape>();
    let borrowed = run_send::<EmulatedShape>();
    let owned = run_send::<CoalescedShape>();

    assert_eq!(
        vectored.bytes, borrowed.bytes,
        "the shapes were not compared on the same traffic",
    );
    assert_eq!(
        vectored.bytes, owned.bytes,
        "the shapes were not compared on the same traffic",
    );
    assert!(
        vectored.writes.iter().all(|&count| count > 1),
        "a pass of several full-sized frames is several writes on the gathering path too: \
         gathering avoids the copy, it does not avoid the syscall, saw {:?}",
        vectored.writes,
    );
    assert!(
        vectored
            .writes
            .iter()
            .zip(&borrowed.writes)
            .all(|(&gathered, &separate)| gathered <= separate),
        "gathering must never cost more writes than writing each block on its own, saw {:?} \
         against {:?}",
        vectored.writes,
        borrowed.writes,
    );
    let first = vectored.writes[0];
    assert!(
        vectored.writes.iter().all(|&count| count == first),
        "one write per frame is a fixed cost of the traffic, not a growing one, saw {:?}",
        vectored.writes,
    );
}

// ----- the server handler path: waking parked handlers allocates nothing -----
//
// SF-4. The server drains the set of woken handlers into a scratch buffer every pass, the
// same discipline the body path follows. This proves it: several handlers are started and
// parked, then woken repeatedly without any new stream, and the driver's drain-and-poll
// pass is measured to allocate nothing once the connection has warmed up.

/// A handler that never finishes, publishing the waker it was last polled with so the
/// harness can wake it again without opening a new stream.
struct Park {
    slot: WakerSlot,
}

/// The cell a parked handler publishes its waker into, shared with the harness.
type WakerSlot = Rc<RefCell<Option<Waker>>>;

impl Future for Park {
    type Output = http::Response<Empty>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cloning an `Arc`-backed waker is a refcount bump, not an allocation, so
        // republishing it every poll costs nothing the measured window would see.
        *self.slot.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

fn peer_client_session() -> Session<()> {
    SessionBuilder::<()>::client()
        .build()
        .expect("peer client session")
}

/// Feeds the server's octets to the peer and posts the peer's octets back. The peer never
/// answers anything — it only opens streams — so all that travels back is protocol
/// bookkeeping.
fn pump_client(peer: &mut Session<()>, to_server: &Wire, from_server: &Wire) {
    let input: Vec<u8> = from_server.borrow_mut().buf.drain(..).collect();
    if !input.is_empty() {
        peer.recv(&input, &mut ()).expect("peer recv");
    }
    let mut out = to_server.borrow_mut();
    while let Some(block) = peer.send(&mut ()).expect("peer send") {
        out.buf.extend(block.iter().copied());
    }
}

fn run_handlers() -> Vec<usize> {
    const HANDLERS: usize = 4;

    let to_server = wire(1 << 20);
    let from_server = wire(1 << 20);
    let transport = Recording::<Native> {
        inbound: Rc::clone(&to_server),
        outbound: Rc::clone(&from_server),
        meter: Rc::new(RefCell::new(Meter::default())),
        _marker: PhantomData,
    };

    let wakers: Rc<RefCell<Vec<WakerSlot>>> = Rc::new(RefCell::new(Vec::new()));
    let published = Rc::clone(&wakers);
    let connection = ngnet_h2::http::server::serve(
        transport,
        move |_request: http::Request<ngnet_h2::http::IncomingBody>| {
            let slot = Rc::new(RefCell::new(None));
            published.borrow_mut().push(Rc::clone(&slot));
            Park { slot }
        },
    )
    .expect("serve");

    let mut peer = peer_client_session();
    for _ in 0..HANDLERS {
        peer.submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/"),
        ])
        .expect("submit request");
    }

    let flag = Arc::new(Flag(std::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut connection = core::pin::pin!(connection);

    for _ in 0..WARMUP {
        pump_client(&mut peer, &to_server, &from_server);
        let _ = step(connection.as_mut(), &waker);
    }

    assert_eq!(
        wakers.borrow().len(),
        HANDLERS,
        "every request should have started a handler that parked",
    );

    let mut per_pass = Vec::with_capacity(MEASURE);
    for _ in 0..MEASURE {
        // Woken outside the window: the marking's own cost is not what this measures, only
        // the driver's drain of the woken set and the poll of each parked handler.
        for slot in wakers.borrow().iter() {
            if let Some(waker) = slot.borrow().as_ref() {
                waker.wake_by_ref();
            }
        }
        arm();
        let _ = step(connection.as_mut(), &waker);
        per_pass.push(disarm());
    }
    per_pass
}

#[test]
fn waking_parked_handlers_allocates_nothing() {
    let per_pass = run_handlers();

    assert!(
        per_pass.iter().all(|&count| count == 0),
        "draining and polling several woken handlers must allocate nothing in steady state, \
         saw {per_pass:?}",
    );
}

#[test]
fn the_counter_notices_a_deliberate_allocation() {
    // The whole harness rests on the counter seeing crate allocations; a counter that never
    // fires would pass every test above vacuously. This proves it fires.
    arm();
    let boxed = Box::new([0u8; 64]);
    core::hint::black_box(&boxed);
    let counted = disarm();
    assert!(
        counted >= 1,
        "the allocation counter must observe a deliberate heap allocation",
    );
}
