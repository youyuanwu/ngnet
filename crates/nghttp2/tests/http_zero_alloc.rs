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
//! # Deferred from Phase 8: is the borrowed path right for `TokioWriter`?
//!
//! Measured here, per steady-state pass of a client upload, for each write shape. The
//! `>0` / `0` allocation columns and the `1` write column are asserted by the tests named
//! in the final column; the parenthesised counts (the "4"s) are illustrative — they are the
//! values observed at the default 64 KiB window, but the tests pin the *properties* (one
//! write however many blocks; more than one write, held constant; allocates every pass,
//! held constant) rather than those incidental numbers, which move with the window size and
//! `bytes`' growth policy.
//!
//! | shape (`write_borrowed`) | heap allocations / pass | transport writes / pass | pinned by |
//! |--------------------------|-------------------------|-------------------------|-----------|
//! | `Some` (borrowed)        | `0`                     | one per block (4 here)  | `steady_state_send_allocates_nothing_on_the_borrowed_path`, `the_borrowed_write_path_writes_each_block_separately` |
//! | `None` (owned)           | `>0`, constant (4 here) | `1` (all blocks coalesced) | `the_owned_write_path_coalesces_a_pass_into_one_write`, `the_owned_write_path_allocates_on_every_pass` |
//!
//! The borrowed shape trades a handful of small writes for zero allocation and zero copy;
//! the owned shape buys a single write per pass by allocating a coalescing buffer and
//! copying every outgoing octet into it, every pass. The block count is small and bounded
//! by the flow-control window, so the writes the borrowed shape adds are few, while the
//! allocation and copy the owned shape adds recur forever. The crate's headline commitment
//! — steady-state zero allocation — is reachable *only* on the borrowed path, which
//! `the_owned_write_path_allocates_on_every_pass` pins by showing the same traffic costs an
//! allocation every pass on the owned shape and none on the borrowed one. The measurement
//! therefore does not contradict the tokio default; it endorses it: `TokioWriter` should go
//! on returning `Some` from `write_borrowed`.
#![cfg(feature = "http")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Wake;

use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};

use http_body::Body;
use nghttp2::http::testing::{
    Empty, Full, bytes_crate as bytes, http_body_crate as http_body, http_crate as http,
    pool_high_water, pool_size,
};
use nghttp2::http::transport::{Transport, TransportRead, TransportWrite};
use nghttp2::{
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

struct Recording {
    inbound: Wire,
    outbound: Wire,
    borrowed: bool,
    meter: Rc<RefCell<Meter>>,
}

struct RecReader {
    inbound: Wire,
}

struct RecWriter {
    outbound: Wire,
    borrowed: bool,
    meter: Rc<RefCell<Meter>>,
}

impl RecWriter {
    fn record(&self, data: &[u8]) {
        let mut meter = self.meter.borrow_mut();
        meter.writes += 1;
        meter.bytes += data.len();
        drop(meter);
        self.outbound.borrow_mut().buf.extend(data.iter().copied());
    }
}

impl Transport for Recording {
    type Reader = RecReader;
    type Writer = RecWriter;

    fn split(self) -> (RecReader, RecWriter) {
        (
            RecReader {
                inbound: self.inbound,
            },
            RecWriter {
                outbound: self.outbound,
                borrowed: self.borrowed,
                meter: self.meter,
            },
        )
    }
}

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

impl TransportWrite for RecWriter {
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        self.record(&buf);
        let written = buf.len();
        (Ok(written), buf)
    }

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // The single override decides the drain strategy: `Some` elects the zero-copy
        // borrowed path, `None` leaves the owned coalescing one. Which shape this writer is
        // was fixed when it was built.
        if !self.borrowed {
            return None;
        }
        self.record(data);
        Some(core::future::ready(Ok(data.len())))
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
/// is stepped by hand rather than by [`nghttp2::http::testing::serve`] because the counter
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
    body: &mut nghttp2::http::IncomingBody,
) -> Option<Result<http_body::Frame<Bytes>, nghttp2::http::Error>> {
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
    let transport = Recording {
        inbound: Rc::clone(&s2c),
        outbound: Rc::clone(&c2s),
        borrowed: true,
        meter: Rc::new(RefCell::new(Meter::default())),
    };

    let (requests, connection) =
        nghttp2::http::handshake::<Recording, Empty>(transport).expect("handshake");

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

fn run_send(borrowed: bool) -> Send {
    let c2s = wire(1 << 22);
    let s2c = wire(1 << 20);
    let meter = Rc::new(RefCell::new(Meter::default()));
    let transport = Recording {
        inbound: Rc::clone(&s2c),
        outbound: Rc::clone(&c2s),
        borrowed,
        meter: Rc::clone(&meter),
    };

    let (requests, connection) =
        nghttp2::http::handshake::<Recording, Full>(transport).expect("handshake");

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
    let measured = run_send(true);

    assert!(
        measured.allocations.iter().all(|&count| count == 0),
        "a steady-state send pass on the borrowed path must allocate nothing, saw {:?}",
        measured.allocations,
    );
}

#[test]
fn the_owned_write_path_coalesces_a_pass_into_one_write() {
    let measured = run_send(false);

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
fn the_borrowed_write_path_writes_each_block_separately() {
    let borrowed = run_send(true);
    let owned = run_send(false);

    // Same traffic, driven identically: the two shapes differ only in how they drain it.
    assert_eq!(
        borrowed.bytes, owned.bytes,
        "the two shapes were not compared on the same traffic",
    );
    assert!(
        borrowed.writes.iter().all(|&count| count > 1),
        "the borrowed path hands over each session block as its own write rather than \
         coalescing, so a multi-block pass is more than one write, saw {:?}",
        borrowed.writes,
    );
    // "At most one per block" is the property that matters, and with a transport that accepts
    // every block whole the count is exactly one per block: the driver's per-block loop never
    // re-issues. That the borrowed count stays put pass to pass is what pins it.
    let first = borrowed.writes[0];
    assert!(
        borrowed.writes.iter().all(|&count| count == first),
        "one write per block is a fixed cost of the traffic, not a growing one, saw {:?}",
        borrowed.writes,
    );
}

#[test]
fn the_owned_write_path_allocates_on_every_pass() {
    // The case for the tokio default resting on the borrowed path is only sound if the owned
    // path really does allocate every steady-state pass — otherwise the module-doc conclusion
    // is rhetoric. This pins that cost so it fails the day it stops being true.
    //
    // The property asserted is "allocates on every pass, and does not grow pass to pass",
    // not an exact count: the owned flush coalesces into a `BytesMut` that grows block by
    // block, so the number of reallocations is a function of the window's block count. That
    // is stable in steady state but is an implementation detail of `bytes`' growth policy, so
    // pinning `> 0` and constant captures what matters — a recurring, non-amortising cost —
    // without welding the test to an incidental number.
    let owned = run_send(false);
    let borrowed = run_send(true);

    assert!(
        owned.allocations.iter().all(|&count| count > 0),
        "the owned path allocates a coalescing buffer on every pass, saw {:?}",
        owned.allocations,
    );
    let first = owned.allocations[0];
    assert!(
        owned.allocations.iter().all(|&count| count == first),
        "the owned path's per-pass allocation is a fixed recurring cost, not a growing one, \
         saw {:?}",
        owned.allocations,
    );
    // The contrast is the whole point: identical traffic, zero on the borrowed path.
    assert!(
        borrowed.allocations.iter().all(|&count| count == 0),
        "the borrowed path carries the same traffic for no allocation, saw {:?}",
        borrowed.allocations,
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
    let transport = Recording {
        inbound: Rc::clone(&to_server),
        outbound: Rc::clone(&from_server),
        borrowed: true,
        meter: Rc::new(RefCell::new(Meter::default())),
    };

    let wakers: Rc<RefCell<Vec<WakerSlot>>> = Rc::new(RefCell::new(Vec::new()));
    let published = Rc::clone(&wakers);
    let connection = nghttp2::http::server::serve(
        transport,
        move |_request: http::Request<nghttp2::http::IncomingBody>| {
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
