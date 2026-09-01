//! What a delivery costs in heap allocations today (Spec SC-025, FR-027).
//!
//! # Why this lives here
//!
//! Counting allocations needs a `#[global_allocator]`, and installing one needs `unsafe`. That
//! rules out both of the places this test would otherwise belong:
//! `crates/ngnet-qmux/tests/invariants.rs::a_caller_never_needs_unsafe` reads every Rust file
//! under `crates/ngnet-qmux/tests/` and fails on the word, and the same file forbids `unsafe`
//! anywhere under `crates/ngnet-qmux/src/io/`. Those invariants are the promise that a caller
//! of the layer never writes `unsafe`, and a measurement harness is not a reason to weaken
//! them. This crate has no such scan, already depends on both QMux crates, and is where the
//! join is exercised, so the harness lives here.
//!
//! A global allocator is a per-binary choice, so this is its own test target: every other test
//! in this crate runs against the system allocator, unaffected.
//!
//! # What is measured
//!
//! `Event::StreamData` carries a `Vec<u8>`, filled by `data.to_vec()` in the read handler in
//! `crates/ngnet-qmux/src/io/conn.rs`. dwnx hands that handler a slice pointing into the record
//! it is parsing, valid only for the duration of the call, so the copy is what makes the bytes
//! outlive it. Every delivery therefore costs at least one allocation, and the bytes of every
//! delivery are allocated afresh.
//!
//! FR-027 asks for that to be pinned before anything tries to remove it. Phase 7 (delivery
//! aliasing) is the phase expected to break the assertions below, and SC-025 is the criterion
//! it is judged against: a delivery that hands out a reference-counted slice of a buffer the
//! layer already owns allocates nothing per delivery, and the count stops growing with the
//! payload.
//!
//! # The direction of the bias
//!
//! The counter is armed around a window that contains the *whole* of the server's receive path,
//! not just the copy: the read buffer, dwnx's own book-keeping and the event queue's growth are
//! all inside it. So the count over-attributes -- it is an upper bound on what deliveries cost
//! and a lower bound on nothing. Every assertion here is therefore written as "at least as many
//! allocations as deliveries", which over-attribution cannot make false, and the load-bearing
//! one is the difference between two payload sizes: a receive path that allocated per delivery
//! shows a difference proportional to the extra deliveries, and one that does not shows a
//! difference near zero however much fixed overhead sits in the window.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ngnet_qmux::StreamId;
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection, Event, StreamOpen};

// Counting is per-thread rather than global. libtest runs test functions on parallel threads,
// and a global counter would charge a sibling's allocations to this test's window -- the false
// positive the whole arrangement exists to avoid.
//
// `Cell` with a const initialiser, so the thread-local needs no destructor: registering one
// allocates, and allocating from inside the allocator is how this recurses.
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

/// Counts this thread's allocations, but only while explicitly armed.
struct Counting;

fn record_allocation(size: usize) {
    // `try_with` rather than `with`: during thread teardown the thread-local may already be
    // gone, and an allocation then must not panic.
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
            let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + size));
        }
    });
}

// SAFETY: every method forwards to the system allocator unchanged; the counter is incidental
// and never affects the returned pointer.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What one measured window cost.
#[derive(Clone, Copy, Debug, Default)]
struct Cost {
    allocations: usize,
    bytes: usize,
}

/// Runs `body` with counting armed on this thread.
fn measured<R>(body: impl FnOnce() -> R) -> (R, Cost) {
    ALLOCATIONS.with(|count| count.set(0));
    BYTES.with(|bytes| bytes.set(0));
    COUNTING.with(|counting| counting.set(true));
    let out = body();
    COUNTING.with(|counting| counting.set(false));
    (
        out,
        Cost {
            allocations: ALLOCATIONS.with(Cell::get),
            bytes: BYTES.with(Cell::get),
        },
    )
}

/// A waker that remembers it was fired.
#[derive(Default)]
struct Flag {
    woken: AtomicBool,
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

/// How many polls either phase may take before the run is declared broken.
///
/// Reached only by a connection that is woken and makes no progress. Generous enough for the
/// largest payload here moving one record per poll, which is the worst case a connection could
/// exhibit before write coalescing and is now an upper bound rather than a description.
const MAX_POLLS: usize = 100_000;

/// Windows wide enough that the whole payload can be written before the peer is ever polled.
///
/// The point of the arrangement is that the client's writing and the server's receiving happen
/// in *different* phases: only the second is measured, and any client work inside the window
/// would be counted as a receive cost. Writing everything up front needs the peer's advertised
/// window to cover the payload, since without it the client would block waiting for credit that
/// only a polled server can send. The read-ahead bound is left at its default 1 MiB, and the
/// payloads stay under it, so the server is never held back from delivering either.
fn config() -> Config {
    Config::new()
        .initial_max_stream_data(4 << 20)
        .initial_max_data(8 << 20)
}

/// The smaller payload.
const SMALL: usize = 64 * 1024;

/// The larger payload.
///
/// Four times the smaller, so the extra deliveries are the dominant difference between the two
/// windows and a fixed overhead common to both cancels out of the subtraction.
const LARGE: usize = 256 * 1024;

/// A pair of connections that have exchanged transport parameters.
fn connected(
    waker: &Waker,
) -> (
    Connection<TestByteStream, TestClock>,
    Connection<TestByteStream, TestClock>,
) {
    let (client_io, server_io) = stream_pair();
    let clock = TestClock::new();
    let mut client =
        Connection::client(client_io, clock.clone(), config()).expect("a client connection");
    let mut server = Connection::server(server_io, clock, config()).expect("a server connection");
    let mut cx = Context::from_waker(waker);

    // Event polling consumes at most one lower read, while the forced pump publishes buffered
    // output before this manual executor suspends either side.
    for _ in 0..MAX_POLLS {
        let _ = client.poll_next_event(&mut cx);
        let _ = server.poll_next_event(&mut cx);
        let _ = client.poll_pump(&mut cx);
        let _ = server.poll_pump(&mut cx);
        if client.peer_transport_params().is_some() && server.peer_transport_params().is_some() {
            return (client, server);
        }
    }
    panic!("the two ends never exchanged transport parameters");
}

/// Writes `payload` from the client without ever polling the server.
///
/// The server is deliberately left un-polled: everything it would do with these bytes is what
/// the measured window is for, and doing any of it here would move the cost out of the
/// measurement. The in-memory byte stream has no capacity bound unless one is set, so the whole
/// payload can sit in it.
fn send(client: &mut Connection<TestByteStream, TestClock>, stream: StreamId, payload: &[u8]) {
    let waker = Waker::from(Arc::new(Flag::default()));
    let mut cx = Context::from_waker(&waker);
    let mut written = 0;
    for _ in 0..MAX_POLLS {
        if written == payload.len() {
            let _ = client.poll_pump(&mut cx);
            return;
        }
        match client.poll_write_stream(&mut cx, stream, &payload[written..], false) {
            Poll::Ready(Ok(taken)) => written += taken,
            Poll::Ready(Err(error)) => panic!("the client could not write: {error}"),
            Poll::Pending => panic!(
                "the client blocked after {written} of {} bytes, which means the peer's \
                 advertised window is smaller than this test assumed",
                payload.len()
            ),
        }
    }
    panic!("the client never finished writing");
}

/// What draining the server's events reported.
#[derive(Debug, Default)]
struct Drained {
    deliveries: usize,
    bytes: usize,
}

/// Reads every event the server has, until `bytes` bytes have been delivered.
///
/// Nothing in here allocates: the tallies are integers on the stack, and the events are dropped
/// as they are read. What the window counts is therefore the receive path's own allocation and
/// nothing the test added to it.
fn drain(server: &mut Connection<TestByteStream, TestClock>, bytes: usize) -> Drained {
    let waker = Waker::from(Arc::new(Flag::default()));
    let mut cx = Context::from_waker(&waker);
    let mut drained = Drained::default();
    for _ in 0..MAX_POLLS {
        if drained.bytes >= bytes {
            return drained;
        }
        match server.poll_next_event(&mut cx) {
            Poll::Ready(Ok(Event::StreamData { data, .. })) => {
                drained.deliveries += 1;
                drained.bytes += data.len();
            }
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => panic!("the server connection failed: {error}"),
            Poll::Pending => {}
        }
    }
    panic!(
        "the server delivered {} of {bytes} bytes and then stopped",
        drained.bytes
    );
}

/// Sends `payload` bytes on a fresh connection pair and measures what receiving them costs.
fn receive(payload: usize) -> (Drained, Cost) {
    let waker = Waker::from(Arc::new(Flag::default()));
    let (mut client, mut server) = connected(&waker);

    let stream = match client.try_open_bidi().expect("opening a stream") {
        StreamOpen::Opened(stream) => stream,
        StreamOpen::Blocked => panic!("the connected peer grants stream capacity"),
    };

    // Not a constant pattern: a payload of one repeated byte would let a receive path that
    // dropped or duplicated a record still add up to the right total.
    let payload: Vec<u8> = (0..payload).map(|index| (index % 251) as u8).collect();
    send(&mut client, stream, &payload);

    let (drained, cost) = measured(|| drain(&mut server, payload.len()));
    assert_eq!(
        drained.bytes,
        payload.len(),
        "the server did not receive what the client sent, so the cost measured is not the cost \
         of receiving it"
    );
    (drained, cost)
}

#[test]
fn today_every_delivery_costs_an_allocation() {
    let (small, small_cost) = receive(SMALL);
    let (large, large_cost) = receive(LARGE);

    assert!(
        small.deliveries > 1 && large.deliveries > small.deliveries,
        "the larger payload should arrive in more deliveries than the smaller one, because a \
         delivery carries at most one record's payload; {} and {} say the workload is not what \
         this test assumes",
        small.deliveries,
        large.deliveries
    );

    assert!(
        small_cost.allocations >= small.deliveries,
        "receiving {SMALL} bytes took {} deliveries and {} allocations. Today's receive path \
         copies each delivery out of dwnx's parse buffer with `data.to_vec()`, so it cannot \
         cost fewer allocations than deliveries. Phase 7 (delivery aliasing) is expected to \
         break this",
        small.deliveries,
        small_cost.allocations
    );

    assert!(
        large_cost.allocations >= large.deliveries,
        "receiving {LARGE} bytes took {} deliveries and {} allocations, for the same reason as \
         above",
        large.deliveries,
        large_cost.allocations
    );

    // The load-bearing one. Everything the window counts besides the per-delivery copy is
    // roughly the same in both runs -- the read buffer is allocated once and reused, dwnx's
    // book-keeping is per connection, and the event queue's growth is amortised -- so the
    // difference between the two counts is what the extra deliveries cost. A receive path that
    // handed out a slice of a buffer it already owned would show a difference near zero here
    // however large the fixed overhead was, which is what makes this the assertion Phase 7
    // has to change.
    let extra_deliveries = large.deliveries - small.deliveries;
    let extra_allocations = large_cost
        .allocations
        .saturating_sub(small_cost.allocations);
    assert!(
        extra_allocations >= extra_deliveries,
        "{extra_deliveries} more deliveries cost only {extra_allocations} more allocations \
         ({} for {} deliveries against {} for {}), so receiving is no longer paying an \
         allocation per delivery",
        large_cost.allocations,
        large.deliveries,
        small_cost.allocations,
        small.deliveries
    );

    // The bytes tell the same story in the units SC-025 is stated in: today the payload is
    // copied into fresh storage, so the difference in bytes allocated is at least the
    // difference in bytes delivered.
    let extra_bytes = large_cost.bytes.saturating_sub(small_cost.bytes);
    assert!(
        extra_bytes >= large.bytes - small.bytes,
        "{} more bytes delivered cost only {extra_bytes} more bytes allocated, so the deliveries \
         are no longer being copied into storage of their own",
        large.bytes - small.bytes
    );
}

#[test]
fn the_counter_sees_an_allocation() {
    // A measurement harness that silently counted nothing would make every assertion above
    // pass by vacuity in one direction and fail confusingly in the other, so it is asked
    // directly whether it can see the one allocation this closure makes.
    let (allocated, cost) = measured(|| {
        let buffer: Vec<u8> = Vec::with_capacity(4096);
        buffer.capacity()
    });
    assert_eq!(
        allocated, 4096,
        "the vector did not allocate what it asked for"
    );
    assert!(
        cost.allocations >= 1 && cost.bytes >= 4096,
        "the allocator saw {} allocations and {} bytes for a 4096-byte vector, so it is not \
         counting",
        cost.allocations,
        cost.bytes
    );

    // And that it stops when disarmed: a counter stuck on would attribute the whole process's
    // work to whichever window happened to be open.
    let before = ALLOCATIONS.with(Cell::get);
    let idle: Vec<u8> = Vec::with_capacity(4096);
    assert_eq!(idle.capacity(), 4096);
    assert_eq!(
        ALLOCATIONS.with(Cell::get),
        before,
        "an allocation outside a measured window was counted"
    );
}
