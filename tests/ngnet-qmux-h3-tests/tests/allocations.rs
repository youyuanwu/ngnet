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
//! `Event::StreamData` carries a `StreamBytes`, which is a reference-counted view of the
//! connection's read buffer rather than a copy of it. It used to be a `Vec<u8>` filled by
//! `data.to_vec()`: dwnx hands the read handler a slice pointing into the buffer it is parsing,
//! valid only for the duration of the call, and the copy was what made the bytes outlive it. So
//! every delivery cost at least one allocation and the bytes of every delivery were allocated
//! afresh. This file asserted exactly that, on FR-027's instruction to pin the figure before
//! anything tried to remove it; the assertions below are the inversion, and SC-014 is the
//! criterion they express.
//!
//! # Two working modes, measured separately
//!
//! SC-014 names the **steady state**: a caller that drops each delivery before the next
//! arrives. There the connection reads into the same buffer for its whole life, and a delivery
//! costs no storage proportional to itself at all.
//!
//! The other mode is a caller that lets the connection read far ahead of it. Deliveries from
//! several reads are then alive at once, the connection cannot reuse the buffer it has, and it
//! takes another rather than waiting -- which FR-016 requires, since waiting would be the stall
//! it forbids. That mode is measured too, and what is asserted of it is that it stays bounded,
//! because it is the mode where an unbounded pool would show.
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
//!
//! That bias now runs *against* the assertions rather than with them. When the claim was "at
//! least one allocation per delivery", counting the surrounding path made the claim easier;
//! now the claim is that receiving costs nearly nothing per delivery, and every allocation the
//! window catches that has nothing to do with deliveries makes it harder. A pass is therefore
//! worth more than it was, and the thresholds below are stated with room for the overhead
//! rather than at zero, because zero is not what an honest window here can show.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ngnet_qmux::StreamId;
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection, Event};

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

/// Runs `body` with counting armed on this thread, adding to whatever is already tallied.
///
/// Separate from [`measured`] because a steady-state run has to open and close the window many
/// times -- once around each of the caller's turns -- while the tally runs across all of them.
fn armed<R>(body: impl FnOnce() -> R) -> R {
    COUNTING.with(|counting| counting.set(true));
    let out = body();
    COUNTING.with(|counting| counting.set(false));
    out
}

/// Forgets what has been tallied so far.
fn reset() {
    ALLOCATIONS.with(|count| count.set(0));
    BYTES.with(|bytes| bytes.set(0));
}

/// What has been tallied since the last [`reset`].
fn tally() -> Cost {
    Cost {
        allocations: ALLOCATIONS.with(Cell::get),
        bytes: BYTES.with(Cell::get),
    }
}

/// Runs `body` with counting armed on this thread.
fn measured<R>(body: impl FnOnce() -> R) -> (R, Cost) {
    reset();
    let out = armed(body);
    (out, tally())
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

/// Polls `future` until it is ready.
fn now_or_never<F: Future>(mut future: core::pin::Pin<&mut F>, waker: &Waker) -> F::Output {
    let mut cx = Context::from_waker(waker);
    for _ in 0..MAX_POLLS {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
    }
    panic!("a future that should have completed immediately did not");
}

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

    // The announcement each end writes on its first pump is what carries the transport
    // parameters, so both ends have to be pumped twice: once to write, once to read what the
    // other wrote.
    for _ in 0..MAX_POLLS {
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
    let mut drained = Drained::default();
    drain_into(server, &mut drained, bytes);
    drained
}

/// As [`drain`], but adding to a running tally and to a running target.
///
/// The steady-state runs need this shape: they hand the server one chunk at a time and read it
/// out before the next arrives, so each turn's target is the total sent so far rather than a
/// fresh figure.
fn drain_into(
    server: &mut Connection<TestByteStream, TestClock>,
    drained: &mut Drained,
    bytes: usize,
) {
    let waker = Waker::from(Arc::new(Flag::default()));
    let mut cx = Context::from_waker(&waker);
    for _ in 0..MAX_POLLS {
        if drained.bytes >= bytes {
            return;
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

/// One record's worth, near enough.
///
/// The steady state SC-014 names is a caller that drops each delivery before the next arrives,
/// so the client is made to hand over about a record at a time and the server is read out
/// between each. Under the read-ahead default the connection would otherwise swallow the whole
/// payload on its first pass and the caller would never be in step with it -- which is a real
/// working mode, measured separately below as the backlog case, but it is not this one.
const CHUNK: usize = 8 * 1024;

/// Sends `payload` a chunk at a time, reading the server out between chunks.
///
/// Only the server's turns are inside the counting window. The client's writing is outside it
/// for the reason the whole-payload harness gives: what is being measured is what receiving
/// costs, and a window containing the sender measures the pair.
fn receive_in_step(payload: usize) -> (Drained, Cost) {
    let waker = Waker::from(Arc::new(Flag::default()));
    let (mut client, mut server) = connected(&waker);

    let stream = {
        let mut opening = core::pin::pin!(core::future::poll_fn(|cx| client.poll_open_bidi(cx)));
        now_or_never(opening.as_mut(), &waker).expect("a stream")
    };

    let payload: Vec<u8> = (0..payload).map(|index| (index % 251) as u8).collect();
    let mut drained = Drained::default();
    let mut sent = 0;
    let mut base = 0;
    let mut warmed = false;
    while sent < payload.len() {
        // The client is given its turn before it writes: the server's turn produced window
        // extensions and acknowledgements, and a client that never read them stops writing
        // long before the payload is done.
        {
            let mut cx = Context::from_waker(&waker);
            for _ in 0..8 {
                let _ = client.poll_pump(&mut cx);
            }
        }
        let end = (sent + CHUNK).min(payload.len());
        send(&mut client, stream, &payload[sent..end]);
        sent = end;
        armed(|| drain_into(&mut server, &mut drained, sent - base));

        // The first turn is thrown away. It carries everything a connection pays once -- the
        // read buffer itself, dwnx's first-record book-keeping, the queue's initial growth --
        // and counting it would report those as a per-delivery cost that a longer run would
        // then dilute, which is a measurement that changes with the length of the run.
        if !warmed {
            reset();
            drained = Drained::default();
            base = sent;
            warmed = true;
        }
    }

    assert_eq!(
        drained.bytes,
        payload.len() - CHUNK.min(payload.len()),
        "the server did not receive what the client sent after the warm-up turn"
    );
    (drained, tally())
}

/// Sends `payload` bytes on a fresh connection pair and measures what receiving them costs.
fn receive(payload: usize) -> (Drained, Cost) {
    let waker = Waker::from(Arc::new(Flag::default()));
    let (mut client, mut server) = connected(&waker);

    let stream = {
        let mut opening = core::pin::pin!(core::future::poll_fn(|cx| client.poll_open_bidi(cx)));
        now_or_never(opening.as_mut(), &waker).expect("a stream")
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
fn a_delivery_costs_no_storage_of_its_own() {
    let (small, small_cost) = receive_in_step(SMALL);
    let (large, large_cost) = receive_in_step(LARGE);

    assert!(
        small.deliveries > 1 && large.deliveries > small.deliveries,
        "the larger payload should arrive in more deliveries than the smaller one; {} and {} \
         say the workload is not what this test assumes",
        small.deliveries,
        large.deliveries
    );

    // The load-bearing one, and the inversion of what this file asserted before delivery
    // aliasing. The receive path used to copy each delivery into fresh storage, so the bytes
    // allocated across a window grew with the bytes delivered in it; now a delivery is a view
    // of a buffer the connection already had, and the two are unrelated. Stated as a
    // difference between two payload sizes so that the fixed cost of standing a connection up
    // -- which this window also contains, and which over-attributes -- cancels out.
    let extra_delivered = large.bytes - small.bytes;
    let extra_bytes = large_cost.bytes.saturating_sub(small_cost.bytes);
    assert!(
        extra_bytes * 8 < extra_delivered,
        "{extra_delivered} more bytes delivered cost {extra_bytes} more bytes allocated ({} for \
         {} against {} for {}). Delivery aliasing is supposed to have made these two figures \
         unrelated, and a difference this close to the payload says the deliveries are being \
         copied into storage of their own again",
        large_cost.bytes,
        large.bytes,
        small_cost.bytes,
        small.bytes
    );

    // And in the units the reader will want: a delivery here is about `CHUNK` bytes, and what
    // receiving one costs is a small constant that has nothing to do with that figure.
    //
    // Asked of the larger run only, and the reason is the bias this file's header describes.
    // The window contains everything a connection pays once as well as everything it pays per
    // delivery -- the framer's retention reaching its full size is the largest of them -- and
    // dividing by seven deliveries charges the whole of that to those seven. Thirty-one
    // deliveries dilute it to where the figure means what it says. The difference between the
    // two runs, asserted above, is the form of this claim that does not need the dilution.
    let per_delivery = large_cost.bytes / large.deliveries;
    assert!(
        per_delivery < CHUNK / 8,
        "the larger payload allocated {per_delivery} bytes per delivery for deliveries \
         averaging {} bytes, which is not a per-delivery cost independent of the payload",
        large.bytes / large.deliveries
    );

    // The allocation count moves the same way, and it is the weaker statement of the two: the
    // window contains a connection's whole receive path, so what it can say is that the count
    // is no longer *two* per delivery -- one for the copy and one for everything else -- which
    // is what it measured before. The ceiling is one and a half per delivery rather than one,
    // because the receive path's own fixed costs are inside the window and a run of this length
    // does not fully dilute them.
    assert!(
        large_cost.allocations * 2 < large.deliveries * 3,
        "receiving {} bytes in {} deliveries took {} allocations. Before delivery aliasing this \
         was two per delivery, because each delivery was copied out on top of everything else \
         the receive path does",
        large.bytes,
        large.deliveries,
        large_cost.allocations
    );
}

#[test]
fn a_caller_that_falls_behind_pays_in_buffers_rather_than_in_copies() {
    // The other working mode, and the honest half of the result. A caller that lets the
    // connection read far ahead of it holds deliveries from several read buffers at once, so
    // the connection cannot reuse the one it has and takes another -- an allocation per read
    // rather than per delivery, of a read buffer rather than of a payload copy.
    //
    // This is deliberate and is what FR-016 asks for: the alternative, waiting for the caller
    // to let go before reading again, is the stall the requirement forbids. What is asserted
    // here is that the mode stays *bounded* -- the bytes allocated stay within a small factor
    // of the bytes delivered, rather than growing without limit as a pool that never reclaimed
    // would.
    let (drained, cost) = receive(LARGE);
    assert!(
        cost.bytes < drained.bytes * 2,
        "receiving {} bytes with the caller behind allocated {} bytes, which is more than the \
         read buffers that backlog can account for",
        drained.bytes,
        cost.bytes
    );
}

#[test]
fn a_caller_that_holds_every_delivery_receives_what_was_sent() {
    // SC-015. The deliveries are kept, not dropped, right across the run: under aliasing they
    // are views of buffers the connection would otherwise be reading into again, so a
    // reclamation rule that let one go too early would show up here as bytes that changed
    // after they were handed over. They are checked at the end rather than as they arrive,
    // which is what makes that visible.
    let waker = Waker::from(Arc::new(Flag::default()));
    let (mut client, mut server) = connected(&waker);

    let stream = {
        let mut opening = core::pin::pin!(core::future::poll_fn(|cx| client.poll_open_bidi(cx)));
        now_or_never(opening.as_mut(), &waker).expect("a stream")
    };

    let payload: Vec<u8> = (0..LARGE).map(|index| (index % 251) as u8).collect();
    send(&mut client, stream, &payload);

    let mut held = Vec::new();
    let mut cx = Context::from_waker(&waker);
    let mut received = 0usize;
    for _ in 0..MAX_POLLS {
        if received >= payload.len() {
            break;
        }
        match server.poll_next_event(&mut cx) {
            Poll::Ready(Ok(Event::StreamData { data, .. })) => {
                received += data.len();
                // Returned as the bytes arrive, while the data itself is kept. FR-016 says
                // that combination must not stop the connection reading, and a run that
                // stalled here would exhaust `MAX_POLLS` rather than finish.
                server
                    .extend_connection_credit(data.len() as u64)
                    .expect("credit");
                held.push(data);
            }
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => panic!("the server connection failed: {error}"),
            Poll::Pending => {}
        }
    }

    // The pool's ceiling, checked while every delivery of the run is still held -- which is
    // the only state in which it could be exceeded. `READ_POOL_LIMIT` in
    // `crates/ngnet-qmux/src/io/conn.rs` is eight, plus the one being read into. A pool that
    // grew with the number of held deliveries rather than stopping here would be the
    // unbounded-retention failure FR-016 forbids, and it would not show in the byte counts
    // above, because those buffers are held by the caller's own deliveries either way.
    #[cfg(debug_assertions)]
    assert!(
        server.read_buffers() <= 9,
        "the connection is watching {} read buffers while the caller holds {} deliveries, past \
         the ceiling the pool is supposed to stop at",
        server.read_buffers(),
        held.len()
    );

    let rejoined: Vec<u8> = held.iter().flat_map(|data| data.iter().copied()).collect();
    assert_eq!(
        rejoined.len(),
        payload.len(),
        "the caller held {} bytes of a {}-byte payload",
        rejoined.len(),
        payload.len()
    );
    assert!(
        rejoined == payload,
        "the bytes a caller held across the whole run are not the bytes that were sent"
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
