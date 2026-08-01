//! Receiving performs no heap allocation attributable to the wrapper (Spec SC-005).
//!
//! Header names, header values and body chunks reach a handler as borrowed slices into
//! libnghttp2's own buffers. Nothing is copied on the way through, so processing a large
//! response costs no Rust allocations.
//!
//! This lives in its own integration test because it installs a global allocator, which
//! is a per-binary choice.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use nghttp2::{FrameInfo, HeaderAction, Session, SessionBuilder, StreamId};

// Counting is per-thread, not global. libtest runs tests in parallel threads, so a global
// flag would charge a sibling test's allocations to this test's measurement window — which
// is exactly the false positive this arrangement exists to avoid.
//
// `Cell` with a const initialiser is used deliberately: it needs no drop glue, so the
// thread-local registers no destructor and accessing it from inside the allocator cannot
// recurse into allocation.
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

/// Counts this thread's allocations, but only while explicitly armed.
struct Counting;

fn record_allocation() {
    // `try_with` rather than `with`: during thread teardown the thread-local may already
    // be gone, and an allocation then must not panic.
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

/// Runs `body` with allocation counting armed on this thread, returning how many occurred.
fn count_allocations<R>(body: impl FnOnce() -> R) -> (R, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    let out = body();
    COUNTING.with(|counting| counting.set(false));
    (out, ALLOCATIONS.with(Cell::get))
}

/// Fixed-size tallies, so the handlers themselves cannot allocate.
#[derive(Debug, Default)]
struct Tally {
    headers: usize,
    header_bytes: usize,
    frames: usize,
    closed: usize,
}

fn drain<C>(session: &mut Session<C>, context: &mut C) -> Vec<u8> {
    let mut wire = Vec::new();
    while let Some(block) = session.send(context).expect("send failed") {
        wire.extend_from_slice(block);
    }
    wire
}

fn hpack_literal(name: &str, value: &str) -> Vec<u8> {
    assert!(name.len() < 127 && value.len() < 127);
    let mut out = vec![0x00];
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
    out
}

fn frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// A request carrying many headers, so any per-header allocation would be obvious.
fn busy_request(stream_id: u32, data_frames: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    for (name, value) in [
        (":method", "POST"),
        (":scheme", "http"),
        (":authority", "example.test"),
        (":path", "/upload"),
    ] {
        payload.extend_from_slice(&hpack_literal(name, value));
    }
    for index in 0..32 {
        payload.extend_from_slice(&hpack_literal(
            &format!("x-header-{index:02}"),
            "some-reasonably-long-header-value-for-the-test",
        ));
    }

    let mut out = frame(0x01, 0x04, stream_id, &payload);
    // A body split across several DATA frames.
    for _ in 0..data_frames {
        out.extend_from_slice(&frame(0x00, 0x00, stream_id, &[b'x'; 512]));
    }
    out.extend_from_slice(&frame(0x00, 0x01, stream_id, b"tail"));
    out
}

#[test]
fn receiving_allocates_nothing_in_the_wrapper() {
    let mut server = SessionBuilder::<Tally>::server()
        .on_begin_headers(|_: &mut Tally, _: FrameInfo| HeaderAction::Continue)
        .on_header(|tally: &mut Tally, _: FrameInfo, name: &[u8], value: &[u8]| {
            // Only arithmetic: the handler must not allocate, or it would be measuring
            // itself rather than the wrapper.
            tally.headers += 1;
            tally.header_bytes += name.len() + value.len();
            HeaderAction::Continue
        })
        .on_data_chunk(|tally: &mut Tally, _: StreamId, chunk: &[u8]| {
            tally.header_bytes += chunk.len();
        })
        .on_frame(|tally: &mut Tally, _: FrameInfo| {
            tally.frames += 1;
        })
        .on_stream_close(|tally: &mut Tally, _: StreamId, _| {
            tally.closed += 1;
        })
        .build()
        .expect("server build failed");

    let mut tally = Tally::default();

    // Complete the handshake outside the measured window.
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let opening = drain(&mut client, &mut ());
    server.recv(&opening, &mut tally).expect("handshake failed");

    // Two requests are measured, the second with four times the body of the first, so
    // anything allocating per header or per chunk would show as a count that grows with
    // the work rather than a stable zero.
    let smaller = busy_request(1, 4);
    let (_, small_allocations) =
        count_allocations(|| server.recv(&smaller, &mut tally).expect("recv failed"));
    assert_eq!(
        small_allocations, 0,
        "receiving must not allocate in the wrapper"
    );

    let request = busy_request(3, 16);

    let (consumed, allocations) =
        count_allocations(|| server.recv(&request, &mut tally).expect("recv failed"));

    assert_eq!(consumed, request.len(), "the whole buffer should be consumed");
    assert!(
        tally.headers >= 72,
        "the handlers should have seen every header of both requests, saw {}",
        tally.headers
    );
    // The stream is only half-closed here: the client signalled END_STREAM but the server
    // has not responded, so it stays open awaiting one. The close handler is registered
    // regardless so its trampoline is on the path being measured.
    assert_eq!(tally.closed, 0, "the stream should still be awaiting a response");
    assert_eq!(
        allocations, 0,
        "receiving must not allocate in the wrapper; {allocations} allocation(s) occurred \
         while processing {} headers and {} frames",
        tally.headers, tally.frames
    );
}

#[test]
fn the_counter_actually_observes_allocations() {
    // Guards against the assertion above passing because counting never worked.
    let (_, allocations) = count_allocations(|| {
        let mut v: Vec<u8> = Vec::with_capacity(4096);
        v.push(1);
        v.len()
    });

    assert!(
        allocations > 0,
        "the counting allocator should have observed a deliberate allocation"
    );
}
