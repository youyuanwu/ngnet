//! Proof that receiving allocates nothing in the wrapper.
//!
//! Inbound field names and values arrive as reference-counted buffers and body chunks as
//! raw pointers; both are handed to handlers as borrowed slices whose lifetime ends when
//! the handler returns. Nothing is copied and no reference count is taken, so delivering a
//! message should not allocate on the Rust side at all.
//!
//! That is a claim worth measuring rather than asserting, because it is exactly the kind
//! of property that decays silently: one `to_vec()` added for convenience inside the
//! wrapper would never fail a functional test. So a counting global allocator is installed
//! and armed around the calls that matter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ngnet_h3::{Conn, ConnBuilder, FieldAction, FixedBody, Header, Role, StreamId, Timestamp};

thread_local! {
    /// Allocations observed while armed.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    /// Whether this thread is currently counting.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

// SAFETY: every method forwards to the system allocator unchanged; the counters are
// thread-local and never affect the pointers returned.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note();
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.alloc_zeroed(layout) }
    }
}

fn note() {
    // `try_with` because the thread-locals may already be destroyed during teardown, and
    // an allocation then must not panic.
    let counting = COUNTING.try_with(Cell::get).unwrap_or(false);
    if counting {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `f` with allocation counting armed, returning how many the wrapper made.
fn count_allocations(f: impl FnOnce()) -> usize {
    ALLOCATIONS.with(|n| n.set(0));
    COUNTING.with(|c| c.set(true));
    f();
    COUNTING.with(|c| c.set(false));
    ALLOCATIONS.with(Cell::get)
}

const CLIENT_CONTROL: i64 = 2;
const CLIENT_QPACK_ENCODER: i64 = 6;
const CLIENT_QPACK_DECODER: i64 = 10;
const SERVER_CONTROL: i64 = 3;
const SERVER_QPACK_ENCODER: i64 = 7;
const SERVER_QPACK_DECODER: i64 = 11;

fn id(raw: i64) -> StreamId {
    StreamId::new(raw).expect("valid stream id")
}

/// Counts only what a handler observed, without copying any of it.
#[derive(Default)]
struct Counts {
    fields: usize,
    name_bytes: usize,
    value_bytes: usize,
    body_bytes: usize,
}

fn request_bytes() -> Vec<(i64, Vec<u8>)> {
    let mut client = ConnBuilder::<()>::new(Role::Client).build().unwrap();
    client.bind_control_stream(id(CLIENT_CONTROL)).unwrap();
    client
        .bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_DECODER))
        .unwrap();
    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "POST").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/allocation").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
                Header::new("accept", "text/plain").unwrap(),
                Header::new("user-agent", "ngnet-h3").unwrap(),
            ],
            // A body as well as fields: inbound chunks arrive through a different callback
            // as a raw pointer and length, so they are a separate opportunity to allocate.
            Some(Box::new(FixedBody::new(vec![0xa5u8; 4096]))),
        )
        .unwrap();

    let mut out: Vec<(i64, Vec<u8>)> = Vec::new();
    let mut drained = false;
    for _ in 0..64 {
        let Some(send) = client.writev_stream(&mut ()).unwrap() else {
            drained = true;
            break;
        };
        let stream = send.stream().get();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).unwrap();
        if taken == 0 {
            drained = true;
            break;
        }
        out.push((stream, bytes));
    }
    // Stopping at the bound would truncate the request, and the allocation count below
    // would then be measuring a partial message while claiming to measure a whole one.
    assert!(drained, "the client never stopped producing bytes");
    out
}

fn server() -> Conn<Counts> {
    let mut conn = ConnBuilder::<Counts>::new(Role::Server)
        .on_field(
            |counts: &mut Counts, _stream, _section, _token, name, value| {
                // Deliberately measuring rather than copying: a `to_vec()` here would be the
                // caller's allocation, not the wrapper's, and would mask what is being tested.
                counts.fields += 1;
                counts.name_bytes += name.len();
                counts.value_bytes += value.len();
                FieldAction::Continue
            },
        )
        .on_data(|counts: &mut Counts, _stream, chunk| {
            counts.body_bytes += chunk.len();
        })
        .build()
        .expect("server");
    conn.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();
    conn
}

#[test]
fn delivering_a_request_allocates_nothing_in_the_wrapper() {
    let data = request_bytes();
    let mut conn = server();
    let mut counts = Counts::default();

    // Everything is measured, including the first delivery. nghttp3's own allocations go
    // through `nghttp3_mem`, which this crate wires straight to libc rather than through
    // Rust's global allocator, so they were never in scope here; what is being counted is
    // exclusively what the wrapper itself allocates while delivering fields and body.
    let allocations = count_allocations(|| {
        for (stream, bytes) in &data {
            conn.read_stream(
                id(*stream),
                bytes,
                false,
                Timestamp::from_nanos(1),
                &mut counts,
            )
            .expect("read");
        }
    });

    assert!(
        counts.fields > 0,
        "no fields were delivered, so this measured nothing"
    );
    assert!(counts.name_bytes > 0 && counts.value_bytes > 0);
    assert_eq!(
        counts.body_bytes, 4096,
        "the whole body should have been delivered, or the body half of this measures \
         nothing"
    );
    assert_eq!(
        allocations, 0,
        "receiving allocated {allocations} times in the wrapper; inbound names, values and \
         body chunks are supposed to be borrowed for the call, not copied"
    );
}

#[test]
fn the_counting_allocator_actually_counts() {
    // Without this, a broken harness would report zero for everything and the assertion
    // above would pass vacuously.
    let allocations = count_allocations(|| {
        let mut v: Vec<u8> = Vec::new();
        v.reserve_exact(4096);
        std::hint::black_box(&v);
    });
    assert!(
        allocations > 0,
        "the allocator shim did not observe a deliberate allocation"
    );
}
