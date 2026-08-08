//! Proof that driving the send loop allocates nothing in the wrapper.
//!
//! The design reason it *can* be true is that the caller supplies the datagram buffer, so
//! nothing has to be allocated per packet. But that is an argument, not a guarantee, and it
//! is exactly the kind of property that decays silently: one `to_vec()` added inside the
//! wrapper for convenience would never fail a functional test.
//!
//! So a counting global allocator is installed and armed around the calls that matter.
//! Following the technique in `crates/ngnet-h3/tests/zero_alloc.rs`.

#![cfg(feature = "tls-ossl")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ngnet_quic::{
    ConnBuilder, EntropySource, Handlers, OsslBackend, Result, Role, Settings, Timestamp,
    TlsBackend, TransportParams, Verify, WriteOutcome,
};

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

/// Records an allocation, if counting is armed.
fn note() {
    COUNTING.with(|counting| {
        if counting.get() {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
        }
    });
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `f` with allocation counting armed, and reports how many were seen.
fn count_allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    let value = f();
    COUNTING.with(|counting| counting.set(false));
    let seen = ALLOCATIONS.with(Cell::get);
    (value, seen)
}

/// A counter, adequate because these tests do not depend on unpredictability.
struct StubEntropy(u8);

impl EntropySource for StubEntropy {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        for slot in dest.iter_mut() {
            self.0 = self.0.wrapping_add(1);
            *slot = self.0;
        }
        Ok(())
    }
}

#[test]
fn writing_packets_allocates_nothing_in_the_wrapper() {
    let backend = OsslBackend::builder(Role::Client)
        .alpn("h3")
        .verify(Verify::DangerouslyAcceptAnyCertificate)
        .build()
        .expect("building a backend");
    let session = backend
        .new_session(Role::Client, None)
        .expect("creating a session");

    let start = Timestamp::from_nanos(1_000_000).unwrap();
    let mut conn = ConnBuilder::new(
        Role::Client,
        Settings::new(start),
        TransportParams::new(),
        Box::new(StubEntropy(0)),
        session,
        "127.0.0.1:1000".parse().unwrap(),
        "127.0.0.1:2000".parse().unwrap(),
    )
    .build(Handlers::new())
    .expect("building the connection");

    // The buffer is the caller's, allocated once and reused -- which is the whole reason
    // the send loop can be allocation-free.
    let mut buf = vec![0u8; 1500];

    // One write outside the count, so any lazily-initialised state inside OpenSSL or ngtcp2
    // is already warm. Counting that would measure the first call, not the loop.
    let _ = conn.write_pkt(&mut buf, start);

    let mut when = 2_000_000u64;
    let (_, allocations) = count_allocations(|| {
        for _ in 0..8 {
            let now = Timestamp::from_nanos(when).unwrap();
            when += 2_000_000;
            match conn.write_pkt(&mut buf, now) {
                Ok(WriteOutcome::Datagram { .. } | WriteOutcome::Idle | WriteOutcome::Blocked) => {}
                Err(_) => break,
            }
        }
    });

    assert_eq!(
        allocations, 0,
        "the send loop allocated {allocations} times; the wrapper is supposed to write \
         into the caller's buffer and nothing else"
    );
}

#[test]
fn asking_for_the_expiry_allocates_nothing() {
    // Called on every pass of a caller's event loop, so it is worth knowing it is free.
    let backend = OsslBackend::builder(Role::Client)
        .alpn("h3")
        .verify(Verify::DangerouslyAcceptAnyCertificate)
        .build()
        .unwrap();
    let session = backend.new_session(Role::Client, None).unwrap();
    let start = Timestamp::from_nanos(1_000_000).unwrap();
    let conn = ConnBuilder::new(
        Role::Client,
        Settings::new(start),
        TransportParams::new(),
        Box::new(StubEntropy(0)),
        session,
        "127.0.0.1:1000".parse().unwrap(),
        "127.0.0.1:2000".parse().unwrap(),
    )
    .build(Handlers::new())
    .unwrap();

    let (_, allocations) = count_allocations(|| {
        for _ in 0..64 {
            let _ = conn.expiry();
            let _ = conn.in_closing_period();
            let _ = conn.in_draining_period();
            let _ = conn.is_handshake_completed();
        }
    });

    assert_eq!(allocations, 0, "querying a connection should be free");
}

#[test]
fn the_counter_would_notice_a_real_allocation() {
    // A counting allocator that had stopped counting would make both tests above assert
    // nothing at all, and would do so silently.
    let (_, allocations) = count_allocations(|| {
        let v: Vec<u8> = Vec::with_capacity(1024);
        core::hint::black_box(&v);
    });
    assert!(
        allocations > 0,
        "the allocation counter is not counting, so the tests above prove nothing"
    );
}

#[test]
fn the_counter_is_disarmed_outside_a_measured_region() {
    let before = ALLOCATIONS.with(Cell::get);
    let v: Vec<u8> = Vec::with_capacity(4096);
    core::hint::black_box(&v);
    let after = ALLOCATIONS.with(Cell::get);
    assert_eq!(
        before, after,
        "allocations outside a measured region must not be counted"
    );
}
