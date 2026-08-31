//! Proof that an HTTP/3 send pass allocates one owned buffer per datagram, and nothing else.
//!
//! The send path hands each datagram it produces to a detached connection's queue, which
//! takes ownership of it — so one owned allocation per datagram is forced and cannot be
//! removed. What *was* removable, and what Phase 6 removed, is the copy that used to sit
//! beside it: the datagram was produced into a scratch buffer and then copied out into the
//! buffer that was handed over. Now it is produced straight into that buffer.
//!
//! This is exactly the kind of property that decays silently: one `to_vec()` added back for
//! convenience would never fail a functional test. So a counting global allocator is armed
//! around a produce pass and the count is asserted to be one per datagram.
//!
//! The measured pass is `produce`, not the stream-writing `drain`. `drain` stages every
//! accepted write into its own retained allocation — the forced copy `ngnet-quic` owes
//! because ngtcp2 keeps the pointer it is handed until acknowledgement — so a drain pass
//! that accepts new stream bytes allocates that copy *besides* the datagram. `produce`
//! stages nothing: it emits what the connection already owes, acknowledgements and probes,
//! so the datagram buffer is the only allocation it can force. That is the pass whose cost
//! this phase is about.
//!
//! The harness is the in-memory endpoint one, single-threaded and clock-driven, so the
//! global allocator sees only this test's own work — the real-socket harness runs on tokio,
//! whose runtime allocates constantly on threads this counter cannot distinguish.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::http::{QuicConnection, StreamSource, WriteOutcome};
use ngnet_quic::endpoint::testing::{TestSocket, socket_pair};
use ngnet_quic::endpoint::{Clock, Config, Endpoint, EndpointBuilder, EndpointDriver};
use ngnet_quic::{EntropySource, OsslBackend, OsslSession, Result, Role, Timestamp};
use ngnet_quic_h3::{NgtcpConnection, accept, connect};
use ngnet_quic_h3_tests::{Credentials, H3_ALPN, TEST_SERVER_NAME};

thread_local! {
    /// Allocations observed while armed.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    /// Whether this thread is currently counting.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    /// Allocations larger than the current threshold.
    static LARGE: Cell<usize> = const { Cell::new(0) };
    /// Only allocations strictly larger than this are counted in `LARGE`.
    static LARGE_THRESHOLD: Cell<usize> = const { Cell::new(usize::MAX) };
}

struct Counting;

// SAFETY: every method forwards to the system allocator unchanged; the counters are
// thread-local and never affect the pointers returned.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarding the caller's own contract.
        unsafe { System.alloc_zeroed(layout) }
    }
}

/// Records an allocation, if counting is armed.
fn note(size: usize) {
    COUNTING.with(|counting| {
        if counting.get() {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
            if size > LARGE_THRESHOLD.with(Cell::get) {
                LARGE.with(|count| count.set(count.get() + 1));
            }
        }
    });
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[cfg(feature = "diagnostics")]
static DIAGNOSTICS_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Runs `f` with allocation counting armed, and reports how many were seen.
fn count_allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let (value, allocations, _) = count_allocations_larger_than(usize::MAX, f);
    (value, allocations)
}

/// Runs `f` while counting all allocations and those above `threshold`.
fn count_allocations_larger_than<T>(threshold: usize, f: impl FnOnce() -> T) -> (T, usize, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    LARGE.with(|count| count.set(0));
    LARGE_THRESHOLD.with(|current| current.set(threshold));
    COUNTING.with(|counting| counting.set(true));
    let value = f();
    COUNTING.with(|counting| counting.set(false));
    LARGE_THRESHOLD.with(|current| current.set(usize::MAX));
    let seen = ALLOCATIONS.with(Cell::get);
    let large = LARGE.with(Cell::get);
    (value, seen, large)
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

/// A clock two endpoints can share across the `Send + Sync` bound `build_detachable`
/// requires. The in-memory `TestClock` is `Rc`-based and cannot cross that bound; this is
/// the same hand-moved clock on an atomic instead. It registers no wakers because the test
/// busy-polls over a no-op waker and moves time by hand between polls, so a sleeper only has
/// to resolve once its deadline has passed.
#[derive(Clone)]
struct SharedClock {
    now: Arc<AtomicU64>,
}

impl SharedClock {
    fn new() -> Self {
        Self {
            now: Arc::new(AtomicU64::new(1_000_000_000)),
        }
    }

    fn advance(&self, nanos: u64) {
        self.now.fetch_add(nanos, Ordering::Relaxed);
    }
}

struct SharedSleep {
    deadline: u64,
    now: Arc<AtomicU64>,
}

impl Future for SharedSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.now.load(Ordering::Relaxed) >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Clock for SharedClock {
    type Sleep = Pin<Box<SharedSleep>>;

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now.load(Ordering::Relaxed))
            .expect("the shared clock stays in range")
    }

    fn sleep_until(&self, deadline: Timestamp) -> Self::Sleep {
        Box::pin(SharedSleep {
            deadline: deadline.as_nanos(),
            now: Arc::clone(&self.now),
        })
    }
}

type Driver = EndpointDriver<TestSocket, SharedClock, OsslBackend>;

/// Builds an endpoint that can hand its connections over, on the shared clock.
fn build(
    role: Role,
    socket: TestSocket,
    clock: SharedClock,
    credentials: &Credentials,
) -> (Endpoint<OsslSession>, Driver) {
    let backend = match role {
        Role::Client => OsslBackend::builder(Role::Client)
            .alpn(H3_ALPN)
            .trust_anchor_pem(credentials.certificate_pem.as_str())
            .use_system_trust_store(false)
            .build()
            .expect("a client backend"),
        Role::Server => OsslBackend::builder(Role::Server)
            .alpn(H3_ALPN)
            .certificate_chain_pem(credentials.certificate_pem.as_str())
            .private_key_pem(credentials.key_pem.as_str())
            .build()
            .expect("a server backend"),
    };
    let seed = if role == Role::Client { 0 } else { 64 };
    let mut builder = EndpointBuilder::new(socket, clock, backend)
        .config(Config::new())
        .entropy(move || StubEntropy(seed));
    if role == Role::Server {
        builder = builder.accepts(true);
    }
    builder.build_detachable().expect("a detachable endpoint")
}

/// A source that offers one stream once, with a fixed payload, then reports nothing more.
struct OneWrite {
    stream: H3StreamId,
    payload: Vec<u8>,
    sent: usize,
    done: bool,
}

impl StreamSource for OneWrite {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(H3StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        if self.done {
            return false;
        }
        let remaining = &self.payload[self.sent..];
        match write(self.stream, &[IoSlice::new(remaining)], true) {
            WriteOutcome::Accepted(taken) => {
                self.sent += taken;
                if self.sent >= self.payload.len() {
                    self.done = true;
                }
                true
            }
            WriteOutcome::Blocked => false,
            WriteOutcome::Gone => {
                self.done = true;
                false
            }
        }
    }
}

/// Drives the two endpoint drivers one pass each and advances the clock.
fn turn(drivers: &mut [Pin<Box<Driver>>], clock: &SharedClock, cx: &mut Context<'_>) {
    for driver in drivers.iter_mut() {
        let _ = driver.as_mut().poll(cx);
    }
    clock.advance(2_000_000);
}

#[test]
fn a_produce_pass_allocates_one_buffer_per_datagram() {
    #[cfg(feature = "diagnostics")]
    let _diagnostics_guard = DIAGNOSTICS_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    #[cfg(feature = "diagnostics")]
    {
        ngnet_quic::diagnostics::reset();
        assert!(!ngnet_quic::diagnostics::is_armed());
    }
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let credentials = Credentials::generate();

    let caddr = "127.0.0.1:4553".parse().unwrap();
    let saddr = "127.0.0.1:4554".parse().unwrap();
    let clock = SharedClock::new();
    let (cs, ss) = socket_pair(caddr, saddr);
    let (client, cdrv) = build(Role::Client, cs, clock.clone(), &credentials);
    let (server, sdrv) = build(Role::Server, ss, clock.clone(), &credentials);
    let mut drivers: Vec<Pin<Box<Driver>>> = vec![Box::pin(cdrv), Box::pin(sdrv)];

    // Establish an HTTP/3 connection and take both ends as detached connections.
    //
    // Both ends detach, so once the client's handshake completes the client endpoint driver
    // stops driving it — and the server still needs the client's final flight to complete
    // its own handshake. So once the client is in hand it is pumped by hand each pass, which
    // is what sends that flight.
    let mut connecting = Box::pin(connect(&client, saddr, Some(TEST_SERVER_NAME)));
    let mut accepting = Box::pin(accept(&server));
    let mut client_conn: Option<NgtcpConnection<OsslSession>> = None;
    let mut server_conn: Option<NgtcpConnection<OsslSession>> = None;
    for _ in 0..1000 {
        turn(&mut drivers, &clock, &mut cx);
        if client_conn.is_none()
            && let Poll::Ready(r) = connecting.as_mut().poll(&mut cx)
        {
            client_conn = Some(r.expect("the client handshake failed"));
        }

        if let Some(conn) = client_conn.as_mut() {
            let _ = conn.poll_event(&mut cx);
        }
        if server_conn.is_none()
            && let Poll::Ready(r) = accepting.as_mut().poll(&mut cx)
        {
            server_conn = Some(r.expect("the server accept failed"));
        }
        if client_conn.is_some() && server_conn.is_some() {
            break;
        }
    }
    let mut client_conn = client_conn.expect("a client connection");
    let mut server_conn = server_conn.expect("a server connection");
    // Warm up the server's send path off the counted region. The connection was detached
    // when the handshake completed, so its outbound queue starts empty; the first datagram
    // it produces would grow that queue, an allocation that has nothing to do with the send
    // path. Producing the handshake-completion datagrams the server still owes grows it here
    // instead, and settles what it owes so the counted debt is only the acknowledgement.
    for _ in 0..50 {
        let _ = server_conn
            .produce_pass_for_test()
            .expect("a warmup produce pass");
        turn(&mut drivers, &clock, &mut cx);
    }

    // The client opens a stream and writes a small payload to the server. This is what
    // leaves the server owing an acknowledgement — the debt the counted produce pass pays.
    let stream = loop {
        match client_conn.poll_open_uni(&mut cx) {
            Poll::Ready(r) => break r.expect("opening a uni stream"),
            Poll::Pending => turn(&mut drivers, &clock, &mut cx),
        }
    };
    let mut source = OneWrite {
        stream,
        payload: vec![0x5au8; 200],
        sent: 0,
        done: false,
    };
    for _ in 0..50 {
        let _ = client_conn
            .poll_transmit(&mut cx, &mut source)
            .map(|r| r.expect("a client transmit pass"));
        turn(&mut drivers, &clock, &mut cx);
        if source.done {
            break;
        }
    }
    assert!(source.done, "the client never placed its write on the wire");

    // Let the write reach the server, then read it in without producing the acknowledgement
    // it now owes. The read is off the counted region deliberately: it is not the send path,
    // and it decrypts, which allocates.
    for _ in 0..50 {
        turn(&mut drivers, &clock, &mut cx);
    }
    server_conn.intake_for_test().expect("reading the write in");

    // The counted pass: produce what the server owes. It stages no stream data, so the only
    // allocation it can force is one owned buffer per datagram it queues.
    let (datagrams, allocations) = count_allocations(|| {
        server_conn
            .produce_pass_for_test()
            .expect("a counted produce pass")
    });

    assert!(
        datagrams >= 1,
        "the produce pass queued no datagram, so an allocation count would prove nothing"
    );
    assert_eq!(
        allocations, datagrams,
        "a produce pass queued {datagrams} datagram(s) but allocated {allocations} times; \
         it is supposed to allocate exactly one owned buffer per datagram and nothing besides"
    );
    eprintln!("produce pass queued {datagrams} datagram(s), allocated {allocations} time(s)");
}

#[test]
fn a_drain_pass_never_allocates_the_complete_large_offer() {
    #[cfg(feature = "diagnostics")]
    let _diagnostics_guard = DIAGNOSTICS_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    #[cfg(feature = "diagnostics")]
    {
        ngnet_quic::diagnostics::reset();
        assert!(!ngnet_quic::diagnostics::is_armed());
        // A stale diagnostic-only control must not be evaluated by a feature-enabled but
        // unarmed representative drain. If the unarmed path consults it, the pass below can
        // accept only one byte.
        ngnet_quic::diagnostics::set_test_staging_limit(Some(1));
    }
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let credentials = Credentials::generate();

    let caddr = "127.0.0.1:4563".parse().unwrap();
    let saddr = "127.0.0.1:4564".parse().unwrap();
    let clock = SharedClock::new();
    let (cs, ss) = socket_pair(caddr, saddr);
    let (client, cdrv) = build(Role::Client, cs, clock.clone(), &credentials);
    let (server, sdrv) = build(Role::Server, ss, clock.clone(), &credentials);
    let mut drivers: Vec<Pin<Box<Driver>>> = vec![Box::pin(cdrv), Box::pin(sdrv)];

    let mut connecting = Box::pin(connect(&client, saddr, Some(TEST_SERVER_NAME)));
    let mut accepting = Box::pin(accept(&server));
    let mut client_conn: Option<NgtcpConnection<OsslSession>> = None;
    let mut server_conn: Option<NgtcpConnection<OsslSession>> = None;
    for _ in 0..1000 {
        turn(&mut drivers, &clock, &mut cx);
        if client_conn.is_none()
            && let Poll::Ready(result) = connecting.as_mut().poll(&mut cx)
        {
            client_conn = Some(result.expect("the client handshake failed"));
        }
        if let Some(conn) = client_conn.as_mut() {
            let _ = conn.poll_event(&mut cx);
        }
        if server_conn.is_none()
            && let Poll::Ready(result) = accepting.as_mut().poll(&mut cx)
        {
            server_conn = Some(result.expect("the server accept failed"));
        }
        if client_conn.is_some() && server_conn.is_some() {
            break;
        }
    }
    let mut client_conn = client_conn.expect("a client connection");
    let mut server_conn = server_conn.expect("a server connection");

    // Settle the detached handshake flights and warm the first stream write outside the
    // counted region. The allocation proof below is about the size of one large-body drain,
    // not lazy connection or stream initialization.
    for _ in 0..50 {
        let _ = client_conn.poll_event(&mut cx);
        let _ = server_conn.poll_event(&mut cx);
        turn(&mut drivers, &clock, &mut cx);
    }
    let warm_stream = loop {
        match client_conn.poll_open_uni(&mut cx) {
            Poll::Ready(result) => break result.expect("opening a warm-up stream"),
            Poll::Pending => turn(&mut drivers, &clock, &mut cx),
        }
    };
    let mut warm = OneWrite {
        stream: warm_stream,
        payload: vec![0x3c; 16],
        sent: 0,
        done: false,
    };
    for _ in 0..50 {
        let _ = client_conn.poll_transmit(&mut cx, &mut warm);
        turn(&mut drivers, &clock, &mut cx);
        let _ = server_conn.poll_event(&mut cx);
        if warm.done {
            break;
        }
    }
    assert!(warm.done, "the warm-up stream write did not complete");

    let stream = loop {
        match client_conn.poll_open_uni(&mut cx) {
            Poll::Ready(result) => break result.expect("opening a uni stream"),
            Poll::Pending => turn(&mut drivers, &clock, &mut cx),
        }
    };
    let body_size = 1024 * 1024;
    let mut source = OneWrite {
        stream,
        payload: vec![0x5a; body_size],
        sent: 0,
        done: false,
    };

    // The transport must allocate one stable retained chunk per packet and one owned
    // datagram buffer per packet. Both are bounded below 64 KiB. An allocation larger than
    // that can only be the complete 1 MiB offer (the Phase 1 behavior), while a zero total
    // would prove the drain did no work.
    let (outcome, allocations, oversized) = count_allocations_larger_than(64 * 1024, || {
        client_conn.poll_transmit(&mut cx, &mut source)
    });
    assert!(matches!(outcome, Poll::Ready(Ok(()))));
    assert!(
        source.sent > 0 && source.sent < body_size,
        "one drain pass must accept a bounded prefix, not zero or the complete 1 MiB offer"
    );
    #[cfg(feature = "diagnostics")]
    {
        assert!(
            source.sent > 1,
            "an unarmed drain evaluated the diagnostic-only staging control"
        );
        assert_eq!(
            ngnet_quic::diagnostics::snapshot(),
            ngnet_quic::diagnostics::Snapshot::default(),
            "the representative unarmed drain recorded diagnostics"
        );
    }
    assert!(
        allocations > 0,
        "the counted drain produced no retained chunks or datagrams"
    );
    assert_eq!(
        oversized, 0,
        "a drain pass allocated storage larger than one datagram; the complete offer was staged"
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn feature_enabled_unarmed_diagnostic_checks_allocate_nothing() {
    let _diagnostics_guard = DIAGNOSTICS_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    ngnet_quic::diagnostics::reset();
    let ((armed, snapshot), allocations) = count_allocations(|| {
        ngnet_quic::diagnostics::record_packet(1, ngnet_quic::Role::Client, true);
        ngnet_quic::diagnostics::record_release(1, ngnet_quic::Role::Client, 7);
        ngnet_quic::diagnostics::record_timer_rearm(1, ngnet_quic::Role::Client);
        ngnet_quic::diagnostics::record_wake_registration(1, ngnet_quic::Role::Client);
        ngnet_quic::diagnostics::record_park(1, ngnet_quic::Role::Client);
        (
            ngnet_quic::diagnostics::is_armed(),
            ngnet_quic::diagnostics::snapshot(),
        )
    });
    assert!(!armed);
    assert_eq!(snapshot, ngnet_quic::diagnostics::Snapshot::default());
    assert_eq!(
        allocations, 0,
        "feature-enabled unarmed diagnostic checks must not allocate"
    );
}
