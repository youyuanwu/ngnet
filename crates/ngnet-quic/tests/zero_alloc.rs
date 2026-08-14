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
    Backend as TlsBackend, ConnBuilder, EntropySource, Handlers, Inspection, OsslBackend,
    OsslSession, ReadOutcome, Result, Role, Settings, StreamWrite, Timestamp, TransportParams,
    Verify, WriteOutcome, inspect,
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
    // A count of zero proves nothing if the loop produced no datagram, so the region also
    // records how many bytes it wrote and asserts a datagram genuinely came out. The
    // warm-up above consumes the first Initial; the client still has a second one to send,
    // so at least one pass here writes a real packet.
    let ((datagrams, bytes), allocations) = count_allocations(|| {
        let mut datagrams = 0usize;
        let mut bytes = 0usize;
        for _ in 0..8 {
            let now = Timestamp::from_nanos(when).unwrap();
            when += 2_000_000;
            match conn.write_pkt(&mut buf, now) {
                Ok(WriteOutcome::Datagram { len }) => {
                    datagrams += 1;
                    bytes += len;
                }
                Ok(WriteOutcome::Idle | WriteOutcome::Blocked) => {}
                Err(_) => break,
            }
        }
        (datagrams, bytes)
    });

    assert!(
        datagrams > 0 && bytes > 0,
        "the send loop produced no datagram ({datagrams} datagrams, {bytes} bytes), so a \
         zero allocation count would prove nothing"
    );
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

    // A count of zero is vacuous if the queries never ran, so the region reports the last
    // expiry it read and how many times it read one. The assertions below require both a
    // concrete deadline and that the loop actually executed -- a region that queried nothing
    // would satisfy neither.
    let ((ran, deadline), allocations) = count_allocations(|| {
        let mut ran = 0usize;
        let mut deadline = None;
        for _ in 0..64 {
            deadline = conn.expiry();
            let _ = conn.in_closing_period();
            let _ = conn.in_draining_period();
            let _ = conn.is_handshake_completed();
            ran += 1;
        }
        (ran, deadline)
    });

    assert!(
        ran > 0,
        "the query loop never ran, so a zero count proves nothing"
    );
    assert!(
        deadline.is_some(),
        "the connection reported no expiry, so the region queried nothing concrete"
    );
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

// The read and handshake regions below need two connections that have actually completed a
// handshake, driven entirely in memory. The wrapper is sans-I/O, so a datagram is moved from
// one side to the other by hand; there is no socket and no runtime. The certificate is the
// committed test one -- generating it would need a dev-dependency the crate forbids.

/// The committed self-signed certificate for `localhost`.
const TEST_CERT_PEM: &str = include_str!("data/test-cert.pem");
/// Its private key. Public and worthless; see `tests/data/README.md`.
const TEST_KEY_PEM: &str = include_str!("data/test-key.pem");

/// A clock the harness advances by hand.
///
/// ngtcp2 paces its sending, so a clock that never moves yields one datagram and then
/// silence. Advancing time between passes is what lets a handshake finish.
struct HandClock {
    now: Cell<u64>,
}

impl HandClock {
    fn new() -> Self {
        // Non-zero: ngtcp2 reads a zero start as a real instant an eternity ago.
        Self {
            now: Cell::new(1_000_000_000),
        }
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now.get()).unwrap()
    }

    fn advance(&self, nanos: u64) {
        self.now.set(self.now.get() + nanos);
    }

    fn advance_to(&self, when: Timestamp) {
        let target = when.as_nanos();
        if target > self.now.get() {
            self.now.set(target);
        }
    }
}

/// A connection under test.
type HandConn = ngnet_quic::Conn<'static, OsslSession>;

fn client_backend() -> OsslBackend {
    OsslBackend::builder(Role::Client)
        .alpn("h3")
        .verify(Verify::DangerouslyAcceptAnyCertificate)
        .build()
        .expect("a client backend")
}

fn server_backend() -> OsslBackend {
    OsslBackend::builder(Role::Server)
        .alpn("h3")
        .certificate_chain_pem(TEST_CERT_PEM)
        .private_key_pem(TEST_KEY_PEM)
        .build()
        .expect("a server backend")
}

/// Drains a connection's outbound datagrams, advancing the clock for pacing.
fn drain(conn: &mut HandConn, clock: &HandClock) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 1500];
    for _ in 0..64 {
        match conn
            .write_pkt(&mut buf, clock.now())
            .expect("writing a packet")
        {
            WriteOutcome::Datagram { len } => {
                out.push(buf[..len].to_vec());
                clock.advance(2_000_000);
            }
            WriteOutcome::Idle | WriteOutcome::Blocked => break,
        }
    }
    out
}

/// Relays datagrams between two connections until both go quiet, honouring their deadlines.
fn pump(client: &mut HandConn, server: &mut HandConn, clock: &HandClock, rounds: usize) {
    for _ in 0..rounds {
        let mut progressed = false;
        for datagram in drain(client, clock) {
            progressed = true;
            if server
                .read_pkt(&datagram, clock.now())
                .expect("server read")
                != ReadOutcome::Processed
            {
                return;
            }
        }
        for datagram in drain(server, clock) {
            progressed = true;
            if client
                .read_pkt(&datagram, clock.now())
                .expect("client read")
                != ReadOutcome::Processed
            {
                return;
            }
        }
        if client.is_handshake_completed() && server.is_handshake_completed() && !progressed {
            break;
        }
        if !progressed {
            let next = [client.expiry(), server.expiry()]
                .into_iter()
                .flatten()
                .min();
            match next {
                Some(deadline) => {
                    clock.advance_to(deadline);
                    clock.advance(1);
                    let _ = client.handle_expiry(clock.now());
                    let _ = server.handle_expiry(clock.now());
                }
                None => break,
            }
        }
    }
}

/// Builds a client and a server and drives them to a completed handshake, in memory.
///
/// The server can only be built from what the client's first packet carries, because
/// `original_dcid` is the client's initial destination identifier and ngtcp2 requires a
/// server to be told it.
fn establish(
    client_backend: &OsslBackend,
    server_backend: &OsslBackend,
    clock: &HandClock,
) -> (HandConn, HandConn) {
    let client_session = client_backend
        .new_session(Role::Client, Some("localhost"))
        .expect("a client session");
    let mut client = ConnBuilder::new(
        Role::Client,
        Settings::new(clock.now()),
        TransportParams::new(),
        Box::new(StubEntropy(0)),
        client_session,
        "127.0.0.1:1000".parse().unwrap(),
        "127.0.0.1:2000".parse().unwrap(),
    )
    .build(Handlers::new())
    .expect("building the client");

    let first_flight = drain(&mut client, clock);
    assert!(
        !first_flight.is_empty(),
        "a fresh client must have a first flight to send"
    );

    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("the first flight should be a supported long header, got {other:?}"),
    };

    let server_session = server_backend
        .new_session(Role::Server, None)
        .expect("a server session");
    let mut server = ConnBuilder::new(
        Role::Server,
        Settings::new(clock.now()),
        TransportParams::new().original_dcid(&original_dcid),
        Box::new(StubEntropy(64)),
        server_session,
        "127.0.0.1:2000".parse().unwrap(),
        "127.0.0.1:1000".parse().unwrap(),
    )
    .dcid(client_scid)
    .build(Handlers::new())
    .expect("building the server");

    for datagram in &first_flight {
        let _ = server.read_pkt(datagram, clock.now());
    }
    pump(&mut client, &mut server, clock, 32);

    (client, server)
}

#[test]
fn reading_a_packet_allocates_nothing_and_is_processed() {
    // SC-008. Reading a datagram on an established connection decrypts in place, so it must
    // allocate nothing -- and a zero count would be vacuous unless the packet was genuinely
    // taken in, so the region asserts the read reported `Processed`.
    let client_backend = client_backend();
    let server_backend = server_backend();
    let clock = HandClock::new();
    let (mut client, mut server) = establish(&client_backend, &server_backend, &clock);
    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the harness did not establish a connection to read on"
    );

    // A 1-RTT datagram carrying stream data, produced outside the count. This is the packet
    // the server will read inside it.
    let stream = client.open_uni_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];
    let payload = [0x5au8; 64];
    let datagram = loop {
        match client
            .write_stream(&mut buf, stream, &payload, true, clock.now())
            .expect("writing stream data")
        {
            StreamWrite::Datagram { len, .. } if len > 0 => break buf[..len].to_vec(),
            StreamWrite::Datagram { .. } => clock.advance(2_000_000),
            other => panic!("the client produced no datagram to read: {other:?}"),
        }
    };

    // Warm the read path once, outside the count, so any lazily-initialised state is ready.
    // A duplicate 1-RTT packet is dropped rather than rejected, so this does not consume the
    // ability to observe a processed read below -- both report `Processed`.
    assert_eq!(
        server.read_pkt(&datagram, clock.now()).expect("warm read"),
        ReadOutcome::Processed
    );

    let (outcome, allocations) = count_allocations(|| {
        server
            .read_pkt(&datagram, clock.now())
            .expect("counted read")
    });

    assert_eq!(
        outcome,
        ReadOutcome::Processed,
        "the packet was not processed, so a zero allocation count would prove nothing"
    );
    assert_eq!(
        allocations, 0,
        "reading a packet on an established connection allocated {allocations} times; \
         ngtcp2 decrypts in place and the wrapper is supposed to add no copy"
    );
}

#[test]
fn establishing_a_connection_bounds_its_allocations() {
    // SC-006 (counting half) and FR-011. Establishing a connection is not allocation-free --
    // a handshake sets up TLS, transport parameters and stream state, and this in-memory
    // harness copies each datagram it relays. So this region is a BOUND, not an attribution:
    // it records the total the code produces today and fails only if a change pushes it
    // higher. Other forced allocations share this region, which is why it does not assert
    // zero. Its non-vacuity comes from asserting both roles reached a completed handshake.
    let client_backend = client_backend();
    let server_backend = server_backend();

    // Warm the backends and the allocator's own lazy state outside the count.
    {
        let clock = HandClock::new();
        let _ = establish(&client_backend, &server_backend, &clock);
    }

    let clock = HandClock::new();
    let ((client, server), allocations) =
        count_allocations(|| establish(&client_backend, &server_backend, &clock));

    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the handshake did not complete for both roles, so the recorded total is meaningless"
    );

    // The recorded bound. The observed total on this harness is 156 in both debug and
    // release; the handshake is deterministic in its allocation count even though its key
    // exchange is not. The bound sits a little above that so an OpenSSL or allocator
    // difference between environments does not fail it, while a change that allocates per
    // datagram or per pass -- the kind this audit removes -- would push it well past.
    const BOUND: usize = 220;
    assert!(
        allocations <= BOUND,
        "establishing a connection allocated {allocations} times, above the recorded bound \
         of {BOUND}; if this is an intended increase, raise the bound deliberately"
    );
    eprintln!("establishing a connection allocated {allocations} times");
}

// Phase 2's region needs a driver pass over an established connection, which is the endpoint
// layer rather than a bare `Conn`. The harness below is the in-memory, runtime-free one the
// endpoint's own integration tests use, rebuilt here from the crate's public
// `endpoint::testing` surface so this test needs no extra dependency.
#[cfg(feature = "endpoint")]
mod driver_pass {
    use super::{StubEntropy, TEST_CERT_PEM, TEST_KEY_PEM, count_allocations};
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use ngnet_quic::endpoint::testing::{TestClock, TestSocket, socket_pair};
    use ngnet_quic::endpoint::{Config, Connection, Endpoint, EndpointBuilder, EndpointDriver};
    use ngnet_quic::{OsslBackend, OsslSession, Role};

    type Driver = EndpointDriver<TestSocket, TestClock, OsslBackend>;

    fn build(role: Role, socket: TestSocket, clock: TestClock) -> (Endpoint<OsslSession>, Driver) {
        let backend = match role {
            Role::Client => OsslBackend::builder(Role::Client)
                .alpn("h3")
                .trust_anchor_pem(TEST_CERT_PEM)
                .use_system_trust_store(false)
                .build()
                .expect("a client backend"),
            Role::Server => OsslBackend::builder(Role::Server)
                .alpn("h3")
                .certificate_chain_pem(TEST_CERT_PEM)
                .private_key_pem(TEST_KEY_PEM)
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
        builder.build().expect("an endpoint")
    }

    fn poll_all(drivers: &mut [Pin<Box<Driver>>], cx: &mut Context<'_>) {
        for d in drivers.iter_mut() {
            let _ = d.as_mut().poll(cx);
        }
    }

    #[test]
    fn a_driver_pass_over_an_established_connection_does_not_allocate_for_iteration() {
        // Phase 2. `service` and `flush` each used to collect a `Vec<u64>` of connection
        // indices on every pass, so no pass could ever report zero however little it did.
        // With those replaced by a reusable scratch, a pass allocates only for the datagram
        // buffer `next_datagram` still takes eagerly -- which Phase 5 removes -- and nothing
        // for walking the connection list. The idle region below asserts exactly that: one
        // established connection, one allocation, which the inversion (restoring the index
        // vectors) turns into three.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let (caddr, saddr) = (
            "127.0.0.1:4433".parse().unwrap(),
            "127.0.0.1:4434".parse().unwrap(),
        );
        let clock = TestClock::new();
        let (cs, ss) = socket_pair(caddr, saddr);
        let (client, cdrv) = build(Role::Client, cs, clock.clone());
        let (server, sdrv) = build(Role::Server, ss, clock.clone());
        let mut drivers: Vec<Pin<Box<Driver>>> = vec![Box::pin(cdrv), Box::pin(sdrv)];

        let mut connecting = Box::pin(client.connect(saddr, Some("localhost")));
        let mut accepting = Box::pin(server.accept());
        let mut cside: Option<Connection> = None;
        let mut sside: Option<Connection> = None;
        for _ in 0..400 {
            poll_all(&mut drivers, &mut cx);
            if cside.is_none()
                && let Poll::Ready(r) = connecting.as_mut().poll(&mut cx)
            {
                cside = Some(r.expect("the client handshake failed"));
            }
            if sside.is_none()
                && let Poll::Ready(r) = accepting.as_mut().poll(&mut cx)
            {
                sside = Some(r.expect("the server accept failed"));
            }
            if cside.is_some() && sside.is_some() {
                break;
            }
            clock.advance(2_000_000);
        }
        let mut cside = cside.expect("a client connection");
        let mut sside = sside.expect("a server connection");
        assert!(
            cside.is_established() && sside.is_established(),
            "the harness did not establish a connection for the pass to service"
        );

        // The pass does real work: 128 bytes of application data cross the connection under
        // the driver's own passes, and the server accepts the stream. A region that measured
        // a pass over a dead or empty connection would prove nothing, so this establishes
        // that the driver is genuinely servicing a live connection before anything is counted.
        let sid = {
            let mut opening = cside.open_uni();
            loop {
                match Pin::new(&mut opening).poll(&mut cx) {
                    Poll::Ready(r) => break r.expect("opening a stream"),
                    Poll::Pending => {
                        poll_all(&mut drivers, &mut cx);
                        clock.advance(2_000_000);
                    }
                }
            }
        };
        cside
            .write(sid, &[0x5au8; 128], true)
            .expect("writing stream data");
        let mut accepted = None;
        for _ in 0..64 {
            poll_all(&mut drivers, &mut cx);
            let mut accepting = sside.accept_stream();
            if let Poll::Ready(r) = Pin::new(&mut accepting).poll(&mut cx) {
                accepted = Some(r.expect("accepting the stream"));
                break;
            }
            clock.advance(2_000_000);
        }
        assert!(
            accepted.is_some(),
            "the server never saw the stream, so the driver pass did no work"
        );

        // Quiesce, then stop the clock so no timer re-arms and no retransmission fires: what
        // remains is a bare pass over an established, idle connection.
        for _ in 0..64 {
            poll_all(&mut drivers, &mut cx);
            clock.advance(2_000_000);
        }
        // Warm the scratch and the armed sleep at the now-fixed clock, outside the count.
        poll_all(&mut drivers, &mut cx);

        // One idle pass of the client driver over its single established connection.
        let (_, allocations) = count_allocations(|| {
            let _ = drivers[0].as_mut().poll(&mut cx);
        });

        assert!(
            cside.is_established() && sside.is_established(),
            "the connection did not survive the pass, so the count is meaningless"
        );
        // At most one buffer per serviced connection, and here there is one connection. That
        // one allocation is `next_datagram` taking a send buffer before it knows there is
        // nothing to send -- removed in Phase 5, which is why this is `<=` rather than `==`.
        // The point of this phase is the absence of the two index vectors, each of which the
        // inversion check restores to push this count to three.
        assert!(
            allocations <= 1,
            "an idle driver pass over one established connection allocated {allocations} \
             times; more than one buffer means iterating the connections is still allocating"
        );
        eprintln!("idle driver pass allocated {allocations} times");
    }
}
