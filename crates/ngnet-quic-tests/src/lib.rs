//! Test harness for driving `ngnet-quic` through real handshakes.
//!
//! `ngnet-quic` is sans-I/O: it never touches a socket and never reads a clock. That makes
//! it easy to drive, but it means something has to play the part of the network. This crate
//! supplies two such somethings — an in-process relay that moves datagrams between two
//! connections directly, and a loopback-UDP driver that puts them through the kernel.
//!
//! It is a separate crate because a handshake needs certificates, and generating them needs
//! a dependency the wrapper is forbidden from having.

use std::sync::atomic::{AtomicU64, Ordering};

use ngnet_quic::{
    ConnBuilder, ConnectionId, EntropySource, ExpiryOutcome, Handlers, OsslBackend, OsslSession,
    ReadOutcome, Result, Role, Settings, Timestamp, TlsBackend, TransportParams, WriteOutcome,
};

pub mod udp;

/// The ALPN protocol the tests negotiate.
pub const TEST_ALPN: &[u8] = b"ngnet-test";

/// The name the generated certificate is issued for.
pub const TEST_SERVER_NAME: &str = "localhost";

/// A clock the test controls.
///
/// The crate under test reads no clock, so time is entirely the harness's to invent. Being
/// able to jump forward is what makes the idle-timeout and loss-recovery paths testable at
/// all.
#[derive(Debug)]
pub struct TestClock {
    now: AtomicU64,
}

impl TestClock {
    /// Starts at a non-zero instant.
    ///
    /// Non-zero deliberately: ngtcp2 treats a zero `initial_ts` as a real time, and a clock
    /// starting there makes every duration look like an eternity.
    pub fn new() -> Self {
        Self {
            now: AtomicU64::new(1_000_000_000),
        }
    }

    /// The current reading.
    pub fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now.load(Ordering::Relaxed))
            .expect("the test clock never reaches the reserved sentinel")
    }

    /// Moves the clock forward.
    pub fn advance(&self, nanos: u64) {
        self.now.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Moves the clock to a given instant, if that is forward.
    pub fn advance_to(&self, when: Timestamp) {
        let target = when.as_nanos();
        let _ = self
            .now
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (target > current).then_some(target)
            });
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

/// A deterministic entropy source.
///
/// **Not suitable for anything but tests.** It is a counter, which is precisely what an
/// attacker would predict. It exists so a failing run can be reproduced byte for byte.
///
/// Each instance starts from a different seed so that a client and a server in the same test
/// do not generate identical connection identifiers.
pub struct TestEntropy {
    state: u64,
}

impl TestEntropy {
    /// Creates a source with the given seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl EntropySource for TestEntropy {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        // xorshift64*, chosen because it is four lines and has no dependencies. Its
        // statistical quality is irrelevant here; its reproducibility is the point.
        for slot in dest.iter_mut() {
            self.state ^= self.state >> 12;
            self.state ^= self.state << 25;
            self.state ^= self.state >> 27;
            *slot = self.state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes()[0];
        }
        Ok(())
    }
}

/// A self-signed certificate and its key, in PEM.
pub struct TestCredentials {
    /// The certificate chain.
    pub certificate_pem: String,
    /// The private key.
    pub key_pem: String,
}

impl TestCredentials {
    /// Generates a certificate for [`TEST_SERVER_NAME`].
    pub fn generate() -> Self {
        let cert = rcgen::generate_simple_self_signed(vec![TEST_SERVER_NAME.to_string()])
            .expect("generating a self-signed certificate");
        Self {
            certificate_pem: cert.cert.pem(),
            key_pem: cert.signing_key.serialize_pem(),
        }
    }
}

/// Builds a client backend that trusts the given certificate and verifies against it.
pub fn client_backend(trust_anchor_pem: &str) -> OsslBackend {
    OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(trust_anchor_pem)
        // Off, so the test depends on the anchor it was given rather than on whatever the
        // machine happens to trust.
        .use_system_trust_store(false)
        .build()
        .expect("building a client backend")
}

/// Builds a server backend from generated credentials.
pub fn server_backend(credentials: &TestCredentials) -> OsslBackend {
    OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("building a server backend")
}

/// The connection type these tests drive.
pub type TestConn<'h> = ngnet_quic::Conn<'h, OsslSession>;

/// Builds a client connection.
pub fn client_conn<'h>(
    backend: &OsslBackend,
    clock: &TestClock,
    handlers: Handlers<'h>,
    local: std::net::SocketAddr,
    remote: std::net::SocketAddr,
    server_name: Option<&str>,
) -> Result<TestConn<'h>> {
    let session = backend.new_session(Role::Client, server_name)?;
    ConnBuilder::new(
        Role::Client,
        Settings::new(clock.now()),
        TransportParams::new(),
        Box::new(TestEntropy::new(0x1234_5678)),
        session,
        core_addr(local),
        core_addr(remote),
    )
    .build(handlers)
}

/// Builds a server connection from what a client's first packet carried.
pub fn server_conn<'h>(
    backend: &OsslBackend,
    clock: &TestClock,
    handlers: Handlers<'h>,
    local: std::net::SocketAddr,
    remote: std::net::SocketAddr,
    original_dcid: &ConnectionId,
    client_scid: ConnectionId,
) -> Result<TestConn<'h>> {
    let session = backend.new_session(Role::Server, None)?;
    ConnBuilder::new(
        Role::Server,
        Settings::new(clock.now()),
        TransportParams::new().original_dcid(original_dcid),
        Box::new(TestEntropy::new(0x8765_4321)),
        session,
        core_addr(local),
        core_addr(remote),
    )
    .dcid(client_scid)
    .build(handlers)
}

/// Converts a `std` socket address into the `core` one the crate takes.
///
/// The crate accepts `core::net` because a test asserts its sources never name `std::net`;
/// the two types are the same shape, and this is where the conversion belongs.
pub fn core_addr(addr: std::net::SocketAddr) -> core::net::SocketAddr {
    addr
}

/// Drains one connection's outbound datagrams.
///
/// Loops until the connection stops offering datagrams, which is the shape every caller of
/// this API must adopt — a single write is almost never enough.
pub fn drain(conn: &mut TestConn<'_>, clock: &TestClock) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 1500];
    // Bounded so a mistake fails the test rather than hanging it.
    for _ in 0..64 {
        match conn.write_pkt(&mut buf, clock.now())? {
            WriteOutcome::Datagram { len } => out.push(buf[..len].to_vec()),
            WriteOutcome::Idle | WriteOutcome::Blocked => break,
        }
    }
    Ok(out)
}

/// Relays datagrams between two connections until both go quiet.
///
/// Returns how many datagrams crossed in total. Honours each side's reported deadline,
/// because a handshake that is never told time has passed will stop halfway through
/// retransmission and look like a hang.
pub fn pump(
    client: &mut TestConn<'_>,
    server: &mut TestConn<'_>,
    clock: &TestClock,
    rounds: usize,
) -> Result<usize> {
    let mut moved = 0;

    for _ in 0..rounds {
        let mut progressed = false;

        for datagram in drain(client, clock)? {
            moved += 1;
            progressed = true;
            match server.read_pkt(&datagram, clock.now())? {
                ReadOutcome::Processed => {}
                terminal => {
                    eprintln!("server ended the exchange: {terminal:?}");
                    return Ok(moved);
                }
            }
        }

        for datagram in drain(server, clock)? {
            moved += 1;
            progressed = true;
            match client.read_pkt(&datagram, clock.now())? {
                ReadOutcome::Processed => {}
                terminal => {
                    eprintln!("client ended the exchange: {terminal:?}");
                    return Ok(moved);
                }
            }
        }

        if client.is_handshake_completed() && server.is_handshake_completed() && !progressed {
            break;
        }

        if !progressed {
            // Nothing to send and nothing received: only a timer can move things on.
            let next = [client.expiry(), server.expiry()]
                .into_iter()
                .flatten()
                .min();
            match next {
                Some(deadline) => {
                    clock.advance_to(deadline);
                    clock.advance(1);
                    if client.handle_expiry(clock.now())? == ExpiryOutcome::IdleClose {
                        break;
                    }
                    if server.handle_expiry(clock.now())? == ExpiryOutcome::IdleClose {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    Ok(moved)
}
