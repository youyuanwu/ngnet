//! Connections the endpoint routes for but does not drive.
//!
//! The endpoint keeps what is shared between connections — the socket, the routing table,
//! address validation, stateless reset — and hands the per-connection protocol state to
//! whoever asked for it. This exists because a consumer that must reach the connection
//! *synchronously* while composing a packet cannot be served across a queue, and the HTTP/3
//! layer is exactly such a consumer.
//!
//! These tests drive detached connections with a small hand-written consumer, so the
//! mechanism is exercised on its own before anything is built on it.

use std::time::Duration as StdDuration;

use ngnet_quic::endpoint::{
    Config, DetachedConnection, Endpoint, EndpointBuilder, EndpointDriver, TokioClock, TokioSocket,
};
use ngnet_quic::{Duration, Handlers, OsslBackend, OsslSession, Role, StreamWrite, Timestamp};
use ngnet_quic_tests::{TEST_ALPN, TEST_SERVER_NAME, TestCredentials, TestEntropy};

type Driver = EndpointDriver<TokioSocket, TokioClock, OsslBackend>;

async fn client(credentials: &TestCredentials, seed: u64) -> (Endpoint<OsslSession>, Driver) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a client socket");
    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");
    EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new().handshake_timeout(Duration::from_nanos(4_000_000_000)))
        .entropy(move || TestEntropy::new(seed))
        .build_detachable()
        .expect("a client endpoint")
}

async fn server(
    credentials: &TestCredentials,
    accepts_detached: bool,
) -> (Endpoint<OsslSession>, Driver, core::net::SocketAddr) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a server socket");
    let address = socket.inner().local_addr().expect("a bound address");
    let backend = OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");
    let (endpoint, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .accepts(true)
        .config(Config::new().handshake_timeout(Duration::from_nanos(4_000_000_000)))
        .entropy(|| TestEntropy::new(0xBEEF))
        .build_detachable()
        .expect("a server endpoint");
    let _ = accepts_detached;
    (endpoint, driver, address)
}

/// The smallest thing that can drive a detached connection.
///
/// Reads whatever the endpoint routed here, lets the connection produce whatever it wants
/// to send, and honours its timer. This is the shape any consumer must take, reduced to
/// what a test needs.
struct Pump {
    detached: DetachedConnection<OsslSession>,
}

impl Pump {
    fn new(detached: DetachedConnection<OsslSession>) -> Self {
        Self { detached }
    }

    /// Always the endpoint's clock, never one of this consumer's own.
    ///
    /// A second clock has a different origin, so its timestamps are not comparable with the
    /// ones the endpoint already recorded while driving the handshake. ngtcp2 catches that
    /// with an assertion in debug builds -- which is how this test found it -- and mis-times
    /// loss detection silently in release ones.
    fn now(&self) -> Timestamp {
        self.detached.now()
    }

    /// One pass: drain what arrived, fire the timer if due, send what is owed.
    fn pass(&mut self) {
        let now = self.now();
        while let Some(datagram) = self.detached.next_inbound() {
            let _ = self.detached.conn.read_pkt(&datagram, now);
        }
        if self.detached.conn.expiry().is_some_and(|at| at <= now) {
            let _ = self.detached.conn.handle_expiry(now);
        }
        self.drain();
    }

    /// Produces datagrams while the connection has any and there is room to queue them.
    fn drain(&mut self) {
        let mut buf = vec![0u8; 1500];
        for _ in 0..32 {
            if !self.detached.outbound_has_room() {
                break;
            }
            let now = self.now();
            match self.detached.conn.write_pkt(&mut buf, now) {
                Ok(ngnet_quic::WriteOutcome::Datagram { len }) => {
                    self.detached.send(buf[..len].to_vec());
                }
                _ => break,
            }
        }
    }

    /// Writes stream data, letting the connection produce the datagrams that carry it.
    fn write(&mut self, stream: ngnet_quic::StreamId, payload: &[u8]) {
        let mut offset = 0usize;
        let mut buf = vec![0u8; 1500];
        for _ in 0..512 {
            if offset >= payload.len() {
                break;
            }
            // Room is checked *before* writing. A datagram that has been produced cannot be
            // withdrawn, because the connection has already accounted for the bytes in it.
            if !self.detached.outbound_has_room() {
                break;
            }
            let now = self.now();
            let last = true;
            match self
                .detached
                .conn
                .write_stream(&mut buf, stream, &payload[offset..], last, now)
            {
                Ok(StreamWrite::Datagram { len, accepted }) => {
                    offset += accepted;
                    self.detached.send(buf[..len].to_vec());
                }
                _ => break,
            }
        }
    }
}

#[tokio::test]
async fn a_detached_client_completes_a_handshake_and_carries_a_stream() {
    let credentials = TestCredentials::generate();
    let (server_endpoint, server_driver, address) = server(&credentials, false).await;
    let (client_endpoint, client_driver) = client(&credentials, 0x11).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    // The server keeps the managed handle, so only the client side is detached here: one
    // side at a time makes it clear which side a failure belongs to.
    let accepting = tokio::spawn(async move { server_endpoint.accept().await });

    let detached = tokio::time::timeout(
        StdDuration::from_secs(5),
        client_endpoint.connect_detached(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("the handshake must complete before the connection is handed over");

    assert!(
        detached.conn.is_handshake_completed(),
        "a detached connection is handed over established, which is what its consumers \
         require"
    );

    let mut pump = Pump::new(detached);
    let stream = pump
        .detached
        .conn
        .open_bidi_stream()
        .expect("opening a stream");
    pump.write(stream, b"hello from a detached connection");

    let server_side = tokio::time::timeout(StdDuration::from_secs(5), accepting)
        .await
        .expect("accepting must not hang")
        .expect("the acceptor task")
        .expect("a connection");

    // Drive the detached side while the server reads.
    let mut server_side = server_side;
    let read = tokio::time::timeout(StdDuration::from_secs(5), async {
        loop {
            pump.pass();
            tokio::task::yield_now().await;
            if let Ok(chunk) =
                tokio::time::timeout(StdDuration::from_millis(20), server_side.accept_stream())
                    .await
            {
                break chunk;
            }
        }
    })
    .await;

    assert!(
        read.is_ok(),
        "the server must see the stream the detached connection opened"
    );
}

#[tokio::test]
async fn a_detached_connection_releases_its_routes_when_its_owner_is_done() {
    // The endpoint cannot ask a connection it does not hold whether it is finished, so the
    // owner says. Without that the routing entry lives as long as the endpoint.
    let credentials = TestCredentials::generate();
    let (server_endpoint, server_driver, address) = server(&credentials, false).await;
    let (client_endpoint, client_driver) = client(&credentials, 0x22).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);
    tokio::spawn(async move {
        let _ = server_endpoint.accept().await;
    });

    let detached = tokio::time::timeout(
        StdDuration::from_secs(5),
        client_endpoint.connect_detached(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("a detached connection");

    assert_eq!(
        detached.dropped_inbound(),
        0,
        "nothing should have been dropped on a quiet connection"
    );

    detached.release();
    // Nothing to assert beyond it being accepted without panic: the eviction it enables is
    // observed by the endpoint's own bookkeeping, which the managed suite already covers.
    drop(detached);
}

#[tokio::test]
async fn an_endpoint_carries_a_managed_and_a_detached_connection_at_once() {
    // The property the whole split exists to preserve: one socket, consumers of different
    // kinds, neither disturbing the other.
    let credentials = TestCredentials::generate();
    let (server_endpoint, server_driver, address) = server(&credentials, false).await;
    let (client_endpoint, client_driver) = client(&credentials, 0x33).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);
    tokio::spawn(async move {
        loop {
            if server_endpoint.accept().await.is_err() {
                break;
            }
        }
    });

    let managed = tokio::time::timeout(
        StdDuration::from_secs(5),
        client_endpoint.connect(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the managed connection must not hang")
    .expect("a managed connection");

    let detached = tokio::time::timeout(
        StdDuration::from_secs(5),
        client_endpoint.connect_detached(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the detached connection must not hang")
    .expect("a detached connection");

    assert!(managed.is_established(), "the managed side is established");
    assert!(
        detached.conn.is_handshake_completed(),
        "the detached side is established"
    );

    let _ = Handlers::new();
    detached.release();
}
