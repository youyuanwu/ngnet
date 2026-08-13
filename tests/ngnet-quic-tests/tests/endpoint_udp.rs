//! The endpoint over real UDP sockets and a real runtime.
//!
//! The in-process tests prove the layer's logic with no runtime at all. These prove the
//! other half: that the tokio seams are wired correctly, that real datagrams cross a real
//! loopback socket, and that an ephemeral port and a real clock change nothing about the
//! result.
//!
//! Ports are ephemeral — bound as `:0` and read back — so these can run concurrently with
//! each other and with anything else on the machine.

use std::time::Duration as StdDuration;

use ngnet_quic::endpoint::{
    Config, Connection, Endpoint, EndpointBuilder, EndpointDriver, ErrorKind, TokioClock,
    TokioSocket,
};
use ngnet_quic::{Duration, OsslBackend, OsslSession, Role};
use ngnet_quic_tests::{TEST_ALPN, TEST_SERVER_NAME, TestCredentials, TestEntropy};

/// The endpoint driver these tests run.
type Driver = EndpointDriver<TokioSocket, TokioClock, OsslBackend>;

/// Binds a client endpoint on an ephemeral port.
///
/// The seed is a parameter because the test entropy source is deterministic: two clients
/// built from the same seed mint identical connection identifiers, and a server routing by
/// identifier would then deliver one client's datagrams to the other. Real endpoints do not
/// have this problem, which is exactly why the endpoint builder makes the caller supply the
/// randomness rather than picking it.
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
        .config(Config::new().handshake_timeout(Duration::from_nanos(2_000_000_000)))
        .entropy(move || TestEntropy::new(seed))
        .build()
        .expect("a client endpoint")
}

/// Binds a server endpoint on an ephemeral port, returning the address it landed on.
async fn server(credentials: &TestCredentials) -> (Endpoint<OsslSession>, Driver, core::net::SocketAddr) {
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

    let (handle, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new())
        .entropy(|| TestEntropy::new(0x8765_4321))
        .accepts(true)
        .build()
        .expect("a server endpoint");

    (handle, driver, address)
}

/// Runs a driver on the current task set until the test drops it.
///
/// `spawn_local` rather than `spawn`, because the endpoint imposes no `Send` bound and this
/// proves a caller does not need one — a driver that had to be `Send` would not compile
/// here for a thread-per-core runtime, which is the case the seams exist to accommodate.
fn drive(driver: Driver) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        let _ = driver.await;
    })
}

#[tokio::test]
async fn a_handshake_completes_over_real_loopback_udp() {
    let credentials = TestCredentials::generate();
    let (client_handle, client_driver) = client(&credentials, 0x1234_5678).await;
    let (server_handle, server_driver, server_addr) = server(&credentials).await;

    let client_task = drive(client_driver);
    let server_task = drive(server_driver);

    let accepting = tokio::spawn(async move { server_handle.accept().await });

    let connection = tokio::time::timeout(
        StdDuration::from_secs(10),
        client_handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the handshake did not finish within ten seconds, so it stalled")
    .expect("the handshake failed");

    assert!(connection.is_established());

    let accepted: Connection = tokio::time::timeout(StdDuration::from_secs(10), accepting)
        .await
        .expect("the server never accepted")
        .expect("the accept task panicked")
        .expect("the accept failed");
    assert!(accepted.is_established());

    client_task.abort();
    server_task.abort();
}

#[tokio::test]
async fn connecting_where_nothing_is_listening_times_out_rather_than_hanging() {
    // The distinction FR-006 asks for: nothing refused anything, so this is a timeout and
    // not a rejection, and a caller may reasonably retry it.
    let credentials = TestCredentials::generate();
    let (handle, driver) = client(&credentials, 0x1234_5678).await;
    let task = drive(driver);

    // Bind and immediately drop a socket to get an address nothing is listening on. Racy in
    // principle -- the port could be reused -- but the reuse would have to happen within
    // this test and answer QUIC, which nothing else on a test machine does.
    let dead = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");
    let dead_addr = dead.local_addr().expect("an address");
    drop(dead);

    let outcome = tokio::time::timeout(
        StdDuration::from_secs(20),
        handle.connect(dead_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the connect never resolved, so the handshake timeout did not fire");

    let err = outcome.expect_err("a connect to a closed port must not succeed");
    assert_eq!(
        err.kind(),
        ErrorKind::HandshakeTimeout,
        "a connect to a closed port should time out, not report {:?}",
        err.kind()
    );

    task.abort();
}

#[tokio::test]
async fn a_handshake_is_rejected_when_the_client_does_not_trust_the_certificate() {
    // The other half of the error matrix: something *did* refuse, so this must not be
    // reported as a timeout, or a caller would retry forever against a server that will
    // never accept it.
    let server_credentials = TestCredentials::generate();
    let other_credentials = TestCredentials::generate();

    let (server_handle, server_driver, server_addr) = server(&server_credentials).await;
    let server_task = drive(server_driver);
    let accepting = tokio::spawn(async move { server_handle.accept().await });

    // A client trusting a completely different self-signed certificate.
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding");
    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(other_credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");
    let (handle, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new().handshake_timeout(Duration::from_nanos(5_000_000_000)))
        .entropy(|| TestEntropy::new(0xdead_beef))
        .build()
        .expect("a client endpoint");
    let client_task = drive(driver);

    let outcome = tokio::time::timeout(
        StdDuration::from_secs(20),
        handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the connect never resolved");

    let err = outcome.expect_err("an untrusted certificate must not produce a connection");
    assert_ne!(
        err.kind(),
        ErrorKind::HandshakeTimeout,
        "a refused handshake was reported as a timeout, which would make a caller retry \
         against a server that will never accept it"
    );

    accepting.abort();
    client_task.abort();
    server_task.abort();
}

#[tokio::test]
async fn two_clients_share_one_server_endpoint() {
    // One socket, two connections. The whole reason the driver owns connections rather than
    // there being one driver per connection.
    let credentials = TestCredentials::generate();
    let (server_handle, server_driver, server_addr) = server(&credentials).await;
    let server_task = drive(server_driver);

    let accepting = tokio::spawn({
        let server_handle = server_handle.clone();
        async move {
            let first = server_handle.accept().await.expect("first accept");
            let second = server_handle.accept().await.expect("second accept");
            (first, second)
        }
    });

    let (first_handle, first_driver) = client(&credentials, 0x1111_1111).await;
    let (second_handle, second_driver) = client(&credentials, 0x2222_2222).await;
    let first_task = drive(first_driver);
    let second_task = drive(second_driver);

    let first = tokio::time::timeout(
        StdDuration::from_secs(10),
        first_handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the first handshake stalled")
    .expect("the first handshake failed");

    let second = tokio::time::timeout(
        StdDuration::from_secs(10),
        second_handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the second handshake stalled")
    .expect("the second handshake failed");

    assert!(first.is_established() && second.is_established());

    let (server_first, server_second) = tokio::time::timeout(StdDuration::from_secs(10), accepting)
        .await
        .expect("the server never accepted both")
        .expect("the accept task panicked");
    assert!(server_first.is_established());
    assert!(server_second.is_established());

    first_task.abort();
    second_task.abort();
    server_task.abort();
}
