//! Address validation over real sockets.
//!
//! The unit tests prove the tokens are unforgeable. These prove the mechanism works end to
//! end: that a validating server really does refuse to build state for an unvalidated first
//! packet, that a client transparently completes the extra round trip, and that a datagram
//! belonging to no connection draws an answer smaller than itself.

use std::time::Duration as StdDuration;

use ngnet_quic::endpoint::{
    Config, Endpoint, EndpointBuilder, EndpointDriver, TokioClock, TokioSocket,
};
use ngnet_quic::{OsslBackend, OsslSession, Role, TokenSecret};
use ngnet_quic_tests::{TEST_ALPN, TEST_SERVER_NAME, TestCredentials, TestEntropy};

/// The endpoint driver these tests run.
type Driver = EndpointDriver<TokioSocket, TokioClock, OsslBackend>;

/// How long a test waits before declaring a handshake stalled.
const PATIENCE: StdDuration = StdDuration::from_secs(20);

async fn client(credentials: &TestCredentials, seed: u64) -> (Endpoint<OsslSession>, Driver) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding");
    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");

    EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new())
        .entropy(move || TestEntropy::new(seed))
        .build()
        .expect("a client endpoint")
}

/// A server that validates client addresses before committing any state.
async fn validating_server(
    credentials: &TestCredentials,
) -> (Endpoint<OsslSession>, Driver, core::net::SocketAddr) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding");
    let address = socket.inner().local_addr().expect("an address");
    let backend = OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");

    let secret = TokenSecret::new(&[0x5e; 32]).expect("a valid secret");
    let (handle, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new())
        .entropy(|| TestEntropy::new(0x8765_4321))
        .accepts(true)
        .validate_addresses(secret)
        .build()
        .expect("a server endpoint");

    (handle, driver, address)
}

fn drive(driver: Driver) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = driver.await;
    })
}

#[tokio::test]
async fn a_client_completes_a_handshake_through_a_retry() {
    // The whole mechanism, from the outside. The client is not told a Retry happened and
    // needs no code for it; the extra round trip is the transport's business.
    let credentials = TestCredentials::generate();
    let (server_handle, server_driver, server_addr) = validating_server(&credentials).await;
    let (client_handle, client_driver) = client(&credentials, 0x1234_5678).await;

    let server_task = drive(server_driver);
    let client_task = drive(client_driver);
    let accepting = tokio::spawn(async move { server_handle.accept().await });

    let connection = tokio::time::timeout(
        PATIENCE,
        client_handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the handshake stalled, which is what a Retry nobody answers looks like")
    .expect("the handshake failed");
    assert!(connection.is_established());

    let accepted = tokio::time::timeout(PATIENCE, accepting)
        .await
        .expect("the server never accepted")
        .expect("the accept task panicked")
        .expect("the accept failed");
    assert!(accepted.is_established());

    client_task.abort();
    server_task.abort();
}

#[tokio::test]
async fn an_unvalidated_first_packet_draws_a_retry_and_no_connection() {
    // The property that makes validation worth having. A server must not commit
    // per-connection state -- nor send a handshake -- for an address it has not checked,
    // because the handshake is several times larger than the packet that provoked it.
    //
    // A genuine Initial is needed to test this, so one is captured by pointing a real
    // client at a socket that only listens, and then replayed from a third socket that
    // never answers. That replay is exactly what a spoofed source looks like.
    let credentials = TestCredentials::generate();

    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");
    let sink_addr = sink.local_addr().expect("an address");

    let (capture_handle, capture_driver) = client(&credentials, 0xfeed_face).await;
    let capture_task = drive(capture_driver);
    let connecting = tokio::spawn(async move {
        let _ = capture_handle.connect(sink_addr, Some(TEST_SERVER_NAME)).await;
    });

    let mut initial = vec![0u8; 2048];
    let (len, _) = tokio::time::timeout(StdDuration::from_secs(5), sink.recv_from(&mut initial))
        .await
        .expect("no first flight arrived")
        .expect("receiving");
    initial.truncate(len);
    connecting.abort();
    capture_task.abort();

    assert!(
        len >= 1200,
        "a QUIC Initial is padded to at least 1200 bytes; got {len}"
    );

    // Now replay it at a validating server from a socket that will never answer.
    let (server_handle, server_driver, server_addr) = validating_server(&credentials).await;
    let server_task = drive(server_driver);

    let spoofer = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");
    spoofer
        .send_to(&initial, server_addr)
        .await
        .expect("sending");

    let mut buffer = [0u8; 2048];
    let mut returned = 0usize;
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(StdDuration::from_millis(200), spoofer.recv_from(&mut buffer))
            .await
        {
            Ok(Ok((got, _))) => returned += got,
            _ => break,
        }
    }

    assert!(
        returned > 0,
        "the server said nothing at all, so a genuine client would never learn to retry"
    );
    assert!(
        returned < len,
        "the server returned {returned} bytes for a {len}-byte unvalidated Initial, which \
         is an amplification factor above one -- the attack Retry exists to prevent"
    );

    // And no connection was created for an address that never answered the Retry.
    let accepted = tokio::time::timeout(StdDuration::from_millis(500), server_handle.accept());
    assert!(
        accepted.await.is_err(),
        "the server built a connection for an address it never validated"
    );

    server_task.abort();
}

#[tokio::test]
async fn an_unmatched_datagram_draws_an_answer_smaller_than_itself() {
    // A stateless reset tells a peer that has lost state to stop retransmitting. It must be
    // smaller than what provoked it, or the mechanism for saying "I lost your connection"
    // becomes a reflector.
    let credentials = TestCredentials::generate();
    let (_server_handle, server_driver, server_addr) = validating_server(&credentials).await;
    let server_task = drive(server_driver);

    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");

    // A short-header packet for a connection that does not exist. The leading bit clear
    // marks it short-header, so it cannot be mistaken for a connection attempt.
    let mut datagram = vec![0x40u8; 800];
    datagram[1..9].copy_from_slice(&[0xaa; 8]);
    probe
        .send_to(&datagram, server_addr)
        .await
        .expect("sending");

    let mut buffer = [0u8; 2048];
    let answer = tokio::time::timeout(StdDuration::from_secs(3), probe.recv_from(&mut buffer)).await;

    // Silence is also correct: the budget may be spent, or the datagram may have been judged
    // too small to answer safely. What must not happen is a *larger* answer.
    if let Ok(Ok((len, _))) = answer {
        assert!(
            len < datagram.len(),
            "a {len}-byte answer to an {}-byte datagram amplifies",
            datagram.len()
        );
        assert!(len > 0);
    }

    server_task.abort();
}

#[tokio::test]
async fn a_flood_of_unmatched_datagrams_does_not_draw_an_unbounded_reply() {
    // The rate limit. Without it, answering unmatched traffic is itself the attack.
    let credentials = TestCredentials::generate();
    let (_server_handle, server_driver, server_addr) = validating_server(&credentials).await;
    let server_task = drive(server_driver);

    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");

    let mut datagram = vec![0x40u8; 400];
    datagram[1..9].copy_from_slice(&[0xbb; 8]);

    let mut sent = 0usize;
    for _ in 0..400 {
        probe
            .send_to(&datagram, server_addr)
            .await
            .expect("sending");
        sent += datagram.len();
    }

    let mut buffer = [0u8; 2048];
    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(StdDuration::from_millis(100), probe.recv_from(&mut buffer))
            .await
        {
            Ok(Ok((len, _))) => received += len,
            _ => break,
        }
    }

    assert!(
        received < sent,
        "{received} bytes came back for {sent} bytes sent, so answering unmatched traffic \
         amplifies rather than merely informing"
    );

    server_task.abort();
}

#[tokio::test]
async fn a_datagram_too_short_to_be_quic_draws_nothing() {
    let credentials = TestCredentials::generate();
    let (_server_handle, server_driver, server_addr) = validating_server(&credentials).await;
    let server_task = drive(server_driver);

    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding");
    probe
        .send_to(&[0x40, 0x01, 0x02], server_addr)
        .await
        .expect("sending");

    let mut buffer = [0u8; 2048];
    let answer = tokio::time::timeout(StdDuration::from_secs(1), probe.recv_from(&mut buffer)).await;
    assert!(
        answer.is_err(),
        "a three-byte datagram drew a reply, and any reply to it amplifies"
    );

    server_task.abort();
}

#[tokio::test]
async fn a_server_without_validation_still_works() {
    // Validation is opt-in, so the unvalidated path must keep working -- a test server on a
    // loopback socket has no amplification concern and should not be forced to configure a
    // secret.
    let credentials = TestCredentials::generate();
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding");
    let address = socket.inner().local_addr().expect("an address");
    let backend = OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");

    let (server_handle, server_driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new())
        .entropy(|| TestEntropy::new(0x8765_4321))
        .accepts(true)
        .build()
        .expect("a server endpoint");

    let server_task = drive(server_driver);
    let accepting = tokio::spawn(async move { server_handle.accept().await });

    let (client_handle, client_driver) = client(&credentials, 0x1357_9bdf).await;
    let client_task = drive(client_driver);

    let connection = tokio::time::timeout(
        PATIENCE,
        client_handle.connect(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the handshake stalled")
    .expect("the handshake failed");
    assert!(connection.is_established());

    let accepted = tokio::time::timeout(PATIENCE, accepting)
        .await
        .expect("the server never accepted")
        .expect("the accept task panicked")
        .expect("the accept failed");
    assert!(accepted.is_established());

    client_task.abort();
    server_task.abort();
}
