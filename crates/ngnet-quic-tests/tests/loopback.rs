//! The same handshake, driven through real loopback UDP sockets.
//!
//! The in-process relay proves the state machine. This proves the parts the relay cannot:
//! that addresses are laid out the way the kernel expects, that datagram boundaries survive
//! the round trip, and that the buffer sizes the crate asks for are the ones it needs.

use ngnet_quic::{Handlers, Inspection, inspect};
use ngnet_quic_tests::udp::{LoopbackSocket, absorb, flush, pump_sockets};
use ngnet_quic_tests::{
    TEST_ALPN, TEST_SERVER_NAME, TestClock, TestCredentials, client_backend, client_conn,
    server_backend, server_conn,
};

#[test]
fn a_handshake_completes_over_loopback_udp() {
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();

    let client_socket = LoopbackSocket::bind().expect("binding the client socket");
    let server_socket = LoopbackSocket::bind().expect("binding the server socket");
    let client_addr = client_socket.local_addr().unwrap();
    let server_addr = server_socket.local_addr().unwrap();

    let client_backend = client_backend(&credentials.certificate_pem);
    let server_backend = server_backend(&credentials);

    let mut client = client_conn(
        &client_backend,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");

    // The client's first flight has to go out before a server can exist, because the
    // server's transport parameters need the identifier it carries.
    let sent =
        flush(&mut client, &clock, &client_socket, server_addr).expect("sending the first flight");
    assert!(sent > 0, "a fresh client must send something");

    // Peek at what arrived so the server can be built from it.
    let mut buf = vec![0u8; 2048];
    let len = server_socket
        .recv(&mut buf)
        .expect("receiving")
        .expect("the first flight should have arrived");
    let (original_dcid, client_scid) = match inspect(&buf[..len], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("expected a supported long header, got {other:?}"),
    };

    let mut server = server_conn(
        &server_backend,
        &clock,
        Handlers::new(),
        server_addr,
        client_addr,
        &original_dcid,
        client_scid,
    )
    .expect("building the server");

    server
        .read_pkt(&buf[..len], clock.now())
        .expect("the server reading the first flight");

    pump_sockets(
        &mut client,
        &client_socket,
        &mut server,
        &server_socket,
        &clock,
        64,
    )
    .expect("pumping the handshake");

    assert!(
        client.is_handshake_completed(),
        "the client did not complete the handshake over UDP"
    );
    assert!(
        server.is_handshake_completed(),
        "the server did not complete the handshake over UDP"
    );
    assert_eq!(client.negotiated_alpn().as_deref(), Some(TEST_ALPN));
}

#[test]
fn a_datagram_survives_the_round_trip_unchanged() {
    // Datagram boundaries are the thing UDP preserves and a byte stream does not. If the
    // crate ever started assuming otherwise, the in-process relay would not notice.
    let socket_a = LoopbackSocket::bind().unwrap();
    let socket_b = LoopbackSocket::bind().unwrap();
    let addr_b = socket_b.local_addr().unwrap();

    let payload: Vec<u8> = (0..1200u32).map(|i| (i % 251) as u8).collect();
    socket_a.send_to(&payload, addr_b).unwrap();

    let mut buf = vec![0u8; 2048];
    let len = socket_b.recv(&mut buf).unwrap().expect("a datagram");
    assert_eq!(&buf[..len], &payload[..]);
}

#[test]
fn a_socket_with_nothing_waiting_times_out_rather_than_blocking() {
    // The property that stops a broken test hanging the suite instead of failing it.
    let socket = LoopbackSocket::bind().unwrap();
    let mut buf = vec![0u8; 128];
    assert!(socket.recv(&mut buf).unwrap().is_none());
}

#[test]
fn absorbing_from_an_empty_socket_consumes_nothing() {
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let socket = LoopbackSocket::bind().unwrap();
    let peer = LoopbackSocket::bind().unwrap();

    let backend = client_backend(&credentials.certificate_pem);
    let mut conn = client_conn(
        &backend,
        &clock,
        Handlers::new(),
        socket.local_addr().unwrap(),
        peer.local_addr().unwrap(),
        Some(TEST_SERVER_NAME),
    )
    .unwrap();

    assert_eq!(absorb(&mut conn, &clock, &socket).unwrap(), 0);
}
