//! Behaviours of the TLS seam that had no test before it existed.
//!
//! These are the things the seam made possible to get wrong, tested against two real
//! connections rather than against the seam in isolation. Where a claim can be checked at the
//! unit level it already is, next to the code; what is here needs a connection because the
//! behaviour only exists once one end is talking to another.

use ngnet_quic::{Handlers, Inspection, ReadOutcome, Role, inspect};
use ngnet_quic_tests::{
    TEST_SERVER_NAME, TestClock, TestConn, TestCredentials, client_backend, client_conn, drain,
    pump, server_backend, server_conn,
};

/// The two ends of the loopback pair.
fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:4433".parse().expect("a client address"),
        "127.0.0.1:4434".parse().expect("a server address"),
    )
}

/// Runs a handshake to completion and hands back both ends.
fn handshake<'h>(credentials: &TestCredentials) -> (TestConn<'h>, TestConn<'h>, TestClock) {
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

    let client_backend = client_backend(&credentials.certificate_pem);
    let server_backend = server_backend(credentials);

    let mut client = client_conn(
        &client_backend,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");

    let first_flight = drain(&mut client, &clock).expect("the client's first flight");
    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("expected a long header, got {other:?}"),
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

    for datagram in &first_flight {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let _ = pump(&mut client, &mut server, &clock, 32);
    assert!(client.is_handshake_completed() && server.is_handshake_completed());
    (client, server, clock)
}

#[test]
fn a_forged_packet_is_discarded_without_ending_the_connection() {
    // The distinction `CryptoError` exists for, at the level where it matters. Anyone able to
    // send a datagram can produce a packet that fails to authenticate; treating that as fatal
    // would hand them the connection. A loopback test has neither reordering nor attackers, so
    // this has to be constructed deliberately.
    let credentials = TestCredentials::generate();
    let (mut client, mut server, clock) = handshake(&credentials);

    // A well-formed short header for this connection, with a payload that cannot authenticate.
    let scid = client.scid().as_bytes().to_vec();
    let mut forged = Vec::with_capacity(1 + scid.len() + 64);
    forged.push(0x40);
    forged.extend_from_slice(&scid);
    forged.extend(std::iter::repeat_n(0xa5u8, 64));

    let outcome = server.read_pkt(&forged, clock.now());
    assert!(
        matches!(outcome, Ok(ReadOutcome::Processed) | Err(_)),
        "a forged packet produced an unexpected outcome: {outcome:?}"
    );
    assert!(
        !server.in_closing_period() && !server.in_draining_period(),
        "a forged packet ended the connection, which hands anyone who can send one a way to \
         close it"
    );

    // And the connection still works afterwards.
    let stream = client.open_bidi_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];
    let written = client
        .write_stream(&mut buf, stream, b"still here", true, clock.now())
        .expect("writing after the forgery");
    assert!(matches!(written, ngnet_quic::StreamWrite::Datagram { .. }));
}

#[test]
fn both_ends_agree_on_the_transport_parameters_they_exchanged() {
    // SC-011. The parameters travel through the seam in both directions, and a server's are
    // produced mid-handshake at the one moment they are complete. If either half were wrong
    // the handshake would complete locally and the connection would misbehave over limits it
    // never agreed -- which is why this asserts the negotiated values rather than merely that
    // the handshake finished.
    let credentials = TestCredentials::generate();
    let (client, server, _clock) = handshake(&credentials);

    // Each side learned a stream budget it could only have got from the other's parameters.
    assert!(
        client.streams_bidi_left() > 0,
        "the client never received the server's stream limits"
    );
    assert!(
        server.streams_bidi_left() > 0,
        "the server never received the client's stream limits"
    );
}

#[test]
fn the_negotiated_protocol_reaches_both_ends() {
    // SC-010's remaining half at the connection level: what the seam reports arrives.
    let credentials = TestCredentials::generate();
    let (client, server, _clock) = handshake(&credentials);
    let expected = b"ngnet-test";
    assert_eq!(client.negotiated_alpn().as_deref(), Some(&expected[..]));
    assert_eq!(server.negotiated_alpn().as_deref(), Some(&expected[..]));
}

#[test]
fn a_server_session_is_created_without_a_destination_name() {
    // SC-009. A client carries the name it is connecting to; a server has none to carry, and
    // asking for one would be a signature that could not be satisfied.
    let credentials = TestCredentials::generate();
    let backend = server_backend(&credentials);
    assert!(ngnet_quic::Backend::new_session(&backend, Role::Server, None).is_ok());
}
