//! The handshake milestone: two `ngnet-quic` connections completing a real TLS 1.3
//! handshake, and the two security-sensitive ways that must fail.
//!
//! Everything before this proved a piece in isolation. This is the first test that requires
//! all of them to be right at once — the callback table, the version constants, the TLS
//! object graph, the packet paths and the timers.

use ngnet_quic::{ConnectionId, Handlers, Inspection, Backend as TlsBackend, Role, Session as TlsSession, inspect};
use ngnet_quic_tests::{
    TEST_ALPN, TEST_SERVER_NAME, TestClock, TestCredentials, client_backend, client_conn, drain,
    pump, server_backend, server_conn,
};

/// Client and server addresses for the in-process tests.
///
/// Nothing binds these; the relay moves datagrams directly. They still have to be
/// well-formed and distinct, because ngtcp2 validates the path.
fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:40001".parse().unwrap(),
        "127.0.0.1:40002".parse().unwrap(),
    )
}

/// Builds both ends and runs the handshake to completion.
///
/// Returns the two connections so a test can go on to inspect them.
fn handshake<'h>(
    credentials: &TestCredentials,
    client_alpn: Option<&[u8]>,
    server_name: Option<&str>,
    trust_anchor: Option<&str>,
) -> (
    ngnet_quic_tests::TestConn<'h>,
    ngnet_quic_tests::TestConn<'h>,
    TestClock,
) {
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

    let client_backend = match client_alpn {
        Some(alpn) => ngnet_quic::OsslBackend::builder(Role::Client)
            .alpn(alpn)
            .trust_anchor_pem(trust_anchor.unwrap_or(&credentials.certificate_pem))
            .use_system_trust_store(false)
            .build()
            .expect("building a client backend"),
        None => client_backend(trust_anchor.unwrap_or(&credentials.certificate_pem)),
    };
    let server_backend = server_backend(credentials);

    let mut client = client_conn(
        &client_backend,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        server_name,
    )
    .expect("building the client");

    // The server is built from what the client's first packet carries, which is the only
    // way to obtain the `original_dcid` ngtcp2 requires of a server.
    let first_flight = drain(&mut client, &clock).expect("the client's first flight");
    assert!(
        !first_flight.is_empty(),
        "a fresh client must have something to send"
    );

    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("the first flight should be a supported long header, got {other:?}"),
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

    // Feed the flight already drained, then let the relay run.
    for datagram in &first_flight {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let _ = pump(&mut client, &mut server, &clock, 32);

    (client, server, clock)
}

#[test]
fn a_client_and_server_complete_a_handshake() {
    let credentials = TestCredentials::generate();
    let (client, server, _clock) = handshake(
        &credentials,
        None,
        Some(TEST_SERVER_NAME),
        Some(&credentials.certificate_pem),
    );

    assert!(
        client.is_handshake_completed(),
        "the client did not complete the handshake"
    );
    assert!(
        server.is_handshake_completed(),
        "the server did not complete the handshake"
    );
}

#[test]
fn both_ends_report_the_negotiated_alpn() {
    let credentials = TestCredentials::generate();
    let (client, server, _clock) = handshake(
        &credentials,
        None,
        Some(TEST_SERVER_NAME),
        Some(&credentials.certificate_pem),
    );

    assert_eq!(client.negotiated_alpn().as_deref(), Some(TEST_ALPN));
    assert_eq!(server.negotiated_alpn().as_deref(), Some(TEST_ALPN));
}

#[test]
fn a_handshake_with_no_common_alpn_fails() {
    // QUIC requires ALPN. A connection that completed without agreeing one would be
    // carrying an unknown protocol, so this must fail rather than proceed.
    let credentials = TestCredentials::generate();
    let (client, server, _clock) = handshake(
        &credentials,
        Some(b"something-else-entirely"),
        Some(TEST_SERVER_NAME),
        Some(&credentials.certificate_pem),
    );

    assert!(
        !client.is_handshake_completed(),
        "a handshake with no common ALPN must not complete"
    );
    assert!(
        !server.is_handshake_completed(),
        "a handshake with no common ALPN must not complete"
    );
}

#[test]
fn a_certificate_that_does_not_match_the_requested_name_is_rejected() {
    // The claim that matters most. Verification is on by default in this crate, unlike the
    // ngtcp2 examples, which verify nothing at all -- so an untested verification path
    // would leave the crate's main security promise unproven.
    let credentials = TestCredentials::generate();
    let (client, _server, _clock) = handshake(
        &credentials,
        None,
        // The certificate is issued for `localhost`, so this name cannot match it.
        Some("wrong.example.com"),
        Some(&credentials.certificate_pem),
    );

    assert!(
        !client.is_handshake_completed(),
        "a certificate for the wrong name must not be accepted"
    );

    let reason = client.tls().failure_reason().unwrap_or_default();
    assert!(
        reason.to_ascii_lowercase().contains("certificate")
            || reason.to_ascii_lowercase().contains("verif"),
        "the failure should name certificate verification, got: {reason}"
    );
}

#[test]
fn an_untrusted_certificate_is_rejected() {
    // The server presents a certificate the client has never heard of, which is the
    // ordinary shape of a man-in-the-middle.
    let server_credentials = TestCredentials::generate();
    let unrelated = TestCredentials::generate();

    let (client, _server, _clock) = handshake(
        &server_credentials,
        None,
        Some(TEST_SERVER_NAME),
        // Trust something else entirely.
        Some(&unrelated.certificate_pem),
    );

    assert!(
        !client.is_handshake_completed(),
        "an untrusted certificate must not be accepted"
    );
}

#[test]
fn the_verification_test_would_notice_if_verification_were_disabled() {
    // A verification test that passed for the wrong reason would be worse than none. This
    // proves the negative tests above depend on verification being on: with it off, the
    // same mismatched setup completes.
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

    let client_backend = ngnet_quic::OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .verify(ngnet_quic::Verify::DangerouslyAcceptAnyCertificate)
        .build()
        .expect("building a non-verifying client");
    let server_backend = server_backend(&credentials);

    let mut client = client_conn(
        &client_backend,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        None,
    )
    .expect("building the client");

    let first_flight = drain(&mut client, &clock).expect("the client's first flight");
    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("unexpected first flight: {other:?}"),
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

    assert!(
        client.is_handshake_completed(),
        "with verification off, the same setup must complete -- otherwise the negative \
         tests above prove nothing"
    );
}

#[test]
fn the_handshake_reports_progress_rather_than_hanging() {
    // A relay that made no progress would hang rather than fail, which is the worst kind of
    // test failure. The pump is bounded and reports how much moved.
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

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
    .unwrap();

    let first_flight = drain(&mut client, &clock).unwrap();
    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).unwrap() {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("unexpected first flight: {other:?}"),
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
    .unwrap();

    for datagram in &first_flight {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let moved = pump(&mut client, &mut server, &clock, 32).unwrap();
    assert!(
        moved >= 2,
        "a handshake should move several datagrams, saw {moved}"
    );
}

#[test]
fn a_connection_can_be_dropped_at_every_lifecycle_point() {
    // The four points the specification asks for. Each is a different arrangement of the
    // TLS object graph, and the teardown order is what makes each safe.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();

    // 1. Before the handshake starts.
    {
        let clock = TestClock::new();
        let backend = client_backend(&credentials.certificate_pem);
        let conn = client_conn(
            &backend,
            &clock,
            Handlers::new(),
            client_addr,
            server_addr,
            Some(TEST_SERVER_NAME),
        )
        .unwrap();
        drop(conn);
    }

    // 2. Mid-handshake: the first flight has been sent, nothing answered.
    {
        let clock = TestClock::new();
        let backend = client_backend(&credentials.certificate_pem);
        let mut conn = client_conn(
            &backend,
            &clock,
            Handlers::new(),
            client_addr,
            server_addr,
            Some(TEST_SERVER_NAME),
        )
        .unwrap();
        let flight = drain(&mut conn, &clock).unwrap();
        assert!(!flight.is_empty());
        drop(conn);
    }

    // 3. After a completed handshake.
    {
        let (client, server, _clock) = handshake(
            &credentials,
            None,
            Some(TEST_SERVER_NAME),
            Some(&credentials.certificate_pem),
        );
        assert!(client.is_handshake_completed());
        drop(client);
        drop(server);
    }

    // 4. After a handshake that failed.
    {
        let (client, server, _clock) = handshake(
            &credentials,
            Some(b"no-such-protocol"),
            Some(TEST_SERVER_NAME),
            Some(&credentials.certificate_pem),
        );
        assert!(!client.is_handshake_completed());
        drop(client);
        drop(server);
    }
}

#[test]
fn a_server_cannot_be_built_without_decoding_the_clients_first_packet() {
    // The assertion ngtcp2 makes and then compiles out of release builds. Ours must hold in
    // both, which is what makes this worth a test rather than a comment.
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();
    let backend = server_backend(&credentials);

    let session = backend.new_session(Role::Server, None).unwrap();
    let result = ngnet_quic::ConnBuilder::new(
        Role::Server,
        ngnet_quic::Settings::new(clock.now()),
        // No `original_dcid`, which is exactly what decoding the client's packet supplies.
        ngnet_quic::TransportParams::new(),
        Box::new(ngnet_quic_tests::TestEntropy::new(1)),
        session,
        server_addr,
        client_addr,
    )
    .build(Handlers::new());

    assert!(
        result.is_err(),
        "a server built without an original_dcid must be refused"
    );
}

#[test]
fn connection_ids_from_the_first_flight_are_usable_after_the_datagram_is_gone() {
    // They borrow into the datagram inside ngtcp2, so `inspect` has to copy them out.
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();
    let backend = client_backend(&credentials.certificate_pem);

    let ids: (ConnectionId, ConnectionId) = {
        let mut client = client_conn(
            &backend,
            &clock,
            Handlers::new(),
            client_addr,
            server_addr,
            Some(TEST_SERVER_NAME),
        )
        .unwrap();
        let flight = drain(&mut client, &clock).unwrap();
        match inspect(&flight[0], 8).unwrap() {
            Inspection::Supported { dcid, scid, .. } => (dcid, scid),
            other => panic!("unexpected first flight: {other:?}"),
        }
        // `flight` and `client` are dropped here.
    };

    assert_eq!(ids.0.as_bytes().len(), 8);
    assert_eq!(ids.1.as_bytes().len(), 8);
}
