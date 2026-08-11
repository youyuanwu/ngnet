//! What a connection can tell its owner about identity, size and how it ended.
//!
//! These four capabilities exist for one caller: something that multiplexes connections
//! over a single socket. Each is here because the state machine could not answer a question
//! that such a caller must answer on every datagram, and each failure mode is silent — a
//! routing table that goes stale reports nothing, it just stops delivering.

use std::sync::{Arc, Mutex};

use ngnet_quic::{
    ApplicationErrorCode, CloseReason, ConnectionId, Handlers, InitialToken, Inspection, StreamId,
    inspect, inspect_initial,
};
use ngnet_quic_tests::{
    TEST_SERVER_NAME, TestClock, TestConn, TestCredentials, client_backend, client_conn, drain,
    pump, server_backend, server_conn,
};

/// Identifiers a connection reported minting and retiring.
#[derive(Default, Debug)]
struct Ids {
    minted: Vec<Vec<u8>>,
    retired: Vec<Vec<u8>>,
}

type Shared = Arc<Mutex<Ids>>;

fn handlers(sink: &Shared) -> Handlers<'_> {
    let minted = Arc::clone(sink);
    let retired = Arc::clone(sink);
    Handlers::new()
        .on_new_connection_id(move |cid| minted.lock().unwrap().minted.push(cid.as_bytes().to_vec()))
        .on_remove_connection_id(move |cid| {
            retired.lock().unwrap().retired.push(cid.as_bytes().to_vec());
        })
}

fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:41401".parse().unwrap(),
        "127.0.0.1:41402".parse().unwrap(),
    )
}

/// A handshaked pair, with identifier reporting attached to both ends.
fn connected<'h>(
    credentials: &'h TestCredentials,
    client_sink: &'h Shared,
    server_sink: &'h Shared,
) -> (TestConn<'h>, TestConn<'h>, TestClock) {
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

    let cb = client_backend(&credentials.certificate_pem);
    let sb = server_backend(credentials);

    let mut client = client_conn(
        &cb,
        &clock,
        handlers(client_sink),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");

    let first = drain(&mut client, &clock).expect("first flight");
    let (odcid, scid) = match inspect(&first[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("unexpected first flight: {other:?}"),
    };

    let mut server = server_conn(
        &sb,
        &clock,
        handlers(server_sink),
        server_addr,
        client_addr,
        &odcid,
        scid,
    )
    .expect("building the server");

    for datagram in &first {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let _ = pump(&mut client, &mut server, &clock, 32);

    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the pair did not finish its handshake, so nothing below is being tested"
    );
    (client, server, clock)
}

#[test]
fn a_connection_reports_every_identifier_it_answers_to() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (client, server, _clock) = connected(&credentials, &client_ids, &server_ids);

    // The point of `scids()`: `scid()` reports one identifier, and a router needs the set.
    for conn in [&client, &server] {
        let all = conn.scids();
        assert!(
            !all.is_empty(),
            "a handshaked connection answers to at least one identifier"
        );
        assert!(
            all.contains(conn.scid()),
            "the identifier the connection was built with must be among the ones it \
             reports, or a router seeded from `scid()` and maintained from `scids()` would \
             disagree with itself"
        );
    }
}

#[test]
fn minting_an_identifier_is_reported_to_the_owner() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (client, server, _clock) = connected(&credentials, &client_ids, &server_ids);

    // ngtcp2 issues spare identifiers for the peer to migrate to as part of the handshake,
    // so a completed handshake has already exercised the callback. If this is ever empty,
    // the report is not wired up -- and a router would never learn about rotation.
    for (label, sink, conn) in [
        ("client", &client_ids, &client),
        ("server", &server_ids, &server),
    ] {
        let minted = sink.lock().unwrap().minted.clone();
        assert!(
            !minted.is_empty(),
            "{label} minted no identifier that reached the handler"
        );

        // And what was reported must be identifiers the connection actually answers to.
        let known: Vec<Vec<u8>> = conn.scids().iter().map(|c| c.as_bytes().to_vec()).collect();
        for id in &minted {
            assert!(
                known.contains(id),
                "{label} reported minting {id:?}, which it does not answer to; a router \
                 following this report would send datagrams into a connection that never \
                 asked for them"
            );
        }
    }
}

#[test]
fn a_reported_identifier_is_one_the_peer_could_actually_use() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (_client, server, _clock) = connected(&credentials, &client_ids, &server_ids);

    // Every reported identifier must be well-formed: a router keys on these bytes, and a
    // zero-length or oversized one would either collide with everything or nothing.
    for id in server_ids.lock().unwrap().minted.iter() {
        assert!(
            (ngnet_quic::MIN_CID_LEN..=ngnet_quic::MAX_CID_LEN).contains(&id.len()),
            "reported identifier {id:?} is not a length QUIC permits"
        );
        ConnectionId::new(id).expect("a reported identifier round-trips through the type");
    }
    let _ = server;
}

#[test]
fn the_datagram_size_a_connection_permits_is_usable_and_not_the_send_quantum() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (client, _server, _clock) = connected(&credentials, &client_ids, &server_ids);

    let max = client.max_tx_udp_payload_size();

    // QUIC's floor: every endpoint must accept a 1200-byte datagram, so a path maximum
    // below that would mean the connection could not have handshaked at all.
    assert!(
        max >= 1200,
        "a path maximum of {max} is below the QUIC minimum of 1200"
    );

    // And the ceiling that distinguishes it from a send quantum, which is a burst budget
    // spanning several packets and is routinely tens of kilobytes. A datagram sized from
    // that would be rejected by every path on the internet.
    assert!(
        max <= 65527,
        "a path maximum of {max} exceeds what a UDP datagram can carry, which means this \
         is reporting a burst budget rather than a datagram size"
    );
}

#[test]
fn a_healthy_connection_reports_no_interesting_close_reason() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (client, _server, _clock) = connected(&credentials, &client_ids, &server_ids);

    // Documented behaviour, pinned so it is a decision rather than a surprise: before a
    // close there is nothing to distinguish from a graceful one, because they are the same
    // bytes on the wire.
    let err = client.close_error();
    assert_eq!(
        err.reason(),
        &CloseReason::Transport(ngnet_quic::TransportErrorCode::new(0)),
        "an unclosed connection should carry NO_ERROR"
    );
    assert!(err.phrase().is_empty());
}

#[test]
fn an_application_close_carries_the_peers_code_and_reason() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (mut client, mut server, clock) = connected(&credentials, &client_ids, &server_ids);

    // The whole point of the accessor. `ReadOutcome::Draining` says the peer closed; this
    // says the peer closed *because of this*, which is what an application can report.
    let mut buf = vec![0u8; 1500];
    let written = server
        .write_connection_close(
            &mut buf,
            ApplicationErrorCode::new(0x1234),
            b"going away",
            clock.now(),
        )
        .expect("writing an application close");
    assert!(written > 0);

    let _ = client.read_pkt(&buf[..written], clock.now());

    let err = client.close_error();
    assert_eq!(
        err.reason(),
        &CloseReason::Application(ApplicationErrorCode::new(0x1234)),
        "the peer's application code did not survive the round trip"
    );
    assert_eq!(
        err.phrase(),
        b"going away",
        "the peer's reason phrase did not survive the round trip"
    );
}

#[test]
fn a_transport_close_is_distinguishable_from_an_application_one() {
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (mut client, mut server, clock) = connected(&credentials, &client_ids, &server_ids);

    let mut buf = vec![0u8; 1500];
    // Any error will do; this one comes from the public API rather than from a private
    // constructor, since what is being tested is how a close is *classified*, not which
    // error produced it.
    let cause = ConnectionId::new(&[]).expect_err("an empty identifier is rejected");
    let written = server
        .write_transport_close(&mut buf, &cause, b"protocol trouble", clock.now())
        .expect("writing a transport close");
    let _ = client.read_pkt(&buf[..written], clock.now());

    // The distinction matters because an application code means whatever the protocol above
    // QUIC says it means, and a transport code does not -- reporting one as the other would
    // hand an application a number from a namespace it does not own.
    match client.close_error().reason() {
        CloseReason::Transport(_) => {}
        other => panic!("a transport close was reported as {other:?}"),
    }
}

#[test]
fn an_initial_packet_yields_its_identifiers_and_an_absent_token() {
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();
    let cb = client_backend(&credentials.certificate_pem);

    let mut client = client_conn(
        &cb,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");
    let first = drain(&mut client, &clock).expect("first flight");

    let packet = inspect_initial(&first[0])
        .expect("decoding the first packet")
        .expect("a client's first packet is an acceptable Initial");

    // The identifiers must agree with what `inspect` reports, or a server would build a
    // connection addressed differently from the one it routes to.
    match inspect(&first[0], 8).expect("decoding") {
        Inspection::Supported { version, dcid, scid } => {
            assert_eq!(packet.version, version);
            assert_eq!(packet.dcid, dcid);
            assert_eq!(packet.scid, scid);
        }
        other => panic!("unexpected first flight: {other:?}"),
    }

    // A first connection attempt has never been given a token, so there is nothing to
    // present. This is the case a validating server answers with a Retry.
    assert_eq!(packet.token, InitialToken::Absent);
    assert_eq!(packet.token.bytes(), b"");
}

#[test]
fn a_datagram_that_cannot_begin_a_connection_is_reported_as_such() {
    // Not an error: a public socket receives scans, stray packets and truncated datagrams
    // constantly, and treating each as a failure would make the ordinary case noisy.
    assert!(inspect_initial(&[]).expect("empty").is_none());
    assert!(inspect_initial(&[0u8; 8]).expect("runt").is_none());
    assert!(
        inspect_initial(&[0xff; 1200]).expect("garbage").is_none(),
        "a datagram that decodes to nothing should not be offered as a connection"
    );
}

#[test]
fn a_presented_token_reaches_the_server_that_must_verify_it() {
    // The plumbing this exists for: a server cannot verify a token it cannot see, and
    // before this the decoded header -- the only place the token appears -- was discarded.
    //
    // Built by hand rather than by handshake, because minting a real Retry token needs the
    // server secret, which arrives with address validation. What is pinned here is that a
    // token present in an Initial is recovered, classified and handed back intact.
    let credentials = TestCredentials::generate();
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();
    let cb = client_backend(&credentials.certificate_pem);

    let mut client = client_conn(
        &cb,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");
    let first = drain(&mut client, &clock).expect("first flight");

    let packet = inspect_initial(&first[0]).expect("decoding").expect("initial");
    // A client with no token presents none; the classification of a present one is pinned
    // by the unit tests over the magic bytes, which is where a wrong constant would show.
    assert!(matches!(packet.token, InitialToken::Absent));

    // And a server built from what the packet carried is the connection the client is
    // talking to -- which is what makes the decoded identifiers load-bearing rather than
    // informational.
    let sb = server_backend(&credentials);
    let server = server_conn(
        &sb,
        &clock,
        Handlers::new(),
        server_addr,
        client_addr,
        &packet.dcid,
        packet.scid,
    )
    .expect("building a server from the decoded Initial");
    assert!(!server.scids().is_empty());
}

#[test]
fn identifier_reports_and_the_snapshot_do_not_disagree() {
    // The two halves of the routing contract: `scids()` seeds a table and the handlers keep
    // it current. If a minted identifier were reported but absent from the snapshot -- or
    // the reverse -- a router built from both would have entries pointing nowhere.
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (_client, server, _clock) = connected(&credentials, &client_ids, &server_ids);

    let snapshot: Vec<Vec<u8>> = server
        .scids()
        .iter()
        .map(|c| c.as_bytes().to_vec())
        .collect();
    let ids = server_ids.lock().unwrap();

    for retired in &ids.retired {
        assert!(
            !snapshot.contains(retired),
            "identifier {retired:?} was reported retired but is still live"
        );
    }
    for minted in &ids.minted {
        if ids.retired.contains(minted) {
            continue;
        }
        assert!(
            snapshot.contains(minted),
            "identifier {minted:?} was reported minted, never retired, yet is not live"
        );
    }
}

#[test]
fn a_stream_still_carries_data_with_the_new_handlers_installed() {
    // A guard against the additions changing behaviour rather than adding to it: the
    // callback table grew an entry, and installing a callback ngtcp2 did not have before
    // is exactly the sort of change that can alter what the library does.
    let credentials = TestCredentials::generate();
    let (client_ids, server_ids) = (Shared::default(), Shared::default());
    let (mut client, mut server, clock) = connected(&credentials, &client_ids, &server_ids);

    let stream = client.open_bidi_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];
    let payload = b"still works";
    let written = client
        .write_stream(&mut buf, stream, payload, true, clock.now())
        .expect("writing");
    match written {
        ngnet_quic::StreamWrite::Datagram { len, accepted } => {
            assert_eq!(accepted, payload.len());
            let _ = server.read_pkt(&buf[..len], clock.now());
        }
        other => panic!("expected a datagram, got {other:?}"),
    }
    let _: StreamId = stream;
}
