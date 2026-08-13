//! Stream limits, and the two directions a stream closes in.
//!
//! Both properties here come from callbacks this crate did not always register, and both
//! fail silently when they are missing rather than producing an error.
//!
//! * The peer raising a stream limit is the only signal that a refused open may now
//!   succeed. Opening past the limit is reported as *blocked*, deliberately, because the
//!   condition is temporary — but a caller that waits for it to lift and is never told has
//!   no timeout to fall back on.
//! * A stream closes in two directions independently, with a code for each. The older
//!   `stream_close` callback reports one code and no direction, so a stream reset one way
//!   and stop-sent the other is indistinguishable from either alone.

use std::sync::{Arc, Mutex};

use ngnet_quic::{
    ApplicationErrorCode, ErrorKind, Handlers, Inspection, StreamCloseReason, StreamWrite,
    TransportParams, inspect,
};
use ngnet_quic_tests::{
    PACING_STEP_NANOS, TEST_SERVER_NAME, TestClock, TestConn, TestCredentials, client_backend,
    client_conn, drain, pump, server_backend,
};

/// What one connection's handlers observed.
#[derive(Default, Debug)]
struct Observed {
    data: Vec<(i64, Vec<u8>)>,
    closed: Vec<(i64, Option<u64>, Option<u64>)>,
    max_bidi: Vec<u64>,
    max_uni: Vec<u64>,
}

type Shared = Arc<Mutex<Observed>>;

fn handlers(sink: &Shared) -> Handlers<'_> {
    let a = Arc::clone(sink);
    let b = Arc::clone(sink);
    let c = Arc::clone(sink);
    let d = Arc::clone(sink);
    Handlers::new()
        .on_stream_data(move |id, bytes, _fin| {
            d.lock().unwrap().data.push((id.get(), bytes.to_vec()));
        })
        .on_stream_close(move |id, reason: StreamCloseReason| {
            a.lock().unwrap().closed.push((
                id.get(),
                reason.receiving().map(|c| c.get()),
                reason.sending().map(|c| c.get()),
            ));
        })
        .on_extend_max_local_streams_bidi(move |max| b.lock().unwrap().max_bidi.push(max))
        .on_extend_max_local_streams_uni(move |max| c.lock().unwrap().max_uni.push(max))
}

fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:41061".parse().unwrap(),
        "127.0.0.1:41062".parse().unwrap(),
    )
}

/// Brings up a handshaked pair. The server's transport parameters are the caller's, so a
/// test can start the client off with almost no stream allowance.
fn connected<'h>(
    credentials: &'h TestCredentials,
    client_sink: &'h Shared,
    server_sink: &'h Shared,
    server_params: TransportParams,
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
    let (odcid, scid) = match inspect(&first[0], 8).expect("decoding the first flight") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("unexpected first flight: {other:?}"),
    };

    let mut server = server_conn_with(
        &sb,
        &clock,
        handlers(server_sink),
        server_addr,
        client_addr,
        &odcid,
        scid,
        server_params,
    )
    .expect("building the server");

    for datagram in &first {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let _ = pump(&mut client, &mut server, &clock, 32);

    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the handshake must complete first"
    );

    (client, server, clock)
}

/// `server_conn` with the transport parameters under the test's control.
#[allow(clippy::too_many_arguments)]
fn server_conn_with<'h>(
    backend: &ngnet_quic::OsslBackend,
    clock: &TestClock,
    handlers: Handlers<'h>,
    local: std::net::SocketAddr,
    remote: std::net::SocketAddr,
    original_dcid: &ngnet_quic::ConnectionId,
    client_scid: ngnet_quic::ConnectionId,
    params: TransportParams,
) -> ngnet_quic::Result<TestConn<'h>> {
    use ngnet_quic::{ConnBuilder, Role, Settings, TlsBackend};
    let session = backend.new_session(Role::Server, None)?;
    ConnBuilder::new(
        Role::Server,
        Settings::new(clock.now()),
        params.original_dcid(original_dcid),
        Box::new(ngnet_quic_tests::TestEntropy::new(0x8765_4321)),
        session,
        ngnet_quic_tests::core_addr(local),
        ngnet_quic_tests::core_addr(remote),
    )
    .dcid(client_scid)
    .build(handlers)
}

#[test]
fn a_stream_reset_one_way_and_stopped_the_other_reports_both_codes() {
    // The case the single-code callback cannot express, and the reason this crate moved to
    // `stream_close2`. The two codes are deliberately different so that a implementation
    // populating both directions from one value would be caught.
    const PEER_RESET: u64 = 0x1111;
    const WE_STOPPED: u64 = 0x2222;

    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();
    let (mut client, mut server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new(),
    );

    let stream = client.open_bidi_stream().expect("opening a stream");

    // Put the stream on the wire so the server knows about it.
    let mut buf = vec![0u8; 1500];
    match client
        .write_stream(&mut buf, stream, b"hello", false, clock.now())
        .expect("writing")
    {
        ngnet_quic::StreamWrite::Datagram { len, .. } => {
            server.read_pkt(&buf[..len], clock.now()).expect("reading");
        }
        other => panic!("expected a datagram, got {other:?}"),
    }
    clock.advance(PACING_STEP_NANOS);

    // The client stops reading: that shuts its *receiving* side with `WE_STOPPED`.
    client
        .stop_sending(stream, ApplicationErrorCode::new(WE_STOPPED))
        .expect("asking the peer to stop sending");

    // The client also abandons what it had left to send: that shuts its *sending* side with
    // `PEER_RESET`.
    client
        .reset_stream(stream, ApplicationErrorCode::new(PEER_RESET))
        .expect("resetting");

    // Let both frames cross and the stream come to rest.
    for _ in 0..16 {
        let mut moved = false;
        for datagram in drain(&mut client, &clock).expect("draining the client") {
            server.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        for datagram in drain(&mut server, &clock).expect("draining the server") {
            client.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        clock.advance(PACING_STEP_NANOS);
        if !moved {
            break;
        }
    }

    let observed = client_sink.lock().unwrap();
    let entry = observed
        .closed
        .iter()
        .find(|(id, _, _)| *id == stream.get())
        .unwrap_or_else(|| panic!("the stream must have closed; saw {:?}", observed.closed));

    let (_, receiving, sending) = entry;
    assert_eq!(
        *receiving,
        Some(WE_STOPPED),
        "the receiving side was shut by the stop-sending this endpoint issued"
    );
    assert_eq!(
        *sending,
        Some(PEER_RESET),
        "the sending side was shut by the reset this endpoint issued"
    );
    assert_ne!(
        receiving, sending,
        "the two directions must be reported independently, which is the whole point of \
         the callback this exercises"
    );
}

#[test]
fn a_clean_close_reports_no_code_in_either_direction() {
    // The companion: absent is not the same as zero. A direction that ended cleanly must
    // report nothing, or a caller cannot tell a graceful finish from a reset with code 0.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();
    let (mut client, mut server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new(),
    );

    let stream = client.open_bidi_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];

    // Finish the sending side.
    match client
        .write_stream(&mut buf, stream, b"done", true, clock.now())
        .expect("writing")
    {
        ngnet_quic::StreamWrite::Datagram { len, .. } => {
            server.read_pkt(&buf[..len], clock.now()).expect("reading");
        }
        other => panic!("expected a datagram, got {other:?}"),
    }
    clock.advance(PACING_STEP_NANOS);

    // The server finishes its side too, which ends the stream in both directions.
    match server
        .write_stream(&mut buf, stream, b"ok", true, clock.now())
        .expect("writing")
    {
        ngnet_quic::StreamWrite::Datagram { len, .. } => {
            client.read_pkt(&buf[..len], clock.now()).expect("reading");
        }
        other => panic!("expected a datagram, got {other:?}"),
    }

    for _ in 0..16 {
        let mut moved = false;
        for datagram in drain(&mut client, &clock).expect("draining the client") {
            server.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        for datagram in drain(&mut server, &clock).expect("draining the server") {
            client.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        clock.advance(PACING_STEP_NANOS);
        if !moved {
            break;
        }
    }

    let observed = client_sink.lock().unwrap();
    if let Some((_, receiving, sending)) =
        observed.closed.iter().find(|(id, _, _)| *id == stream.get())
    {
        assert_eq!(*receiving, None, "a clean receiving side carries no code");
        assert_eq!(*sending, None, "a clean sending side carries no code");
    }
}

#[test]
fn the_peer_raising_a_stream_limit_is_reported() {
    // A server that starts by permitting a single bidirectional stream. Once the client has
    // used it and the stream ends, the server's limit rises, and the client is told.
    //
    // Without this notification the client's second open fails as blocked forever, with
    // nothing to wait on — which is a hang, not an error.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();

    let (mut client, mut server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new().initial_max_streams_bidi(1),
    );

    let first = client.open_bidi_stream().expect("the first stream is allowed");

    // The second must be refused: the peer permitted exactly one.
    let refused = client.open_bidi_stream();
    assert!(
        refused.is_err(),
        "a second stream must be refused while the peer's limit is one"
    );

    // The limit is also reported once at handshake, when the peer's transport parameters
    // first establish it -- going from nothing to one is a strict extension like any other.
    // What matters here is the *later* rise, so the starting point is recorded rather than
    // assumed to be empty.
    let already = client_sink.lock().unwrap().max_bidi.clone();
    assert!(
        already.iter().all(|max| *max <= 1),
        "before the peer grants more, no report may exceed the one stream it allowed: \
         {already:?}"
    );

    // Put the first stream to use, then have the server grant room for another. Granting is
    // the application's decision in this API rather than something the library infers, so
    // the test makes it explicitly.
    let mut buf = vec![0u8; 1500];
    match client
        .write_stream(&mut buf, first, b"x", true, clock.now())
        .expect("writing")
    {
        ngnet_quic::StreamWrite::Datagram { len, .. } => {
            server.read_pkt(&buf[..len], clock.now()).expect("reading");
        }
        other => panic!("expected a datagram, got {other:?}"),
    }
    clock.advance(PACING_STEP_NANOS);
    server.extend_max_streams_bidi(1);

    for _ in 0..24 {
        let mut moved = false;
        for datagram in drain(&mut server, &clock).expect("draining the server") {
            client.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        for datagram in drain(&mut client, &clock).expect("draining the client") {
            server.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        clock.advance(PACING_STEP_NANOS);
        if client_sink.lock().unwrap().max_bidi.len() > already.len() {
            break;
        }
        if !moved {
            break;
        }
    }

    let raised = client_sink.lock().unwrap().max_bidi.clone();
    assert!(
        raised.len() > already.len(),
        "the peer raising the limit must be reported; without it a caller waiting for \
         room waits forever. Saw {raised:?}, started from {already:?}"
    );
    assert!(
        raised.last().is_some_and(|max| *max >= 2),
        "the reported figure is the cumulative total this endpoint may now open, not an \
         increment: {raised:?}"
    );

    // And the open that was refused now succeeds, which is the point of being told.
    assert!(
        client.open_bidi_stream().is_ok(),
        "the limit having risen, another stream must be allowed"
    );
}

#[test]
fn a_stream_limit_of_zero_still_reports_when_it_rises() {
    // The startup case: a peer that permits no unidirectional streams at all. HTTP/3 opens
    // three before it can do anything, so an endpoint that never hears the limit rise never
    // starts.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();

    let (mut client, mut server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new().initial_max_streams_uni(0),
    );

    assert!(
        client.open_uni_stream().is_err(),
        "no unidirectional stream may be opened while the peer permits none"
    );

    server.extend_max_streams_uni(3);

    for _ in 0..24 {
        let mut moved = false;
        for datagram in drain(&mut server, &clock).expect("draining the server") {
            client.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        for datagram in drain(&mut client, &clock).expect("draining the client") {
            server.read_pkt(&datagram, clock.now()).expect("delivering");
            moved = true;
        }
        clock.advance(PACING_STEP_NANOS);
        if !client_sink.lock().unwrap().max_uni.is_empty() {
            break;
        }
        if !moved {
            break;
        }
    }

    let raised = client_sink.lock().unwrap().max_uni.clone();
    assert!(
        !raised.is_empty(),
        "raising a unidirectional limit from zero must be reported"
    );
    assert!(
        client.open_uni_stream().is_ok(),
        "and the open it permits must then succeed"
    );
}

#[test]
fn a_write_to_a_finished_stream_says_the_stream_is_closed_not_that_the_call_was_wrong() {
    // These two used to share an error kind. A layer multiplexing many streams over one
    // connection has to tell them apart: one means stop offering bytes for this stream and
    // carry on with the others, the other means fix a bug.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();
    let (mut client, _server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new(),
    );

    let stream = client.open_bidi_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];

    // Finish the write side.
    client
        .write_stream(&mut buf, stream, b"last", true, clock.now())
        .expect("the finishing write is allowed");

    let refused = client
        .write_stream(&mut buf, stream, b"more", false, clock.now())
        .expect_err("writing after the end must fail");

    assert_eq!(
        refused.kind(),
        ErrorKind::StreamClosed,
        "a finished stream reports itself closed, not that the caller made a mistake"
    );
}

#[test]
fn a_vectored_write_arrives_as_one_ordered_run_of_bytes() {
    // The ranges are joined into the copy retention has to make anyway, so this costs
    // nothing over a caller joining them -- and a caller joining them would pay twice.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Arc::default();
    let server_sink: Shared = Arc::default();
    let (mut client, mut server, clock) = connected(
        &credentials,
        &client_sink,
        &server_sink,
        TransportParams::new(),
    );

    let stream = client.open_bidi_stream().expect("opening a stream");
    let mut buf = vec![0u8; 1500];

    let head = b"HEAD".as_slice();
    let middle = b"-middle-".as_slice();
    let tail = b"TAIL".as_slice();
    let expected: Vec<u8> = [head, middle, tail].concat();

    let accepted = match client
        .write_stream_vectored(&mut buf, stream, &[head, middle, tail], true, clock.now())
        .expect("writing several ranges")
    {
        StreamWrite::Datagram { len, accepted } => {
            server.read_pkt(&buf[..len], clock.now()).expect("reading");
            accepted
        }
        other => panic!("expected a datagram, got {other:?}"),
    };

    assert_eq!(
        accepted,
        expected.len(),
        "the accepted count covers the whole offer, not just its first range"
    );

    let seen = server_sink.lock().unwrap();
    let bytes: Vec<u8> = seen
        .data
        .iter()
        .filter(|(id, _)| *id == stream.get())
        .flat_map(|(_, d)| d.clone())
        .collect();
    assert_eq!(
        bytes, expected,
        "the ranges must arrive concatenated and in the order given"
    );
}
