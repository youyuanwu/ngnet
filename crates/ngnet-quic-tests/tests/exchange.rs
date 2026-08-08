//! Exchanging data on streams after a completed handshake.
//!
//! The handshake tests prove two endpoints can agree keys. These prove they can then
//! actually carry bytes — which exercises the parts of the API a caller spends its time in,
//! and the flow-control obligations that produce no error when forgotten.

use std::sync::{Arc, Mutex};

use ngnet_quic::{
    ApplicationErrorCode, Handlers, Inspection, StreamCloseReason, StreamId, StreamWrite, inspect,
};
use ngnet_quic_tests::{
    TEST_SERVER_NAME, TestClock, TestConn, TestCredentials, client_backend, client_conn, drain,
    pump, server_backend, server_conn,
};

/// What a connection's handlers observed.
#[derive(Default, Debug)]
struct Observed {
    opened: Vec<i64>,
    data: Vec<(i64, Vec<u8>, bool)>,
    closed: Vec<(i64, String)>,
    acked: Vec<(i64, u64)>,
    reset: Vec<(i64, u64)>,
}

/// `Arc<Mutex<_>>` rather than `Rc<RefCell<_>>`, because handlers must be `Send`: a `Conn`
/// is `Send` and owns them, so a non-`Send` capture would let safe code move a non-atomic
/// refcount across threads. The compiler enforces it now; this is what that costs.
type Shared = Arc<Mutex<Observed>>;

fn handlers(sink: &Shared) -> Handlers<'_> {
    let a = Arc::clone(sink);
    let b = Arc::clone(sink);
    let c = Arc::clone(sink);
    let d = Arc::clone(sink);
    let e = Arc::clone(sink);
    Handlers::new()
        .on_stream_reset(move |id, code| {
            e.lock().unwrap().reset.push((id.get(), code.get()));
        })
        .on_stream_open(move |id| a.lock().unwrap().opened.push(id.get()))
        .on_stream_data(move |id, data, fin| {
            b.lock().unwrap().data.push((id.get(), data.to_vec(), fin));
        })
        .on_stream_close(move |id, reason| {
            // A wildcard is required rather than optional: `StreamCloseReason` is
            // `#[non_exhaustive]`, which is the crate promising it may add reasons without
            // that being a breaking change.
            let text = match reason {
                StreamCloseReason::Finished => "finished".to_string(),
                StreamCloseReason::Reset(code) => format!("reset:{}", code.get()),
                other => format!("{other:?}"),
            };
            c.lock().unwrap().closed.push((id.get(), text));
        })
        .on_acked_stream_data(move |id, len| d.lock().unwrap().acked.push((id.get(), len)))
}

fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:41001".parse().unwrap(),
        "127.0.0.1:41002".parse().unwrap(),
    )
}

/// Brings up a handshaked pair with handlers attached to both ends.
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
        "the handshake must complete before data can be exchanged"
    );

    (client, server, clock)
}

/// Sends `payload` on `stream` and relays until it is delivered.
///
/// Two details matter and are easy to get wrong. The FIN goes on the **last** write, not
/// the first: setting it early closes the write side, and the next attempt is refused. And
/// the clock must advance between writes, because ngtcp2 paces its sending.
fn send_on(
    from: &mut TestConn<'_>,
    to: &mut TestConn<'_>,
    clock: &TestClock,
    stream: StreamId,
    payload: &[u8],
    fin: bool,
) {
    let mut buf = vec![0u8; 1500];
    let mut offset = 0;
    let mut fin_sent = false;

    for _ in 0..512 {
        if offset >= payload.len() && (!fin || fin_sent) {
            break;
        }

        let remaining = &payload[offset..];
        // Only the write that carries the last byte may carry the FIN.
        let with_fin = fin && offset + remaining.len() >= payload.len();

        let outcome = from
            .write_stream(&mut buf, stream, remaining, with_fin, clock.now())
            .expect("writing stream data");

        match outcome {
            StreamWrite::Datagram { len, accepted } => {
                offset += accepted;
                if with_fin && offset >= payload.len() {
                    fin_sent = true;
                }
                to.read_pkt(&buf[..len], clock.now()).expect("delivering");
            }
            StreamWrite::Idle
            | StreamWrite::Blocked
            | StreamWrite::StreamBlocked
            | StreamWrite::ConnectionBlocked => {
                // Let acknowledgements and window updates come back.
                let _ = pump(from, to, clock, 4);
            }
        }
        clock.advance(ngnet_quic_tests::PACING_STEP_NANOS);
    }

    // Drain both sides so the receiver sees everything that was sent.
    let _ = pump(from, to, clock, 16);
}

#[test]
fn a_client_can_send_a_payload_to_the_server() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().expect("opening a stream");
    let payload = b"the quick brown fox jumps over the lazy dog";
    send_on(&mut client, &mut server, &clock, stream, payload, true);

    let observed = server_sink.lock().unwrap();
    assert!(
        observed.opened.contains(&stream.get()),
        "the server should have seen the stream open, saw {:?}",
        observed.opened
    );

    let received: Vec<u8> = observed
        .data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, bytes, _)| bytes.clone())
        .collect();
    assert_eq!(
        received, payload,
        "the bytes received must be identical to those sent"
    );
}

#[test]
fn the_server_observes_the_end_of_the_stream() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().unwrap();
    send_on(&mut client, &mut server, &clock, stream, b"short", true);

    let observed = server_sink.lock().unwrap();
    assert!(
        observed
            .data
            .iter()
            .any(|(id, _, fin)| *id == stream.get() && *fin),
        "the server should have seen the FIN, saw {:?}",
        observed.data
    );
}

#[test]
fn the_sender_is_told_which_bytes_were_acknowledged() {
    // This is what releases retransmission buffers. An application that ignores it holds
    // every byte it ever sent, so it is worth proving the event actually arrives.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().unwrap();
    send_on(
        &mut client,
        &mut server,
        &clock,
        stream,
        b"acknowledge me",
        true,
    );
    let _ = pump(&mut client, &mut server, &clock, 16);

    let observed = client_sink.lock().unwrap();
    let total: u64 = observed
        .acked
        .iter()
        .filter(|(id, _)| *id == stream.get())
        .map(|(_, len)| *len)
        .sum();
    assert!(
        total > 0,
        "the sender should have been told its data was acknowledged, saw {:?}",
        observed.acked
    );
}

#[test]
fn both_directions_carry_data_on_one_bidirectional_stream() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().unwrap();
    send_on(&mut client, &mut server, &clock, stream, b"request", false);
    send_on(&mut server, &mut client, &clock, stream, b"response", false);

    let from_client: Vec<u8> = server_sink
        .lock()
        .unwrap()
        .data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, b, _)| b.clone())
        .collect();
    let from_server: Vec<u8> = client_sink
        .lock()
        .unwrap()
        .data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, b, _)| b.clone())
        .collect();

    assert_eq!(from_client, b"request");
    assert_eq!(from_server, b"response");
}

#[test]
fn a_payload_larger_than_one_datagram_arrives_intact() {
    // Anything that fits in a single packet would not exercise segmentation, reassembly or
    // the flow-control loop at all.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().unwrap();
    let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
    send_on(&mut client, &mut server, &clock, stream, &payload, true);

    let received: Vec<u8> = server_sink
        .lock()
        .unwrap()
        .data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, b, _)| b.clone())
        .collect();
    assert_eq!(
        received.len(),
        payload.len(),
        "every byte of a multi-datagram payload must arrive"
    );
    assert_eq!(received, payload, "and in the order they were sent");
}

#[test]
fn a_unidirectional_stream_carries_data_one_way() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_uni_stream().expect("opening a uni stream");
    assert_eq!(
        stream.directionality(),
        ngnet_quic::Directionality::Unidirectional
    );
    send_on(&mut client, &mut server, &clock, stream, b"one way", true);

    let received: Vec<u8> = server_sink
        .lock()
        .unwrap()
        .data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, b, _)| b.clone())
        .collect();
    assert_eq!(received, b"one way");
}

#[test]
fn a_reset_stream_is_reported_to_the_peer() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let stream = client.open_bidi_stream().unwrap();
    send_on(&mut client, &mut server, &clock, stream, b"partial", false);

    client
        .reset_stream(stream, ApplicationErrorCode::new(0x99))
        .expect("resetting the stream");
    let _ = pump(&mut client, &mut server, &clock, 16);

    // Asserting on the reset specifically, and on its error code. An earlier version of
    // this test accepted "or any data event on the stream", which the `send_on` above had
    // already guaranteed -- so it passed whether or not RESET_STREAM was ever emitted.
    let observed = server_sink.lock().unwrap();
    assert!(
        observed
            .reset
            .iter()
            .any(|(id, code)| *id == stream.get() && *code == 0x99),
        "the server should have seen the stream reset with code 0x99, saw reset={:?}",
        observed.reset
    );
}

#[test]
fn an_ordinary_close_is_not_an_error_for_either_side() {
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (mut client, mut server, clock) = connected(&credentials, &client_sink, &server_sink);

    let mut buf = vec![0u8; 1500];
    let written = client
        .write_connection_close(&mut buf, ApplicationErrorCode::new(0), b"bye", clock.now())
        .expect("writing a close packet");
    assert!(written > 0);
    assert!(client.in_closing_period());

    // The peer should accept it as an ordinary end, reporting draining rather than failing.
    let outcome = server
        .read_pkt(&buf[..written], clock.now())
        .expect("an ordinary close must not be an error");
    assert!(
        matches!(
            outcome,
            ngnet_quic::ReadOutcome::Draining | ngnet_quic::ReadOutcome::Processed
        ),
        "expected the peer to enter draining, got {outcome:?}"
    );
}

#[test]
fn stream_credit_becomes_available_once_the_peer_transport_parameters_arrive() {
    // Before the handshake there is no credit; after it there is. A caller that opened
    // streams eagerly would otherwise see an unexplained failure.
    let credentials = TestCredentials::generate();
    let client_sink: Shared = Shared::default();
    let server_sink: Shared = Shared::default();
    let (client, _server, _clock) = connected(&credentials, &client_sink, &server_sink);

    assert!(
        client.streams_bidi_left() > 0,
        "a handshaked client should have stream credit"
    );
}
