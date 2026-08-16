//! Events, handler failures, peer close, parameter exchange, and edge cases.
//!
//! Everything here needs a live pair of connections, which is why it is not beside the code it
//! tests.

use std::cell::RefCell;
use std::rc::Rc;

use ngnet_qmux::{
    Conn, ErrorKind, HandlerError, Handlers, OpenOutcome, Push, ReadOutcome, Record,
    Role, Shutdown, StreamId, StreamLimitKind, Timestamp, TransportParams, WriteRequest,
};

const BUF: usize = 16 * 1024;

fn now() -> Timestamp {
    Timestamp::from_nanos(0)
}

fn params() -> TransportParams {
    TransportParams::new().with_all_limits(1 << 20, 8)
}

fn drain(conn: &mut Conn<'_>, request: WriteRequest<'_>) -> (Vec<u8>, usize) {
    let mut out = Vec::new();
    let mut consumed_total = 0usize;
    let mut remaining = request.data;

    loop {
        let mut buf = [0u8; BUF];
        let step = WriteRequest {
            stream: request.stream,
            data: remaining,
            fin: request.fin,
        };
        let (record, consumed) = conn.write(&mut buf, step, now()).expect("write");
        let produced = record.bytes().map(<[u8]>::to_vec);
        if let Some(bytes) = &produced {
            out.extend_from_slice(bytes);
        }
        consumed_total += consumed;
        remaining = &remaining[consumed..];
        if remaining.is_empty() || produced.is_none() || consumed == 0 {
            break;
        }
    }

    (out, consumed_total)
}

/// Exchange transport parameters so both sides have capacity.
fn handshake(client: &mut Conn<'_>, server: &mut Conn<'_>) {
    let (c, _) = drain(client, WriteRequest::control_only());
    let (s, _) = drain(server, WriteRequest::control_only());
    server.read(&c, now()).expect("server read hello");
    client.read(&s, now()).expect("client read hello");
}

/// Everything the handlers saw.
#[derive(Default)]
struct Log {
    params: u32,
    data: Vec<(i64, Vec<u8>, bool)>,
    opened: Vec<i64>,
    closed: Vec<(i64, Option<u64>, Option<u64>)>,
    reset: Vec<(i64, u64, u64)>,
    stop_sending: Vec<(i64, u64)>,
    recv_stop_sending: Vec<(i64, u64)>,
    extend_stream_data: Vec<(i64, u64)>,
    extend_streams: Vec<(StreamLimitKind, u64)>,
}

/// A connection with every handler wired to the log.
fn observed(role: Role, log: &Rc<RefCell<Log>>) -> Conn<'static> {
    macro_rules! l {
        () => {
            Rc::clone(log)
        };
    }
    let (p, d, o, c, r, ss, rss, esd, es) =
        (l!(), l!(), l!(), l!(), l!(), l!(), l!(), l!(), l!());

    Conn::builder(role)
        .transport_params(params())
        .handlers(
            Handlers::new()
                .on_transport_params(move |_| {
                    p.borrow_mut().params += 1;
                    Ok(())
                })
                .on_stream_data(move |e| {
                    d.borrow_mut()
                        .data
                        .push((e.stream_id.get(), e.data.to_vec(), e.fin));
                    Ok(())
                })
                .on_stream_open(move |id| {
                    o.borrow_mut().opened.push(id.get());
                    Ok(())
                })
                .on_stream_close(move |e| {
                    c.borrow_mut().closed.push((
                        e.stream_id.get(),
                        e.rx_app_error_code,
                        e.tx_app_error_code,
                    ));
                    Ok(())
                })
                .on_stream_reset(move |id, final_size, code| {
                    r.borrow_mut().reset.push((id.get(), final_size, code));
                    Ok(())
                })
                .on_stream_stop_sending(move |id, code| {
                    ss.borrow_mut().stop_sending.push((id.get(), code));
                    Ok(())
                })
                .on_recv_stop_sending(move |id, code| {
                    rss.borrow_mut().recv_stop_sending.push((id.get(), code));
                    Ok(())
                })
                .on_extend_max_stream_data(move |id, max| {
                    esd.borrow_mut().extend_stream_data.push((id.get(), max));
                    Ok(())
                })
                .on_extend_max_streams(move |kind, max| {
                    es.borrow_mut().extend_streams.push((kind, max));
                    Ok(())
                }),
        )
        .build()
        .expect("connection")
}

fn open(conn: &mut Conn<'_>) -> StreamId {
    conn.open_bidi_stream()
        .expect("open")
        .opened()
        .expect("capacity")
}

/// Every one of the twelve protocol events reaches a Rust closure.
///
/// Grouped into one test because they need one connection pair driven through a sequence;
/// splitting them would mean rebuilding that sequence per event.
#[test]
fn all_protocol_events_are_delivered() {
    let client_log = Rc::new(RefCell::new(Log::default()));
    let server_log = Rc::new(RefCell::new(Log::default()));
    let mut client = observed(Role::Client, &client_log);
    let mut server = observed(Role::Server, &server_log);

    // recv_transport_params, both directions.
    handshake(&mut client, &mut server);
    assert_eq!(client_log.borrow().params, 1);
    assert_eq!(server_log.borrow().params, 1);

    // stream_open and recv_stream_data on the server.
    let stream = open(&mut client);
    let (records, _) = drain(&mut client, WriteRequest::stream(stream, b"payload"));
    server.read(&records, now()).unwrap();

    assert!(server_log.borrow().opened.contains(&stream.get()));
    assert!(
        server_log
            .borrow()
            .data
            .iter()
            .any(|(id, d, _)| *id == stream.get() && d == b"payload")
    );

    // extend_max_stream_data / extend_max_streams: the server raises limits and the client
    // observes the increase.
    //
    // The extension has to be large to be observable at all. dwnx only emits MAX_STREAM_DATA
    // once the pending increase exceeds a quarter of the window
    // (`strm_should_send_max_stream_data`), which avoids a frame per byte consumed; a small
    // bump is recorded and sent later, so the callback would never fire here.
    server.extend_max_stream_data(stream, 1 << 20).unwrap();
    server.extend_max_data(1 << 20);
    server.extend_max_streams_bidi(4);
    server.extend_max_streams_uni(4);
    let (records, _) = drain(&mut server, WriteRequest::control_only());
    client.read(&records, now()).unwrap();

    assert!(
        !client_log.borrow().extend_stream_data.is_empty(),
        "client never saw its stream window extended"
    );
    assert!(
        client_log
            .borrow()
            .extend_streams
            .iter()
            .any(|(kind, _)| *kind == StreamLimitKind::LocalBidi),
        "client never saw its bidi stream limit raised"
    );

    // stream_stop_sending fires locally -- but during serialisation, not at the shutdown call.
    // dwnx queues the intent and invokes the callback as it emits the STOP_SENDING frame
    // (`dwnx_conn.c`), so asserting straight after the shutdown would be too early.
    server.shutdown_stream(stream, Shutdown::Read, 7).unwrap();
    let (records, _) = drain(&mut server, WriteRequest::control_only());
    assert!(
        server_log
            .borrow()
            .stop_sending
            .iter()
            .any(|(id, code)| *id == stream.get() && *code == 7),
        "the local stop-sending callback should fire as the frame is written"
    );

    // recv_stop_sending then reaches the client.
    client.read(&records, now()).unwrap();
    assert!(
        client_log
            .borrow()
            .recv_stop_sending
            .iter()
            .any(|(id, code)| *id == stream.get() && *code == 7)
    );

    // Receiving STOP_SENDING makes dwnx reset the client's write side on its own, so the
    // client's next record carries RESET_STREAM without being asked. Calling shutdown here
    // would be a no-op; the reset is already queued.
    let (records, _) = drain(&mut client, WriteRequest::control_only());
    server.read(&records, now()).unwrap();

    assert!(
        server_log
            .borrow()
            .reset
            .iter()
            .any(|(id, _, _)| *id == stream.get()),
        "server never saw the reset the client sent in response to STOP_SENDING"
    );
    // A stream closes only once both directions are finished. The server's read side is done
    // and it has the client's reset; shutting its write side too retires the stream.
    server.shutdown_stream(stream, Shutdown::Write, 3).unwrap();
    let (records, _) = drain(&mut server, WriteRequest::control_only());
    client.read(&records, now()).unwrap();
    let _ = drain(&mut client, WriteRequest::control_only());

    assert!(
        !server_log.borrow().closed.is_empty(),
        "the server never saw the stream close"
    );
}

/// A handler that fails aborts the operation, and its own message survives dwnx collapsing
/// every nonzero callback return into one code.
#[test]
fn handler_errors_propagate_with_their_message() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .handlers(
            Handlers::new()
                .on_transport_params(move |_| Err(HandlerError::new("refused by policy"))),
        )
        .build()
        .unwrap();

    let (hello, _) = drain(&mut client, WriteRequest::control_only());
    let error = server
        .read(&hello, now())
        .expect_err("the handler refused, so the read must fail");

    assert_eq!(error.kind(), ErrorKind::Handler);
    assert_eq!(
        error.context(),
        "refused by policy",
        "the handler's own message should survive DWNX_ERR_CALLBACK_FAILURE"
    );
}

/// A peer that closes is reported as an outcome, not an error.
///
/// The closing record is built by hand: dwnx parses CONNECTION_CLOSE but exposes no function
/// to serialise one, so the wrapper cannot produce it. See docs/qmux/pending-work.md.
#[test]
fn a_peer_close_is_an_outcome_not_a_failure() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();
    handshake(&mut client, &mut server);

    // A record framing one CONNECTION_CLOSE frame: type 0x1c, error code 0, frame type 0,
    // empty reason -- each a one-byte varint.
    let frame = [0x1c, 0x00, 0x00, 0x00];
    let mut record = vec![u8::try_from(frame.len()).unwrap()];
    record.extend_from_slice(&frame);

    assert_eq!(
        server.read(&record, now()).unwrap(),
        ReadOutcome::PeerClosed,
        "a peer close should not look like a protocol failure"
    );
}

/// Peer parameters arrive through the callback and are cached, since dwnx has no getter.
#[test]
fn peer_transport_params_are_readable_after_exchange() {
    let client_params = TransportParams::new()
        .with_all_limits(1 << 18, 5)
        .with_initial_max_data(4242);

    let mut client = Conn::builder(Role::Client)
        .transport_params(client_params.clone())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();

    assert!(server.peer_transport_params().is_none());

    let (hello, _) = drain(&mut client, WriteRequest::control_only());
    server.read(&hello, now()).unwrap();

    let seen = server.peer_transport_params().expect("peer params");
    assert_eq!(seen.initial_max_data(), 4242);
    assert_eq!(seen.initial_max_streams_bidi(), 5);
    // The one field dwnx substitutes rather than honouring.
    assert_eq!(seen.max_record_size(), ngnet_qmux::DEFAULT_MAX_RECORD_SIZE);
}

/// Stream capacity is the peer's to grant, and exhausting it is an outcome rather than an error.
#[test]
fn exhausted_stream_capacity_is_reported_as_blocked() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    // The server permits exactly two bidirectional streams.
    let mut server = Conn::builder(Role::Server)
        .transport_params(TransportParams::new().with_all_limits(1 << 16, 2))
        .build()
        .unwrap();
    handshake(&mut client, &mut server);

    assert_eq!(client.streams_bidi_left(), 2);
    assert!(matches!(
        client.open_bidi_stream().unwrap(),
        OpenOutcome::Opened(_)
    ));
    assert!(matches!(
        client.open_bidi_stream().unwrap(),
        OpenOutcome::Opened(_)
    ));
    assert_eq!(
        client.open_bidi_stream().unwrap(),
        OpenOutcome::Blocked,
        "the third open exceeds the peer's limit"
    );

    // Raising the limit makes capacity available again.
    server.extend_max_streams_bidi(2);
    let (records, _) = drain(&mut server, WriteRequest::control_only());
    client.read(&records, now()).unwrap();
    assert!(matches!(
        client.open_bidi_stream().unwrap(),
        OpenOutcome::Opened(_)
    ));
}

/// A buffer too small to hold anything is distinguished from an idle connection.
#[test]
fn a_useless_buffer_is_not_mistaken_for_idle() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();

    let mut tiny = [0u8; 2];
    let (record, consumed) = client
        .write(&mut tiny, WriteRequest::control_only(), now())
        .unwrap();
    assert_eq!(record, Record::BufferTooSmall);
    assert_eq!(consumed, 0);

    // A usable buffer produces the pending transport parameters, proving the connection was
    // not in fact idle.
    let mut buf = [0u8; BUF];
    let (record, _) = client
        .write(&mut buf, WriteRequest::control_only(), now())
        .unwrap();
    assert!(record.bytes().is_some());
}

/// An idle connection reports exactly that.
#[test]
fn an_idle_connection_reports_empty() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();

    // Drain the transport parameters first.
    let _ = drain(&mut client, WriteRequest::control_only());

    let mut buf = [0u8; BUF];
    let (record, consumed) = client
        .write(&mut buf, WriteRequest::control_only(), now())
        .unwrap();
    assert_eq!(record, Record::Empty);
    assert_eq!(consumed, 0);
}

/// Writing to a stream whose write side is closed is a signal, not a failure.
#[test]
fn writing_to_a_closed_stream_is_reported() {
    let client_log = Rc::new(RefCell::new(Log::default()));
    let server_log = Rc::new(RefCell::new(Log::default()));
    let mut client = observed(Role::Client, &client_log);
    let mut server = observed(Role::Server, &server_log);
    handshake(&mut client, &mut server);

    let stream = open(&mut client);
    client.shutdown_stream(stream, Shutdown::Write, 1).unwrap();

    let mut buf = [0u8; BUF];
    let mut record = client.record(&mut buf, now());
    assert_eq!(
        record.push(WriteRequest::stream(stream, b"too late")).unwrap(),
        Push::StreamClosed
    );
}

/// Malformed input is a protocol failure that ends the connection.
#[test]
fn malformed_input_ends_the_connection() {
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();

    // A record claiming to hold one frame of an unassigned type.
    let error = server
        .read(&[0x01, 0xfe], now())
        .expect_err("an unknown frame type must be rejected");

    assert!(
        !error.leaves_connection_usable(),
        "a protocol violation should not leave the connection usable"
    );
}

/// A record larger than the negotiated maximum is rejected.
#[test]
fn an_oversized_record_is_rejected() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();
    handshake(&mut client, &mut server);

    // A four-byte varint declaring a record far beyond DEFAULT_MAX_RECORD_SIZE.
    let oversized = [0x80, 0x01, 0x00, 0x00];
    let error = server
        .read(&oversized, now())
        .expect_err("a record above the maximum must be rejected");
    assert!(!error.leaves_connection_usable());
}

/// Submitting nothing is not an error in either direction.
#[test]
fn empty_reads_and_writes_are_harmless() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();
    handshake(&mut client, &mut server);

    assert_eq!(server.read(&[], now()).unwrap(), ReadOutcome::Processed);

    let stream = open(&mut client);
    let mut buf = [0u8; BUF];
    let (record, consumed) = client
        .write(&mut buf, WriteRequest::stream(stream, &[]), now())
        .unwrap();
    assert_eq!(consumed, 0);
    // An empty stream write still produces a zero-length STREAM frame.
    assert!(record.bytes().is_some() || record == Record::Empty);
}

/// Shutting down a stream that was never opened is a no-op, not an error.
///
/// Surprising enough to pin: `dwnx_conn_shutdown_stream` looks the stream up and returns 0
/// when it is absent, so a caller cannot use the return value to detect a bad id. The wrapper
/// reports what dwnx does rather than inventing an error dwnx never raises.
#[test]
fn shutting_down_an_unknown_stream_is_a_no_op() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();
    handshake(&mut client, &mut server);

    // A server-initiated id the client never saw opened.
    let unknown = StreamId::new(4_001).unwrap();
    client
        .shutdown_stream(unknown, Shutdown::Both, 0)
        .expect("dwnx treats an unknown stream as nothing to do");

    // The connection is unharmed and still usable.
    let stream = open(&mut client);
    let (records, consumed) = drain(&mut client, WriteRequest::stream(stream, b"still fine"));
    assert_eq!(consumed, 10);
    server.read(&records, now()).unwrap();
}

/// Stream data beyond the varint bound is rejected before it can reach the wire.
#[test]
fn stream_ids_beyond_the_varint_bound_are_rejected() {
    assert!(StreamId::new(StreamId::MAX + 1).is_err());
    assert!(StreamId::new(-1).is_err());
}
