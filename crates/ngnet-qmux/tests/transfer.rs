//! An in-memory client and server exchanging real QMux records.
//!
//! No socket and no TLS appear here, and that is not a shortcut. QMux runs over any ordered,
//! reliable, bidirectional byte stream and encrypts nothing itself; the draft explicitly
//! permits substrates that provide no security at all. Relaying bytes between two connections
//! in memory is therefore a legitimate deployment of the protocol, and it exercises exactly
//! the same code a TCP or TLS carrier would.

use std::cell::RefCell;
use std::rc::Rc;

use ngnet_qmux::{
    Conn, Error, ReadOutcome, Role, StreamId, Timestamp, TransportParams, WriteRequest,
};

/// A buffer big enough for a full record.
const BUF: usize = 16 * 1024;

fn now() -> Timestamp {
    Timestamp::from_nanos(0)
}

/// Limits generous enough that flow control never interferes.
fn params() -> TransportParams {
    TransportParams::new().with_all_limits(1 << 20, 32)
}

/// Serialise everything a connection wants to send, following the record loop.
///
/// Returns the bytes and how much of the payload went into them. Several records may be
/// needed, so this keeps going until the payload is exhausted or the stream stops accepting.
fn drain(conn: &mut Conn<'_>, request: WriteRequest<'_>) -> Result<(Vec<u8>, usize), Error> {
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
        let (record, consumed) = conn.write(&mut buf, step, now())?;

        let produced = record.bytes().map(<[u8]>::to_vec);
        if let Some(bytes) = &produced {
            out.extend_from_slice(bytes);
        }

        consumed_total += consumed;
        remaining = &remaining[consumed..];

        // Stop when there is nothing left to send and nothing more was produced.
        if remaining.is_empty() || produced.is_none() || consumed == 0 {
            break;
        }
    }

    Ok((out, consumed_total))
}

/// What the peer observed, recorded by handlers.
#[derive(Default)]
struct Observed {
    stream_data: Vec<(i64, Vec<u8>, bool)>,
    opened: Vec<i64>,
    params_seen: bool,
}

fn build(role: Role, observed: &Rc<RefCell<Observed>>) -> Conn<'static> {
    let for_data = Rc::clone(observed);
    let for_open = Rc::clone(observed);
    let for_params = Rc::clone(observed);

    Conn::builder(role)
        .transport_params(params())
        .handlers(
            ngnet_qmux::Handlers::new()
                .on_stream_data(move |event| {
                    for_data.borrow_mut().stream_data.push((
                        event.stream_id.get(),
                        event.data.to_vec(),
                        event.fin,
                    ));
                    Ok(())
                })
                .on_stream_open(move |id| {
                    for_open.borrow_mut().opened.push(id.get());
                    Ok(())
                })
                .on_transport_params(move |_| {
                    for_params.borrow_mut().params_seen = true;
                    Ok(())
                }),
        )
        .build()
        .expect("connection")
}

/// Feed bytes to a connection, optionally one at a time.
fn feed(conn: &mut Conn<'_>, bytes: &[u8], fragment: bool) -> Result<ReadOutcome, Error> {
    if fragment {
        let mut last = ReadOutcome::Processed;
        for byte in bytes {
            last = conn.read(&[*byte], now())?;
        }
        Ok(last)
    } else {
        conn.read(bytes, now())
    }
}

/// The headline test: a stream transfer between two connections, end to end.
fn transfer(fragment: bool) {
    let client_seen = Rc::new(RefCell::new(Observed::default()));
    let server_seen = Rc::new(RefCell::new(Observed::default()));

    let mut client = build(Role::Client, &client_seen);
    let mut server = build(Role::Server, &server_seen);

    // Each side announces its transport parameters before anything else can flow.
    let (client_hello, _) = drain(&mut client, WriteRequest::control_only()).unwrap();
    let (server_hello, _) = drain(&mut server, WriteRequest::control_only()).unwrap();
    assert!(!client_hello.is_empty(), "client sent no transport params");

    feed(&mut server, &client_hello, fragment).unwrap();
    feed(&mut client, &server_hello, fragment).unwrap();

    assert!(
        server_seen.borrow().params_seen,
        "server never saw the client's transport parameters"
    );
    assert!(client.peer_transport_params().is_some());
    assert!(server.peer_transport_params().is_some());

    // The client opens a stream and sends a payload.
    let stream = client
        .open_bidi_stream()
        .unwrap()
        .opened()
        .expect("no stream capacity");

    let payload = b"the quick brown fox jumps over the lazy dog";
    let (records, consumed) =
        drain(&mut client, WriteRequest::stream(stream, payload).with_fin(true)).unwrap();
    assert_eq!(consumed, payload.len(), "not all data was serialised");

    feed(&mut server, &records, fragment).unwrap();

    // What arrived must be exactly what was sent, on the same stream.
    let seen = server_seen.borrow();
    let received: Vec<u8> = seen
        .stream_data
        .iter()
        .filter(|(id, _, _)| *id == stream.get())
        .flat_map(|(_, data, _)| data.clone())
        .collect();

    assert_eq!(received, payload, "payload did not survive the round trip");
    assert!(
        seen.stream_data.iter().any(|(_, _, fin)| *fin),
        "the stream never signalled fin"
    );
}

#[test]
fn stream_data_survives_a_round_trip() {
    transfer(false);
}

/// The same transfer with every byte delivered separately, which splits records across calls.
///
/// dwnx buffers a partial record and resumes, so the observable events must be identical. This
/// is the property a real transport makes unavoidable -- TCP does not preserve write
/// boundaries -- so it is worth proving rather than assuming.
#[test]
fn records_may_be_split_across_reads() {
    transfer(true);
}

/// A connection with no handlers at all still runs; the events simply go unobserved.
#[test]
fn a_connection_without_handlers_still_transfers() {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .unwrap();

    // Both directions: stream capacity comes from the *peer's* advertised limits, so a
    // connection cannot open anything until it has read them.
    let (client_hello, _) = drain(&mut client, WriteRequest::control_only()).unwrap();
    let (server_hello, _) = drain(&mut server, WriteRequest::control_only()).unwrap();
    assert_eq!(
        server.read(&client_hello, now()).unwrap(),
        ReadOutcome::Processed
    );
    assert_eq!(
        client.read(&server_hello, now()).unwrap(),
        ReadOutcome::Processed
    );

    // The parameters were cached even with no handler asking for them.
    assert!(server.peer_transport_params().is_some());
    assert!(client.peer_transport_params().is_some());

    let stream = client
        .open_bidi_stream()
        .unwrap()
        .opened()
        .expect("no stream capacity");
    let (records, consumed) =
        drain(&mut client, WriteRequest::stream(stream, b"payload")).unwrap();
    assert_eq!(consumed, 7);
    assert_eq!(
        server.read(&records, now()).unwrap(),
        ReadOutcome::Processed
    );
}

/// Streams opened by the client carry client-initiated ids, and the server sees them as remote.
#[test]
fn stream_ownership_is_visible_from_both_sides() {
    let client_seen = Rc::new(RefCell::new(Observed::default()));
    let server_seen = Rc::new(RefCell::new(Observed::default()));
    let mut client = build(Role::Client, &client_seen);
    let mut server = build(Role::Server, &server_seen);

    // Before the peer's parameters arrive there is no capacity at all: the limits are the
    // peer's to grant, and dwnx's defaults grant none.
    assert_eq!(client.streams_bidi_left(), 0);
    assert_eq!(
        client.open_bidi_stream().unwrap(),
        ngnet_qmux::OpenOutcome::Blocked,
        "a stream should not be openable before the peer advertises capacity"
    );

    let (server_hello, _) = drain(&mut server, WriteRequest::control_only()).unwrap();
    client.read(&server_hello, now()).unwrap();
    assert_eq!(client.streams_bidi_left(), 32);

    let first = client
        .open_bidi_stream()
        .unwrap()
        .opened()
        .expect("no stream capacity");

    assert_eq!(first.get(), 0, "the first client bidi stream is id 0");
    assert!(first.is_bidirectional());
    assert!(client.is_local_stream(first));
    assert!(!client.is_local_stream(StreamId::new(1).unwrap()));
}
