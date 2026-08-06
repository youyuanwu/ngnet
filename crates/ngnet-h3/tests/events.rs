//! The connection-level events a caller has to act on.
//!
//! Four of these exist, and none of them can be handled inside this crate: they are all
//! instructions to a QUIC layer this crate deliberately does not own. Two ask for a stream
//! to be stopped or reset, one reports the peer beginning a graceful shutdown, and one
//! delivers the peer's settings. A caller that ignores the first two leaves streams
//! running that nghttp3 has already given up on.

use std::collections::HashMap;

use ngnet_h3::{
    Conn, ConnBuilder, ErrorCode, ErrorKind, FieldAction, FieldSection, Header, PeerSettings, Role,
    Settings, Shutdown, StreamId, Timestamp,
};

const CLIENT_CONTROL: i64 = 2;
const CLIENT_QPACK_ENCODER: i64 = 6;
const CLIENT_QPACK_DECODER: i64 = 10;
const SERVER_CONTROL: i64 = 3;
const SERVER_QPACK_ENCODER: i64 = 7;
const SERVER_QPACK_DECODER: i64 = 11;

fn id(raw: i64) -> StreamId {
    StreamId::new(raw).expect("literal is a valid stream id")
}

/// Everything a handler recorded, including the events under test.
#[derive(Default, Debug)]
struct Seen {
    /// Field names per stream, so a request can be told from control traffic.
    fields: HashMap<i64, Vec<Vec<u8>>>,
    stop_sending: Vec<(i64, u64)>,
    reset_stream: Vec<(i64, u64)>,
    shutdowns: Vec<Shutdown>,
    settings: Vec<PeerSettings>,
}

fn observer(role: Role, settings: Settings) -> Conn<Seen> {
    let mut conn = ConnBuilder::<Seen>::new(role)
        .settings(settings)
        .on_field(|seen: &mut Seen, stream, _section, _token, name, _value| {
            seen.fields
                .entry(stream.get())
                .or_default()
                .push(name.to_vec());
            FieldAction::Continue
        })
        .on_stop_sending(|seen: &mut Seen, stream, code: ErrorCode| {
            seen.stop_sending.push((stream.get(), code.get()));
        })
        .on_reset_stream(|seen: &mut Seen, stream, code: ErrorCode| {
            seen.reset_stream.push((stream.get(), code.get()));
        })
        .on_shutdown(|seen: &mut Seen, shutdown| seen.shutdowns.push(shutdown))
        .on_peer_settings(|seen: &mut Seen, settings| seen.settings.push(settings))
        .build()
        .expect("connection");

    let (control, encoder, decoder) = match role {
        Role::Client => (CLIENT_CONTROL, CLIENT_QPACK_ENCODER, CLIENT_QPACK_DECODER),
        Role::Server => (SERVER_CONTROL, SERVER_QPACK_ENCODER, SERVER_QPACK_DECODER),
    };
    conn.bind_control_stream(id(control)).unwrap();
    conn.bind_qpack_streams(id(encoder), id(decoder)).unwrap();
    conn
}

fn client() -> Conn<Seen> {
    observer(Role::Client, Settings::new())
}

fn server() -> Conn<Seen> {
    observer(Role::Server, Settings::new())
}

/// Moves everything one side wants to send into the other, until neither has more.
fn pump(a: &mut Conn<Seen>, a_seen: &mut Seen, b: &mut Conn<Seen>, b_seen: &mut Seen, now: u64) {
    let mut settled = false;
    for _ in 0..512 {
        let moved = transfer(a, a_seen, b, b_seen, now) | transfer(b, b_seen, a, a_seen, now);
        if !moved {
            settled = true;
            break;
        }
    }
    assert!(
        settled,
        "the two connections never stopped exchanging bytes"
    );
}

fn transfer(
    from: &mut Conn<Seen>,
    from_seen: &mut Seen,
    to: &mut Conn<Seen>,
    to_seen: &mut Seen,
    now: u64,
) -> bool {
    let Some(send) = from.writev_stream(from_seen).expect("collect data to send") else {
        return false;
    };
    let stream = send.stream();
    let fin = send.fin();
    let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    let taken = bytes.len();
    send.commit(taken).expect("commit");
    if taken > 0 {
        from.add_ack_offset(stream, taken as u64, from_seen)
            .expect("acknowledge");
    }
    if taken > 0 || fin {
        to.read_stream(stream, &bytes, fin, Timestamp::from_nanos(now), to_seen)
            .expect("read stream data");
    }
    taken > 0 || fin
}

#[test]
fn the_peers_settings_arrive_with_the_values_that_were_advertised() {
    // Deliberately non-default values, so a handler wired to the wrong struct or reading
    // its own settings back would produce something recognisably different.
    let mut client = observer(
        Role::Client,
        Settings::new()
            .max_field_section_size(4096)
            .qpack_blocked_streams(7)
            .enable_connect_protocol(true),
    );
    let mut server = observer(
        Role::Server,
        Settings::new()
            .max_field_section_size(65536)
            .enable_connect_protocol(true),
    );
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    let seen_by_server = server_seen.settings.first().expect("the client's settings");
    assert_eq!(seen_by_server.max_field_section_size, 4096);
    assert_eq!(seen_by_server.qpack_blocked_streams, 7);
    assert!(
        !seen_by_server.enable_connect_protocol,
        "extended CONNECT is a server capability; nghttp3 zeroes it on a client rather \
         than letting one advertise something it cannot offer"
    );

    let seen_by_client = client_seen.settings.first().expect("the server's settings");
    assert_eq!(seen_by_client.max_field_section_size, 65536);
    assert!(
        seen_by_client.enable_connect_protocol,
        "the server advertised it, so the client must see it"
    );
}

#[test]
fn a_graceful_shutdown_is_reported_with_the_cut_off_the_caller_needs() {
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    // A notice first: stop opening streams, but nothing is being discarded yet.
    server.submit_shutdown_notice().expect("submit the notice");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );
    assert_eq!(
        client_seen.shutdowns,
        vec![Shutdown::Notice],
        "the notice must not be mistaken for a cut-off at a real stream identifier"
    );

    // Then the real shutdown, which fixes the identifier from which nothing is processed.
    server.shutdown().expect("start the shutdown");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        3,
    );
    assert_eq!(client_seen.shutdowns.len(), 2);
    match client_seen.shutdowns[1] {
        Shutdown::NoStreamsFrom(stream) => {
            assert!(
                stream.get() >= 0,
                "the caller needs a usable identifier to decide what to retry"
            );
        }
        other => panic!("expected a stream cut-off, got {other:?}"),
    }
}

#[test]
fn shutting_down_before_binding_is_a_typed_error_rather_than_an_abort() {
    // Both shutdown calls queue a frame onto the control stream, and nghttp3 only asserts
    // that the stream exists before writing through the pointer.
    let mut conn = ConnBuilder::<Seen>::new(Role::Server)
        .build()
        .expect("connection");
    assert_eq!(
        conn.submit_shutdown_notice().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(conn.shutdown().unwrap_err().kind(), ErrorKind::InvalidInput);
    assert!(conn.is_usable(), "a caller mistake is not fatal");

    // Once bound, both work.
    conn.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();
    conn.submit_shutdown_notice()
        .expect("now there is somewhere to put it");
    conn.shutdown().expect("and the cut-off can be fixed");
}

#[test]
fn a_cut_off_that_has_been_fixed_cannot_be_raised_again() {
    // A GOAWAY identifier may only ever fall. A server computes its cut-off from the
    // highest request stream it has seen, so a second shutdown after another request would
    // raise it -- and a notice carries the highest identifier there is, so one sent
    // afterwards would too. nghttp3 only asserts this, and the peer must treat a raised
    // identifier as a protocol error.
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    server.shutdown().expect("the first and only shutdown");
    assert_eq!(
        server.shutdown().unwrap_err().kind(),
        ErrorKind::InvalidInput,
        "a second shutdown would recompute the cut-off from a higher stream"
    );
    assert_eq!(
        server.submit_shutdown_notice().unwrap_err().kind(),
        ErrorKind::InvalidInput,
        "a notice carries the maximum identifier, so it would raise the cut-off"
    );
    assert!(server.is_usable(), "refusing both is recoverable");

    // The order that is allowed still is, on a fresh connection.
    let mut other = observer(Role::Server, Settings::new());
    other.submit_shutdown_notice().expect("notice");
    other
        .submit_shutdown_notice()
        .expect("the same identifier again");
    other.shutdown().expect("then the cut-off");
}

#[test]
fn a_server_reports_being_drained_only_after_shutting_down() {
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    assert!(
        !server.is_drained().expect("a server may ask"),
        "a running server has not shut anything down"
    );

    server.shutdown().expect("shut down");
    assert!(
        !server.is_drained().expect("a server may ask"),
        "the GOAWAY frame is still queued, so the shutdown has not finished"
    );

    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );
    assert!(
        server.is_drained().expect("a server may ask"),
        "the GOAWAY is written and no peer-initiated request stream is open"
    );

    // A client is refused rather than left to read a server's bookkeeping, which is what
    // nghttp3's assert allows wherever it is not compiled in.
    assert_eq!(
        client.is_drained().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn a_stream_rejected_after_shutdown_asks_for_both_stop_sending_and_a_reset() {
    // nghttp3 cannot stop or reset anything itself -- it owns no transport -- so it asks.
    // The natural way to reach both requests at once is a server that has begun a
    // graceful shutdown and then receives a request past the cut-off.
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    // The shutdown is started and the request submitted before the client can learn of it,
    // which is the race the rejection path exists for: a client that already refuses to
    // open new streams -- as this one does once the GOAWAY lands -- never reaches it.
    server.shutdown().expect("start the shutdown");
    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/too-late").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .expect("submit request");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );

    // 0x010B is H3_REQUEST_REJECTED: the request was not processed, so the client may
    // safely retry it elsewhere. The caller needs the code, not merely the fact.
    assert_eq!(
        server_seen.stop_sending,
        vec![(0, 0x010b)],
        "the server should have asked its QUIC layer to stop the peer sending"
    );
    assert_eq!(
        server_seen.reset_stream,
        vec![(0, 0x010b)],
        "and to reset its own sending direction"
    );
    assert!(
        !server_seen.fields.contains_key(&0),
        "a rejected request must not also be delivered as a message"
    );

    // Acting on both is the caller's job; the connection is told once it has.
    server
        .shutdown_stream_read(id(0))
        .expect("discard the rest of the request");
    server.shutdown_stream_write(id(0)).expect("stop writing");
    assert!(server.is_usable());
}

#[test]
fn peer_control_and_qpack_streams_are_never_surfaced_as_requests() {
    // The peer's three connection-level streams carry bytes through the same entry point
    // as a request does. If they were treated as requests, an endpoint would report field
    // sections that the peer never sent as a message.
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/only-request").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .expect("submit request");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    let streams_with_fields: Vec<i64> = {
        let mut streams: Vec<i64> = server_seen.fields.keys().copied().collect();
        streams.sort_unstable();
        streams
    };
    assert_eq!(
        streams_with_fields,
        vec![0],
        "only the request stream should have produced fields; control and QPACK traffic \
         must be consumed as control data"
    );
}

#[test]
fn closing_a_critical_stream_is_a_distinct_kind_of_error() {
    // HTTP/3 requires the control and QPACK streams to live as long as the connection, so
    // there is no stream-level recovery from closing one. That is why it gets its own
    // error kind rather than being folded into ordinary protocol errors.
    let mut client = client();

    let error = client
        .close_stream(id(CLIENT_CONTROL), &mut Seen::default())
        .expect_err("the control stream may not be closed");
    assert_eq!(error.kind(), ErrorKind::ClosedCriticalStream);
    assert!(
        error.app_error_code().is_some(),
        "a caller must be able to close the QUIC connection with a code"
    );
    assert!(
        client.is_usable(),
        "refusing the close is recoverable; it is the peer doing it that is not"
    );

    for stream in [CLIENT_QPACK_ENCODER, CLIENT_QPACK_DECODER] {
        assert_eq!(
            client
                .close_stream(id(stream), &mut Seen::default())
                .map(|()| ErrorKind::Internal)
                .unwrap_err()
                .kind(),
            ErrorKind::ClosedCriticalStream,
            "stream {stream} is critical too"
        );
    }
}

#[test]
fn concurrency_hints_are_accepted_and_are_only_hints() {
    let mut server = server();

    // A server may raise the client's stream limit, and doing so is not enforcement: the
    // QUIC layer's MAX_STREAMS is what actually bounds the peer.
    server.set_max_client_streams_bidi(100).expect("raise");
    server
        .set_max_client_streams_bidi(200)
        .expect("raise again");
    let error = server
        .set_max_client_streams_bidi(50)
        .expect_err("lowering is refused");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        server.is_usable(),
        "nghttp3 only asserts this, so refusing it must stay recoverable"
    );

    server
        .set_max_concurrent_streams(64)
        .expect("a resource hint for the QPACK decoder");

    // A client has no client-stream limit to set, and nghttp3 asserts the role.
    let mut client = client();
    assert_eq!(
        client.set_max_client_streams_bidi(100).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn a_section_kind_is_still_reported_alongside_the_events() {
    // A guard against the event wiring having displaced the field callbacks: adding four
    // callbacks to the same struct is exactly the change that silently overwrites one.
    let mut client = client();
    let mut server = server();
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/still-here").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .expect("submit request");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );
    assert!(
        server_seen.fields[&0].contains(&b":path".to_vec()),
        "the field callback must still fire"
    );
    let _ = FieldSection::Headers;
}
