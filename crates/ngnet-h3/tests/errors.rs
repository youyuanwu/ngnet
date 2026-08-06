//! Every enumerated edge case produces a typed outcome, not a panic, hang or silent loss.
//!
//! The spec lists eleven of them. Some are proven elsewhere because they belong to the
//! machinery that was built with them — the retain contract's cases live in `body.rs` —
//! and each of those is named here so the coverage can be seen in one place rather than
//! inferred.
//!
//! The recurring hazard behind most of these is that nghttp3 validates a great deal with
//! C `assert`, which is not an error report: it aborts where it is compiled in and checks
//! nothing where it is not. Either way it is no use to a caller, so this crate checks the
//! precondition first and the tests below prove it does.

use std::collections::HashMap;

use ngnet_h3::{
    BodyOutcome, BodySource, Conn, ConnBuilder, ErrorCode, ErrorKind, FieldAction, FixedBody,
    Header, RetainedBytes, Role, StreamId, Timestamp,
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

/// One field, copied out of the slices it was borrowed as.
type Field = (Vec<u8>, Vec<u8>);

#[derive(Default, Debug)]
struct Seen {
    fields: HashMap<i64, Vec<Field>>,
    body: HashMap<i64, Vec<u8>>,
    ended: Vec<i64>,
}

fn observer(role: Role) -> Conn<Seen> {
    let mut conn = ConnBuilder::<Seen>::new(role)
        .on_field(|seen: &mut Seen, stream, _section, _token, name, value| {
            seen.fields
                .entry(stream.get())
                .or_default()
                .push((name.to_vec(), value.to_vec()));
            FieldAction::Continue
        })
        .on_data(|seen: &mut Seen, stream, chunk| {
            seen.body
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_end_stream(|seen: &mut Seen, stream| seen.ended.push(stream.get()))
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

/// Moves everything one side wants to send into the other, until neither has more.
fn pump(a: &mut Conn<Seen>, a_seen: &mut Seen, b: &mut Conn<Seen>, b_seen: &mut Seen, now: u64) {
    for _ in 0..512 {
        let moved = transfer(a, a_seen, b, b_seen, now) | transfer(b, b_seen, a, a_seen, now);
        if !moved {
            return;
        }
    }
    panic!("the two connections never stopped exchanging bytes");
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

fn request(path: &'static str) -> [Header<'static>; 4] {
    [
        Header::new(":method", "GET").unwrap(),
        Header::new(":scheme", "https").unwrap(),
        Header::new(":path", path).unwrap(),
        Header::new(":authority", "example.test").unwrap(),
    ]
}

// ---------------------------------------------------------------------------
// Edge case: header fields that are invalid for HTTP/3 are rejected before the wire.
// ---------------------------------------------------------------------------

#[test]
fn an_invalid_field_is_refused_at_construction_rather_than_written() {
    // Building the field is where this is caught, so a malformed one cannot even be put
    // into an argument list -- there is no path from here to the wire.
    for name in [
        &b""[..],
        &b"Content-Type"[..],
        &b"has space"[..],
        &b"has\nnewline"[..],
        &b"has\0nul"[..],
    ] {
        let error = Header::new(name, "value")
            .map(|_| ())
            .expect_err("a field name that HTTP/3 forbids");
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidInput,
            "name {name:?} should have been refused"
        );
    }

    for value in [
        &b"has\nnewline"[..],
        &b"has\rcarriage"[..],
        &b"has\0nul"[..],
    ] {
        assert_eq!(
            Header::new("x-name", value).map(|_| ()).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "value {value:?} should have been refused"
        );
    }

    // And the well-formed ones still work, so this is not refusing everything.
    Header::new(":method", "GET").expect("a valid field");
    Header::new("x-trailer", "").expect("an empty value is legal");
}

#[test]
fn a_malformed_message_from_the_peer_is_a_typed_protocol_error_with_a_code() {
    // A request with no pseudo-fields at all is malformed HTTP/3 messaging. The receiving
    // side must say so in a way its caller can act on: the kind separates it from a local
    // mistake, and the code is what the QUIC connection is closed with.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    client
        .submit_request(
            id(0),
            &[Header::new("x-only", "no-pseudo-fields").unwrap()],
            None,
        )
        .expect("nghttp3 does not validate outbound messages, so this is accepted here");

    let mut error = None;
    for _ in 0..64 {
        let Some(send) = client
            .writev_stream(&mut client_seen)
            .expect("collect data to send")
        else {
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).expect("commit");
        if taken == 0 && !fin {
            break;
        }
        match server.read_stream(
            stream,
            &bytes,
            fin,
            Timestamp::from_nanos(2),
            &mut server_seen,
        ) {
            Ok(_) => {}
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }

    let error = error.expect("the server should have rejected the malformed message");
    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert!(
        error.app_error_code().is_some(),
        "a protocol error must carry the code to close the QUIC connection with"
    );
    assert!(
        !server.is_usable(),
        "a read-path failure makes further calls undefined behaviour, so the connection \
         latches it"
    );
    // Dropping a poisoned connection is always allowed and always cleans up.
    drop(server);
}

// ---------------------------------------------------------------------------
// Edge case: a response arrives for a stream the caller has already abandoned.
// ---------------------------------------------------------------------------

#[test]
fn abandoning_one_stream_leaves_its_neighbours_working() {
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    for stream in [0i64, 4, 8] {
        client
            .submit_request(id(stream), &request("/parallel"), None)
            .expect("submit request");
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    // The client gives up on the middle stream before any response arrives. A real caller
    // would reset it through QUIC at the same time, which is why nothing further is
    // delivered for it below: the transport has stopped carrying it.
    client
        .close_stream(id(4), ErrorCode::new(0x010c), &mut client_seen)
        .expect("abandon the stream");
    assert!(
        client.is_usable(),
        "abandoning one stream is an ordinary operation"
    );

    // The server, which has not heard about it yet, answers all three.
    for stream in [0i64, 4, 8] {
        server
            .submit_response(
                id(stream),
                &[
                    Header::new(":status", "200").unwrap(),
                    Header::new("x-stream", if stream == 4 { "gone" } else { "here" }).unwrap(),
                ],
                None,
            )
            .expect("submit response");
    }

    // Deliver everything except what belongs to the abandoned stream.
    for _ in 0..256 {
        let Some(send) = server
            .writev_stream(&mut server_seen)
            .expect("collect data to send")
        else {
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).expect("commit");
        if taken == 0 && !fin {
            break;
        }
        if stream == id(4) {
            continue;
        }
        client
            .read_stream(
                stream,
                &bytes,
                fin,
                Timestamp::from_nanos(2),
                &mut client_seen,
            )
            .expect("read stream data");
    }

    assert!(client.is_usable(), "one abandoned stream is not fatal");
    for stream in [0i64, 8] {
        let fields = client_seen
            .fields
            .get(&stream)
            .unwrap_or_else(|| panic!("stream {stream} lost its response"));
        assert!(
            fields.iter().any(|(n, v)| n == b":status" && v == b"200"),
            "stream {stream} should be unaffected by the abandoned one"
        );
    }
    assert!(
        !client_seen.fields.contains_key(&4),
        "nothing was delivered for the abandoned stream, so nothing may appear for it"
    );
}

#[test]
fn a_response_delivered_for_an_abandoned_stream_is_reported_not_absorbed() {
    // The companion to the test above, and the harder half of the spec's edge case. A
    // caller that abandons a stream and then keeps feeding its bytes in is asking about a
    // stream this endpoint has told nghttp3 is gone. That is reported as a typed protocol
    // error carrying a code, rather than being silently absorbed or attributed elsewhere.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    for stream in [0i64, 4] {
        client
            .submit_request(id(stream), &request("/parallel"), None)
            .expect("submit request");
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    client
        .close_stream(id(4), ErrorCode::new(0x010c), &mut client_seen)
        .expect("abandon the stream");

    server
        .submit_response(id(4), &[Header::new(":status", "200").unwrap()], None)
        .expect("the server has not heard about it");

    let mut outcome = None;
    for _ in 0..256 {
        let Some(send) = server
            .writev_stream(&mut server_seen)
            .expect("collect data to send")
        else {
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).expect("commit");
        if taken == 0 && !fin {
            break;
        }
        if let Err(error) = client.read_stream(
            stream,
            &bytes,
            fin,
            Timestamp::from_nanos(2),
            &mut client_seen,
        ) {
            outcome = Some(error);
            break;
        }
    }

    let error = outcome.expect("delivering bytes for a stream this endpoint closed is refused");
    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert!(
        error.app_error_code().is_some(),
        "the caller needs a code to close the QUIC connection with"
    );
    assert!(
        !client.is_usable(),
        "this arrives through the read path, where nghttp3 documents that continuing is \
         undefined behaviour -- so it is latched rather than left to the caller to ignore"
    );
    // Which is why a caller must stop delivering a stream's bytes at the moment it
    // abandons it, exactly as a QUIC layer that has reset the stream would.
    drop(client);
}

// ---------------------------------------------------------------------------
// Edge case: a zero-length body with an end-of-stream marker.
// (That an offer must be committed even when it carries nothing is proven in
// `handshake.rs`, which drives the send transaction directly.)
// ---------------------------------------------------------------------------

#[test]
fn an_empty_body_is_an_end_of_stream_signal_not_a_zero_length_chunk() {
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    client
        .submit_request(
            id(0),
            &request("/empty"),
            Some(Box::new(FixedBody::new(Vec::new()))),
        )
        .expect("submit request");

    // Driven by hand rather than through `pump`, so every offer is visible. Each is
    // committed, including any carrying no bytes: that a skipped commit stalls the
    // connection is proven separately, in `handshake.rs`.
    let mut writes: Vec<(usize, bool)> = Vec::new();
    for _ in 0..64 {
        let Some(send) = client
            .writev_stream(&mut client_seen)
            .expect("collect data to send")
        else {
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        writes.push((taken, fin));
        send.commit(taken)
            .expect("commit, even with nothing on offer");
        if taken == 0 && !fin {
            break;
        }
        server
            .read_stream(
                stream,
                &bytes,
                fin,
                Timestamp::from_nanos(1),
                &mut server_seen,
            )
            .expect("read");
    }

    assert!(
        !writes.is_empty(),
        "nothing was offered at all, so this measured nothing"
    );
    assert_eq!(
        writes.iter().filter(|(_, fin)| *fin).count(),
        1,
        "the stream must end exactly once. nghttp3 attaches the end to the last write it \
         already had bytes for rather than emitting a separate empty one, which is why \
         this counts ends rather than asserting an empty final write"
    );
    assert_eq!(
        server_seen.ended,
        vec![0],
        "the end of the stream must be reported"
    );
    assert!(
        !server_seen.body.contains_key(&0),
        "an empty body is an end-of-stream signal, not a zero-length chunk"
    );
}

// ---------------------------------------------------------------------------
// Edge case: a body source fails midway through sending.
// ---------------------------------------------------------------------------

/// A body that produces one chunk and then gives up.
struct FailsAfterOneChunk {
    sent: bool,
}

impl BodySource for FailsAfterOneChunk {
    fn next(&mut self) -> BodyOutcome {
        if self.sent {
            return BodyOutcome::Fail;
        }
        self.sent = true;
        BodyOutcome::Wrote(vec![RetainedBytes::from(&b"the first chunk"[..])])
    }
}

#[test]
fn a_body_source_that_gives_up_fails_the_connection_and_releases_its_buffers() {
    // nghttp3 offers exactly one failure code for a data callback and it is
    // connection-fatal; there is no per-stream variant, and the header carries a TODO
    // saying so. Surfacing that honestly is better than pretending the stream alone died.
    let mut client = observer(Role::Client);
    let mut client_seen = Seen::default();

    client
        .submit_request(
            id(0),
            &request("/gives-up"),
            Some(Box::new(FailsAfterOneChunk { sent: false })),
        )
        .expect("submit request");

    let mut failure = None;
    for _ in 0..64 {
        match client.writev_stream(&mut client_seen) {
            Ok(Some(send)) => {
                let taken = send.len();
                send.commit(taken).expect("commit");
            }
            Ok(None) => break,
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }

    let failure = failure.expect("the write path should have failed");
    assert!(
        failure.is_fatal(),
        "a callback failure is fatal wherever it appears"
    );
    assert!(!client.is_usable());
    assert_eq!(
        client.retained_body_buffers(),
        0,
        "buffers waiting for an acknowledgement that can never arrive must be released"
    );
    // Every further call refuses without re-entering nghttp3, which would be undefined --
    // including the ones that only ask a question, because nghttp3 draws no distinction.
    assert_eq!(
        client
            .submit_request(id(4), &request("/after"), None)
            .unwrap_err()
            .kind(),
        ErrorKind::ConnectionUnusable
    );
    assert_eq!(
        client.is_stream_writable(id(0)).unwrap_err().kind(),
        ErrorKind::ConnectionUnusable
    );
    assert_eq!(
        client.is_drained().unwrap_err().kind(),
        ErrorKind::ConnectionUnusable,
        "a query is still a call into a connection that may not be called into"
    );
    drop(client);
}

// ---------------------------------------------------------------------------
// Edge case: bytes for a stream arrive before that stream is known.
// ---------------------------------------------------------------------------

#[test]
fn bytes_arriving_before_a_stream_is_understood_are_not_lost() {
    // A peer's unidirectional stream announces what it is with a varint prefix, which can
    // be split across deliveries. Until it has arrived the stream's purpose is unknown, so
    // this is the case where bytes must be held rather than dropped.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    // Collect everything the server wants to say, then feed it to the client one byte at
    // a time -- the most adversarial split there is.
    let mut traffic: Vec<(StreamId, Vec<u8>, bool)> = Vec::new();
    for _ in 0..64 {
        let Some(send) = server
            .writev_stream(&mut server_seen)
            .expect("collect data to send")
        else {
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).expect("commit");
        if taken == 0 && !fin {
            break;
        }
        traffic.push((stream, bytes, fin));
    }
    assert!(!traffic.is_empty(), "the server had nothing to send");

    let mut credit = 0u64;
    for (stream, bytes, fin) in &traffic {
        for (n, byte) in bytes.iter().enumerate() {
            let last = n + 1 == bytes.len();
            credit += client
                .read_stream(
                    *stream,
                    &[*byte],
                    *fin && last,
                    Timestamp::from_nanos(1),
                    &mut client_seen,
                )
                .expect("one byte at a time is still valid input")
                .bytes();
        }
    }

    let total: usize = traffic.iter().map(|(_, bytes, _)| bytes.len()).sum();
    assert_eq!(
        credit, total as u64,
        "every byte must be accounted for, however it was split"
    );
    assert!(client.is_usable());

    // And a request still completes afterwards, so nothing was left half-parsed.
    client
        .submit_request(id(0), &request("/after-the-split"), None)
        .expect("submit request");
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );
    assert!(server_seen.fields.contains_key(&0));
}

// ---------------------------------------------------------------------------
// Edge case: a caller declares the same stream twice, or one stream for two roles.
// ---------------------------------------------------------------------------

#[test]
fn conflicting_connection_level_declarations_are_refused_recoverably() {
    let mut conn = ConnBuilder::<Seen>::new(Role::Client)
        .build()
        .expect("connection");
    conn.bind_control_stream(id(CLIENT_CONTROL)).unwrap();

    // The control stream reused for QPACK.
    let error = conn
        .bind_qpack_streams(id(CLIENT_CONTROL), id(CLIENT_QPACK_DECODER))
        .expect_err("a stream cannot hold two roles");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // The QPACK pair sharing one stream. nghttp3 assigns the encoder before it builds the
    // decoder, so without this check the connection would be left half-bound.
    let error = conn
        .bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_ENCODER))
        .expect_err("the encoder and decoder cannot share a stream");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // Nothing above was accepted, so the real binding still works.
    conn.bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_DECODER))
        .expect("the QPACK streams are still unbound");
    assert!(conn.is_bound());

    // And a second attempt at either role is refused without poisoning.
    assert_eq!(
        conn.bind_control_stream(id(14)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        conn.bind_qpack_streams(id(18), id(22)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(conn.is_usable());
}

// ---------------------------------------------------------------------------
// Edge case: a peer opens more streams than the caller wishes to serve.
// ---------------------------------------------------------------------------

#[test]
fn stream_concurrency_is_a_hint_and_never_silently_drops_a_request() {
    // Enforcement belongs to the QUIC layer's MAX_STREAMS. What this crate offers is a
    // hint, and the important property is that setting it low does not make requests
    // vanish -- a caller that wanted them refused has to refuse them itself.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    server.set_max_concurrent_streams(1).expect("a hint");
    server.set_max_client_streams_bidi(2).expect("a hint");

    for stream in [0i64, 4, 8] {
        client
            .submit_request(id(stream), &request("/over-limit"), None)
            .expect("submit request");
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    for stream in [0i64, 4, 8] {
        assert!(
            server_seen.fields.contains_key(&stream),
            "stream {stream} was dropped; the hint must not silently discard requests"
        );
    }
    assert!(server.is_usable());
}

// ---------------------------------------------------------------------------
// The remaining enumerated cases, and where they are proven.
// ---------------------------------------------------------------------------

/// The retain contract's own edge cases live with the machinery they belong to.
///
/// This is a signpost rather than a test: `tests/body.rs` proves that dropping a
/// connection releases retained buffers, that a body acknowledged in partial ranges is
/// released only after its last byte, that over-reporting acknowledgement is refused, and
/// that a source yielding nothing without an end becomes a deferral rather than a
/// zero-length message.
#[test]
fn the_retain_contract_edge_cases_are_covered_in_the_body_tests() {
    // A compile-time reminder that the types those tests exercise are the public ones, so
    // this note cannot rot silently if they are renamed.
    let _: fn(Vec<u8>) -> FixedBody = FixedBody::new;
    let _: RetainedBytes = RetainedBytes::from(&b"still public"[..]);
}
