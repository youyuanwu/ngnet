//! A header-only request and response, exchanged in memory with no QUIC present.
//!
//! Everything here runs both connections against each other directly. Where Phase 2's
//! tests proved the preface, these prove that a request submitted on one side arrives as
//! the same fields on the other, that submission preconditions are refused rather than
//! asserted, and that many exchanges can be in flight at once without their fields being
//! confused.
//!
//! Trailer *delivery* is not proven here: a message with no body source ends its stream at
//! the header section, so there is nothing for a trailer to follow. What is proven is that
//! submitting one in that state is refused, and refused recoverably. Delivery is proven in
//! `body.rs`, where a body keeps the stream open long enough for trailers to follow it.

use std::collections::HashMap;

use ngnet_h3::{
    Conn, ConnBuilder, ErrorCode, ErrorKind, FieldAction, FieldSection, FixedBody, Header, Role,
    StreamId, Timestamp,
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

/// A field observed by a handler, copied out of the borrowed slices.
#[derive(Debug, PartialEq, Eq, Clone)]
struct Field {
    stream: i64,
    section: FieldSection,
    name: Vec<u8>,
    value: Vec<u8>,
}

/// What one side observed.
#[derive(Default, Debug)]
struct Seen {
    fields: Vec<Field>,
    body: HashMap<i64, Vec<u8>>,
    ended: Vec<i64>,
    sections_begun: usize,
    sections_ended: usize,
}

impl Seen {
    fn named(&self, stream: i64, name: &[u8]) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|f| f.stream == stream && f.name == name)
            .map(|f| f.value.as_slice())
    }

    fn section_of(&self, stream: i64, name: &[u8]) -> Option<FieldSection> {
        self.fields
            .iter()
            .find(|f| f.stream == stream && f.name == name)
            .map(|f| f.section)
    }
}

fn observer(role: Role) -> Conn<Seen> {
    let mut conn = ConnBuilder::<Seen>::new(role)
        .on_section_begin(|seen: &mut Seen, _stream, _section| seen.sections_begun += 1)
        .on_field(|seen: &mut Seen, stream, section, _token, name, value| {
            seen.fields.push(Field {
                stream: stream.get(),
                section,
                name: name.to_vec(),
                value: value.to_vec(),
            });
            FieldAction::Continue
        })
        .on_section_end(|seen: &mut Seen, _stream, _section| seen.sections_ended += 1)
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
///
/// Returns the number of passes, so a test can tell a genuine settling from a bound being
/// hit.
fn pump(a: &mut Conn<Seen>, a_state: &mut Seen, b: &mut Conn<Seen>, b_state: &mut Seen, now: u64) {
    let mut settled = false;
    for _ in 0..256 {
        let moved = transfer(a, a_state, b, b_state, now) | transfer(b, b_state, a, a_state, now);
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

/// Drains one offer from `from` and delivers it to `to`. Returns whether anything moved.
fn transfer(
    from: &mut Conn<Seen>,
    from_state: &mut Seen,
    to: &mut Conn<Seen>,
    to_state: &mut Seen,
    now: u64,
) -> bool {
    let Some(send) = from
        .writev_stream(from_state)
        .expect("collect data to send")
    else {
        return false;
    };
    let stream = send.stream();
    let fin = send.fin();
    let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    let taken = bytes.len();
    send.commit(taken).expect("commit");

    // Reported straight away, because it is the only thing that releases retained body
    // buffers and a test that never reported it would hold every one of them.
    if taken > 0 {
        from.add_ack_offset(stream, taken as u64, from_state)
            .expect("report acknowledgement");
    }

    if taken > 0 || fin {
        to.read_stream(stream, &bytes, fin, Timestamp::from_nanos(now), to_state)
            .expect("read stream data");
    }
    taken > 0 || fin
}

#[test]
fn a_request_and_response_round_trip_in_memory() {
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    let request = id(0);
    client
        .submit_request(
            request,
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/hello").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
                Header::new("accept", "text/plain").unwrap(),
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

    assert_eq!(server_seen.named(0, b":method"), Some(&b"GET"[..]));
    assert_eq!(server_seen.named(0, b":path"), Some(&b"/hello"[..]));
    assert_eq!(server_seen.named(0, b"accept"), Some(&b"text/plain"[..]));
    assert_eq!(
        server_seen.section_of(0, b":method"),
        Some(FieldSection::Headers)
    );

    server
        .submit_response(
            request,
            &[
                Header::new(":status", "200").unwrap(),
                Header::new("content-type", "text/plain").unwrap(),
            ],
            None,
        )
        .expect("submit response");

    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );

    assert_eq!(client_seen.named(0, b":status"), Some(&b"200"[..]));
    assert_eq!(
        client_seen.named(0, b"content-type"),
        Some(&b"text/plain"[..])
    );
}

#[test]
fn trailers_after_a_finished_stream_are_refused_recoverably() {
    // Trailers follow a body, and a request submitted without a body source ends its
    // stream immediately -- so there is nothing for a trailer to trail. nghttp3 reports
    // that as INVALID_STATE, which is a caller mistake rather than a fatal condition.
    //
    // Delivery of trailers, and the assertion that a receiver tells them apart from
    // headers, therefore needs a body to exist first and belongs with the body phase. The
    // `FieldSection::Trailers` path is wired here but is exercised there.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    let request = id(0);
    client
        .submit_request(
            request,
            &[
                Header::new(":method", "POST").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/upload").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .unwrap();

    let error = client
        .submit_trailers(request, &[Header::new("x-checksum", "abc123").unwrap()])
        .expect_err("the stream is already finished");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!error.is_fatal(), "a caller mistake must not poison");
    assert!(client.is_usable());

    // And the exchange the client did submit still completes.
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );
    assert_eq!(server_seen.named(0, b":method"), Some(&b"POST"[..]));
    assert_eq!(
        server_seen.section_of(0, b":method"),
        Some(FieldSection::Headers)
    );
}

#[test]
fn a_request_with_no_body_ends_the_stream_without_a_body_chunk() {
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    let request = id(0);
    client
        .submit_request(
            request,
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .unwrap();
    // With no body source attached, submitting the request alone ends the stream.
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    assert!(
        server_seen.body.is_empty(),
        "a bodyless request must produce no body chunk, got {:?}",
        server_seen.body
    );
    assert!(
        server_seen.ended.contains(&0),
        "the stream should have been reported as ended"
    );
}

#[test]
fn ten_concurrent_exchanges_keep_their_fields_apart() {
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    // Client-initiated bidirectional streams: 0, 4, 8, ...
    let streams: Vec<i64> = (0..10).map(|n| n * 4).collect();

    for &stream in &streams {
        let path = format!("/resource/{stream}");
        client
            .submit_request(
                id(stream),
                &[
                    Header::new(":method", "GET").unwrap(),
                    Header::new(":scheme", "https").unwrap(),
                    Header::new(":path", &path).unwrap(),
                    Header::new(":authority", "example.test").unwrap(),
                ],
                None,
            )
            .unwrap_or_else(|e| panic!("submit on stream {stream}: {e}"));
    }

    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    for &stream in &streams {
        let expected = format!("/resource/{stream}");
        assert_eq!(
            server_seen.named(stream, b":path"),
            Some(expected.as_bytes()),
            "stream {stream} got the wrong path, so fields were crossed between streams"
        );
    }

    for &stream in &streams {
        server
            .submit_response(id(stream), &[Header::new(":status", "204").unwrap()], None)
            .unwrap();
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );

    for &stream in &streams {
        assert_eq!(client_seen.named(stream, b":status"), Some(&b"204"[..]));
    }
}

#[test]
fn a_handler_mutates_state_it_never_captured() {
    // The handler is registered once, before the state exists. The state is passed at call
    // time and stays owned by the caller throughout -- no interior mutability, no clone.
    let mut server = ConnBuilder::<Vec<String>>::new(Role::Server)
        .on_field(
            |seen: &mut Vec<String>, _stream, _section, _token, name, _value| {
                seen.push(String::from_utf8_lossy(name).into_owned());
                FieldAction::Continue
            },
        )
        .build()
        .unwrap();
    server.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    server
        .bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();

    let mut client = observer(Role::Client);
    let mut client_seen = Seen::default();
    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .unwrap();

    let mut names: Vec<String> = Vec::new();
    let mut drained = false;
    for _ in 0..64 {
        let Some(send) = client.writev_stream(&mut client_seen).unwrap() else {
            drained = true;
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).unwrap();
        if taken > 0 || fin {
            server
                .read_stream(stream, &bytes, fin, Timestamp::from_nanos(1), &mut names)
                .unwrap();
        } else {
            drained = true;
            break;
        }
    }
    // Stopping at the bound would leave the assertions below judging a truncated request.
    assert!(drained, "the client never stopped producing bytes");
    let _ = &mut client_seen;

    assert!(
        names.iter().any(|n| n == ":method"),
        "the caller's own vector should have been mutated, got {names:?}"
    );
}

#[test]
fn submission_preconditions_are_checked_rather_than_asserted() {
    // Every one of these is something nghttp3 checks only with `assert`, which aborts or
    // checks nothing depending on the build, and reports an error in neither.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);

    // Wrong role.
    let error = client
        .submit_response(id(0), &[Header::new(":status", "200").unwrap()], None)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let error = server
        .submit_request(id(0), &[Header::new(":method", "GET").unwrap()], None)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // Wrong stream shape: a unidirectional stream cannot carry a request.
    let error = client
        .submit_request(id(2), &[Header::new(":method", "GET").unwrap()], None)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // Neither refusal poisoned anything.
    assert!(client.is_usable() && server.is_usable());
}

#[test]
fn submitting_before_binding_is_a_typed_error_rather_than_an_abort() {
    // nghttp3 asserts the QPACK encoder is bound before it encodes a field section, so
    // without this check the connection aborts or corrupts its state instead.
    let mut client = ConnBuilder::<Seen>::new(Role::Client).build().unwrap();
    let error = client
        .submit_request(id(0), &[Header::new(":method", "GET").unwrap()], None)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(client.is_usable());
}

#[test]
fn stream_close_is_reported_with_both_directions() {
    #[derive(Default)]
    struct Closed {
        seen: Vec<(i64, Option<u64>, Option<u64>)>,
    }

    let mut server = ConnBuilder::<Closed>::new(Role::Server)
        .on_stream_close(|state: &mut Closed, stream, closed| {
            state.seen.push((
                stream.get(),
                closed.receiving.map(|c| c.get()),
                closed.sending.map(|c| c.get()),
            ));
        })
        .build()
        .unwrap();
    server.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    server
        .bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();

    let mut client = observer(Role::Client);
    let mut client_seen = Seen::default();
    let mut closed = Closed::default();

    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
            ],
            None,
        )
        .unwrap();

    let mut drained = false;
    for _ in 0..64 {
        let Some(send) = client.writev_stream(&mut client_seen).unwrap() else {
            drained = true;
            break;
        };
        let stream = send.stream();
        let fin = send.fin();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).unwrap();
        if taken == 0 && !fin {
            drained = true;
            break;
        }
        server
            .read_stream(stream, &bytes, fin, Timestamp::from_nanos(1), &mut closed)
            .unwrap();
    }
    assert!(drained, "the client never stopped producing bytes");
    let _ = &mut client_seen;

    // Closing fires the handler through the same bridge the read path uses -- and closing
    // is the only call in this crate that installs one around a callback that is not a
    // read, which is worth exercising rather than assuming.
    server
        .close_stream(id(0), ErrorCode::new(0x0100), &mut closed)
        .expect("close the request stream");

    let (stream, receiving, sending) = closed
        .seen
        .first()
        .copied()
        .expect("the close handler should have been called");
    assert_eq!(stream, 0);
    // Closing with an explicit code reports it on both directions, each with its flag set.
    // The options are still the right model: nghttp3 signals "this direction carried no
    // error" with a clear flag rather than with a zero code, so collapsing them would make
    // a clean close indistinguishable from one closed with H3_NO_ERROR.
    assert_eq!(receiving, Some(0x0100));
    assert_eq!(sending, Some(0x0100));

    assert!(server.is_usable());
    drop(server);
}

#[test]
fn ten_concurrent_exchanges_with_bodies_attribute_every_chunk() {
    // The header-only version above proves fields are not crossed between streams. Body
    // chunks arrive through a different callback with no framing of their own, so they
    // need their own proof.
    let mut client = observer(Role::Client);
    let mut server = observer(Role::Server);
    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    let streams: Vec<i64> = (0..10).map(|n| n * 4).collect();
    // Distinct lengths as well as distinct contents, so a body attributed to the wrong
    // stream cannot coincidentally match.
    let payload = |stream: i64| -> Vec<u8> { vec![stream as u8; 100 + stream as usize] };

    for &stream in &streams {
        let path = format!("/body/{stream}");
        client
            .submit_request(
                id(stream),
                &[
                    Header::new(":method", "POST").unwrap(),
                    Header::new(":scheme", "https").unwrap(),
                    Header::new(":path", &path).unwrap(),
                    Header::new(":authority", "example.test").unwrap(),
                ],
                Some(Box::new(FixedBody::new(payload(stream)))),
            )
            .unwrap_or_else(|e| panic!("submit on stream {stream}: {e}"));
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        1,
    );

    for &stream in &streams {
        let expected = format!("/body/{stream}");
        assert_eq!(
            server_seen.named(stream, b":path"),
            Some(expected.as_bytes()),
            "stream {stream} got the wrong path"
        );
        assert_eq!(
            server_seen.body.get(&stream).map(Vec::as_slice),
            Some(payload(stream).as_slice()),
            "stream {stream} got the wrong body, so chunks were crossed between streams"
        );
    }
    assert_eq!(
        client.retained_body_buffers(),
        0,
        "every buffer was acknowledged, so none should still be held"
    );

    for &stream in &streams {
        server
            .submit_response(
                id(stream),
                &[Header::new(":status", "200").unwrap()],
                Some(Box::new(FixedBody::new(payload(stream + 1)))),
            )
            .unwrap();
    }
    pump(
        &mut client,
        &mut client_seen,
        &mut server,
        &mut server_seen,
        2,
    );

    for &stream in &streams {
        assert_eq!(
            client_seen.body.get(&stream).map(Vec::as_slice),
            Some(payload(stream + 1).as_slice())
        );
    }
    assert_eq!(server.retained_body_buffers(), 0);
}

#[test]
fn a_second_body_on_one_stream_is_refused() {
    // A memory-safety guard rather than tidiness: replacing the entry would drop the
    // first body's retained buffers while nghttp3 still held pointers into them.
    let mut client = observer(Role::Client);
    let fields = [
        Header::new(":method", "POST").unwrap(),
        Header::new(":scheme", "https").unwrap(),
        Header::new(":path", "/upload").unwrap(),
        Header::new(":authority", "example.test").unwrap(),
    ];
    client
        .submit_request(
            id(0),
            &fields,
            Some(Box::new(FixedBody::new(b"payload".to_vec()))),
        )
        .expect("the first body");

    let error = client
        .submit_request(
            id(0),
            &fields,
            Some(Box::new(FixedBody::new(b"again".to_vec()))),
        )
        .expect_err("a stream carries at most one body");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(client.is_usable(), "a caller mistake is not fatal");
}

#[test]
fn trailers_are_refused_on_connection_level_streams() {
    // nghttp3 registers the control and QPACK streams in the same map it looks requests up
    // in, so its own "stream not found" net does not catch this: without a check here the
    // trailers would be scheduled onto a critical stream with the end-of-stream flag set.
    let mut client = observer(Role::Client);

    for stream in [CLIENT_CONTROL, CLIENT_QPACK_ENCODER, CLIENT_QPACK_DECODER] {
        let error = client
            .submit_trailers(id(stream), &[Header::new("x-trailer", "v").unwrap()])
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidInput,
            "stream {stream} should have been refused"
        );
    }
    assert!(client.is_usable());
}

#[test]
fn informational_responses_are_server_only() {
    let mut client = observer(Role::Client);
    let error = client
        .submit_info(id(0), &[Header::new(":status", "103").unwrap()])
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    let mut server = observer(Role::Server);
    let error = server
        .submit_info(id(2), &[Header::new(":status", "103").unwrap()])
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::InvalidInput,
        "a unidirectional stream cannot carry a response"
    );
}
