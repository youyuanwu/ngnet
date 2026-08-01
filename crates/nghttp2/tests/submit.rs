//! Message submission: header validation, requests, responses, trailers, and the
//! stream/connection control frames (Spec SC-012, SC-017, US-1, US-2).

use nghttp2::{
    ErrorCode, ErrorKind, FrameInfo, FrameType, Header, HeaderAction, Session, SessionBuilder,
    StreamId,
};

const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn drain<C>(session: &mut Session<C>, context: &mut C) -> Vec<u8> {
    let mut wire = Vec::new();
    while let Some(block) = session.send(context).expect("send failed") {
        wire.extend_from_slice(block);
    }
    wire
}

fn parse_frames(mut wire: &[u8]) -> Vec<(u8, u8, u32)> {
    if wire.starts_with(CLIENT_MAGIC) {
        wire = &wire[CLIENT_MAGIC.len()..];
    }
    let mut frames = Vec::new();
    while wire.len() >= 9 {
        let len = u32::from_be_bytes([0, wire[0], wire[1], wire[2]]) as usize;
        frames.push((
            wire[3],
            wire[4],
            u32::from_be_bytes([wire[5], wire[6], wire[7], wire[8]]) & 0x7fff_ffff,
        ));
        if wire.len() < 9 + len {
            break;
        }
        wire = &wire[9 + len..];
    }
    frames
}

/// What a peer observed during an exchange.
#[derive(Debug, Default)]
struct Seen {
    headers: Vec<(String, String)>,
    streams_begun: Vec<i32>,
    closed: Vec<(i32, u32)>,
}

fn recording() -> SessionBuilder<Seen> {
    SessionBuilder::<Seen>::server()
        .on_begin_headers(|seen: &mut Seen, info: FrameInfo| {
            seen.streams_begun.push(info.stream_id().get());
            HeaderAction::Continue
        })
        .on_header(
            |seen: &mut Seen, _info: FrameInfo, name: &[u8], value: &[u8]| {
                seen.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .on_stream_close(|seen: &mut Seen, stream: StreamId, code: ErrorCode, _body_error| {
            seen.closed.push((stream.get(), code.get()));
        })
}

fn request_headers() -> Vec<Header<'static>> {
    vec![
        Header::new(":method", "GET"),
        Header::new(":scheme", "http"),
        Header::new(":authority", "example.test"),
        Header::new(":path", "/resource"),
    ]
}

#[test]
fn a_request_is_assigned_a_stream_and_reaches_the_server() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    let stream = client
        .submit_request(&request_headers())
        .expect("submitting a request should succeed");
    assert_eq!(stream.get(), 1, "the first client stream is 1");

    let wire = drain(&mut client, &mut ());
    let frames = parse_frames(&wire);
    assert!(
        frames
            .iter()
            .any(|(kind, _, s)| *kind == FrameType::HEADERS.get() && *s == 1),
        "the request HEADERS frame should be on the wire, saw {frames:?}"
    );

    let consumed = server.recv(&wire, &mut seen).expect("server recv failed");
    assert_eq!(consumed, wire.len());

    assert_eq!(seen.streams_begun, vec![1]);
    assert_eq!(
        seen.headers,
        vec![
            (":method".into(), "GET".into()),
            (":scheme".into(), "http".into()),
            (":authority".into(), "example.test".into()),
            (":path".into(), "/resource".into()),
        ],
        "headers should arrive in order, intact"
    );
}

#[test]
fn a_response_reaches_the_client() {
    let mut client = SessionBuilder::<Seen>::client()
        .on_header(
            |seen: &mut Seen, _info: FrameInfo, name: &[u8], value: &[u8]| {
                seen.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .build()
        .unwrap();
    let mut server = recording().build().unwrap();

    let mut client_seen = Seen::default();
    let mut server_seen = Seen::default();

    client.submit_request(&request_headers()).unwrap();
    let to_server = drain(&mut client, &mut client_seen);
    server.recv(&to_server, &mut server_seen).unwrap();

    server
        .submit_response(
            StreamId::new(1),
            &[Header::new(":status", "200"), Header::new("x-note", "hello")],
        )
        .expect("submitting a response should succeed");

    let to_client = drain(&mut server, &mut server_seen);
    client.recv(&to_client, &mut client_seen).unwrap();

    assert!(
        client_seen
            .headers
            .contains(&(":status".into(), "200".into())),
        "the client should have observed the status, saw {:?}",
        client_seen.headers
    );
    assert!(
        client_seen
            .headers
            .contains(&("x-note".into(), "hello".into()))
    );
}

#[test]
fn a_second_response_for_one_stream_is_rejected() {
    // libnghttp2 documents this as a programming error that may crash, so the wrapper has
    // to catch it rather than forward it.
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    client.submit_request(&request_headers()).unwrap();
    let wire = drain(&mut client, &mut ());
    server.recv(&wire, &mut seen).unwrap();

    let status = [Header::new(":status", "200")];
    server.submit_response(StreamId::new(1), &status).unwrap();

    let error = server
        .submit_response(StreamId::new(1), &status)
        .expect_err("a second response must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("already been submitted"));
}

#[test]
fn a_response_for_an_unopened_stream_is_accepted_and_produces_nothing() {
    // Verified against the library: only stream zero is rejected. A well-formed but
    // unopened identifier returns success and simply drops the frame.
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    let before = drain(&mut server, &mut seen);
    server
        .submit_response(StreamId::new(99), &[Header::new(":status", "200")])
        .expect("libnghttp2 accepts a well-formed but unopened stream identifier");

    let after = drain(&mut server, &mut seen);
    let frames = parse_frames(&after);
    assert!(
        !frames
            .iter()
            .any(|(kind, _, s)| *kind == FrameType::HEADERS.get() && *s == 99),
        "no HEADERS frame should have been produced, saw {frames:?} (earlier: {before:?})"
    );
}

#[test]
fn a_response_for_stream_zero_is_rejected() {
    let mut server = recording().build().unwrap();

    let error = server
        .submit_response(StreamId::CONNECTION, &[Header::new(":status", "200")])
        .expect_err("stream zero is not a message stream");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn invalid_header_sets_are_rejected_and_the_session_survives() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();

    let cases: Vec<(&str, Vec<Header<'_>>)> = vec![
        ("empty set", vec![]),
        (
            "empty name",
            vec![Header::new(":method", "GET"), Header::from_bytes(b"", b"x")],
        ),
        (
            "uppercase name",
            vec![Header::new(":method", "GET"), Header::new("X-Bad", "x")],
        ),
        (
            "space in name",
            vec![Header::new(":method", "GET"), Header::new("bad name", "x")],
        ),
        (
            "newline in value",
            vec![Header::new(":method", "GET"), Header::new("x-bad", "a\r\nb")],
        ),
        (
            "NUL in value",
            vec![
                Header::new(":method", "GET"),
                Header::from_bytes(b"x-bad", b"a\0b"),
            ],
        ),
        (
            "SOH control character in value",
            vec![
                Header::new(":method", "GET"),
                Header::from_bytes(b"x-bad", b"a\x01b"),
            ],
        ),
        (
            "DEL in value",
            vec![
                Header::new(":method", "GET"),
                Header::from_bytes(b"x-bad", b"a\x7fb"),
            ],
        ),
        (
            "vertical tab in value",
            vec![
                Header::new(":method", "GET"),
                Header::from_bytes(b"x-bad", b"a\x0bb"),
            ],
        ),
        (
            "colon inside a name",
            vec![Header::new(":method", "GET"), Header::new("x-a:b", "1")],
        ),
        (
            "leading whitespace in value",
            vec![Header::new(":method", "GET"), Header::new("x-bad", " oops")],
        ),
        (
            "pseudo-header after a regular field",
            vec![Header::new("x-first", "1"), Header::new(":method", "GET")],
        ),
        ("bare colon", vec![Header::new(":", "GET")]),
    ];

    for (label, headers) in cases {
        let error = client
            .submit_request(&headers)
            .unwrap_err_or_else(label);

        assert_eq!(
            error.kind(),
            ErrorKind::InvalidInput,
            "{label}: should be reported as caller error"
        );
    }

    // Rejection must leave the session usable, not poisoned.
    let stream = client
        .submit_request(&request_headers())
        .expect("the session must still accept a valid request");
    assert_eq!(stream.get(), 1);
}

/// Small helper so a failure names which case failed.
trait UnwrapErrOrElse {
    fn unwrap_err_or_else(self, label: &str) -> nghttp2::Error;
}

impl UnwrapErrOrElse for Result<StreamId, nghttp2::Error> {
    fn unwrap_err_or_else(self, label: &str) -> nghttp2::Error {
        match self {
            Ok(stream) => panic!("{label}: expected rejection, but got stream {stream}"),
            Err(error) => error,
        }
    }
}

#[test]
fn a_valid_request_still_works_after_rejections() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();

    assert!(client.submit_request(&[]).is_err());
    assert!(
        client
            .submit_request(&[Header::new("X-Bad", "1")])
            .is_err()
    );

    let stream = client
        .submit_request(&request_headers())
        .expect("the session must remain usable after rejected header sets");
    assert_eq!(stream.get(), 1, "no stream should have been consumed by the rejections");
}

#[test]
fn trailers_may_not_carry_pseudo_headers() {
    let mut server = recording().build().unwrap();

    let error = server
        .submit_trailer(StreamId::new(1), &[Header::new(":status", "200")])
        .expect_err("a pseudo-header in a trailer block must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("pseudo-header"));
}

#[test]
fn resetting_a_stream_is_observed_by_the_peer_with_the_chosen_code() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    let stream = client.submit_request(&request_headers()).unwrap();
    let wire = drain(&mut client, &mut ());
    server.recv(&wire, &mut seen).unwrap();

    client
        .reset_stream(stream, ErrorCode::REFUSED_STREAM)
        .expect("resetting a stream should succeed");

    let wire = drain(&mut client, &mut ());
    let frames = parse_frames(&wire);
    assert!(
        frames
            .iter()
            .any(|(kind, _, s)| *kind == FrameType::RST_STREAM.get() && *s == 1),
        "the peer should observe a RST_STREAM, saw {frames:?}"
    );

    server.recv(&wire, &mut seen).unwrap();
    assert!(
        seen.closed
            .iter()
            .any(|(s, code)| *s == 1 && *code == ErrorCode::REFUSED_STREAM.get()),
        "the server should have been told why the stream closed, saw {:?}",
        seen.closed
    );
}

#[test]
fn shutting_down_emits_a_goaway_naming_the_last_stream() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    client.submit_request(&request_headers()).unwrap();
    let wire = drain(&mut client, &mut ());
    server.recv(&wire, &mut seen).unwrap();

    server
        .shutdown(StreamId::new(1), ErrorCode::NO_ERROR)
        .expect("graceful shutdown should succeed");

    let wire = drain(&mut server, &mut seen);
    let frames = parse_frames(&wire);
    assert!(
        frames
            .iter()
            .any(|(kind, _, _)| *kind == FrameType::GOAWAY.get()),
        "a GOAWAY should have been queued, saw {frames:?}"
    );

    // The GOAWAY payload begins with the last stream identifier.
    let goaway_start = wire
        .windows(9)
        .position(|w| w[3] == FrameType::GOAWAY.get())
        .expect("GOAWAY frame not located");
    let payload = &wire[goaway_start + 9..];
    let last_stream = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
        & 0x7fff_ffff;
    assert_eq!(last_stream, 1, "the GOAWAY should name the last processed stream");
}

#[test]
fn a_sensitive_header_is_still_delivered() {
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    let mut headers = request_headers();
    headers.push(Header::new("authorization", "secret-token").sensitive());

    client.submit_request(&headers).unwrap();
    let wire = drain(&mut client, &mut ());
    server.recv(&wire, &mut seen).unwrap();

    assert!(
        seen.headers
            .contains(&("authorization".into(), "secret-token".into())),
        "marking a header sensitive must not stop it being sent, saw {:?}",
        seen.headers
    );
}

#[test]
fn a_value_may_contain_tabs_spaces_and_high_bytes() {
    // The tightened control-character rule must not reject legitimate values: HTAB, SP
    // and obs-text are all permitted by RFC 9110.
    let mut client = SessionBuilder::<()>::client().build().unwrap();

    let mut headers = request_headers();
    headers.push(Header::new("x-inner", "a\tb c"));
    headers.push(Header::from_bytes(b"x-obs", b"caf\xc3\xa9"));

    client
        .submit_request(&headers)
        .expect("tabs, spaces and high bytes are legal inside a value");
}

#[test]
fn responding_to_an_unopened_stream_does_not_poison_it() {
    // A response for a stream that is not open is dropped by libnghttp2 without ever
    // closing a stream. If the duplicate guard recorded that, the identifier would be
    // unusable once a genuine stream claimed it.
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = recording().build().unwrap();
    let mut seen = Seen::default();

    // Stream 3 does not exist yet.
    server
        .submit_response(StreamId::new(3), &[Header::new(":status", "204")])
        .expect("an unopened stream identifier is accepted");

    // Now open streams 1 and 3 for real.
    client.submit_request(&request_headers()).unwrap();
    let second = client.submit_request(&request_headers()).unwrap();
    assert_eq!(second.get(), 3);

    let wire = drain(&mut client, &mut ());
    server.recv(&wire, &mut seen).unwrap();

    server
        .submit_response(StreamId::new(3), &[Header::new(":status", "200")])
        .expect("the genuine stream 3 must still accept its response");

    // And the duplicate guard still works on it.
    let error = server
        .submit_response(StreamId::new(3), &[Header::new(":status", "200")])
        .expect_err("a genuine duplicate must still be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}
