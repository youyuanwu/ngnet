//! Receive-path behaviour: handlers, the borrowed context, and the error model
//! (Spec SC-004 runtime half, SC-014, SC-016 runtime half, and FR-030).

use nghttp2::{
    ErrorKind, FrameInfo, FrameType, HeaderAction, Session, SessionBuilder, Setting, StreamId,
};

/// The 24-byte client connection preface.
const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn drain<C>(session: &mut Session<C>, context: &mut C) -> Vec<u8> {
    let mut wire = Vec::new();
    while let Some(block) = session.send(context).expect("send failed") {
        wire.extend_from_slice(block);
    }
    wire
}

/// Everything a test observed, accumulated through the borrowed context.
#[derive(Debug, Default, PartialEq, Eq)]
struct Observed {
    begun: Vec<i32>,
    headers: Vec<(String, String)>,
    frames: Vec<u8>,
    closed: Vec<(i32, u32)>,
    body: Vec<u8>,
}

/// Feeds a client's opening bytes to a server so the server has a live connection.
fn handshake(server: &mut Session<Observed>, observed: &mut Observed) {
    let mut client = SessionBuilder::<()>::client()
        .build()
        .expect("client build failed");
    let opening = drain(&mut client, &mut ());

    let consumed = server.recv(&opening, observed).expect("server recv failed");
    assert_eq!(
        consumed,
        opening.len(),
        "the server should consume the whole buffer it was given"
    );
}

#[test]
fn handlers_receive_the_caller_context_and_borrowed_data() {
    let mut server = SessionBuilder::<Observed>::server()
        .on_begin_headers(|observed: &mut Observed, info: FrameInfo| {
            observed.begun.push(info.stream_id().get());
            HeaderAction::Continue
        })
        .on_header(
            |observed: &mut Observed, _info: FrameInfo, name: &[u8], value: &[u8]| {
                observed.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .on_frame(|observed: &mut Observed, info: FrameInfo| {
            observed.frames.push(info.kind().get());
        })
        .on_stream_close(|observed: &mut Observed, stream: StreamId, code, _body_error| {
            observed.closed.push((stream.get(), code.get()));
        })
        .build()
        .expect("server build failed");

    let mut observed = Observed::default();
    handshake(&mut server, &mut observed);

    assert!(
        observed.frames.contains(&FrameType::SETTINGS.get()),
        "the client's SETTINGS frame should have been reported, saw {:?}",
        observed.frames
    );
}

#[test]
fn an_unregistered_event_is_silently_ignored() {
    // No handlers at all: processing must still succeed and consume everything.
    let mut server = SessionBuilder::<Observed>::server()
        .build()
        .expect("server build failed");

    let mut observed = Observed::default();
    handshake(&mut server, &mut observed);

    assert_eq!(
        observed,
        Observed::default(),
        "nothing should have been recorded without handlers"
    );
    assert!(server.want_read(), "the session should still be usable");
}

#[test]
fn an_empty_input_is_a_no_op() {
    let mut server = SessionBuilder::<Observed>::server().build().unwrap();
    let mut observed = Observed::default();

    assert_eq!(server.recv(&[], &mut observed).unwrap(), 0);
}

#[test]
fn a_bad_client_preface_is_a_typed_error() {
    // One of the few conditions libnghttp2 reports as fatal rather than handling itself.
    let mut server = SessionBuilder::<Observed>::server().build().unwrap();
    let mut observed = Observed::default();

    let error = server
        .recv(b"NOT-THE-HTTP2-PREFACE!!!\x00\x00\x00\x04\x00\x00\x00\x00\x00", &mut observed)
        .expect_err("a wrong client preface must be reported");

    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert!(
        error.to_string().contains("mem_recv2"),
        "the message should name the failing operation, got: {error}"
    );
}

/// Builds the bytes a well-formed client sends before any request.
fn client_opening() -> Vec<u8> {
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    drain(&mut client, &mut ())
}

/// Splits a wire buffer into (kind, flags, stream_id) triples.
///
/// Written out rather than approximated with a byte scan so that assertions about which
/// frames were emitted cannot pass by coincidence.
fn parse_frames(mut wire: &[u8]) -> Vec<(u8, u8, u32)> {
    // A server's output has no preface; a client's starts with one.
    if wire.starts_with(CLIENT_MAGIC) {
        wire = &wire[CLIENT_MAGIC.len()..];
    }

    let mut frames = Vec::new();
    while wire.len() >= 9 {
        let len = u32::from_be_bytes([0, wire[0], wire[1], wire[2]]) as usize;
        let kind = wire[3];
        let flags = wire[4];
        let stream_id = u32::from_be_bytes([wire[5], wire[6], wire[7], wire[8]]) & 0x7fff_ffff;
        frames.push((kind, flags, stream_id));

        if wire.len() < 9 + len {
            break;
        }
        wire = &wire[9 + len..];
    }
    frames
}

/// Frames a payload with the nine-octet HTTP/2 frame header.
fn frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + payload.len());
    let len = payload.len() as u32;
    out.extend_from_slice(&len.to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn protocol_violations_are_reported_by_going_away_not_by_erroring() {
    // This is the heart of FR-030. libnghttp2 handles ordinary violations itself, so a
    // successful return does NOT mean the peer behaved; the GOAWAY it queued does.
    let cases: Vec<(&str, Vec<u8>)> = vec![
        // A SETTINGS frame whose length is not a multiple of six.
        ("settings length not a multiple of six", frame(0x04, 0, 0, &[0u8; 5])),
        // A DATA frame on the connection control stream.
        ("DATA on stream zero", frame(0x00, 0, 0, b"body")),
        // A WINDOW_UPDATE with a zero increment.
        ("zero WINDOW_UPDATE increment", frame(0x08, 0, 0, &0u32.to_be_bytes())),
        // A header block that is not valid HPACK.
        (
            "corrupt header block",
            frame(0x01, 0x04, 1, &[0xff, 0xff, 0xff, 0xff, 0xff]),
        ),
    ];

    for (label, bad) in cases {
        let mut server = SessionBuilder::<Observed>::server().build().unwrap();
        let mut observed = Observed::default();

        let mut input = client_opening();
        input.extend_from_slice(&bad);

        let consumed = server
            .recv(&input, &mut observed)
            .unwrap_or_else(|e| panic!("{label}: should not be reported as an error, got {e}"));
        assert_eq!(
            consumed,
            input.len(),
            "{label}: the input should be reported as processed"
        );

        let wire = drain(&mut server, &mut observed);
        let frames = parse_frames(&wire);
        assert!(
            frames
                .iter()
                .any(|(kind, _, _)| *kind == FrameType::GOAWAY.get()),
            "{label}: expected a queued GOAWAY, saw frames {frames:?}"
        );
        assert!(
            !server.want_read(),
            "{label}: a terminated connection should no longer want to read"
        );
    }
}

#[test]
fn an_oversized_frame_is_handled_not_returned_as_an_error() {
    let mut server = SessionBuilder::<Observed>::server().build().unwrap();
    let mut observed = Observed::default();

    let mut input = client_opening();
    // Declares a payload far larger than the default maximum frame size.
    input.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);

    let consumed = server
        .recv(&input, &mut observed)
        .expect("an oversized frame is handled internally, not returned as an error");
    assert!(consumed > 0);

    let frames = parse_frames(&drain(&mut server, &mut observed));
    assert!(
        frames
            .iter()
            .any(|(kind, _, _)| *kind == FrameType::GOAWAY.get()),
        "an oversized frame should terminate the connection, saw {frames:?}"
    );
}

#[test]
fn a_session_survives_a_violation_and_can_still_be_used() {
    let mut server = SessionBuilder::<Observed>::server().build().unwrap();
    let mut observed = Observed::default();

    let mut input = client_opening();
    input.extend_from_slice(&frame(0x08, 0, 0, &0u32.to_be_bytes()));

    server.recv(&input, &mut observed).unwrap();

    // The session must remain a valid object: draining and dropping it must not fault.
    let _ = drain(&mut server, &mut observed);
    assert!(!server.want_write());
}

#[test]
fn the_context_type_is_whatever_the_caller_chose() {
    // The context is the caller's own type, not a trait object or a map.
    let mut session = SessionBuilder::<Vec<String>>::client()
        .setting(Setting::MaxConcurrentStreams(1))
        .on_frame(|log: &mut Vec<String>, info: FrameInfo| {
            log.push(format!("frame {}", info.kind().get()));
        })
        .build()
        .unwrap();

    let mut log: Vec<String> = Vec::new();
    let opening = drain(&mut session, &mut log);
    assert!(opening.starts_with(CLIENT_MAGIC));
}

/// Encodes one HPACK "literal header field without indexing, new name" entry.
///
/// Hand-rolled so the receive path can be exercised before message submission exists.
/// Only handles names and values shorter than 127 octets, which is all these tests need.
fn hpack_literal(name: &str, value: &str) -> Vec<u8> {
    assert!(name.len() < 127 && value.len() < 127);
    let mut out = vec![0x00];
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
    out
}

/// A minimal well-formed request header block for stream 1.
fn request_headers_frame() -> Vec<u8> {
    let mut payload = Vec::new();
    for (name, value) in [
        (":method", "GET"),
        (":scheme", "http"),
        (":authority", "example.test"),
        (":path", "/"),
    ] {
        payload.extend_from_slice(&hpack_literal(name, value));
    }
    // END_HEADERS | END_STREAM
    frame(0x01, 0x04 | 0x01, 1, &payload)
}

#[test]
fn a_header_handler_can_cancel_its_stream() {
    let mut server = SessionBuilder::<Observed>::server()
        .on_header(
            |observed: &mut Observed, _info: FrameInfo, name: &[u8], value: &[u8]| {
                observed.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                // Refuse anything addressed to /
                if name == b":path" && value == b"/" {
                    HeaderAction::CancelStream
                } else {
                    HeaderAction::Continue
                }
            },
        )
        .on_stream_close(|observed: &mut Observed, stream: StreamId, code, _body_error| {
            observed.closed.push((stream.get(), code.get()));
        })
        .build()
        .unwrap();

    let mut observed = Observed::default();
    let mut input = client_opening();
    input.extend_from_slice(&request_headers_frame());

    server.recv(&input, &mut observed).expect("recv failed");

    let frames = parse_frames(&drain(&mut server, &mut observed));
    assert!(
        frames
            .iter()
            .any(|(kind, _, stream)| *kind == FrameType::RST_STREAM.get() && *stream == 1),
        "cancelling from a header handler should reset stream 1, saw {frames:?}"
    );
    assert!(
        observed.closed.iter().any(|(stream, _)| *stream == 1),
        "the stream-close handler should have been told, saw {:?}",
        observed.closed
    );
}

#[test]
fn a_begin_headers_handler_can_cancel_its_stream() {
    let mut server = SessionBuilder::<Observed>::server()
        .on_begin_headers(|observed: &mut Observed, info: FrameInfo| {
            observed.begun.push(info.stream_id().get());
            HeaderAction::CancelStream
        })
        .build()
        .unwrap();

    let mut observed = Observed::default();
    let mut input = client_opening();
    input.extend_from_slice(&request_headers_frame());

    server.recv(&input, &mut observed).expect("recv failed");

    assert_eq!(observed.begun, vec![1]);
    let frames = parse_frames(&drain(&mut server, &mut observed));
    assert!(
        frames
            .iter()
            .any(|(kind, _, stream)| *kind == FrameType::RST_STREAM.get() && *stream == 1),
        "cancelling at the start of a header block should reset the stream, saw {frames:?}"
    );
}

#[test]
fn a_request_reaches_the_handlers_intact() {
    let mut server = SessionBuilder::<Observed>::server()
        .on_header(
            |observed: &mut Observed, _info: FrameInfo, name: &[u8], value: &[u8]| {
                observed.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .build()
        .unwrap();

    let mut observed = Observed::default();
    let mut input = client_opening();
    input.extend_from_slice(&request_headers_frame());

    server.recv(&input, &mut observed).expect("recv failed");

    assert_eq!(
        observed.headers,
        vec![
            (":method".into(), "GET".into()),
            (":scheme".into(), "http".into()),
            (":authority".into(), "example.test".into()),
            (":path".into(), "/".into()),
        ],
        "headers should arrive in order with names and values intact"
    );
}

/// Records the category of every header block a peer receives, in arrival order.
#[derive(Debug, Default)]
struct Categories {
    blocks: Vec<(i32, Option<nghttp2::HeaderCategory>)>,
    goaway: Vec<(i32, u32)>,
}

fn categorising(builder: SessionBuilder<Categories>) -> SessionBuilder<Categories> {
    builder.on_frame(|seen: &mut Categories, info: FrameInfo| {
        if info.kind() == FrameType::HEADERS {
            seen.blocks.push((info.stream_id().get(), info.category()));
        }
        if let Some(goaway) = info.goaway() {
            seen.goaway
                .push((goaway.last_stream_id().get(), goaway.code().get()));
        }
    })
}

/// A body that emits one chunk and then announces trailers.
struct TrailingBody {
    sent: bool,
}

impl nghttp2::BodySource for TrailingBody {
    fn fill(&mut self, buf: &mut [u8]) -> nghttp2::BodyOutcome {
        if self.sent {
            return nghttp2::BodyOutcome::EofWithTrailers(0);
        }
        let body = b"payload";
        buf[..body.len()].copy_from_slice(body);
        self.sent = true;
        nghttp2::BodyOutcome::Wrote(body.len())
    }
}

#[test]
fn a_trailing_header_block_is_distinguishable_from_the_one_that_opened_the_message() {
    // FR-030. HTTP/2 carries both in a HEADERS frame, so without the category a trailing
    // block is indistinguishable from a second set of response headers — and an async
    // layer would deliver trailers as headers.
    let mut client = categorising(SessionBuilder::<Categories>::client())
        .build()
        .unwrap();
    let mut server = categorising(SessionBuilder::<Categories>::server())
        .build()
        .unwrap();
    let (mut seen_client, mut seen_server) = (Categories::default(), Categories::default());

    let stream = client
        .submit_request(&[
            nghttp2::Header::new(":method", "GET"),
            nghttp2::Header::new(":scheme", "http"),
            nghttp2::Header::new(":authority", "example.test"),
            nghttp2::Header::new(":path", "/trailers"),
        ])
        .unwrap();

    let wire = drain(&mut client, &mut seen_client);
    server.recv(&wire, &mut seen_server).unwrap();

    server
        .submit_response_with_body(
            stream,
            &[nghttp2::Header::new(":status", "200")],
            TrailingBody { sent: false },
        )
        .unwrap();

    // Drain until the trailer window opens, then send the trailing block.
    for _ in 0..16 {
        let out = drain(&mut server, &mut seen_server);
        if !out.is_empty() {
            client.recv(&out, &mut seen_client).unwrap();
        }
        if server.trailers_ready(stream) {
            break;
        }
    }
    server
        .submit_trailer(stream, &[nghttp2::Header::new("checksum", "abc123")])
        .unwrap();
    let out = drain(&mut server, &mut seen_server);
    client.recv(&out, &mut seen_client).unwrap();

    // The server saw the request that opened the stream.
    assert_eq!(
        seen_server.blocks,
        vec![(stream.get(), Some(nghttp2::HeaderCategory::Request))],
        "a request block should be categorised as opening a request"
    );

    // The client saw the response, then the trailers — same frame type, different roles.
    let categories: Vec<_> = seen_client.blocks.iter().map(|(_, cat)| *cat).collect();
    assert_eq!(
        categories,
        vec![
            Some(nghttp2::HeaderCategory::Response),
            Some(nghttp2::HeaderCategory::Trailing)
        ],
        "the opening block and the trailing block must be distinguishable"
    );
    assert!(
        !seen_client.blocks[0].1.unwrap().is_trailing(),
        "the response block is not trailers"
    );
    assert!(
        seen_client.blocks[1].1.unwrap().is_trailing(),
        "the trailing block is"
    );
}

#[test]
fn a_received_goaway_reports_the_last_stream_the_peer_processed() {
    // FR-035. Without this the async layer cannot tell a caller which requests were
    // abandoned and are therefore safe to retry, and it may not reach past the safe
    // surface to find out.
    let mut client = categorising(SessionBuilder::<Categories>::client())
        .build()
        .unwrap();
    let mut server = SessionBuilder::<()>::server().build().unwrap();
    let mut seen = Categories::default();

    let wire = drain(&mut client, &mut seen);
    server.recv(&wire, &mut ()).unwrap();

    server
        .shutdown(StreamId::new(7), nghttp2::ErrorCode::ENHANCE_YOUR_CALM)
        .unwrap();
    let out = drain(&mut server, &mut ());
    client.recv(&out, &mut seen).unwrap();

    assert_eq!(
        seen.goaway,
        vec![(7, nghttp2::ErrorCode::ENHANCE_YOUR_CALM.get())],
        "the peer's last processed stream and reason should both survive"
    );
}

#[test]
fn a_truncated_frame_body_is_distinguishable_from_a_clean_close() {
    // FR-036. Feeding a frame header without its body leaves the session mid-frame, which
    // is what lets a transport EOF there be reported as truncation rather than an orderly
    // shutdown. Truncation inside the nine-octet header is not observable this way, and
    // the second half of this test pins that limit so it cannot be claimed by accident.
    let mut server = SessionBuilder::<Observed>::server().build().unwrap();
    let mut observed = Observed::default();
    handshake(&mut server, &mut observed);

    assert!(
        !server.mid_frame(),
        "a session between frames is not mid-frame"
    );

    // A SETTINGS frame header announcing 6 octets of payload, with none supplied.
    let header = [0x00, 0x00, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00];
    server.recv(&header, &mut observed).unwrap();
    assert!(
        server.mid_frame(),
        "a header without its payload leaves the session part-way through a frame"
    );

    // Completing the frame clears it again.
    let payload = [0x00, 0x03, 0x00, 0x00, 0x00, 0x64];
    server.recv(&payload, &mut observed).unwrap();
    assert!(
        !server.mid_frame(),
        "completing the frame ends the partial state"
    );

    // The documented limit: a truncated frame *header* reports nothing, because nothing
    // fires until a header is whole.
    server.recv(&header[..4], &mut observed).unwrap();
    assert!(
        !server.mid_frame(),
        "truncation inside a frame header is not observable, as documented"
    );
}
