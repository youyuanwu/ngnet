//! A complete client/server exchange with no sockets, no threads and no runtime
//! (Spec US-3, SC-001, SC-010, SC-015).
//!
//! This is the shape the crate is designed for: the caller owns all movement of bytes, so
//! a whole connection can be driven inside one test function.

use nghttp2::{
    ErrorCode, FrameInfo, Header, HeaderAction, Session, SessionBuilder, Setting, StreamId,
};

/// Everything one side of the connection observed.
#[derive(Debug, Default)]
struct Peer {
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    closed: Vec<(i32, u32)>,
}

impl Peer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

fn recording(mut builder: SessionBuilder<Peer>) -> SessionBuilder<Peer> {
    builder = builder
        .on_header(|peer: &mut Peer, _info: FrameInfo, name: &[u8], value: &[u8]| {
            peer.headers.push((
                String::from_utf8_lossy(name).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            ));
            HeaderAction::Continue
        })
        .on_data_chunk(|peer: &mut Peer, _stream: StreamId, chunk: &[u8]| {
            peer.body.extend_from_slice(chunk);
        })
        .on_stream_close(
            |peer: &mut Peer, stream: StreamId, code: ErrorCode, _body_error| {
                peer.closed.push((stream.get(), code.get()));
            },
        );
    builder
}

/// Moves every pending byte from `from` into `to`, asserting the whole buffer is consumed.
///
/// Returns how many octets moved, so callers can tell when the connection has gone quiet.
fn transfer<A, B>(
    from: &mut Session<A>,
    from_ctx: &mut A,
    to: &mut Session<B>,
    to_ctx: &mut B,
) -> usize {
    let mut moved = 0;

    // Each block must be handed over before asking for the next: libnghttp2 invalidates
    // the previous one, which is why this borrows and releases in turn.
    while let Some(block) = from.send(from_ctx).expect("send failed").map(<[u8]>::to_vec) {
        let consumed = to.recv(&block, to_ctx).expect("recv failed");
        assert_eq!(
            consumed,
            block.len(),
            "the receiver should report consuming exactly what it was given"
        );
        moved += consumed;
    }

    moved
}

/// Runs both sides until neither has anything left to say.
fn settle(
    client: &mut Session<Peer>,
    client_ctx: &mut Peer,
    server: &mut Session<Peer>,
    server_ctx: &mut Peer,
) {
    for _ in 0..64 {
        let up = transfer(client, client_ctx, server, server_ctx);
        let down = transfer(server, server_ctx, client, client_ctx);
        if up == 0 && down == 0 {
            return;
        }
    }
    panic!("the exchange never settled");
}

#[test]
fn a_full_request_and_response_with_a_body_completes_in_memory() {
    let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 253) as u8).collect();

    let mut client = recording(SessionBuilder::<Peer>::client()).build().unwrap();
    let mut server = recording(SessionBuilder::<Peer>::server()).build().unwrap();
    let (mut client_ctx, mut server_ctx) = (Peer::default(), Peer::default());

    let stream = client
        .submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/download"),
        ])
        .expect("submitting the request");

    // Deliver the request.
    transfer(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    assert_eq!(server_ctx.header(":path"), Some("/download"));
    assert_eq!(server_ctx.header(":method"), Some("GET"));

    server
        .submit_response_with_body(
            stream,
            &[
                Header::new(":status", "200"),
                Header::new("content-type", "application/octet-stream"),
            ],
            nghttp2::BytesBody::new(payload.clone()),
        )
        .expect("submitting the response");

    settle(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    assert_eq!(client_ctx.header(":status"), Some("200"));
    assert_eq!(
        client_ctx.body.len(),
        payload.len(),
        "the whole body should have arrived"
    );
    assert_eq!(client_ctx.body, payload, "the body should be byte-identical");
    assert!(
        client_ctx.closed.iter().any(|(s, _)| *s == stream.get()),
        "the client should have been told the stream closed"
    );
}

#[test]
fn several_concurrent_streams_complete_independently() {
    let mut client = recording(SessionBuilder::<Peer>::client()).build().unwrap();
    let mut server = recording(SessionBuilder::<Peer>::server()).build().unwrap();
    let (mut client_ctx, mut server_ctx) = (Peer::default(), Peer::default());

    let paths = ["/one", "/two", "/three", "/four"];
    let mut streams = Vec::new();
    for path in paths {
        streams.push(
            client
                .submit_request(&[
                    Header::new(":method", "GET"),
                    Header::new(":scheme", "http"),
                    Header::new(":authority", "example.test"),
                    Header::from_bytes(b":path", path.as_bytes()),
                ])
                .expect("submitting"),
        );
    }

    transfer(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    for (index, stream) in streams.iter().enumerate() {
        server
            .submit_response_with_body(
                *stream,
                &[Header::new(":status", "200")],
                nghttp2::BytesBody::new(format!("body {index}").into_bytes()),
            )
            .expect("responding");
    }

    settle(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    for path in paths {
        assert!(
            server_ctx
                .headers
                .iter()
                .any(|(n, v)| n == ":path" && v == path),
            "the server should have seen {path}"
        );
    }
    assert_eq!(
        client_ctx.closed.len(),
        streams.len(),
        "every stream should have closed exactly once"
    );
}

#[test]
fn an_exchange_completes_with_no_handlers_registered() {
    // FR-019 and SC-015: unregistered events are ignored, and nothing depends on a
    // handler being present for the protocol itself to work.
    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let mut server = SessionBuilder::<()>::server().build().unwrap();

    let stream = client
        .submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/"),
        ])
        .unwrap();

    transfer(&mut client, &mut (), &mut server, &mut ());

    server
        .submit_response_with_body(
            stream,
            &[Header::new(":status", "204")],
            nghttp2::BytesBody::new(b"quiet".to_vec()),
        )
        .unwrap();

    for _ in 0..32 {
        let up = transfer(&mut client, &mut (), &mut server, &mut ());
        let down = transfer(&mut server, &mut (), &mut client, &mut ());
        if up == 0 && down == 0 {
            break;
        }
    }

    assert!(
        !client.want_write() && !server.want_write(),
        "both sides should have gone quiet"
    );
}

#[test]
fn want_read_and_want_write_track_the_connection_lifecycle() {
    // SC-010. A fresh session has its preface and SETTINGS pending and expects the peer's
    // SETTINGS in return; after a graceful shutdown has been exchanged, neither side has
    // anything left to do and the connection may be dropped.
    let mut client = recording(SessionBuilder::<Peer>::client()).build().unwrap();
    let mut server = recording(SessionBuilder::<Peer>::server()).build().unwrap();
    let (mut client_ctx, mut server_ctx) = (Peer::default(), Peer::default());

    assert!(client.want_write(), "the preface and SETTINGS are pending");
    assert!(client.want_read(), "a fresh session expects the peer's SETTINGS");
    assert!(!client.is_finished());

    let stream = client
        .submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/"),
        ])
        .unwrap();

    transfer(&mut client, &mut client_ctx, &mut server, &mut server_ctx);
    server
        .submit_response(stream, &[Header::new(":status", "200")])
        .unwrap();
    settle(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    assert!(
        !client.want_write(),
        "everything pending should have been drained"
    );
    assert!(
        client.want_read(),
        "the connection is still open, so the peer may still speak"
    );

    // Now shut down gracefully from the server side.
    server
        .shutdown(stream, ErrorCode::NO_ERROR)
        .expect("graceful shutdown");
    settle(&mut client, &mut client_ctx, &mut server, &mut server_ctx);

    assert!(
        server.is_finished(),
        "after GOAWAY the server has nothing left to do"
    );
    assert!(
        !client.want_write(),
        "the client has nothing further to send"
    );
}

#[test]
fn the_exchange_performs_no_io_and_uses_no_sockets() {
    // The whole point of the sans-I/O shape: this test moves every byte itself.
    let mut client = SessionBuilder::<()>::client()
        .setting(Setting::MaxConcurrentStreams(10))
        .build()
        .unwrap();
    let mut server = SessionBuilder::<()>::server().build().unwrap();

    let moved = transfer(&mut client, &mut (), &mut server, &mut ());
    assert!(moved > 0, "the handshake should have moved some bytes");

    let back = transfer(&mut server, &mut (), &mut client, &mut ());
    assert!(back > 0, "the server should have answered with its SETTINGS");
}
