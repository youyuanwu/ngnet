//! A complete h2c exchange over real loopback sockets, using nothing but `std::net`.
//!
//! The rest of the test suite drives both peers in memory, which proves the protocol
//! logic but says nothing about how the sans-I/O surface is meant to be attached to a
//! transport. That is what this file is for: it is the smallest honest answer to "how do
//! I actually use this over a socket?", written against blocking `TcpStream`s and one
//! thread per peer.
//!
//! Three things about the transport loop are not obvious and are what the helpers below
//! exist to get right:
//!
//! * A read returns an arbitrary slice of the stream. It may hold several frames, part of
//!   one, or bytes belonging to several streams at once, so the receive call has to be
//!   fed in a loop rather than once per read.
//! * Bytes only become available to write as a consequence of bytes read, so every pass
//!   round the loop must flush before it blocks on the socket. Blocking first is how a
//!   connection deadlocks.
//! * Handlers are never handed the session, so a server cannot answer a request from
//!   inside a handler. It records what arrived and responds between reads, which is the
//!   `step` hook below.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use nghttp2::{
    BytesBody, ErrorCode, FrameInfo, Header, HeaderAction, Session, SessionBuilder, StreamId,
};

/// Errors cross a thread boundary when the server thread is joined, hence `Send + Sync`.
type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// Bounds how long a stalled exchange takes to fail. A blocking socket with no timeout
/// turns a protocol mistake into a hung test rather than a failing one.
const PATIENCE: Duration = Duration::from_secs(10);

/// Writes everything the session currently has queued.
///
/// Each block must be handed to the socket before the next is asked for: libnghttp2
/// invalidates the previous one, which is why the borrow returned by `send` is tied to
/// the session and released at the end of each iteration.
fn flush<C>(session: &mut Session<C>, ctx: &mut C, socket: &mut TcpStream) -> Fallible {
    while let Some(block) = session.send(ctx)? {
        socket.write_all(block)?;
    }
    socket.flush()?;
    Ok(())
}

/// Feeds one socket read to the session, which may consume it in several bites.
fn feed<C>(session: &mut Session<C>, mut input: &[u8], ctx: &mut C) -> Fallible {
    while !input.is_empty() {
        let consumed = session.recv(input, ctx)?;
        if consumed == 0 {
            // The session has stopped accepting input — it has been terminated. The
            // remainder is not an error, it is simply no longer wanted.
            break;
        }
        input = &input[consumed..];
    }
    Ok(())
}

/// Runs one session over a blocking socket until it has nothing left to do.
///
/// `step` is called after each batch of received bytes, with the session available, and
/// is where a peer acts on what just arrived — submitting a response, or deciding the
/// exchange is over. Returning `false` stops the loop after a final flush.
fn drive<C>(
    session: &mut Session<C>,
    ctx: &mut C,
    socket: &mut TcpStream,
    mut step: impl FnMut(&mut Session<C>, &mut C) -> Fallible<bool>,
) -> Fallible {
    let mut buf = vec![0u8; 16 * 1024];

    loop {
        flush(session, ctx, socket)?;

        if !session.want_read() {
            return Ok(());
        }

        let read = socket.read(&mut buf)?;
        if read == 0 {
            // The peer closed. Anything still queued has nowhere to go.
            return Ok(());
        }

        feed(session, &buf[..read], ctx)?;

        if !step(session, ctx)? {
            return flush(session, ctx, socket);
        }
    }
}

/// What one server-side connection observed, and what it still owes the client.
#[derive(Debug, Default)]
struct Requests {
    /// Header fields per stream, in arrival order.
    headers: BTreeMap<i32, Vec<(String, String)>>,
    /// Request payload per stream.
    bodies: BTreeMap<i32, Vec<u8>>,
    /// Streams whose request is complete and which have not been answered yet.
    complete: Vec<StreamId>,
}

impl Requests {
    fn field(&self, stream: StreamId, name: &str) -> Option<&str> {
        self.headers
            .get(&stream.get())?
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// What the client observed about one response.
#[derive(Debug, Default)]
struct Responses {
    status: BTreeMap<i32, String>,
    bodies: BTreeMap<i32, Vec<u8>>,
    closed: Vec<StreamId>,
}

fn server_session() -> Session<Requests> {
    SessionBuilder::<Requests>::server()
        .on_header(
            |req: &mut Requests, frame: FrameInfo, name: &[u8], value: &[u8]| {
                req.headers
                    .entry(frame.stream_id().get())
                    .or_default()
                    .push((
                        String::from_utf8_lossy(name).into_owned(),
                        String::from_utf8_lossy(value).into_owned(),
                    ));
                HeaderAction::Continue
            },
        )
        .on_data_chunk(|req: &mut Requests, stream: StreamId, chunk: &[u8]| {
            req.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_frame(|req: &mut Requests, frame: FrameInfo| {
            // `is_end_stream` is only reported for the frame types that carry message
            // content, so this does not fire for a SETTINGS acknowledgement — which
            // reuses the same flag bit.
            if frame.is_end_stream() {
                req.complete.push(frame.stream_id());
            }
        })
        .build()
        .expect("building the server session")
}

fn client_session() -> Session<Responses> {
    SessionBuilder::<Responses>::client()
        .on_header(
            |res: &mut Responses, frame: FrameInfo, name: &[u8], value: &[u8]| {
                if name == b":status" {
                    res.status.insert(
                        frame.stream_id().get(),
                        String::from_utf8_lossy(value).into_owned(),
                    );
                }
                HeaderAction::Continue
            },
        )
        .on_data_chunk(|res: &mut Responses, stream: StreamId, chunk: &[u8]| {
            res.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_stream_close(
            |res: &mut Responses, stream: StreamId, _code: ErrorCode, _body_error| {
                res.closed.push(stream);
            },
        )
        .build()
        .expect("building the client session")
}

/// Serves exactly one connection, answering every request until the client goes away.
///
/// Each response echoes the request back, so the test can tell which stream produced
/// which answer without the server having to know anything about the test.
fn serve_one(listener: &TcpListener) -> Fallible<Requests> {
    let (mut socket, _peer) = listener.accept()?;
    socket.set_read_timeout(Some(PATIENCE))?;
    socket.set_nodelay(true)?;

    let mut session = server_session();
    let mut requests = Requests::default();

    drive(
        &mut session,
        &mut requests,
        &mut socket,
        |session, requests| {
            // Answer everything that completed during the read that just happened. A
            // single read can complete more than one request, which is the whole point of
            // multiplexing them onto one connection.
            for stream in std::mem::take(&mut requests.complete) {
                let path = requests.field(stream, ":path").unwrap_or("/").to_owned();
                let echoed = requests
                    .bodies
                    .get(&stream.get())
                    .cloned()
                    .unwrap_or_default();

                let body = if echoed.is_empty() {
                    format!("served {path}").into_bytes()
                } else {
                    echoed
                };

                session.submit_response_with_body(
                    stream,
                    &[
                        Header::new(":status", "200"),
                        Header::new("content-type", "text/plain"),
                    ],
                    BytesBody::new(body),
                )?;
            }
            Ok(true)
        },
    )?;

    Ok(requests)
}

/// Connects, runs `submit` to place requests, then reads until `expected` streams close.
fn exchange(
    addr: SocketAddr,
    expected: usize,
    submit: impl FnOnce(&mut Session<Responses>) -> Fallible<Vec<StreamId>>,
) -> Fallible<(Vec<StreamId>, Responses)> {
    let mut socket = TcpStream::connect(addr)?;
    socket.set_read_timeout(Some(PATIENCE))?;
    socket.set_nodelay(true)?;

    let mut session = client_session();
    let mut responses = Responses::default();

    let streams = submit(&mut session)?;

    drive(
        &mut session,
        &mut responses,
        &mut socket,
        |_session, responses| Ok(responses.closed.len() < expected),
    )?;

    // Say goodbye properly rather than dropping the socket on the server's foot. A client
    // never processes a peer-initiated stream, so the last one it accounts for is zero.
    session.shutdown(StreamId::CONNECTION, ErrorCode::NO_ERROR)?;
    flush(&mut session, &mut responses, &mut socket)?;
    socket.shutdown(Shutdown::Write)?;

    Ok((streams, responses))
}

#[test]
fn a_request_and_response_travel_over_a_real_socket() -> Fallible {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = thread::spawn(move || serve_one(&listener));

    let (streams, responses) = exchange(addr, 1, |session| {
        Ok(vec![session.submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/hello"),
        ])?])
    })?;

    let stream = streams[0];
    assert_eq!(
        responses.status.get(&stream.get()).map(String::as_str),
        Some("200")
    );
    assert_eq!(
        responses.bodies.get(&stream.get()).map(Vec::as_slice),
        Some(b"served /hello".as_slice())
    );
    assert_eq!(responses.closed, vec![stream]);

    let requests = server.join().expect("the server thread panicked")?;
    assert_eq!(requests.field(stream, ":method"), Some("GET"));
    assert_eq!(requests.field(stream, ":path"), Some("/hello"));
    assert_eq!(requests.field(stream, ":authority"), Some("example.test"));

    Ok(())
}

#[test]
fn two_streams_share_one_connection() -> Fallible {
    // Both requests are submitted before a single byte is read, so their frames interleave
    // on the wire and the responses may well arrive in one read. Handling that is the
    // difference between a transport loop that works and one that only works in a test.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = thread::spawn(move || serve_one(&listener));

    let payload = b"the body of the second request".to_vec();
    let sent = payload.clone();

    let (streams, responses) = exchange(addr, 2, move |session| {
        let first = session.submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/first"),
        ])?;

        // The second carries a body, which the server echoes back.
        let second = session.submit_request_with_body(
            &[
                Header::new(":method", "POST"),
                Header::new(":scheme", "http"),
                Header::new(":authority", "example.test"),
                Header::new(":path", "/second"),
                Header::new("content-type", "text/plain"),
            ],
            BytesBody::new(payload),
        )?;

        Ok(vec![first, second])
    })?;

    let (first, second) = (streams[0], streams[1]);
    assert_ne!(first, second, "each request gets its own stream");

    assert_eq!(
        responses.status.get(&first.get()).map(String::as_str),
        Some("200")
    );
    assert_eq!(
        responses.status.get(&second.get()).map(String::as_str),
        Some("200")
    );
    assert_eq!(
        responses.bodies.get(&first.get()).map(Vec::as_slice),
        Some(b"served /first".as_slice())
    );
    assert_eq!(
        responses.bodies.get(&second.get()).map(Vec::as_slice),
        Some(sent.as_slice()),
        "the second response should echo the request body"
    );

    assert_eq!(responses.closed.len(), 2);

    let requests = server.join().expect("the server thread panicked")?;
    assert_eq!(requests.field(first, ":path"), Some("/first"));
    assert_eq!(requests.field(second, ":path"), Some("/second"));
    assert_eq!(
        requests.bodies.get(&second.get()).map(Vec::as_slice),
        Some(sent.as_slice())
    );

    Ok(())
}

#[test]
fn a_large_body_crosses_the_socket_intact() -> Fallible {
    // Big enough to exhaust the default 64 KiB flow-control window several times over, so
    // the exchange only completes if WINDOW_UPDATE frames are travelling in the opposite
    // direction to the payload while it is being written.
    let payload: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = thread::spawn(move || serve_one(&listener));

    let (streams, responses) = exchange(addr, 1, move |session| {
        Ok(vec![session.submit_request_with_body(
            &[
                Header::new(":method", "POST"),
                Header::new(":scheme", "http"),
                Header::new(":authority", "example.test"),
                Header::new(":path", "/upload"),
            ],
            BytesBody::new(payload),
        )?])
    })?;

    let stream = streams[0];
    assert_eq!(
        responses.bodies.get(&stream.get()).map(Vec::as_slice),
        Some(expected.as_slice()),
        "the echoed body should survive fragmentation and flow control"
    );

    let requests = server.join().expect("the server thread panicked")?;
    assert_eq!(
        requests.bodies.get(&stream.get()).map(Vec::as_slice),
        Some(expected.as_slice())
    );

    Ok(())
}
