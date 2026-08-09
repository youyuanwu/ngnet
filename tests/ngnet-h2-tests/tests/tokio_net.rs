//! A client and a server exchanging HTTP/2 over real tokio sockets.
//!
//! The blocking counterpart lives in `crates/ngnet-h2/tests/std_net.rs`. Between them they
//! make the same point from two directions: the session is indifferent to who moves its
//! bytes, and the whole of the difference between a blocking transport and an
//! asynchronous one is where the `.await` points fall.
//!
//! What these tests add over the blocking ones is what only a runtime can show — that a
//! session may be moved into a spawned task and that many connections can be in flight at
//! once, which follows from `Session` being `Send` but deliberately not `Sync`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ngnet_h2::{
    BytesBody, ErrorCode, FrameInfo, Header, HeaderAction, Session, SessionBuilder, StreamId,
};
use ngnet_h2_tests::{Fallible, drive, flush};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// Bounds how long a stalled exchange takes to fail. Without it a protocol mistake would
/// hang the test run rather than fail it.
const PATIENCE: Duration = Duration::from_secs(10);

/// What one server-side connection has seen, and what it still owes the client.
#[derive(Debug, Default)]
struct Requests {
    headers: BTreeMap<i32, Vec<(String, String)>>,
    bodies: BTreeMap<i32, Vec<u8>>,
    /// Streams whose request has fully arrived and which have not been answered yet.
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

/// What the client observed about the responses it asked for.
#[derive(Debug, Default)]
struct Responses {
    status: BTreeMap<i32, String>,
    bodies: BTreeMap<i32, Vec<u8>>,
    closed: Vec<StreamId>,
}

/// Paths the server answered, in the order it answered them, across all connections.
type ServedPaths = Arc<Mutex<Vec<String>>>;

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
            // `is_end_stream` is reported only for frames that carry message content, so
            // this does not fire for a SETTINGS acknowledgement, which reuses the bit.
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

/// Serves one connection until the client goes away.
///
/// A request carrying a body is answered with that body; anything else is answered with
/// its own path, so a client can tell which stream produced which response.
async fn serve_connection(mut socket: TcpStream, served: ServedPaths) -> Fallible<Requests> {
    socket.set_nodelay(true)?;

    // The session is created inside the task that owns it and never leaves. `Session` is
    // `Send` but not `Sync`, so this is the only shape a spawned connection can take.
    let mut session = server_session();
    let mut requests = Requests::default();

    drive(
        &mut session,
        &mut requests,
        &mut socket,
        |session, requests| {
            // Answer everything completed by the read that just happened — possibly more
            // than one request, which is the point of multiplexing them onto one socket.
            for stream in std::mem::take(&mut requests.complete) {
                let path = requests.field(stream, ":path").unwrap_or("/").to_owned();
                let echoed = requests
                    .bodies
                    .get(&stream.get())
                    .cloned()
                    .unwrap_or_default();

                served.lock().expect("served paths").push(path.clone());

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
    )
    .await?;

    Ok(requests)
}

/// Binds a listener and serves every connection it accepts, each in its own task.
///
/// Returns the address to connect to and the log of paths served. The accept loop runs
/// until the runtime shuts down at the end of the test.
async fn spawn_server() -> Fallible<(SocketAddr, ServedPaths)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let served: ServedPaths = ServedPaths::default();
    let log = Arc::clone(&served);

    tokio::spawn(async move {
        while let Ok((socket, _peer)) = listener.accept().await {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                if let Err(error) = serve_connection(socket, log).await {
                    eprintln!("connection ended: {error}");
                }
            });
        }
    });

    Ok((addr, served))
}

/// Connects, submits requests, then reads until `expected` streams have closed.
async fn exchange(
    addr: SocketAddr,
    expected: usize,
    submit: impl FnOnce(&mut Session<Responses>) -> Fallible<Vec<StreamId>>,
) -> Fallible<(Vec<StreamId>, Responses)> {
    let mut socket = TcpStream::connect(addr).await?;
    socket.set_nodelay(true)?;

    let mut session = client_session();
    let mut responses = Responses::default();

    let streams = submit(&mut session)?;

    drive(
        &mut session,
        &mut responses,
        &mut socket,
        |_session, responses| Ok(responses.closed.len() < expected),
    )
    .await?;

    // Say goodbye rather than dropping the socket on the server's foot. A client never
    // processes a peer-initiated stream, so the last one it accounts for is zero.
    session.shutdown(StreamId::CONNECTION, ErrorCode::NO_ERROR)?;
    flush(&mut session, &mut responses, &mut socket).await?;
    socket.shutdown().await?;

    Ok((streams, responses))
}

/// Fails the test rather than hanging it if an exchange never settles.
async fn within<T>(future: impl Future<Output = Fallible<T>>) -> Fallible<T> {
    tokio::time::timeout(PATIENCE, future)
        .await
        .map_err(|_| "the exchange did not settle in time")?
}

#[tokio::test]
async fn a_request_and_response_travel_over_a_tokio_socket() -> Fallible {
    let (addr, served) = spawn_server().await?;

    let (streams, responses) = within(exchange(addr, 1, |session| {
        Ok(vec![session.submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/hello"),
        ])?])
    }))
    .await?;

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
    assert_eq!(served.lock().expect("served paths").as_slice(), ["/hello"]);

    Ok(())
}

#[tokio::test]
async fn two_streams_share_one_connection() -> Fallible {
    // Both requests are submitted before a single byte is read, so their frames interleave
    // on the wire and both responses may well arrive in one read. Coping with that is the
    // difference between a transport loop that works and one that only works in a test.
    let (addr, _served) = spawn_server().await?;

    let payload = b"the body of the second request".to_vec();
    let sent = payload.clone();

    let (streams, responses) = within(exchange(addr, 2, move |session| {
        let first = session.submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/first"),
        ])?;

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
    }))
    .await?;

    let (first, second) = (streams[0], streams[1]);
    assert_ne!(first, second, "each request gets its own stream");
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

    Ok(())
}

#[tokio::test]
async fn a_large_body_crosses_the_socket_intact() -> Fallible {
    // Several times the default 64 KiB flow-control window, so the exchange only completes
    // if WINDOW_UPDATE frames travel in the opposite direction to the payload while it is
    // still being written.
    let payload: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let (addr, _served) = spawn_server().await?;

    let (streams, responses) = within(exchange(addr, 1, move |session| {
        Ok(vec![session.submit_request_with_body(
            &[
                Header::new(":method", "POST"),
                Header::new(":scheme", "http"),
                Header::new(":authority", "example.test"),
                Header::new(":path", "/upload"),
            ],
            BytesBody::new(payload),
        )?])
    }))
    .await?;

    assert_eq!(
        responses.bodies.get(&streams[0].get()).map(Vec::as_slice),
        Some(expected.as_slice()),
        "the echoed body should survive fragmentation and flow control"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_connections_are_served_at_once() -> Fallible {
    // Each connection owns a session outright, in a task the runtime may move between
    // threads. That is exactly what `Session: Send` buys, and what the absence of `Sync`
    // stops anyone doing instead: sharing one session between these tasks would not
    // compile.
    const CONNECTIONS: usize = 16;

    let (addr, served) = spawn_server().await?;

    let clients: Vec<_> = (0..CONNECTIONS)
        .map(|index| {
            tokio::spawn(async move {
                let path = format!("/client-{index}");
                let (streams, responses) = within(exchange(addr, 1, |session| {
                    Ok(vec![session.submit_request(&[
                        Header::new(":method", "GET"),
                        Header::new(":scheme", "http"),
                        Header::new(":authority", "example.test"),
                        Header::new(":path", &path),
                    ])?])
                }))
                .await?;

                let body = responses
                    .bodies
                    .get(&streams[0].get())
                    .cloned()
                    .unwrap_or_default();
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(String::from_utf8(body)?)
            })
        })
        .collect();

    let mut answered = Vec::new();
    for client in clients {
        answered.push(client.await.expect("a client task panicked")?);
    }
    answered.sort();

    let mut wanted: Vec<String> = (0..CONNECTIONS)
        .map(|index| format!("served /client-{index}"))
        .collect();
    wanted.sort();

    assert_eq!(answered, wanted, "every connection got its own answer");
    assert_eq!(served.lock().expect("served paths").len(), CONNECTIONS);

    Ok(())
}
