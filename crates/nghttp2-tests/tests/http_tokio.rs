//! The asynchronous API over real sockets, and over transports that are not sockets.
//!
//! Three claims are made here that nothing inside the `nghttp2` crate can make on its own.
//! The first is that the ready-made tokio transport works against a real kernel rather than
//! against an in-memory pipe. The second is that the transport abstraction genuinely spans
//! both families of async I/O: the **same exchange**, the **same calling code**, over a
//! readiness-based transport and a completion-shaped one, with nothing runtime-specific
//! supplied but the transport itself. The third is that a transport whose types cannot move
//! between threads still compiles and runs — the thread-per-core case the traits exist to
//! serve.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body::{Body, Frame};
use nghttp2::http::transport::{Transport, TransportRead, TransportWrite};
use nghttp2::http::{IncomingBody, server, transport::TokioIo};
use tokio::net::{TcpListener, TcpStream};

type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// Bounds how long a stalled exchange takes to fail, so a mistake fails the run rather than
/// hanging it.
const PATIENCE: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

/// A body already held in memory.
#[derive(Debug, Default)]
struct Full {
    data: Option<Bytes>,
}

impl Full {
    fn new(data: impl Into<Bytes>) -> Self {
        let data = data.into();
        Self {
            data: (!data.is_empty()).then_some(data),
        }
    }
}

impl Body for Full {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        _context: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        core::task::Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }
}

/// Reads a whole received body.
async fn drain(mut body: IncomingBody) -> Result<Vec<u8>, nghttp2::http::Error> {
    let mut received = Vec::new();
    while let Some(frame) =
        core::future::poll_fn(|context| core::pin::Pin::new(&mut body).poll_frame(context)).await
    {
        if let Some(data) = frame?.data_ref() {
            received.extend_from_slice(data);
        }
    }
    Ok(received)
}

/// The handler both halves of the SC-015 comparison run.
///
/// Named rather than written twice, because "the calling code differs only in the
/// transport" is the property under test and a copy would quietly weaken it.
async fn echo(request: http::Request<IncomingBody>) -> http::Response<Full> {
    let path = request.uri().path().to_owned();
    let body = drain(request.into_body()).await.unwrap_or_default();

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header("x-path", path)
        .body(Full::new(body))
        .expect("a well-formed response")
}

/// One request over a connection, from the client side. Also shared by both halves.
async fn ask<T: Transport>(transport: T, path: &str, payload: &'static [u8]) -> Fallible<Vec<u8>> {
    let (requests, connection) = nghttp2::http::handshake::<T, Full>(transport)?;

    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://example.test{path}"))
            .body(Full::new(payload))?,
    );

    // Driven alongside the exchange rather than spawned: nothing here may assume a spawner
    // exists, since the completion half below has no `Send` to offer one.
    let exchange = async {
        let response = response.await?;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get("x-path").map(|v| v.as_bytes()),
            Some(path.as_bytes()),
        );
        let received = drain(response.into_body()).await?;
        drop(requests);
        Ok::<_, Box<dyn StdError + Send + Sync>>(received)
    };

    let (received, _connection) = tokio::join!(exchange, connection);
    received
}

// ---------------------------------------------------------------------------
// Real sockets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_request_and_response_cross_a_real_socket() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        let serving = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(TokioIo::new(stream), "/echo", b"over a socket")
            .await
            .expect("an exchange");

        assert_eq!(received, b"over a socket");
        serving.await.expect("the server task").expect("the server");
    })
    .await
    .expect("the exchange stalled");
}

#[tokio::test]
async fn many_connections_are_served_at_once() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            loop {
                let (stream, _peer) = listener.accept().await.expect("accepting");
                tokio::spawn(async move {
                    let _ = server::serve(TokioIo::new(stream), echo)
                        .expect("serving")
                        .await;
                });
            }
        });

        let mut asking = Vec::new();
        for _ in 0..8u32 {
            asking.push(tokio::spawn(async move {
                let stream = TcpStream::connect(addr).await.expect("connecting");
                ask(TokioIo::new(stream), "/echo", b"concurrent")
                    .await
                    .expect("an exchange")
            }));
        }

        for handle in asking {
            assert_eq!(handle.await.expect("a task"), b"concurrent");
        }
    })
    .await
    .expect("the exchanges stalled");
}

#[tokio::test]
async fn a_large_body_crosses_a_real_socket_intact() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        static PAYLOAD: std::sync::LazyLock<Vec<u8>> =
            std::sync::LazyLock::new(|| (0..400 * 1024).map(|i| (i % 251) as u8).collect());

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(TokioIo::new(stream), "/echo", &PAYLOAD)
            .await
            .expect("an exchange");

        assert_eq!(received.len(), PAYLOAD.len());
        assert_eq!(received, *PAYLOAD);
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// The same exchange over a completion-shaped transport (SC-015)
// ---------------------------------------------------------------------------

/// A transport that owns every buffer it is handed, as a completion-based one must.
///
/// It keeps each buffer for the duration of the operation and hands it back afterwards,
/// which is the ownership discipline `io_uring` imposes: the kernel may still be writing
/// into a buffer after the future that submitted the operation is gone, so the buffer
/// cannot be borrowed. Backed by a tokio socket underneath, because what is under test is
/// the *shape* of the API, not the syscall behind it.
///
/// It deliberately does **not** override `write_borrowed`, so the connection takes the
/// coalescing path — the other half of the comparison.
struct Completion {
    stream: TcpStream,
}

struct CompletionReader {
    half: tokio::net::tcp::OwnedReadHalf,
}

struct CompletionWriter {
    half: tokio::net::tcp::OwnedWriteHalf,
}

impl Transport for Completion {
    type Reader = CompletionReader;
    type Writer = CompletionWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.stream.into_split();
        (
            CompletionReader { half: reader },
            CompletionWriter { half: writer },
        )
    }
}

impl TransportRead for CompletionReader {
    async fn read(&mut self, mut buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // Owned throughout: the buffer goes in, comes back, and is never borrowed from
        // anywhere else while the operation is outstanding.
        let result = tokio::io::AsyncReadExt::read_buf(&mut self.half, &mut buf).await;
        (result, buf)
    }
}

impl TransportWrite for CompletionWriter {
    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let result = tokio::io::AsyncWriteExt::write(&mut self.half, &buf).await;
        (result, buf)
    }
}

#[tokio::test]
async fn the_same_exchange_runs_over_a_completion_shaped_transport() {
    // Spec SC-015. The calling code below is `ask` and `echo` — the very functions the
    // readiness-based tests call. Only the transport differs, and nothing runtime-specific
    // is supplied beyond it: no spawner, no executor, no timer.
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(Completion { stream }, echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(Completion { stream }, "/echo", b"owned buffers")
            .await
            .expect("an exchange");

        assert_eq!(received, b"owned buffers");
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// A transport that cannot move between threads
// ---------------------------------------------------------------------------

/// A transport built on `Rc`, as the thread-per-core runtimes are.
///
/// Nothing in the transport traits requires [`Send`], precisely so that runtimes like this
/// can implement them. This stands in for one: if a `Send` bound ever appeared anywhere in
/// the traits, this file would stop compiling.
struct ThreadBound {
    inner: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
    peer: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

struct ThreadBoundReader {
    inner: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

struct ThreadBoundWriter {
    peer: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

impl Transport for ThreadBound {
    type Reader = ThreadBoundReader;
    type Writer = ThreadBoundWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            ThreadBoundReader { inner: self.inner },
            ThreadBoundWriter { peer: self.peer },
        )
    }
}

impl TransportRead for ThreadBoundReader {
    async fn read(&mut self, mut buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        loop {
            let taken: Vec<u8> = core::mem::take(&mut *self.inner.borrow_mut());
            if !taken.is_empty() {
                buf.extend_from_slice(&taken);
                return (Ok(taken.len()), buf);
            }
            tokio::task::yield_now().await;
        }
    }
}

impl TransportWrite for ThreadBoundWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)> {
        self.peer.borrow_mut().extend_from_slice(&buf);
        core::future::ready((Ok(buf.len()), buf))
    }
}

#[tokio::test]
async fn a_transport_that_cannot_move_between_threads_compiles_and_runs() {
    // Spec SC-015, and the reason there is no `Send` bound in the traits. A connection over
    // this transport is not `Send` — asserted below by the fact that it is driven with
    // `LocalSet` rather than `spawn`, which would not accept it.
    tokio::time::timeout(PATIENCE, async {
        let to_server = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let to_client = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        let client = ThreadBound {
            inner: std::rc::Rc::clone(&to_client),
            peer: std::rc::Rc::clone(&to_server),
        };
        let peer = ThreadBound {
            inner: to_server,
            peer: to_client,
        };

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let serving = tokio::task::spawn_local(async move {
                    let _ = server::serve(peer, echo).expect("serving").await;
                });

                let received = ask(client, "/echo", b"thread bound")
                    .await
                    .expect("an exchange");
                assert_eq!(received, b"thread bound");
                serving.abort();
            })
            .await;
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// A third-party client
// ---------------------------------------------------------------------------

/// What one request to the async server looked like from `curl`'s side.
#[derive(Debug)]
struct CurlResult {
    status: String,
    body: String,
}

fn curl(url: &str, payload: Option<&str>) -> Option<CurlResult> {
    let mut command = std::process::Command::new("curl");
    command.args(["--http2-prior-knowledge", "-s", "-i", url]);
    if let Some(payload) = payload {
        command.args(["--data", payload]);
    }

    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))?;
    Some(CurlResult {
        status: head.lines().next()?.trim().to_owned(),
        body: body.to_owned(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_third_party_client_completes_a_request() {
    // Spec SC-002. Everything else in this suite has this crate on both ends, which cannot
    // catch a shared misreading of the protocol. `curl` has no such stake.
    if std::process::Command::new("curl")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("curl is not installed; skipping the third-party client check");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
    let addr = listener.local_addr().expect("an address");

    let serving = tokio::spawn(async move {
        loop {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            tokio::spawn(async move {
                let _ = server::serve(TokioIo::new(stream), echo)
                    .expect("serving")
                    .await;
            });
        }
    });

    let plain = tokio::task::spawn_blocking(move || curl(&format!("http://{addr}/hello"), None))
        .await
        .expect("the curl task")
        .expect("curl completed a request");
    assert_eq!(plain.status, "HTTP/2 200");
    assert_eq!(plain.body, "");

    let echoed = tokio::task::spawn_blocking(move || {
        curl(&format!("http://{addr}/echo"), Some("from curl"))
    })
    .await
    .expect("the curl task")
    .expect("curl completed a request");
    assert_eq!(echoed.status, "HTTP/2 200");
    assert_eq!(echoed.body, "from curl");

    serving.abort();
}

// ---------------------------------------------------------------------------
// Concurrency on one connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn four_streams_share_one_real_connection() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (requests, connection) =
            nghttp2::http::handshake::<_, Full>(TokioIo::new(stream)).expect("handshake");

        let seen: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let recorder = Arc::clone(&seen);

        let exchange = async move {
            let mut waiting = Vec::new();
            for index in 0..4 {
                let path = format!("/stream-{index}");
                waiting.push((
                    path.clone(),
                    requests.send_request(
                        http::Request::builder()
                            .method(http::Method::POST)
                            .uri(format!("http://example.test{path}"))
                            .body(Full::new(format!("payload {index}")))
                            .expect("a request"),
                    ),
                ));
            }

            // Awaited in reverse, so a response that arrived while nothing was looking is
            // still waiting when someone finally looks.
            for (path, response) in waiting.into_iter().rev() {
                let response = response.await.expect("a response");
                let body = drain(response.into_body()).await.expect("a body");
                recorder.lock().expect("record").insert(path, body);
            }
            drop(requests);
        };

        tokio::join!(exchange, connection)
            .1
            .expect("the connection");

        let seen = seen.lock().expect("record");
        assert_eq!(seen.len(), 4);
        for index in 0..4 {
            assert_eq!(
                seen.get(&format!("/stream-{index}")).map(Vec::as_slice),
                Some(format!("payload {index}").as_bytes()),
            );
        }
    })
    .await
    .expect("the exchanges stalled");
}
