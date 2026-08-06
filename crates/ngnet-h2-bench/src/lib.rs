//! Shared harness for the `ngnet-h2` vs `hyper` HTTP/2 comparison.
//!
//! The two stacks are put on the same footing here so the individual benches carry no
//! fairness logic of their own: identical workload, identical protocol settings, and a
//! connection that is stood up *before* anything is measured. Both ends of each connection
//! run over a `tokio::io::duplex` — an in-memory pipe, no sockets — so what is timed is the
//! protocol and wrapper CPU work, never the kernel. See `docs/benchmarks.md` for what that
//! does and does not tell you.
//!
//! # The second family: a real socket, three arms
//!
//! A second benchmark family runs over a real loopback TCP connection, which is what a
//! completion runtime needs to appear at all — a duplex has no file descriptor, so compio
//! cannot enter the first family. It has three arms, and they complete the matrix:
//!
//! | | tokio (epoll) | compio (io_uring) |
//! | --- | --- | --- |
//! | **`ngnet-h2`** | [`TokioSocket`] | [`CompioSocket`] |
//! | **hyper** | [`HyperSocket`] | n/a — hyper has no completion transport |
//!
//! Read pairwise: [`CompioSocket`] against [`TokioSocket`] varies only the I/O model,
//! [`TokioSocket`] against [`HyperSocket`] varies only the HTTP/2 stack, and
//! [`CompioSocket`] against [`HyperSocket`] varies both at once and so attributes to
//! neither. The confound controls these benches rely on — matched `TCP_NODELAY`, one worker
//! thread each, external `taskset` pinning — are set here in the fixtures and documented in
//! `docs/benchmarks.md`.
//!
//! # The two `TokioIo` types
//!
//! This crate touches two unrelated adapters that happen to share a name:
//! `ngnet_h2::http::transport::TokioIo`, which carries a tokio stream into *this* repo's
//! transport traits, and `hyper_util::rt::TokioIo`, which carries one into hyper's. They
//! are aliased to `NgHttpIo` and `HyperHttpIo` at the import site below so no reader has to
//! guess which is in play.

use std::convert::Infallible;
use std::fmt::Debug;
use std::future::poll_fn;
use std::pin::Pin;

use bytes::Bytes;
use http_body::Body;
use http_body_util::Full;
use tokio::io::duplex;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinSet;

use compio::driver::DriverType;
use compio::net::{TcpListener as CompioTcpListener, TcpStream as CompioTcpStream};
use compio::runtime::Runtime as CompioRuntime;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};

use ngnet_h2::http::transport::{CompioIo, TokioIo as NgHttpIo};
use ngnet_h2::http::{
    Config, IncomingBody, SendRequest, handshake_shared_with, handshake_with, serve_shared_with,
    serve_with,
};

use hyper::client::conn::http2 as hyper_client;
use hyper::server::conn::http2 as hyper_server;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperHttpIo};

/// The body type carried in both directions, on both stacks.
///
/// Kept identical on purpose: a fully in-memory `Full<Bytes>` request, and a `Full<Bytes>`
/// response the server builds from the bytes it drained. Nothing about the body differs
/// between the two stacks, so a difference in the numbers cannot be a difference in body.
type BenchBody = Full<Bytes>;

// ---------------------------------------------------------------------------
// Matched protocol configuration
// ---------------------------------------------------------------------------
//
// `ngnet-h2`'s async layer advertises only two settings of its own and leaves the rest at
// libnghttp2's defaults (see `crates/ngnet-h2/src/http/config.rs` and `driver.rs`). hyper's
// builders default to much larger windows and header limits, so its builders are pinned to
// libnghttp2's defaults here. The flow-control windows matter most: a mismatched initial
// window alone can move body throughput by 2x and say nothing about either implementation.

/// libnghttp2's default `SETTINGS_INITIAL_WINDOW_SIZE`, and the connection-level window it
/// starts from and does not grow. hyper is pinned to the same and its adaptive growth is
/// switched off, so both stacks throttle a large body against the same ceiling.
pub const WINDOW: u32 = 65_535;
/// libnghttp2's default `SETTINGS_MAX_FRAME_SIZE`.
pub const MAX_FRAME_SIZE: u32 = 16_384;
/// libnghttp2's default HPACK dynamic table size.
pub const HEADER_TABLE_SIZE: u32 = 4_096;
/// This crate's advertised `SETTINGS_MAX_CONCURRENT_STREAMS` ([`Config`] default).
pub const MAX_CONCURRENT_STREAMS: u32 = 128;
/// This crate's advertised `SETTINGS_MAX_HEADER_LIST_SIZE` ([`Config`] default).
pub const MAX_HEADER_LIST_SIZE: u32 = 64 * 1024;

/// The in-memory pipe's capacity. Large enough that the pipe itself is not the bottleneck —
/// the flow-control window is — but the same for both stacks regardless.
const DUPLEX_CAPACITY: usize = 1 << 20;

/// The URI every request carries, absolute so both stacks derive the same `:scheme`,
/// `:authority` and `:path` pseudo-headers from it.
const WORKLOAD_URI: &str = "http://bench.local/bench";

/// The [`Config`] both `ngnet-h2` ends run with — the defaults, stated explicitly so the
/// matching against hyper is visible in one place.
pub fn ngnet_h2_config() -> Config {
    Config::default()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE)
}

// ---------------------------------------------------------------------------
// Runtimes
// ---------------------------------------------------------------------------

/// A single-threaded runtime, the default for these benches: there are no syscalls over a
/// duplex, so a multi-threaded scheduler would only add cross-thread wakeup noise unrelated
/// to either HTTP/2 stack.
pub fn current_thread_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime")
}

/// A multi-threaded runtime, used only by the explicitly-named multi-thread concurrency
/// group so the deterministic single-threaded numbers stay the headline.
pub fn multi_thread_runtime(workers: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("a multi-thread runtime")
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

/// A body of `len` identical octets. `Bytes` is reference-counted, so cloning the returned
/// value per iteration is a refcount bump rather than a copy of the payload.
pub fn body_of(len: usize) -> Bytes {
    Bytes::from(vec![b'a'; len])
}

/// The one request shape both stacks send: same method, path and headers.
pub fn request_for(body: Bytes) -> http::Request<BenchBody> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(WORKLOAD_URI)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header("x-bench", "1")
        .body(Full::new(body))
        .expect("a well-formed request")
}

/// The one response shape both servers send, echoing the bytes they were given.
fn response_for(body: Bytes) -> http::Response<BenchBody> {
    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .body(Full::new(body))
        .expect("a well-formed response")
}

/// Reads a whole received body and reports its length, without accumulating it.
///
/// The point is that the client actually *drains* the response: the two stacks defer
/// different amounts of work until the body is read, so a client that never reads it would
/// be measuring nothing. The length is returned only to give the caller something to hand
/// to `black_box`.
async fn drain<B>(mut body: B) -> usize
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Debug,
{
    let mut total = 0;
    while let Some(frame) = poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await {
        let frame = frame.expect("a body frame");
        if let Some(data) = frame.data_ref() {
            total += data.len();
        }
    }
    total
}

/// Reads a whole received body into contiguous bytes, so a server can echo it back. Both
/// servers do exactly this, so neither is doing less work than the other.
async fn collect<B>(mut body: B) -> Bytes
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Debug,
{
    let mut buffer = Vec::new();
    while let Some(frame) = poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await {
        let frame = frame.expect("a body frame");
        if let Some(data) = frame.data_ref() {
            buffer.extend_from_slice(data);
        }
    }
    Bytes::from(buffer)
}

// ---------------------------------------------------------------------------
// The `ngnet-h2` stack
// ---------------------------------------------------------------------------

/// The `ngnet-h2` server handler: drain the request body, echo it back.
async fn ngnet_h2_echo(request: http::Request<IncomingBody>) -> http::Response<BenchBody> {
    let body = collect(request.into_body()).await;
    response_for(body)
}

/// A live `ngnet-h2` client connected to a live `ngnet-h2` server over one duplex, with both
/// drivers already spawned. Handing back only the request handle keeps the drivers running
/// for the fixture's whole life.
pub struct NgnetH2 {
    handle: SendRequest<BenchBody>,
}

impl NgnetH2 {
    /// Stands the connection up. Call this *outside* the measured closure — establishing it
    /// is setup, not the thing under test.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let server = serve_with(NgHttpIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        tokio::spawn(server);

        let (handle, connection) =
            handshake_with::<_, BenchBody>(NgHttpIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        tokio::spawn(connection);

        Self { handle }
    }

    /// One request, awaited to its response head and then drained to the end.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` requests issued together on the one connection and awaited as a group. Each runs
    /// on its own task so all `n` are in flight before any is awaited — the multiplexing
    /// that serial latency cannot show. The spawn cost is paid identically by the hyper side.
    pub async fn concurrent(&self, n: usize) {
        let mut set = JoinSet::new();
        for _ in 0..n {
            let handle = self.handle.clone();
            set.spawn(async move {
                let response = handle
                    .send_request(request_for(Bytes::new()))
                    .await
                    .expect("a response head");
                drain(response.into_body()).await
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.expect("a request task");
        }
    }
}

// ---------------------------------------------------------------------------
// The hyper stack
// ---------------------------------------------------------------------------

/// The hyper server handler: drain the request body, echo it back. The mirror of
/// [`ngnet_h2_echo`], differing only in the body type hyper hands it and the `Result` its
/// `Service` signature requires.
async fn hyper_echo(
    request: http::Request<hyper::body::Incoming>,
) -> Result<http::Response<BenchBody>, Infallible> {
    let body = collect(request.into_body()).await;
    Ok(response_for(body))
}

/// hyper's server builder with every knob pinned to libnghttp2's defaults.
///
/// Factored out because hyper now appears on two transports — an in-memory duplex and a real
/// socket — and a matched-configuration table is only worth anything if there is exactly one
/// place the matching happens. Two copies drifting apart would silently turn a settings
/// difference into a result.
fn hyper_server_builder() -> hyper_server::Builder<TokioExecutor> {
    let mut builder = hyper_server::Builder::new(TokioExecutor::new());
    builder
        .initial_stream_window_size(WINDOW)
        .initial_connection_window_size(WINDOW)
        .adaptive_window(false)
        .max_frame_size(MAX_FRAME_SIZE)
        .header_table_size(HEADER_TABLE_SIZE)
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE)
        // hyper's server adds a `Date` header to every response by default; this crate's
        // server adds none. Switched off so both put the same header set on the wire.
        .auto_date_header(false);
    builder
}

/// hyper's client builder, pinned to the same defaults. See [`hyper_server_builder`].
fn hyper_client_builder() -> hyper_client::Builder<TokioExecutor> {
    let mut builder = hyper_client::Builder::new(TokioExecutor::new());
    builder
        .initial_stream_window_size(WINDOW)
        .initial_connection_window_size(WINDOW)
        .adaptive_window(false)
        .max_frame_size(MAX_FRAME_SIZE)
        .header_table_size(HEADER_TABLE_SIZE)
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE);
    builder
}

/// A live hyper client connected to a live hyper server over one duplex, with both drivers
/// already spawned. The mirror of [`NgnetH2`], down to the same workload and the same drain.
pub struct Hyper {
    sender: hyper_client::SendRequest<BenchBody>,
}

impl Hyper {
    /// Stands the connection up, with every builder knob pinned to libnghttp2's defaults so
    /// the two stacks advertise the same protocol limits. Call it outside the measured
    /// closure.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let server = hyper_server_builder()
            .serve_connection(HyperHttpIo::new(server_io), service_fn(hyper_echo));
        tokio::spawn(async move {
            let _ = server.await;
        });

        let (sender, connection) = hyper_client_builder()
            .handshake(HyperHttpIo::new(client_io))
            .await
            .expect("a client connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Self { sender }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let mut sender = self.sender.clone();
        let response = sender
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection. See [`NgnetH2::concurrent`].
    pub async fn concurrent(&self, n: usize) {
        let mut set = JoinSet::new();
        for _ in 0..n {
            let mut sender = self.sender.clone();
            set.spawn(async move {
                let response = sender
                    .send_request(request_for(Bytes::new()))
                    .await
                    .expect("a response head");
                drain(response.into_body()).await
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.expect("a request task");
        }
    }
}

// ---------------------------------------------------------------------------
// The real-socket family: three arms, two axes
// ---------------------------------------------------------------------------
//
// The three fixtures below all run the same workload over a real loopback TCP connection,
// and are meant to be read *pairwise*, never as a single ranking:
//
//   `CompioSocket` vs `TokioSocket` — same `ngnet-h2` stack, different I/O model. Isolates
//       completion against readiness.
//   `TokioSocket`  vs `HyperSocket` — same tokio/epoll I/O, different HTTP/2 stack. Isolates
//       this crate against hyper, on a real socket rather than the duplex.
//   `CompioSocket` vs `HyperSocket` — *both* differ. This is the end-to-end "fastest thing
//       here against the reference implementation" number, and nothing in it can be
//       attributed to either axis alone.
//
// Everything the three share — the `Config` and its hyper-side match, the request shape, the
// echo handler, the drain, `TCP_NODELAY` on every endpoint — is reused from above rather than
// restated, so a difference in the numbers cannot be a difference in what was measured.

/// A single-threaded compio runtime, asserted to be io_uring.
///
/// One worker thread, to match the tokio side's `current_thread` runtime: comparing a
/// thread-per-core runtime against a work-stealing one spread over every core would measure
/// the schedulers, not the I/O model. The backend is checked here and printed, because a
/// benchmark result outlives the manifest that produced it — a number carried forward without
/// its provenance is worthless, and a number from epoll wearing io_uring's name is worse.
/// Anything but io_uring aborts the run rather than publishing.
pub fn compio_runtime() -> CompioRuntime {
    let runtime = CompioRuntime::new().expect("compio needs io_uring to start");
    let backend = runtime.driver_type();
    assert_eq!(
        backend,
        DriverType::IoUring,
        "the completion transport must run on io_uring; a readiness driver here means \
         compio's `polling` feature was enabled somewhere in the dependency graph, and any \
         numbers taken through it would not be about a completion transport"
    );
    println!("completion transport backend: {backend:?}");
    runtime
}

/// Sets the confound-controlling socket options both transports must agree on.
///
/// `TCP_NODELAY` on both ends: Nagle waiting on a delayed ACK is exactly the kind of thing
/// that would dominate a small-request benchmark and say nothing about the I/O model, so it
/// is switched off identically here rather than left to a default the two runtimes happen to
/// share. This is the readiness side; [`compio_nodelay`] is its completion-side twin, kept
/// separate only because the two runtimes' `TcpStream` types are unrelated.
fn tokio_nodelay(stream: &TokioTcpStream) {
    stream.set_nodelay(true).expect("TCP_NODELAY on tokio");
}

/// The completion-side twin of [`tokio_nodelay`]; see it for why this is set explicitly.
fn compio_nodelay(stream: &CompioTcpStream) {
    stream.set_nodelay(true).expect("TCP_NODELAY on compio");
}

/// Binds an ephemeral loopback port, connects, accepts, and sets `TCP_NODELAY` on both ends.
///
/// Shared by both tokio-side fixtures so the `ngnet-h2` and hyper arms sit on sockets set up
/// identically — the readiness-side socket setup is not something either arm should be able
/// to differ in. The connect completes against the listen backlog, so awaiting it before the
/// accept does not deadlock on loopback; the accept then dequeues the same connection. The
/// listener is dropped on return: the connection is already established, and nothing here
/// accepts a second one.
async fn tokio_socket_pair() -> (TokioTcpStream, TokioTcpStream) {
    let listener = TokioTcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port");
    let addr = listener.local_addr().expect("the bound address");

    let client_io = TokioTcpStream::connect(addr)
        .await
        .expect("connecting to the server");
    let (server_io, _peer) = listener.accept().await.expect("accepting the client");
    tokio_nodelay(&client_io);
    tokio_nodelay(&server_io);

    (client_io, server_io)
}

/// A live `ngnet-h2` client and server over one real loopback TCP connection, driven on
/// tokio's readiness runtime. The readiness arm of the transport comparison.
pub struct TokioSocket {
    handle: SendRequest<BenchBody>,
}

impl TokioSocket {
    /// Binds, connects, accepts and spawns both drivers — all here, outside the measured
    /// closure, so Criterion attributes none of the handshake to the routine under test. The
    /// connection is established once and reused for every iteration, which is also what
    /// keeps many-iteration runs from exhausting ephemeral ports.
    pub async fn establish() -> Self {
        let (client_io, server_io) = tokio_socket_pair().await;

        let server = serve_with(NgHttpIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        tokio::spawn(server);

        let (handle, connection) =
            handshake_with::<_, BenchBody>(NgHttpIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        tokio::spawn(connection);

        Self { handle }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection. See [`NgnetH2::concurrent`].
    pub async fn concurrent(&self, n: usize) {
        let mut set = JoinSet::new();
        for _ in 0..n {
            let handle = self.handle.clone();
            set.spawn(async move {
                let response = handle
                    .send_request(request_for(Bytes::new()))
                    .await
                    .expect("a response head");
                drain(response.into_body()).await
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.expect("a request task");
        }
    }
}

/// A live `ngnet-h2` client and server over one real loopback TCP connection, driven on
/// compio's io_uring runtime. The completion arm of the transport comparison — identical to
/// [`TokioSocket`] down to the workload, differing only in transport and runtime.
pub struct CompioSocket {
    handle: SendRequest<BenchBody>,
}

impl CompioSocket {
    /// The completion-side mirror of [`TokioSocket::establish`]: same establish-outside-timing
    /// shape, same one-connection reuse, spawning on compio's runtime instead of tokio's.
    /// Must be called inside a compio runtime's `block_on`, which is where its `spawn` finds
    /// the current runtime.
    pub async fn establish() -> Self {
        let listener = CompioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding an ephemeral port");
        let addr = listener.local_addr().expect("the bound address");

        let client_io = CompioTcpStream::connect(addr)
            .await
            .expect("connecting to the server");
        let (server_io, _peer) = listener.accept().await.expect("accepting the client");
        compio_nodelay(&client_io);
        compio_nodelay(&server_io);

        let server = serve_with(CompioIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        compio::runtime::spawn(server).detach();

        let (handle, connection) =
            handshake_with::<_, BenchBody>(CompioIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        compio::runtime::spawn(connection).detach();

        Self { handle }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection, spawned so all `n` are in flight before
    /// any is awaited — the compio equivalent of [`NgnetH2::concurrent`]. compio's tasks carry no
    /// `Send` bound, which is the property a thread-per-core runtime needs.
    pub async fn concurrent(&self, n: usize) {
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let handle = self.handle.clone();
            handles.push(compio::runtime::spawn(async move {
                let response = handle
                    .send_request(request_for(Bytes::new()))
                    .await
                    .expect("a response head");
                drain(response.into_body()).await
            }));
        }
        for joined in handles {
            joined.await.expect("a request task");
        }
    }
}

/// A live hyper client and server over one real loopback TCP connection, driven on tokio's
/// readiness runtime — the reference-implementation arm of the real-socket family.
///
/// This is [`TokioSocket`] with the HTTP/2 stack swapped and nothing else changed: same
/// socket setup via [`tokio_socket_pair`], same runtime, same workload, same echo, same
/// drain, and the same matched protocol settings via [`hyper_server_builder`] /
/// [`hyper_client_builder`]. It is [`Hyper`] moved off the duplex onto a real socket, which
/// is what makes hyper comparable against the completion arm at all — a duplex has no file
/// descriptor, so no completion runtime can appear in that family.
pub struct HyperSocket {
    sender: hyper_client::SendRequest<BenchBody>,
}

impl HyperSocket {
    /// Binds, connects, accepts and spawns both drivers, all outside the measured closure.
    /// See [`TokioSocket::establish`], whose shape this follows exactly.
    pub async fn establish() -> Self {
        let (client_io, server_io) = tokio_socket_pair().await;

        let server = hyper_server_builder()
            .serve_connection(HyperHttpIo::new(server_io), service_fn(hyper_echo));
        tokio::spawn(async move {
            let _ = server.await;
        });

        let (sender, connection) = hyper_client_builder()
            .handshake(HyperHttpIo::new(client_io))
            .await
            .expect("a client connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Self { sender }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let mut sender = self.sender.clone();
        let response = sender
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection. See [`NgnetH2::concurrent`].
    pub async fn concurrent(&self, n: usize) {
        let mut set = JoinSet::new();
        for _ in 0..n {
            let mut sender = self.sender.clone();
            set.spawn(async move {
                let response = sender
                    .send_request(request_for(Bytes::new()))
                    .await
                    .expect("a response head");
                drain(response.into_body()).await
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.expect("a request task");
        }
    }
}

// ---------------------------------------------------------------------------------------
// The shared-body arms.
//
// Each of these stands up exactly the fixture above it, changing one thing: the connection
// is opened with `handshake_shared_with` / `serve_shared_with` rather than the plain forms,
// so bodies are handed over rather than copied. Same workload, same body type, same
// transport, same runtime, same config — so a difference between an arm and its twin is the
// body strategy or it is drift, and the unchanged arms measured in the same session are
// what say which.
//
// They are deliberately near-duplicates of the push fixtures rather than a parameterised
// abstraction: the whole point is that the two paths are independently constructed, so a
// refactor cannot accidentally make an arm measure its own twin.
// ---------------------------------------------------------------------------------------

/// [`NgnetH2`] over a duplex, opened on the shared-body path.
pub struct NgnetH2Shared {
    handle: SendRequest<BenchBody>,
}

impl NgnetH2Shared {
    /// See [`NgnetH2::establish`]. Differs only in the two entry points.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let server = serve_shared_with(NgHttpIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        tokio::spawn(server);

        let (handle, connection) =
            handshake_shared_with::<_, BenchBody>(NgHttpIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        tokio::spawn(connection);

        Self { handle }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }
}

/// [`TokioSocket`] over a real loopback socket, opened on the shared-body path.
pub struct TokioSharedSocket {
    handle: SendRequest<BenchBody>,
}

impl TokioSharedSocket {
    /// See [`TokioSocket::establish`]. Differs only in the two entry points.
    pub async fn establish() -> Self {
        let (client_io, server_io) = tokio_socket_pair().await;

        let server = serve_shared_with(NgHttpIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        tokio::spawn(server);

        let (handle, connection) =
            handshake_shared_with::<_, BenchBody>(NgHttpIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        tokio::spawn(connection);

        Self { handle }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }
}

/// [`CompioSocket`] over a real loopback socket, opened on the shared-body path.
///
/// This is the arm that was *expected* to gain most, and did not — which is why the expectation
/// is recorded here rather than quietly dropped. The reasoning was that the completion push
/// path pays a coalescing copy *as well as* the memset and the source copy, because compio's
/// vectored write needs owned buffers and borrowed slices can never be `'static`; handing the
/// body over makes the payload the caller's own `Bytes`, which is a valid owned region. All of
/// that is true.
///
/// What it missed is that the coalescing path already gathered a whole pass into a single
/// write, so this arm had no syscall collapse left to win — only the copy, and it gives part of
/// that back minting frame headers. Measured, it gains *least*: about 4% at 1 MiB, against
/// roughly 30% for the readiness arms, and that 4% does not clear the benchmark's own drift
/// bar. See `docs/benchmarks.md`.
pub struct CompioSharedSocket {
    handle: SendRequest<BenchBody>,
}

impl CompioSharedSocket {
    /// See [`CompioSocket::establish`]. Differs only in the two entry points.
    pub async fn establish() -> Self {
        let listener = CompioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding an ephemeral port");
        let addr = listener.local_addr().expect("the bound address");

        let client_io = CompioTcpStream::connect(addr)
            .await
            .expect("connecting to the server");
        let (server_io, _peer) = listener.accept().await.expect("accepting the client");
        compio_nodelay(&client_io);
        compio_nodelay(&server_io);

        let server = serve_shared_with(CompioIo::new(server_io), ngnet_h2_echo, ngnet_h2_config())
            .expect("a server connection");
        compio::runtime::spawn(server).detach();

        let (handle, connection) =
            handshake_shared_with::<_, BenchBody>(CompioIo::new(client_io), ngnet_h2_config())
                .expect("a client connection");
        compio::runtime::spawn(connection).detach();

        Self { handle }
    }

    /// One request, awaited to its response head and then drained. See [`NgnetH2::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }
}
