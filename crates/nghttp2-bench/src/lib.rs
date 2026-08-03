//! Shared harness for the `nghttp2` vs `hyper` HTTP/2 comparison.
//!
//! The two stacks are put on the same footing here so the individual benches carry no
//! fairness logic of their own: identical workload, identical protocol settings, and a
//! connection that is stood up *before* anything is measured. Both ends of each connection
//! run over a `tokio::io::duplex` — an in-memory pipe, no sockets — so what is timed is the
//! protocol and wrapper CPU work, never the kernel. See `docs/benchmarks.md` for what that
//! does and does not tell you.
//!
//! # The two `TokioIo` types
//!
//! This crate touches two unrelated adapters that happen to share a name:
//! `nghttp2::http::transport::TokioIo`, which carries a tokio stream into *this* repo's
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

use nghttp2::http::transport::TokioIo as NgHttpIo;
use nghttp2::http::{Config, IncomingBody, SendRequest, handshake_with, serve_with};

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
// `nghttp2`'s async layer advertises only two settings of its own and leaves the rest at
// libnghttp2's defaults (see `crates/nghttp2/src/http/config.rs` and `driver.rs`). hyper's
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

/// The [`Config`] both `nghttp2` ends run with — the defaults, stated explicitly so the
/// matching against hyper is visible in one place.
pub fn ngrs_config() -> Config {
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
// The `nghttp2` stack
// ---------------------------------------------------------------------------

/// The `nghttp2` server handler: drain the request body, echo it back.
async fn ngrs_echo(request: http::Request<IncomingBody>) -> http::Response<BenchBody> {
    let body = collect(request.into_body()).await;
    response_for(body)
}

/// A live `nghttp2` client connected to a live `nghttp2` server over one duplex, with both
/// drivers already spawned. Handing back only the request handle keeps the drivers running
/// for the fixture's whole life.
pub struct Ngrs {
    handle: SendRequest<BenchBody>,
}

impl Ngrs {
    /// Stands the connection up. Call this *outside* the measured closure — establishing it
    /// is setup, not the thing under test.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let server = serve_with(NgHttpIo::new(server_io), ngrs_echo, ngrs_config())
            .expect("a server connection");
        tokio::spawn(server);

        let (handle, connection) =
            handshake_with::<_, BenchBody>(NgHttpIo::new(client_io), ngrs_config())
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

/// A live hyper client connected to a live hyper server over one duplex, with both drivers
/// already spawned. The mirror of [`Ngrs`], down to the same workload and the same drain.
pub struct Hyper {
    sender: hyper_client::SendRequest<BenchBody>,
}

impl Hyper {
    /// Stands the connection up, with every builder knob pinned to libnghttp2's defaults so
    /// the two stacks advertise the same protocol limits. Call it outside the measured
    /// closure.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let service = service_fn(|request: http::Request<hyper::body::Incoming>| async move {
            let body = collect(request.into_body()).await;
            Ok::<_, Infallible>(response_for(body))
        });

        let server = hyper_server::Builder::new(TokioExecutor::new())
            .initial_stream_window_size(WINDOW)
            .initial_connection_window_size(WINDOW)
            .adaptive_window(false)
            .max_frame_size(MAX_FRAME_SIZE)
            .header_table_size(HEADER_TABLE_SIZE)
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .max_header_list_size(MAX_HEADER_LIST_SIZE)
            // hyper's server adds a `Date` header to every response by default; this crate's
            // server adds none. Switched off so both put the same header set on the wire.
            .auto_date_header(false)
            .serve_connection(HyperHttpIo::new(server_io), service);
        tokio::spawn(async move {
            let _ = server.await;
        });

        let (sender, connection) = hyper_client::Builder::new(TokioExecutor::new())
            .initial_stream_window_size(WINDOW)
            .initial_connection_window_size(WINDOW)
            .adaptive_window(false)
            .max_frame_size(MAX_FRAME_SIZE)
            .header_table_size(HEADER_TABLE_SIZE)
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .max_header_list_size(MAX_HEADER_LIST_SIZE)
            .handshake(HyperHttpIo::new(client_io))
            .await
            .expect("a client connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Self { sender }
    }

    /// One request, awaited to its response head and then drained. See [`Ngrs::round_trip`].
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let mut sender = self.sender.clone();
        let response = sender
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection. See [`Ngrs::concurrent`].
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
