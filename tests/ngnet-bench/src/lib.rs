//! Shared harness for this workspace's benchmark suite: `ngnet-h2` against `hyper`,
//! HTTP/2 against HTTP/3-over-QMux, and HTTP/3 over Quinn or ngtcp2.
//!
//! The stacks are put on the same footing here so the individual benches carry no
//! fairness logic of their own: identical workload, identical protocol settings, and a
//! connection that is stood up *before* anything is measured. Both ends of each connection
//! run over a `tokio::io::duplex` — an in-memory pipe, no sockets — so what is timed is the
//! protocol and wrapper CPU work, never the kernel. See `docs/benchmarks/interpreting.md`
//! for what that does and does not tell you.
//!
//! # The third family: two protocols
//!
//! [`NgnetQmuxH3`] and [`NgnetQmuxH3Socket`] are the HTTP/3-over-QMux arms, and they are not
//! a family of their own: each joins the existing family on its substrate, beside the HTTP/2
//! arm it is there to be compared against. What differs between an HTTP/2 arm and its QMux
//! counterpart is the protocol stack; the substrate, the runtime, the request, the body, the
//! echo and the drain are shared code. The settings the two protocols hold in common are
//! matched from the named constants below — see [`qmux_config`] and [`qmux_h3_config`], and
//! `docs/benchmarks/configuration.md` for what cannot be matched and which way it leans.
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
//! `docs/benchmarks/controls.md`.
//!
//! # The two `TokioIo` types
//!
//! This crate touches two unrelated adapters that happen to share a name:
//! `ngnet_h2::http::transport::TokioIo`, which carries a tokio stream into *this* repo's
//! transport traits, and `hyper_util::rt::TokioIo`, which carries one into hyper's. They
//! are aliased to `NgHttpIo` and `HyperHttpIo` at the import site below so no reader has to
//! guess which is in play.

use std::any::Any;
use std::convert::Infallible;
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body::Body;
use http_body_util::Full;
use tokio::io::duplex;
use tokio::runtime::{Builder, Runtime};
use tokio::task::{JoinHandle, JoinSet};

use compio::driver::DriverType;
use compio::net::{TcpListener as CompioTcpListener, TcpStream as CompioTcpStream};
use compio::runtime::Runtime as CompioRuntime;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};

use ngnet_h2::http::transport::{CompioIo, TokioIo as NgHttpIo};
use ngnet_h2::http::{
    Config, IncomingBody, SendRequest, handshake_shared_with, handshake_with, serve_shared_with,
    serve_with,
};

use ngnet_h3::http::{
    IncomingBody as H3IncomingBody, SendRequest as H3SendRequest, handshake as h3_handshake,
    handshake_with as h3_handshake_with, serve as h3_serve, serve_with as h3_serve_with,
};
use ngnet_h3_quinn::QuinnBackend;
use ngnet_qmux::io::{AsyncByteStream, TokioClock, TokioStream, Written};
use ngnet_qmux_h3::{HttpConfig, TransportConfig, connect_with, serve_with as qmux_serve_with};
use ngnet_quic::OsslSession;
use ngnet_quic::endpoint::Endpoint as NgtcpEndpoint;
use ngnet_quic_h3_tests::{
    Credentials as NgtcpCredentials, TEST_SERVER_NAME as NGTCP_SERVER_NAME,
    client_endpoint as ngtcp_client_endpoint, server_endpoint as ngtcp_server_endpoint,
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

// The QMux side of the same matching. Everything below exists so that the HTTP/3-over-QMux
// arms grant the same credit and keep the same compression state as the HTTP/2 arms; where a
// value is derived from one of the constants above, it is written as that constant rather
// than as its number, so a change to the matched value cannot reach one stack and miss the
// other. The two figures that are *not* derived — the stream allowance and the read-ahead —
// are harness parameters rather than protocol settings, and each says below what it is
// protecting against.

/// The credit each QMux end grants the other **on one stream**, matched to [`WINDOW`].
///
/// libnghttp2 fixes an HTTP/2 stream at 65535 bytes and `ngnet-h2`'s configuration surface
/// cannot reach it, so the matching is done from this side: the stack that exposes the
/// setting is set to the value the other one is fixed at. A mismatch here is the single
/// easiest way to turn a body-throughput comparison into a comparison of two windows.
pub const QMUX_STREAM_WINDOW: u64 = WINDOW as u64;

/// The credit each QMux end grants the other **across the whole connection**, also [`WINDOW`].
///
/// Equal in number to [`QMUX_STREAM_WINDOW`] and to HTTP/2's connection window, and not quite
/// equal in meaning: HTTP/3's three unidirectional control streams are ordinary QMux streams
/// and spend connection credit, where HTTP/2's control frames sit outside flow control
/// entirely. The difference is a few hundred bytes over a connection's life against a window
/// that is extended per consumed byte, so it biases nothing measurable — but it is a real
/// asymmetry between the two arms rather than an exact match, and is recorded as such.
pub const QMUX_CONNECTION_WINDOW: u64 = WINDOW as u64;

/// How many bidirectional streams each QMux end lets the other open **over the connection's
/// whole life**.
///
/// Not a concurrency limit, and this is the trap. QMux stream capacity is a cumulative budget
/// that nothing recycles — neither dwnx nor `ngnet-qmux` returns it when a stream closes, and
/// `ngnet-qmux-h3` never calls `extend_stream_limit` — so a connection admits exactly this
/// many requests in total and the one after that **hangs**: the open waits for capacity that
/// will never arrive, no error is reported at either end, and no timeout surrounds a Criterion
/// measurement. These benches establish one connection and reuse it for every iteration of
/// every sample of every parameter value, so the default of 100 is exhausted almost
/// immediately.
///
/// Both bounds therefore matter, and a value outside either breaks a different way:
///
/// - **Too low** — anywhere near what a run can consume — and the suite stops partway through
///   without failing, which is worse than failing.
/// - **Too high** — above dwnx's `DWNX_MAX_STREAMS`, which is `1 << 60`
///   (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_transport_params.h:63`) — and the *peer*
///   rejects the parameter as it
///   decodes it, failing the connection during setup with an error that names nothing about
///   streams. `TransportParams::validate`'s varint check does not catch this — it bounds the
///   encoding, not dwnx's limit — so a value at `1 << 61` is accepted where it is configured
///   and fails on the wire.
///
/// 2^40 is roughly a trillion: about six orders of magnitude above the ~260,000 streams a
/// deliberately heavy soak of these fixtures spent on one connection, and about six below the
/// ceiling. It costs nothing to sit there — 300,000 sequential streams on one connection moved
/// RSS by 216 kB, so no state accumulates behind the allowance.
pub const QMUX_MAX_STREAMS_BIDI: u64 = 1 << 40;

/// How many unidirectional streams each QMux end lets the other open.
///
/// HTTP/3 needs three — control, QPACK encoder, QPACK decoder — and will do nothing at all
/// until it has them, so a value below three yields a connection whose peer can never start.
/// Sixteen is those three with room for a peer that opens more, and it is small on purpose:
/// unlike the bidirectional allowance nothing consumes this repeatedly, so there is no reason
/// to reach for a number that hides a miscount.
pub const QMUX_MAX_STREAMS_UNI: u64 = 16;

/// How many bytes the QMux layer will hold for the HTTP/3 layer before it has reported
/// consuming some.
///
/// Purely local — it is not advertised and is not a protocol setting — so it is a harness
/// parameter, and it is stated rather than left at its 1 MiB default for the same reason
/// every other harness parameter here is stated. The constraint that matters is that it must
/// **not fall below** [`QMUX_CONNECTION_WINDOW`]: below it, the layer refuses to read bytes
/// the peer has already been told it may send, which stalls the connection with no error.
/// Equal to the connection window is the smallest value that respects that, and it transfers
/// a 1 MiB body in window-sized instalments exactly as the default does.
pub const QMUX_READ_AHEAD: u64 = QMUX_CONNECTION_WINDOW;

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

/// The QMux transport configuration both HTTP/3 ends run with.
///
/// Every field that bears on the comparison is set, including where the value coincides with
/// the default, for the same reason [`ngnet_h2_config`] restates h2's defaults: a comparison
/// resting on two crates' defaults staying equal is one upstream edit away from silently
/// comparing unlike things. See each constant for what it matches and what breaks if it moves.
///
/// The one field left alone is `max_idle_timeout`, and deliberately: its default of zero means
/// "none", nothing in QMux enforces one in either direction, and a benchmark that advertised a
/// deadline nobody keeps would be stating a fiction rather than matching anything. HTTP/2 has
/// no counterpart to match it against.
pub fn qmux_config() -> TransportConfig {
    TransportConfig::new()
        .initial_max_stream_data(QMUX_STREAM_WINDOW)
        .initial_max_data(QMUX_CONNECTION_WINDOW)
        .max_streams_bidi(QMUX_MAX_STREAMS_BIDI)
        .max_streams_uni(QMUX_MAX_STREAMS_UNI)
        .read_ahead(QMUX_READ_AHEAD)
}

/// The HTTP/3 configuration both ends run with, built from the *same* named constants the
/// HTTP/2 arms use.
///
/// The three settings here are the ones the two protocols hold in common under different
/// names: `max_concurrent_streams` against `SETTINGS_MAX_CONCURRENT_STREAMS`,
/// `max_field_section_size` against `SETTINGS_MAX_HEADER_LIST_SIZE`, and
/// `qpack_max_dtable_capacity` against HPACK's dynamic table size. `ngnet-h3`'s defaults
/// already equal `ngnet-h2`'s, deliberately — but coinciding is not matching, so each value
/// is written here as the constant the h2 side reads, and there is one place to change.
pub fn qmux_h3_config() -> HttpConfig {
    HttpConfig::default()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_field_section_size(MAX_HEADER_LIST_SIZE as u64)
        .qpack_max_dtable_capacity(HEADER_TABLE_SIZE as usize)
}

/// Comparison-only ngnet H3 configuration matched to hyperium H3's fixed QPACK setting.
///
/// Hyperium H3 0.0.8 exposes no dynamic-table-capacity control, so the matched QMux pair
/// disables the ngnet side's dynamic table rather than silently comparing unlike state.
pub fn qmux_h3_matched_config() -> HttpConfig {
    HttpConfig::default()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_field_section_size(MAX_HEADER_LIST_SIZE as u64)
        .qpack_max_dtable_capacity(0)
}

/// Comparison-only ngnet H3 configuration for the ngtcp2 pair, matched to hyperium H3.
///
/// Identical in value to [`qmux_h3_matched_config`] and separate on purpose: the two
/// comparisons are free to diverge, and a shared helper would silently couple them. The
/// reason for zeroing the dynamic table is the same in both — hyperium H3 0.0.8 exposes no
/// QPACK dynamic-table control, and `ngnet-h3` defaults to a 4 KiB table, so leaving the
/// default in place would compare unlike header state and call the difference "the HTTP/3
/// implementation".
pub fn ngtcp_h3_matched_config() -> HttpConfig {
    HttpConfig::default()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_field_section_size(MAX_HEADER_LIST_SIZE as u64)
        .qpack_max_dtable_capacity(0)
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

#[cfg(test)]
mod checked_progress_tests {
    use super::{CheckedIntegrity, reported_integrity};

    #[test]
    fn partial_exact_progress_is_not_a_length_mismatch() {
        assert_eq!(
            reported_integrity(512, 1024, CheckedIntegrity::ExactSoFar, false),
            CheckedIntegrity::ExactSoFar
        );
    }

    #[test]
    fn terminal_short_or_overlong_bodies_are_length_mismatches() {
        assert_eq!(
            reported_integrity(512, 1024, CheckedIntegrity::ExactSoFar, true),
            CheckedIntegrity::LengthMismatch
        );
        assert_eq!(
            reported_integrity(2048, 1024, CheckedIntegrity::LengthMismatch, false),
            CheckedIntegrity::LengthMismatch
        );
    }
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

/// Stable application boundary for supervised HTTP/3 liveness probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedPhase {
    /// Waiting for the response headers.
    ResponseHead,
    /// Reading response body bytes before the expected length has arrived.
    BodyDrain,
    /// All expected bytes arrived and the body is waiting for its terminal indication.
    TerminalWait,
    /// The body stream reached its terminal indication; integrity is reported separately.
    Complete,
}

/// Integrity known about response bytes observed so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedIntegrity {
    /// Every observed byte matches and the expected length has not been exceeded.
    ExactSoFar,
    /// At least one observed byte differs from the expected body.
    ContentMismatch,
    /// Received length differs from expected: an overrun, or a short body at completion.
    LengthMismatch,
}

fn reported_integrity(
    total: usize,
    expected: usize,
    integrity: CheckedIntegrity,
    terminal: bool,
) -> CheckedIntegrity {
    if terminal && total != expected {
        CheckedIntegrity::LengthMismatch
    } else {
        integrity
    }
}

/// Last durable application-level progress for one checked exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckedProgressSnapshot {
    /// Current application boundary.
    pub phase: CheckedPhase,
    /// Response bytes observed so far.
    pub received: usize,
    /// Integrity known before any later timeout or transport failure.
    pub integrity: CheckedIntegrity,
}

/// Stable failure category used by supervised checked exchanges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedFailureKind {
    /// The native HTTP/3 connection ended.
    Closed,
    /// A request, protocol, body, or peer response failed for another reason.
    Other,
}

/// A checked-exchange failure with a typed category and printable detail.
#[derive(Debug)]
pub struct CheckedFailure {
    kind: CheckedFailureKind,
    detail: String,
}

impl CheckedFailure {
    /// Builds a connection-ending failure.
    pub fn closed(detail: impl Into<String>) -> Self {
        Self {
            kind: CheckedFailureKind::Closed,
            detail: detail.into(),
        }
    }

    /// Builds a non-connection-ending failure.
    pub fn other(detail: impl Into<String>) -> Self {
        Self {
            kind: CheckedFailureKind::Other,
            detail: detail.into(),
        }
    }

    fn ngnet(context: &'static str, error: ngnet_h3::http::Error) -> Self {
        Self {
            kind: if error.kind() == ngnet_h3::http::ErrorKind::Closed {
                CheckedFailureKind::Closed
            } else {
                CheckedFailureKind::Other
            },
            detail: format!("{context}: {error:?}"),
        }
    }

    /// Stable failure category.
    pub fn kind(&self) -> CheckedFailureKind {
        self.kind
    }
}

impl core::fmt::Display for CheckedFailure {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

/// Shared progress cell used by an external supervisor while an exchange is pending.
#[derive(Clone)]
pub struct CheckedProgress {
    state: Arc<Mutex<CheckedProgressSnapshot>>,
    observer: Option<Arc<dyn Fn(CheckedProgressSnapshot) + Send + Sync>>,
}

impl Default for CheckedProgress {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(CheckedProgressSnapshot {
                phase: CheckedPhase::ResponseHead,
                received: 0,
                integrity: CheckedIntegrity::ExactSoFar,
            })),
            observer: None,
        }
    }
}

impl CheckedProgress {
    /// Creates a progress cell that reports durable application-boundary changes.
    pub fn observed(observer: impl Fn(CheckedProgressSnapshot) + Send + Sync + 'static) -> Self {
        Self {
            observer: Some(Arc::new(observer)),
            ..Self::default()
        }
    }

    /// Returns the most recently recorded phase and byte offset.
    pub fn snapshot(&self) -> CheckedProgressSnapshot {
        *self.state.lock().expect("checked progress mutex poisoned")
    }

    fn record(&self, phase: CheckedPhase, received: usize, integrity: CheckedIntegrity) {
        let snapshot = CheckedProgressSnapshot {
            phase,
            received,
            integrity,
        };
        *self.state.lock().expect("checked progress mutex poisoned") = snapshot;
        if let Some(observer) = &self.observer {
            observer(snapshot);
        }
    }
}

impl core::fmt::Debug for CheckedProgress {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CheckedProgress")
            .field("snapshot", &self.snapshot())
            .field("observed", &self.observer.is_some())
            .finish()
    }
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
    B::Error: Debug + 'static,
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

async fn try_drain_checked<B>(mut body: B, expected: &[u8]) -> Result<(usize, bool), String>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Debug + 'static,
{
    try_drain_checked_observed(&mut body, expected, None)
        .await
        .map_err(|error| error.to_string())
}

async fn try_drain_checked_observed<B>(
    body: &mut B,
    expected: &[u8],
    progress: Option<&CheckedProgress>,
) -> Result<(usize, bool), CheckedFailure>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Debug + 'static,
{
    let mut total = 0usize;
    let mut exact = true;
    let mut integrity = CheckedIntegrity::ExactSoFar;
    if let Some(progress) = progress {
        progress.record(CheckedPhase::BodyDrain, 0, CheckedIntegrity::ExactSoFar);
    }
    loop {
        if let Some(progress) = progress {
            progress.record(
                if total >= expected.len() {
                    CheckedPhase::TerminalWait
                } else {
                    CheckedPhase::BodyDrain
                },
                total,
                integrity,
            );
        }
        let Some(frame) = poll_fn(|context| Pin::new(&mut *body).poll_frame(context)).await else {
            break;
        };
        let frame = frame.map_err(|error| {
            let kind = (&error as &dyn Any)
                .downcast_ref::<ngnet_h3::http::Error>()
                .map_or(CheckedFailureKind::Other, |error| {
                    if error.kind() == ngnet_h3::http::ErrorKind::Closed {
                        CheckedFailureKind::Closed
                    } else {
                        CheckedFailureKind::Other
                    }
                });
            CheckedFailure {
                kind,
                detail: format!("response body frame failed: {error:?}"),
            }
        })?;
        if let Some(data) = frame.data_ref() {
            let end = total.saturating_add(data.len());
            let expected_range = expected.get(total..end);
            let exceeded = expected_range.is_none();
            if exceeded {
                integrity = CheckedIntegrity::LengthMismatch;
            }
            if exact && !expected_range.is_some_and(|range| range == data.as_ref()) {
                if let Some(expected_range) = expected_range {
                    let mismatch = expected_range
                        .iter()
                        .zip(data.iter())
                        .position(|(expected, actual)| expected != actual)
                        .map_or(total, |offset| total + offset);
                    let frame_offset = mismatch.saturating_sub(total);
                    let actual_end = (frame_offset + 16).min(data.len());
                    let expected_end = (mismatch + 16).min(expected.len());
                    eprintln!(
                        "checked body first differs at byte {mismatch}; frame={total}..{end}; \
                         expected={:?}; actual={:?}",
                        &expected[mismatch..expected_end],
                        &data[frame_offset..actual_end],
                    );
                } else {
                    eprintln!(
                        "checked body exceeded expected length at byte {total}; \
                         frame={total}..{end}; expected_len={}",
                        expected.len()
                    );
                }
                exact = false;
                if integrity != CheckedIntegrity::LengthMismatch {
                    integrity = CheckedIntegrity::ContentMismatch;
                }
            }
            total = end;
            if let Some(progress) = progress {
                progress.record(
                    if total >= expected.len() {
                        CheckedPhase::TerminalWait
                    } else {
                        CheckedPhase::BodyDrain
                    },
                    total,
                    integrity,
                );
            }
        }
    }
    let exact = exact && total == expected.len();
    if let Some(progress) = progress {
        progress.record(
            CheckedPhase::Complete,
            total,
            reported_integrity(total, expected.len(), integrity, true),
        );
    }
    Ok((total, exact))
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
/// bar. See `docs/benchmarks/findings/handing-bodies-over.md`.
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

// ---------------------------------------------------------------------------------------
// The HTTP/3-over-QMux arms
//
// Two fixtures, one per substrate, so the cross-protocol comparison spans exactly the two
// families the HTTP/2 arms already span: [`NgnetQmuxH3`] beside [`NgnetH2`] and [`Hyper`] on
// the in-memory duplex, [`NgnetQmuxH3Socket`] beside [`TokioSocket`] and [`HyperSocket`] on a
// real loopback socket. Both take the same request shape, the same body, the same echo and
// the same drain as every arm above — `request_for`, `body_of`, `collect`, `drain` and
// `WORKLOAD_URI` are reused rather than restated, so "the two stacks ran the same workload"
// is a property of there being one definition rather than an assertion about two.
//
// The one thing that *is* restated is the echo handler, and only its signature: the HTTP/3
// server hands its handler an `ngnet_h3::http::IncomingBody` where the HTTP/2 server hands it
// an `ngnet_h2::http::IncomingBody`. The two are unrelated types with the same name, so a
// second function is unavoidable; its body is one call to the shared `collect` and one to the
// shared `response_for`, which is as close to no restatement as the type system allows.
//
// Each matched fixture spawns exactly one task per endpoint. The ngnet connection future
// already combines transport and H3 progress, so its task is only counted. Hyperium exposes
// separate adapter and H3 futures, so one bench-local future polls them together and retains
// the adapter close tail after H3 exits. Both tasks use plain `tokio::spawn`; non-`Send`
// portability remains covered by the adapter tests rather than changing benchmark topology.
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct CounterState {
    armed: AtomicBool,
    lower_read_calls: AtomicU64,
    lower_read_bytes: AtomicU64,
    lower_write_calls: AtomicU64,
    lower_write_bytes: AtomicU64,
    lower_write_not_now: AtomicU64,
    endpoint_polls: AtomicU64,
    overflowed: AtomicBool,
}

/// Per-fixture symmetric lower-I/O and endpoint-poll counters.
///
/// One instance aggregates the client and server endpoints of exactly one fixture. It starts
/// disarmed, and every `establish` method resets it after the explicit warm-up. Criterion never
/// arms it; fixed diagnostic probes and fixture tests call [`BenchCounters::reset_and_arm`]
/// immediately before their measured exchange.
#[derive(Clone, Default)]
pub struct BenchCounters {
    state: Arc<CounterState>,
}

/// One counter interval.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BenchCounterSnapshot {
    /// Lower read polls.
    pub lower_read_calls: u64,
    /// Bytes returned by lower reads.
    pub lower_read_bytes: u64,
    /// Lower write polls.
    pub lower_write_calls: u64,
    /// Bytes accepted by lower writes.
    pub lower_write_bytes: u64,
    /// Lower writes returning `NotNow`.
    pub lower_write_not_now: u64,
    /// Polls of one endpoint task, summed across both endpoints of the fixture.
    pub endpoint_polls: u64,
    /// Whether any counter saturated.
    pub overflowed: bool,
}

impl BenchCounters {
    fn add(&self, counter: &AtomicU64, value: u64) {
        if !self.state.armed.load(Ordering::Relaxed) || value == 0 {
            return;
        }
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(value);
            match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    if next == u64::MAX && current != u64::MAX {
                        self.state.overflowed.store(true, Ordering::Relaxed);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Resets the aggregate client+server interval and controls subsequent counting.
    pub fn reset_and_arm(&self, armed: bool) {
        self.state.armed.store(false, Ordering::Release);
        for counter in [
            &self.state.lower_read_calls,
            &self.state.lower_read_bytes,
            &self.state.lower_write_calls,
            &self.state.lower_write_bytes,
            &self.state.lower_write_not_now,
            &self.state.endpoint_polls,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.state.overflowed.store(false, Ordering::Relaxed);
        self.state.armed.store(armed, Ordering::Release);
    }

    /// Returns the current aggregate client+server fixture interval.
    #[must_use]
    pub fn snapshot(&self) -> BenchCounterSnapshot {
        BenchCounterSnapshot {
            lower_read_calls: self.state.lower_read_calls.load(Ordering::Relaxed),
            lower_read_bytes: self.state.lower_read_bytes.load(Ordering::Relaxed),
            lower_write_calls: self.state.lower_write_calls.load(Ordering::Relaxed),
            lower_write_bytes: self.state.lower_write_bytes.load(Ordering::Relaxed),
            lower_write_not_now: self.state.lower_write_not_now.load(Ordering::Relaxed),
            endpoint_polls: self.state.endpoint_polls.load(Ordering::Relaxed),
            overflowed: self.state.overflowed.load(Ordering::Relaxed),
        }
    }

    fn endpoint_poll(&self) {
        self.add(&self.state.endpoint_polls, 1);
    }
}

/// Byte-stream wrapper used identically around both measured QMux adapters.
pub struct CountingStream<S> {
    inner: S,
    counters: BenchCounters,
}

impl<S> CountingStream<S> {
    fn new(inner: S, counters: BenchCounters) -> Self {
        Self { inner, counters }
    }
}

impl<S: AsyncByteStream> AsyncByteStream for CountingStream<S> {
    type Error = S::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        self.counters.add(&self.counters.state.lower_read_calls, 1);
        let result = self.inner.poll_read(cx, buffer);
        if let Poll::Ready(Ok(bytes)) = &result {
            self.counters
                .add(&self.counters.state.lower_read_bytes, *bytes as u64);
        }
        result
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        self.counters.add(&self.counters.state.lower_write_calls, 1);
        let result = self.inner.poll_write(cx, buffer);
        match &result {
            Poll::Ready(Ok(Written::Accepted(bytes))) => self
                .counters
                .add(&self.counters.state.lower_write_bytes, *bytes as u64),
            Poll::Ready(Ok(Written::NotNow)) => self
                .counters
                .add(&self.counters.state.lower_write_not_now, 1),
            Poll::Pending | Poll::Ready(Err(_)) => {}
        }
        result
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_shutdown(cx)
    }
}

async fn run_counted_endpoint<F>(future: F, counters: BenchCounters)
where
    F: Future<Output = ()>,
{
    let mut future = Box::pin(future);
    poll_fn(|cx| {
        counters.endpoint_poll();
        future.as_mut().poll(cx)
    })
    .await;
}

async fn run_combined_endpoint<D, H, E>(driver: D, h3: H, counters: BenchCounters)
where
    D: Future<Output = Result<(), E>>,
    H: Future<Output = ()>,
{
    let mut driver = Box::pin(driver);
    let mut h3 = Box::pin(h3);
    let mut driver_done = false;
    let mut h3_done = false;
    poll_fn(|cx| {
        counters.endpoint_poll();
        if !h3_done && h3.as_mut().poll(cx).is_ready() {
            h3_done = true;
        }
        if !driver_done && driver.as_mut().poll(cx).is_ready() {
            driver_done = true;
        }
        if h3_done && driver_done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// The HTTP/3 server handler: drain the request body, echo it back.
///
/// [`ngnet_h2_echo`] for HTTP/3. Same work, same shared helpers; only the incoming body's
/// type differs, and that is the whole reason there are two of these.
async fn qmux_h3_echo(request: http::Request<H3IncomingBody>) -> http::Response<BenchBody> {
    let body = collect(request.into_body()).await;
    response_for(body)
}

/// Refuses a concurrency the QMux arms' configuration will not admit, before it is offered.
///
/// The bound is [`MAX_CONCURRENT_STREAMS`], the value both stacks are configured with. On the
/// HTTP/3 side the server is what enforces it, and it enforces it by *resetting* an exchange
/// that arrives while that many handlers are already running rather than by queueing it
/// (`crates/ngnet-h3/src/http/server.rs:257-264`). So a concurrency above the limit neither
/// fails cleanly nor simply works: whether an iteration completes depends on how many handlers
/// happen to be in flight as each head arrives, which is a benchmark that reports times for
/// some samples and panics part-way through others.
///
/// Checking here rather than letting the connection answer is the general rule these fixtures
/// follow, and the reason is sharper than this one parameter. The characteristic failure on
/// this stack is a request that neither completes nor fails — an exhausted stream allowance, a
/// peer whose control streams never opened, a window nobody extended — and nothing wraps a
/// Criterion measurement in a timeout, so a parameter that gets as far as being offered cannot
/// be recovered from. A panic with a legible message, before anything reaches the wire, is the
/// only recovery available.
///
/// The transport's much larger [`QMUX_MAX_STREAMS_BIDI`] cannot bind first — that is what
/// makes it the *cumulative* allowance and this the *concurrent* one.
///
/// # Panics
///
/// If `n` exceeds what the configuration admits.
fn admit_concurrency(n: usize) {
    assert!(
        n <= MAX_CONCURRENT_STREAMS as usize,
        "a concurrency of {n} exceeds the {MAX_CONCURRENT_STREAMS} concurrent exchanges both \
         stacks are configured for; offered to the QMux arm, the server would reset whichever \
         exchanges arrived over the limit, so the run would report a time for some iterations \
         and fail part-way through others"
    );
}

/// The condition under which a body of any size is admissible, checked once at compile time.
///
/// The companion to [`admit_concurrency`], and deliberately *not* its twin, because a body
/// meets a different kind of limit and the difference decides where the check belongs. No
/// configured value bounds a body: flow-control credit is extended per consumed byte at both
/// the stream and the connection level, so a body larger than the window is delivered in
/// window-sized instalments rather than refused. There is therefore no body size to reject,
/// and a per-body runtime check would be a test whose answer does not depend on its argument.
///
/// What a multi-instalment body does depend on is [`QMUX_READ_AHEAD`] not sitting below
/// [`QMUX_CONNECTION_WINDOW`]. Below it the layer declines to read bytes the peer was already
/// told it could send, and a body needing more than one instalment stops halfway with nothing
/// reported at either end — the same silent-stall failure mode the stream allowance has, and
/// just as invisible to a benchmark.
///
/// That is a relation between two constants, so it is asserted as one. A `const` block fails
/// the *build* if a later edit lowers the read-ahead beneath the window, which is strictly
/// stronger than a runtime assertion that can only fire on a machine actually running the
/// suite — and it is honest about the fact that no value of any body length can trip it.
const _: () = assert!(
    QMUX_READ_AHEAD >= QMUX_CONNECTION_WINDOW,
    "the configured read-ahead is below the connection window, so a body needing more than \
     one instalment would stall part-way with no error reported at either end"
);

/// One complete exchange, before anything is timed.
///
/// The HTTP/2 arms need no equivalent and this asymmetry is deliberate rather than leftover,
/// so here is why it is not redundant. A QMux connection's transport-parameter exchange is
/// scheduled when the connection is constructed but only leaves on the first pump
/// (`crates/ngnet-qmux/src/io/conn.rs:429-434`), and until the peer's parameters arrive every
/// limit is zero and no stream can be opened; on top of that the HTTP/3 driver's first act is
/// to open three unidirectional streams and exchange SETTINGS
/// (`crates/ngnet-h3/src/http/driver.rs:407-417`). None of that happens in `establish` unless
/// something makes it happen, so without this the *first timed iteration* would pay for the
/// whole handshake and be reported as a measurement of a round trip. One completed
/// request-response settles all of it: the parameters have been exchanged, the control streams
/// are open, and the connection is in the state every subsequent iteration will find it in.
///
/// The HTTP/2 fixtures need none because `handshake_with` completes their handshake during
/// setup, which is the difference — not that one stack is warmed and the other is not.
///
/// # Panics
///
/// If the exchange fails, which is the same treatment a failed exchange gets inside a timed
/// iteration: this is setup, and setup that did not work must not be measured over.
async fn qmux_warm_up(handle: &H3SendRequest<BenchBody>) {
    let response = handle
        .send_request(request_for(Bytes::new()))
        .await
        .expect("a warm-up response head");
    assert!(response.status().is_success());
    let echoed = drain(response.into_body()).await;
    assert_eq!(echoed, 0, "the warm-up exchange echoes an empty body");
}

/// A live HTTP/3-over-QMux client connected to a live server over one duplex, with both
/// drivers already spawned and one exchange already completed.
///
/// The cross-protocol counterpart of [`NgnetH2`]: same substrate, same runtime arrangement,
/// same workload, same drain — the protocol stack is what differs, which is the whole point.
/// The duplex halves are wrapped in `TokioStream`, QMux's byte-stream adapter for anything
/// implementing `AsyncRead` and `AsyncWrite`; the clocks come from `TokioClock`, one per end,
/// since a QMux connection needs a clock of its own and timestamps are never compared across
/// connections.
pub struct NgnetQmuxH3 {
    handle: H3SendRequest<BenchBody>,
    server: JoinHandle<()>,
}

impl NgnetQmuxH3 {
    /// Stands the connection up and warms it. Call this *outside* the measured closure —
    /// establishing it is setup, not the thing under test, and [`qmux_warm_up`] explains why
    /// standing it up is not by itself enough.
    ///
    /// # Panics
    ///
    /// If either end cannot be built, or the warm-up exchange fails.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);

        let server = qmux_serve_with(
            TokioStream::new(server_io),
            TokioClock::new(),
            qmux_h3_echo,
            qmux_config(),
            qmux_h3_config(),
        )
        .expect("a server connection");
        let server = tokio::spawn(async move {
            let _ = server.await;
        });

        let (handle, connection) = connect_with::<_, _, BenchBody>(
            TokioStream::new(client_io),
            TokioClock::new(),
            qmux_config(),
            qmux_h3_config(),
        )
        .expect("a client connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        qmux_warm_up(&handle).await;
        Self { handle, server }
    }

    /// One request, awaited to its response head and then drained to the end. See
    /// [`NgnetH2::round_trip`], whose shape and failure handling this matches: a failed
    /// exchange panics out of the timed closure rather than being recorded as a time.
    ///
    /// # Panics
    ///
    /// If the exchange fails. No body size is inadmissible; see the `const` assertion
    /// beside [`admit_concurrency`] for why that is a property of the constants, not of `body`.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` requests issued together on the one connection and awaited as a group. See
    /// [`NgnetH2::concurrent`], which this mirrors down to the `JoinSet` and the spawn cost,
    /// so the two arms pay the same harness overhead.
    ///
    /// # Panics
    ///
    /// If `n` is inadmissible ([`admit_concurrency`]), or any exchange fails.
    pub async fn concurrent(&self, n: usize) {
        admit_concurrency(n);
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

    /// Takes the server away without telling the client, so the next exchange fails.
    pub fn abandon_server(&self) {
        self.server.abort();
    }
}

/// The ngnet H3-over-QMux duplex fixture with QPACK disabled to match hyperium H3.
pub struct NgnetQmuxH3Matched {
    handle: H3SendRequest<BenchBody>,
    server: JoinHandle<()>,
    counters: BenchCounters,
}

impl NgnetQmuxH3Matched {
    /// Establishes and warms the matched ngnet fixture.
    pub async fn establish() -> Self {
        let (client_io, server_io) = duplex(DUPLEX_CAPACITY);
        let counters = BenchCounters::default();
        let server = qmux_serve_with(
            CountingStream::new(TokioStream::new(server_io), counters.clone()),
            TokioClock::new(),
            qmux_h3_echo,
            qmux_config(),
            qmux_h3_matched_config(),
        )
        .expect("a matched ngnet server");
        let server_counters = counters.clone();
        let server = tokio::spawn(run_counted_endpoint(
            async move {
                let _ = server.await;
            },
            server_counters,
        ));
        let (handle, connection) = connect_with::<_, _, BenchBody>(
            CountingStream::new(TokioStream::new(client_io), counters.clone()),
            TokioClock::new(),
            qmux_config(),
            qmux_h3_matched_config(),
        )
        .expect("a matched ngnet client");
        tokio::spawn(run_counted_endpoint(
            async move {
                let _ = connection.await;
            },
            counters.clone(),
        ));
        qmux_warm_up(&handle).await;
        tokio::task::yield_now().await;
        counters.reset_and_arm(false);
        Self {
            handle,
            server,
            counters,
        }
    }

    /// Sends one request body and drains its exact echo.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a matched ngnet response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// Sends one request and validates every echoed byte for diagnostic probes.
    pub async fn try_round_trip_checked(&self, body: Bytes) -> Result<(usize, bool), String> {
        let expected = body.clone();
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .map_err(|error| format!("matched ngnet response head failed: {error:?}"))?;
        try_drain_checked(response.into_body(), &expected).await
    }

    /// Takes away the matched ngnet server for failure-path tests.
    pub fn abandon_server(&self) {
        self.server.abort();
    }

    /// Enables a fresh symmetric counter interval.
    pub fn arm_counters(&self) {
        self.counters.reset_and_arm(true);
    }

    /// Current symmetric fixture counters.
    #[must_use]
    pub fn counter_snapshot(&self) -> BenchCounterSnapshot {
        self.counters.snapshot()
    }
}

type UpstreamMemoryOpener =
    h3_ngnet_qmux::OpenStreams<CountingStream<TokioStream<tokio::io::DuplexStream>>, TokioClock>;
type UpstreamMemorySender = h3::client::SendRequest<UpstreamMemoryOpener, Bytes>;
type UpstreamSocketOpener =
    h3_ngnet_qmux::OpenStreams<CountingStream<TokioStream<TokioTcpStream>>, TokioClock>;
type UpstreamSocketSender = h3::client::SendRequest<UpstreamSocketOpener, Bytes>;

/// Pending peer accepts reserved by the benchmark adapter.
pub const UPSTREAM_QMUX_PENDING_ACCEPTS: usize = MAX_CONCURRENT_STREAMS as usize;

/// Rejects a workload that exceeds the adapter's pending-accept resource policy.
///
/// Benchmark targets call only serial round trips today. This guard is public so any future
/// concurrent target and the fixture tests share one pre-wire admission rule.
pub fn validate_upstream_qmux_concurrency(n: usize) {
    assert!(
        n <= UPSTREAM_QMUX_PENDING_ACCEPTS,
        "a concurrency of {n} exceeds the {UPSTREAM_QMUX_PENDING_ACCEPTS} pending accepts \
         reserved by the hyperium QMux fixture"
    );
}

async fn upstream_h3_qmux_server<C>(connection: C)
where
    C: h3::quic::Connection<Bytes>,
{
    let mut builder = h3::server::builder();
    builder.send_grease(false);
    builder.max_field_section_size(MAX_HEADER_LIST_SIZE as u64);
    let mut connection = builder
        .build::<_, Bytes>(connection)
        .await
        .expect("an upstream H3 QMux server");
    'requests: loop {
        let resolver = match connection.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) | Err(_) => return,
        };
        let (_request, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(_) => continue,
        };
        let mut body = BytesMut::new();
        loop {
            match stream.recv_data().await {
                Ok(Some(chunk)) => body.put(chunk),
                Ok(None) => break,
                Err(_) => continue 'requests,
            }
        }
        if stream
            .send_response(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/octet-stream")
                    .body(())
                    .expect("an upstream H3 QMux response"),
            )
            .await
            .is_err()
        {
            continue;
        }
        if !body.is_empty() && stream.send_data(body.freeze()).await.is_err() {
            continue;
        }
        let _ = stream.finish().await;
    }
}

fn upstream_qmux_request_head() -> http::Request<()> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(WORKLOAD_URI)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header("x-bench", "1")
        .body(())
        .expect("an upstream QMux request")
}

async fn upstream_qmux_round_trip<O>(
    sender: &h3::client::SendRequest<O, Bytes>,
    body: Bytes,
) -> usize
where
    O: h3::quic::OpenStreams<Bytes> + Clone,
{
    let mut sender = sender.clone();
    let mut stream = sender
        .send_request(upstream_qmux_request_head())
        .await
        .expect("an upstream QMux request stream");
    if !body.is_empty() {
        stream
            .send_data(body)
            .await
            .expect("upstream QMux request data");
    }
    stream.finish().await.expect("finish upstream QMux request");
    let response = stream
        .recv_response()
        .await
        .expect("an upstream QMux response head");
    assert!(response.status().is_success());
    let mut total = 0;
    while let Some(chunk) = stream
        .recv_data()
        .await
        .expect("upstream QMux response data")
    {
        total += chunk.remaining();
    }
    total
}

async fn upstream_qmux_round_trip_checked<O>(
    sender: &h3::client::SendRequest<O, Bytes>,
    body: Bytes,
) -> Result<(usize, bool), String>
where
    O: h3::quic::OpenStreams<Bytes> + Clone,
{
    let expected = body.clone();
    let mut sender = sender.clone();
    let mut stream = sender
        .send_request(upstream_qmux_request_head())
        .await
        .map_err(|error| format!("upstream QMux request failed: {error:?}"))?;
    if !body.is_empty() {
        stream
            .send_data(body)
            .await
            .map_err(|error| format!("upstream QMux request data failed: {error:?}"))?;
    }
    stream
        .finish()
        .await
        .map_err(|error| format!("upstream QMux request finish failed: {error:?}"))?;
    let response = stream
        .recv_response()
        .await
        .map_err(|error| format!("upstream QMux response failed: {error:?}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "upstream QMux response status was {}",
            response.status()
        ));
    }
    let mut received = BytesMut::new();
    while let Some(chunk) = stream
        .recv_data()
        .await
        .map_err(|error| format!("upstream QMux response data failed: {error:?}"))?
    {
        received.put(chunk);
    }
    Ok((received.len(), received.as_ref() == expected.as_ref()))
}

struct UpstreamQmuxTasks {
    server: JoinHandle<()>,
    client: JoinHandle<()>,
}

impl UpstreamQmuxTasks {
    fn abandon_server(&self) {
        self.server.abort();
    }
}

impl Drop for UpstreamQmuxTasks {
    fn drop(&mut self) {
        self.server.abort();
        self.client.abort();
    }
}

async fn upstream_memory_pair() -> (UpstreamMemorySender, UpstreamQmuxTasks, BenchCounters) {
    let (client_io, server_io) = duplex(DUPLEX_CAPACITY);
    let counters = BenchCounters::default();
    let server_lower = ngnet_qmux::io::Connection::server(
        CountingStream::new(TokioStream::new(server_io), counters.clone()),
        TokioClock::new(),
        qmux_config(),
    )
    .expect("an upstream server QMux connection");
    let (server_connection, server_driver) =
        h3_ngnet_qmux::from_qmux(server_lower, UPSTREAM_QMUX_PENDING_ACCEPTS);
    let server = tokio::spawn(run_combined_endpoint(
        server_driver,
        upstream_h3_qmux_server(server_connection),
        counters.clone(),
    ));

    let client_lower = ngnet_qmux::io::Connection::client(
        CountingStream::new(TokioStream::new(client_io), counters.clone()),
        TokioClock::new(),
        qmux_config(),
    )
    .expect("an upstream client QMux connection");
    let (client_connection, client_driver) =
        h3_ngnet_qmux::from_qmux(client_lower, UPSTREAM_QMUX_PENDING_ACCEPTS);
    let (sender_tx, sender_rx) = tokio::sync::oneshot::channel();
    let client_counters = counters.clone();
    let client = tokio::spawn(async move {
        let mut adapter = Box::pin(client_driver);
        let mut builder = h3::client::builder();
        builder.send_grease(false);
        builder.max_field_section_size(MAX_HEADER_LIST_SIZE as u64);
        let building = builder.build(client_connection);
        tokio::pin!(building);
        let (mut h3, sender) = poll_fn(|cx| {
            client_counters.endpoint_poll();
            if let Poll::Ready(built) = building.as_mut().poll(cx) {
                return Poll::Ready(built);
            }
            let _ = adapter.as_mut().poll(cx);
            Poll::Pending
        })
        .await
        .expect("an upstream H3 QMux client");
        sender_tx.send(sender).ok();
        run_combined_endpoint(
            adapter,
            async move {
                let _ = poll_fn(|context| h3.poll_close(context)).await;
            },
            client_counters,
        )
        .await;
    });
    let sender = sender_rx.await.expect("upstream H3 client sender");
    (sender, UpstreamQmuxTasks { server, client }, counters)
}

/// A persistent hyperium H3-over-QMux duplex fixture.
pub struct UpstreamH3Qmux {
    sender: UpstreamMemorySender,
    tasks: UpstreamQmuxTasks,
    counters: BenchCounters,
}

impl UpstreamH3Qmux {
    /// Establishes and warms the hyperium fixture outside the measured closure.
    pub async fn establish() -> Self {
        let (sender, tasks, counters) = upstream_memory_pair().await;
        let fixture = Self {
            sender,
            tasks,
            counters,
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        tokio::task::yield_now().await;
        fixture.counters.reset_and_arm(false);
        fixture
    }

    /// Sends one request body and drains its exact echo.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        upstream_qmux_round_trip(&self.sender, body).await
    }

    /// Sends and drains one body while checking every echoed byte.
    pub async fn try_round_trip_checked(&self, body: Bytes) -> Result<(usize, bool), String> {
        upstream_qmux_round_trip_checked(&self.sender, body).await
    }

    /// Takes away both server-side drivers.
    pub fn abandon_server(&self) {
        self.tasks.abandon_server();
    }

    /// Enables a fresh symmetric counter interval.
    pub fn arm_counters(&self) {
        self.counters.reset_and_arm(true);
    }

    /// Current symmetric fixture counters.
    #[must_use]
    pub fn counter_snapshot(&self) -> BenchCounterSnapshot {
        self.counters.snapshot()
    }
}

// ---------------------------------------------------------------------------
// HTTP/3 over Quinn: ngnet-h3 against h3 + h3-quinn
// ---------------------------------------------------------------------------

const QUINN_WORKLOAD_URI: &str = "https://bench.local/bench";
const H3_ALPN: &[u8] = b"h3";

fn quic_request_for(body: Bytes) -> http::Request<BenchBody> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(QUINN_WORKLOAD_URI)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header("x-bench", "1")
        .body(Full::new(body))
        .expect("a well-formed HTTP/3 request")
}

fn quinn_request_head() -> http::Request<()> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(QUINN_WORKLOAD_URI)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header("x-bench", "1")
        .body(())
        .expect("a well-formed HTTP/3 request")
}

fn certified() -> (
    quinn::rustls::pki_types::CertificateDer<'static>,
    quinn::rustls::pki_types::PrivatePkcs8KeyDer<'static>,
) {
    let certified =
        rcgen::generate_simple_self_signed(vec!["bench.local".to_string()]).expect("a certificate");
    let key =
        quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    (certified.cert.into(), key)
}

fn quinn_server_endpoint() -> (
    quinn::Endpoint,
    quinn::rustls::pki_types::CertificateDer<'static>,
) {
    let (certificate, key) = certified();
    let mut crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .expect("a server TLS configuration");
    crypto.alpn_protocols = vec![H3_ALPN.to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .expect("a QUIC server TLS configuration");
    let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = quinn::Endpoint::server(config, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("a Quinn server endpoint");
    (endpoint, certificate)
}

fn quinn_client_endpoint(
    certificate: quinn::rustls::pki_types::CertificateDer<'static>,
) -> quinn::Endpoint {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(certificate)
        .expect("trust the benchmark certificate");
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![H3_ALPN.to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .expect("a QUIC client TLS configuration");
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("a Quinn client endpoint");
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    endpoint
}

async fn quinn_pair() -> (
    quinn::Connection,
    quinn::Connection,
    (quinn::Endpoint, quinn::Endpoint),
) {
    let (server, certificate) = quinn_server_endpoint();
    let address = server.local_addr().expect("the server address");
    let client = quinn_client_endpoint(certificate);

    let accepting = {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .accept()
                .await
                .expect("an incoming QUIC connection")
                .await
                .expect("an accepted QUIC connection")
        })
    };
    let connected = client
        .connect(address, "bench.local")
        .expect("start a QUIC connection")
        .await
        .expect("a connected QUIC client");
    let accepted = accepting.await.expect("the Quinn accept task");
    (connected, accepted, (client, server))
}

async fn ngnet_h3_echo(request: http::Request<H3IncomingBody>) -> http::Response<BenchBody> {
    response_for(collect(request.into_body()).await)
}

/// A persistent ngnet-h3 connection over the extracted Quinn adapter.
pub struct NgnetH3Quinn {
    handle: H3SendRequest<BenchBody>,
    client: JoinHandle<()>,
    server: JoinHandle<()>,
    _endpoints: (quinn::Endpoint, quinn::Endpoint),
}

impl NgnetH3Quinn {
    /// Establishes and warms a client/server pair outside the measured closure.
    pub async fn establish() -> Self {
        let (client_quic, server_quic, endpoints) = quinn_pair().await;
        let server_driver =
            h3_serve(QuinnBackend::new(server_quic), ngnet_h3_echo).expect("ngnet server");
        let server = tokio::spawn(async move {
            server_driver.await.expect("the ngnet server driver");
        });
        let (handle, client_driver) =
            h3_handshake::<_, BenchBody>(QuinnBackend::new(client_quic)).expect("ngnet client");
        let client = tokio::spawn(async move {
            client_driver.await.expect("the ngnet client driver");
        });

        let fixture = Self {
            handle,
            client,
            server,
            _endpoints: endpoints,
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        fixture
    }

    /// Sends one request and drains the echoed response body.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(quic_request_for(body))
            .await
            .expect("an ngnet response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }
}

impl Drop for NgnetH3Quinn {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
    }
}

/// A persistent ngnet-h3 connection over ngtcp2 and OpenSSL.
///
/// This is an end-to-end stack comparison with [`NgnetH3Quinn`], not an adapter-only
/// comparison: QUIC, TLS, endpoint driving, and transport integration all differ.
pub struct NgnetNgtcpH3 {
    handle: H3SendRequest<BenchBody>,
    client: JoinHandle<()>,
    server: JoinHandle<()>,
    client_endpoint_driver: JoinHandle<()>,
    server_endpoint_driver: JoinHandle<()>,
    _endpoints: (NgtcpEndpoint<OsslSession>, NgtcpEndpoint<OsslSession>),
}

impl NgnetNgtcpH3 {
    /// Establishes and warms an ngtcp2 client/server pair outside the measured closure.
    pub async fn establish() -> Self {
        let credentials = NgtcpCredentials::generate();
        let (server_endpoint, server_endpoint_driver, address) =
            ngtcp_server_endpoint(&credentials).await;
        let (client_endpoint, client_endpoint_driver) =
            ngtcp_client_endpoint(&credentials, 0xBEE5).await;

        let server_endpoint_driver = tokio::spawn(async move {
            server_endpoint_driver
                .await
                .expect("the ngtcp2 server endpoint driver");
        });
        let client_endpoint_driver = tokio::spawn(async move {
            client_endpoint_driver
                .await
                .expect("the ngtcp2 client endpoint driver");
        });

        let accepting = server_endpoint.clone();
        let server = tokio::spawn(async move {
            let backend = ngnet_quic_h3::accept(&accepting)
                .await
                .expect("an ngtcp2 server connection");
            let driver = h3_serve(backend, ngnet_h3_echo).expect("an ngtcp2 HTTP/3 server");
            driver.await.expect("the ngtcp2 HTTP/3 server driver");
        });

        let backend = ngnet_quic_h3::connect(&client_endpoint, address, Some(NGTCP_SERVER_NAME))
            .await
            .expect("an ngtcp2 client connection");
        let (handle, client_driver) =
            h3_handshake::<_, BenchBody>(backend).expect("an ngtcp2 HTTP/3 client");
        let client = tokio::spawn(async move {
            client_driver
                .await
                .expect("the ngtcp2 HTTP/3 client driver");
        });

        let fixture = Self {
            handle,
            client,
            server,
            client_endpoint_driver,
            server_endpoint_driver,
            _endpoints: (client_endpoint, server_endpoint),
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        fixture
    }

    /// Sends one request and drains the echoed response body.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(quic_request_for(body))
            .await
            .expect("an ngtcp2 response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// Sends one request and verifies the echoed response byte for byte while draining it.
    ///
    /// This is the fixed-count correctness-probe path. It deliberately compares streaming
    /// chunks in place rather than collecting another body-sized buffer.
    pub async fn round_trip_checked(&self, body: Bytes) -> (usize, bool) {
        self.try_round_trip_checked(body)
            .await
            .expect("an exact ngtcp2 round trip")
    }

    /// Sends one request and reports response-head/body failures without panicking.
    pub async fn try_round_trip_checked(
        &self,
        body: Bytes,
    ) -> Result<(usize, bool), CheckedFailure> {
        self.try_round_trip_checked_observed(body, &CheckedProgress::default())
            .await
    }

    /// Checked round trip that records its current application boundary.
    pub async fn try_round_trip_checked_observed(
        &self,
        body: Bytes,
        progress: &CheckedProgress,
    ) -> Result<(usize, bool), CheckedFailure> {
        progress.record(CheckedPhase::ResponseHead, 0, CheckedIntegrity::ExactSoFar);
        let expected = body.clone();
        let response = self
            .handle
            .send_request(quic_request_for(body))
            .await
            .map_err(|error| CheckedFailure::ngnet("ngtcp2 response head failed", error))?;
        if !response.status().is_success() {
            return Err(CheckedFailure::other(format!(
                "ngtcp2 response status was not successful: {}",
                response.status()
            )));
        }
        try_drain_checked_observed(&mut response.into_body(), &expected, Some(progress)).await
    }
}

impl Drop for NgnetNgtcpH3 {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
        self.client_endpoint_driver.abort();
        self.server_endpoint_driver.abort();
    }
}

// ---------------------------------------------------------------------------
// HTTP/3 over ngtcp2: ngnet-h3 against hyperium h3 + h3-ngnet-quic
//
// The matched pair. Both arms run the *same* QUIC transport -- the same
// `ngnet-quic-h3-tests` credentials, endpoints, ALPN, server name and `Config` -- so what
// differs is the HTTP/3 implementation and the adapter that joins it to the transport, and
// nothing else that could be held equal.
//
// `NgnetNgtcpH3` above is deliberately left alone so run 25's record stays reproducible;
// this pair adds a separately configured native arm instead.
//
// # The asymmetries that could not be removed
//
// These are listed here, next to the code, because a reader of the fixtures should not have
// to go looking for them. `docs/benchmarks/cases/quic-h3-comparison.md` carries the same list
// alongside the results.
//
// 1. **Where the HTTP/3 driving happens relative to the timed region.** `ngnet-h3` advances
//    its state machine in its spawned driver task; hyperium advances a request stream from
//    whichever task is polling it, which here is the task inside the measured closure. UDP I/O
//    is *not* asymmetric -- both arms hand that to the shared endpoint driver -- but the
//    h3-to-stream driving is, and that work lands inside the timed region on one side and
//    partly outside it on the other. This is inherent to comparing these two drivers.
// 2. **Two independently written QUIC pumps.** `ngnet-quic-h3`'s `pump`/`transmit` and
//    `h3-ngnet-quic`'s pump are separate implementations of the same idea. Differences between
//    them count as "the adapter", which is part of what is being measured, but they are not
//    differences in the HTTP/3 state machine.
// 3. **Hyperium clones its request handle per exchange**, because `SendRequest::send_request`
//    takes `&mut self`; the native handle does not need it. Already disclosed for the QMux
//    pair for the same reason.
// 4. **Hyperium has more await points inside the timed region**: `send_request`, `send_data`,
//    `finish`, `recv_response` and the `recv_data` loop, against the native arm's single
//    `send_request` plus `drain`.
// 5. **Body chunking granularity may differ** between the two HTTP/3 layers even for an
//    identical payload; neither layer exposes a control that would let this be equalised.
//
// One deliberate non-match, for completeness: the native config sets `max_concurrent_streams`
// and hyperium 0.0.8 has no equivalent setting. It does not reach the wire as a difference --
// concurrent streams are bounded by the QUIC transport's `MAX_STREAMS`, which is identical for
// both arms -- and the workload is serial anyway.
// ---------------------------------------------------------------------------

/// The native ngtcp2 arm, configured to match hyperium rather than to its own defaults.
///
/// Identical to [`NgnetNgtcpH3`] except that it goes through `handshake_with`/`serve_with`
/// with [`ngtcp_h3_matched_config`]. Without that, this arm would carry a 4 KiB QPACK dynamic
/// table its comparison partner cannot have.
pub struct NgnetNgtcpH3Matched {
    handle: H3SendRequest<BenchBody>,
    client: JoinHandle<()>,
    server: JoinHandle<()>,
    client_endpoint_driver: JoinHandle<()>,
    server_endpoint_driver: JoinHandle<()>,
    _endpoints: (NgtcpEndpoint<OsslSession>, NgtcpEndpoint<OsslSession>),
}

impl NgnetNgtcpH3Matched {
    /// Establishes and warms a matched native ngtcp2 pair outside the measured closure.
    pub async fn establish() -> Self {
        let credentials = NgtcpCredentials::generate();
        let (server_endpoint, server_endpoint_driver, address) =
            ngtcp_server_endpoint(&credentials).await;
        let (client_endpoint, client_endpoint_driver) =
            ngtcp_client_endpoint(&credentials, 0xBEE5).await;

        // One spawned endpoint driver per endpoint, exactly as the upstream arm spawns.
        let server_endpoint_driver = tokio::spawn(async move {
            server_endpoint_driver
                .await
                .expect("the ngtcp2 server endpoint driver");
        });
        let client_endpoint_driver = tokio::spawn(async move {
            client_endpoint_driver
                .await
                .expect("the ngtcp2 client endpoint driver");
        });

        let accepting = server_endpoint.clone();
        let server = tokio::spawn(async move {
            let backend = ngnet_quic_h3::accept(&accepting)
                .await
                .expect("an ngtcp2 server connection");
            let driver = h3_serve_with(backend, ngnet_h3_echo, ngtcp_h3_matched_config())
                .expect("a matched ngtcp2 HTTP/3 server");
            driver.await.expect("the ngtcp2 HTTP/3 server driver");
        });

        let backend = ngnet_quic_h3::connect(&client_endpoint, address, Some(NGTCP_SERVER_NAME))
            .await
            .expect("an ngtcp2 client connection");
        let (handle, client_driver) =
            h3_handshake_with::<_, BenchBody>(backend, ngtcp_h3_matched_config())
                .expect("a matched ngtcp2 HTTP/3 client");
        let client = tokio::spawn(async move {
            client_driver
                .await
                .expect("the ngtcp2 HTTP/3 client driver");
        });

        let fixture = Self {
            handle,
            client,
            server,
            client_endpoint_driver,
            server_endpoint_driver,
            _endpoints: (client_endpoint, server_endpoint),
        };
        // One empty warm-up, outside every measured closure, exactly as the upstream arm does.
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        fixture
    }

    /// Sends one request and drains the echoed response body.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(quic_request_for(body))
            .await
            .expect("an ngtcp2 response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// Sends one request and reports response-head/body failures without panicking.
    ///
    /// The supervised-probe path: a stall or a truncation has to be reportable, because the
    /// transport under both arms has a known unresolved large-body liveness defect and a probe
    /// that panicked would lose the evidence.
    pub async fn try_round_trip_checked(
        &self,
        body: Bytes,
    ) -> Result<(usize, bool), CheckedFailure> {
        self.try_round_trip_checked_observed(body, &CheckedProgress::default())
            .await
    }

    /// Checked round trip that records its current application boundary.
    pub async fn try_round_trip_checked_observed(
        &self,
        body: Bytes,
        progress: &CheckedProgress,
    ) -> Result<(usize, bool), CheckedFailure> {
        progress.record(CheckedPhase::ResponseHead, 0, CheckedIntegrity::ExactSoFar);
        let expected = body.clone();
        let response = self
            .handle
            .send_request(quic_request_for(body))
            .await
            .map_err(|error| CheckedFailure::ngnet("ngtcp2 response head failed", error))?;
        if !response.status().is_success() {
            return Err(CheckedFailure::other(format!(
                "ngtcp2 response status was not successful: {}",
                response.status()
            )));
        }
        try_drain_checked_observed(&mut response.into_body(), &expected, Some(progress)).await
    }
}

impl Drop for NgnetNgtcpH3Matched {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
        self.client_endpoint_driver.abort();
        self.server_endpoint_driver.abort();
    }
}

/// The hyperium request head, byte-identical to the native arm's.
///
/// Written out rather than borrowed from the QMux comparison: that one uses a different
/// scheme, and a `:scheme` that differs between arms is an unmatched variable in the one
/// place the comparison is about.
fn ngtcp_upstream_request_head() -> http::Request<()> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(QUINN_WORKLOAD_URI)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header("x-bench", "1")
        .body(())
        .expect("a well-formed HTTP/3 request")
}

/// The hyperium echo server, matched to `ngnet_h3_echo`.
async fn upstream_h3_ngtcp_server(connection: h3_ngnet_quic::Connection<OsslSession>) {
    let mut builder = h3::server::builder();
    builder.send_grease(false);
    builder.max_field_section_size(MAX_HEADER_LIST_SIZE as u64);
    let mut connection = builder
        .build::<_, Bytes>(connection)
        .await
        .expect("an upstream H3 ngtcp2 server");
    'requests: loop {
        let resolver = match connection.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) | Err(_) => return,
        };
        let (_request, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(_) => continue,
        };
        let mut body = BytesMut::new();
        loop {
            match stream.recv_data().await {
                Ok(Some(chunk)) => body.put(chunk),
                Ok(None) => break,
                Err(_) => continue 'requests,
            }
        }
        if stream
            .send_response(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/octet-stream")
                    .body(())
                    .expect("an upstream H3 ngtcp2 response"),
            )
            .await
            .is_err()
        {
            continue;
        }
        if !body.is_empty() && stream.send_data(body.freeze()).await.is_err() {
            continue;
        }
        let _ = stream.finish().await;
    }
}

/// Hyperium H3 over the same ngtcp2 transport, through `h3-ngnet-quic`.
pub struct UpstreamH3Ngtcp {
    sender: h3::client::SendRequest<h3_ngnet_quic::OpenStreams<OsslSession>, Bytes>,
    client: JoinHandle<()>,
    server: JoinHandle<()>,
    client_endpoint_driver: JoinHandle<()>,
    server_endpoint_driver: JoinHandle<()>,
    _endpoints: (NgtcpEndpoint<OsslSession>, NgtcpEndpoint<OsslSession>),
}

impl UpstreamH3Ngtcp {
    /// Establishes and warms a hyperium/ngtcp2 pair outside the measured closure.
    pub async fn establish() -> Self {
        let credentials = NgtcpCredentials::generate();
        let (server_endpoint, server_endpoint_driver, address) =
            ngtcp_server_endpoint(&credentials).await;
        let (client_endpoint, client_endpoint_driver) =
            ngtcp_client_endpoint(&credentials, 0xBEE5).await;

        let server_endpoint_driver = tokio::spawn(async move {
            server_endpoint_driver
                .await
                .expect("the ngtcp2 server endpoint driver");
        });
        let client_endpoint_driver = tokio::spawn(async move {
            client_endpoint_driver
                .await
                .expect("the ngtcp2 client endpoint driver");
        });

        // The adapter has no driver of its own, so this is one spawned task per endpoint
        // beyond the endpoint driver -- the same count as the native arm's HTTP/3 driver.
        let accepting = server_endpoint.clone();
        let server = tokio::spawn(async move {
            let detached = accepting
                .accept_detached()
                .await
                .expect("an ngtcp2 server connection");
            upstream_h3_ngtcp_server(h3_ngnet_quic::from_detached(detached)).await;
        });

        let detached = client_endpoint
            .connect_detached(address, Some(NGTCP_SERVER_NAME))
            .await
            .expect("an ngtcp2 client connection");
        let mut builder = h3::client::builder();
        builder.send_grease(false);
        builder.max_field_section_size(MAX_HEADER_LIST_SIZE as u64);
        let (mut driver, sender) = builder
            .build(h3_ngnet_quic::from_detached(detached))
            .await
            .expect("an upstream H3 ngtcp2 client");
        let client = tokio::spawn(async move {
            let _ = poll_fn(|context| driver.poll_close(context)).await;
        });

        let fixture = Self {
            sender,
            client,
            server,
            client_endpoint_driver,
            server_endpoint_driver,
            _endpoints: (client_endpoint, server_endpoint),
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        fixture
    }

    /// Sends one request and drains the echoed response body.
    ///
    /// The request handle is cloned per exchange because hyperium's `SendRequest::send_request`
    /// takes `&mut self`; the native arm's does not. That asymmetry cannot be removed and is
    /// disclosed with the results.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let mut sender = self.sender.clone();
        let mut stream = sender
            .send_request(ngtcp_upstream_request_head())
            .await
            .expect("an upstream ngtcp2 request stream");
        if !body.is_empty() {
            stream
                .send_data(body)
                .await
                .expect("upstream ngtcp2 request data");
        }
        stream
            .finish()
            .await
            .expect("finish upstream ngtcp2 request");
        let response = stream
            .recv_response()
            .await
            .expect("an upstream ngtcp2 response head");
        assert!(response.status().is_success());
        let mut total = 0;
        while let Some(chunk) = stream
            .recv_data()
            .await
            .expect("upstream ngtcp2 response data")
        {
            total += chunk.remaining();
        }
        total
    }

    /// Sends one request and reports failures without panicking, for supervised probes.
    pub async fn try_round_trip_checked(&self, body: Bytes) -> Result<(usize, bool), String> {
        self.try_round_trip_checked_observed(body, &CheckedProgress::default())
            .await
    }

    /// Checked round trip that records its current application boundary.
    pub async fn try_round_trip_checked_observed(
        &self,
        body: Bytes,
        progress: &CheckedProgress,
    ) -> Result<(usize, bool), String> {
        let expected = body.clone();
        let mut sender = self.sender.clone();
        let mut stream = sender
            .send_request(ngtcp_upstream_request_head())
            .await
            .map_err(|error| format!("upstream ngtcp2 request failed: {error:?}"))?;
        if !expected.is_empty() {
            stream
                .send_data(expected.clone())
                .await
                .map_err(|error| format!("upstream ngtcp2 request data failed: {error:?}"))?;
        }
        stream
            .finish()
            .await
            .map_err(|error| format!("upstream ngtcp2 finish failed: {error:?}"))?;
        progress.record(CheckedPhase::ResponseHead, 0, CheckedIntegrity::ExactSoFar);
        let response = stream
            .recv_response()
            .await
            .map_err(|error| format!("upstream ngtcp2 response head failed: {error:?}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "upstream ngtcp2 response status was not successful: {}",
                response.status()
            ));
        }
        let mut total = 0usize;
        let mut exact = true;
        let mut integrity = CheckedIntegrity::ExactSoFar;
        loop {
            progress.record(
                if total == expected.len() {
                    CheckedPhase::TerminalWait
                } else {
                    CheckedPhase::BodyDrain
                },
                total,
                integrity,
            );
            match stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    while chunk.has_remaining() {
                        let piece = chunk.chunk();
                        let end = total + piece.len();
                        if end > expected.len() {
                            exact = false;
                            integrity = CheckedIntegrity::LengthMismatch;
                        } else if piece != &expected[total..end] {
                            exact = false;
                            if integrity != CheckedIntegrity::LengthMismatch {
                                integrity = CheckedIntegrity::ContentMismatch;
                            }
                        }
                        total = end;
                        let taken = piece.len();
                        chunk.advance(taken);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    return Err(format!("upstream ngtcp2 response data failed: {error:?}"));
                }
            }
        }
        if total != expected.len() {
            exact = false;
        }
        progress.record(
            CheckedPhase::Complete,
            total,
            reported_integrity(total, expected.len(), integrity, true),
        );
        Ok((total, exact))
    }
}

impl Drop for UpstreamH3Ngtcp {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
        self.client_endpoint_driver.abort();
        self.server_endpoint_driver.abort();
    }
}

async fn upstream_h3_quinn_server(quic: quinn::Connection) {
    let mut connection = h3::server::Connection::<_, Bytes>::new(h3_quinn::Connection::new(quic))
        .await
        .expect("an upstream h3 server");
    loop {
        let Some(resolver) = connection.accept().await.expect("an upstream request") else {
            return;
        };
        let (_request, mut stream) = resolver
            .resolve_request()
            .await
            .expect("upstream request headers");
        let mut body = BytesMut::new();
        while let Some(chunk) = stream.recv_data().await.expect("upstream request data") {
            body.put(chunk);
        }
        stream
            .send_response(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/octet-stream")
                    .body(())
                    .expect("an upstream response"),
            )
            .await
            .expect("upstream response headers");
        if !body.is_empty() {
            stream
                .send_data(body.freeze())
                .await
                .expect("upstream response data");
        }
        stream.finish().await.expect("finish upstream response");
    }
}

/// A persistent upstream h3 + h3-quinn connection using the same Quinn setup.
pub struct UpstreamH3Quinn {
    sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    client: JoinHandle<()>,
    server: JoinHandle<()>,
    _endpoints: (quinn::Endpoint, quinn::Endpoint),
}

impl UpstreamH3Quinn {
    /// Establishes and warms a client/server pair outside the measured closure.
    pub async fn establish() -> Self {
        let (client_quic, server_quic, endpoints) = quinn_pair().await;
        let server = tokio::spawn(upstream_h3_quinn_server(server_quic));
        let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(client_quic))
            .await
            .expect("an upstream h3 client");
        let client = tokio::spawn(async move {
            let _ = poll_fn(|context| driver.poll_close(context)).await;
        });

        let fixture = Self {
            sender,
            client,
            server,
            _endpoints: endpoints,
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        fixture
    }

    /// Sends one request and drains the echoed response body.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let mut sender = self.sender.clone();
        let mut stream = sender
            .send_request(quinn_request_head())
            .await
            .expect("send an upstream request");
        if !body.is_empty() {
            stream.send_data(body).await.expect("upstream request data");
        }
        stream.finish().await.expect("finish upstream request");
        let response = stream
            .recv_response()
            .await
            .expect("an upstream response head");
        assert!(response.status().is_success());

        let mut total = 0;
        while let Some(chunk) = stream.recv_data().await.expect("upstream response data") {
            total += chunk.remaining();
        }
        total
    }
}

impl Drop for UpstreamH3Quinn {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
    }
}

/// A live HTTP/3-over-QMux client and server over one real loopback TCP connection.
///
/// The cross-protocol counterpart of [`TokioSocket`], and its mirror in every respect other
/// than the protocol stack: the socket pair comes from the same [`tokio_socket_pair`], so
/// `TCP_NODELAY` is set on both endpoints by the same code that sets it for the HTTP/2 arms,
/// and the runtime, workload, echo and drain are the ones every other arm uses.
pub struct NgnetQmuxH3Socket {
    handle: H3SendRequest<BenchBody>,
    server: JoinHandle<()>,
}

impl NgnetQmuxH3Socket {
    /// Binds, connects, accepts, spawns both drivers and warms the connection — all outside
    /// the measured closure. See [`TokioSocket::establish`], whose shape this follows, and
    /// [`qmux_warm_up`] for the one step that has no HTTP/2 counterpart.
    ///
    /// # Panics
    ///
    /// If the socket pair cannot be established, either end cannot be built, or the warm-up
    /// exchange fails.
    pub async fn establish() -> Self {
        let (client_io, server_io) = tokio_socket_pair().await;

        let server = qmux_serve_with(
            TokioStream::new(server_io),
            TokioClock::new(),
            qmux_h3_echo,
            qmux_config(),
            qmux_h3_config(),
        )
        .expect("a server connection");
        let server = tokio::spawn(async move {
            let _ = server.await;
        });

        let (handle, connection) = connect_with::<_, _, BenchBody>(
            TokioStream::new(client_io),
            TokioClock::new(),
            qmux_config(),
            qmux_h3_config(),
        )
        .expect("a client connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        qmux_warm_up(&handle).await;
        Self { handle, server }
    }

    /// One request, awaited to its response head and then drained. See
    /// [`NgnetQmuxH3::round_trip`].
    ///
    /// # Panics
    ///
    /// If the exchange fails. No body size is inadmissible; see the `const` assertion
    /// beside [`admit_concurrency`] for why that is a property of the constants, not of `body`.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a response head");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// `n` concurrent requests on the one connection. See [`NgnetQmuxH3::concurrent`].
    ///
    /// # Panics
    ///
    /// If `n` is inadmissible ([`admit_concurrency`]), or any exchange fails.
    pub async fn concurrent(&self, n: usize) {
        admit_concurrency(n);
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

    /// Takes the server away without telling the client. See [`NgnetQmuxH3::abandon_server`].
    pub fn abandon_server(&self) {
        self.server.abort();
    }
}

/// The ngnet H3-over-QMux socket fixture with QPACK disabled to match hyperium H3.
pub struct NgnetQmuxH3MatchedSocket {
    handle: H3SendRequest<BenchBody>,
    server: JoinHandle<()>,
    counters: BenchCounters,
}

impl NgnetQmuxH3MatchedSocket {
    /// Establishes and warms the matched ngnet socket fixture.
    pub async fn establish() -> Self {
        let (client_io, server_io) = tokio_socket_pair().await;
        let counters = BenchCounters::default();
        let server = qmux_serve_with(
            CountingStream::new(TokioStream::new(server_io), counters.clone()),
            TokioClock::new(),
            qmux_h3_echo,
            qmux_config(),
            qmux_h3_matched_config(),
        )
        .expect("a matched ngnet socket server");
        let server = tokio::spawn(run_counted_endpoint(
            async move {
                let _ = server.await;
            },
            counters.clone(),
        ));
        let (handle, connection) = connect_with::<_, _, BenchBody>(
            CountingStream::new(TokioStream::new(client_io), counters.clone()),
            TokioClock::new(),
            qmux_config(),
            qmux_h3_matched_config(),
        )
        .expect("a matched ngnet socket client");
        tokio::spawn(run_counted_endpoint(
            async move {
                let _ = connection.await;
            },
            counters.clone(),
        ));
        qmux_warm_up(&handle).await;
        tokio::task::yield_now().await;
        counters.reset_and_arm(false);
        Self {
            handle,
            server,
            counters,
        }
    }

    /// Sends one body and drains its exact echo.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .expect("a matched ngnet socket response");
        assert!(response.status().is_success());
        drain(response.into_body()).await
    }

    /// Sends one request and validates every echoed byte for diagnostic probes.
    pub async fn try_round_trip_checked(&self, body: Bytes) -> Result<(usize, bool), String> {
        let expected = body.clone();
        let response = self
            .handle
            .send_request(request_for(body))
            .await
            .map_err(|error| format!("matched ngnet socket response failed: {error:?}"))?;
        try_drain_checked(response.into_body(), &expected).await
    }

    /// Takes away the matched ngnet socket server.
    pub fn abandon_server(&self) {
        self.server.abort();
    }

    /// Enables a fresh symmetric counter interval.
    pub fn arm_counters(&self) {
        self.counters.reset_and_arm(true);
    }

    /// Current symmetric fixture counters.
    #[must_use]
    pub fn counter_snapshot(&self) -> BenchCounterSnapshot {
        self.counters.snapshot()
    }
}

async fn upstream_socket_pair() -> (UpstreamSocketSender, UpstreamQmuxTasks, BenchCounters) {
    let (client_io, server_io) = tokio_socket_pair().await;
    let counters = BenchCounters::default();
    let server_lower = ngnet_qmux::io::Connection::server(
        CountingStream::new(TokioStream::new(server_io), counters.clone()),
        TokioClock::new(),
        qmux_config(),
    )
    .expect("an upstream socket server QMux connection");
    let (server_connection, server_driver) =
        h3_ngnet_qmux::from_qmux(server_lower, UPSTREAM_QMUX_PENDING_ACCEPTS);
    let server = tokio::spawn(run_combined_endpoint(
        server_driver,
        upstream_h3_qmux_server(server_connection),
        counters.clone(),
    ));

    let client_lower = ngnet_qmux::io::Connection::client(
        CountingStream::new(TokioStream::new(client_io), counters.clone()),
        TokioClock::new(),
        qmux_config(),
    )
    .expect("an upstream socket client QMux connection");
    let (client_connection, client_driver) =
        h3_ngnet_qmux::from_qmux(client_lower, UPSTREAM_QMUX_PENDING_ACCEPTS);
    let (sender_tx, sender_rx) = tokio::sync::oneshot::channel();
    let client_counters = counters.clone();
    let client = tokio::spawn(async move {
        let mut adapter = Box::pin(client_driver);
        let mut builder = h3::client::builder();
        builder.send_grease(false);
        builder.max_field_section_size(MAX_HEADER_LIST_SIZE as u64);
        let building = builder.build(client_connection);
        tokio::pin!(building);
        let (mut h3, sender) = poll_fn(|cx| {
            client_counters.endpoint_poll();
            if let Poll::Ready(built) = building.as_mut().poll(cx) {
                return Poll::Ready(built);
            }
            let _ = adapter.as_mut().poll(cx);
            Poll::Pending
        })
        .await
        .expect("an upstream H3 socket client");
        sender_tx.send(sender).ok();
        run_combined_endpoint(
            adapter,
            async move {
                let _ = poll_fn(|context| h3.poll_close(context)).await;
            },
            client_counters,
        )
        .await;
    });
    let sender = sender_rx.await.expect("upstream H3 socket sender");
    (sender, UpstreamQmuxTasks { server, client }, counters)
}

/// A persistent hyperium H3-over-QMux loopback TCP fixture.
pub struct UpstreamH3QmuxSocket {
    sender: UpstreamSocketSender,
    tasks: UpstreamQmuxTasks,
    counters: BenchCounters,
}

impl UpstreamH3QmuxSocket {
    /// Establishes and warms the hyperium socket fixture.
    pub async fn establish() -> Self {
        let (sender, tasks, counters) = upstream_socket_pair().await;
        let fixture = Self {
            sender,
            tasks,
            counters,
        };
        assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
        tokio::task::yield_now().await;
        fixture.counters.reset_and_arm(false);
        fixture
    }

    /// Sends one body and drains its exact echo.
    pub async fn round_trip(&self, body: Bytes) -> usize {
        upstream_qmux_round_trip(&self.sender, body).await
    }

    /// Sends and drains one body while checking every echoed byte.
    pub async fn try_round_trip_checked(&self, body: Bytes) -> Result<(usize, bool), String> {
        upstream_qmux_round_trip_checked(&self.sender, body).await
    }

    /// Takes away both server-side socket drivers.
    pub fn abandon_server(&self) {
        self.tasks.abandon_server();
    }

    /// Enables a fresh symmetric counter interval.
    pub fn arm_counters(&self) {
        self.counters.reset_and_arm(true);
    }

    /// Current symmetric fixture counters.
    #[must_use]
    pub fn counter_snapshot(&self) -> BenchCounterSnapshot {
        self.counters.snapshot()
    }
}
