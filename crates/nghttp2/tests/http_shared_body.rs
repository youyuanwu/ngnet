//! The no-copy sending path, end to end (Spec SC-007, SC-008, SC-012, SC-013).
//!
//! The push-model send tests in `http_body_send.rs` prove a caller's body reaches the peer
//! under the caller's control. These prove the same of the *handed-over* body — the one
//! whose octets travel to the transport without being copied into libnghttp2's frame
//! buffer — reached through [`handshake_shared`](nghttp2::http::handshake_shared) and
//! [`serve_shared`](nghttp2::http::serve_shared) rather than the copying entry points.
//!
//! Phase 2 makes the payload arrive correctly; it does not yet make it arrive without a
//! copy into the driver's coalescing buffer (that is Phases 3–4). So every assertion here
//! is about *fidelity, ordering and lifecycle*, never about copy or allocation volume of
//! the payload itself — the one thing this phase deliberately does not promise.
//!
//! Everything runs on one task, as elsewhere in this suite: no runtime, no spawning.

#![cfg(feature = "http")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use core::future::Future;

use nghttp2::http::testing::{
    Duplex, DuplexReader, DuplexWriter, Empty, Full, Scripted, alongside, block_on,
    bytes_crate as bytes, duplex, duplex_vectored, failing, http_crate as http, scripted, serve,
};
use nghttp2::http::transport::{Transport, TransportWrite};
use nghttp2::http::{Error as HttpError, IncomingBody};
use nghttp2::{
    BytesBody, ErrorCode, FrameType, Header, HeaderAction, HeaderCategory, Session, SessionBuilder,
    StreamId,
};

use bytes::Bytes;
use http_body::{Body, Frame};

// ---------------------------------------------------------------------------
// A counting allocator, so "no bodies changes nothing" can be a measurement.
// ---------------------------------------------------------------------------

// Duplicated from `http_zero_alloc.rs` on purpose: a `#[global_allocator]` is a per-binary
// choice and cannot be shared between integration-test binaries. Counting is per-thread and
// armed explicitly, so a sibling test running in parallel cannot charge its allocations to
// this one's window, and every test that does not arm it is unaffected.
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

fn record_allocation() {
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        }
    });
}

// SAFETY: every method forwards to the system allocator unchanged; the counter is
// incidental and never affects the returned pointer.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `body`, counting the Rust allocations it makes on this thread.
fn count_allocations(body: impl FnOnce()) -> usize {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    body();
    COUNTING.with(|counting| counting.set(false));
    ALLOCATIONS.with(Cell::get)
}

/// The driver's vectored threshold, restated because it is not public API. A mismatch would
/// only weaken a size choice below, never make a test wrongly pass.
const VECTORED_THRESHOLD: usize = 256;

/// A payload with no repeating structure, so a misplaced or duplicated chunk shows up as a
/// mismatch rather than hiding behind a coincidence.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Yields once, so everything else on the task gets a full poll.
async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(move |cx| {
        if yielded {
            core::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// The peer, as a server: the client under test uploads, this records.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ServerPeer {
    paths: BTreeMap<i32, String>,
    pending: Vec<i32>,
    complete: Vec<i32>,
    bodies: BTreeMap<i32, Vec<u8>>,
    data_frames: BTreeMap<i32, usize>,
    trailers: BTreeMap<i32, Vec<(String, String)>>,
    opening: BTreeMap<i32, Vec<(String, String)>>,
    order: Vec<(i32, &'static str)>,
    closed: BTreeMap<i32, u32>,
}

fn server_peer() -> Session<ServerPeer> {
    SessionBuilder::<ServerPeer>::server()
        .on_begin_headers(|peer: &mut ServerPeer, frame| {
            if frame.is_trailers() {
                peer.opening.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|peer: &mut ServerPeer, frame, name: &[u8], value: &[u8]| {
            let stream = frame.stream_id().get();
            let name = String::from_utf8_lossy(name).into_owned();
            let value = String::from_utf8_lossy(value).into_owned();
            if let Some(fields) = peer.opening.get_mut(&stream) {
                fields.push((name, value));
            } else if name == ":path" {
                peer.paths.insert(stream, value);
            }
            HeaderAction::Continue
        })
        .on_data_chunk(|peer: &mut ServerPeer, stream, chunk: &[u8]| {
            peer.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_frame(|peer: &mut ServerPeer, frame| {
            let stream = frame.stream_id().get();
            if frame.kind() == FrameType::DATA {
                *peer.data_frames.entry(stream).or_default() += 1;
                peer.order.push((stream, "data"));
            }
            if frame.kind() == FrameType::HEADERS && frame.is_end_headers() {
                if frame.category() == Some(HeaderCategory::Request) {
                    peer.pending.push(stream);
                } else if let Some(fields) = peer.opening.remove(&stream) {
                    peer.trailers.insert(stream, fields);
                    peer.order.push((stream, "trailers"));
                }
            }
            if frame.is_end_stream() {
                peer.complete.push(stream);
            }
        })
        .on_stream_close(|peer: &mut ServerPeer, stream, code, _failure| {
            peer.closed.insert(stream.get(), code.get());
        })
        .build()
        .expect("building the peer session")
}

/// Answers each request the instant its head arrives, without waiting for the body — so a
/// response settles while the body is still being handed over or is still deferred.
fn answer_at_once(session: &mut Session<ServerPeer>, peer: &mut ServerPeer) {
    for stream in core::mem::take(&mut peer.pending) {
        let path = peer.paths.get(&stream).cloned().unwrap_or_default();
        session
            .submit_response(
                StreamId::new(stream),
                &[Header::new(":status", "200"), Header::new("x-path", &path)],
            )
            .expect("submitting a response");
    }
}

/// Answers only once a request has ended, so a body that never finishes never gets a
/// response — the only way a body failure can be observed through the response future.
fn answer_when_complete(session: &mut Session<ServerPeer>, peer: &mut ServerPeer) {
    let ready: Vec<i32> = core::mem::take(&mut peer.complete)
        .into_iter()
        .filter(|stream| peer.pending.contains(stream))
        .collect();
    for stream in ready {
        peer.pending.retain(|held| *held != stream);
        let path = peer.paths.get(&stream).cloned().unwrap_or_default();
        session
            .submit_response(
                StreamId::new(stream),
                &[Header::new(":status", "200"), Header::new("x-path", &path)],
            )
            .expect("submitting a response");
    }
}

/// Resets every pending request instead of answering it, so the client meets a stream that
/// vanished while its body was being handed over.
fn reset_pending(session: &mut Session<ServerPeer>, peer: &mut ServerPeer) {
    for stream in core::mem::take(&mut peer.pending) {
        session
            .reset_stream(StreamId::new(stream), ErrorCode::CANCEL)
            .expect("resetting a stream");
    }
}

fn upload<B>(path: &str, body: B) -> http::Request<B> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://example.test{path}"))
        .body(body)
        .expect("building a request")
}

// ---------------------------------------------------------------------------
// The peer, as a client: the server under test answers, this records.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ClientPeer {
    outgoing: Vec<String>,
    heads: BTreeMap<i32, Vec<(String, String)>>,
    opening: BTreeMap<i32, Vec<(String, String)>>,
    bodies: BTreeMap<i32, Vec<u8>>,
    trailers: BTreeMap<i32, Vec<(String, String)>>,
    trailers_after_data: std::collections::BTreeSet<i32>,
    closed: BTreeMap<i32, u32>,
}

impl ClientPeer {
    fn head(&self, stream: i32, name: &str) -> Option<&str> {
        self.heads
            .get(&stream)?
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

fn client_peer() -> Session<ClientPeer> {
    SessionBuilder::<ClientPeer>::client()
        .on_begin_headers(|peer: &mut ClientPeer, frame| {
            if frame.category() == Some(HeaderCategory::Response) || frame.is_trailers() {
                peer.opening.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|peer: &mut ClientPeer, frame, name: &[u8], value: &[u8]| {
            if let Some(fields) = peer.opening.get_mut(&frame.stream_id().get()) {
                fields.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
            }
            HeaderAction::Continue
        })
        .on_frame(|peer: &mut ClientPeer, frame| {
            if frame.kind() == FrameType::HEADERS && frame.is_end_headers() {
                if let Some(fields) = peer.opening.remove(&frame.stream_id().get()) {
                    let stream = frame.stream_id().get();
                    if frame.is_trailers() {
                        if peer
                            .bodies
                            .get(&stream)
                            .is_some_and(|body| !body.is_empty())
                        {
                            peer.trailers_after_data.insert(stream);
                        }
                        peer.trailers.insert(stream, fields);
                    } else {
                        peer.heads.insert(stream, fields);
                    }
                }
            }
        })
        .on_data_chunk(|peer: &mut ClientPeer, stream, chunk: &[u8]| {
            peer.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_stream_close(|peer: &mut ClientPeer, stream, code, _failure| {
            peer.closed.insert(stream.get(), code.get());
        })
        .build()
        .expect("building the peer session")
}

/// Opens whatever requests are queued, one bodyless GET per pass.
fn ask(session: &mut Session<ClientPeer>, peer: &mut ClientPeer) {
    for path in core::mem::take(&mut peer.outgoing) {
        let fields = [
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", path.as_str()),
        ];
        session.submit_request(&fields).expect("submitting");
    }
}

// ---------------------------------------------------------------------------
// Client-upload fidelity
// ---------------------------------------------------------------------------

/// Uploads `body` over a no-copy client and returns what the peer received.
fn upload_shared<B>(path: &str, body: B) -> ServerPeer
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, B>(client_side).expect("handshake");
    let response = requests.send_request(upload(path, body));

    let exchange = async {
        let head = response.await.expect("a response");
        assert_eq!(head.status(), http::StatusCode::OK);
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));
    peer
}

#[test]
fn a_handed_over_body_arrives_intact_at_every_boundary() {
    // Spec SC-008, the fidelity half: the same octets, in the same order, whatever the body
    // length does to the framing. Zero and one exercise the empty and single-octet edges; a
    // length just under the driver's vectored threshold, the two either side of the 16 KiB
    // maximum frame, and roughly a megabyte exercise frame-aligned, one-past-aligned and
    // many-frame bodies — the boundaries a copy-free hand-over is most likely to get wrong.
    for len in [
        0,
        1,
        VECTORED_THRESHOLD - 1,
        16383,
        16384,
        16385,
        1024 * 1024,
    ] {
        let expected = payload(len);
        let peer = upload_shared("/whole", Full::new(expected.clone()));
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "a {len}-octet handed-over body did not arrive intact",
        );
    }
}

#[test]
fn a_body_delivered_in_ragged_chunks_reassembles_exactly() {
    // The producer's chunk boundaries have nothing to do with the wire's frame boundaries,
    // and the leftover held between consultations is sliced with `Bytes::split_to` rather
    // than copied — so a chunk larger than a frame, and one straddling a frame edge, are the
    // cases most able to drop or duplicate an octet if the slicing arithmetic is wrong.
    let chunks = [
        1usize,
        VECTORED_THRESHOLD - 1,
        16384,
        16385,
        4096,
        100000,
        7,
    ];
    let mut expected = Vec::new();

    let (body, script) = scripted();
    for (index, len) in chunks.iter().enumerate() {
        let chunk = payload(*len)
            .into_iter()
            .map(|octet| octet.wrapping_add(index as u8))
            .collect::<Vec<u8>>();
        expected.extend_from_slice(&chunk);
        script.send(chunk);
    }
    script.finish();

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/ragged", body));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..32 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_when_complete),
    ));

    assert_eq!(
        peer.bodies.get(&1).cloned().unwrap_or_default(),
        expected,
        "a body delivered in ragged chunks did not reassemble",
    );
}

#[test]
fn the_copying_fallback_transport_still_delivers_every_octet() {
    // A transport that lends neither fast path takes ownership of what it is handed, so the
    // driver coalesces the whole pass — records included — into one owned write, paying one
    // copy. That copy is expected here and is not what this test is about: it pins that the
    // owned path is *correct* for a handed-over body, not that it is cheap. `duplex(false)`
    // is exactly that transport, and `upload_shared` runs over it, so a large multi-frame
    // body arriving intact is the whole assertion.
    let expected = payload(300 * 1024);
    let peer = upload_shared("/owned", Full::new(expected.clone()));
    assert_eq!(
        peer.bodies.get(&1).cloned().unwrap_or_default(),
        expected,
        "the owned coalescing path mangled a handed-over body",
    );
}

// ---------------------------------------------------------------------------
// Ordering (SC-008)
// ---------------------------------------------------------------------------

/// A transport that keeps the octets of every owned write, grouped by the write that
/// produced them.
///
/// It wraps an ordinary owned [`Duplex`] — the coalescing shape, which offers neither fast
/// path — and forwards every write on unchanged, so the connection it drives is a real one
/// talking to a real peer. All it adds is a tap: each owned write's octets are cloned into
/// `passes` before being handed on. On the owned shape the driver gathers a whole pass into
/// a single `write`, so one entry in `passes` is exactly one driver pass, in order. That is
/// what lets the ordering test speak about *passes* rather than only about the flat wire —
/// which frames the driver chose to emit together, and in what order within the pass.
///
/// Defined here rather than in `testing.rs` for the same reason `http_flush.rs` keeps its
/// `GatheringBuffer` local: it exists to make one point in one file, and the crate's public
/// testing surface is pinned by `compat_surface.rs`, not a place to add things casually.
struct Recording {
    inner: Duplex,
    passes: Rc<RefCell<Vec<Vec<u8>>>>,
}

struct RecordingWriter {
    inner: DuplexWriter,
    passes: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Recording {
    /// Wraps `inner`, returning the transport and a handle to the passes it will record.
    ///
    /// The handle is shared rather than reachable through the transport because
    /// [`Transport::split`] consumes the transport and moves the writer out of reach, and
    /// the recorded passes are exactly what the test must read afterwards.
    fn over(inner: Duplex) -> (Self, Rc<RefCell<Vec<Vec<u8>>>>) {
        let passes = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                inner,
                passes: Rc::clone(&passes),
            },
            passes,
        )
    }
}

impl Transport for Recording {
    type Reader = DuplexReader;
    type Writer = RecordingWriter;

    fn split(self) -> (DuplexReader, RecordingWriter) {
        let (reader, writer) = self.inner.split();
        (
            reader,
            RecordingWriter {
                inner: writer,
                passes: self.passes,
            },
        )
    }
}

impl TransportWrite for RecordingWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
        // Recorded before the write is forwarded, so the group order is the pass order.
        // Overriding neither `write_borrowed` nor `write_vectored` keeps the driver on the
        // owned strategy, where one call here is one whole pass — the property this tap
        // relies on to attribute frames to passes.
        self.passes.borrow_mut().push(buf.to_vec());
        self.inner.write(buf)
    }
}

/// The HTTP/2 connection preface, which opens the first pass and is not a frame.
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The frames in one recorded pass, as `(type, stream id, offset in the pass, payload len)`.
///
/// The offset is what lets the negative control lift a frame's exact octets back out to
/// reorder them. Parsing stops at the first short tail rather than panicking, since a pass
/// is always a whole number of frames and a short tail would be a bug worth surfacing as a
/// failed assertion downstream, not a parser panic here.
fn frames_in(pass: &[u8]) -> Vec<(u8, u32, usize, usize)> {
    let mut offset = if pass.starts_with(PREFACE) {
        PREFACE.len()
    } else {
        0
    };
    let mut frames = Vec::new();
    while pass.len() - offset >= 9 {
        let header = &pass[offset..];
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let kind = header[3];
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        if pass.len() - offset < 9 + len {
            break;
        }
        frames.push((kind, stream, offset, len));
        offset += 9 + len;
    }
    frames
}

/// Whether a frame is a handed-over payload rather than a serialised block.
///
/// On a no-copy client the only `DATA` it emits on the upload stream is the caller's own
/// `Bytes`, handed over uncopied — that is a payload (a record). Every other frame —
/// `SETTINGS`, its acknowledgement, the request `HEADERS`, the `WINDOW_UPDATE`s it sends as
/// it drains the response — is serialised by libnghttp2 into a block. Stream one is the
/// only request in this exchange, so `DATA` there is unambiguous.
fn is_payload(kind: u8, stream: u32) -> bool {
    kind == FrameType::DATA.get() && stream == 1
}

/// The upload body, and the peer's echoed response body.
///
/// The upload is several times the 65535-octet initial connection window, so it can only
/// leave as the peer grants more — which is what spreads it across many passes with the
/// peer's `WINDOW_UPDATE`s falling between them. The response body is what makes the client
/// emit `WINDOW_UPDATE`s *of its own*, interleaved with its upload: those are the serialised
/// blocks that turn up mid-stream, so the ordering rule is tested against blocks that share
/// a pass with payloads, not only against the handshake frames that all land up front.
const ORDERING_UPLOAD: usize = 512 * 1024;
const ORDERING_ECHO: usize = 300 * 1024;

/// Answers each request with a body of its own, so the client, draining that response,
/// emits `WINDOW_UPDATE`s interleaved with its upload.
fn answer_with_body(session: &mut Session<ServerPeer>, peer: &mut ServerPeer) {
    for stream in core::mem::take(&mut peer.pending) {
        let path = peer.paths.get(&stream).cloned().unwrap_or_default();
        session
            .submit_response_with_body(
                StreamId::new(stream),
                &[Header::new(":status", "200"), Header::new("x-path", &path)],
                BytesBody::new(vec![b'y'; ORDERING_ECHO]),
            )
            .expect("submitting a response with a body");
    }
}

/// Drains one frame from a response body, crediting the window as it goes.
async fn drain_frame(body: &mut IncomingBody) -> Option<Result<Frame<Bytes>, HttpError>> {
    core::future::poll_fn(|cx| core::pin::Pin::new(&mut *body).poll_frame(cx)).await
}

/// Runs the ordering exchange over the *push* path and returns the octets it put on the
/// wire, captured through [`VectoredLog::octets`].
///
/// This is the independent oracle. The push path copies each body chunk into libnghttp2's
/// serialisation buffer, so every octet it writes is a serialised block and the vectored
/// duplex's log sees all of them — a complete wire, produced by the copying code with no
/// no-copy machinery anywhere in it. The no-copy path is only correct if it reproduces this
/// exactly.
fn push_oracle_octets() -> Vec<u8> {
    let (client_side, server_side) = duplex_vectored();
    let log = client_side.vectored_log();
    let (requests, connection) =
        nghttp2::http::handshake::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/ordering", Full::new(payload(ORDERING_UPLOAD))));

    let exchange = async {
        let head = response.await.expect("a response");
        let mut body = head.into_body();
        while let Some(frame) = drain_frame(&mut body).await {
            frame.expect("a response body frame");
        }
        for _ in 0..64 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_with_body),
    ));
    log.octets()
}

/// Runs the *same* exchange over the no-copy path, returning the flat wire and the per-pass
/// octet groups the [`Recording`] transport captured.
fn shared_ordering_capture() -> (Vec<u8>, Vec<Vec<u8>>) {
    let (client_side, server_side) = duplex(false);
    let (transport, passes) = Recording::over(client_side);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Full>(transport).expect("handshake");
    let response = requests.send_request(upload("/ordering", Full::new(payload(ORDERING_UPLOAD))));

    let exchange = async {
        let head = response.await.expect("a response");
        let mut body = head.into_body();
        while let Some(frame) = drain_frame(&mut body).await {
            frame.expect("a response body frame");
        }
        for _ in 0..64 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_with_body),
    ));

    let passes = passes.borrow().clone();
    let wire: Vec<u8> = passes.iter().flatten().copied().collect();
    (wire, passes)
}

/// Rebuilds the wire with every pass's payloads hoisted ahead of its blocks.
///
/// This is the misordering the ordering rule forbids, made concrete: within each pass the
/// handed-over `DATA` is moved in front of the serialised frames it actually followed. For
/// the passes that carry both — the handshake pass, and every pass whose leading
/// `WINDOW_UPDATE`s precede its `DATA` — this genuinely reorders the octets; for a pass that
/// is all payload or all block it is a no-op. Feeding the result to the same comparison the
/// positive assertion uses is what proves that comparison can see a reordering at all,
/// rather than passing because records and blocks each parse as valid HTTP/2 in either
/// order — the circularity the previous version of this test fell into.
fn payloads_hoisted_ahead_of_blocks(passes: &[Vec<u8>]) -> Vec<u8> {
    let mut wire = Vec::new();
    for pass in passes {
        let mut payloads = Vec::new();
        let mut blocks = Vec::new();
        if pass.starts_with(PREFACE) {
            // The preface must lead; it is not a frame and cannot be reordered.
            wire.extend_from_slice(PREFACE);
        }
        for (kind, stream, offset, len) in frames_in(pass) {
            let frame = &pass[offset..offset + 9 + len];
            if is_payload(kind, stream) {
                payloads.extend_from_slice(frame);
            } else {
                blocks.extend_from_slice(frame);
            }
        }
        wire.extend_from_slice(&payloads);
        wire.extend_from_slice(&blocks);
    }
    wire
}

#[test]
fn payloads_and_serialised_blocks_keep_their_order_under_a_mixed_workload() {
    // Spec SC-008. The earlier form of this test drove a body through the no-copy path and
    // checked only that the peer reassembled the same octets. That was circular: records and
    // blocks are each whole frame sequences, so however they are interleaved the result
    // still parses as HTTP/2 and the body still reassembles — a reordering the guarantee
    // exists to forbid would pass unseen. So the oracle here is not this path's own output
    // but the *push* path's: the same logical exchange driven through the copying API, whose
    // octets libnghttp2 alone chose with no ordering decision of the crate's in them.
    //
    // The workload is a genuine mix. A 512 KiB upload cannot leave in one window, so it is
    // spread across many passes as the peer grants credit; the peer answers with a body of
    // its own, so the client emits `WINDOW_UPDATE`s as it drains that response, interleaved
    // with its upload. The wire therefore carries `SETTINGS` and its acknowledgement, the
    // request `HEADERS`, a stream of handed-over `DATA` payloads, and `WINDOW_UPDATE`s
    // scattered through the middle — blocks sharing passes with payloads, not only bunched
    // at the front.
    //
    // Three of the four control-frame kinds SC-008 names are exercised as serialised blocks
    // here: `SETTINGS`, `WINDOW_UPDATE` and `HEADERS`. `PING` is not, and cannot be: the
    // client emits a `PING` only if something submits one, and `Session` exposes no
    // submit-`PING` entry point, while this harness's oracle — the push path's
    // `VectoredLog` over a live peer — offers no seam at which to inject a raw `PING` frame
    // into the client's inbound. The gap costs the test nothing it is here to prove: the
    // oracle compares *every* octet in order, so a `PING` acknowledgement, were one present,
    // would be held to the same ordering as any other block. The mix's job is to put blocks
    // among payloads across many passes, which `WINDOW_UPDATE` does throughout the exchange;
    // which particular block kinds appear does not change what ordering rule is under test.
    let oracle = push_oracle_octets();
    let (wire, passes) = shared_ordering_capture();

    // The independent-oracle equivalence: the no-copy wire is the push wire, octet for
    // octet. No-copy changes who writes a payload, never what — or in what order — goes on
    // the wire.
    assert_eq!(
        wire, oracle,
        "the no-copy path put different octets on the wire than the push path did for the \
         same exchange; a record and a block must have swapped places",
    );

    // Classify each pass by how many payloads and how many blocks it carried, so the shape
    // assertions can fail loudly if the workload stops exercising the interleaving rather
    // than silently proving nothing.
    let shapes: Vec<(usize, usize)> = passes
        .iter()
        .map(|pass| {
            let mut payloads = 0;
            let mut blocks = 0;
            for (kind, stream, _, _) in frames_in(pass) {
                if is_payload(kind, stream) {
                    payloads += 1;
                } else {
                    blocks += 1;
                }
            }
            (payloads, blocks)
        })
        .collect();

    // Shape one: a pass that produced a serialised block and a payload together. This is the
    // case the session-level precedent
    // (`session.rs::records_and_a_block_from_one_call_match_the_push_path_octet_for_octet`)
    // pins at the call level; here it is the driver-pass level. libnghttp2 orders its
    // control frames ahead of `DATA` within a pass, so these passes are a block then its
    // payloads — the ordering that must survive the hand-over.
    let block_with_payload = shapes.iter().any(|(p, b)| *p >= 1 && *b >= 1);
    assert!(
        block_with_payload,
        "no pass carried a serialised block and a payload together, so the in-pass ordering \
         rule was never exercised; shapes were {shapes:?}",
    );

    // Shape two: several payloads produced, then a block — observed across a pass boundary,
    // because within a pass libnghttp2 never emits a payload ahead of a block. A pass that
    // is nothing but payloads, followed later by a pass carrying a block, is that shape: the
    // records ran ahead, and a block still landed after them without overtaking any.
    let payloads_then_block = shapes
        .iter()
        .enumerate()
        .any(|(index, (payloads, blocks))| {
            *payloads >= 2
                && *blocks == 0
                && shapes[index + 1..].iter().any(|(_, later)| *later >= 1)
        });
    assert!(
        payloads_then_block,
        "no run of several payloads was followed by a later block, so the across-boundary \
         ordering rule was never exercised; shapes were {shapes:?}",
    );

    // Negative control. Hoisting each pass's payloads ahead of its blocks is the misordering
    // the rule forbids; that the reconstructed wire no longer matches the oracle is what
    // proves the equivalence assertion above can in fact see a reordering, and is not
    // passing merely because both orders parse.
    let misordered = payloads_hoisted_ahead_of_blocks(&passes);
    assert_ne!(
        misordered, oracle,
        "moving payloads ahead of the blocks they followed still matched the push oracle; \
         the equivalence assertion cannot be detecting order and is testing nothing",
    );
}

#[test]
fn the_vectored_transport_carries_a_handed_over_body_intact() {
    // The gathering strategy writes its blocks *immediately* and its coalescing buffer only
    // at the end of a pass, so a record it must interleave forces the pass onto the
    // coalescing path. Driving a real body over the vectored duplex is what exercises that
    // switch; the body arriving intact is what proves the switch preserved order.
    let expected = payload(200 * 1024);

    let (client_side, server_side) = duplex_vectored();
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/vectored", Full::new(expected.clone())));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..32 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.bodies.get(&1).cloned().unwrap_or_default(),
        expected,
        "the vectored transport mangled a handed-over body",
    );
}

// ---------------------------------------------------------------------------
// Deferral, trailers, failure, reset
// ---------------------------------------------------------------------------

#[test]
fn a_deferred_handed_over_body_resumes_and_arrives() {
    // The deferral bridge is the same code the push path uses, so this need not re-prove
    // that a parked body emits no frames — `http_body_send.rs` does — only that a no-copy
    // body parks and then, once woken with content, hands it over correctly.
    let (body, script) = scripted();

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/deferred", body));

    let first = payload(4096);
    let second = payload(20000);
    let mut expected = first.clone();
    expected.extend_from_slice(&second);

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..8 {
            yield_now().await;
        }
        assert!(script.is_deferred(), "the body never parked");
        script.send(first.clone());
        for _ in 0..8 {
            yield_now().await;
        }
        script.send(second.clone());
        script.finish();
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.bodies.get(&1).cloned().unwrap_or_default(),
        expected,
        "a resumed handed-over body did not arrive intact",
    );
}

#[test]
fn trailers_follow_the_final_handed_over_data() {
    // Spec SC-008, trailer ordering. `http_body` yields trailers after the last data frame,
    // and the wire requires the same order; the no-copy adapter learns the trailers one
    // consultation *after* the data, exactly as the push adapter does, so the block must
    // still land after every `DATA`.
    let (body, script) = scripted();
    script.send(payload(9000));
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-checksum", http::HeaderValue::from_static("42"));
    script.finish_with_trailers(trailers);

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/trailed", body));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..24 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.trailers.get(&1).map(Vec::as_slice).unwrap_or_default(),
        &[("x-checksum".to_owned(), "42".to_owned())],
        "the trailing block did not arrive",
    );
    let data_then_trailers = peer
        .order
        .iter()
        .filter(|(stream, _)| *stream == 1)
        .collect::<Vec<_>>();
    assert_eq!(
        data_then_trailers.last(),
        Some(&&(1, "trailers")),
        "trailers did not follow the final data frame",
    );
}

#[test]
fn a_failing_source_resets_the_stream_and_surfaces_its_error() {
    // Spec SC-013's sibling for a *source* failure rather than a transport one: the caller's
    // own body reported an error, so the stream must go and the error must reach the caller
    // as the cause rather than a printed rendering of it.
    let (body, script) = scripted();
    script.send(payload(200));
    script.fail("the disk went away");

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/failing", body));

    let exchange = async {
        let outcome = response.await;
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
        outcome
    };

    let mut peer = ServerPeer::default();
    let outcome = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_when_complete),
    ));

    let error = outcome.expect_err("a body that failed");
    assert_eq!(error.kind(), nghttp2::http::ErrorKind::Body);
    let cause = std::error::Error::source(&error).expect("the originating error");
    assert!(
        cause.to_string().contains("the disk went away"),
        "the caller's own error did not survive: {cause}",
    );
    assert!(
        peer.closed.contains_key(&1),
        "the peer never saw the stream close",
    );
}

#[test]
fn a_stream_reset_while_handing_over_a_body_is_observed() {
    // The peer resets each request instead of answering it, so the reset lands while the
    // body is still being handed over. The client must notice the stream is gone rather than
    // going on offering payload for a stream libnghttp2 has already closed.
    let (body, script) = scripted();
    script.send(payload(400 * 1024));

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/reset", body));

    let exchange = async {
        let outcome = response.await;
        for _ in 0..32 {
            yield_now().await;
        }
        drop(requests);
        outcome
    };

    let mut peer = ServerPeer::default();
    let outcome = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, reset_pending),
    ));

    outcome.expect_err("a stream the peer reset");
    assert!(
        peer.closed.contains_key(&1),
        "the peer never recorded the reset stream closing",
    );
}

// ---------------------------------------------------------------------------
// Lifecycle (SC-012)
// ---------------------------------------------------------------------------

/// Octets whose liveness is observable through a strong count.
///
/// The payload is a [`Bytes`] built with [`Bytes::from_owner`] over one of these; the owner
/// holds a clone of `alive`, so while any handle to the payload is live the count is two —
/// the test's and the owner's — and it falls to one the instant the last handle is dropped.
/// Every `Bytes` clone shares the one owner, so the count cannot distinguish two crate
/// handles from one: it reports *whether* the crate is still holding, not *how many*. What
/// makes it a witness of release rather than a snapshot is reading it at both ends of the
/// hand-over — two while held, one once released — and, for the write and reset cases,
/// reading the released end while the connection is still alive, so a payload merely carried
/// to teardown cannot pass for one let go on time.
struct Witness {
    data: Vec<u8>,
    _alive: Arc<()>,
}

impl AsRef<[u8]> for Witness {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

fn witnessed(len: usize) -> (Bytes, Arc<()>) {
    let alive = Arc::new(());
    let bytes = Bytes::from_owner(Witness {
        data: payload(len),
        _alive: Arc::clone(&alive),
    });
    (bytes, alive)
}

#[test]
fn a_handed_over_payload_is_released_once_written() {
    // Spec SC-012, the ordinary case. The earlier form asserted the count was one only
    // *after* the whole connection had been driven to completion and dropped — a point at
    // which a payload retained until teardown is indistinguishable from one released on
    // write, since both leave the witness at one. Here the count starts at two (held), and
    // the return to one is read from inside the exchange, while the connection future and
    // the request handle are both still alive: the crate can only have reached one by
    // letting the payload go when it wrote it, not by being torn down. The body-arrival
    // check afterwards confirms the release was asserted against a body that really was
    // written, not one that never left.
    let (bytes, alive) = witnessed(200 * 1024);
    assert_eq!(
        Arc::strong_count(&alive),
        2,
        "the witness should see the payload held before the exchange begins",
    );

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/released", Full::new(bytes)));

    let exchange = async {
        response.await.expect("a response");
        // A fixed, generous run rather than an early break: breaking the instant the count
        // reached one would return from the main future before the peer had drained the
        // last octets still buffered in the pipe, since `alongside` stops as soon as its
        // main future finishes. These yields let the whole body reach the peer *and* the
        // source be dropped at stream close, all while the connection future and `requests`
        // are still held, so the count read next is an in-flight release, not a torn-down
        // connection.
        for _ in 0..64 {
            yield_now().await;
        }
        assert_eq!(
            Arc::strong_count(&alive),
            1,
            "the crate kept a handle to a payload it had already written",
        );
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.bodies.get(&1).map(Vec::len).unwrap_or_default(),
        200 * 1024,
        "the body the release was asserted against did not fully arrive",
    );
}

#[test]
fn a_payload_is_released_when_its_stream_is_reset_before_it_is_sent() {
    // Spec SC-012's reset clause: a stream reset while a body is mid-flight must release
    // every payload the crate is holding — staged, buffered as leftover, or sitting in the
    // record sink. The peer resets the stream instead of answering it; the source is then
    // dropped at stream close, which the driver processes while the connection is still
    // running. Reading the count back to one from inside the exchange, before `requests` is
    // dropped, witnesses that release at the reset rather than at teardown.
    let (bytes, alive) = witnessed(400 * 1024);
    assert_eq!(
        Arc::strong_count(&alive),
        2,
        "the witness should see the payload held before the exchange begins",
    );
    let (body, script) = scripted();
    script.send(bytes);
    script.finish();

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
    let response = requests.send_request(upload("/reset-release", body));

    let exchange = async {
        let _ = response.await;
        for _ in 0..256 {
            if Arc::strong_count(&alive) == 1 {
                break;
            }
            yield_now().await;
        }
        assert_eq!(
            Arc::strong_count(&alive),
            1,
            "a reset stream leaked the payload it was handing over",
        );
        drop(requests);
    };

    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, reset_pending),
    ));
}

#[test]
fn a_torn_down_connection_releases_a_payload_it_never_finished_sending() {
    // Spec SC-012's last clause, and the case the earlier suite was missing entirely: a
    // connection torn down with payload still unwritten must release it, not carry it into
    // the grave. A body several times the initial flow-control window cannot be written in
    // one pass, and a transport that fails on its very first write tears the connection down
    // with the source still holding the unsent remainder — the driver's record sink is
    // cleared on the error exit (design decision D3) and the source is dropped as the whole
    // machine unwinds. The witness starts at two (held), and its return to one after the
    // connection has failed is what proves the teardown released the caller's own `Bytes`
    // rather than leaking it past the connection's death; that the write failed, and no
    // octet reached a peer, is what proves the payload really was outstanding at that point.
    let (bytes, alive) = witnessed(400 * 1024);
    assert_eq!(
        Arc::strong_count(&alive),
        2,
        "the witness should see the payload held before the exchange begins",
    );

    let (client_side, _server_side) = failing(1, false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/torn", Full::new(bytes)));

    // As in the write-failure test: the connection future is what surfaces the broken
    // transport, so its verdict is captured out of a background future while the request
    // future is the main one.
    let captured = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);
    let driven = async move {
        *sink.lock().expect("verdict") = Some(connection.await);
    };
    let exchange = async {
        let outcome = response.await;
        drop(requests);
        outcome
    };

    let request_outcome = block_on(alongside(exchange, driven));
    let connection_outcome = captured
        .lock()
        .expect("verdict")
        .take()
        .expect("connection ended");

    connection_outcome.expect_err("a transport that broke before the body was written");
    assert!(
        request_outcome.is_err(),
        "a request whose body never reached the peer reported success",
    );
    assert_eq!(
        Arc::strong_count(&alive),
        1,
        "a torn-down connection leaked the payload it had not finished sending",
    );
}

// ---------------------------------------------------------------------------
// Transport write failure (SC-013)
// ---------------------------------------------------------------------------

#[test]
fn a_transport_failure_while_writing_a_payload_closes_the_connection() {
    // Spec SC-013. A body larger than one write forces a second write to carry payload, and
    // the transport is made to fail on it. The connection must report the transport failure,
    // and the request must not settle as a success — its octets never reached the peer.
    let (client_side, _server_side) = failing(1, false);
    let (requests, connection) =
        nghttp2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/broken", Full::new(payload(400 * 1024))));

    // The connection is the future that surfaces a broken transport; the request future only
    // ever learns its connection went away. Both are needed, so the connection's verdict is
    // captured out of a background future while the request future is awaited as the main.
    let captured = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);
    let driven = async move {
        *sink.lock().expect("verdict") = Some(connection.await);
    };
    let exchange = async {
        let outcome = response.await;
        drop(requests);
        outcome
    };

    let request_outcome = block_on(alongside(exchange, driven));
    let connection_outcome = captured
        .lock()
        .expect("verdict")
        .take()
        .expect("connection ended");

    let error = connection_outcome.expect_err("a transport that broke while writing");
    assert_eq!(
        error.kind(),
        nghttp2::http::ErrorKind::Transport,
        "a broken transport reported something else: {error}",
    );
    assert!(
        request_outcome.is_err(),
        "a request reported success though its body never reached the peer",
    );
}

// ---------------------------------------------------------------------------
// No bodies (SC-007)
// ---------------------------------------------------------------------------

/// Drives a bodyless client exchange to completion, returning the octets the peer received.
fn drive_nobody<Conn>(
    requests: nghttp2::http::SendRequest<Empty>,
    connection: Conn,
    server_side: Duplex,
) -> Vec<u8>
where
    Conn: core::future::Future<Output = Result<(), nghttp2::http::Error>>,
{
    let response = requests.send_request(
        http::Request::builder()
            .uri("http://example.test/nobody")
            .body(Empty)
            .expect("a request"),
    );
    let exchange = async {
        response.await.expect("a response");
        for _ in 0..8 {
            yield_now().await;
        }
        drop(requests);
    };
    let mut peer = ServerPeer::default();
    block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, server_peer(), &mut peer, answer_at_once),
    ));
    peer.bodies.get(&1).cloned().unwrap_or_default()
}

/// Drives a bodyless client exchange over the vectored transport, returning the number of
/// write calls the peer half saw and the octets it received.
fn nobody_exchange(shared: bool) -> (usize, Vec<u8>) {
    let (client_side, server_side) = duplex_vectored();
    let log = client_side.vectored_log();

    let received = if shared {
        let (requests, connection) =
            nghttp2::http::handshake_shared::<_, Empty>(client_side).expect("handshake");
        drive_nobody(requests, connection, server_side)
    } else {
        let (requests, connection) =
            nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");
        drive_nobody(requests, connection, server_side)
    };

    let writes = log.calls().len() - log.retries();
    (writes, received)
}

#[test]
fn a_connection_sending_no_bodies_is_indistinguishable_from_the_copying_one() {
    // Spec SC-007. With no bodies the no-copy path never constructs a source and the flush
    // loop never enters its record branch, so it must run byte-for-byte and write-for-write
    // like the copying path. Comparing the two directly is the strongest statement of "no
    // change": anything the shared path did differently would show here.
    let (push_writes, push_octets) = nobody_exchange(false);
    let (shared_writes, shared_octets) = nobody_exchange(true);

    assert_eq!(
        shared_octets,
        Vec::<u8>::new(),
        "a bodyless request carried a body"
    );
    assert_eq!(
        push_octets, shared_octets,
        "the wire octets differed with no bodies"
    );
    assert_eq!(
        push_writes, shared_writes,
        "the no-copy path changed the write count of a bodyless exchange",
    );
}

#[test]
fn no_bodies_allocates_no_differently_on_the_no_copy_path() {
    // Spec SC-007, the allocation half. The whole exchange is measured both ways — setup
    // included, because it is identical either way — so a difference could only come from
    // the sending path. With no bodies there is nothing for the no-copy path to do
    // differently, so the two counts must match exactly.
    let push = count_allocations(|| {
        let _ = nobody_exchange(false);
    });
    let shared = count_allocations(|| {
        let _ = nobody_exchange(true);
    });
    assert_eq!(
        push, shared,
        "a bodyless no-copy exchange allocated differently from the copying one",
    );
}

// ---------------------------------------------------------------------------
// Server responses (serve_shared)
// ---------------------------------------------------------------------------

#[test]
fn a_no_copy_server_hands_its_response_body_back_intact() {
    // The server counterpart: `serve_shared` hands each response body over uncopied, and a
    // peer client reading it back must see exactly what the handler produced, across the
    // same frame boundaries the client test covers.
    for len in [0, 1, 16384, 16385, 200 * 1024] {
        let expected = payload(len);
        let answer = expected.clone();

        let (server_side, client_side) = duplex(false);
        let body = answer.clone();
        let connection = nghttp2::http::serve_shared(server_side, move |_request| {
            let body = body.clone();
            async move {
                http::Response::builder()
                    .status(200)
                    .body(Full::new(body))
                    .expect("a response")
            }
        })
        .expect("serving");

        let mut peer = ClientPeer::default();
        peer.outgoing.push("/answer".to_owned());

        let driving = async {
            for _ in 0..48 {
                yield_now().await;
            }
        };

        block_on(alongside(
            alongside(driving, connection),
            serve(client_side, client_peer(), &mut peer, ask),
        ));

        assert_eq!(peer.head(1, ":status"), Some("200"));
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "a {len}-octet handed-over response body did not arrive intact",
        );
    }
}
