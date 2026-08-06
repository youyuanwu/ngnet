//! The no-copy sending path, end to end (Spec SC-001, SC-002, SC-007, SC-008, SC-012,
//! SC-013).
//!
//! The push-model send tests in `http_body_send.rs` prove a caller's body reaches the peer
//! under the caller's control. These prove the same of the *handed-over* body — the one
//! whose octets travel to the transport without being copied into libnghttp2's frame
//! buffer — reached through [`handshake_shared`](ngnet_h2::http::handshake_shared) and
//! [`serve_shared`](ngnet_h2::http::serve_shared) rather than the copying entry points.
//!
//! Most of what is asserted here is *fidelity, ordering and lifecycle*: the octets arrive
//! intact and in order, and a handed-over payload is released exactly once however the
//! stream ends. Two groups go further and assert the copy itself. The two-sided pointer
//! coverage of SC-001 shows the octets the transport is offered occupy the same memory the
//! caller supplied, and
//! `handing_a_body_over_collapses_the_write_count_on_the_gathering_path` pins the write-count
//! collapse that turned out to be the dominant mechanism behind the measured gain. (An
//! earlier version of this note said the file never asserted copy or allocation volume. That
//! was true when only Phase 2 had landed and stopped being true in Phase 3.)
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

use ngnet_h2::http::testing::{
    Duplex, DuplexReader, DuplexWriter, Empty, Failing, Full, Scripted, VectoredLog, alongside,
    block_on, bytes_crate as bytes, duplex, duplex_owned_regions, duplex_vectored, failing,
    failing_borrowed, failing_vectored, http_crate as http, scripted, serve,
};
use ngnet_h2::http::transport::{Transport, TransportWrite};
use ngnet_h2::http::{
    Error as HttpError, IncomingBody, ResponseFuture, SendRequest, handshake, handshake_shared,
};
use ngnet_h2::{
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
        ngnet_h2::http::handshake_shared::<_, B>(client_side).expect("handshake");
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
    // Spec SC-002, the fidelity criterion: the same octets, in the same order, whatever the body
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
// Pointer coverage — the SC-001 proof (design decision D8, instrument 2)
// ---------------------------------------------------------------------------

/// The shared handle through which a [`TrackingBody`]'s handed-over chunk ranges are read:
/// each entry is a `(base, len)` pair naming one caller allocation. Aliased so the coverage
/// assertion and the body's constructor name the same thing rather than repeating the nested
/// generic.
type Ranges = Arc<Mutex<Vec<(usize, usize)>>>;

/// A body that hands over `Bytes` and records the address range of every chunk it yields.
///
/// This is the instrument the no-copy proof is made with. The recorded ranges are caller
/// memory — the very allocations the body owns — so a transport region whose address falls
/// inside one of them is octets travelling to the wire *without a copy*. A region that landed
/// in the driver's own buffer instead would fall outside every range, and the coverage sum
/// would come up short. `Bytes` is a view over a stable heap allocation, so moving the value
/// into a frame does not move the octets: the pointer recorded here is the pointer the driver
/// slices its per-frame payloads from.
struct TrackingBody {
    chunks: std::collections::VecDeque<Bytes>,
    ranges: Ranges,
}

impl TrackingBody {
    /// A body over `chunks`, and the handle through which its handed-over ranges are read.
    fn new(chunks: Vec<Bytes>) -> (Self, Ranges) {
        let ranges = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                chunks: chunks.into(),
                ranges: Arc::clone(&ranges),
            },
            ranges,
        )
    }
}

impl Body for TrackingBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match self.chunks.pop_front() {
            Some(chunk) => {
                // Recorded before the chunk is moved into the frame; the address is the
                // allocation's, unchanged by the move.
                self.ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((chunk.as_ptr() as usize, chunk.len()));
                core::task::Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            None => core::task::Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// Uploads `body` over `client_side`, served by a live peer over `server_side`, and returns
/// what the peer received. The transport shape `client_side` was built with — vectored,
/// borrowed or owned — is what decides which write strategy the driver takes, so the same
/// helper drives all three.
fn upload_tracked(client_side: Duplex, server_side: Duplex, body: TrackingBody) -> ServerPeer {
    let (requests, connection) =
        ngnet_h2::http::handshake_shared::<_, TrackingBody>(client_side).expect("handshake");
    let response = requests.send_request(upload("/coverage", body));

    let exchange = async {
        let head = response.await.expect("a response");
        assert_eq!(head.status(), http::StatusCode::OK);
        for _ in 0..64 {
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

/// Asserts the two-sided no-copy proof over the regions a readiness transport logged.
///
/// A region is a *handed-over payload* exactly when its address range intersects caller
/// memory — a frame header (nine octets in the driver's record sink) or a serialised block
/// (the driver's gathering buffer) is a distinct allocation and intersects nothing here.
///
/// Both sides are required, and neither suffices alone:
///
/// * **Wholly inside (no copy).** Every payload region lies entirely within one caller chunk.
///   A region that merely *overlapped* a chunk would mean the driver had gathered caller
///   octets together with octets from elsewhere — a copy. Checking containment alone,
///   though, would pass a run that quietly dropped half the body.
/// * **Sums to the body (no drop).** The payload regions' lengths total the body length
///   exactly. This alone would pass a path that copied every octet into its own buffer and
///   never touched caller memory, since then there would be no payload regions to fall short
///   — which is why the containment side is needed beside it.
fn assert_pointer_coverage(
    log: &VectoredLog,
    ranges: &[(usize, usize)],
    body_len: usize,
    strategy: &str,
) {
    let calls = log.calls();
    let bases = log.bases();
    let mut covered = 0usize;
    for (lengths, addresses) in calls.iter().zip(&bases) {
        for (&len, &base) in lengths.iter().zip(addresses) {
            let start = base;
            let end = base + len;
            let intersects = ranges.iter().any(|&(chunk_start, chunk_len)| {
                start < chunk_start + chunk_len && chunk_start < end
            });
            if !intersects {
                continue;
            }
            let inside = ranges.iter().any(|&(chunk_start, chunk_len)| {
                start >= chunk_start && end <= chunk_start + chunk_len
            });
            assert!(
                inside,
                "{strategy}: a handed-over region [{start:#x}, {end:#x}) straddled the boundary of \
                 caller memory rather than lying wholly inside one chunk; the driver gathered \
                 caller octets together with octets from elsewhere, which is a copy",
            );
            covered += len;
        }
    }
    assert_eq!(
        covered, body_len,
        "{strategy}: the handed-over payload regions summed to {covered} octets, not the whole \
         {body_len}-octet body; some payload was copied through the driver's buffer rather than \
         handed over, or dropped",
    );
}

#[test]
fn the_readiness_paths_hand_over_the_whole_payload_from_caller_memory() {
    // Spec SC-001, the phase's headline: on a readiness transport the payload reaches the
    // wire uncopied. The proof is two-sided pointer coverage (design decision D8, instrument
    // 2), run against a transport electing the vectored strategy and again against one
    // electing the borrowed strategy: every region the transport was offered that touches
    // caller memory lies wholly inside it, and those regions account for the body in full.
    //
    // The body is handed over in three chunks of unequal, non-frame-aligned length, so the
    // driver's per-frame slicing crosses chunk boundaries and a chunk both larger and smaller
    // than a 16 KiB frame is exercised; their ranges are what the coverage is measured
    // against. `answer_at_once` lets the peer settle the response while the body is still in
    // flight, and its `WINDOW_UPDATE`s spread the upload over several passes — so the log
    // holds many gathering writes, not one, and the proof is over all of them.
    let chunks = [200_001usize, 4_096, 120_003];
    let body_len: usize = chunks.iter().sum();
    let make_body = || {
        let mut offset = 0u8;
        let pieces: Vec<Bytes> = chunks
            .iter()
            .map(|&len| {
                let piece: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_add(offset)).collect();
                offset = offset.wrapping_add(37);
                Bytes::from(piece)
            })
            .collect();
        let expected: Vec<u8> = pieces.iter().flat_map(|p| p.to_vec()).collect();
        let (body, ranges) = TrackingBody::new(pieces);
        (body, ranges, expected)
    };

    // Vectored: the payload regions are interleaved with header and gathered-run regions in
    // multi-region gathering writes.
    {
        let (client_side, server_side) = duplex_vectored();
        let log = client_side.vectored_log();
        let (body, ranges, expected) = make_body();
        let peer = upload_tracked(client_side, server_side, body);
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "the vectored path did not deliver the body intact",
        );
        assert_pointer_coverage(
            &log,
            &ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            body_len,
            "vectored",
        );
    }

    // Borrowed: each payload is its own single-region uncopied write, logged the same way.
    {
        let (client_side, server_side) = duplex(true);
        let log = client_side.vectored_log();
        let (body, ranges, expected) = make_body();
        let peer = upload_tracked(client_side, server_side, body);
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "the borrowed path did not deliver the body intact",
        );
        assert_pointer_coverage(
            &log,
            &ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            body_len,
            "borrowed",
        );
    }

    // Owned-region (completion): the payloads ride uncopied in a `Vec<Bytes>` gathering
    // write, interleaved with minted header regions and coalesced small-block runs — the
    // same two-sided proof as the vectored path, on the strategy a completion runtime takes.
    // This is design decision D8's instrument 2 retargeted to the owned path, the second
    // headline of the whole work: a completion transport gathers rather than coalesces.
    {
        let (client_side, server_side) = duplex_owned_regions();
        let log = client_side.vectored_log();
        let (body, ranges, expected) = make_body();
        let peer = upload_tracked(client_side, server_side, body);
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "the owned-region path did not deliver the body intact",
        );
        assert_pointer_coverage(
            &log,
            &ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            body_len,
            "owned-region",
        );
    }

    // Coalescing: asserted for byte fidelity only. This path copies the payload into the
    // owned buffer by construction, so pointer coverage does not apply — the point of the
    // contrast is that correctness holds on all three while only the first two are copy-free.
    {
        let (client_side, server_side) = duplex(false);
        let (body, _ranges, expected) = make_body();
        let peer = upload_tracked(client_side, server_side, body);
        assert_eq!(
            peer.bodies.get(&1).cloned().unwrap_or_default(),
            expected,
            "the coalescing path did not deliver the body intact",
        );
    }
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
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Full>(transport).expect("handshake");
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

/// A request whose head is large enough that its serialised `HEADERS` block clears the
/// vectored threshold, so a fast-path pass carries a genuine *large* block beside its small
/// `WINDOW_UPDATE`s and its handed-over records — the three-way mix design decision D9's
/// block-triggered mid-pass flush has to keep in order.
fn request_with_large_head<B>(body: B) -> http::Request<B> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri("http://example.test/ordering-mixed")
        .header("x-bulk", "z".repeat(1024))
        .body(body)
        .expect("building a request")
}

/// Drives the mixed exchange to completion, draining the response body so the client emits
/// `WINDOW_UPDATE`s of its own. Generic over the connection future so the copying and
/// handed-over entry points share it; `requests` is moved in and dropped last so the request
/// half stays open until the exchange is done.
fn drive_mixed(
    requests: SendRequest<Full>,
    connection: impl Future<Output = Result<(), HttpError>>,
    response: ResponseFuture,
    server_side: Duplex,
) {
    let exchange = async move {
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
}

/// Runs the mixed workload over the *vectored* transport and returns the octets it put on the
/// wire, captured through [`VectoredLog::octets`]. `shared` chooses the handed-over entry
/// point over the copying one; the workload, the peer and the driving are otherwise identical.
fn mixed_ordering_vectored_octets(shared: bool) -> Vec<u8> {
    let (client_side, server_side) = duplex_vectored();
    let log = client_side.vectored_log();
    let request = request_with_large_head(Full::new(payload(ORDERING_UPLOAD)));
    if shared {
        let (requests, connection) =
            ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
        let response = requests.send_request(request);
        drive_mixed(requests, connection, response, server_side);
    } else {
        let (requests, connection) =
            ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");
        let response = requests.send_request(request);
        drive_mixed(requests, connection, response, server_side);
    }
    log.octets()
}

#[test]
fn a_large_block_among_records_keeps_its_place_on_the_gathering_path() {
    // Spec SC-008, on the fast path this time. The test above proves ordering over the owned
    // Recording transport; this proves it where design decision D9 actually decides order —
    // the gathering path, whose block-triggered mid-pass flush is the one piece of ordering
    // logic the crate owns rather than inherits from libnghttp2. The workload is the mixed
    // one plus a large request head, so a single fast-path pass gathers all three region
    // kinds: runs of small `WINDOW_UPDATE` blocks, the large `HEADERS` block that forces an
    // immediate flush, and the handed-over `DATA` records between them.
    //
    // The oracle is again the *push* path — the same exchange copied into libnghttp2's
    // buffer, whose octet order is libnghttp2's alone — captured over the same vectored
    // duplex so the two wires are directly comparable. If the fast path reorders a record and
    // a block across a mid-pass flush, the two wires diverge.
    let oracle = mixed_ordering_vectored_octets(false);
    let fast = mixed_ordering_vectored_octets(true);

    assert!(
        !fast.is_empty(),
        "the fast path put nothing on the wire, so the comparison proved nothing",
    );
    assert_eq!(
        fast, oracle,
        "the gathering path put different octets on the wire than the copying path did for the \
         same mixed exchange; a record and a block swapped places across a mid-pass flush",
    );
}

#[test]
fn the_vectored_transport_carries_a_handed_over_body_intact() {
    // The gathering strategy writes its large blocks immediately and its small-block runs at
    // the end of a pass, interleaving the handed-over records between them as regions of one
    // uncopied gathering write rather than coalescing them. Driving a real body over the
    // vectored duplex is what exercises that interleaving; the body arriving intact is what
    // proves it preserved order.
    let expected = payload(200 * 1024);

    let (client_side, server_side) = duplex_vectored();
    let (requests, connection) =
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
    assert_eq!(error.kind(), ngnet_h2::http::ErrorKind::Body);
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Scripted>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
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
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
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
        ngnet_h2::http::ErrorKind::Transport,
        "a broken transport reported something else: {error}",
    );
    assert!(
        request_outcome.is_err(),
        "a request reported success though its body never reached the peer",
    );
}

/// Drives a handed-over body over a failing transport and returns the connection's verdict,
/// the request's verdict, and what the transport logged before it broke.
///
/// The three SC-013 fast-path tests differ only in which failing transport they run over, so
/// the capture dance — the connection surfaces the broken transport, the request only learns
/// its connection went away, so the connection's verdict is taken from a background future
/// while the request future is the main one — lives here once.
fn broken_fast_path_exchange(
    client_side: Failing,
    bytes: Bytes,
) -> (
    Result<(), HttpError>,
    Result<http::Response<IncomingBody>, HttpError>,
) {
    let (requests, connection) =
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(upload("/broken-fast-path", Full::new(bytes)));

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
    (connection_outcome, request_outcome)
}

#[test]
fn a_vectored_transport_failure_while_writing_a_payload_closes_the_connection_and_releases_it() {
    // Spec SC-013 on the *vectored* fast path. The test above drives the owned/coalescing
    // path only — `failing` hands the driver a transport that elects no fast path — so the
    // transport error it raises never travels through `flush_regions`, the code the vectored
    // and borrowed strategies share. `failing_vectored` fixes that: it fails while electing
    // the gathering write, so the error surfaces from the fast path.
    //
    // A 400 KiB body cannot leave in one write, so the first pass gathers a run of small
    // handshake blocks *with* the payload into one vectored write of many regions; failing
    // that first write lands the failure on a write genuinely carrying payload, which the
    // log assertion below confirms rather than assumes.
    let (bytes, alive) = witnessed(400 * 1024);
    assert_eq!(
        Arc::strong_count(&alive),
        2,
        "the witness should see the payload held before the exchange begins",
    );

    let (client_side, _server_side) = failing_vectored(1, false);
    let log = client_side.vectored_log();
    let (connection_outcome, request_outcome) = broken_fast_path_exchange(client_side, bytes);

    let error = connection_outcome.expect_err("a vectored transport that broke while writing");
    assert_eq!(
        error.kind(),
        ngnet_h2::http::ErrorKind::Transport,
        "a broken vectored transport reported something else: {error}",
    );
    assert!(
        request_outcome.is_err(),
        "a request reported success though its body never reached the peer",
    );

    // Proof the failure reached `flush_regions` with a populated region list: the transport
    // saw a single gathering write of more than one region, one of them a whole payload
    // frame. A failure on a bare handshake write would show neither.
    let calls = log.calls();
    assert!(
        calls
            .iter()
            .any(|regions| { regions.len() > 1 && regions.iter().any(|&len| len >= 16 * 1024) }),
        "the failing vectored write did not gather a payload region: {calls:?}",
    );

    // And the caller's `Bytes` is not retained past the connection's death. This holds
    // whether or not `flush_regions` disposed of its sink on the error exit — teardown
    // releases the sink either way — so it guards against a leak rather than proving *when*
    // the release happened; the driver unit tests pin the on-error disposal directly.
    assert_eq!(
        Arc::strong_count(&alive),
        1,
        "a broken vectored connection retained a handle to the caller's payload",
    );
}

#[test]
fn a_borrowed_transport_failure_while_writing_a_payload_closes_the_connection_and_releases_it() {
    // Spec SC-013 on the *borrowed* fast path, the vectored test's twin. The borrowed
    // strategy cannot gather, so `flush_regions` reaches it as one write per region: the run
    // of handshake blocks, then each `DATA` frame's header and payload on their own. The
    // fifth write is the first payload region, so failing there — rather than on the very
    // first handshake write — lands the failure on octets that are actually the body, which
    // the log assertion confirms.
    let (bytes, alive) = witnessed(400 * 1024);
    assert_eq!(
        Arc::strong_count(&alive),
        2,
        "the witness should see the payload held before the exchange begins",
    );

    let (client_side, _server_side) = failing_borrowed(5, false);
    let log = client_side.vectored_log();
    let (connection_outcome, request_outcome) = broken_fast_path_exchange(client_side, bytes);

    let error = connection_outcome.expect_err("a borrowed transport that broke while writing");
    assert_eq!(
        error.kind(),
        ngnet_h2::http::ErrorKind::Transport,
        "a broken borrowed transport reported something else: {error}",
    );
    assert!(
        request_outcome.is_err(),
        "a request reported success though its body never reached the peer",
    );

    // Proof the failing write carried payload: the borrowed path logs each uncopied write as
    // a single region, and the last one before the break is a whole payload frame, not a
    // handshake block. A borrowed write is never multi-region, so "carrying payload" is read
    // as a large final region rather than a gathered one.
    let calls = log.calls();
    let last = calls.last().expect("the transport wrote before it broke");
    assert_eq!(
        last.len(),
        1,
        "a borrowed write should offer exactly one region: {calls:?}",
    );
    assert!(
        last[0] >= 16 * 1024,
        "the failing borrowed write was not carrying a payload region: {calls:?}",
    );

    assert_eq!(
        Arc::strong_count(&alive),
        1,
        "a broken borrowed connection retained a handle to the caller's payload",
    );
}

// ---------------------------------------------------------------------------
// No bodies (SC-007)
// ---------------------------------------------------------------------------

/// Drives a bodyless client exchange to completion, returning the octets the peer received.
fn drive_nobody<Conn>(
    requests: ngnet_h2::http::SendRequest<Empty>,
    connection: Conn,
    server_side: Duplex,
) -> Vec<u8>
where
    Conn: core::future::Future<Output = Result<(), ngnet_h2::http::Error>>,
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
            ngnet_h2::http::handshake_shared::<_, Empty>(client_side).expect("handshake");
        drive_nobody(requests, connection, server_side)
    } else {
        let (requests, connection) =
            ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");
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
    // An equality between two counts is satisfied by `0 == 0`, so a counter that never fired
    // would pass this test having measured nothing at all. Establishing an exchange
    // allocates — the session, the buffers, the maps — so both arms must be non-zero for the
    // equality below to mean anything. `the_counter_notices_a_deliberate_allocation` pins the
    // counter itself; this pins that it was armed and firing across *these* two windows.
    assert!(
        push > 0 && shared > 0,
        "the allocation counter recorded nothing for either arm ({push}, {shared}), so the \
         equality below would hold vacuously",
    );
    assert_eq!(
        push, shared,
        "a bodyless no-copy exchange allocated differently from the copying one",
    );
}

#[test]
fn the_counter_notices_a_deliberate_allocation() {
    // The counter is a `#[global_allocator]` duplicated into this binary, and every
    // allocation assertion in this file rests on it firing. A counter wired up wrongly —
    // never armed, or armed on the wrong thread — would report zero for everything and make
    // those assertions pass vacuously. This proves it fires, in this binary, on this thread.
    // The same canary guards the sibling harness in `http_zero_alloc.rs`.
    let counted = count_allocations(|| {
        let boxed = Box::new([0u8; 64]);
        core::hint::black_box(&boxed);
    });
    assert!(
        counted >= 1,
        "the allocation counter must observe a deliberate heap allocation",
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
        let connection = ngnet_h2::http::serve_shared(server_side, move |_request| {
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

/// Uploads a body over a vectored duplex through the given entry point, and reports how many
/// writes the client half performed.
///
/// A macro rather than a function because `handshake` and `handshake_shared` return distinct
/// opaque connection futures, so the two cannot share one signature.
///
/// The two arms are identical but for the entry point, which is the same discipline the
/// benchmark arms follow: whatever differs in the count is the body strategy.
macro_rules! writes_for_upload {
    ($body:expr, $handshake:ident) => {{
        let (client_side, server_side) = duplex_vectored();
        let counter = client_side.write_counter();

        let (requests, connection) = $handshake::<_, Full>(client_side).expect("handshake");
        let response = requests.send_request(upload("/writes", Full::new($body)));

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

        counter.get()
    }};
}

/// Handing a body over collapses the write count on the gathering path, and that — not the
/// copy it also removes — is the larger part of why the readiness benchmark arms move.
///
/// This test exists because the benchmark result was *too good*. Removing the memset and the
/// source copy was measured, before any of this was built, to be worth at most about 5.8% of a
/// real-socket 1 MiB exchange. The readiness arms moved five times that, and a gain larger than
/// its stated mechanism is exactly the shape of a measurement artefact — so the mechanism had
/// to be found or the result discarded.
///
/// It is real, and it is a second prize nobody had costed. On the push path libnghttp2 hands
/// back one serialised block per `mem_send2` call, each a DATA frame header joined to its
/// 16 KiB payload, so a large upload is one write per frame. Handing the body over turns each
/// frame into two regions — a minted 9-byte header and the caller's own payload — which the
/// driver accumulates into a single gathering write.
///
/// The measured counts, and the real-socket readiness gain each corresponds to:
///
/// | body | push writes | shared writes | ratio | measured `tokio-shared` gain |
/// |------|-------------|---------------|-------|------------------------------|
/// | 0 B     | 1  | 1  | 1.0x | none (+1.0%, inside drift) |
/// | 1 KiB   | 2  | 1  | 2.0x | 35.3% |
/// | 64 KiB  | 5  | 2  | 2.5x | 25.4% |
/// | 1 MiB   | 65 | 17 | 3.8x | 30.6% |
///
/// The gain tracks the ratio and disappears exactly where the ratio is 1, which is what makes
/// this the explanation rather than a coincidence. It also explains why the *completion*
/// transport gains far less (about 4%): its push path already coalesced a whole pass into one
/// write, so there was never a syscall prize there — only the copy, which is what the original
/// estimate covered.
///
/// What bounds the batch at 1 MiB is *flow control*, not the region cap. The initial stream
/// window is 64 KiB, so a pass emits about four 16 KiB frames — eight regions — before waiting
/// for a `WINDOW_UPDATE`. That is comfortably under `MAX_REGIONS`, which is why the ratio is
/// 3.8x rather than the `MAX_REGIONS / 2` a cap-bound batch would give. The cap is a guard
/// rail here, not the binding constraint.
///
/// The assertions pin the ratios loosely and the 0 B control exactly: exact counts depend on
/// framing and windowing details that are libnghttp2's business, but the *shape* — no change
/// with no body, a growing collapse as the body grows — is the mechanism.
#[test]
fn handing_a_body_over_collapses_the_write_count_on_the_gathering_path() {
    // With no body there is nothing to hand over and nothing to batch, so the two paths must
    // agree exactly. This is the control: a difference here would mean everything below is
    // measuring something other than body handling.
    let empty_push = writes_for_upload!(Bytes::new(), handshake);
    let empty_shared = writes_for_upload!(Bytes::new(), handshake_shared);
    assert_eq!(
        empty_push, empty_shared,
        "with no body there is nothing to hand over, so both paths must write identically"
    );

    // Each larger body should collapse at least as much as the one below it.
    let mut previous_ratio = 1.0;
    for (size, least_ratio) in [(1024usize, 2.0), (64 * 1024, 2.0), (1024 * 1024, 3.0)] {
        let body = Bytes::from(payload(size));
        let push = writes_for_upload!(body.clone(), handshake);
        let shared = writes_for_upload!(body.clone(), handshake_shared);

        assert!(
            shared < push,
            "a {size}-byte body should need fewer gathering writes when handed over, but the \
             shared path took {shared} against the push path's {push}"
        );

        let ratio = push as f64 / shared as f64;
        assert!(
            ratio >= least_ratio,
            "a {size}-byte body should collapse the write count by at least {least_ratio}x, \
             but {push} push writes became {shared} shared ones, only {ratio:.1}x"
        );
        assert!(
            ratio >= previous_ratio,
            "the collapse should not weaken as bodies grow: a {size}-byte body managed only \
             {ratio:.1}x where a smaller one managed {previous_ratio:.1}x"
        );
        previous_ratio = ratio;
    }
}
