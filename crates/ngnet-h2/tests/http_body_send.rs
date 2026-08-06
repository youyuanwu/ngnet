//! Sending a message body: deferral without spinning, trailers, and failures.
//!
//! The receive side proves octets arrive; this proves they leave under the caller's
//! control rather than the connection's. Three properties matter and none is visible from
//! the outside without a peer that counts frames: a body that is not ready must produce
//! *no* frames and *no* further questions, a body must never be read ahead of what it was
//! asked for, and a body that fails must take its stream down and say why.
//!
//! Everything runs on one task, as elsewhere in this suite: no runtime, no spawning.

#![cfg(feature = "http")]

use std::collections::BTreeMap;

use ngnet_h2::http::testing::{
    Empty, Full, Scripted, alongside, block_on, buffered_chunks, duplex, http_crate as http,
    scripted, serve,
};
use ngnet_h2::{FrameType, Header, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId};

// ---------------------------------------------------------------------------
// The peer
// ---------------------------------------------------------------------------

/// What the peer server observed of each request.
#[derive(Debug, Default)]
struct Peer {
    paths: BTreeMap<i32, String>,
    /// Streams whose request head is complete and which have not been answered.
    pending: Vec<i32>,
    /// Streams whose request has ended.
    complete: Vec<i32>,
    /// Payload received, by stream.
    bodies: BTreeMap<i32, Vec<u8>>,
    /// How many `DATA` frames arrived, by stream — including empty ones, which is the
    /// whole point of counting frames rather than octets.
    data_frames: BTreeMap<i32, usize>,
    /// Trailing header blocks received, by stream.
    trailers: BTreeMap<i32, Vec<(String, String)>>,
    /// Trailing blocks still arriving, by stream.
    opening: BTreeMap<i32, Vec<(String, String)>>,
    /// What arrived, in order, so trailers can be shown to follow the final data.
    order: Vec<(i32, &'static str)>,
    /// Streams the peer reset, or that were reset on it.
    closed: BTreeMap<i32, u32>,
}

fn peer_session() -> Session<Peer> {
    SessionBuilder::<Peer>::server()
        .on_begin_headers(|peer: &mut Peer, frame| {
            if frame.is_trailers() {
                peer.opening.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|peer: &mut Peer, frame, name: &[u8], value: &[u8]| {
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
        .on_data_chunk(|peer: &mut Peer, stream, chunk: &[u8]| {
            peer.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_frame(|peer: &mut Peer, frame| {
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
        .on_stream_close(|peer: &mut Peer, stream, code, _failure| {
            peer.closed.insert(stream.get(), code.get());
        })
        .build()
        .expect("building the peer session")
}

/// Answers each request as soon as its head arrives, without waiting for its body.
fn answer_at_once(session: &mut Session<Peer>, peer: &mut Peer) {
    for stream in core::mem::take(&mut peer.pending) {
        respond(session, peer, stream);
    }
}

/// Answers only once a request has ended, so a body that never finishes never gets a
/// response — which is what lets a failure be observed through the response future.
fn answer_when_complete(session: &mut Session<Peer>, peer: &mut Peer) {
    let ready: Vec<i32> = core::mem::take(&mut peer.complete)
        .into_iter()
        .filter(|stream| peer.pending.contains(stream))
        .collect();
    for stream in ready {
        peer.pending.retain(|held| *held != stream);
        respond(session, peer, stream);
    }
}

fn respond(session: &mut Session<Peer>, peer: &Peer, stream: i32) {
    let path = peer.paths.get(&stream).cloned().unwrap_or_default();
    session
        .submit_response(
            StreamId::new(stream),
            &[Header::new(":status", "200"), Header::new("x-path", &path)],
        )
        .expect("submitting a response");
}

// ---------------------------------------------------------------------------
// Client-side helpers
// ---------------------------------------------------------------------------

fn upload<B>(path: &str, body: B) -> http::Request<B> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://example.test{path}"))
        .body(body)
        .expect("building a request")
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

/// A payload with no repeating structure, so a misplaced chunk shows up as a mismatch.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn a_deferred_body_emits_no_frames_and_is_asked_nothing_further() {
    // Spec SC-008, from the wire's side. The deferral tests prove the body is not
    // consulted; this proves the *connection* stays quiet too — a driver that answered a
    // deferral by emitting empty DATA frames would satisfy the consultation count and
    // flood the peer.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    let response = requests.send_request(upload("/deferred", body));

    let exchange = async {
        // The peer answers on the head, so this resolves while the body is still deferred.
        let head = response.await.expect("a response");
        assert_eq!(head.status(), http::StatusCode::OK);

        // Let the body defer, and note where it stood.
        for _ in 0..8 {
            yield_now().await;
        }
        let deferred_at = script.consultations();
        assert!(script.is_deferred(), "the body never parked");

        // Many more passes, with nothing to wake it.
        for _ in 0..32 {
            yield_now().await;
        }
        let after = script.consultations();

        script.finish();
        for _ in 0..8 {
            yield_now().await;
        }
        drop(requests);
        (deferred_at, after)
    };

    let mut peer = Peer::default();
    let (deferred_at, after) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        after, deferred_at,
        "a parked body was consulted again with nothing to wake it",
    );
    assert_eq!(
        peer.data_frames.get(&1).copied().unwrap_or_default(),
        1,
        "a deferred body put frames on the wire; only the one ending the body is allowed",
    );
    assert_eq!(peer.bodies.get(&1).map(Vec::len).unwrap_or_default(), 0);
}

#[test]
fn at_most_one_chunk_is_held_back_and_the_body_is_not_read_ahead() {
    // Spec SC-018, asserted two ways at once. The hook reports the most chunks any body
    // ever held; the consultation count reports whether the body was asked for more than
    // it was asked for. A payload far larger than one DATA frame is what makes both mean
    // something: the bridge must serve many frames from a single chunk.
    let expected = payload(400 * 1024);

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    script.send(expected.clone());
    let response = requests.send_request(upload("/one-chunk", body));

    let exchange = async {
        response.await.expect("a response");

        // Drain the single chunk. It spans many frames and more than one flow-control
        // window, so the bridge is consulted for far fewer chunks than it emits frames.
        for _ in 0..512 {
            yield_now().await;
        }
        let while_draining = script.consultations();

        script.finish();
        for _ in 0..16 {
            yield_now().await;
        }
        let held = buffered_chunks(&requests);
        drop(requests);
        (while_draining, held)
    };

    let mut peer = Peer::default();
    let (while_draining, held) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(held, 1, "the send path held back more than one chunk");
    assert_eq!(
        while_draining, 2,
        "the body was read ahead: one consultation for the chunk, one that found nothing \
         after it, and no more",
    );
    assert!(
        peer.data_frames.get(&1).copied().unwrap_or_default() > 16,
        "the payload did not span enough frames to prove anything",
    );
    assert_eq!(peer.bodies.get(&1), Some(&expected));
}

#[test]
fn outgoing_trailers_reach_the_peer_after_the_final_data() {
    // Spec SC-023, asserted from the receiving side: what matters is what the peer saw,
    // and in what order.
    let expected = payload(3_000);

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    script.send(expected.clone());
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-checksum", http::HeaderValue::from_static("deadbeef"));
    script.finish_with_trailers(trailers);

    let response = requests.send_request(upload("/trailers", body));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..32 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = Peer::default();
    let () = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(peer.bodies.get(&1), Some(&expected));
    assert_eq!(
        peer.data_frames.get(&1).copied().unwrap_or_default(),
        1,
        "announcing trailers put an empty frame on the wire; libnghttp2 cancels a \
         zero-length DATA frame that carries no end-of-stream, so the trailing block \
         should follow the final data directly",
    );
    assert_eq!(
        peer.trailers.get(&1).map(Vec::as_slice),
        Some(&[("x-checksum".to_owned(), "deadbeef".to_owned())][..]),
    );

    let seen: Vec<&'static str> = peer
        .order
        .iter()
        .filter(|(stream, _)| *stream == 1)
        .map(|(_, what)| *what)
        .collect();
    assert_eq!(
        seen.last(),
        Some(&"trailers"),
        "the trailing block did not come last: {seen:?}",
    );
    assert!(
        seen.iter().filter(|what| **what == "trailers").count() == 1,
        "the trailing block arrived more than once: {seen:?}",
    );
}

#[test]
fn two_streams_trailing_in_one_pass_each_get_their_own_block() {
    // Trailers are stashed by stream and drained in one place, so two bodies announcing
    // in the same serialisation pass are exactly the case where a stash keyed wrongly —
    // or drained assuming one entry — would go unnoticed. Their blocks differ, so a
    // crossed wire is visible rather than merely possible.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let mut sent = Vec::new();
    for (index, mark) in ["first", "second"].into_iter().enumerate() {
        let (body, script) = scripted();
        script.send(payload(1_000 + index));
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-mark", http::HeaderValue::from_static(mark));
        script.finish_with_trailers(trailers);
        sent.push(requests.send_request(upload(&format!("/{mark}"), body)));
    }

    let exchange = async {
        for response in sent {
            response.await.expect("a response");
        }
        for _ in 0..32 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = Peer::default();
    let () = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.trailers.get(&1).map(Vec::as_slice),
        Some(&[("x-mark".to_owned(), "first".to_owned())][..]),
    );
    assert_eq!(
        peer.trailers.get(&3).map(Vec::as_slice),
        Some(&[("x-mark".to_owned(), "second".to_owned())][..]),
    );
    assert_eq!(peer.bodies.get(&1).map(Vec::len), Some(1_000));
    assert_eq!(peer.bodies.get(&3).map(Vec::len), Some(1_001));
}

#[test]
fn a_forbidden_outgoing_trailer_fails_only_its_own_stream() {
    // A trailing block this crate cannot encode is one message's problem. Failing the
    // connection instead would make a caller's own bad trailer more destructive than a
    // peer's — the receive side resets just the stream for the mirror image of this.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (bad_body, bad) = scripted();
    bad.send(payload(500));
    let mut forbidden = http::HeaderMap::new();
    // Connection-specific, and RFC 9113 §8.2.2 makes a message carrying one malformed.
    forbidden.insert("connection", http::HeaderValue::from_static("keep-alive"));
    bad.finish_with_trailers(forbidden);
    let first = requests.send_request(upload("/forbidden", bad_body));

    let (good_body, good) = scripted();
    good.send(payload(700));
    let mut allowed = http::HeaderMap::new();
    allowed.insert("x-mark", http::HeaderValue::from_static("fine"));
    good.finish_with_trailers(allowed);
    let second = requests.send_request(upload("/allowed", good_body));

    let exchange = async {
        // Whichever channel is still open is the one the caller hears through: the
        // response future while it is unanswered, the receiving body once it is not.
        let rejected = first.await.err();
        let second = second.await.expect("a response");

        for _ in 0..32 {
            yield_now().await;
        }

        drop((second, requests));
        rejected
    };

    let mut peer = Peer::default();
    let reported = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    let reported = reported.expect("the rejected trailer was never reported");
    assert_eq!(reported.kind(), ngnet_h2::http::ErrorKind::Protocol);
    assert!(
        reported.to_string().contains("connection-specific"),
        "the caller was not told which field was the problem: {reported}",
    );

    assert!(
        !peer.trailers.contains_key(&1),
        "a forbidden trailing block reached the peer",
    );
    assert_eq!(
        peer.trailers.get(&3).map(Vec::as_slice),
        Some(&[("x-mark".to_owned(), "fine".to_owned())][..]),
        "the other stream's trailers were lost with it",
    );
    assert_eq!(peer.bodies.get(&3).map(Vec::len), Some(700));
}

#[test]
fn a_failing_body_resets_the_stream_and_surfaces_its_error() {
    // Spec SC-010. Two halves: the peer must see the stream go away, and the caller must
    // get back the error their own body produced rather than a rendering of it.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    script.send(payload(200));
    script.fail("the disk went away");

    // The peer answers only completed requests, so nothing but the failure can settle
    // this future.
    let response = requests.send_request(upload("/failing", body));

    let exchange = async {
        let outcome = response.await;
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
        outcome
    };

    let mut peer = Peer::default();
    let outcome = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_when_complete),
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
fn a_body_larger_than_the_flow_control_window_transfers_intact() {
    // Spec SC-007, sending direction. Several times the default window, so capacity has
    // to be granted repeatedly for any of it to arrive.
    let expected = payload(400 * 1024);

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");

    let response = requests.send_request(upload("/large", Full::new(expected.clone())));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..512 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = Peer::default();
    let () = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(peer.bodies.get(&1).map(Vec::len), Some(expected.len()));
    assert_eq!(peer.bodies.get(&1), Some(&expected));
}

#[test]
fn a_zero_length_body_emits_no_spurious_data_frame() {
    // One `DATA` frame is unavoidable — something has to carry end-of-stream — but only
    // one. A bridge that forwarded the empty frame its body produced *and* the one that
    // ends the message would send two, and would keep sending them for a body that
    // yielded empty frames indefinitely.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");

    let response = requests.send_request(upload("/empty", Full::new(&b""[..])));

    let exchange = async {
        response.await.expect("a response");
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
    };

    let mut peer = Peer::default();
    let () = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(
        peer.data_frames.get(&1).copied().unwrap_or_default(),
        1,
        "a zero-length body cost more than the one frame that ends it",
    );
    assert_eq!(peer.bodies.get(&1).map(Vec::len).unwrap_or_default(), 0);
}

#[test]
fn a_request_with_no_body_emits_no_data_frame_at_all() {
    // A body that reports itself already ended is never submitted as a body, so the head
    // carries end-of-stream and nothing follows it.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let response = requests.send_request(
        http::Request::builder()
            .uri("http://example.test/bodyless")
            .body(Empty)
            .expect("building a request"),
    );

    let exchange = async {
        response.await.expect("a response");
        drop(requests);
    };

    let mut peer = Peer::default();
    let () = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_at_once),
    ));

    assert_eq!(peer.data_frames.get(&1).copied().unwrap_or_default(), 0);
}
