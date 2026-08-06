//! A client exchange, end to end over an in-memory transport (Spec SC-005, SC-012).
//!
//! Everything here runs on one task. That is not a simplification for the tests' benefit
//! — it is the property under test. A connection that needed a runtime, a spawner or a
//! thread to make progress could not be asserted this way at all.

#![cfg(feature = "http")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ngnet_h2::http::testing::{
    self, Empty, Full, alongside, block_on, duplex, http_crate as http, serve,
};
use ngnet_h2::{
    BodyOutcome, BodySource, ErrorCode, FrameType, Header, HeaderAction, HeaderCategory, Session,
    SessionBuilder, StreamId,
};

/// What the peer server observed, and what it still owes.
#[derive(Debug, Default)]
struct Peer {
    /// Paths of requests whose head has completed, by stream.
    paths: BTreeMap<i32, String>,
    /// Streams whose request head is complete and which have not been answered.
    pending: Vec<i32>,
    /// Payload received, by stream.
    bodies: BTreeMap<i32, Vec<u8>>,
}

/// A response body that never produces anything and never ends.
///
/// Used to hold a stream open after its response head has been sent, which is how "the
/// head is delivered before the exchange finishes" becomes assertable rather than
/// coincidental.
struct NeverEnds;

impl BodySource for NeverEnds {
    fn fill(&mut self, _buf: &mut [u8]) -> BodyOutcome {
        BodyOutcome::Defer
    }
}

fn peer_session() -> Session<Peer> {
    SessionBuilder::<Peer>::server()
        .on_header(|peer: &mut Peer, frame, name: &[u8], value: &[u8]| {
            if name == b":path" {
                peer.paths.insert(
                    frame.stream_id().get(),
                    String::from_utf8_lossy(value).into_owned(),
                );
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
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && frame.category() == Some(HeaderCategory::Request)
            {
                peer.pending.push(frame.stream_id().get());
            }
        })
        .build()
        .expect("building the peer session")
}

/// Answers every outstanding request with `200` and the path it asked for, echoed back.
fn answer_plainly(session: &mut Session<Peer>, peer: &mut Peer) {
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

fn request(path: &str) -> http::Request<Empty> {
    http::Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(Empty)
        .expect("building a request")
}

#[test]
fn a_request_and_response_complete() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/hello"));

    let exchange = async {
        let response = response.await.expect("a response");
        drop(requests);
        response
    };

    let response = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-path")
            .map(http::HeaderValue::as_bytes),
        Some(b"/hello".as_slice()),
    );
}

#[test]
fn a_request_body_reaches_the_peer() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/upload")
            .body(Full::new(&b"payload"[..]))
            .expect("building a request"),
    );

    let exchange = async {
        let response = response.await.expect("a response");
        drop(requests);
        response
    };

    let response = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        peer.bodies.get(&1).map(Vec::as_slice),
        Some(&b"payload"[..])
    );
}

#[test]
fn four_concurrent_requests_resolve_in_reverse_order() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let futures: Vec<_> = (0..4)
        .map(|index| requests.send_request(request(&format!("/{index}"))))
        .collect();

    let exchange = async {
        let mut seen = Vec::new();
        // Awaited in the opposite order to submission: a response that arrived while
        // nothing was awaiting it must still be waiting when someone finally looks.
        for future in futures.into_iter().rev() {
            let response = future.await.expect("a response");
            seen.push(
                String::from_utf8_lossy(
                    response
                        .headers()
                        .get("x-path")
                        .expect("the echoed path")
                        .as_bytes(),
                )
                .into_owned(),
            );
        }
        drop(requests);
        seen
    };

    let seen = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(seen, vec!["/3", "/2", "/1", "/0"]);
    assert_eq!(peer.paths.len(), 4);
}

#[test]
fn the_response_head_arrives_before_the_exchange_ends() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/stream"));

    // The peer answers with a body that never produces anything, so the stream stays open
    // for as long as the test runs. Anything that waited for the exchange to finish would
    // hang here instead of returning a head.
    let answer_and_stall = |session: &mut Session<Peer>, peer: &mut Peer| {
        for stream in core::mem::take(&mut peer.pending) {
            session
                .submit_response_with_body(
                    StreamId::new(stream),
                    &[Header::new(":status", "206")],
                    NeverEnds,
                )
                .expect("submitting a response");
        }
    };

    let response = block_on(alongside(
        alongside(async { response.await.expect("a response") }, connection),
        serve(server_side, peer_session(), &mut peer, answer_and_stall),
    ));

    assert_eq!(response.status(), http::StatusCode::PARTIAL_CONTENT);
}

#[test]
fn an_informational_head_does_not_settle_the_future_as_final() {
    // A hostile or ordinary peer sends `103 Early Hints` before the real `200`. libnghttp2
    // surfaces the `1xx` head to the client before it marks the stream as expecting a
    // final response, so a client that settled on the first head would resolve with `103`
    // and discard the `200` that follows. The future must resolve with `200`.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/hinted"));

    // Counts the informational heads actually put on the wire, so a future resolving with
    // `200` means the `1xx` was sent and ignored — not that the peer never sent one.
    let hints = Arc::new(AtomicUsize::new(0));
    let hints_sent = Arc::clone(&hints);

    // The `1xx` and the final head cannot be submitted in one pass: libnghttp2 rejects a
    // second HEADERS on a stream whose first is not yet serialised. So the `103` goes out
    // on the pass a request arrives, and the `200` on the next — which is also the order a
    // real server produces them.
    let mut phase: BTreeMap<i32, u8> = BTreeMap::new();
    let interim_then_final = move |session: &mut Session<Peer>, peer: &mut Peer| {
        for stream in core::mem::take(&mut peer.pending) {
            phase.entry(stream).or_insert(0);
        }
        for (&stream, step) in &mut phase {
            match *step {
                0 => {
                    session
                        .submit_informational(
                            StreamId::new(stream),
                            &[Header::new(":status", "103"), Header::new("link", "</a>")],
                        )
                        .expect("submitting 103");
                    hints_sent.fetch_add(1, Ordering::AcqRel);
                    *step = 1;
                }
                1 => {
                    session
                        .submit_response(
                            StreamId::new(stream),
                            &[Header::new(":status", "200"), Header::new("x-final", "yes")],
                        )
                        .expect("submitting 200");
                    *step = 2;
                }
                _ => {}
            }
        }
    };

    let response = block_on(alongside(
        alongside(async { response.await.expect("a response") }, connection),
        serve(server_side, peer_session(), &mut peer, interim_then_final),
    ));

    assert_eq!(
        response.status(),
        http::StatusCode::OK,
        "the future settled on an informational head instead of the final response",
    );
    assert_eq!(
        response
            .headers()
            .get("x-final")
            .map(http::HeaderValue::as_bytes),
        Some(b"yes".as_slice()),
        "the resolved head is not the final one",
    );
    assert!(
        hints.load(Ordering::Acquire) >= 1,
        "the peer never actually sent an informational head, so nothing was ignored",
    );
}

#[test]
fn a_stream_carrying_only_an_informational_head_fails_rather_than_hangs() {
    // The other side of ignoring `1xx`: if a stream only ever carries an informational
    // head and then ends, the future must resolve — with an error — rather than wait for a
    // final response that will never come.
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/hint-only"));

    let mut phase: BTreeMap<i32, u8> = BTreeMap::new();
    let interim_then_reset = move |session: &mut Session<Peer>, peer: &mut Peer| {
        for stream in core::mem::take(&mut peer.pending) {
            phase.entry(stream).or_insert(0);
        }
        for (&stream, step) in &mut phase {
            match *step {
                0 => {
                    session
                        .submit_informational(
                            StreamId::new(stream),
                            &[Header::new(":status", "100")],
                        )
                        .expect("submitting 100");
                    *step = 1;
                }
                // On the pass after the `100` is serialised, end the stream without a final
                // response. Resetting on the next pass rather than the same one is what
                // lets the informational head reach the client first.
                1 => {
                    session
                        .reset_stream(StreamId::new(stream), ErrorCode::INTERNAL_ERROR)
                        .expect("resetting");
                    *step = 2;
                }
                _ => {}
            }
        }
    };

    let outcome = block_on(alongside(
        alongside(response, connection),
        serve(server_side, peer_session(), &mut peer, interim_then_reset),
    ));

    let error = outcome.expect_err("a stream that never sent a final response must fail");
    assert_eq!(
        error.kind(),
        ngnet_h2::http::ErrorKind::Stream,
        "an informational-only stream ended with the wrong error: {error}",
    );
}

#[test]
fn dropping_the_driver_resolves_every_pending_request() {
    let (client_side, _server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let first = requests.send_request(request("/one"));
    let second = requests.send_request(request("/two"));

    // Never polled, so nothing was ever submitted. The requests must still be answered:
    // a caller waiting on a connection that no longer exists would otherwise wait forever.
    drop(connection);

    for (name, future) in [("first", first), ("second", second)] {
        let error = block_on(future).expect_err("the connection is gone");
        assert!(error.is_closed(), "{name} reported {error}");
    }

    // And a request made afterwards fails the same way, through the same channel.
    let later =
        block_on(requests.send_request(request("/three"))).expect_err("the connection is gone");
    assert!(later.is_closed());
    assert!(requests.is_closed());
}

#[test]
fn a_cloned_handle_submits_on_the_same_connection() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let clone = requests.clone();
    let first = requests.send_request(request("/from-original"));
    let second = clone.send_request(request("/from-clone"));

    let exchange = async {
        let first = first.await.expect("a response");
        let second = second.await.expect("a response");
        drop((requests, clone));
        (first, second)
    };

    let (first, second) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(first.status(), http::StatusCode::OK);
    assert_eq!(second.status(), http::StatusCode::OK);
    assert_eq!(peer.paths.len(), 2);
}

#[test]
fn the_connection_ends_once_the_last_handle_is_dropped() {
    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/last"));

    // The driver is the *main* future here, so the test only finishes if the driver itself
    // decides there is nothing left to do.
    let outcome = block_on(alongside(
        alongside(connection, async {
            response.await.expect("a response");
            drop(requests);
        }),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    outcome.expect("the connection finished cleanly");
}

#[test]
fn a_driver_over_a_send_transport_is_send() {
    // Asserted by inference rather than by a bound. Nothing in the transport traits
    // requires `Send`, precisely so thread-per-core runtimes can implement them; what
    // makes `tokio::spawn` usable is that the property propagates when it holds.
    fn assert_send<T: Send>(_value: &T) {}

    let (client_side, _server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    assert_send(&connection);
    assert_send(&requests);
    assert_send(&requests.send_request(request("/anything")));
}

#[test]
fn the_client_session_reports_consumption_itself() {
    // Spec SC-029. Asserted here, against a session built exactly as the driver builds it,
    // rather than against a constant that could drift away from the code that reads it.
    assert!(testing::client_session_has_manual_flow_control());
}

#[test]
fn a_request_without_an_authority_is_rejected() {
    let (client_side, _server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let response = requests.send_request(
        http::Request::builder()
            .uri("/no-authority")
            .body(Empty)
            .expect("building a request"),
    );

    let error = block_on(alongside(response, connection)).expect_err("a request with no authority");
    assert_eq!(error.kind(), ngnet_h2::http::ErrorKind::Protocol);
}

#[test]
fn a_connection_specific_field_is_rejected() {
    let (client_side, _server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let response = requests.send_request(
        http::Request::builder()
            .uri("http://example.test/")
            .header("connection", "keep-alive")
            .body(Empty)
            .expect("building a request"),
    );

    let error = block_on(alongside(response, connection)).expect_err("a connection-specific field");
    assert_eq!(error.kind(), ngnet_h2::http::ErrorKind::Protocol);
}

#[test]
fn the_borrowed_write_path_carries_an_exchange_too() {
    // Which write strategy runs is the transport's choice and the two are mutually
    // exclusive, so both have to be exercised. This is the same exchange as the first
    // test over a transport that takes its octets borrowed.
    let (client_side, server_side) = duplex(true);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/borrowed")
            .body(Full::new(&b"zero copy"[..]))
            .expect("building a request"),
    );

    let exchange = async {
        let response = response.await.expect("a response");
        drop(requests);
        response
    };

    let response = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        peer.bodies.get(&1).map(Vec::as_slice),
        Some(&b"zero copy"[..]),
    );
}

#[test]
fn a_body_frame_larger_than_one_data_frame_arrives_whole() {
    // The session hands a body a bounded buffer, and an `http_body` frame is whatever size
    // its producer chose; the two do not line up. Without a cursor over the remainder,
    // everything past the first buffer's worth would simply be dropped — silently, and
    // only for large payloads.
    let payload: Vec<u8> = (0..70_000u32).map(|index| (index % 251) as u8).collect();

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Full>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/large")
            .body(Full::new(payload.clone()))
            .expect("building a request"),
    );

    let exchange = async {
        let response = response.await.expect("a response");
        // The head may arrive before the upload finishes, which is the point of the
        // streaming design — so the body still needs passes to drain.
        for _ in 0..64 {
            yield_now().await;
        }
        drop(requests);
        response
    };

    let response = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        peer.bodies.get(&1).map(Vec::len),
        Some(payload.len()),
        "every octet of a multi-frame body reached the peer",
    );
    assert_eq!(peer.bodies.get(&1), Some(&payload));
}

/// Yields once, so the driver gets a full poll before the test looks again.
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
