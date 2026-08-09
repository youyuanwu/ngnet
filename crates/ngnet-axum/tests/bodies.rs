//! Bodies over the wire: size, streaming, trailers and multiplexing (SC-002, SC-003,
//! SC-005, SC-010).
//!
//! These are the tests that would catch a body integration that *looks* right. A response
//! that is buffered whole still returns the correct bytes; a body that ignores flow control
//! still works below the initial window; a handler that runs to completion before its
//! response is written still answers. Each test here is built so that the plausible wrong
//! implementation fails it, not merely so that the right one passes.

mod support;

use axum::Router;
use axum::routing::{get, post};
use bytes::Bytes;
use http_body::{Body, Frame};
use std::pin::Pin;
use std::task::{Context, Poll};
use support::{Client, TestServer, get as get_request, post as post_request, text, within};
use tokio::sync::{mpsc, oneshot};

/// A response body that yields what the test tells it to, when the test says so.
///
/// axum's `Body::from_stream` would want a `futures_core::Stream` and therefore another
/// dependency; a channel behind `http_body::Body` is smaller and states the intent directly.
/// Sending `None` for the data part with trailers attached ends the body with trailers.
struct Scripted {
    frames: mpsc::UnboundedReceiver<Frame<Bytes>>,
}

impl Body for Scripted {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.frames.poll_recv(context).map(|frame| frame.map(Ok))
    }
}

/// A payload larger than HTTP/2's initial 65 535-byte stream flow-control window.
///
/// The size is the point. Anything smaller fits in the window the peer grants before any
/// `WINDOW_UPDATE` is exchanged, so a body implementation that ignored flow control
/// entirely would still pass. Crossing the window forces the exchange to work.
fn oversized() -> Bytes {
    // A repeating but non-uniform pattern: a truncation or a duplicated chunk shows up as a
    // mismatch at a definite offset rather than as bytes that happen to look the same.
    let mut payload = Vec::with_capacity(200 * 1024);
    let mut counter: u32 = 0;
    while payload.len() < 200 * 1024 {
        payload.extend_from_slice(&counter.to_le_bytes());
        counter = counter.wrapping_add(1);
    }
    Bytes::from(payload)
}

/// A request body crossing the flow-control window arrives whole and in order (SC-002).
#[tokio::test]
async fn a_request_body_larger_than_the_flow_control_window_arrives_intact() {
    let router = Router::new().route("/echo", post(|body: Bytes| async move { body }));
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let sent = oversized();
    let (head, received) = client
        .exchange(post_request(server.address, "/echo", sent.clone()))
        .await;

    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(
        received.len(),
        sent.len(),
        "echoed body had the wrong length"
    );
    assert_eq!(received, sent, "echoed body differed from what was sent");

    client.disconnect();
    server.shutdown().await;
}

/// A response body crossing the flow-control window arrives whole and in order (SC-002).
///
/// Worth testing in both directions: the sending and receiving paths are separate code with
/// separate flow-control accounting, and this crate sits on both.
#[tokio::test]
async fn a_response_body_larger_than_the_flow_control_window_arrives_intact() {
    let payload = oversized();
    let served = payload.clone();
    let router = Router::new().route(
        "/big",
        get(move || {
            let served = served.clone();
            async move { served }
        }),
    );
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let (head, received) = client.exchange(get_request(server.address, "/big")).await;

    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);

    client.disconnect();
    server.shutdown().await;
}

/// A response is streamed, not buffered (SC-003).
///
/// This is the discriminating test in the file, and it is built to *deadlock* against a
/// buffering implementation rather than to measure timings. The handler returns after
/// sending only the first chunk; the second is sent only once the client reports having
/// received the first. An implementation that collected the whole body before writing
/// anything would never deliver the first chunk, the client would never signal, the second
/// chunk would never be sent, and the test would fail on its timeout. Nothing here depends
/// on how fast the machine is.
#[tokio::test]
async fn a_response_body_is_streamed_rather_than_buffered() {
    let (frames, receiver) = mpsc::unbounded_channel();
    let (first_seen, wait_for_first) = oneshot::channel::<()>();
    let receiver = std::sync::Arc::new(std::sync::Mutex::new(Some(receiver)));

    let router = Router::new().route(
        "/stream",
        get(move || {
            let receiver = receiver
                .lock()
                .expect("a lock")
                .take()
                .expect("one request");
            async move {
                axum::response::Response::new(axum::body::Body::new(Scripted { frames: receiver }))
            }
        }),
    );

    // The first chunk is queued before the request is made; the second is not queued until
    // the client has seen the first.
    frames
        .send(Frame::data(Bytes::from_static(b"first")))
        .expect("a live body");

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let response = within(
        "a response head",
        client
            .sender
            .clone()
            .send_request(get_request(server.address, "/stream")),
    )
    .await
    .expect("a response");
    let mut body = response.into_body();

    let chunk = within("the first chunk", next_data(&mut body)).await;
    assert_eq!(chunk, Bytes::from_static(b"first"));
    let _ = first_seen.send(());

    // Only now does the rest of the body exist.
    within("the client's acknowledgement", wait_for_first)
        .await
        .expect("an acknowledgement");
    frames
        .send(Frame::data(Bytes::from_static(b"second")))
        .expect("a live body");
    drop(frames);

    let chunk = within("the second chunk", next_data(&mut body)).await;
    assert_eq!(chunk, Bytes::from_static(b"second"));

    drop(body);
    client.disconnect();
    server.shutdown().await;
}

/// Trailers a handler set arrive after the data (SC-005).
#[tokio::test]
async fn trailers_arrive_after_the_body() {
    let (frames, receiver) = mpsc::unbounded_channel();
    frames
        .send(Frame::data(Bytes::from_static(b"payload")))
        .expect("a live body");
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    frames.send(Frame::trailers(trailers)).expect("a live body");
    drop(frames);

    let receiver = std::sync::Arc::new(std::sync::Mutex::new(Some(receiver)));
    let router = Router::new().route(
        "/trailers",
        get(move || {
            let receiver = receiver
                .lock()
                .expect("a lock")
                .take()
                .expect("one request");
            async move {
                axum::response::Response::new(axum::body::Body::new(Scripted { frames: receiver }))
            }
        }),
    );

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let response = within(
        "a response",
        client
            .sender
            .clone()
            .send_request(get_request(server.address, "/trailers")),
    )
    .await
    .expect("a response");

    // `collect` keeps the data and the trailers apart, which is what makes the ordering
    // claim assertable: trailers that arrived before the data would not be trailers.
    let collected = within(
        "a complete body",
        http_body_util::BodyExt::collect(response.into_body()),
    )
    .await
    .expect("a complete body");
    let trailers = collected.trailers().cloned();
    let data = collected.to_bytes();

    assert_eq!(data, Bytes::from_static(b"payload"));
    let trailers = trailers.expect("trailers");
    assert_eq!(trailers.get("grpc-status").expect("the trailer"), "0");

    client.disconnect();
    server.shutdown().await;
}

/// Several requests share one connection, and the one that arrived first completes last
/// (SC-010).
///
/// The synchronisation here is load-bearing and was got wrong once, so it is worth stating.
/// The `/slow` request must be *received and parked at the server* before `/fast` is sent;
/// otherwise the test proves nothing. hyper enqueues a request when `send_request` is
/// called, and a `#[tokio::test]` runs on a current-thread runtime, so simply spawning the
/// slow request and then sending the fast one puts `/fast` on the wire first -- measured,
/// not assumed. A server that serialised streams in arrival order would then answer `/fast`,
/// whose handler releases the gate, and go on to answer `/slow`, passing every assertion
/// without ever having two streams in flight.
///
/// So the test waits for the slow handler to announce that it has entered and parked. From
/// that point a serialising server cannot answer `/fast` at all: its only stream is occupied
/// by a handler that will not return until `/fast` releases it. The test deadlocks against
/// it and fails on its timeout, which is what a test of multiplexing should do.
#[tokio::test]
async fn requests_are_multiplexed_and_may_finish_out_of_order() {
    let (slow_entered, mut slow_is_parked) = mpsc::unbounded_channel::<()>();
    let (release_first, first_released) = oneshot::channel::<()>();
    let release_first = std::sync::Arc::new(std::sync::Mutex::new(Some(release_first)));
    let first_released = std::sync::Arc::new(tokio::sync::Mutex::new(Some(first_released)));

    let router = Router::new()
        .route(
            "/slow",
            get(move || {
                let first_released = std::sync::Arc::clone(&first_released);
                let slow_entered = slow_entered.clone();
                async move {
                    let gate = first_released.lock().await.take().expect("one request");
                    let _ = slow_entered.send(());
                    let _ = gate.await;
                    "slow"
                }
            }),
        )
        .route(
            "/fast",
            get(move || {
                let release_first = std::sync::Arc::clone(&release_first);
                async move {
                    // Answering releases the slow handler, so the ordering is caused rather
                    // than hoped for.
                    if let Some(release) = release_first.lock().expect("a lock").take() {
                        let _ = release.send(());
                    }
                    "fast"
                }
            }),
        );

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let slow = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move {
            sender
                .send_request(get_request(address, "/slow"))
                .await
                .expect("a response")
        }
    });

    // The slow request is now genuinely in flight with its handler parked. Everything the
    // test claims depends on this having happened before the next line runs.
    within("the slow handler to park", slow_is_parked.recv())
        .await
        .expect("the slow handler to park");

    let (fast_head, fast_body) = client.exchange(get_request(server.address, "/fast")).await;
    assert_eq!(fast_head.status, http::StatusCode::OK);
    assert_eq!(text(&fast_body), "fast");

    let slow = within("the slow response", slow)
        .await
        .expect("the request task not to panic");
    assert_eq!(slow.status(), http::StatusCode::OK);
    let slow_body = within(
        "the slow body",
        http_body_util::BodyExt::collect(slow.into_body()),
    )
    .await
    .expect("a complete body")
    .to_bytes();
    assert_eq!(text(&slow_body), "slow");

    client.disconnect();
    server.shutdown().await;
}

/// Reads frames until a data frame appears, returning its bytes.
async fn next_data(body: &mut hyper::body::Incoming) -> Bytes {
    loop {
        let frame = http_body_util::BodyExt::frame(body)
            .await
            .expect("another frame")
            .expect("a readable frame");
        if let Ok(data) = frame.into_data() {
            return data;
        }
    }
}
