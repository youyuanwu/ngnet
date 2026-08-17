//! A response body that fails partway, across real loopback UDP.
//!
//! The HTTP/3 layer answers a failed response body by resetting that one stream and never
//! ending it, so a caller's read of an abandoned message fails rather than completing. That
//! decision is made above the transport and asserted there; what these tests add is that it
//! survives the trip through ngtcp2 and a real socket, where a reset is a frame the peer has
//! to receive and act on rather than a call on a mock.
//!
//! Both shapes are covered, because they used to differ. With a backlog still queued the
//! reset discarded it and the truncation was plain to see; with nothing queued the peer had
//! already been handed what looked like a whole message, and no content-length to doubt it
//! by. The second case is the one that used to pass silently.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::time::Duration as StdDuration;

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full};
use ngnet_quic_h3::{accept, connect};
use ngnet_quic_h3_tests::{Credentials, TEST_SERVER_NAME, client_endpoint, server_endpoint};

type Payload = Full<Bytes>;

/// How long the body waits before it fails.
///
/// Long enough for the bytes it already offered to have crossed the socket, short enough
/// that a test spends no real time on it. Without the pause the reset overtakes the response
/// entirely, which is a real case but a different one.
const PAUSE: StdDuration = StdDuration::from_millis(50);

/// How long the caller waits before reading, so the backlog is still there when it does.
///
/// Comfortably longer than the body's own pause, and the dependence is in the safe
/// direction: the backlogged case needs the failure to happen before the caller drains, so a
/// larger margin makes it more reliable rather than less. Draining sooner would empty the
/// queue and quietly turn it into the other case.
const SETTLE: StdDuration = StdDuration::from_millis(500);

/// The failure the response body reports.
#[derive(Debug)]
struct Broken;

impl core::fmt::Display for Broken {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the response body failed partway through")
    }
}

impl core::error::Error for Broken {}

/// A body that offers `count` copies of a chunk, pauses, and then fails.
///
/// An application whose response falls apart once its headers have gone out, which is the
/// only way an HTTP/3 response gets abandoned that late: the status line has already been
/// promised, so the stream has to be reset rather than the answer changed.
///
/// How much it offers decides which case is being exercised. The chunks are handed over in
/// one pass, so many of them leave the transport with far more queued than its windows let
/// it write; one small chunk leaves nothing queued at all.
struct Failing {
    chunk: Bytes,
    remaining: usize,
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Failing {
    fn new(chunk: Bytes, count: usize) -> Self {
        Self {
            chunk,
            remaining: count,
            pause: None,
        }
    }
}

impl Body for Failing {
    type Data = Bytes;
    type Error = Broken;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Broken>>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            let chunk = self.chunk.clone();
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        let pause = self
            .pause
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(PAUSE)));
        match pause.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => Poll::Ready(Some(Err(Broken))),
        }
    }
}

/// A predictable payload of `len` bytes.
///
/// A repeating non-power-of-two cycle rather than zeroes, matching what the rest of this
/// crate sends, so a body that did arrive whole is recognisably whole.
fn pattern(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Serves one connection with a response that fails partway, and returns once it has
/// finished.
async fn serve_one(
    endpoint: ngnet_quic::endpoint::Endpoint<ngnet_quic::OsslSession>,
    chunk: usize,
    chunks: usize,
) {
    let backend = accept(&endpoint).await.expect("accepting a connection");

    let connection = ngnet_h3::http::serve(backend, move |request| async move {
        // Draining the request body matters: a handler that ignores it never returns the
        // flow-control credit the client needs to finish sending.
        let (_parts, incoming) = request.into_parts();
        let _ = incoming.collect().await;
        http::Response::builder()
            .status(200)
            .header("content-type", "application/octet-stream")
            .body(Failing::new(pattern(chunk), chunks))
            .expect("a response")
    })
    .expect("serving");

    if let Err(err) = connection.await {
        eprintln!("SERVER DRIVER ENDED: {err:?}");
    }
}

#[tokio::test]
async fn a_response_body_that_fails_with_a_backlog_queued_fails_the_callers_read() {
    // Sixteen mebibytes in quarter-mebibyte chunks: far more than the connection's windows
    // allow in flight, so a backlog exists for as long as the caller has not drained it and
    // the reset that ends the exchange has queued bytes to discard.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    let (client, client_driver) = client_endpoint(&credentials, 0xD1).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    tokio::spawn(serve_one(server, 256 * 1024, 64));

    let backend = tokio::time::timeout(
        StdDuration::from_secs(10),
        connect(&client, address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("a connection");

    let (sender, connection) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("CLIENT DRIVER ENDED: {err:?}");
        }
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(10),
        sender.send_request(
            http::Request::builder()
                .method("GET")
                .uri("https://localhost/broken")
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("its headers arrive before its body fails");

    assert_eq!(response.status(), 200);

    // Deliberately not read yet: leaving the backlog where it is until the handler's body
    // has failed is what puts this case on the path where the reset has something to throw
    // away.
    tokio::time::sleep(SETTLE).await;

    let outcome = tokio::time::timeout(StdDuration::from_secs(30), response.into_body().collect())
        .await
        .expect("the broken body must not hang");

    let error = match outcome {
        Ok(collected) => panic!(
            "a response body that failed must reach the caller as a failed read, but it \
             completed with {} bytes",
            collected.to_bytes().len(),
        ),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset"),
        "and it must say the peer reset the exchange, because that is what tells the caller \
         the answer was abandoned rather than merely interrupted: {error}",
    );
}

#[tokio::test]
async fn a_response_body_that_fails_with_nothing_queued_behind_it_still_fails_the_callers_read() {
    // One chunk of five hundred and twelve bytes: far below the smallest window either end
    // advertises, so all of it is written and gone before the body fails. The reset carries
    // the whole of what the caller is told, because nothing else is left to withhold — which
    // is why this case, and not the one above, is where a message that stopped short used to
    // arrive looking complete.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    let (client, client_driver) = client_endpoint(&credentials, 0xD2).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    tokio::spawn(serve_one(server, 512, 1));

    let backend = tokio::time::timeout(
        StdDuration::from_secs(10),
        connect(&client, address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("a connection");

    let (sender, connection) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("CLIENT DRIVER ENDED: {err:?}");
        }
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(10),
        sender.send_request(
            http::Request::builder()
                .method("GET")
                .uri("https://localhost/broken")
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("its headers arrive before its body fails");

    assert_eq!(response.status(), 200);

    // Read straight away, unlike the backlogged case: there is nothing to hold back, and a
    // stream abandoned this way is left suspended until its reset goes out — so if the reset
    // never reached the transport this read is where it would show, as a body that never
    // ends rather than as a wrong answer.
    let outcome = tokio::time::timeout(StdDuration::from_secs(30), response.into_body().collect())
        .await
        .expect("the broken body must not hang");

    let error = match outcome {
        Ok(collected) => panic!(
            "a response body that failed must reach the caller as a failed read; ending it \
             normally hands over the {} bytes that did arrive as though they were the whole \
             answer, and the caller has no way to know otherwise",
            collected.to_bytes().len(),
        ),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset"),
        "and it must say the peer reset the exchange, because that is what tells the caller \
         the answer was abandoned rather than merely interrupted: {error}",
    );
}
