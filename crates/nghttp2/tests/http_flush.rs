//! The driver's flush contract: octets it produces reach the peer before it waits on one.
//!
//! A transport is allowed to buffer its writes — `tokio::io::BufWriter` and `BufStream` do,
//! and they satisfy the `AsyncWrite` bound `TokioIo` accepts. For such a transport `write`
//! only fills a buffer; the octets become peer-visible when it is flushed. The driver's
//! obligation is to flush ([`TransportWrite::commit`]) after draining a write pass and
//! before it parks awaiting readable input, so it never blocks on a response to a request
//! still sitting in a buffer.
//!
//! `testing::buffering()` is exactly such a transport. This exercise drives a full request
//! and response over it and asserts the exchange completes. Remove the driver's `commit`
//! call and it does not: the request never leaves the buffer, the peer never answers, and
//! the client waits forever. The budget below turns that regression into a failure rather
//! than a hung suite.

#![cfg(feature = "http")]

use core::future::{Future, poll_fn};
use core::task::{Context, Poll};

use nghttp2::http::testing::{Empty, alongside, block_on, buffering, http_crate as http};
use nghttp2::http::{IncomingBody, server};

/// Drives `work`, but gives up after `budget` self-woken polls.
///
/// The in-memory executor parks on a condvar when every future returns `Pending`, so a
/// genuine deadlock would block the test thread forever. Self-waking each poll keeps the
/// executor re-polling until either `work` finishes or the budget runs out, at which point
/// this returns `None` and the caller can fail deliberately instead of hanging.
async fn within_budget<F: Future>(work: F, budget: usize) -> Option<F::Output> {
    let mut work = Box::pin(work);
    let mut left = budget;
    poll_fn(move |cx: &mut Context<'_>| {
        if let Poll::Ready(value) = work.as_mut().poll(cx) {
            return Poll::Ready(Some(value));
        }
        if left == 0 {
            return Poll::Ready(None);
        }
        left -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

fn get(path: &str) -> http::Request<Empty> {
    http::Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(Empty)
        .expect("building a request")
}

#[test]
fn a_buffering_transport_still_completes_an_exchange() {
    // The client's writing half buffers until `commit`; the peer is an ordinary duplex.
    let (client_transport, server_transport) = buffering();

    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_transport).expect("handshake");

    let serving = server::serve(server_transport, |request: http::Request<IncomingBody>| {
        drop(request.into_body());
        async move {
            http::Response::builder()
                .status(200)
                .header("x-answered", "yes")
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let exchange = async {
        let response = requests
            .send_request(get("/buffered"))
            .await
            .expect("a response");
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-answered")
                .and_then(|value| value.to_str().ok()),
            Some("yes"),
            "the response did not round-trip through the buffering transport",
        );
        drop(requests);
    };

    // A healthy exchange settles in well under this many polls; the budget only bites if the
    // driver stops flushing, which is the regression this test exists to catch.
    let outcome = block_on(within_budget(
        alongside(alongside(exchange, connection), serving),
        200_000,
    ));

    assert!(
        outcome.is_some(),
        "the exchange never completed: without the driver's commit, a buffering transport \
         holds the request and the peer never sees it",
    );
}
