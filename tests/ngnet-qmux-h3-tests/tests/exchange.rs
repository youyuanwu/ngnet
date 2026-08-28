//! A request and a response, with bodies in both directions, over an in-memory pair.
//!
//! The test the whole crate exists for: HTTP/3 running over this workspace's own QMux
//! implementation, with nothing between the two ends but a byte stream.

use bytes::Bytes;
use ngnet_qmux_h3_tests::{LIMIT, drain, get, memory_pair, ok, post};
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn a_request_and_a_response_carry_bodies_both_ways() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                // Draining the request body matters beyond checking it: a handler that
                // ignores it never returns the flow-control credit the client needs to
                // finish sending, and a large enough request would stall instead of failing.
                let (parts, incoming) = request.into_parts();
                let received = drain(incoming).await.expect("the request body");
                assert_eq!(parts.method, "POST");
                assert_eq!(received.as_ref(), b"the request body");
                ok(Bytes::from_static(b"the response body"))
            });

            let response = timeout(
                LIMIT,
                sender.send_request(post("https://qmux.test/echo", "the request body")),
            )
            .await
            .expect("the request must not hang")
            .expect("a response");

            assert_eq!(response.status(), 200);
            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");
            assert_eq!(body.as_ref(), b"the response body");
        })
        .await;
}

/// The HTTP/3 half of the transport-level data → boundary → close trace.
///
/// `translation.rs` observes the exact QMux events. This test keeps the HTTP/3 driver in the
/// path and proves that a response head settles and its nonempty final data remains readable.
/// If the stream close overtakes that data, the response future or body fails instead.
#[tokio::test]
async fn a_response_head_and_its_final_data_settle_before_stream_close() {
    LocalSet::new()
        .run_until(async {
            let sender =
                memory_pair(|_request| async move { ok(Bytes::from_static(b"final data")) });

            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/final")))
                .await
                .expect("the response head must not hang")
                .expect("a response head");
            assert_eq!(response.status(), 200);

            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the final data must not hang")
                .expect("the final data");
            assert_eq!(body.as_ref(), b"final data");
        })
        .await;
}

#[tokio::test]
async fn the_first_request_on_a_fresh_connection_completes() {
    // The handshake hazard, isolated. The HTTP/3 layer's first act is to open three
    // unidirectional streams, and it cannot until the peer's transport parameters arrive --
    // which they only do if something reads the byte stream before anything has been asked
    // to write to it. A join that pumped only from its transmit path deadlocks here and
    // nowhere else, because every later request finds the connection already going.
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|_request| async move { ok("first") });

            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
                .await
                .expect("the first request must not hang")
                .expect("a response");

            assert_eq!(response.status(), 200);
            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");
            assert_eq!(body.as_ref(), b"first");
        })
        .await;
}
