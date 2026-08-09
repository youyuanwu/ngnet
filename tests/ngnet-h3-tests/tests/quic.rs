//! The same exchanges the wrapper proves in memory, over a real QUIC connection.
//!
//! These run against a loopback socket with real encryption and real congestion control.
//! What they add over the in-memory suite is not more protocol coverage but a different
//! kind of confidence: that the stream identifiers, the send transaction and the
//! backpressure signals line up with a transport nobody wrote to suit them.

use std::sync::Arc;

use ngnet_h3_tests::{Field, Message, Request, Tuning, echo, exchange};

fn body(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|n| (n as u8).wrapping_add(seed)).collect()
}

#[tokio::test]
async fn a_request_and_response_cross_a_real_quic_connection() {
    let responses = exchange(vec![Request::get("/hello")], echo(), Tuning::roomy())
        .await
        .expect("the exchange should complete");

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].field(":status"), Some("200"));
    assert!(responses[0].ended, "the response stream should have ended");
}

#[tokio::test]
async fn a_body_survives_the_round_trip_unchanged() {
    let payload = body(64 * 1024, 7);
    let responses = exchange(
        vec![Request::post("/echo", payload.clone())],
        echo(),
        Tuning::roomy(),
    )
    .await
    .expect("the exchange should complete");

    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0].body, payload,
        "a body crossing a real connection must arrive byte for byte"
    );
}

#[tokio::test]
async fn trailers_arrive_as_trailers_over_quic() {
    let responder: ngnet_h3_tests::Responder = Arc::new(|request: &Message| {
        (
            200,
            request.body.clone(),
            vec![("x-checksum".to_string(), "deadbeef".to_string())],
        )
    });

    let request = Request::post("/trailed", b"payload".to_vec())
        .with_trailers(vec![("x-request-trailer".to_string(), "sent".to_string())]);
    let responses = exchange(vec![request], responder, Tuning::roomy())
        .await
        .expect("the exchange should complete");

    assert_eq!(responses[0].body, b"payload");
    assert_eq!(
        responses[0].trailers,
        vec![("x-checksum".to_string(), "deadbeef".to_string())],
        "the field must be delivered as a trailer, not folded into the headers"
    );
    assert!(
        !responses[0]
            .headers
            .iter()
            .any(|(name, _)| name == "x-checksum"),
        "and must not also appear as a header"
    );
}

#[tokio::test]
async fn several_concurrent_requests_keep_their_bodies_apart() {
    // Distinct lengths as well as distinct contents, so a body attributed to the wrong
    // stream cannot coincidentally match.
    let requests: Vec<Request> = (0..8u8)
        .map(|n| Request::post(format!("/concurrent/{n}"), body(1000 + n as usize, n)))
        .collect();
    let expected: Vec<Vec<u8>> = requests.iter().map(|r| r.body.clone()).collect();

    let responses = exchange(requests, echo(), Tuning::roomy())
        .await
        .expect("the exchange should complete");

    assert_eq!(responses.len(), 8);
    for (n, (response, want)) in responses.iter().zip(&expected).enumerate() {
        assert_eq!(response.field(":status"), Some("200"), "request {n}");
        assert_eq!(
            &response.body, want,
            "request {n} came back with another request's body"
        );
    }
}

#[tokio::test]
async fn a_cramped_transport_produces_identical_results() {
    // Windows small enough that every body is written in many pieces and streams block
    // and unblock repeatedly. This is where the two-phase send transaction and the
    // block/unblock pair earn their keep, and the result must be indistinguishable.
    let requests: Vec<Request> = (0..4u8)
        .map(|n| Request::post(format!("/cramped/{n}"), body(40 * 1024 + n as usize, n)))
        .collect();
    let expected: Vec<Vec<u8>> = requests.iter().map(|r| r.body.clone()).collect();

    let roomy = exchange(requests.clone(), echo(), Tuning::roomy())
        .await
        .expect("the roomy exchange should complete");
    let cramped = exchange(requests, echo(), Tuning::cramped())
        .await
        .expect("the cramped exchange should complete");

    for (n, want) in expected.iter().enumerate() {
        assert_eq!(&roomy[n].body, want, "roomy request {n}");
    }
    assert_eq!(
        cramped, roomy,
        "a transport that accepts bytes grudgingly must not change a single one of them"
    );
}

#[tokio::test]
async fn an_empty_body_still_ends_the_stream() {
    let responder: ngnet_h3_tests::Responder =
        Arc::new(|_request: &Message| (204, Vec::new(), Vec::<Field>::new()));

    let responses = exchange(vec![Request::get("/nothing")], responder, Tuning::roomy())
        .await
        .expect("the exchange should complete");

    assert_eq!(responses[0].field(":status"), Some("204"));
    assert!(responses[0].body.is_empty());
    assert!(
        responses[0].ended,
        "a response with no body still has to end its stream"
    );
}
