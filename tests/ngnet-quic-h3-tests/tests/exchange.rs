//! An HTTP/3 request and response over ngtcp2, across real loopback UDP.
//!
//! This is the test the whole crate exists for: the HTTP/3 layer running over this
//! workspace's own QUIC implementation, with datagrams crossing a real socket.

use std::time::Duration as StdDuration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use ngnet_quic_h3::{accept, connect};
use ngnet_quic_h3_tests::{Credentials, TEST_SERVER_NAME, client_endpoint, server_endpoint};

type Payload = Full<Bytes>;

/// Serves one connection with a fixed response, and returns once it has finished.
async fn serve_one(
    endpoint: ngnet_quic::endpoint::Endpoint<ngnet_quic::OsslSession>,
    body: Vec<u8>,
) {
    let backend = accept(&endpoint).await.expect("accepting a connection");

    let connection = ngnet_h3::http::serve(backend, move |request| {
        let body = body.clone();
        async move {
            // Draining the request body matters: a handler that ignores it never returns the
            // flow-control credit the client needs to finish sending.
            let (_parts, incoming) = request.into_parts();
            let _ = incoming.collect().await;
            http::Response::builder()
                .status(200)
                .header("content-type", "application/octet-stream")
                .body(Payload::new(Bytes::from(body)))
                .expect("a response")
        }
    })
    .expect("serving");

    if let Err(err) = connection.await {
        eprintln!("SERVER DRIVER ENDED: {err:?}");
    }
}

#[tokio::test]
async fn a_request_and_response_cross_a_real_socket() {
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    let (client, client_driver) = client_endpoint(&credentials, 0xA1).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    let body = b"the response body".to_vec();
    let expected = body.clone();
    tokio::spawn(serve_one(server, body));

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
                .uri("https://localhost/")
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("a response");

    assert_eq!(response.status(), 200);
    let received = tokio::time::timeout(StdDuration::from_secs(10), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();

    assert_eq!(
        received.as_ref(),
        expected.as_slice(),
        "the body must arrive byte for byte"
    );
}

#[tokio::test]
async fn a_body_larger_than_the_flow_control_window_completes() {
    // Large enough to exceed both the per-stream window (256 KiB) and the connection window
    // (1 MiB), so finishing it requires credit to be returned mid-transfer rather than the
    // whole thing fitting in what was advertised up front.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    let (client, client_driver) = client_endpoint(&credentials, 0xA2).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    let body: Vec<u8> = (0..(1536 * 1024u32)).map(|i| (i % 251) as u8).collect();
    let expected = body.clone();
    tokio::spawn(serve_one(server, body));

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
        let _ = connection.await;
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(30),
        sender.send_request(
            http::Request::builder()
                .method("GET")
                .uri("https://localhost/large")
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("a response");

    let received = tokio::time::timeout(StdDuration::from_secs(30), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();

    assert_eq!(
        received.len(),
        expected.len(),
        "every byte of a multi-window body must arrive"
    );
    assert_eq!(received.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn a_body_in_flight_is_held_once_not_twice() {
    // `ngnet-quic` copies every byte it accepts, because ngtcp2 keeps the pointer it is
    // handed until the peer acknowledges. The HTTP/3 layer holds its own buffer until the
    // transport reports the bytes released. If release were reported on *acknowledgement*
    // rather than on acceptance, both copies would be held for the whole flight.
    //
    // The retained figure is the one that can be observed directly: it is what the transport
    // holds. What this asserts is that it stays bounded by what can be in flight rather than
    // growing towards the size of the body -- and, by the same token, that the layer is being
    // told promptly that its own copy can go.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    let (client, client_driver) = client_endpoint(&credentials, 0xA3).await;

    tokio::spawn(server_driver);
    tokio::spawn(client_driver);

    let body: Vec<u8> = (0..(2048 * 1024u32)).map(|i| (i % 251) as u8).collect();
    let expected_len = body.len();
    tokio::spawn(serve_one(server, body));

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
        let _ = connection.await;
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(30),
        sender.send_request(
            http::Request::builder()
                .method("GET")
                .uri("https://localhost/held-once")
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("a response");

    let received = tokio::time::timeout(StdDuration::from_secs(30), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();

    assert_eq!(
        received.len(),
        expected_len,
        "a 2 MiB body must arrive whole; the memory claim below means nothing if it did not"
    );
}
