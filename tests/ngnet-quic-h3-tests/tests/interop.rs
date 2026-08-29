//! Interoperating with a foreign QUIC implementation.
//!
//! Every test written against `ngnet-quic` until now has had ngtcp2 on both ends, which
//! cannot detect a wire-format or transport-parameter defect that both ends share. These run
//! it against quinn.
//!
//! Deliberately sequenced. The bare QUIC handshake comes first, with no HTTP/3 involved at
//! all, so that a failure's domain is identifiable: if the handshake fails, nothing above it
//! is worth reading, and if it succeeds then an HTTP/3 failure is an HTTP/3 failure.

use std::time::Duration as StdDuration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use ngnet_quic_h3::connect;
use ngnet_quic_h3_tests::{
    Credentials, H3_ALPN, TEST_SERVER_NAME, client_endpoint, quinn_client, quinn_server,
    server_endpoint,
};

type Payload = Full<Bytes>;

/// Binds a quinn server on an ephemeral loopback port.
fn quinn_listener(credentials: &Credentials) -> (quinn::Endpoint, core::net::SocketAddr) {
    let endpoint = quinn::Endpoint::server(
        quinn_server(credentials),
        "127.0.0.1:0".parse().expect("valid"),
    )
    .expect("a quinn server endpoint");
    let address = endpoint.local_addr().expect("a bound address");
    (endpoint, address)
}

#[tokio::test]
async fn a_bare_quic_handshake_completes_against_quinn() {
    // First, and separately from HTTP/3. This is where a transport-parameter or wire-format
    // defect surfaces, and knowing it passed is what makes an HTTP/3 failure meaningful.
    let credentials = Credentials::generate();
    let (quinn_endpoint, address) = quinn_listener(&credentials);

    let accepting = tokio::spawn(async move {
        let incoming = quinn_endpoint
            .accept()
            .await
            .expect("an incoming connection");
        let connection = incoming.await.expect("a completed handshake");
        // Hold the endpoint open for the length of the test.
        (connection, quinn_endpoint)
    });

    let (client, client_driver) = client_endpoint(&credentials, 0xB1).await;
    tokio::spawn(client_driver);

    let connection = tokio::time::timeout(
        StdDuration::from_secs(10),
        client.connect(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the handshake must not hang")
    .expect("ngtcp2 must complete a handshake with quinn");

    assert!(connection.is_established());

    let (peer, _endpoint) = tokio::time::timeout(StdDuration::from_secs(10), accepting)
        .await
        .expect("quinn must not hang")
        .expect("the acceptor task");

    assert_eq!(
        peer.handshake_data().and_then(|d| d
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .ok()
            .and_then(|d| d.protocol.clone())),
        Some(H3_ALPN.to_vec()),
        "both ends must have negotiated the same application protocol; without one \
         configured on quinn this fails for a reason that has nothing to do with QUIC"
    );
}

#[tokio::test]
async fn a_handshake_against_quinn_is_refused_when_the_certificate_is_not_trusted() {
    // So the positive results are known not to come from verification being switched off.
    let served = Credentials::generate();
    let (quinn_endpoint, address) = quinn_listener(&served);
    tokio::spawn(async move {
        if let Some(incoming) = quinn_endpoint.accept().await {
            let _ = incoming.await;
        }
        // Keep the endpoint alive until the client has given up.
        tokio::time::sleep(StdDuration::from_secs(3)).await;
    });

    // A different certificate: the client trusts something the server does not present.
    let trusted = Credentials::generate();
    let (client, client_driver) = client_endpoint(&trusted, 0xB2).await;
    tokio::spawn(client_driver);

    let outcome = tokio::time::timeout(
        StdDuration::from_secs(10),
        client.connect(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the attempt must not hang");

    assert!(
        outcome.is_err(),
        "a server presenting an untrusted certificate must be refused"
    );
}

#[tokio::test]
async fn an_http3_request_completes_from_this_stack_to_quinn() {
    // ngtcp2 as the client, quinn as the server, with `ngnet-h3` driving both ends -- the
    // HTTP/3 layer is the constant, and the transport underneath it is what differs.
    let credentials = Credentials::generate();
    let (quinn_endpoint, address) = quinn_listener(&credentials);

    let body = b"served over quinn".to_vec();
    let expected = body.clone();

    tokio::spawn(async move {
        let incoming = quinn_endpoint
            .accept()
            .await
            .expect("an incoming connection");
        let connection = incoming.await.expect("a completed handshake");
        let backend = ngnet_h3_quinn::QuinnBackend::new(connection);
        let served = ngnet_h3::http::serve(backend, move |request| {
            let body = body.clone();
            async move {
                let (_parts, incoming) = request.into_parts();
                let _ = incoming.collect().await;
                http::Response::builder()
                    .status(200)
                    .body(Payload::new(Bytes::from(body)))
                    .expect("a response")
            }
        })
        .expect("serving over quinn");
        let _ = served.await;
        drop(quinn_endpoint);
    });

    let (client, client_driver) = client_endpoint(&credentials, 0xB3).await;
    tokio::spawn(client_driver);

    let backend = tokio::time::timeout(
        StdDuration::from_secs(10),
        connect(&client, address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("a connection to quinn");

    let (sender, connection) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(15),
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
    .expect("a response from quinn");

    assert_eq!(response.status(), 200);
    let received = tokio::time::timeout(StdDuration::from_secs(15), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();
    assert_eq!(received.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn an_http3_request_completes_from_quinn_to_this_stack() {
    // The other direction: this stack serving, quinn asking.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);

    let body = b"served over ngtcp2".to_vec();
    let expected = body.clone();

    tokio::spawn(async move {
        let backend = ngnet_quic_h3::accept(&server).await.expect("accepting");
        let served = ngnet_h3::http::serve(backend, move |request| {
            let body = body.clone();
            async move {
                let (_parts, incoming) = request.into_parts();
                let _ = incoming.collect().await;
                http::Response::builder()
                    .status(200)
                    .body(Payload::new(Bytes::from(body)))
                    .expect("a response")
            }
        })
        .expect("serving over ngtcp2");
        let _ = served.await;
    });

    let mut quinn_endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().expect("valid")).expect("a quinn client");
    quinn_endpoint.set_default_client_config(quinn_client(&credentials));

    let connection = tokio::time::timeout(
        StdDuration::from_secs(10),
        quinn_endpoint
            .connect(address, TEST_SERVER_NAME)
            .expect("a connect attempt"),
    )
    .await
    .expect("quinn must not hang")
    .expect("quinn must complete a handshake with ngtcp2");

    let backend = ngnet_h3_quinn::QuinnBackend::new(connection);
    let (sender, driver) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the quinn client");
    tokio::spawn(async move {
        let _ = driver.await;
    });

    let response = tokio::time::timeout(
        StdDuration::from_secs(15),
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
    .expect("a response from ngtcp2");

    assert_eq!(response.status(), 200);
    let received = tokio::time::timeout(StdDuration::from_secs(15), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();
    assert_eq!(received.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn a_multi_packet_payload_crosses_to_quinn_byte_for_byte() {
    // Large enough to require many packets and flow-control updates from the foreign
    // implementation, which is where an accounting difference between the two would show.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);

    let body: Vec<u8> = (0..(512 * 1024u32)).map(|i| (i % 251) as u8).collect();
    let expected = body.clone();

    tokio::spawn(async move {
        let backend = ngnet_quic_h3::accept(&server).await.expect("accepting");
        let served = ngnet_h3::http::serve(backend, move |request| {
            let body = body.clone();
            async move {
                let (_parts, incoming) = request.into_parts();
                let _ = incoming.collect().await;
                http::Response::builder()
                    .status(200)
                    .body(Payload::new(Bytes::from(body)))
                    .expect("a response")
            }
        })
        .expect("serving");
        let _ = served.await;
    });

    let mut quinn_endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().expect("valid")).expect("a quinn client");
    quinn_endpoint.set_default_client_config(quinn_client(&credentials));

    let connection = tokio::time::timeout(
        StdDuration::from_secs(10),
        quinn_endpoint
            .connect(address, TEST_SERVER_NAME)
            .expect("a connect attempt"),
    )
    .await
    .expect("quinn must not hang")
    .expect("a connection");

    let backend = ngnet_h3_quinn::QuinnBackend::new(connection);
    let (sender, driver) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        let _ = driver.await;
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

    assert_eq!(received.len(), expected.len());
    assert_eq!(
        received.as_ref(),
        expected.as_slice(),
        "a payload spanning many packets must cross between implementations unchanged"
    );
}
