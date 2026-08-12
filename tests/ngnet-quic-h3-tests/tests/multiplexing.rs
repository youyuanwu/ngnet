//! Many requests, many connections, and the behaviours whose absence is silence.
//!
//! The multiplexing tests establish that the endpoint restructuring kept what it was
//! supposed to keep. The behaviour tests cover properties that fail quietly — a connection
//! that establishes and then stalls looks like a hang, not an error, and several defects of
//! exactly that shape have already been found in this code.

use std::time::Duration as StdDuration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use ngnet_quic_h3::{accept, connect};
use ngnet_quic_h3_tests::{Credentials, TEST_SERVER_NAME, client_endpoint, server_endpoint};

type Payload = Full<Bytes>;

/// Serves every connection the endpoint accepts, echoing the request path back.
fn serve_echo(endpoint: ngnet_quic::endpoint::Endpoint<ngnet_quic::OsslSession>) {
    tokio::spawn(async move {
        loop {
            let Ok(backend) = accept(&endpoint).await
            else {
                break;
            };
            tokio::spawn(async move {
                let served = ngnet_h3::http::serve(backend, |request| async move {
                    let path = request.uri().path().to_string();
                    let (_parts, incoming) = request.into_parts();
                    let _ = incoming.collect().await;
                    http::Response::builder()
                        .status(200)
                        .body(Payload::new(Bytes::from(path)))
                        .expect("a response")
                })
                .expect("serving");
                let _ = served.await;
            });
        }
    });
}

/// Opens a client and returns a request sender.
async fn sender(
    credentials: &Credentials,
    address: core::net::SocketAddr,
    seed: u64,
) -> ngnet_h3::http::SendRequest<Payload> {
    let (client, client_driver) = client_endpoint(credentials, seed).await;
    tokio::spawn(client_driver);
    let backend = tokio::time::timeout(
        StdDuration::from_secs(10),
        connect(&client, address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("connecting must not hang")
    .expect("a connection");
    // The endpoint has to outlive the connection, so it is leaked into the task that drives
    // the exchange rather than dropped here.
    Box::leak(Box::new(client));
    let (sender, connection) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sender
}

async fn get(sender: &ngnet_h3::http::SendRequest<Payload>, path: &str) -> String {
    let response = tokio::time::timeout(
        StdDuration::from_secs(15),
        sender.send_request(
            http::Request::builder()
                .method("GET")
                .uri(format!("https://localhost{path}"))
                .body(Payload::default())
                .expect("a request"),
        ),
    )
    .await
    .expect("the request must not hang")
    .expect("a response");
    let body = tokio::time::timeout(StdDuration::from_secs(15), response.into_body().collect())
        .await
        .expect("the body must not hang")
        .expect("a body")
        .to_bytes();
    String::from_utf8(body.to_vec()).expect("a utf-8 body")
}

#[tokio::test]
async fn concurrent_requests_on_one_connection_each_get_their_own_response() {
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);
    serve_echo(server);

    let sender = sender(&credentials, address, 0xC1).await;

    let mut pending = Vec::new();
    for index in 0..10 {
        let sender = sender.clone();
        pending.push(tokio::spawn(async move {
            let path = format!("/request-{index}");
            let body = get(&sender, &path).await;
            (path, body)
        }));
    }

    for task in pending {
        let (path, body) = task.await.expect("a request task");
        assert_eq!(
            body, path,
            "each response must match the request it answers, not another one"
        );
    }
}

#[tokio::test]
async fn two_clients_on_one_server_endpoint_do_not_see_each_others_bytes() {
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);
    serve_echo(server);

    // Different seeds: the test entropy source is deterministic, so two clients from one
    // seed would mint identical connection identifiers and the server would route one's
    // datagrams to the other. Real endpoints do not have this problem, which is why the
    // builder makes the caller supply the randomness.
    let first = sender(&credentials, address, 0xC201).await;
    let second = sender(&credentials, address, 0xC301).await;

    let (a, b) = tokio::join!(get(&first, "/first"), get(&second, "/second"));

    assert_eq!(a, "/first");
    assert_eq!(b, "/second");
}

#[tokio::test]
async fn a_request_on_a_quiescent_connection_is_carried_promptly() {
    // The connection is established and then left alone before the request is made. Nothing
    // is arriving and no timer is due, so the only thing that can carry the request is the
    // work itself waking the connection. Without that it waits for the idle timeout, which
    // closes the connection rather than serving it.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);
    serve_echo(server);

    let sender = sender(&credentials, address, 0xC4).await;

    // Let the connection go quiet.
    tokio::time::sleep(StdDuration::from_millis(200)).await;

    let started = std::time::Instant::now();
    let body = get(&sender, "/after-a-pause").await;
    let elapsed = started.elapsed();

    assert_eq!(body, "/after-a-pause");
    assert!(
        elapsed < StdDuration::from_secs(2),
        "a request on a quiet connection must be carried in about a round trip, not left \
         until some unrelated event happens; took {elapsed:?}"
    );
}

#[tokio::test]
async fn an_endpoint_serves_http3_and_raw_quic_at_the_same_time() {
    // The property the endpoint split exists to preserve: one socket, consumers of
    // different kinds, neither disturbing the other.
    let credentials = Credentials::generate();
    let (server, server_driver, address) = server_endpoint(&credentials).await;
    tokio::spawn(server_driver);
    serve_echo(server);

    let (client, client_driver) = client_endpoint(&credentials, 0xC5).await;
    tokio::spawn(client_driver);

    // A raw QUIC connection: no HTTP/3 anywhere in it.
    let raw = tokio::time::timeout(
        StdDuration::from_secs(10),
        client.connect(address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the raw connection must not hang")
    .expect("a raw connection");
    assert!(raw.is_established());

    // And an HTTP/3 one, over the same socket.
    let backend = tokio::time::timeout(
        StdDuration::from_secs(10),
        connect(&client, address, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the HTTP/3 connection must not hang")
    .expect("an HTTP/3 connection");

    let (sender, connection) =
        ngnet_h3::http::handshake::<_, Payload>(backend).expect("starting the client");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_eq!(get(&sender, "/both").await, "/both");
    assert!(
        raw.is_established(),
        "the raw connection must be undisturbed by the HTTP/3 one beside it"
    );
}

#[tokio::test]
async fn a_connection_to_nothing_fails_rather_than_hanging() {
    // Deterministic, unlike waiting for a peer to fall silent: nothing is listening, so the
    // handshake can only time out, and it must do so within the configured limit rather than
    // leaving the caller waiting.
    let credentials = Credentials::generate();
    let (client, client_driver) = client_endpoint(&credentials, 0xC601).await;
    tokio::spawn(client_driver);

    // A port nothing is bound to. Bind and release one, so the number is real and free.
    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("binding a probe socket");
    let dead = probe.local_addr().expect("a bound address");
    drop(probe);

    let outcome = tokio::time::timeout(
        StdDuration::from_secs(20),
        connect(&client, dead, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the attempt must give up on its own rather than waiting for this timeout");

    assert!(
        outcome.is_err(),
        "connecting to an address where nothing listens must fail"
    );
}
