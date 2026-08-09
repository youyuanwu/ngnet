//! Quiescence: what the stop signal does, and what it deliberately does not (SC-007).
//!
//! The method is called `with_stop_signal` rather than `with_graceful_shutdown` because it
//! is not a drain. It stops the server accepting; established connections continue until
//! their peers end them. Nothing tells a connected peer to wind up, because `ngnet-h2`
//! exposes no server-side way to say so. These tests pin the semantics that exist rather
//! than the ones the name `graceful` would imply.

mod support;

use std::time::Duration;

use axum::Router;
use axum::routing::get;
use support::{Client, TestServer, get as get_request, text, within};

/// The accept loop serves more than one connection, which is the difference between
/// `serve_connection` and a server.
#[tokio::test]
async fn the_accept_loop_serves_successive_connections() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let server = TestServer::start(router).await;

    for attempt in 0..3 {
        let client = Client::connect(server.address).await;
        let (head, body) = client.exchange(get_request(server.address, "/hello")).await;
        assert_eq!(head.status, http::StatusCode::OK, "connection {attempt}");
        assert_eq!(text(&body), "world");
        client.disconnect();
    }

    assert_eq!(server.reported(), 0);
    server.shutdown().await;
}

/// After the stop signal, an established connection still answers (SC-007).
///
/// This is the positive half of quiescence and the half that is not racy: whether a *new*
/// connection is refused depends on what the kernel has already queued in the accept
/// backlog, but whether an existing one keeps working is a property of the server alone.
#[tokio::test]
async fn a_stopped_server_still_answers_an_established_connection() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let mut server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    // Established before the stop, and answered after it.
    let (head, body) = client.exchange(get_request(server.address, "/hello")).await;
    assert_eq!(head.status, http::StatusCode::OK);

    server.signal_stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (head, body_after) = client.exchange(get_request(server.address, "/hello")).await;
    assert_eq!(
        head.status,
        http::StatusCode::OK,
        "an established connection stopped being served after the stop signal"
    );
    assert_eq!(text(&body_after), text(&body));

    // And the server finishes once that peer goes away -- the other half of the contract.
    client.disconnect();
    server.finished().await;
}

/// The server future resolves once the last connection ends, and not before (SC-007).
///
/// The ordering is the claim. Asserting only that it eventually resolves would pass against
/// a server that dropped its connections the moment it was told to stop, which is the
/// behaviour quiescence exists to avoid.
#[tokio::test]
async fn the_server_future_waits_for_the_last_connection() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let mut server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;
    let (head, _) = client.exchange(get_request(server.address, "/hello")).await;
    assert_eq!(head.status, http::StatusCode::OK);

    server.signal_stop();

    // Still running, because the peer has not gone away.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let premature = tokio::time::timeout(Duration::from_millis(200), server.finished()).await;
    assert!(
        premature.is_err(),
        "the server finished while a connection was still established"
    );

    client.disconnect();
    within("the server to finish", server.finished()).await;
}

/// A new connection is not served after the stop signal.
///
/// A negative about sockets, and therefore best-effort: the kernel may already have accepted
/// a connection into the backlog before the listener was dropped, so a client can succeed in
/// connecting and then find nothing answering. The load-bearing assertion is that no
/// *request* is answered, which holds either way; the connection attempt itself is allowed
/// to succeed or fail.
#[tokio::test]
async fn a_new_connection_is_not_served_after_the_stop_signal() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let mut server = TestServer::start(router).await;
    let address = server.address;

    // Establish and finish one connection first, so the listener is definitely up and the
    // stop signal is definitely the reason for what follows.
    let client = Client::connect(address).await;
    let (head, _) = client.exchange(get_request(address, "/hello")).await;
    assert_eq!(head.status, http::StatusCode::OK);
    client.disconnect();

    server.signal_stop();
    within("the server to finish", server.finished()).await;

    // The server is gone. Connecting may still appear to work if the kernel queued it, but
    // no request can be answered.
    // Connected by hand rather than through the harness, because every step here is
    // allowed to fail and the harness rightly treats a failed connect as a broken test.
    let answered = tokio::time::timeout(Duration::from_secs(2), async {
        let stream = tokio::net::TcpStream::connect(address).await.ok()?;
        let (mut sender, connection) =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .handshake::<_, http_body_util::Empty<bytes::Bytes>>(hyper_util::rt::TokioIo::new(
                    stream,
                ))
                .await
                .ok()?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        sender
            .send_request(get_request(address, "/hello"))
            .await
            .ok()
    })
    .await;

    // Refused connection, failed handshake, errored request, or nothing at all within the
    // bound -- all are the server not serving. Only a real response is a failure.
    if let Ok(Some(response)) = answered {
        panic!(
            "a stopped server answered a new request with {}",
            response.status()
        );
    }
}

/// A stop signal that is *already* resolved beats a connection that is already queued
/// (FR-011).
///
/// This is about arbitration, not about sockets. A flat `select!` picks at random among
/// ready branches, so a server told to stop while a client sits in the kernel's backlog
/// would admit that client half the time -- and then, because stopping only quiesces, wait
/// for a connection it should never have accepted.
///
/// The assertion is that the server finishes *while the peers are still connected*. That
/// can only happen if none of them was served: a connection the loop accepted would keep
/// the server alive until its peer went away, and these peers never do. Ten rounds because
/// a random choice would have to lose every one of them to slip through.
#[tokio::test]
async fn an_already_stopped_server_admits_no_queued_connection() {
    use std::net::Ipv4Addr;
    use tokio::net::{TcpListener, TcpStream};

    for round in 0..10 {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");

        // Queue peers *before* the server runs, so the accept branch is ready on the very
        // first pass, at the same moment the stop signal is.
        let mut queued = Vec::new();
        for _ in 0..4 {
            queued.push(TcpStream::connect(address).await.expect("a queued peer"));
        }

        let router = Router::new().route("/hello", get(|| async { "world" }));
        let server = tokio::spawn(
            ngnet_axum::serve(listener, router)
                .with_stop_signal(std::future::ready(()))
                .into_future(),
        );

        within(
            &format!("round {round}: the server to finish without serving a queued peer"),
            server,
        )
        .await
        .expect("the server task not to panic");

        // Held to the end on purpose: the peers must still be alive for the wait above to
        // mean anything.
        drop(queued);
    }
}
