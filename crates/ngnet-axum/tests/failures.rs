//! Failure isolation and reporting (SC-008, SC-009, SC-018, US-6).
//!
//! The claim under test is that a connection is the unit of failure. One peer speaking the
//! wrong protocol, one handler panicking, one client vanishing mid-request: each costs that
//! connection and nothing else, and each is reported once with the peer that caused it.
//!
//! **A panic in these tests is raised inside a handler future and must stay there.** A panic
//! raised inside an outgoing body's `poll_frame` is pulled synchronously from within an
//! `extern "C"` callback and aborts the process rather than unwinding — see the crate's
//! documentation on how panics differ by layer. A future maintainer "simplifying" one of
//! these tests by panicking from a body would replace a passing test with a test binary that
//! dies without explanation.

mod support;

use std::net::SocketAddr;

use axum::Router;
use axum::routing::get;
use support::{Client, TestServer, get as get_request, text, within};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// A peer that is not speaking HTTP/2 costs its own connection and no other (SC-008).
///
/// The impostor is a raw socket writing an HTTP/1.1 request line rather than a hyper HTTP/1
/// client, so that exactly one connection is at stake and the assertion is about that
/// connection's reported failure. `ngnet-h2` is cleartext h2c with no negotiation and no
/// fallback, so this is an error by design rather than a missing feature.
#[tokio::test]
async fn an_http1_speaker_fails_only_its_own_connection() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let server = TestServer::start(router).await;

    let mut impostor = TcpStream::connect(server.address)
        .await
        .expect("a connection");
    impostor
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("a written request");
    impostor.flush().await.expect("a flush");

    server.await_reports(1).await;
    server.with_errors(|errors| {
        assert_eq!(errors.len(), 1, "expected exactly one failure");
        // The peer is the client's ephemeral address, not the server's, so the assertion
        // is on what is knowable: it is a real loopback address rather than a placeholder.
        assert!(
            SocketAddr::from(errors[0].peer_addr()).ip().is_loopback(),
            "a connection failure should name its peer, got {:?}",
            errors[0].peer()
        );
    });

    // The server is still serving. This is the half that matters: reporting a failure is
    // worth little if the failure took the accept loop with it.
    let client = Client::connect(server.address).await;
    let (head, body) = client.exchange(get_request(server.address, "/hello")).await;
    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(text(&body), "world");
    assert_eq!(
        server.reported(),
        1,
        "serving a good connection reported a failure"
    );

    drop(impostor);
    client.disconnect();
    server.shutdown().await;
}

/// A handler that panics, written as a named function so that its declared return type
/// gives axum a response type to reason about. An `async` block whose body diverges has type
/// `!`, which does not implement `IntoResponse`.
async fn always_panics() -> &'static str {
    panic!("deliberate: exercising SC-009 -- this panic is expected")
}

/// A panicking handler costs its connection, and the server keeps serving others (SC-009).
///
/// The panic is raised in the handler future, where it unwinds out of the driver, fails the
/// connection, and reaches the task boundary as a `JoinError`. See the module note: it must
/// not be moved into a body.
#[tokio::test]
async fn a_panicking_handler_does_not_stop_the_server() {
    let router = Router::new()
        .route("/panic", get(always_panics))
        .route("/fine", get(|| async { "fine" }));
    let server = TestServer::start(router).await;

    // A connection of its own, so that the panic's blast radius is observable. The request
    // is expected to fail; what it fails with is hyper's business, not this crate's.
    let doomed = Client::connect(server.address).await;
    let doomed_peer = doomed.local;
    let outcome = within(
        "the doomed request to resolve",
        doomed
            .sender
            .clone()
            .send_request(get_request(server.address, "/panic")),
    )
    .await;
    assert!(outcome.is_err(), "a panicking handler answered a request");

    server.await_reports(1).await;
    server.with_errors(|errors| {
        assert_eq!(
            SocketAddr::from(errors[0].peer_addr()),
            doomed_peer,
            "the failure named the wrong peer"
        );
    });

    // A different connection is unaffected.
    let healthy = Client::connect(server.address).await;
    let (head, body) = healthy.exchange(get_request(server.address, "/fine")).await;
    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(text(&body), "fine");

    doomed.disconnect();
    healthy.disconnect();
    server.shutdown().await;
}

/// A client that vanishes mid-request ends its own connection, quietly, and takes its
/// in-flight handler with it (US-6).
///
/// This comes from a user story that never became a success criterion of its own, which is
/// precisely why it was nearly lost. Two things it pins are worth stating because neither is
/// the obvious guess:
///
/// * **No failure is reported.** A peer that closes its connection is not a server error,
///   even with an exchange outstanding, so nothing reaches the observation point. An
///   implementation that reported it would flood a busy server's logs with the ordinary
///   behaviour of clients.
/// * **The handler is dropped, not resumed and not cancelled.** Handlers run inside the
///   connection future; when that future is done, an outstanding handler is dropped at its
///   next suspension point. It does not observe `Cancelled`, and code after its current
///   `await` never runs. Handlers must therefore put cleanup in `Drop`, not after the await
///   -- which is why this test proves the drop rather than assuming it.
#[tokio::test]
async fn a_client_that_disappears_mid_request_is_not_an_error_and_drops_its_handler() {
    /// Reports through the channel when it is dropped, whether or not the handler finished.
    struct Guard(tokio::sync::mpsc::UnboundedSender<()>);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    let (entered, mut handler_entered) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (dropped, mut handler_dropped) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (finished, mut handler_finished) = tokio::sync::mpsc::unbounded_channel::<()>();

    let router = Router::new()
        .route(
            "/park",
            get(move || {
                let entered = entered.clone();
                let guard = Guard(dropped.clone());
                let finished = finished.clone();
                async move {
                    let _guard = guard;
                    let _ = entered.send(());
                    // Long enough that the disconnect certainly lands first. The test never
                    // waits for this to elapse; if the handler ever gets past it, that is
                    // itself the failure the next line reports.
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    let _ = finished.send(());
                    "never sent"
                }
            }),
        )
        .route("/fine", get(|| async { "fine" }));
    let server = TestServer::start(router).await;

    let vanishing = Client::connect(server.address).await;
    let request = tokio::spawn({
        let mut sender = vanishing.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/park")).await }
    });
    within("the handler to start", handler_entered.recv())
        .await
        .expect("the handler to start");

    request.abort();
    vanishing.disconnect();

    within("the handler to be dropped", handler_dropped.recv())
        .await
        .expect("the handler to be dropped");
    assert!(
        handler_finished.try_recv().is_err(),
        "the handler ran to completion after its client had gone"
    );
    assert_eq!(
        server.reported(),
        0,
        "a client closing its own connection was reported as a server failure"
    );

    let survivor = Client::connect(server.address).await;
    let (head, body) = survivor
        .exchange(get_request(server.address, "/fine"))
        .await;
    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(text(&body), "fine");
    assert_eq!(server.reported(), 0);

    survivor.disconnect();
    server.shutdown().await;
}

/// Reported failures name the peer they came from, across several of them (SC-018).
///
/// One failure could name the right peer by luck if the implementation always reported the
/// most recent accept. Two concurrent bad connections cannot both be explained that way.
#[tokio::test]
async fn every_reported_failure_names_its_own_peer() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let server = TestServer::start(router).await;

    let mut impostors = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let mut socket = TcpStream::connect(server.address)
            .await
            .expect("a connection");
        addresses.push(socket.local_addr().expect("a local address"));
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("a written request");
        impostors.push(socket);
    }

    server.await_reports(3).await;
    let reported: Vec<SocketAddr> = server.with_errors(|errors| {
        errors
            .iter()
            .map(|error| SocketAddr::from(error.peer_addr()))
            .collect()
    });

    for address in &addresses {
        assert!(
            reported.contains(address),
            "no failure was reported for {address}, only {reported:?}"
        );
    }

    drop(impostors);
    server.shutdown().await;
}
