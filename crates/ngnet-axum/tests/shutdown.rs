//! Graceful shutdown: what the drain does, and where its edges are (SC-007).
//!
//! `with_graceful_shutdown` stops the server accepting and tells every established peer to
//! wind up: a `GOAWAY` naming the last request that connection will answer. What has to be
//! true, and is pinned here, is all four of it at once -- in-flight requests are answered,
//! later ones are refused, connections close without their peers doing anything, and the
//! server future waits for the last response rather than racing it.
//!
//! One caution for anyone extending this file. Most tests here use hyper as the client, and
//! hyper closes the socket itself when it sees a `GOAWAY` with nothing outstanding -- so it
//! cannot distinguish a server that drains from one that merely waits to be disconnected
//! from. That is not theoretical: reverting the driver's completion signal leaves every such
//! test green. Only `the_drain_ends_a_connection_whose_peer_never_closes`, which uses a
//! socket that never closes, tells them apart.

mod support;

use std::time::Duration;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use support::{Client, TestServer, get as get_request, text, within};
use tokio::sync::Notify;

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

/// An idle connection is closed by the drain, without its peer doing anything (SC-007).
///
/// This is the case that used to hang, and it is the whole reason the drain exists. Under
/// quiescence a peer that connected and then said nothing held the server open for as long
/// as it cared to, because there was no way to tell it to go away. Now it is told.
///
/// The assertion is that the *server* finishes while the client is still very much present
/// -- not disconnected, not dropped, still held live to the end of the test.
#[tokio::test]
async fn an_idle_connection_is_closed_by_the_drain() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let mut server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    // One exchange, so the connection is established and known to work, and then silence.
    let (head, _) = client.exchange(get_request(server.address, "/hello")).await;
    assert_eq!(head.status, http::StatusCode::OK);

    server.signal_stop();
    server.finished().await;

    // Held to the end deliberately: had this been dropped earlier the wait above would have
    // proved nothing, since a vanished peer ends a connection all by itself.
    drop(client);
}

/// The server future resolves once the last request finishes, and not before (SC-007).
///
/// The ordering is the claim. Asserting only that it eventually resolves would pass against
/// a server that dropped its connections the moment it was told to stop -- which is exactly
/// what a drain must not do, and what distinguishes it from closing the sockets.
#[tokio::test]
async fn the_server_future_waits_for_the_last_request() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let router = Router::new().route("/slow", {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        get(move || {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.notify_one();
                release.notified().await;
                "eventually"
            }
        })
    });

    let mut server = TestServer::start(router).await;
    let client = Client::<Empty<Bytes>>::connect(server.address).await;

    let mut sender = client.sender.clone();
    let inflight = {
        let request = get_request(server.address, "/slow");
        tokio::spawn(async move { sender.send_request(request).await })
    };
    within("the handler to start", started.notified()).await;

    server.signal_stop();

    // Still running, because the request it accepted has not been answered yet.
    let premature = tokio::time::timeout(Duration::from_millis(300), server.finished()).await;
    assert!(
        premature.is_err(),
        "the server finished while it still owed a response"
    );

    release.notify_one();
    let response = within("the in-flight response", inflight)
        .await
        .expect("the task not to panic")
        .expect("a response");
    assert_eq!(response.status(), http::StatusCode::OK);

    within("the server to finish", server.finished()).await;
    drop(client);
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
/// would admit that client half the time -- and then immediately drain a connection it
/// should never have accepted, having served it nothing.
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
            ngnet_axum::serve(ngnet_axum::TcpListener::new(listener), router)
                .with_graceful_shutdown(std::future::ready(()))
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

/// A request in flight when the drain begins is still answered in full (SC-007).
///
/// This is the claim that separates a drain from a disconnect, and the ordering is the
/// whole of it: the handler is held open until the drain has definitely been asked for, so
/// a shutdown that cancelled in-flight work, or that let the connection close before the
/// response reached the wire, fails here rather than intermittently somewhere else.
#[tokio::test]
async fn a_request_in_flight_when_the_drain_starts_is_still_answered() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let router = Router::new().route("/slow", {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        get(move || {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.notify_one();
                release.notified().await;
                "answered anyway"
            }
        })
    });

    let mut server = TestServer::start(router).await;
    let client = Client::<Empty<Bytes>>::connect(server.address).await;

    let mut sender = client.sender.clone();
    let inflight = {
        let request = get_request(server.address, "/slow");
        tokio::spawn(async move { sender.send_request(request).await })
    };

    // Everything below is strictly after the request reached the handler.
    within("the handler to start", started.notified()).await;

    server.signal_stop();

    // Let the drain reach the wire before the handler answers, so the response genuinely
    // follows the GOAWAY instead of racing it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.notify_one();

    let response = within("the in-flight response", inflight)
        .await
        .expect("the task not to panic")
        .expect("a response");
    assert_eq!(response.status(), http::StatusCode::OK);

    let body = within("the in-flight body", response.into_body().collect())
        .await
        .expect("a complete body")
        .to_bytes();
    assert_eq!(text(&body), "answered anyway");

    // And the server ends of its own accord, with the client still connected. Before the
    // drain existed this was the case that hung: quiescence had no way to end a connection
    // whose peer was not going anywhere.
    server.finished().await;
    drop(client);
}

/// A request begun after the drain is refused rather than served.
///
/// The peer is told which streams will be honoured, and a request above that mark has to be
/// turned away — otherwise "drain" would mean "keep serving until the client loses
/// interest", and the connection would never reach an empty registry to close on.
///
/// What the client observes is asserted loosely on purpose: hyper may surface the refusal
/// as a transport error or as a `REFUSED_STREAM`, and pinning the shape of somebody else's
/// error would break on their refactorings. What matters here is that no handler ran.
#[tokio::test]
async fn a_request_begun_after_the_drain_is_refused() {
    let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());

    let router = Router::new()
        .route("/slow", {
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            get(move || {
                let started = Arc::clone(&started);
                let gate = Arc::clone(&gate);
                async move {
                    started.notify_one();
                    gate.notified().await;
                    "first"
                }
            })
        })
        .route("/late", {
            let served = Arc::clone(&served);
            get(move || {
                let served = Arc::clone(&served);
                async move {
                    served.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    "should never be served"
                }
            })
        });

    let mut server = TestServer::start(router).await;
    let client = Client::<Empty<Bytes>>::connect(server.address).await;

    // Hold one stream open so the connection is still alive when the late request is made.
    let mut sender = client.sender.clone();
    let inflight = {
        let request = get_request(server.address, "/slow");
        tokio::spawn(async move { sender.send_request(request).await })
    };
    within("the first handler to start", started.notified()).await;

    server.signal_stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now ask for something new on the same connection, after the GOAWAY.
    let late = within(
        "the late request to settle",
        client
            .sender
            .clone()
            .send_request(get_request(server.address, "/late")),
    )
    .await;
    if let Ok(response) = late {
        // If anything came back at all it must not be the handler's work.
        let status = response.status();
        let body = within("the late body", response.into_body().collect())
            .await
            .map(|collected| collected.to_bytes())
            .unwrap_or_default();
        assert_ne!(
            text(&body),
            "should never be served",
            "a request begun after the drain reached its handler (status {status})"
        );
    }

    assert_eq!(
        served.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the handler for a stream begun after the drain must not run"
    );

    gate.notify_one();
    let first = within("the first response", inflight)
        .await
        .expect("the task not to panic")
        .expect("a response");
    assert_eq!(first.status(), http::StatusCode::OK);

    server.finished().await;
    drop(client);
}

/// The drain ends a connection whose peer ignores the GOAWAY (SC-007).
///
/// Every other test here uses hyper as the client, and hyper is well behaved: it sees the
/// GOAWAY, has nothing outstanding, and closes the socket -- at which point the server ends
/// because it read EOF, which it would have done with or without a drain. That makes those
/// tests silent about the half of this change that lives in `ngnet-h2`, and this one exists
/// because inverting that half left all of them passing.
///
/// So the peer here is a bare socket that completes the HTTP/2 handshake and then does
/// nothing at all: it never sends another frame and, crucially, never closes. Against such a
/// peer a server that only stops when it is disconnected from waits forever. The assertion
/// is that this server hangs up on it instead.
#[tokio::test]
async fn the_drain_ends_a_connection_whose_peer_never_closes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let router = Router::new().route("/hello", get(|| async { "world" }));
    let mut server = TestServer::start(router).await;

    let mut socket = tokio::net::TcpStream::connect(server.address)
        .await
        .expect("a connection");

    // The client preface, then an empty SETTINGS frame: length 0, type 0x04, no flags, on
    // the connection stream. Enough to be a real HTTP/2 peer, and nothing more.
    socket
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .expect("the preface to be sent");
    socket
        .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
        .await
        .expect("settings to be sent");
    socket.flush().await.expect("a flush");

    // Let the accept loop take the connection before asking it to stop.
    tokio::time::sleep(Duration::from_millis(150)).await;

    server.signal_stop();

    // Read to EOF. Whatever the server chooses to send on the way out is its business; the
    // claim is only that the stream ends, and that we did not end it.
    let mut sink = Vec::new();
    let closed = within("the server to close the connection", async {
        loop {
            let mut chunk = [0u8; 1024];
            match socket.read(&mut chunk).await {
                Ok(0) => break true,
                Ok(n) => sink.extend_from_slice(&chunk[..n]),
                Err(_) => break false,
            }
        }
    })
    .await;
    assert!(closed, "the server did not close the connection");

    server.finished().await;

    // Never closed by us, at any point.
    drop(socket);
}
