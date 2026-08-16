//! HTTP/3 over QMux over a real socket.
//!
//! The in-memory tests prove the protocol logic; this one proves the seam. A loopback TCP
//! stream reads short, writes short and delivers records split at arbitrary points, and it
//! is driven by a work-stealing runtime rather than by a single-threaded `LocalSet` — so it
//! is also where the claim that a `Send` byte stream yields a `Send` connection is checked,
//! by `tokio::spawn` refusing to compile if it were not.

use bytes::Bytes;
use ngnet_qmux_h3_tests::{LIMIT, Payload, drain, get, ok, pattern, tcp_pair, tokio_clock};
use tokio::time::timeout;

/// Large enough to need many records and a window extension, so the socket is exercised
/// rather than merely touched.
const SIZE: usize = 512 * 1024;

// Multi-threaded deliberately: the two ends then run on different threads, so the connection
// really is moved across one rather than merely satisfying a bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_completes_over_loopback_tcp() {
    let (client_io, server_io) = tcp_pair().await;

    let server = ngnet_qmux_h3::serve(server_io, tokio_clock(), |request| async move {
        let received = drain(request.into_body()).await.expect("the request body");
        assert_eq!(
            received,
            pattern(SIZE),
            "the request body must arrive whole"
        );
        ok(pattern(SIZE))
    })
    .expect("serving");
    let serving = tokio::spawn(server);

    let (sender, connection) = ngnet_qmux_h3::connect::<_, _, Payload>(client_io, tokio_clock())
        .expect("starting the client");
    let driving = tokio::spawn(connection);

    let request = http::Request::builder()
        .method("POST")
        .uri("https://qmux.test/loopback")
        .body(Payload::new(pattern(SIZE)))
        .expect("a request");

    // The handle is dropped with the exchange still in flight. That is what lets the client
    // driver wind down when the exchange finishes: it discovers there are no handles left on
    // the pass that completes the request, rather than having to be woken by a handle that
    // was dropped while it was parked with nothing to do.
    let response = sender.send_request(request);
    drop(sender);

    let response = timeout(LIMIT, response)
        .await
        .expect("the request must not hang")
        .expect("a response");
    assert_eq!(response.status(), 200);
    let body = timeout(LIMIT, drain(response.into_body()))
        .await
        .expect("the body must not hang")
        .expect("a body");
    assert_eq!(body, pattern(SIZE), "the response body must arrive whole");

    let client = timeout(LIMIT, driving)
        .await
        .expect("the client connection must finish")
        .expect("the client task");
    assert!(
        client.is_ok(),
        "the client connection ended cleanly: {client:?}"
    );

    let served = timeout(LIMIT, serving)
        .await
        .expect("the served connection must finish")
        .expect("the server task");
    assert!(
        served.is_ok(),
        "the served connection ended cleanly: {served:?}"
    );
}

/// SC-027. A client that vanishes is the connection ending, not a protocol failure.
///
/// A socket that simply closes is the ordinary end of a connection over the public internet:
/// a process exits, a laptop lid shuts, a middlebox gives up. A server that reported each of
/// those as a protocol failure would fill its logs with alarms about its own users' network
/// conditions, and a monitor built on that signal would be useless.
#[tokio::test]
async fn a_client_that_disappears_is_not_a_protocol_failure() {
    let (client_io, server_io) = tcp_pair().await;

    let server = ngnet_qmux_h3::serve(server_io, tokio_clock(), |request| async move {
        let _ = drain(request.into_body()).await;
        ok(Bytes::from_static(b"answered"))
    })
    .expect("serving");
    let serving = tokio::spawn(server);

    let (sender, connection) = ngnet_qmux_h3::connect::<_, _, Payload>(client_io, tokio_clock())
        .expect("starting the client");
    let driving = tokio::spawn(connection);

    let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
        .await
        .expect("the request must not hang")
        .expect("a response");
    assert_eq!(response.status(), 200);

    // No close, no shutdown, no goodbye. Aborting the task drops the driver and with it the
    // socket, which is as close to a disappearing client as a test can arrange.
    driving.abort();
    drop(sender);
    drop(response);

    let served = timeout(LIMIT, serving)
        .await
        .expect("the served connection must notice and finish rather than wait forever")
        .expect("the server task");
    assert!(
        served.is_ok(),
        "a client that hung up must be the end of the connection and nothing worse: {served:?}",
    );
}
