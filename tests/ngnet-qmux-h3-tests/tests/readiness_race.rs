//! Bounded multi-thread readiness races for the QMux/H3 pump boundary.

use std::sync::Arc;

use bytes::Bytes;
use ngnet_qmux_h3_tests::{LIMIT, Payload, drain, get, ok, tcp_pair, tokio_clock};
use tokio::sync::Barrier;
use tokio::time::timeout;

/// Two peer responses become writable together while the client driver is active.
///
/// This stays deliberately below the unrelated high-concurrency multi-worker backlog case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_peer_writes_racing_a_productive_turn_both_make_progress() {
    let (client_io, server_io) = tcp_pair().await;
    let barrier = Arc::new(Barrier::new(2));

    let server = ngnet_qmux_h3::serve(server_io, tokio_clock(), move |request| {
        let barrier = Arc::clone(&barrier);
        async move {
            let path = request.uri().path().as_bytes().to_vec();
            barrier.wait().await;
            ok(Bytes::from(path))
        }
    })
    .expect("serving");
    let serving = tokio::spawn(server);

    let (sender, connection) = ngnet_qmux_h3::connect::<_, _, Payload>(client_io, tokio_clock())
        .expect("starting the client");
    let driving = tokio::spawn(connection);

    let exchange = async {
        let (first, second) = tokio::join!(
            sender.send_request(get("https://qmux.test/first")),
            sender.send_request(get("https://qmux.test/second")),
        );
        let first = first.expect("the first response");
        let second = second.expect("the second response");
        let (first, second) = tokio::join!(drain(first.into_body()), drain(second.into_body()));
        assert_eq!(
            first.expect("the first body"),
            Bytes::from_static(b"/first")
        );
        assert_eq!(
            second.expect("the second body"),
            Bytes::from_static(b"/second")
        );
    };
    timeout(LIMIT, exchange)
        .await
        .expect("both raced peer writes must wake and complete");

    drop(sender);
    assert!(
        timeout(LIMIT, driving)
            .await
            .expect("the client driver must finish")
            .expect("the client task")
            .is_ok()
    );
    assert!(
        timeout(LIMIT, serving)
            .await
            .expect("the server driver must finish")
            .expect("the server task")
            .is_ok()
    );
}
