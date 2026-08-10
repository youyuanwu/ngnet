//! Request extensions a handler can rely on (SC-006, SC-011).

mod support;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::routing::get;
use ngnet_axum::PeerAddr;
use support::{Client, TestServer, get as get_request, text, within};
use tokio::sync::{mpsc, oneshot};

/// A handler sees the address of the peer that connected, and it is the *client's* address
/// rather than the server's (SC-006).
///
/// Comparing against the client socket's own local address is what makes this meaningful. A
/// test that only checked "some address arrived" would pass against an implementation that
/// inserted the listener's address, which is the mistake worth catching.
#[tokio::test]
async fn a_handler_sees_the_peer_address() {
    let router = Router::new().route(
        "/peer",
        get(|request: Request| async move {
            let peer = request
                .extensions()
                .get::<PeerAddr>()
                .copied()
                .expect("a peer address");
            peer.to_string()
        }),
    );

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;
    let expected = client.local;

    let (head, body) = client.exchange(get_request(server.address, "/peer")).await;

    assert_eq!(head.status, http::StatusCode::OK);
    assert_eq!(
        text(&body),
        expected.to_string(),
        "the handler saw an address that was not the client's"
    );
    assert_ne!(
        text(&body),
        server.address.to_string(),
        "the handler saw the listener's address, not the peer's"
    );

    client.disconnect();
    server.shutdown().await;
}

/// `ngnet-h2`'s cancellation signal reaches a handler *and fires* (SC-011).
///
/// Presence alone is not worth asserting. An implementation that inserted an inert signal —
/// one constructed on the spot rather than taken from the stream that carries it — would
/// pass a presence check and mislead every handler that trusted it. So the client resets the
/// stream while the handler is still running, and the handler must observe
/// `Cancelled::cancelled()` resolving. That is a claim only a live signal can satisfy.
#[tokio::test]
async fn the_cancellation_signal_fires_when_the_client_gives_up() {
    let (entered, mut handler_entered) = mpsc::unbounded_channel::<()>();
    let (observed, was_cancelled) = oneshot::channel::<bool>();
    let observed = Arc::new(std::sync::Mutex::new(Some(observed)));

    let router = Router::new().route(
        "/slow",
        get(move |request: Request| {
            let entered = entered.clone();
            let observed = Arc::clone(&observed);
            async move {
                let cancelled = request
                    .extensions()
                    .get::<ngnet_h2::http::Cancelled>()
                    .cloned()
                    .expect("a cancellation signal");
                let _ = entered.send(());

                // Resolves when the peer resets the stream. The timeout is the failure
                // path, not the success path: if the signal never fires the handler still
                // finishes and the test reports what happened rather than hanging.
                let fired = tokio::time::timeout(Duration::from_secs(5), cancelled.cancelled())
                    .await
                    .is_ok();
                if let Some(observed) = observed.lock().expect("a lock").take() {
                    let _ = observed.send(fired);
                }
                "unreachable in practice"
            }
        }),
    );

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let request = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/slow")).await }
    });

    // Only abandon the request once the handler is definitely running; resetting a stream
    // whose handler has not started would prove nothing about the signal reaching it.
    within("the handler to start", handler_entered.recv())
        .await
        .expect("the handler to start");
    request.abort();

    let fired = within("the handler's verdict", was_cancelled)
        .await
        .expect("a verdict");
    assert!(
        fired,
        "the handler was given a cancellation signal that never fired"
    );

    client.disconnect();
    server.shutdown().await;
}
