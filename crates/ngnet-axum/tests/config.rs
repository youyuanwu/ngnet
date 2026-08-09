//! Connection configuration and what it does — and does not — enforce (SC-013).

mod support;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use bytes::Bytes;
use http_body_util::Empty;
use ngnet_axum::Config;
use support::{Client, TestServer, get as get_request, text, within};
use tokio::sync::{mpsc, oneshot};

/// A router whose handler announces its entry and then parks until released.
///
/// Returning the entry channel lets a test assert on the *order handlers began*, which is
/// the property that distinguishes a concurrency limit from a coincidence.
fn parking_router() -> (Router, mpsc::UnboundedReceiver<u8>, ReleaseAll) {
    let (entered, entries) = mpsc::unbounded_channel::<u8>();
    let (first_gate, first_wait) = oneshot::channel::<()>();
    let first_wait = Arc::new(tokio::sync::Mutex::new(Some(first_wait)));

    let router = Router::new()
        .route(
            "/first",
            get({
                let entered = entered.clone();
                move || {
                    let entered = entered.clone();
                    let first_wait = Arc::clone(&first_wait);
                    async move {
                        let _ = entered.send(1);
                        if let Some(gate) = first_wait.lock().await.take() {
                            let _ = gate.await;
                        }
                        "first"
                    }
                }
            }),
        )
        .route(
            "/second",
            get(move || {
                let entered = entered.clone();
                async move {
                    let _ = entered.send(2);
                    "second"
                }
            }),
        );

    (router, entries, ReleaseAll(Some(first_gate)))
}

struct ReleaseAll(Option<oneshot::Sender<()>>);

impl ReleaseAll {
    fn release(&mut self) {
        if let Some(gate) = self.0.take() {
            let _ = gate.send(());
        }
    }
}

/// The control for the test below: by default, handlers run concurrently.
///
/// Without this, `a_concurrency_limit_of_one_serialises_handlers` would be indistinguishable
/// from a server that never runs handlers concurrently at all, and would pass whether or not
/// the configuration was applied. `ngnet-h2` runs server handlers concurrently without
/// spawning them, so the second handler is expected to start while the first is parked.
#[tokio::test]
async fn by_default_handlers_run_concurrently() {
    let (router, mut entries, mut release) = parking_router();
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let first = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/first")).await }
    });
    assert_eq!(
        within("the first handler", entries.recv()).await,
        Some(1),
        "the first handler did not start"
    );

    // The first handler is parked. A concurrent server starts the second anyway.
    let second = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/second")).await }
    });
    assert_eq!(
        within("the second handler", entries.recv()).await,
        Some(2),
        "the second handler did not start while the first was parked"
    );

    release.release();
    drain(first).await;
    drain(second).await;

    client.disconnect();
    server.shutdown().await;
}

/// With the concurrent-stream limit at one, the second handler cannot begin until the first
/// has finished (SC-013).
///
/// The assertion is about *handler entry ordering*, deliberately, because that holds however
/// the client behaves. A compliant client holds the second request back until the first
/// stream closes; if it sends it anyway the server answers `REFUSED_STREAM`. Asserting on
/// the second response's status would pin whichever of those hyper happens to do.
#[tokio::test]
async fn a_concurrency_limit_of_one_serialises_handlers() {
    let (router, mut entries, mut release) = parking_router();
    let server = TestServer::start_with(router, Config::default().max_concurrent_streams(1)).await;
    let client = Client::connect(server.address).await;

    // Let the server's SETTINGS reach the client before it decides what it may send.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let first = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/first")).await }
    });
    assert_eq!(within("the first handler", entries.recv()).await, Some(1));

    let second = tokio::spawn({
        let mut sender = client.sender.clone();
        let address = server.address;
        async move { sender.send_request(get_request(address, "/second")).await }
    });

    // A bounded wait for something that must not happen. It cannot prove the negative, but
    // it is the only honest way to look for it, and it fails loudly when the limit stops
    // being applied.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        entries.try_recv().is_err(),
        "the second handler started while the first was still running, \
         despite a concurrent-stream limit of one"
    );

    release.release();
    assert_eq!(
        within("the second handler", entries.recv()).await,
        Some(2),
        "the second handler never started after the first finished"
    );

    drain(first).await;
    drain(second).await;
    client.disconnect();
    server.shutdown().await;
}

/// The header-list-size setting is an advertisement, not a limit this server enforces.
///
/// This test asserts the opposite of what the name of the setting suggests, and that is the
/// point of having it. `SETTINGS_MAX_HEADER_LIST_SIZE` is advisory in HTTP/2: it tells a
/// peer what the server would prefer to receive, and a peer that ignores it is not
/// violating the protocol. Measured here rather than assumed: with the value advertised as
/// 256 octets, a request carrying a 64 KiB header field is still routed and answered
/// normally.
///
/// So it must not be used as a defence. A deployment that needs to reject oversized headers
/// needs a check it controls -- a middleware layer on the router, or a limit enforced
/// before the request reaches this crate. Pinning the behaviour in a test means that if
/// `ngnet-h2` ever starts enforcing the value, this test fails and the documentation gets
/// corrected rather than quietly becoming wrong in the safer direction.
#[tokio::test]
async fn the_header_list_size_setting_is_advisory() {
    let router = Router::new().route("/headers", get(|| async { "served" }));
    let server = TestServer::start_with(router, Config::default().max_header_list_size(256)).await;
    let client: Client<Empty<Bytes>> = Client::connect(server.address).await;

    // Long enough for the server's SETTINGS to have been received and, if it were going to
    // be, honoured.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let oversized = "x".repeat(64 * 1024);
    let request = http::Request::builder()
        .uri(format!("http://{}/headers", server.address))
        .header("x-oversized", &oversized)
        .body(Empty::new())
        .expect("a request");

    let (head, body) = client.exchange(request).await;
    assert_eq!(
        head.status,
        http::StatusCode::OK,
        "the advertised header limit was enforced -- the documentation now understates it"
    );
    assert_eq!(text(&body), "served");
    assert_eq!(server.reported(), 0);

    client.disconnect();
    server.shutdown().await;
}

/// Awaits a spawned request and discards its body, so that no response outlives the server.
async fn drain(
    request: tokio::task::JoinHandle<Result<http::Response<hyper::body::Incoming>, hyper::Error>>,
) {
    let response = within("a request task", request)
        .await
        .expect("the request task not to panic")
        .expect("a response");
    let _ = within(
        "a complete body",
        http_body_util::BodyExt::collect(response.into_body()),
    )
    .await;
}
