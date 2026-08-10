//! Routing, state and middleware over the wire (SC-001, SC-012).
//!
//! These pin the claim the crate exists to make: an *unmodified* axum `Router` — its
//! matcher, its method filter, its extractors, its state and its layers — behaves the same
//! when hyper is not underneath it. Everything is asserted from what a real HTTP/2 client
//! received, never from calling the `Router` directly, because calling it directly would
//! test axum and not this crate.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use support::{Client, TestServer, get as get_request, post as post_request, text};

#[tokio::test]
async fn a_routed_request_reaches_its_handler() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let (response, response_body) = client.exchange(get_request(server.address, "/hello")).await;

    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(text(&response_body), "world");

    client.disconnect();
    server.shutdown().await;
}

#[tokio::test]
async fn path_extractors_and_request_bodies_arrive_intact() {
    let router = Router::new().route(
        "/echo/{name}",
        post(|Path(name): Path<String>, body: String| async move { format!("{name}:{body}") }),
    );
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let request = post_request(server.address, "/echo/alice", "payload".into());
    let (response, response_body) = client.exchange(request).await;

    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(text(&response_body), "alice:payload");

    client.disconnect();
    server.shutdown().await;
}

/// axum's fallback, not ours.
///
/// Worth asserting separately from the routed case: a naive integration that dispatched on
/// its own before consulting the `Router` would still pass the routed test.
#[tokio::test]
async fn an_unrouted_path_gets_axums_own_404() {
    let router = Router::new().route("/hello", get(|| async { "world" }));
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let (response, response_body) = client
        .exchange(get_request(server.address, "/nowhere"))
        .await;

    assert_eq!(response.status, http::StatusCode::NOT_FOUND);
    assert!(response_body.is_empty());

    client.disconnect();
    server.shutdown().await;
}

/// The method filter, including the `allow` header axum generates with the 405.
///
/// The header is the discriminating part. A 405 could be produced by accident; a correct
/// `allow` listing only the methods that route can serve could only come from axum's own
/// method router being consulted.
#[tokio::test]
async fn a_wrong_method_gets_405_with_an_allow_header() {
    let router = Router::new().route("/only-get", get(|| async { "yes" }));
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let request = post_request(server.address, "/only-get", "ignored".into());
    let (response, _response_body) = client.exchange(request).await;

    assert_eq!(response.status, http::StatusCode::METHOD_NOT_ALLOWED);
    let allow = response
        .headers
        .get(http::header::ALLOW)
        .expect("an allow header")
        .to_str()
        .expect("ascii");
    assert!(allow.contains("GET"), "allow was {allow:?}");
    assert!(!allow.contains("POST"), "allow was {allow:?}");

    client.disconnect();
    server.shutdown().await;
}

/// Response headers a handler set arrive unaltered, and the crate adds no framing header of
/// its own (SC-001).
///
/// `content-length` is the one to watch. axum sets it for a known-length body, and an
/// integration that re-derived framing -- or that dropped headers while converting a
/// response -- would show up here rather than in a status-code assertion. The handler sets
/// `content-type` by hand rather than through `axum::Json`, which would mean enabling
/// axum's `json` feature and taking on `serde_json` for one header.
#[tokio::test]
async fn a_handlers_own_response_headers_survive_the_trip() {
    let router = Router::new().route(
        "/typed",
        get(|| async {
            (
                [(http::header::CONTENT_TYPE, "application/json")],
                r#"{"ok":"yes"}"#,
            )
        }),
    );
    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    let (response, response_body) = client.exchange(get_request(server.address, "/typed")).await;

    let headers = response.headers;
    assert_eq!(
        headers
            .get(http::header::CONTENT_TYPE)
            .expect("a content-type"),
        "application/json"
    );
    let declared: usize = headers
        .get(http::header::CONTENT_LENGTH)
        .expect("a content-length")
        .to_str()
        .expect("ascii")
        .parse()
        .expect("a number");

    assert_eq!(
        declared,
        response_body.len(),
        "content-length disagreed with the body"
    );
    assert_eq!(text(&response_body), r#"{"ok":"yes"}"#);

    client.disconnect();
    server.shutdown().await;
}

/// A router carrying state, behind a layer (SC-012).
///
/// State and middleware are the two axum features most likely to be broken by an integration
/// that reconstructs requests rather than passing them through: state lives in the `Router`
/// itself and middleware sees the request on its way in. The counter proves the layer ran on
/// the same request the handler saw.
#[tokio::test]
async fn state_and_middleware_work_unchanged() {
    #[derive(Clone)]
    struct Counter(Arc<AtomicUsize>);

    let seen = Arc::new(AtomicUsize::new(0));
    let layer_seen = Arc::clone(&seen);

    let router = Router::new()
        .route(
            "/count",
            get(|State(Counter(count)): State<Counter>| async move {
                format!("{}", count.load(Ordering::SeqCst))
            }),
        )
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let layer_seen = Arc::clone(&layer_seen);
                async move {
                    layer_seen.fetch_add(1, Ordering::SeqCst);
                    next.run(request).await
                }
            },
        ))
        .with_state(Counter(Arc::clone(&seen)));

    let server = TestServer::start(router).await;
    let client = Client::connect(server.address).await;

    // The layer increments before the handler reads, so the first request sees 1.
    let (first, first_body) = client.exchange(get_request(server.address, "/count")).await;
    assert_eq!(text(&first_body), "1");
    assert_eq!(first.status, http::StatusCode::OK);

    let (second, second_body) = client.exchange(get_request(server.address, "/count")).await;
    assert_eq!(text(&second_body), "2");
    assert_eq!(second.status, http::StatusCode::OK);

    client.disconnect();
    server.shutdown().await;
}
