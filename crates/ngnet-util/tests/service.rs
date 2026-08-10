//! The `tower_service::Service` impl, driven end to end.
//!
//! This is not a type-level check. The impl is three lines, and the thing worth testing about
//! three lines that delegate is that they delegate to something that *works* — a `Service`
//! whose `call` returns a future the pool never drives would compile, satisfy every layer
//! that wraps it, and hang.

mod support;

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::{Client, Error};
use support::{TestServer, collect, get, within};
use tower_service::Service;

/// Sends through a `Service` rather than through the concrete client.
///
/// Generic on purpose: it can only see the request through the trait, so nothing here can
/// accidentally reach the inherent `Client::request` and pass while the impl is broken.
async fn send<S>(service: &mut S, request: http::Request<Full<Bytes>>) -> Result<S::Response, Error>
where
    S: Service<http::Request<Full<Bytes>>, Error = Error>,
{
    std::future::poll_fn(|context| service.poll_ready(context)).await?;
    service.call(request).await
}

#[tokio::test]
async fn a_request_sent_through_the_service_trait_is_answered() {
    let server = TestServer::start().await;
    let mut client: Client<Full<Bytes>> = Client::new();

    let response = within(
        "the request",
        send(&mut client, get(server.uri("/traited"))),
    )
    .await
    .expect("the request succeeds");

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(collect(response).await, Bytes::from_static(b"/traited"));
    assert_eq!(server.accepts(), 1);
}

#[tokio::test]
async fn the_service_shares_the_pool_with_the_inherent_api() {
    // A `Service` impl that built its own pool per call would pass the test above. This is
    // the one that catches it: two requests, one through each API, one connection.
    let server = TestServer::start().await;
    let mut client: Client<Full<Bytes>> = Client::new();

    let _ = within(
        "the inherent request",
        client.request(get(server.uri("/a"))),
    )
    .await
    .expect("succeeds");
    let _ = within(
        "the service request",
        send(&mut client, get(server.uri("/b"))),
    )
    .await
    .expect("succeeds");

    assert_eq!(
        server.accepts(),
        1,
        "the Service impl must use the same pool as the inherent API"
    );
}

#[tokio::test]
async fn readiness_dials_nothing() {
    // `poll_ready` is asked before the request exists, so it cannot know which origin it
    // would be getting ready for. It must therefore reserve nothing — and, in particular,
    // must not dial the last origin used, or anything else.
    let server = TestServer::start().await;
    let mut client: Client<Full<Bytes>> = Client::new();

    for _ in 0..5 {
        std::future::poll_fn(|context| client.poll_ready(context))
            .await
            .expect("always ready");
    }

    assert_eq!(server.accepts(), 0);
    assert_eq!(ngnet_util::testing::resolution_count(&client), 0);
}

#[tokio::test]
async fn a_service_error_is_this_crate_s_error() {
    // The associated type is part of the API: a caller writing a tower layer over this has
    // to be able to match on what comes out of it.
    let mut client: Client<Full<Bytes>> = Client::new();
    let uri: http::Uri = "/relative".parse().expect("a valid relative URI");

    let error: Error = within("the request", send(&mut client, get(uri)))
        .await
        .expect_err("a relative URI has no origin");
    assert_eq!(error.kind(), ngnet_util::ErrorKind::Uri);
}
