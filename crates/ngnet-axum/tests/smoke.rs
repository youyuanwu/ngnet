//! One real request, over one real TCP connection, through a real axum `Router`.
//!
//! This is the phase-one proof that the integration works at all. It deliberately uses
//! hyper as the *client*: a client this workspace did not write is the only kind that can
//! show `ngnet-h2` speaking HTTP/2 rather than agreeing with itself. hyper is a
//! dev-dependency for exactly this reason and appears nowhere in the normal graph.

use std::net::{Ipv4Addr, SocketAddr};

use axum::Router;
use axum::routing::get;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2 as hyper_client;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};
use ngnet_axum::serve_connection;
use ngnet_h2::http::Config;
use tokio::net::{TcpListener, TcpStream};

/// Serves exactly one connection with `router` and returns once it has finished.
///
/// Binding on port zero and reporting the bound address keeps the test independent of what
/// else is running on the machine.
async fn serve_one(router: Router) -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a bound listener");
    let address = listener.local_addr().expect("a bound address");

    tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.expect("an accepted connection");
        stream.set_nodelay(true).expect("nodelay");
        let connection = serve_connection(stream, router, peer, Config::default())
            .expect("a started connection");
        let _ = connection.await;
    });

    address
}

/// A hyper HTTP/2 client on a fresh connection, with its driver already spawned.
async fn connect(address: SocketAddr) -> hyper_client::SendRequest<Empty<Bytes>> {
    let stream = TcpStream::connect(address).await.expect("a connection");
    stream.set_nodelay(true).expect("nodelay");

    let (sender, connection) = hyper_client::Builder::new(TokioExecutor::new())
        .handshake(HyperIo::new(stream))
        .await
        .expect("a client handshake");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    sender
}

fn request(address: SocketAddr, path: &str) -> http::Request<Empty<Bytes>> {
    http::Request::builder()
        .uri(format!("http://{address}{path}"))
        .body(Empty::new())
        .expect("a request")
}

/// A routed request, an unrouted one, and the headers axum put on both.
///
/// The header assertions are the interesting part. `ngnet-h2` synthesises neither
/// `content-type` nor `content-length`, so their presence proves these are axum's own
/// headers arriving unaltered rather than something the HTTP/2 layer invented.
#[tokio::test]
async fn routes_a_request_and_forwards_axums_own_headers() {
    let router = Router::new()
        .route("/hello", get(|| async { "hello" }))
        .route("/other", get(|| async { "other" }));

    let address = serve_one(router).await;
    let mut sender = connect(address).await;

    let response = sender
        .send_request(request(address, "/hello"))
        .await
        .expect("a response");

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .expect("a content-type axum set"),
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .expect("a content-length axum set"),
        "5"
    );

    let body = response
        .into_body()
        .collect()
        .await
        .expect("a complete body")
        .to_bytes();
    assert_eq!(body, Bytes::from_static(b"hello"));
}

/// A path the router does not know answers 404, from axum's fallback rather than from an
/// HTTP/2 error. Routing decisions have to survive the substitution too, not just success.
#[tokio::test]
async fn an_unrouted_path_gets_axums_fallback() {
    let router = Router::new().route("/hello", get(|| async { "hello" }));

    let address = serve_one(router).await;
    let mut sender = connect(address).await;

    let response = sender
        .send_request(request(address, "/nowhere"))
        .await
        .expect("a response");

    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
}
