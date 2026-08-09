//! The server surface: the accept loop, the peer extension and the stop signal.
//!
//! Phase two's own coverage. The full behavioural matrix — isolation, panics, limits,
//! trailers, streaming — is the acceptance suite's job; what is pinned here is that
//! `serve` accepts more than one connection, that handlers can see who they are talking to,
//! and that the stop signal stops accepting without cutting off work in progress.

use std::future::IntoFuture;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::routing::get;
use axum::{Extension, Router};
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2 as hyper_client;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};
use ngnet_axum::{PeerAddr, serve};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// Every test here can deadlock rather than fail if something goes wrong — a connection
/// that never ends, a server future that never resolves. A bound turns that into a failure
/// with a name attached.
const LIMIT: Duration = Duration::from_secs(10);

async fn bind() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a bound listener");
    let address = listener.local_addr().expect("a bound address");
    (listener, address)
}

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

async fn get_text(sender: &mut hyper_client::SendRequest<Empty<Bytes>>, uri: String) -> String {
    let request = http::Request::builder()
        .uri(uri)
        .body(Empty::new())
        .expect("a request");
    let response = sender.send_request(request).await.expect("a response");
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a complete body")
        .to_bytes();
    String::from_utf8(body.to_vec()).expect("utf-8")
}

/// A handler sees the address of the peer that connected, and it is the *client's* address
/// rather than the server's — the distinction a test that only checked "some address
/// arrived" would miss.
#[tokio::test]
async fn a_handler_sees_the_peer_address() {
    let (listener, address) = bind().await;
    let router = Router::new().route(
        "/who",
        get(|Extension(PeerAddr(peer)): Extension<PeerAddr>| async move { peer.to_string() }),
    );

    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(
        serve(listener, router)
            .with_stop_signal(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    let stream = TcpStream::connect(address).await.expect("a connection");
    let client_address = stream.local_addr().expect("a local address");
    stream.set_nodelay(true).expect("nodelay");
    let (mut sender, connection) = hyper_client::Builder::new(TokioExecutor::new())
        .handshake(HyperIo::new(stream))
        .await
        .expect("a client handshake");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let seen = tokio::time::timeout(
        LIMIT,
        get_text(&mut sender, format!("http://{address}/who")),
    )
    .await
    .expect("a response within the limit");

    assert_eq!(seen, client_address.to_string());

    drop(sender);
    let _ = stop.send(());
    tokio::time::timeout(LIMIT, server)
        .await
        .expect("the server to finish within the limit")
        .expect("the server task not to panic");
}

/// The accept loop serves more than one connection, which is the difference between
/// `serve_connection` and a server. Both are answered correctly, in sequence.
#[tokio::test]
async fn the_accept_loop_serves_successive_connections() {
    let (listener, address) = bind().await;
    let router = Router::new().route("/hello", get(|| async { "hello" }));

    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(
        serve(listener, router)
            .with_stop_signal(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    for _ in 0..3 {
        let mut sender = connect(address).await;
        let body = tokio::time::timeout(
            LIMIT,
            get_text(&mut sender, format!("http://{address}/hello")),
        )
        .await
        .expect("a response within the limit");
        assert_eq!(body, "hello");
    }

    let _ = stop.send(());
    tokio::time::timeout(LIMIT, server)
        .await
        .expect("the server to finish within the limit")
        .expect("the server task not to panic");
}

/// After the stop signal, an established connection still works. This is the positive half
/// of quiescence, and the half that is not racy: whether a *new* connection is refused
/// depends on what the kernel has already queued, but whether an existing one keeps
/// answering is a property of the server alone.
#[tokio::test]
async fn a_stopped_server_still_answers_an_established_connection() {
    let (listener, address) = bind().await;
    let router = Router::new().route("/hello", get(|| async { "hello" }));

    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(
        serve(listener, router)
            .with_stop_signal(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    let mut sender = connect(address).await;
    let before = tokio::time::timeout(
        LIMIT,
        get_text(&mut sender, format!("http://{address}/hello")),
    )
    .await
    .expect("a response within the limit");
    assert_eq!(before, "hello");

    let _ = stop.send(());

    let after = tokio::time::timeout(
        LIMIT,
        get_text(&mut sender, format!("http://{address}/hello")),
    )
    .await
    .expect("a response within the limit");
    assert_eq!(after, "hello", "a stopped server still serves what it has");

    // And the server finishes once that peer goes away — the other half of quiescence, and
    // the reason this is not a drain: it is the *client* leaving that ends it.
    drop(sender);
    tokio::time::timeout(LIMIT, server)
        .await
        .expect("the server to finish once its last peer goes")
        .expect("the server task not to panic");
}
