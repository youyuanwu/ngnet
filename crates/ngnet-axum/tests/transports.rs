//! The seam is generic: transports that are not TCP are served by the same call.
//!
//! These are the tests that make the abstraction worth having. Everything else in this
//! suite runs over TCP, which would be equally true of a crate that had merely renamed its
//! socket parameter -- so the claim under test here is that a listener the crate was not
//! designed around goes through unchanged.
//!
//! Two non-TCP listeners appear. The Unix one is shipped, and is the case that proves a
//! peer address which is neither `Copy` nor `Display` survives the whole path. The
//! in-memory one is defined here in the test, and is the more interesting of the two: it is
//! written by an author outside the crate, using only the crate's public API, over a
//! transport that is not a socket at all. If that compiles and serves, the seam is real.

mod support;

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use axum::extract::Extension;
use axum::routing::get;
use axum::{Router, http::Request};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2 as hyper_client;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};
use ngnet_axum::{FallibleListener, PeerAddr, RetryingListener, serve};
use ngnet_h2::http::transport::TokioIo;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::{mpsc, oneshot};

use support::{LIMIT, within};

/// How a third-party listener is written, using only this crate's public API.
///
/// It hands out in-memory pipes rather than sockets, so nothing here touches the network.
/// Note what is absent: no retry, no error classification, no timing, no yielding. Those
/// come from wrapping it in [`RetryingListener`], which is the whole point of shipping that
/// type -- the safe path is meant to be the short one.
struct DuplexAcceptor {
    incoming: mpsc::Receiver<DuplexStream>,
    next: u64,
}

/// A peer address that is not a socket address, not `Copy`, and not `Display`.
///
/// Deliberately awkward on all three axes, because each corresponds to a bound that would
/// have been easy to require by accident and that a Unix-domain address would then have
/// failed. A newtype over `SocketAddr` would have proven nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PipeId(String);

impl FallibleListener for DuplexAcceptor {
    type Io = TokioIo<DuplexStream>;
    type Addr = PipeId;

    async fn accept(&mut self) -> std::io::Result<(Self::Io, Self::Addr)> {
        let stream = self.incoming.recv().await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "no more clients")
        })?;
        self.next += 1;
        Ok((TokioIo::new(stream), PipeId(format!("pipe-{}", self.next))))
    }
}

/// SC-001, SC-017: a listener over a transport that is not a socket serves requests.
///
/// The listener is written here rather than in the crate, so this also pins that the public
/// API is sufficient to write one from outside.
#[tokio::test]
async fn a_third_party_in_memory_listener_serves_requests() {
    let (clients, incoming) = mpsc::channel(4);
    let listener = RetryingListener::new(DuplexAcceptor { incoming, next: 0 });

    let router = Router::new().route(
        "/whoami",
        get(|Extension(PeerAddr(id)): Extension<PeerAddr<PipeId>>| async move { id.0 }),
    );

    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(
        serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    clients.send(server_side).await.expect("the listener to be up");

    let (mut sender, connection) = hyper_client::handshake(TokioExecutor::new(), HyperIo::new(client_side))
        .await
        .expect("an HTTP/2 handshake over an in-memory pipe");
    tokio::spawn(connection);

    let response = within(
        "a response over an in-memory transport",
        sender.send_request(
            Request::builder()
                // HTTP/2 requires an authority, and an in-memory pipe has no address to
                // derive one from -- so it gets a synthetic one. That is itself a small
                // demonstration of the point: the transport need not have a network
                // identity for the crate to serve over it.
                .uri("http://in-memory.invalid/whoami")
                .body(Empty::<bytes::Bytes>::new())
                .expect("a request"),
        ),
    )
    .await
    .expect("a response");

    let body = response.into_body().collect().await.expect("a body").to_bytes();

    // SC-012's in-memory counterpart: the handler read a peer address of the listener's own
    // type out of the request extensions, with no socket anywhere in the path.
    assert_eq!(
        &body[..],
        b"pipe-1",
        "the handler should have seen the listener's own address type"
    );

    let _ = stop.send(());
    within("the in-memory server to drain", server)
        .await
        .expect("the server task to finish");
}

/// SC-013: a failure over a listener whose address is not `Copy` reports that address.
///
/// The client is dropped mid-connection so the connection fails, and the address the
/// failure names has to be the listener's type rather than something erased to a string.
#[tokio::test]
async fn a_failure_reports_a_peer_address_that_is_not_a_socket_address() {
    let (clients, incoming) = mpsc::channel(4);
    let listener = RetryingListener::new(DuplexAcceptor { incoming, next: 0 });

    let errors: Arc<Mutex<Vec<PipeId>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);

    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(
        serve(listener, Router::new())
            .on_error(move |error| sink.lock().expect("a lock").push(error.peer().0.clone()))
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    let (server_side, mut client_side) = tokio::io::duplex(64 * 1024);
    clients.send(server_side).await.expect("the listener to be up");

    // Garbage where the HTTP/2 connection preface should be. The session fails after the
    // connection has been accepted and spawned, which is the path that has to carry the
    // peer address back out through the JoinSet -- the interesting one, and the one where
    // a generic address could have been dropped on the floor.
    client_side
        .write_all(b"not an HTTP/2 preface at all\r\n\r\n")
        .await
        .expect("the pipe to accept a write");
    client_side.flush().await.expect("a flush");

    let deadline = tokio::time::Instant::now() + LIMIT;
    let reported = loop {
        {
            let seen = errors.lock().expect("a lock");
            if let Some(first) = seen.first() {
                break first.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server never reported the failed in-memory connection"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    assert_eq!(
        reported,
        PipeId("pipe-1".to_owned()),
        "the failure should name the peer with the listener's own address type, not a string"
    );

    let _ = stop.send(());
    let _ = within("the server to drain", server).await;
}

/// SC-020: a listener whose source has gone away does not yield and does not block shutdown.
///
/// The channel is closed immediately, so the acceptor fails on every call with a
/// non-transient error and the retry wrapper paces it. A server that treated a listener's
/// permanent failure as a reason to spin, or that could not be stopped while a listener was
/// mid-backoff, would show up here.
#[tokio::test]
async fn a_listener_whose_source_is_gone_neither_yields_nor_blocks_shutdown() {
    let (clients, incoming) = mpsc::channel::<DuplexStream>(1);
    drop(clients);
    let listener = RetryingListener::new(DuplexAcceptor { incoming, next: 0 });

    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(
        serve(listener, Router::new())
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    // Long enough that the listener is certainly inside a backoff sleep when the stop
    // arrives -- which is SC-019: the stop is honoured without waiting the backoff out.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let _ = stop.send(());
    within(
        "a server on a dead listener to stop promptly rather than waiting out its backoff",
        server,
    )
    .await
    .expect("the server task to finish");
}

/// SC-012, SC-018: the shipped Unix listener serves, exposes its address type, and drains.
///
/// Unlike the in-memory listener above, this one is shipped rather than written in the
/// test, so it is the case a user actually gets. It is also where the peer address is most
/// obviously not a `SocketAddr`: a client that has not bound a path is unnamed, and there is
/// no address that could have been invented for it.
#[cfg(unix)]
#[tokio::test]
async fn the_unix_listener_serves_and_drains() {
    use axum::http::Request;
    use tokio::net::UnixStream;

    let directory = std::env::temp_dir().join(format!("ngnet-axum-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join("test.sock");
    let _ = std::fs::remove_file(&path);

    let bound = tokio::net::UnixListener::bind(&path).expect("a bound unix listener");
    let listener = ngnet_axum::UnixListener::new(bound);

    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let recorder = Arc::clone(&seen);

    let router = Router::new().route(
        "/whoami",
        get(
            move |Extension(peer): Extension<PeerAddr<tokio::net::unix::SocketAddr>>| {
                let recorder = Arc::clone(&recorder);
                async move {
                    // The address type is the listener's own, not a socket address. Rendered
                    // with `Debug` because a Unix address implements no `Display`.
                    *recorder.lock().expect("a lock") = Some(format!("{:?}", peer.0));
                    "unix"
                }
            },
        ),
    );

    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(
        serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
    );

    let client_side = UnixStream::connect(&path).await.expect("a connected client");
    let (mut sender, connection) =
        hyper_client::handshake(TokioExecutor::new(), HyperIo::new(client_side))
            .await
            .expect("an HTTP/2 handshake over a unix socket");
    tokio::spawn(connection);

    let response = within(
        "a response over a unix socket",
        sender.send_request(
            Request::builder()
                .uri("http://unix.invalid/whoami")
                .body(Empty::<bytes::Bytes>::new())
                .expect("a request"),
        ),
    )
    .await
    .expect("a response");

    let body = response.into_body().collect().await.expect("a body").to_bytes();
    assert_eq!(&body[..], b"unix");

    assert!(
        seen.lock().expect("a lock").is_some(),
        "the handler should have read a unix peer address from the request extensions"
    );

    // SC-018: shutdown drains rather than cancels, over a listener that is not TCP.
    let _ = stop.send(());
    within("the unix server to drain", server)
        .await
        .expect("the server task to finish");

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&directory);
}
