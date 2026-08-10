//! Shared harness for the acceptance suite.
//!
//! Every test here drives a real HTTP/2 client over a real loopback TCP connection, because
//! the claims being made are about the wire. The client is hyper's, deliberately: a client
//! from this workspace could only show `ngnet-h2` agreeing with itself.
//!
//! Two names collide in these tests and the alias below is not cosmetic. `TokioIo` exists in
//! both `ngnet_h2::http::transport` (the transport the crate under test wraps sockets in)
//! and `hyper_util::rt` (the one the test client needs). They are different types with the
//! same name; only the hyper one appears here, aliased to say so.

#![allow(dead_code)] // Each integration test file uses a different part of this.

use std::future::{Future, IntoFuture};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::client::conn::http2 as hyper_client;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};
use ngnet_axum::{Config, Error, serve};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The bound every test that could hang is given.
///
/// A stalled connection, an unreported failure or a server future that never resolves would
/// otherwise hang CI with no indication of which test stopped. This turns each into a
/// failure that names itself. Generous enough that a loaded machine does not trip it.
pub const LIMIT: Duration = Duration::from_secs(10);

/// Fails with a message rather than hanging, if `work` outlives [`LIMIT`].
pub async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
    match tokio::time::timeout(LIMIT, work).await {
        Ok(value) => value,
        Err(_) => panic!("timed out waiting for {what}"),
    }
}

/// A running server, its address, and everything it reported.
pub struct TestServer {
    pub address: SocketAddr,
    errors: Arc<Mutex<Vec<Error>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Starts a server on an ephemeral loopback port with the default configuration.
    pub async fn start(router: Router) -> Self {
        Self::start_with(router, Config::default()).await
    }

    /// Starts a server whose connections use `config`.
    pub async fn start_with(router: Router, config: Config) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");

        let errors = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&errors);
        let (stop, stopped) = oneshot::channel();

        let task = tokio::spawn(
            serve(listener, router)
                .config(config)
                .on_error(move |error| sink.lock().expect("a lock").push(error))
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .into_future(),
        );

        Self {
            address,
            errors,
            stop: Some(stop),
            task: Some(task),
        }
    }

    /// How many failures the server has reported so far.
    pub fn reported(&self) -> usize {
        self.errors.lock().expect("a lock").len()
    }

    /// Waits until the server has reported at least `count` failures, or fails.
    ///
    /// Polling rather than sleeping a fixed interval: the report happens on the accept
    /// loop's task and there is no synchronisation point a test can wait on, so the choice
    /// is between polling with a bound and guessing a duration. Polling turns a slow machine
    /// into a slower test rather than a flaky one.
    pub async fn await_reports(&self, count: usize) {
        within(&format!("{count} reported failure(s)"), async {
            while self.reported() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
    }

    /// Runs `inspect` over everything reported so far.
    pub fn with_errors<T>(&self, inspect: impl FnOnce(&[Error]) -> T) -> T {
        inspect(&self.errors.lock().expect("a lock"))
    }

    /// Signals the server to stop accepting, then waits for it to finish.
    pub async fn shutdown(mut self) {
        self.signal_stop();
        self.finished().await;
    }

    /// Signals the server to stop accepting without waiting for it.
    pub fn signal_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    /// Waits for the server future to resolve.
    ///
    /// The handle is *borrowed*, not taken, and cleared only once the wait succeeds. A
    /// caller may legitimately wrap this in a `timeout` to assert the server has *not*
    /// finished yet, and cancelling the returned future must not consume the handle: taking
    /// it up front would detach the task and make every later call return instantly, which
    /// silently turns the subsequent "and now it does finish" assertion into a no-op.
    pub async fn finished(&mut self) {
        if let Some(task) = self.task.as_mut() {
            within("the server to finish", task)
                .await
                .expect("the server task not to panic");
        }
        self.task = None;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // A test that fails part-way through should not leave a server running behind it.
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// An HTTP/2 client on a fresh connection, with its driver spawned.
///
/// `B` is the request body type, so a test can send [`Empty`], [`Full`], or something that
/// yields chunks over time.
pub struct Client<B> {
    pub sender: hyper_client::SendRequest<B>,
    pub local: SocketAddr,
    driver: JoinHandle<()>,
}

impl<B> Client<B>
where
    B: http_body::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// Connects and completes the HTTP/2 handshake.
    pub async fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).await.expect("a connection");
        let local = stream.local_addr().expect("a local address");
        stream.set_nodelay(true).expect("nodelay");

        let (sender, connection) = hyper_client::Builder::new(TokioExecutor::new())
            .handshake(HyperIo::new(stream))
            .await
            .expect("a client handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            sender,
            local,
            driver,
        }
    }

    /// Sends `request` and reads the response to completion.
    ///
    /// Returning the head and the collected body, rather than the response, is the harness
    /// enforcing something that cost an afternoon to diagnose. hyper's `Incoming` keeps its
    /// connection alive while it is outstanding, so a test that asserts only on the status
    /// and lets the response fall out of scope later still has an open connection when it
    /// asks the server to stop -- and [`TestServer::shutdown`] then waits the full timeout
    /// for a peer that has not, in fact, gone away. The server is behaving exactly as
    /// documented; the test is lying about the client. The failure surfaces as a timeout in
    /// `shutdown` with nothing to connect it to the response, so the harness removes the
    /// opportunity instead of leaving a comment about it. Tests that need to observe a body
    /// arriving in pieces use [`Client::sender`] directly and take on the obligation.
    pub async fn exchange(
        &self,
        request: http::Request<B>,
    ) -> (http::response::Parts, bytes::Bytes) {
        let response = within("a response", self.sender.clone().send_request(request))
            .await
            .expect("a response");
        let (head, body) = response.into_parts();
        let bytes = within("a complete body", body.collect())
            .await
            .expect("a complete body")
            .to_bytes();
        (head, bytes)
    }

    /// Ends the connection and its driver, as a client going away abruptly would.
    pub fn disconnect(self) {
        drop(self.sender);
        self.driver.abort();
    }
}

/// A GET with no body, addressed at `path` on `address`.
pub fn get(address: SocketAddr, path: &str) -> http::Request<Empty<Bytes>> {
    http::Request::builder()
        .uri(format!("http://{address}{path}"))
        .body(Empty::new())
        .expect("a request")
}

/// A POST carrying `body`.
pub fn post(address: SocketAddr, path: &str, body: Bytes) -> http::Request<Full<Bytes>> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{address}{path}"))
        .body(Full::new(body))
        .expect("a request")
}

/// Interprets a collected body as text.
pub fn text(body: &Bytes) -> &str {
    std::str::from_utf8(body).expect("utf-8")
}
