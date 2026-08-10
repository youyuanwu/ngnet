//! Shared harness for the acceptance suite.
//!
//! Every test drives the real client over real loopback TCP against a real HTTP/2 server,
//! because the claims being made are about connections. The server is **hyper's**, and that
//! is the mirror image of `ngnet-axum`'s reasoning rather than a coincidence: there, an
//! independent *client* was needed because the server was under test. Here the client is
//! under test, so the server must be one this workspace did not write — otherwise the suite
//! would show `ngnet-h2` agreeing with itself and nothing more.
//!
//! # Why almost everything is asserted at the server
//!
//! Every plausibly-wrong implementation of a connection pool still returns correct responses.
//! A pool that dials afresh for every request answers each one perfectly. A pool that
//! serialises every origin behind one lock answers them all, slowly. A pool that never
//! evicts a dead connection answers until it doesn't. So the assertions here are on what the
//! *server* saw — how many connections it accepted, in what order requests arrived, which
//! frames it received — and the response is checked only to confirm the request worked at
//! all.
//!
//! The accept counter is the workhorse. `assert_eq!(server.accepts(), 1)` after three
//! requests is the whole of "the connection was reused", and without it the reuse test
//! passes against a client that opens three sockets.
//!
//! # Two `TokioIo` types
//!
//! The alias below is not cosmetic. `TokioIo` exists in both `ngnet_h2::http::transport` (the
//! one the crate under test wraps its sockets in) and `hyper_util::rt` (the one the test
//! server needs). Different types, same name; only hyper's appears here, aliased to say so.

#![allow(dead_code)] // Each integration test file uses a different part of this.

use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http2 as hyper_server;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The bound every test that could hang is given.
///
/// A pool bug's natural failure mode is not a wrong answer but a wait that never ends — a
/// waiter parked behind a dial that is not happening, a shutdown waiting on a caller that has
/// gone. Without a bound those are a hung CI job with no indication of which test stopped.
/// This turns each into a failure that names itself.
pub const LIMIT: Duration = Duration::from_secs(10);

/// Fails with a message rather than hanging, if `work` outlives [`LIMIT`].
pub async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
    match tokio::time::timeout(LIMIT, work).await {
        Ok(value) => value,
        Err(_) => panic!("timed out waiting for {what}"),
    }
}

/// Polls `condition` until it holds, or fails once [`LIMIT`] is up.
///
/// Used where a state change has no completion to await — notably "the client has observed
/// the peer's `GOAWAY`", which the peer cannot tell us about because it learns nothing after
/// sending it. Polling an actual predicate is not the same as sleeping: sleeping asserts that
/// the event probably happened within a guessed interval, which is how a test comes to pass
/// on a fast machine and fail on a loaded one.
pub async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    within(what, async {
        while !condition() {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}

/// What one request arriving at the test server looked like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub path: String,
    pub body: Bytes,
    /// Which accepted connection it arrived on, numbered from 1 in accept order.
    pub connection: usize,
}

/// A hyper HTTP/2 server on an ephemeral loopback port, counting what it sees.
pub struct TestServer {
    pub address: SocketAddr,
    accepts: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Starts a server on IPv4 loopback that answers every request with its own path.
    pub async fn start() -> Self {
        Self::start_on(IpAddr::V4(Ipv4Addr::LOCALHOST)).await
    }

    /// Starts a server on IPv6 loopback.
    pub async fn start_v6() -> Self {
        Self::start_on(IpAddr::V6(Ipv6Addr::LOCALHOST)).await
    }

    /// Starts a server on a *specific* address.
    ///
    /// Needed by the connect suite, which has to fail a dial at an address and then make that
    /// same address work. An ephemeral port would be a different origin and would prove
    /// nothing about the pool's memory of the first one.
    pub async fn start_at(address: SocketAddr) -> Self {
        let listener = TcpListener::bind(address).await.expect("a bound listener");
        Self::from_listener(listener)
    }

    async fn start_on(host: IpAddr) -> Self {
        let listener = TcpListener::bind((host, 0))
            .await
            .expect("a bound listener");
        Self::from_listener(listener)
    }

    fn from_listener(listener: TcpListener) -> Self {
        let address = listener.local_addr().expect("a bound address");

        let accepts = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (stop, mut stopped) = oneshot::channel();

        let accept_sink = Arc::clone(&accepts);
        let seen_sink = Arc::clone(&seen);

        let task = tokio::spawn(async move {
            loop {
                let socket = tokio::select! {
                    // `&mut stopped` and not `stopped`: a `select!` branch that loses is
                    // dropped and rebuilt on the next pass, so moving the receiver in would
                    // recreate a *fresh* one each time round and the stop signal would never
                    // be observed. This is one of the two liveness bugs this repository has
                    // already been bitten by; it is not repeated here.
                    _ = &mut stopped => break,
                    accepted = listener.accept() => match accepted {
                        Ok((socket, _)) => socket,
                        Err(_) => continue,
                    },
                };

                // Numbered before the connection task starts, so the number is assigned in
                // accept order rather than in whatever order the tasks get scheduled.
                let number = accept_sink.fetch_add(1, Ordering::SeqCst) + 1;
                let requests = Arc::clone(&seen_sink);

                tokio::spawn(async move {
                    let service = service_fn(move |request: http::Request<Incoming>| {
                        let requests = Arc::clone(&requests);
                        async move {
                            let path = request.uri().path().to_owned();
                            let body = request
                                .into_body()
                                .collect()
                                .await
                                .map(|collected| collected.to_bytes())
                                .unwrap_or_default();

                            requests.lock().expect("a lock").push(Seen {
                                path: path.clone(),
                                body,
                                connection: number,
                            });

                            Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from(path))))
                        }
                    });

                    // hyper's own HTTP/2 server builder rather than hyper-util's protocol
                    // -sniffing `auto` one: this stack is h2c-only, so there is nothing to
                    // sniff, and `auto` lives behind a hyper-util feature the workspace does
                    // not enable. Using it would mean turning on a feature to obtain a
                    // negotiation that cannot happen.
                    let _ = hyper_server::Builder::new(TokioExecutor::new())
                        .serve_connection(HyperIo::new(socket), service)
                        .await;
                });
            }
        });

        Self {
            address,
            accepts,
            seen,
            stop: Some(stop),
            task: Some(task),
        }
    }

    /// How many TCP connections this server has accepted.
    ///
    /// The single most load-bearing observable in the suite. "The connection was reused" is
    /// this returning 1 after three requests, and nothing else demonstrates it.
    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    /// Every request this server has served, in arrival order.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("a lock").clone()
    }

    /// The authority a client should use to reach this server.
    ///
    /// `SocketAddr`'s own `Display` already brackets IPv6, which is the form a URI wants.
    pub fn authority(&self) -> String {
        self.address.to_string()
    }

    /// A `http://<authority><path>` URI naming this server.
    pub fn uri(&self, path: &str) -> http::Uri {
        format!("http://{}{}", self.authority(), path)
            .parse()
            .expect("a valid test URI")
    }
}

impl Drop for TestServer {
    /// Stops the accept loop behind a failing test.
    ///
    /// Without this a panicking test leaves its server task running for the rest of the
    /// process, which is harmless but makes the port and the counters outlive the test that
    /// owned them.
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// The request body every test uses unless it needs something else.
pub fn body(content: &str) -> Full<Bytes> {
    Full::new(Bytes::from(content.to_owned()))
}

/// An empty request body.
pub fn empty() -> Full<Bytes> {
    Full::new(Bytes::new())
}

/// Collects a response body into bytes.
pub async fn collect(response: http::Response<ngnet_util::IncomingBody>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("the response body collects")
        .to_bytes()
}

/// A `GET` at `uri` with an empty body.
pub fn get(uri: http::Uri) -> http::Request<Full<Bytes>> {
    http::Request::get(uri)
        .body(empty())
        .expect("a valid request")
}
