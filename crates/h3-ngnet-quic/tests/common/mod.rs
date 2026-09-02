//! A loopback client/server pair, both ends adapted for hyperium H3.
//!
//! Real sockets and a real runtime, deliberately: these tests exist to catch wire-format and
//! timing defects, and an in-memory shortcut would hide exactly those.

// Each integration test file compiles this module into its own binary, so items only some of
// them use are legitimately unused in the others.
#![allow(dead_code, unused_macros, unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h3_ngnet_quic::Connection;
use ngnet_quic::OsslSession;
use ngnet_quic::endpoint::Endpoint;
use ngnet_quic_h3_tests::{Credentials, TEST_SERVER_NAME, client_endpoint, server_endpoint};
use tokio::task::JoinHandle;

/// How long any test will wait for something that should be prompt.
///
/// Bounded so a liveness defect fails the test rather than hanging the suite.
pub const TIMEOUT: Duration = Duration::from_secs(20);

/// Both adapted ends of a live loopback connection, plus the tasks driving the endpoints.
pub struct Pair {
    pub client: Option<Connection<OsslSession>>,
    pub server: Option<Connection<OsslSession>>,
    tasks: Vec<JoinHandle<()>>,
    // Kept alive: dropping an endpoint stops its driver and fails every connection on it.
    _endpoints: (Endpoint<OsslSession>, Endpoint<OsslSession>),
    _credentials: Arc<Credentials>,
}

impl Pair {
    /// Establishes a connection and hands both ends to the adapter.
    ///
    /// The client is pumped while the server's accept completes. That is not a workaround:
    /// once a connection is detached, nothing drives it but its owner, so the client's final
    /// handshake flight does not leave until something polls the adapter. Real callers get
    /// this for free because they hand the connection to hyperium immediately; a test that
    /// awaited the accept first would simply deadlock.
    pub async fn new() -> Self {
        let credentials = Arc::new(Credentials::generate());
        let (server, server_driver, address) = server_endpoint(&credentials).await;
        let (client, client_driver) = client_endpoint(&credentials, 0xBEE5).await;

        let tasks = vec![
            tokio::spawn(async move {
                let _ = server_driver.await;
            }),
            tokio::spawn(async move {
                let _ = client_driver.await;
            }),
        ];

        let accepting = server.clone();
        let accept = tokio::spawn(async move { accepting.accept_detached().await });
        let connecting = client
            .connect_detached(address, Some(TEST_SERVER_NAME))
            .await
            .expect("a detached client connection");
        let mut client_connection = h3_ngnet_quic::from_detached(connecting);

        tokio::pin!(accept);
        let accepted = std::future::poll_fn(|cx| {
            // Any poll pumps; accepting a stream is the one that is always harmless.
            let _ = h3::quic::Connection::poll_accept_bidi(&mut client_connection, cx);
            accept.as_mut().poll(cx)
        })
        .await
        .expect("the accept task")
        .expect("a detached server connection");

        Self {
            client: Some(client_connection),
            server: Some(h3_ngnet_quic::from_detached(accepted)),
            tasks,
            _endpoints: (client, server),
            _credentials: credentials,
        }
    }

    /// Takes both adapted connections out, leaving the endpoints and their drivers alive.
    pub fn split(&mut self) -> (Connection<OsslSession>, Connection<OsslSession>) {
        (
            self.client.take().expect("client taken once"),
            self.server.take().expect("server taken once"),
        )
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Reads a hyperium request or response stream to its end, returning every byte in order.
///
/// A macro rather than a function because hyperium's client and server `RequestStream` are
/// distinct types with no common trait, and `recv_data` is inherent on each.
macro_rules! drain_body {
    ($stream:expr) => {{
        let mut out = bytes::BytesMut::new();
        loop {
            match $stream.recv_data().await {
                Ok(Some(chunk)) => bytes::BufMut::put(&mut out, chunk),
                Ok(None) => break,
                Err(err) => panic!("reading stream data: {err}"),
            }
        }
        out.freeze()
    }};
}

pub(crate) use drain_body;

/// A body of `len` bytes whose contents depend on their position.
///
/// Position-dependent so a test that reorders, duplicates or drops a range fails, which a
/// buffer of identical bytes would not catch.
pub fn body_of(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Fails the test if `future` does not finish promptly.
pub async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(TIMEOUT, future).await {
        Ok(value) => value,
        Err(_) => panic!("{what} did not complete within {TIMEOUT:?}"),
    }
}
