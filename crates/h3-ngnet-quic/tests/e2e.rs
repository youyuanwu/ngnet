//! End-to-end HTTP/3 over the adapter, using hyperium's own client and server.
//!
//! These run the real thing: hyperium opens its control streams, exchanges settings, and
//! carries requests and responses over a live loopback ngtcp2 connection.

mod common;

use common::{Pair, body_of, drain_body, within};
use ngnet_quic::OsslSession;

/// An echo server, serving until its connection ends.
pub async fn echo_server(connection: h3_ngnet_quic::Connection<OsslSession>) {
    let mut builder = h3::server::builder();
    builder.send_grease(false);
    let mut connection = builder
        .build::<_, bytes::Bytes>(connection)
        .await
        .expect("a hyperium server");
    loop {
        let resolver = match connection.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) | Err(_) => return,
        };
        let (_request, mut stream) = match resolver.resolve_request().await {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        let body = drain_body!(stream);
        if stream
            .send_response(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/octet-stream")
                    .body(())
                    .expect("a response head"),
            )
            .await
            .is_err()
        {
            continue;
        }
        if !body.is_empty() && stream.send_data(body).await.is_err() {
            continue;
        }
        let _ = stream.finish().await;
    }
}

/// A client and a server on one live connection, with both drivers spawned.
struct Exchange {
    sender: h3::client::SendRequest<h3_ngnet_quic::OpenStreams<OsslSession>, bytes::Bytes>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _pair: Pair,
}

impl Exchange {
    async fn new() -> Self {
        let mut pair = Pair::new().await;
        let (client, server) = pair.split();

        let mut tasks = vec![tokio::spawn(echo_server(server))];

        let mut builder = h3::client::builder();
        builder.send_grease(false);
        let (mut driver, sender) = builder.build(client).await.expect("a hyperium client");
        tasks.push(tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        }));

        Self {
            sender,
            tasks,
            _pair: pair,
        }
    }

    /// One request/response exchange, asserting the echo is byte-exact.
    async fn round_trip(&mut self, size: usize) {
        let body = body_of(size);
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("https://localhost/echo")
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .body(())
            .expect("a request head");
        let mut stream = within("send_request", self.sender.send_request(request))
            .await
            .expect("opening a request stream");
        if !body.is_empty() {
            within("send_data", stream.send_data(body.clone()))
                .await
                .expect("sending the request body");
        }
        within("finish", stream.finish())
            .await
            .expect("finishing the request");
        let response = within("recv_response", stream.recv_response())
            .await
            .expect("receiving the response head");
        assert_eq!(response.status(), http::StatusCode::OK);
        let echoed = within("drain", async { drain_body!(stream) }).await;
        assert_eq!(
            echoed.len(),
            body.len(),
            "the echoed body must be the same length as the one sent, for size {size}"
        );
        assert_eq!(
            echoed, body,
            "the echoed body must be byte-for-byte what was sent, for size {size}"
        );
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[tokio::test]
async fn an_empty_body_round_trips() {
    let mut exchange = Exchange::new().await;
    exchange.round_trip(0).await;
}

#[tokio::test]
async fn a_small_body_round_trips_exactly() {
    let mut exchange = Exchange::new().await;
    exchange.round_trip(1024).await;
}

/// A body far larger than one packet, so the transport must accept it across many writes.
///
/// This is the test that catches a retained-write defect: a partial acceptance that advanced
/// optimistically would truncate, and one that re-offered from the wrong position would
/// duplicate. Both show up as a length or content mismatch here.
#[tokio::test]
async fn a_body_spanning_many_packets_round_trips_exactly() {
    let mut exchange = Exchange::new().await;
    exchange.round_trip(64 * 1024).await;
}

#[tokio::test]
async fn several_exchanges_on_one_connection_each_round_trip_exactly() {
    let mut exchange = Exchange::new().await;
    for size in [0usize, 37, 1024, 9000] {
        exchange.round_trip(size).await;
    }
}
