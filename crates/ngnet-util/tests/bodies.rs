//! Bodies, in both directions, and the claim that no adapter was needed.
//!
//! `ngnet-axum` records the same finding on the server side: `ngnet-h2` already accepts any
//! `http_body::Body<Data = Bytes>` outbound and returns an `IncomingBody` that is already
//! `http_body::Body<Data = Bytes> + Send + 'static` inbound, so there was nothing to convert.
//! This file is what makes that a tested claim rather than an assertion in a doc comment —
//! including for a body large enough to exhaust the initial flow control window, which is
//! where a "no conversion needed" claim would break if it were wrong.

mod support;

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::Full;
use ngnet_util::Client;
use support::{TestServer, collect, within};

/// The HTTP/2 default initial window, and the reason this file has a large-body test.
///
/// A body under this size is written in one go and proves nothing about flow control. A body
/// over it cannot be sent until the peer says there is room, so it exercises the path where
/// the sender has to hold data back and resume — the path a body adapter, if one existed,
/// would be most likely to get wrong.
const INITIAL_WINDOW: usize = 65_535;

#[tokio::test]
async fn a_body_larger_than_the_initial_window_arrives_intact() {
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let payload = Bytes::from(vec![b'x'; INITIAL_WINDOW * 3 + 17]);
    let request = http::Request::post(server.uri("/large"))
        .body(Full::new(payload.clone()))
        .expect("a valid request");

    let response = within("the request", client.request(request))
        .await
        .expect("the request succeeds");
    assert_eq!(response.status(), http::StatusCode::OK);

    let seen = server.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].body.len(),
        payload.len(),
        "a body spanning several windows must arrive whole"
    );
    assert_eq!(seen[0].body, payload);
}

/// A body that yields its chunks one at a time, from a channel.
///
/// `Full` hands over everything it has at once, so it never exercises a body that is not yet
/// complete when the request is sent. This one does: the request goes out before the last
/// chunk exists.
///
/// Written by hand rather than assembled from `StreamBody` and a `Stream` adapter, because
/// that route needs `futures-core` and `tokio-stream`, and adding a dependency to this
/// workspace means adding a rationale comment for it to the root manifest. "One test wanted a
/// `Stream` impl" is not a rationale. `http_body::Body` has one required method, and this is
/// it.
struct Streaming(tokio::sync::mpsc::Receiver<Bytes>);

impl Body for Streaming {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(context)
            .map(|chunk| chunk.map(|bytes| Ok(Frame::data(bytes))))
    }
}

#[tokio::test]
async fn a_streaming_body_reaches_the_server_incrementally() {
    let server = TestServer::start().await;
    let client: Client<Streaming> = Client::new();

    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let request = http::Request::post(server.uri("/streamed"))
        .body(Streaming(receiver))
        .expect("a valid request");

    // The request is sent before the body exists. A pool that waited for a complete body
    // before acquiring a connection would deadlock here, because nothing feeds the channel
    // until the request future is already in flight.
    let exchange = tokio::spawn(client.request(request));

    for chunk in ["one ", "two ", "three"] {
        sender
            .send(Bytes::from_static(chunk.as_bytes()))
            .await
            .expect("the body is still being read");
    }
    drop(sender);

    let response = within("the request", exchange)
        .await
        .expect("the task completes")
        .expect("the request succeeds");
    assert_eq!(response.status(), http::StatusCode::OK);

    assert_eq!(
        server.seen()[0].body,
        Bytes::from_static(b"one two three"),
        "every chunk must arrive, in order"
    );
}

#[tokio::test]
async fn the_response_body_is_a_plain_http_body() {
    // The inbound half of the "no adapter" claim: `IncomingBody` is used through the
    // `http_body::Body` trait alone, with nothing from this crate involved.
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let request = http::Request::get(server.uri("/plain"))
        .body(Full::new(Bytes::new()))
        .expect("a valid request");
    let response = within("the request", client.request(request))
        .await
        .expect("succeeds");

    fn assert_is_a_body<T: Body<Data = Bytes> + Send + 'static>(_: &T) {}
    assert_is_a_body(response.body());

    assert_eq!(collect(response).await, Bytes::from_static(b"/plain"));
}
