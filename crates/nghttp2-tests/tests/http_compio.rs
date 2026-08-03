//! The shipped compio transport, over io_uring, against a real socket.
//!
//! Enabled by the optional `completion` feature. This file used to carry its own adapter and
//! prove the transport traits *fit* a completion runtime by compiling. The adapter now ships
//! in the `nghttp2` crate, so what is proven here changed: that the public transport works
//! over a real completion-based socket, not that one could be written.
//!
//! # Why there is no tolerance of a missing io_uring
//!
//! This workspace asks compio for the `io-uring` backend and no readiness one, so there is
//! nothing compiled in to fall back to. A host without io_uring fails to start a runtime, and
//! that is the intended behaviour rather than a case to be tolerated — the alternative would
//! be a transport quietly running on epoll while still calling itself completion-based. An
//! earlier version of this file skipped when a runtime could not be created, which was right
//! when compiling was the claim and is wrong now that running is.
//!
//! # Why the backend is asserted rather than assumed
//!
//! The manifest is not a guarantee, because cargo unifies features across the whole
//! dependency graph: if any crate anywhere in a build enables compio's `polling` feature,
//! compio compiles its fusion driver, which probes the kernel and silently degrades to epoll.
//! Nothing in this workspace asks for `polling` today, but nothing in this workspace could
//! stop a future dependency from doing so either. The assertion below is what would notice —
//! it is the only check that survives a change to a manifest nobody here owns.

#![cfg(feature = "completion")]

use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use core::future::Future;
use http_body::{Body, Frame};
use nghttp2::http::transport::CompioIo;
use nghttp2::http::{IncomingBody, server};

/// A body already held in memory.
#[derive(Debug, Default)]
struct Full {
    data: Option<Bytes>,
}

impl Full {
    fn new(data: impl Into<Bytes>) -> Self {
        let data = data.into();
        Self {
            data: (!data.is_empty()).then_some(data),
        }
    }
}

impl Body for Full {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        _context: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        core::task::Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }
}

async fn drain(mut body: IncomingBody) -> Vec<u8> {
    let mut received = Vec::new();
    while let Some(frame) =
        core::future::poll_fn(|context| core::pin::Pin::new(&mut body).poll_frame(context)).await
    {
        if let Some(data) = frame.expect("a body frame").data_ref() {
            received.extend_from_slice(data);
        }
    }
    received
}

async fn echo(request: http::Request<IncomingBody>) -> http::Response<Full> {
    let body = drain(request.into_body()).await;
    http::Response::builder()
        .status(http::StatusCode::OK)
        .body(Full::new(body))
        .expect("a well-formed response")
}

/// The runtime this build actually gets is io_uring, not a readiness driver wearing its name.
///
/// Guards the property the whole feature rests on. `polling` is absent from this workspace's
/// compio features, so compio builds its io_uring driver alone — but feature unification means
/// a dependency added years from now could put `polling` back, restoring the fusion driver and
/// its silent epoll fallback. This test is what turns that from an invisible regression into a
/// failing build.
#[test]
fn the_completion_transport_runs_on_io_uring() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");
    assert_eq!(
        runtime.driver_type(),
        compio::driver::DriverType::IoUring,
        "the completion transport must run on io_uring; a readiness driver here means \
         compio's `polling` feature was enabled somewhere in the dependency graph, which \
         restores the fusion driver and its silent fallback to epoll"
    );
}

/// A whole exchange over the shipped transport, on a real socket, on compio's runtime.
///
/// Failing to start a runtime is a failure of this test, not a reason to skip it.
#[test]
fn an_exchange_completes_over_the_shipped_compio_transport() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");
    assert_eq!(
        runtime.driver_type(),
        compio::driver::DriverType::IoUring,
        "this exchange must run on io_uring for it to be evidence about a completion transport"
    );

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        let serving = compio::runtime::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(CompioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (requests, connection) =
            nghttp2::http::handshake::<_, Full>(CompioIo::new(stream)).expect("handshake");

        let response = requests.send_request(
            http::Request::builder()
                .method(http::Method::POST)
                .uri("http://example.test/echo")
                .body(Full::new(&b"completion based"[..]))
                .expect("a request"),
        );

        let exchange = async {
            let response = response.await.expect("a response");
            assert_eq!(response.status(), http::StatusCode::OK);
            let received = drain(response.into_body()).await;
            drop(requests);
            received
        };

        // Neither future is spawned. Nothing here is `Send`, and nothing needs to be —
        // which is the property a thread-per-core runtime needs and the reason the
        // transport traits carry no `Send` bound.
        let received = alongside(exchange, connection).await;
        assert_eq!(received, b"completion based");
        serving.detach();
    });
}

/// Polls two futures on one task, finishing when the first completes.
///
/// Written out rather than taken from a combinator crate: this file exists to show what a
/// caller needs, and needing a third crate to run two futures alongside each other would be
/// part of that answer.
async fn alongside<A: Future, B: Future>(main: A, background: B) -> A::Output {
    let mut main = core::pin::pin!(main);
    let mut background = core::pin::pin!(background);
    let mut finished = false;

    core::future::poll_fn(|context| {
        if !finished && background.as_mut().poll(context).is_ready() {
            finished = true;
        }
        main.as_mut().poll(context)
    })
    .await
}
