//! The shipped compio transport, over io_uring, against a real socket.
//!
//! Enabled by the optional `completion` feature. This file used to carry its own adapter and
//! prove the transport traits *fit* a completion runtime by compiling. The adapter now ships
//! in the `ngnet-h2` crate, so what is proven here changed: that the public transport works
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
//! # Why the backend is asserted, and exactly what that does and does not catch
//!
//! The manifest is not a guarantee, because cargo unifies features across the whole
//! dependency graph: if any crate anywhere in a build enables compio's `polling` feature,
//! compio compiles its fusion driver, which probes the kernel and silently degrades to epoll.
//! Nothing in this workspace asks for `polling` today, and nothing in it could stop a future
//! dependency from doing so.
//!
//! The assertion below is worth having, but it is narrower than it first appears and the
//! difference matters. In the build this workspace actually produces — io_uring alone — the
//! reported driver type is a compile-time constant
//! (`compio-driver-0.12.4/src/sys/driver/iour/mod.rs:148-150`), so the assertion is trivially
//! true and costs nothing. It can only *fail* in a fusion build running on a host that lacks
//! io_uring. So:
//!
//! - It **catches** a real degradation: a fusion build that quietly fell back to epoll,
//!   which is the outcome that would make every measurement taken through this transport a
//!   lie.
//! - It **does not catch** a `polling` feature arriving through unification on a host that
//!   has io_uring, because the fusion driver would still probe and choose io_uring there. The
//!   build would be less guaranteed than intended and every test would still pass.
//!
//! The check for that second case is the dependency tree, not a runtime assertion:
//! `cargo tree -e features` shows whether `compio-driver` carries `polling`. CI runs that
//! check on every change, so the two together cover both cases — this test catches a fallback
//! that happened, and the tree check catches a build where one became possible.

#![cfg(feature = "completion")]

use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use core::future::Future;
use http_body::{Body, Frame};
use ngnet_h2::http::transport::{CompioIo, Transport, TransportWrite};
use ngnet_h2::http::{IncomingBody, server};

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

/// The runtime this build gets is io_uring, not a readiness driver wearing its name.
///
/// See the module documentation for what this catches and what it does not: it fires only in
/// a fusion build on a host without io_uring, which is the case where a silent fallback would
/// otherwise make everything measured through this transport misleading.
#[test]
fn the_completion_transport_runs_on_io_uring() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");
    assert_eq!(
        runtime.driver_type(),
        compio::driver::DriverType::IoUring,
        "the completion transport is running on a readiness driver. Both of these are true: \
         compio's `polling` feature is enabled somewhere in the dependency graph, so the \
         fusion driver was compiled; and io_uring could not be obtained on this host, so it \
         fell back. Check `cargo tree -e features` for the first and the kernel for the second"
    );
}

/// A whole exchange over the shipped transport, on a real socket, on compio's runtime.
///
/// Failing to start a runtime is a failure of this test, not a reason to skip it.
#[test]
fn an_exchange_completes_over_the_shipped_compio_transport() {
    // The driver is asserted once, in `the_completion_transport_runs_on_io_uring`. Repeating
    // it here would be noise rather than defence: both construct the runtime the same way, so
    // neither could fail without the other.
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");

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
            ngnet_h2::http::handshake::<_, Full>(CompioIo::new(stream)).expect("handshake");

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

/// The same exchange over the *handed-over* entry point, so the completion transport takes
/// its owned-region write path on a real io_uring socket rather than the coalescing one.
///
/// `handshake_shared` frames each `DATA` as a record the driver offers as an owned region,
/// so a body spanning several frames drives a genuine multi-region `write_regions` — which on
/// this transport reaches `TcpStream::write_vectored`, an `IORING_OP_SENDMSG`. The body is
/// therefore several frames long: a single-frame body would gather one payload region and
/// prove nothing a coalesced write would not.
///
/// # What the echo assertion does and does not prove
///
/// The echo alone is *not* evidence that the gathering path ran. An earlier version of this
/// test asserted only the echo while its documentation claimed the path taken was the
/// gathering one, and that claim was false: flipping
/// [`TransportWrite::gathers_owned_regions`] to `false` on the shipped transport sends every
/// octet down the coalescing fallback and the echo is still correct — verified by mutation,
/// which passed this test *and the entire workspace suite* unchanged.
///
/// The path is pinned in two independent halves, neither sufficient alone:
///
/// 1. **The shipped transport advertises the path** —
///    `the_shipped_compio_transport_elects_the_owned_region_path` below.
/// 2. **The driver honours the advertisement** — pinned in memory by
///    `http_transport.rs::a_transport_can_elect_the_owned_region_path` (the transport-side
///    contract) and `the_owned_region_election_is_read_once_a_pass_not_once_a_write` (the
///    driver actually electing it), the latter mutation-verified.
///
/// Together those give what the echo cannot: this exchange really did leave through
/// `write_regions`. The echo remains worth asserting for a different reason — it is what
/// catches a region dropped, reordered or duplicated by the gathering write itself.
#[test]
fn a_shared_body_exchange_gathers_over_the_shipped_compio_transport() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");

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
        // Handed over rather than copied: this is the entry point that reaches the
        // owned-region strategy, and the whole point of the exercise.
        let (requests, connection) =
            ngnet_h2::http::handshake_shared::<_, Full>(CompioIo::new(stream)).expect("handshake");

        // Several frames' worth, so the gathering write carries a run of header and payload
        // regions rather than a single coalesced one. The payload is a recognisable pattern
        // so a dropped or reordered region would corrupt the echo rather than pass silently.
        let body: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
        let expected = body.clone();

        let response = requests.send_request(
            http::Request::builder()
                .method(http::Method::POST)
                .uri("http://example.test/echo")
                .body(Full::new(body))
                .expect("a request"),
        );

        let exchange = async {
            let response = response.await.expect("a response");
            assert_eq!(response.status(), http::StatusCode::OK);
            let received = drain(response.into_body()).await;
            drop(requests);
            received
        };

        let received = alongside(exchange, connection).await;
        assert_eq!(
            received, expected,
            "the handed-over body did not round-trip intact over the gathering write path",
        );
        serving.detach();
    });
}

/// The shipped completion transport must advertise the owned-region strategy.
///
/// This is half one of the two-part argument documented on
/// `a_shared_body_exchange_gathers_over_the_shipped_compio_transport`, and it exists because
/// that test cannot supply it: an exchange over the coalescing fallback echoes just as
/// correctly as one over the gathering write, so no end-to-end assertion on a real socket can
/// tell the two apart. Before this test existed, flipping
/// [`TransportWrite::gathers_owned_regions`] to `false` in `transport/compio.rs` left the
/// whole workspace suite green — the entire completion fast path could have regressed to
/// copying every octet without a single failure.
///
/// The election is a plain predicate rather than a fallible call precisely so it can be
/// asserted like this, which is design decision D5: an `Option`-returning `write_regions`
/// that declined *after* being handed the owned `Vec<Bytes>` would consume and lose the
/// regions, so the choice is split from the write. That split is what makes the property
/// observable here without a socket write.
#[test]
fn the_shipped_compio_transport_elects_the_owned_region_path() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");
        let accepting = compio::runtime::spawn(async move { listener.accept().await });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (_reader, writer) = CompioIo::new(stream).split();

        assert!(
            writer.gathers_owned_regions(),
            "the shipped compio transport must elect the owned-region path; without it every \
             handed-over body is coalesced into a fresh buffer and the copy this work exists \
             to remove comes straight back, silently and with every test still passing",
        );

        accepting.detach();
    });
}

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
