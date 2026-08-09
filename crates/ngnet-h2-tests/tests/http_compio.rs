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
use ngnet_h2::http::transport::{CompioIo, CompioWriter, Completion, Transport, TransportWrite};
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
/// gathering one, and that claim was false: flipping the shipped transport's election to
/// `false` sent every octet down the coalescing fallback and the echo was still correct —
/// verified by mutation, which passed this test *and the entire workspace suite* unchanged.
///
/// The path is pinned in three independent parts, none sufficient alone:
///
/// 1. **The shipped transport declares the model** —
///    `the_shipped_compio_transport_elects_the_owned_region_path` below, a compile-time
///    assertion on the writer's associated `Model` type. This says which *trait* carries the
///    write; on its own it no longer says which drain runs.
/// 2. **The shipped transport declares its gathering real** —
///    `the_shipped_compio_transport_declares_its_gathering_real` below. Since the drain
///    follows `TransportWrite::is_write_vectored` rather than the model, a `false` here would
///    send this exchange down the completion coalescing drain and `write_regions` would never
///    be reached — reproducing exactly the vacuity described above, under a new mechanism.
///    The `Model` assertion cannot catch that, which is why part 2 is separate from part 1.
/// 3. **The driver honours the declaration** — pinned by
///    `http_transport.rs::a_transport_can_elect_the_owned_region_path`, which counts the
///    region writes the driver actually performs, and by
///    `http_vectored.rs::the_completion_side_selects_its_drain_from_the_declaration_too`,
///    which pins that a `false` declaration on the completion side really does coalesce.
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
/// tell the two apart. Before this test existed, flipping the shipped transport's election to
/// `false` in `transport/compio.rs` left the whole workspace suite green — the entire
/// completion fast path could have regressed to copying every octet without a single failure.
///
/// The election is now the writer's declared
/// [`Model`](ngnet_h2::http::transport::TransportWrite::Model), so this assertion is
/// made **at compile time** rather than by calling a predicate. That is strictly stronger
/// than what stood here before: the old flag could be read `true` by this test while some
/// other part of the contract went unimplemented, whereas declaring
/// [`Completion`](ngnet_h2::http::transport::Completion) is inseparable from supplying
/// [`RegionWrite`](ngnet_h2::http::transport::RegionWrite) — the compiler will not accept one
/// without the other.
///
/// Mutation-verified: changing `CompioWriter`'s `type Model` to `Readiness` fails this file to
/// compile, with `expected `Completion`, found `Readiness``.
///
/// Note what this test stopped covering when the drain moved onto the capability. Declaring
/// `Completion` obliges the writer to supply `RegionWrite`, and that much is still enforced
/// here by the compiler — but it no longer implies the driver will *use* the gathering half of
/// `RegionWrite`. That implication now belongs to
/// `the_shipped_compio_transport_declares_its_gathering_real`.
#[test]
fn the_shipped_compio_transport_elects_the_owned_region_path() {
    /// Compiles only if `W` declares exactly `Completion`. A different model is a type
    /// error, not a failed assertion, so this cannot go vacuously green.
    fn elects_owned_regions<W>()
    where
        W: TransportWrite<Model = Completion>,
    {
    }

    elects_owned_regions::<CompioWriter<compio::net::TcpStream>>();
}

/// The shipped compio transport reports that its gathering is a real scatter-gather write.
///
/// Confirmed rather than assumed. `CompioWriter` overrides `RegionWrite::write_regions` with a
/// call to compio's `AsyncWriteExt::write_vectored` on a `TcpStream`, which submits an
/// `IORING_OP_SENDMSG` — one submission carrying every region, not a loop. So `true` is the
/// honest answer, and it is the answer that keeps
/// `a_shared_body_exchange_gathers_over_the_shipped_compio_transport` above meaningful: with
/// `false` the driver would coalesce and that test's `write_regions` would never run.
///
/// This is asserted on a live writer rather than as a trait-level constant because
/// `is_write_vectored` takes `&self` — it is allowed to vary per instance, as tokio's does,
/// and pinning it for the type would assert something the trait does not promise.
#[test]
fn the_shipped_compio_transport_declares_its_gathering_real() {
    let runtime = compio::runtime::Runtime::new().expect("compio needs io_uring to start");

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");
        let accepting = compio::runtime::spawn(async move {
            let _ = listener.accept().await.expect("accepting");
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (_reader, writer) = Transport::split(CompioIo::new(stream));
        assert!(
            writer.is_write_vectored(),
            "the shipped compio transport overrides `write_regions` with a real \
             `IORING_OP_SENDMSG` and must declare it, or the driver coalesces and the \
             override is dead code",
        );

        accepting.await.expect("the acceptor");
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
