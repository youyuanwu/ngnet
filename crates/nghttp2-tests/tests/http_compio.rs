//! The transport traits against a real completion-based runtime.
//!
//! Enabled by the optional `completion` feature. Compiling is the bar: what is being proven
//! is that the traits *fit* a completion API — its buffer ownership, its `BufResult` shape,
//! its thread-per-core executor — rather than only fitting an in-process imitation of one.
//! io_uring is not available everywhere, so the exchange at the bottom is attempted and its
//! absence tolerated, while the adapter above it must always compile.
//!
//! # What this file is evidence of
//!
//! The adapter is four lines of body. `compio`'s
//! `async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B>` is, after
//! destructuring, exactly `(io::Result<usize>, B)` — the same thing
//! [`TransportRead::read`] asks for. That correspondence is not a coincidence: the traits
//! were shaped from the completion side precisely so that the runtimes hardest to serve
//! need no translation, and the readiness-based ones pay a single overridable copy instead.

#![cfg(feature = "completion")]

use bytes::{Bytes, BytesMut};
use compio::buf::BufResult;
use compio::io::{AsyncReadExt, AsyncWrite};
use compio::net::{TcpListener, TcpStream};
use http_body::{Body, Frame};
use nghttp2::http::transport::{Transport, TransportRead, TransportWrite};
use nghttp2::http::{IncomingBody, server};

/// Carries a compio stream into this crate's transport traits.
struct CompioIo {
    stream: TcpStream,
}

impl Transport for CompioIo {
    type Reader = CompioHalf;
    type Writer = CompioHalf;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.stream.into_split();
        (CompioHalf { stream: reader }, CompioHalf { stream: writer })
    }
}

/// One direction of a compio stream. Both halves are the same type here, because compio's
/// own split hands back two handles to the same socket.
struct CompioHalf {
    stream: TcpStream,
}

impl TransportRead for CompioHalf {
    async fn read(&mut self, buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // `append` rather than `read`, so octets land after whatever the buffer already
        // holds — the same contract tokio's `read_buf` has, and the one the connection
        // relies on.
        let BufResult(result, buf) = self.stream.append(buf).await;
        (result, buf)
    }
}

impl TransportWrite for CompioHalf {
    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let BufResult(result, buf) = self.stream.write(buf).await;
        (result, buf)
    }

    // `write_borrowed` is deliberately not overridden. A completion runtime cannot lend the
    // kernel a borrowed buffer, which is the whole reason the owned path is the default.
}

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

/// Names every part of the adapter, so it must compile whether or not it can run.
///
/// This is the assertion the phase actually makes. A `#[test]` that only ran where
/// io_uring happens to be available would prove nothing on the machines where it does not,
/// and the claim being made — that the traits fit a completion API — is a claim about
/// types.
#[test]
fn the_transport_traits_fit_a_completion_runtime() {
    fn assert_transport<T: Transport>() {}
    fn assert_read<R: TransportRead>() {}
    fn assert_write<W: TransportWrite>() {}

    assert_transport::<CompioIo>();
    assert_read::<CompioHalf>();
    assert_write::<CompioHalf>();

    // And the connection builds over it, which is what actually has to hold.
    fn assert_serves<T: Transport>(transport: T) {
        let _ = server::serve(transport, echo);
    }
    let _: fn(CompioIo) = assert_serves::<CompioIo>;

    fn assert_asks<T: Transport>(transport: T) {
        let _ = nghttp2::http::handshake::<T, Full>(transport);
    }
    let _: fn(CompioIo) = assert_asks::<CompioIo>;
}

/// Runs a whole exchange on compio's own runtime, where the platform allows one.
///
/// The tolerance is deliberately narrow: only *creating* the runtime is allowed to fail,
/// because a driver may be unavailable and the claim above is already made by compiling.
/// Everything after that runs with its assertions intact — wrapping the exchange itself
/// would turn every failure inside it into a pass.
#[test]
fn an_exchange_completes_on_compio_where_the_platform_allows() {
    let started = std::panic::catch_unwind(compio::runtime::Runtime::new);
    let runtime = match started {
        Ok(Ok(runtime)) => runtime,
        Ok(Err(error)) => {
            eprintln!("compio could not start a runtime here: {error}");
            return;
        }
        Err(_panic) => {
            eprintln!("compio has no driver here; the adapter still compiles");
            return;
        }
    };

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        let serving = compio::runtime::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(CompioIo { stream }, echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (requests, connection) =
            nghttp2::http::handshake::<_, Full>(CompioIo { stream }).expect("handshake");

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
