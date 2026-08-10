//! The asynchronous API over real sockets, and over transports that are not sockets.
//!
//! Three claims are made here that nothing inside the `ngnet-h2` crate can make on its own.
//! The first is that the ready-made tokio transport works against a real kernel rather than
//! against an in-memory pipe. The second is that the transport abstraction genuinely spans
//! both families of async I/O: the **same exchange**, the **same calling code**, over a
//! readiness-based transport and a completion-shaped one, with nothing runtime-specific
//! supplied but the transport itself. The third is that a transport whose types cannot move
//! between threads still compiles and runs — the thread-per-core case the traits exist to
//! serve.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body::{Body, Frame};
use ngnet_h2::http::transport::{
    BorrowedWrite, Completion, RegionWrite, Transport, TransportRead, TransportWrite,
};
use ngnet_h2::http::{IncomingBody, server, transport::TokioIo};
use tokio::net::{TcpListener, TcpStream};

type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// Bounds how long a stalled exchange takes to fail, so a mistake fails the run rather than
/// hanging it.
const PATIENCE: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

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

/// Reads a whole received body.
async fn drain(mut body: IncomingBody) -> Result<Vec<u8>, ngnet_h2::http::Error> {
    let mut received = Vec::new();
    while let Some(frame) =
        core::future::poll_fn(|context| core::pin::Pin::new(&mut body).poll_frame(context)).await
    {
        if let Some(data) = frame?.data_ref() {
            received.extend_from_slice(data);
        }
    }
    Ok(received)
}

/// The handler both halves of the SC-015 comparison run.
///
/// Named rather than written twice, because "the calling code differs only in the
/// transport" is the property under test and a copy would quietly weaken it.
async fn echo(request: http::Request<IncomingBody>) -> http::Response<Full> {
    let path = request.uri().path().to_owned();
    let body = drain(request.into_body()).await.unwrap_or_default();

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header("x-path", path)
        .body(Full::new(body))
        .expect("a well-formed response")
}

/// One request over a connection, from the client side. Also shared by both halves.
async fn ask<T: Transport>(transport: T, path: &str, payload: &'static [u8]) -> Fallible<Vec<u8>> {
    let (requests, connection) = ngnet_h2::http::handshake::<T, Full>(transport)?;

    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://example.test{path}"))
            .body(Full::new(payload))?,
    );

    // Driven alongside the exchange rather than spawned: nothing here may assume a spawner
    // exists, since the completion half below has no `Send` to offer one.
    let exchange = async {
        let response = response.await?;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get("x-path").map(|v| v.as_bytes()),
            Some(path.as_bytes()),
        );
        let received = drain(response.into_body()).await?;
        drop(requests);
        Ok::<_, Box<dyn StdError + Send + Sync>>(received)
    };

    let (received, _connection) = tokio::join!(exchange, connection);
    received
}

/// The no-copy counterpart of [`ask`], reaching the server through
/// [`handshake_shared`](ngnet_h2::http::handshake_shared) so the request body is handed over
/// uncopied. The calling code is otherwise identical, which is the point: opting in changes
/// the entry point and nothing else a caller writes.
async fn ask_shared<T: Transport>(
    transport: T,
    path: &str,
    payload: &'static [u8],
) -> Fallible<Vec<u8>> {
    let (requests, connection) = ngnet_h2::http::handshake_shared::<T, Full>(transport)?;

    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://example.test{path}"))
            .body(Full::new(payload))?,
    );

    let exchange = async {
        let response = response.await?;
        assert_eq!(response.status(), http::StatusCode::OK);
        let received = drain(response.into_body()).await?;
        drop(requests);
        Ok::<_, Box<dyn StdError + Send + Sync>>(received)
    };

    let (received, _connection) = tokio::join!(exchange, connection);
    received
}

#[tokio::test]
async fn a_no_copy_exchange_crosses_a_real_socket_intact() {
    // Both ends opt in to the no-copy path — the server through `serve_shared`, the client
    // through `handshake_shared` — and a body several times the initial flow-control window
    // is echoed back over a real kernel socket. Nothing about the exchange a caller can see
    // differs from the copying version; what is proven here is that the handed-over payload
    // survives a real round trip byte for byte, window grants and all.
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve_shared(TokioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        static PAYLOAD: std::sync::LazyLock<Vec<u8>> =
            std::sync::LazyLock::new(|| (0..256 * 1024).map(|i| (i % 251) as u8).collect());

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask_shared(TokioIo::new(stream), "/echo", &PAYLOAD)
            .await
            .expect("a no-copy exchange");

        assert_eq!(received.len(), PAYLOAD.len());
        assert_eq!(received, *PAYLOAD);
    })
    .await
    .expect("the exchange stalled");
}

/// The shipped tokio transport forwards the stream's own answer, and a real `TcpStream` says
/// yes.
///
/// This test exists because its absence was a hole. `TokioWriter::is_write_vectored` returns a
/// field cached from `tokio::io::AsyncWrite::is_write_vectored` at `split` time, and replacing
/// the whole method body with a constant `false` left the **entire workspace suite green** —
/// verified by mutation. Every other pin on the capability is on a test duplex, so the shipped
/// adapter's declaration was inferred rather than measured, and the one number the change was
/// supposed to preserve on a real socket — one gathered `writev` per pass — rested on nothing.
///
/// Two properties, and the second is what makes the first non-vacuous:
///
/// 1. A `TokioIo<TcpStream>` reports `true`. A Linux `TcpStream` implements
///    `poll_write_vectored` with a real `writev`, so `false` would be a lie that costs every
///    tokio user a full copy per pass.
/// 2. The answer is the *stream's*, not a constant. A `TokioIo` over a writer that inherits
///    tokio's first-region-only default reports `false` through the same code path. A constant
///    `true` passes part 1 and fails here; a constant `false` fails part 1. Neither survives.
///
/// Part 2 needs a stream that *inherits* tokio's `poll_write_vectored` default, and finding
/// one is harder than it looks: `tokio::io::DuplexStream` was tried first and reports `true`,
/// because it overrides `poll_write_vectored` itself. So the negative half uses a hand-written
/// `AsyncWrite` that implements only `poll_write` — which is precisely the third-party wrapper
/// the conservative default exists for, and a more honest fixture than a tokio type that
/// happens to be well-behaved.
/// A tokio stream that implements only `poll_write`, and so inherits
/// `is_write_vectored() == false` from `AsyncWrite`'s provided default.
///
/// The reads and writes go nowhere: nothing is ever driven over this: it exists solely to be
/// asked one question at `split` time.
struct NoVectoredWrite;

impl tokio::io::AsyncRead for NoVectoredWrite {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for NoVectoredWrite {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    // No `is_write_vectored`, and no `poll_write_vectored`. That omission is the fixture.
}

#[tokio::test]
async fn the_shipped_tokio_transport_forwards_the_streams_own_gathering_answer() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");
        let accepting = tokio::spawn(async move {
            let _ = listener.accept().await.expect("accepting");
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (_reader, writer) = Transport::split(TokioIo::new(stream));
        assert!(
            writer.is_write_vectored(),
            "a real `TcpStream` gathers through `writev`, and the shipped adapter must say so \
             or every tokio connection pays a copy of every outgoing octet",
        );

        // The same adapter over a stream that does not gather. If this also reported `true`
        // the assertion above would be satisfied by a constant and would pin nothing.
        let (_reader, writer) = Transport::split(TokioIo::new(NoVectoredWrite));
        assert!(
            !writer.is_write_vectored(),
            "the adapter reported gathering for a stream that inherits tokio's \
             first-region-only default, so it is answering from a constant rather than from \
             the stream",
        );

        accepting.await.expect("the acceptor");
    })
    .await
    .expect("the probe stalled");
}

/// A stream that records the length of every `poll_write` and never gathers.
///
/// Distinct from [`NoVectoredWrite`] only in that it counts, which is what lets a caller tell
/// an emulated gathering write (one call per region) from a first-region-only forward (one
/// call, one region's worth of octets).
#[derive(Default)]
struct CountingUnvectoredWrite(std::rc::Rc<std::cell::RefCell<Vec<usize>>>);

impl tokio::io::AsyncWrite for CountingUnvectoredWrite {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.borrow_mut().push(buf.len());
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    // No `poll_write_vectored`: this stream inherits tokio's provided default, which writes
    // the first region and silently ignores the rest.
}

impl tokio::io::AsyncRead for CountingUnvectoredWrite {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn a_direct_vectored_call_on_a_non_gathering_tokio_writer_still_writes_every_region() {
    // Decision D-2, made load-bearing. `TokioWriter::write_vectored` keeps a branch that
    // emulates gathering when the wrapped stream does not gather natively. Since the
    // capability change that branch is unreachable *from the driver* — the adapter reports
    // `false` for such a stream, and `false` routes to the coalescing drain, which calls
    // `write_borrowed` and never this method. D-2 kept the branch anyway, justified as a
    // defence for direct callers of the public `BorrowedWrite` trait.
    //
    // That justification was an assertion with nothing behind it: replacing the branch
    // condition with `true`, so that every writer forwards natively, left the entire suite
    // green. This test is what makes the justification true. It is deliberately a *direct*
    // call, because a direct call is exactly the situation D-2 claims to protect, and no
    // driven connection can reach it.
    //
    // The discriminator is sharp: tokio's provided `poll_write_vectored` writes the first
    // region and ignores the rest, so a forward would produce ONE call of 5 octets and lose
    // ten. Emulation produces THREE calls totalling fifteen.
    let log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let stream = CountingUnvectoredWrite(std::rc::Rc::clone(&log));
    let (_reader, mut writer) = Transport::split(TokioIo::new(stream));

    assert!(
        !writer.is_write_vectored(),
        "the fixture is meant to be a non-gathering stream; if it reports `true` the branch \
         under test is not the one being taken and this test proves nothing",
    );

    let written = writer
        .write_vectored(&[
            std::io::IoSlice::new(b"alpha"),
            std::io::IoSlice::new(b"bravo"),
            std::io::IoSlice::new(b"delta"),
        ])
        .await
        .expect("the emulated gathering write");

    assert_eq!(
        written, 15,
        "a direct `write_vectored` on a non-gathering tokio stream reported {written} octets \
         for three five-octet regions; forwarding to tokio's first-region-only default would \
         report exactly 5, which is the bug the retained branch exists to prevent",
    );
    assert_eq!(
        *log.borrow(),
        vec![5, 5, 5],
        "the emulation must reach the underlying stream once per region; a single call means \
         the regions after the first were dropped",
    );
}

#[tokio::test]
async fn a_request_and_response_cross_a_real_socket() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        let serving = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(TokioIo::new(stream), "/echo", b"over a socket")
            .await
            .expect("an exchange");

        assert_eq!(received, b"over a socket");
        serving.await.expect("the server task").expect("the server");
    })
    .await
    .expect("the exchange stalled");
}

#[tokio::test]
async fn many_connections_are_served_at_once() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            loop {
                let (stream, _peer) = listener.accept().await.expect("accepting");
                tokio::spawn(async move {
                    let _ = server::serve(TokioIo::new(stream), echo)
                        .expect("serving")
                        .await;
                });
            }
        });

        let mut asking = Vec::new();
        for _ in 0..8u32 {
            asking.push(tokio::spawn(async move {
                let stream = TcpStream::connect(addr).await.expect("connecting");
                ask(TokioIo::new(stream), "/echo", b"concurrent")
                    .await
                    .expect("an exchange")
            }));
        }

        for handle in asking {
            assert_eq!(handle.await.expect("a task"), b"concurrent");
        }
    })
    .await
    .expect("the exchanges stalled");
}

#[tokio::test]
async fn a_large_body_crosses_a_real_socket_intact() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        static PAYLOAD: std::sync::LazyLock<Vec<u8>> =
            std::sync::LazyLock::new(|| (0..400 * 1024).map(|i| (i % 251) as u8).collect());

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(TokioIo::new(stream), "/echo", &PAYLOAD)
            .await
            .expect("an exchange");

        assert_eq!(received.len(), PAYLOAD.len());
        assert_eq!(received, *PAYLOAD);
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// The same exchange over a completion-shaped transport (SC-015)
// ---------------------------------------------------------------------------

/// A transport that owns every buffer it is handed, as a completion-based one must.
///
/// It keeps each buffer for the duration of the operation and hands it back afterwards,
/// which is the ownership discipline `io_uring` imposes: the kernel may still be writing
/// into a buffer after the future that submitted the operation is gone, so the buffer
/// cannot be borrowed. Backed by a tokio socket underneath, because what is under test is
/// the *shape* of the API, not the syscall behind it.
///
/// It deliberately does **not** override `write_borrowed`, so the connection takes the
/// coalescing path — the other half of the comparison.
struct CompletionShaped {
    stream: TcpStream,
}

struct CompletionReader {
    half: tokio::net::tcp::OwnedReadHalf,
}

struct CompletionWriter {
    half: tokio::net::tcp::OwnedWriteHalf,
}

impl Transport for CompletionShaped {
    type Reader = CompletionReader;
    type Writer = CompletionWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.stream.into_split();
        (
            CompletionReader { half: reader },
            CompletionWriter { half: writer },
        )
    }
}

impl TransportRead for CompletionReader {
    async fn read(&mut self, mut buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // Owned throughout: the buffer goes in, comes back, and is never borrowed from
        // anywhere else while the operation is outstanding.
        let result = tokio::io::AsyncReadExt::read_buf(&mut self.half, &mut buf).await;
        (result, buf)
    }
}

impl TransportWrite for CompletionWriter {
    // Owned throughout, so the completion model.
    type Model = Completion;

    // And no scatter-gather write behind it: `tokio::io::AsyncWriteExt::write` on an owned
    // half takes one buffer. Saying so is not decoration — it is what routes this connection
    // onto the completion *coalescing* drain, which until this transport declared itself had
    // no end-to-end coverage anywhere in the workspace. Leaving the method off would give the
    // same `false` by default; it is spelled out because being on that drain is the point.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

/// The whole write obligation, discharged by the one owned primitive.
///
/// `write_regions` is provided: its default loops the owned regions through `write_owned`
/// below. So this transport *can* gather — as every transport can — without naming a single
/// extra method, which is the shape that makes the provided default affordable for a backend
/// that has nothing better to offer. It does not *claim* to gather, though, and the driver
/// believes the claim rather than the capability: because `is_write_vectored` is `false`, the
/// driver coalesces each pass into one owned buffer and `write_regions` is never reached.
///
/// The owned primitive lives *here*, on the completion model's trait, and not on
/// `TransportWrite`. A tokio transport built on the readiness model — the one in
/// `ngnet-h2`'s `transport::tokio` — is never asked for it, because taking ownership is a
/// completion requirement rather than a write requirement. This type opts into it by
/// declaring `Completion`.
impl RegionWrite for CompletionWriter {
    async fn write_owned(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let result = tokio::io::AsyncWriteExt::write(&mut self.half, &buf).await;
        (result, buf)
    }
}

#[tokio::test]
async fn the_same_exchange_runs_over_a_completion_shaped_transport() {
    // Spec SC-015. The calling code below is `ask` and `echo` — the very functions the
    // readiness-based tests call. Only the transport differs, and nothing runtime-specific
    // is supplied beyond it: no spawner, no executor, no timer.
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(CompletionShaped { stream }, echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let received = ask(CompletionShaped { stream }, "/echo", b"owned buffers")
            .await
            .expect("an exchange");

        assert_eq!(received, b"owned buffers");
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// A transport that cannot move between threads
// ---------------------------------------------------------------------------

/// Compiles only while `T` is *not* `Send`.
///
/// Two blanket impls overlap for anything that *is* `Send`, so naming the method becomes
/// ambiguous exactly then. Written out because this workspace takes no dependency on
/// `static_assertions`, and asserted at all because the obvious alternative — "it was
/// driven on a `LocalSet`" — proves nothing: `spawn_local` accepts `Send` futures too, so a
/// connection that had quietly become `Send` would run there just as happily.
trait AmbiguousIfSend<A> {}

impl<T: ?Sized> AmbiguousIfSend<()> for T {}
impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

fn assert_not_send<T: AmbiguousIfSend<A> + ?Sized, A>(_value: &T) {}

/// A transport built on `Rc`, as the thread-per-core runtimes are.
///
/// Nothing in the transport traits requires [`Send`], precisely so that runtimes like this
/// can implement them. This stands in for one: if a `Send` bound ever appeared anywhere in
/// the traits, this file would stop compiling.
struct ThreadBound {
    inner: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
    peer: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

struct ThreadBoundReader {
    inner: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

struct ThreadBoundWriter {
    peer: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

impl Transport for ThreadBound {
    type Reader = ThreadBoundReader;
    type Writer = ThreadBoundWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            ThreadBoundReader { inner: self.inner },
            ThreadBoundWriter { peer: self.peer },
        )
    }
}

impl TransportRead for ThreadBoundReader {
    async fn read(&mut self, mut buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        loop {
            let taken: Vec<u8> = core::mem::take(&mut *self.inner.borrow_mut());
            if !taken.is_empty() {
                buf.extend_from_slice(&taken);
                return (Ok(taken.len()), buf);
            }
            tokio::task::yield_now().await;
        }
    }
}

impl TransportWrite for ThreadBoundWriter {
    type Model = Completion;
    // No override of `write_regions`, so `false` by default is the honest answer. This
    // fixture exists to prove a non-`Send` connection compiles and runs; which drain carries
    // its octets is immaterial to that, and the default is the one that needs no line.
}

impl RegionWrite for ThreadBoundWriter {
    fn write_owned(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)> {
        self.peer.borrow_mut().extend_from_slice(&buf);
        core::future::ready((Ok(buf.len()), buf))
    }
}

#[tokio::test]
async fn a_transport_that_cannot_move_between_threads_compiles_and_runs() {
    // Spec SC-015, and the reason there is no `Send` bound in the traits. A connection over
    // this transport is not `Send` — asserted below by the fact that it is driven with
    // `LocalSet` rather than `spawn`, which would not accept it.
    tokio::time::timeout(PATIENCE, async {
        let to_server = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let to_client = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        let client = ThreadBound {
            inner: std::rc::Rc::clone(&to_client),
            peer: std::rc::Rc::clone(&to_server),
        };
        let peer = ThreadBound {
            inner: to_server,
            peer: to_client,
        };

        // The guarantee itself, checked by the compiler rather than implied by how the
        // test happens to run it. If a `Send` bound ever appeared in the transport traits,
        // this connection would become `Send` and the call below would stop compiling.
        let (_probe, probing) = ngnet_h2::http::handshake::<_, Full>(ThreadBound {
            inner: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            peer: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        })
        .expect("handshake");
        assert_not_send(&probing);
        drop(probing);

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let serving = tokio::task::spawn_local(async move {
                    let _ = server::serve(peer, echo).expect("serving").await;
                });

                let received = ask(client, "/echo", b"thread bound")
                    .await
                    .expect("an exchange");
                assert_eq!(received, b"thread bound");
                serving.abort();
            })
            .await;
    })
    .await
    .expect("the exchange stalled");
}

// ---------------------------------------------------------------------------
// A third-party client
// ---------------------------------------------------------------------------

/// What one request to the async server looked like from `curl`'s side.
#[derive(Debug)]
struct CurlResult {
    status: String,
    body: String,
}

fn curl(url: &str, payload: Option<&str>) -> Option<CurlResult> {
    let mut command = std::process::Command::new("curl");
    command.args(["--http2-prior-knowledge", "-s", "-i", url]);
    if let Some(payload) = payload {
        command.args(["--data", payload]);
    }

    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))?;
    Some(CurlResult {
        status: head.lines().next()?.trim().to_owned(),
        body: body.to_owned(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_third_party_client_completes_a_request() {
    // Spec SC-002. Everything else in this suite has this crate on both ends, which cannot
    // catch a shared misreading of the protocol. `curl` has no such stake.
    if std::process::Command::new("curl")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("curl is not installed; skipping the third-party client check");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
    let addr = listener.local_addr().expect("an address");

    let serving = tokio::spawn(async move {
        loop {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            tokio::spawn(async move {
                let _ = server::serve(TokioIo::new(stream), echo)
                    .expect("serving")
                    .await;
            });
        }
    });

    let plain = tokio::task::spawn_blocking(move || curl(&format!("http://{addr}/hello"), None))
        .await
        .expect("the curl task")
        .expect("curl completed a request");
    assert_eq!(plain.status, "HTTP/2 200");
    assert_eq!(plain.body, "");

    let echoed = tokio::task::spawn_blocking(move || {
        curl(&format!("http://{addr}/echo"), Some("from curl"))
    })
    .await
    .expect("the curl task")
    .expect("curl completed a request");
    assert_eq!(echoed.status, "HTTP/2 200");
    assert_eq!(echoed.body, "from curl");

    serving.abort();
}

// ---------------------------------------------------------------------------
// Concurrency on one connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn four_streams_share_one_real_connection() {
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let _ = server::serve(TokioIo::new(stream), echo)
                .expect("serving")
                .await;
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (requests, connection) =
            ngnet_h2::http::handshake::<_, Full>(TokioIo::new(stream)).expect("handshake");

        let seen: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let recorder = Arc::clone(&seen);

        let exchange = async move {
            let mut waiting = Vec::new();
            for index in 0..4 {
                let path = format!("/stream-{index}");
                waiting.push((
                    path.clone(),
                    requests.send_request(
                        http::Request::builder()
                            .method(http::Method::POST)
                            .uri(format!("http://example.test{path}"))
                            .body(Full::new(format!("payload {index}")))
                            .expect("a request"),
                    ),
                ));
            }

            // Awaited in reverse, so a response that arrived while nothing was looking is
            // still waiting when someone finally looks.
            for (path, response) in waiting.into_iter().rev() {
                let response = response.await.expect("a response");
                let body = drain(response.into_body()).await.expect("a body");
                recorder.lock().expect("record").insert(path, body);
            }
            drop(requests);
        };

        tokio::join!(exchange, connection)
            .1
            .expect("the connection");

        let seen = seen.lock().expect("record");
        assert_eq!(seen.len(), 4);
        for index in 0..4 {
            assert_eq!(
                seen.get(&format!("/stream-{index}")).map(Vec::as_slice),
                Some(format!("payload {index}").as_bytes()),
            );
        }
    })
    .await
    .expect("the exchanges stalled");
}

// ---------------------------------------------------------------------------
// One handle, shared across tasks
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_client_handle_serves_many_tasks() {
    // SF-6. The `SendRequest` handle is `Clone` and `Send` on purpose, so a request may be
    // issued from a task that is not the one driving the connection. Nothing proved that
    // until here: the other multi-stream test submits every request from the single task
    // it also drives the connection on, and the `Send` check elsewhere is only a type
    // assertion. This spawns the driver on its own task, clones the handle into several
    // more, and has each issue a distinct request concurrently — the realistic failure is
    // a silent hang, which the surrounding timeout is here to catch.
    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        // The set of paths the one server connection saw. That every stream lands in this
        // one connection's record is what proves they shared the single connection.
        let seen: Arc<Mutex<BTreeSet<String>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let recorder = Arc::clone(&seen);

        let serving = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            server::serve(
                TokioIo::new(stream),
                move |request: http::Request<IncomingBody>| {
                    let recorder = Arc::clone(&recorder);
                    async move {
                        let path = request.uri().path().to_owned();
                        let body = drain(request.into_body()).await.unwrap_or_default();
                        recorder.lock().expect("record").insert(path.clone());
                        http::Response::builder()
                            .status(http::StatusCode::OK)
                            .header("x-path", path)
                            .body(Full::new(body))
                            .expect("a well-formed response")
                    }
                },
            )
            .expect("serving")
            .await
        });

        let stream = TcpStream::connect(addr).await.expect("connecting");
        let (requests, connection) =
            ngnet_h2::http::handshake::<_, Full>(TokioIo::new(stream)).expect("handshake");

        // The driver is a task of its own; every request below is issued from a different
        // one, through a clone of the handle.
        let driving = tokio::spawn(connection);

        const TASKS: usize = 6;
        let mut tasks = Vec::new();
        for index in 0..TASKS {
            let handle = requests.clone();
            tasks.push(tokio::spawn(async move {
                let path = format!("/task-{index}");
                let payload = format!("payload {index}");
                let response = handle
                    .send_request(
                        http::Request::builder()
                            .method(http::Method::POST)
                            .uri(format!("http://example.test{path}"))
                            .body(Full::new(payload.clone()))
                            .expect("a request"),
                    )
                    .await
                    .expect("a response");
                assert_eq!(response.status(), http::StatusCode::OK);
                assert_eq!(
                    response
                        .headers()
                        .get("x-path")
                        .map(|value| value.as_bytes()),
                    Some(path.as_bytes()),
                    "a task received a response meant for another",
                );
                let body = drain(response.into_body()).await.expect("a body");
                assert_eq!(body, payload.as_bytes(), "the echoed body did not match");
            }));
        }

        // The connection winds down once the last handle is gone: this one, and the clones
        // the tasks drop as they finish.
        drop(requests);
        for task in tasks {
            task.await.expect("a task");
        }

        driving
            .await
            .expect("the driver task")
            .expect("the connection");
        serving.await.expect("the server task").expect("the server");

        let seen = seen.lock().expect("record");
        assert_eq!(seen.len(), TASKS, "the peer did not see every stream");
        for index in 0..TASKS {
            assert!(
                seen.contains(&format!("/task-{index}")),
                "the peer never saw stream /task-{index}",
            );
        }
    })
    .await
    .expect("the exchanges stalled");
}

/// A drained server hangs up on a peer that will not hang up on it.
///
/// The `ngnet-axum` suite covers the drain end to end, but it does so with hyper as the
/// client, and hyper is well behaved: it reads the GOAWAY, has nothing outstanding, and
/// closes the socket. A server that merely waited to be disconnected from would look
/// identical against such a peer. That is not a hypothetical worry — reverting the driver's
/// completion signal left every one of those tests green, which is why this one exists here,
/// against the layer that actually owns the behaviour.
///
/// The peer is therefore a bare socket: it completes the handshake and then does nothing
/// forever. It sends no further frames and, above all, never closes. The assertion is that
/// the server's own future resolves, and that the socket sees a clean end of stream that it
/// did not ask for.
#[tokio::test]
async fn a_drained_server_closes_a_connection_whose_peer_never_does() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    tokio::time::timeout(PATIENCE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
        let addr = listener.local_addr().expect("an address");

        let (ready, drained) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accepting");
            let connection = server::serve(TokioIo::new(stream), echo).expect("serving");

            // Taken before the connection is awaited, which is the only order available: the
            // future is consumed by the `await` below and there is nothing to ask afterwards.
            ready.send(connection.drain_handle()).expect("a listener");
            let _ = connection.await;
        });

        let mut socket = TcpStream::connect(addr).await.expect("connecting");

        // The client preface and an empty SETTINGS frame — length 0, type 0x04, no flags, on
        // the connection stream. A real HTTP/2 peer, and nothing beyond that.
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .expect("the preface");
        socket
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .await
            .expect("settings");
        socket.flush().await.expect("a flush");

        let handle = drained.await.expect("a drain handle");
        handle.drain();

        // Read to the end. What the server sends on the way out is its own business; the
        // claim is that the stream ends cleanly, and that we are not the ones ending it.
        let mut sink = Vec::new();
        socket
            .read_to_end(&mut sink)
            .await
            .expect("a clean end of stream");

        server.await.expect("the server task");

        // Still open here, deliberately. Had it been dropped earlier the wait above would
        // have proved nothing, since a vanished peer ends a connection all by itself.
        drop(socket);
    })
    .await
    .expect("the drain stalled");
}
