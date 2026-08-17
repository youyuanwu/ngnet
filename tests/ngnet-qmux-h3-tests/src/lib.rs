//! Harness for driving HTTP/3 over QMux.
//!
//! Two substrates, because they prove different things. The in-memory pair runs a whole
//! connection with no runtime underneath it and no `Send` anywhere, which is what shows the
//! join imposes neither; loopback TCP puts real bytes through a real socket, which is where
//! framing and short-write defects live.
//!
//! Everything here is deliberately thin. A harness that drove the connection itself would be
//! testing the harness: these helpers build the two ends, hand the drivers to a runtime and
//! then get out of the way.

use core::convert::Infallible;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full};
use ngnet_h3::http::{IncomingBody, SendRequest};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{TokioClock, TokioStream};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use tokio::net::TcpStream;

/// The body type the tests send and receive.
pub type Payload = Full<Bytes>;

/// The client handle these helpers hand back.
pub type Sender = SendRequest<Payload>;

/// How long anything in these tests may take before it is treated as hung.
///
/// Generous, because a loaded machine is not a deadlock. Present at all because the failure
/// this suite is most likely to catch is a connection that never makes progress, and a test
/// that hangs reports that as a timed-out job with no output rather than as a failure.
pub const LIMIT: core::time::Duration = core::time::Duration::from_secs(30);

/// Starts a client and a server over an in-memory byte stream pair.
///
/// Both drivers are spawned on the current [`LocalSet`](tokio::task::LocalSet), which is
/// required rather than incidental: neither the byte streams nor the clock is `Send`, and
/// that is the point — a caller on a thread-per-core runtime has exactly this arrangement.
///
/// # Panics
///
/// If either end cannot be built.
pub fn memory_pair<H, F, B>(handler: H) -> Sender
where
    H: FnMut(Request<IncomingBody>) -> F + 'static,
    F: Future<Output = Response<B>> + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    memory_pair_sending::<Payload, _, _, _>(handler)
}

/// [`memory_pair`], for a client that sends something other than a [`Payload`].
///
/// Separate only because the request body type is the client handle's, and a test that wants
/// a body which stalls or fails has to name its own.
///
/// # Panics
///
/// If either end cannot be built.
pub fn memory_pair_sending<Out, H, F, B>(handler: H) -> SendRequest<Out>
where
    Out: Body<Data = Bytes> + Send + 'static,
    Out::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
    H: FnMut(Request<IncomingBody>) -> F + 'static,
    F: Future<Output = Response<B>> + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let (client_io, server_io) = stream_pair();
    let clock = TestClock::new();

    let server = ngnet_qmux_h3::serve(server_io, clock.clone(), handler).expect("serving");
    tokio::task::spawn_local(async move {
        let _ = server.await;
    });

    let (sender, connection) =
        ngnet_qmux_h3::connect::<_, _, Out>(client_io, clock).expect("starting the client");
    tokio::task::spawn_local(async move {
        let _ = connection.await;
    });
    sender
}

/// [`memory_pair`], with configurations other than the defaults.
///
/// Both ends are built through the `_with` entry points and both are given the *same* pair of
/// configurations. That is deliberate rather than incidental: a QMux end's transport
/// configuration is a set of permissions it advertises to its peer, so configuring one end
/// only would leave a test unable to say which direction its observation came from. A test
/// that needs the asymmetry — the stream allowance a server grants a client, say — should
/// build the two ends itself from [`memory_streams`].
///
/// # Panics
///
/// If either end cannot be built.
pub fn memory_pair_with<H, F, B>(handler: H, transport: TransportConfig, http: HttpConfig) -> Sender
where
    H: FnMut(Request<IncomingBody>) -> F + 'static,
    F: Future<Output = Response<B>> + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let (client_io, server_io) = stream_pair();
    let clock = TestClock::new();

    let server = ngnet_qmux_h3::serve_with(server_io, clock.clone(), handler, transport, http)
        .expect("serving");
    tokio::task::spawn_local(async move {
        let _ = server.await;
    });

    let (sender, connection) =
        ngnet_qmux_h3::connect_with::<_, _, Payload>(client_io, clock, transport, http)
            .expect("starting the client");
    tokio::task::spawn_local(async move {
        let _ = connection.await;
    });
    sender
}

/// The two halves of an in-memory pair, plus the clock both ends share.
///
/// For a test that builds the two ends itself rather than taking [`memory_pair`]'s.
#[must_use]
pub fn memory_streams() -> (TestByteStream, TestByteStream, TestClock) {
    let (left, right) = stream_pair();
    (left, right, TestClock::new())
}

/// A connected loopback TCP pair.
///
/// The server half comes from an accept on an ephemeral port, so nothing here depends on a
/// fixed port being free.
///
/// # Panics
///
/// If the loopback socket cannot be bound, connected or accepted.
pub async fn tcp_pair() -> (TokioStream<TcpStream>, TokioStream<TcpStream>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding a loopback listener");
    let address = listener.local_addr().expect("a bound address");
    let connecting = tokio::spawn(async move { TcpStream::connect(address).await });
    let (server, _peer) = listener.accept().await.expect("accepting a connection");
    let client = connecting
        .await
        .expect("the connect task")
        .expect("connecting to the loopback listener");
    // Nagle's algorithm would hold a small record back waiting for company, which turns an
    // exchange into a round trip per timer tick and makes a working connection look slow.
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    (TokioStream::new(client), TokioStream::new(server))
}

/// The clock the loopback tests use.
#[must_use]
pub fn tokio_clock() -> TokioClock {
    TokioClock::new()
}

/// A request with no body.
///
/// # Panics
///
/// If `uri` is not a valid URI.
#[must_use]
pub fn get(uri: &str) -> Request<Payload> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Payload::default())
        .expect("a request")
}

/// A request carrying `body`.
///
/// # Panics
///
/// If `uri` is not a valid URI.
#[must_use]
pub fn post(uri: &str, body: impl Into<Bytes>) -> Request<Payload> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .body(Payload::new(body.into()))
        .expect("a request")
}

/// A 200 carrying `body`.
///
/// # Panics
///
/// Never; every part of it is fixed.
#[must_use]
pub fn ok(body: impl Into<Bytes>) -> Response<Payload> {
    Response::builder()
        .status(200)
        .body(Payload::new(body.into()))
        .expect("a response")
}

/// Reads an incoming body to the end.
///
/// # Errors
///
/// Reports whatever the HTTP/3 layer reported about the body.
pub async fn drain(body: IncomingBody) -> Result<Bytes, Box<dyn core::error::Error + Send + Sync>> {
    Ok(body.collect().await?.to_bytes())
}

/// How long [`Failing`] waits before it fails.
///
/// Long enough for the chunk it already produced to have reached the peer, short enough that
/// a test spends no real time on it.
const PAUSE: core::time::Duration = core::time::Duration::from_millis(50);

/// A predictable payload of `len` bytes.
///
/// Not zeroes and not random: a repeating non-power-of-two cycle, so a reassembly that
/// duplicated or dropped a record shows up as a mismatch rather than as an identical byte in
/// the wrong place.
#[must_use]
pub fn pattern(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// A body that offers what it was given, pauses, and then fails.
///
/// An application whose response falls apart partway through, which is the only way an
/// HTTP/3 response gets abandoned once its headers have gone out: the status line has
/// already been promised, so the stream has to be reset rather than the answer changed.
///
/// The pause is what puts the headers and the bytes already offered on the wire before the
/// failure happens -- without it the reset overtakes the response entirely, which is a real
/// case but a different one.
///
/// How much is offered is the caller's to choose, and the choice decides which case is
/// being exercised. The chunks are handed over in one pass, so many of them leave the
/// transport with far more queued than it can write and the reset has a backlog to discard;
/// one small chunk leaves nothing queued at all, and then the reset is the only thing that
/// distinguishes an abandoned message from a complete one.
pub struct Failing {
    chunk: Bytes,
    remaining: usize,
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Failing {
    /// A body that offers `count` copies of `chunk` and then fails.
    #[must_use]
    pub fn new(chunk: Bytes, count: usize) -> Self {
        Self {
            chunk,
            remaining: count,
            pause: None,
        }
    }
}

/// The failure [`Failing`] reports.
#[derive(Debug)]
pub struct Broken;

impl core::fmt::Display for Broken {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the response body failed partway through")
    }
}

impl core::error::Error for Broken {}

impl Body for Failing {
    type Data = Bytes;
    type Error = Broken;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Broken>>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            let chunk = self.chunk.clone();
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        let pause = self
            .pause
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(PAUSE)));
        match pause.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => Poll::Ready(Some(Err(Broken))),
        }
    }
}

/// A body that yields one chunk and then stalls forever.
///
/// What an application that started a body and then stopped producing looks like. The stall
/// is what makes the peer's reaction observable: a request abandoned mid-body has to tell
/// the other end, or the stream stays open and the credit it spent is never returned.
pub struct Stalling {
    first: Option<Bytes>,
    stall: bool,
}

impl Stalling {
    /// A body whose first and only chunk is `first`, and which then stops.
    #[must_use]
    pub fn new(first: Bytes) -> Self {
        Self {
            first: Some(first),
            stall: true,
        }
    }

    /// A body that is over before it starts.
    ///
    /// The ordinary request on a connection whose client body type is this one -- a test
    /// that abandons one request still has to make another, and the second is only
    /// interesting for completing.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            first: None,
            stall: false,
        }
    }
}

impl Body for Stalling {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(chunk) = self.first.take() {
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        if self.stall {
            // Never woken, deliberately. This is a body that has stopped, not one that is
            // waiting for something.
            return Poll::Pending;
        }
        Poll::Ready(None)
    }
}
