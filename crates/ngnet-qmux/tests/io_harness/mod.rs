//! The exchange body, written once because three test files and a later runtime run it.
//!
//! # Why there is an executor in here
//!
//! Two connections over an in-memory byte-stream pair make progress only by taking turns: the
//! client cannot open a stream until the server's transport parameters have arrived, and they
//! arrive only when the server has been polled. A single-future `block_on` deadlocks on that
//! immediately. So this module polls both futures round-robin until each completes.
//!
//! It is a real executor rather than a spin loop, and the difference matters. The waker it
//! hands out records that it fired; a pass in which neither future completed *and* nothing
//! woke is a genuine stall, and it is reported as one rather than spun on until a timeout.
//! That turns "the test hangs" -- the least informative failure a connection can produce --
//! into a named assertion. It is built on [`Wake`], which is safe: nothing in this crate's
//! tests uses `unsafe`, and a structural test enforces that.
//!
//! # Why the bodies are generic and asynchronous
//!
//! [`client_exchange`] and [`server_exchange`] name no byte stream and no clock. The same two
//! functions therefore run over the in-memory pair here and over a real socket once a runtime
//! implementation of the seam exists, which is what makes "the seam is not shaped around one
//! implementation" something a reader can check rather than take on trust. They are `async`
//! for the same reason: a runtime drives them by spawning, and this file drives them by
//! polling, and neither has to know how the other does it.

// Each test target uses a different subset of this module, and an unused helper in one of them
// is not a defect. Denying it would push every test towards using everything.
#![allow(dead_code)]
// Included by targets that are themselves gated on the feature; stated here as well so the
// module is inert rather than broken when the layer is absent.
#![cfg(feature = "io")]

use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Clock, Config, Connection, Error, Event, Result};
use ngnet_qmux::{CloseReason, Role, StreamId};

/// How many round-robin passes before an exchange is declared broken.
///
/// Only reached by a future that is woken on every pass and never finishes -- a livelock, which
/// the wake-tracking stall detector cannot see. Generous enough that a byte-at-a-time transfer
/// of a large payload finishes well inside it.
const MAX_PASSES: usize = 20_000_000;

/// A waker that remembers it was woken.
#[derive(Default)]
struct Flag {
    woken: AtomicBool,
}

impl Flag {
    fn take(&self) -> bool {
        self.woken.swap(false, Ordering::SeqCst)
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

/// Runs one future to completion, failing the test if it stops making progress.
pub fn run<F: Future>(future: F) -> F::Output {
    let flag = Arc::new(Flag::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    for _ in 0..MAX_PASSES {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        assert!(
            flag.take(),
            "the connection is waiting for something that will never happen: it reported \
             pending and registered no wake"
        );
    }
    panic!("the exchange never finished; it is being woken without making progress");
}

/// Runs two futures round-robin, which is what two connections over one pair require.
pub fn run_pair<A: Future, B: Future>(left: A, right: B) -> (A::Output, B::Output) {
    let flag = Arc::new(Flag::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);

    let mut left = Box::pin(left);
    let mut right = Box::pin(right);
    let mut left_out = None;
    let mut right_out = None;

    for _ in 0..MAX_PASSES {
        if left_out.is_none()
            && let Poll::Ready(output) = left.as_mut().poll(&mut cx)
        {
            left_out = Some(output);
        }
        if right_out.is_none()
            && let Poll::Ready(output) = right.as_mut().poll(&mut cx)
        {
            right_out = Some(output);
        }
        // Taken and put back rather than tested and then unwrapped: the outputs are values,
        // and only the pass on which both are present may move them out.
        match (left_out.take(), right_out.take()) {
            (Some(left_done), Some(right_done)) => return (left_done, right_done),
            (left_pending, right_pending) => {
                left_out = left_pending;
                right_out = right_pending;
            }
        }
        // A pass in which nothing completed and nothing was woken cannot be followed by a pass
        // that differs, so the exchange is stuck. Said here rather than left to a timeout.
        assert!(
            flag.take(),
            "the exchange stalled: neither side finished and neither registered a wake"
        );
    }
    panic!("the exchange never finished; both sides are being woken without making progress");
}

/// Polls once with a waker that does nothing, for answers that are immediate.
pub fn poll_once<T>(f: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    f(&mut cx)
}

/// A pair of connections over one in-memory byte stream, in the two roles.
pub fn connected_pair(
    config: Config,
) -> (
    Connection<TestByteStream, TestClock>,
    Connection<TestByteStream, TestClock>,
) {
    let (client_side, server_side) = stream_pair();
    let client = Connection::client(client_side, TestClock::new(), config).expect("a client");
    let server = Connection::server(server_side, TestClock::new(), config).expect("a server");
    (client, server)
}

/// The same, with the caps a byte-at-a-time exchange needs applied to both halves.
pub fn connected_pair_one_byte_at_a_time(
    config: Config,
) -> (
    Connection<TestByteStream, TestClock>,
    Connection<TestByteStream, TestClock>,
) {
    let (client_side, server_side) = stream_pair();
    for side in [&client_side, &server_side] {
        side.set_read_cap(Some(1));
        side.set_write_cap(Some(1));
    }
    let client = Connection::client(client_side, TestClock::new(), config).expect("a client");
    let server = Connection::server(server_side, TestClock::new(), config).expect("a server");
    (client, server)
}

/// One connection with the far end of its byte stream left in the test's hands.
///
/// The far end is what a test uses to play the peer: to deliver bytes no conforming peer would
/// send, to read what the connection wrote, or to vanish at a chosen moment.
pub fn connection_with_peer_stream(
    role: Role,
) -> (Connection<TestByteStream, TestClock>, TestByteStream) {
    let (near, far) = stream_pair();
    let conn = match role {
        Role::Client => Connection::client(near, TestClock::new(), Config::new()),
        Role::Server => Connection::server(near, TestClock::new(), Config::new()),
    }
    .expect("constructing a connection");
    (conn, far)
}

/// Whatever the connection has written so far, read off the far end of the byte stream.
pub fn drain_written(far: &mut TestByteStream) -> Vec<u8> {
    let mut collected = Vec::new();
    let mut buffer = [0u8; 4096];
    while let Poll::Ready(Ok(read)) = poll_once(|cx| far.poll_read(cx, &mut buffer)) {
        if read == 0 {
            break;
        }
        collected.extend_from_slice(&buffer[..read]);
    }
    collected
}

/// A real transport-parameters record, produced by a real connection in `role`.
///
/// Handwritten bytes would be a second implementation of the encoding, and a test built on
/// them would pass while the layer and the wire disagreed. These came off a connection.
pub fn announcement_record(role: Role) -> Vec<u8> {
    let (mut conn, mut far) = connection_with_peer_stream(role);
    let pumped = poll_once(|cx| conn.poll_pump(cx));
    assert!(
        matches!(pumped, Poll::Ready(Ok(()))),
        "the first pump emits the announcement without needing anything from the peer"
    );
    let bytes = drain_written(&mut far);
    assert!(
        !bytes.is_empty(),
        "a connection announces its transport parameters unprompted; nothing else can proceed \
         until it does"
    );
    bytes
}

/// The next event, or the connection's ending.
pub async fn next_event<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
) -> Result<Event> {
    poll_fn(|cx| conn.poll_next_event(cx)).await
}

/// Opens a bidirectional stream, waiting for capacity if the peer has not granted it yet.
pub async fn open_bidi<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
) -> Result<StreamId> {
    poll_fn(|cx| conn.poll_open_bidi(cx)).await
}

/// Opens a unidirectional stream.
pub async fn open_uni<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
) -> Result<StreamId> {
    poll_fn(|cx| conn.poll_open_uni(cx)).await
}

/// Writes every byte of `data`, however many records and windows that takes.
///
/// A write reports what it took, which may be less than it was offered; a caller that ignored
/// the count would truncate its own payload silently, so the loop is part of using the API
/// rather than an optimisation.
pub async fn write_all<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
    stream: StreamId,
    data: &[u8],
    fin: bool,
) -> Result<()> {
    let mut written = 0usize;
    while written < data.len() {
        let taken = poll_fn(|cx| conn.poll_write_stream(cx, stream, &data[written..], fin)).await?;
        written += taken;
    }
    if data.is_empty() && fin {
        // An end of stream carrying nothing still has to be sent, and the loop above has no
        // iteration to send it in.
        poll_fn(|cx| conn.poll_write_stream(cx, stream, &[], true)).await?;
    }
    Ok(())
}

/// Drives the connection until everything produced has reached the byte stream.
pub async fn flush<S: AsyncByteStream, C: Clock>(conn: &mut Connection<S, C>) -> Result<()> {
    poll_fn(|cx| conn.poll_pump(cx)).await
}

/// Closes the connection with `reason`, and waits for the close to reach the byte stream.
pub async fn close<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
    reason: &CloseReason,
) -> Result<()> {
    poll_fn(|cx| conn.poll_close(cx, reason)).await
}

/// Collects data from `stream` until its end of stream arrives.
pub async fn read_stream<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
    stream: StreamId,
) -> Result<Vec<u8>> {
    let mut received = Vec::new();
    loop {
        if let Event::StreamData {
            stream_id,
            data,
            fin,
            ..
        } = next_event(conn).await?
            && stream_id == stream
        {
            received.extend_from_slice(&data);
            if fin {
                return Ok(received);
            }
        }
    }
}

/// Waits for the peer's first stream, and collects it to its end.
pub async fn accept_stream<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
) -> Result<(StreamId, Vec<u8>)> {
    let mut received = Vec::new();
    let mut accepted: Option<StreamId> = None;
    loop {
        if let Event::StreamData {
            stream_id,
            data,
            fin,
            ..
        } = next_event(conn).await?
        {
            let stream = *accepted.get_or_insert(stream_id);
            if stream_id != stream {
                continue;
            }
            received.extend_from_slice(&data);
            if fin {
                return Ok((stream, received));
            }
        }
    }
}

/// The client half of the canonical exchange: one request out, one response back.
///
/// Opening comes first and waits until the peer's parameters arrive, which is the first-flight
/// problem in miniature -- it completes only because both sides announce themselves without
/// being asked.
pub async fn client_exchange<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
    request: &[u8],
) -> Vec<u8> {
    let stream = open_bidi(conn).await.expect("opening a stream");
    write_all(conn, stream, request, true)
        .await
        .expect("writing the request");
    read_stream(conn, stream)
        .await
        .expect("reading the response")
}

/// The server half: accepts the client's stream, reads it whole, and answers on the same one.
///
/// Returns what it received, so a test can assert on both directions from one exchange.
pub async fn server_exchange<S: AsyncByteStream, C: Clock>(
    conn: &mut Connection<S, C>,
    response: &[u8],
) -> Vec<u8> {
    let (stream, received) = accept_stream(conn).await.expect("accepting a stream");
    write_all(conn, stream, response, true)
        .await
        .expect("writing the response");
    flush(conn).await.expect("flushing the response");
    received
}

/// Runs the canonical exchange over a pair of connections and asserts both directions.
pub fn exchange<S: AsyncByteStream, C: Clock>(
    client: &mut Connection<S, C>,
    server: &mut Connection<S, C>,
    request: &[u8],
    response: &[u8],
) {
    let (received_response, received_request) = run_pair(
        client_exchange(client, request),
        server_exchange(server, response),
    );

    assert_eq!(
        received_request, request,
        "the request did not survive the exchange"
    );
    assert_eq!(
        received_response, response,
        "the response did not survive the exchange"
    );
}

/// Polls for events until the connection reports how it ended.
pub fn drain_to_ending<S: AsyncByteStream, C: Clock>(conn: &mut Connection<S, C>) -> Error {
    run(async {
        loop {
            match next_event(conn).await {
                Ok(_) => {}
                Err(error) => return error,
            }
        }
    })
}
