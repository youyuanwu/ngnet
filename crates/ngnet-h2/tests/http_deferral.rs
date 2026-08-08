//! The deferral mechanism, proved (Spec SC-008).
//!
//! An asynchronous outgoing body says "not yet" by suspending its stream, and nothing but
//! `resume_body` will ever restart it. Everything downstream of that — streaming uploads,
//! backpressure, the server's response bodies — is built on the assumption that a wake
//! reliably brings the stream back and that nothing else does.
//!
//! These are the tests that make it an assumption worth building on. They assert the
//! mechanism from both directions: that a wake *does* resume the stream, and that without
//! one the body is left alone entirely.

#![cfg(feature = "http")]

use std::io;
use std::sync::{Arc, Mutex};

use ngnet_h2::http::testing::{
    self, Duplex, DuplexWriter, Scripted, alongside, block_on, bytes_crate as bytes, duplex,
    http_crate as http, scripted, serve,
};
use ngnet_h2::http::transport::Coalesced;
use ngnet_h2::http::{Transport, TransportRead};
use ngnet_h2::{
    FrameType, Header, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

/// Yields once, so the driver gets a full poll before the test looks again.
async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(move |cx| {
        if yielded {
            core::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    })
    .await;
}

/// Gives the driver several passes to do whatever it is going to do.
///
/// Deliberately generous. A property of the form "this did *not* happen" is only worth
/// asserting if the thing had ample opportunity to happen.
async fn settle() {
    for _ in 0..16 {
        yield_now().await;
    }
}

/// What the peer received, readable from the test while the peer is still running.
#[derive(Debug, Default)]
struct Received {
    payload: Vec<u8>,
    pending: Vec<i32>,
    answered: usize,
}

/// The peer's state, shared so assertions can be made mid-exchange.
#[derive(Debug, Default, Clone)]
struct Recorder(Arc<Mutex<Received>>);

impl Recorder {
    fn with<R>(&self, act: impl FnOnce(&mut Received) -> R) -> R {
        act(&mut self.0.lock().expect("the recorder"))
    }
}

fn recording_peer() -> Session<Recorder> {
    SessionBuilder::<Recorder>::server()
        .on_header(
            |_peer: &mut Recorder, _frame, _name: &[u8], _value: &[u8]| HeaderAction::Continue,
        )
        .on_data_chunk(|peer: &mut Recorder, _stream, chunk: &[u8]| {
            peer.with(|state| state.payload.extend_from_slice(chunk));
        })
        .on_frame(|peer: &mut Recorder, frame| {
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && frame.category() == Some(HeaderCategory::Request)
            {
                peer.with(|state| state.pending.push(frame.stream_id().get()));
            }
        })
        .build()
        .expect("building the peer session")
}

fn answer_plainly(session: &mut Session<Recorder>, peer: &mut Recorder) {
    let pending = peer.with(|state| core::mem::take(&mut state.pending));
    for stream in pending {
        session
            .submit_response(StreamId::new(stream), &[Header::new(":status", "200")])
            .expect("submitting a response");
        peer.with(|state| state.answered += 1);
    }
}

fn upload(body: Scripted) -> http::Request<Scripted> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri("http://example.test/upload")
        .body(body)
        .expect("building a request")
}

/// Runs `main` with the connection alongside, remembering how the connection ended.
///
/// The connection is the background future, so a failure would otherwise be invisible —
/// which is precisely the failure mode several of these tests are guarding against.
fn watched<M, C, O>(main: M, connection: C) -> (O, Option<ngnet_h2::http::Result<()>>)
where
    M: Future<Output = O>,
    C: Future<Output = ngnet_h2::http::Result<()>>,
{
    let outcome: Arc<Mutex<Option<ngnet_h2::http::Result<()>>>> = Arc::new(Mutex::new(None));
    let recorded = Arc::clone(&outcome);
    let watcher = async move {
        let result = connection.await;
        *recorded.lock().expect("the connection outcome") = Some(result);
    };

    let value = block_on(alongside(main, watcher));
    let ended = outcome.lock().expect("the connection outcome").take();
    (value, ended)
}

use core::future::Future;

#[test]
fn a_deferred_body_is_consulted_only_after_a_wake() {
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    let peer = Recorder::default();
    let mut peer_context = peer.clone();
    let response = requests.send_request(upload(body));

    let exchange = async {
        settle().await;

        // One consultation, which deferred. Everything after this point is about what
        // must *not* happen while the body stays deferred.
        assert_eq!(script.consultations(), 1, "the body was consulted once");
        assert!(script.is_deferred(), "the body parked a waker");
        assert!(
            peer.with(|state| state.payload.is_empty()),
            "no payload reached the peer",
        );

        settle().await;
        assert_eq!(
            script.consultations(),
            1,
            "a deferred body must not be consulted again without a wake",
        );

        script.send(&b"first"[..]);
        settle().await;
        assert!(
            script.consultations() >= 2,
            "the wake brought the body back",
        );
        assert_eq!(
            peer.with(|state| state.payload.clone()),
            b"first".to_vec(),
            "the resumed body's octets reached the peer",
        );

        let consulted = script.consultations();
        settle().await;
        assert_eq!(
            script.consultations(),
            consulted,
            "the body deferred again and was left alone again",
        );

        script.send(&b"second"[..]);
        script.finish();
        let response = response.await.expect("a response");
        // The head may already have arrived, so awaiting it need not give the driver a
        // pass. The remaining octets still have to reach the peer.
        settle().await;
        drop(requests);
        response
    };

    let (response, ended) = watched(
        alongside(
            exchange,
            serve(
                server_side,
                recording_peer(),
                &mut peer_context,
                answer_plainly,
            ),
        ),
        connection,
    );

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(peer.with(|state| state.payload.clone()), b"firstsecond");
    if let Some(outcome) = ended {
        outcome.expect("the connection finished cleanly");
    }
}

#[test]
fn a_spurious_wake_costs_exactly_one_consultation() {
    let (client_side, _server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    let response = requests.send_request(upload(body));

    let exchange = async {
        settle().await;
        assert_eq!(script.consultations(), 1);

        // A wake with nothing behind it is permitted by the `Future` contract, so the
        // driver has to survive one. Surviving it means consulting the body once more and
        // letting it defer again — not spinning, and not treating the wake as readiness.
        script.wake_spuriously();
        settle().await;
        assert_eq!(
            script.consultations(),
            2,
            "a spurious wake costs one consultation and no more",
        );

        script.wake_spuriously();
        script.wake_spuriously();
        script.wake_spuriously();
        settle().await;
        assert!(
            script.consultations() <= 5,
            "three wakes cost at most three consultations, not a spin: {}",
            script.consultations(),
        );

        drop(requests);
    };

    let (_, ended) = watched(exchange, connection);
    assert!(
        !matches!(ended, Some(Err(_))),
        "spurious wakes must not fail the connection",
    );
    drop(response);
}

#[test]
fn a_wake_after_the_body_finished_is_swallowed() {
    let (client_side, _server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    let response = requests.send_request(upload(body));

    let exchange = async {
        settle().await;
        let waker = script.stale_waker().expect("the body parked a waker");

        // The body ends, but the peer never answers, so the stream stays in the driver's
        // registry. A wake now reaches `resume_body` and finds nothing deferred — the
        // exact `INVALID_ARGUMENT` the driver must swallow rather than propagate.
        script.finish();
        settle().await;

        for _ in 0..8 {
            waker.wake_by_ref();
        }
        settle().await;

        drop(requests);
    };

    let (_, ended) = watched(exchange, connection);
    assert!(
        !matches!(ended, Some(Err(_))),
        "a stale readiness note must fail neither the stream nor the connection",
    );
    drop(response);
}

#[test]
fn stale_wakes_from_closed_streams_do_not_accumulate() {
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(client_side).expect("handshake");

    let (body, script) = scripted();
    let mut peer_context = Recorder::default();
    let response = requests.send_request(upload(body));

    let exchange = async {
        settle().await;
        let waker = script.stale_waker().expect("the body parked a waker");

        script.send(&b"done"[..]);
        script.finish();
        response.await.expect("a response");
        settle().await;

        // The stream has closed. A body that cloned its waker can still fire it, and
        // de-duplication alone would not stop the identifiers piling up — the liveness
        // token is what does.
        for _ in 0..10_000 {
            waker.wake_by_ref();
        }
        assert_eq!(
            testing::pending_wakes(&requests),
            0,
            "wakes for a closed stream are discarded rather than queued",
        );

        drop(requests);
    };

    let (_, ended) = watched(
        alongside(
            exchange,
            serve(
                server_side,
                recording_peer(),
                &mut peer_context,
                answer_plainly,
            ),
        ),
        connection,
    );
    assert!(!matches!(ended, Some(Err(_))));
}

/// A body that wakes itself while being consulted, which happens inside `Session::send`.
struct SelfWaking {
    remaining: usize,
}

impl testing::http_body_crate::Body for SelfWaking {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Result<testing::http_body_crate::Frame<bytes::Bytes>, Self::Error>>>
    {
        if self.remaining == 0 {
            return core::task::Poll::Ready(None);
        }
        self.remaining -= 1;
        // Fired from inside the session's own serialisation. If waking needed a lock the
        // driver was already holding, this would deadlock rather than fail.
        cx.waker().wake_by_ref();
        core::task::Poll::Pending
    }
}

#[test]
fn a_body_that_wakes_itself_does_not_deadlock() {
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, SelfWaking>(client_side).expect("handshake");

    let mut peer_context = Recorder::default();
    let response = requests.send_request(
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/upload")
            .body(SelfWaking { remaining: 4 })
            .expect("building a request"),
    );

    let exchange = async {
        let response = response.await.expect("a response");
        drop(requests);
        response
    };

    let (response, ended) = watched(
        alongside(
            exchange,
            serve(
                server_side,
                recording_peer(),
                &mut peer_context,
                answer_plainly,
            ),
        ),
        connection,
    );

    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(!matches!(ended, Some(Err(_))));
}

/// A reading half that never completes, over a real writing half.
struct StalledRead(Duplex<Coalesced>);

/// A read that stays in flight for as long as the connection lives.
struct NeverReads;

impl TransportRead for NeverReads {
    async fn read(&mut self, buf: bytes::BytesMut) -> (io::Result<usize>, bytes::BytesMut) {
        // The buffer is held for the whole operation, exactly as a completion-based
        // transport would hold it while the kernel had it.
        let _held = buf;
        core::future::pending::<()>().await;
        unreachable!("a pending read never completes")
    }
}

impl Transport for StalledRead {
    type Reader = NeverReads;
    type Writer = DuplexWriter<Coalesced>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (_reader, writer) = self.0.split();
        (NeverReads, writer)
    }
}

#[test]
fn a_read_in_flight_does_not_block_a_write() {
    let (client_side, _server_side) = duplex();
    let writes = client_side.write_counter();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Scripted>(StalledRead(client_side)).expect("handshake");

    let (body, script) = scripted();
    let response = requests.send_request(upload(body));

    let exchange = async {
        settle().await;
        // The preface and the request head are out, and the read that followed them is
        // still in flight and will never complete.
        let before = writes.get();
        assert!(before > 0, "the connection wrote before parking on a read");

        script.send(&b"payload"[..]);
        settle().await;

        assert!(
            writes.get() > before,
            "an outgoing body's wake produced a write while a read was in flight",
        );

        drop(requests);
    };

    let (_, ended) = watched(exchange, connection);
    assert!(!matches!(ended, Some(Err(_))));
    drop(response);
}
