//! The async HTTP/3 layer, over a real QUIC connection.
//!
//! Everything the wrapper's own suite proves, it proves in memory. This is the other half:
//! the same exchanges over real QUIC, with real encryption, a real UDP socket and a real
//! congestion controller, driven by a genuinely independent implementation of the backend
//! trait.
//!
//! That independence is the point. If the trait had quietly been shaped around the in-memory
//! double, writing `QuinnBackend` would have been a fight; it was not, and these tests are
//! what says so. The two implementations also disagree in exactly the way the trait expects
//! them to: the double declares `RETAINS_BUFFERS = true` and must report release explicitly,
//! quinn declares `false` because it copies. Both arms are exercised.
//!
//! `tests/ngnet-h3-tests/tests/quic.rs` remains alongside this, driving the *sans-I/O core*
//! over quinn. Neither replaces the other.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use ngnet_h3::http::quic::Timestamp;
use ngnet_h3::http::{
    Cancelled, IncomingBody, QuicConnection, QuicEvent, StreamSource, handshake, serve,
};
use ngnet_h3::{ErrorCode, StreamId};
use ngnet_h3_quinn::{QuinnBackend, QuinnError};
use ngnet_h3_tests::Tuning;

/// A single-chunk body, since `http-body-util` is not a dependency here either.
struct Payload {
    chunk: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
}

fn empty() -> Payload {
    Payload {
        chunk: None,
        trailers: None,
    }
}

fn once(bytes: Bytes) -> Payload {
    Payload {
        chunk: Some(bytes),
        trailers: None,
    }
}

fn with_trailers(bytes: Bytes, trailers: http::HeaderMap) -> Payload {
    Payload {
        chunk: Some(bytes),
        trailers: Some(trailers),
    }
}

impl Body for Payload {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(chunk) = self.chunk.take() {
            return std::task::Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        if let Some(trailers) = self.trailers.take() {
            return std::task::Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        std::task::Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.chunk.is_none() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

type Answer = std::pin::Pin<Box<dyn core::future::Future<Output = http::Response<Payload>> + Send>>;

#[derive(Default)]
struct EventLog {
    pending: u64,
    fins: HashMap<StreamId, u64>,
    closes: HashMap<StreamId, (u64, Option<ErrorCode>, Option<ErrorCode>, usize)>,
    peer_resets: Vec<(StreamId, ErrorCode)>,
    local_resets: Vec<(StreamId, ErrorCode)>,
    local_stops: Vec<(StreamId, ErrorCode)>,
}

impl EventLog {
    fn record(&mut self, event: &QuicEvent) {
        match event {
            QuicEvent::Data {
                stream, fin: true, ..
            } => {
                self.fins.insert(*stream, self.pending);
            }
            QuicEvent::StreamClosed {
                stream,
                rx_code,
                tx_code,
            } => {
                let entry =
                    self.closes
                        .entry(*stream)
                        .or_insert((self.pending, *rx_code, *tx_code, 0));
                entry.3 += 1;
            }
            QuicEvent::Reset { stream, code } => self.peer_resets.push((*stream, *code)),
            _ => {}
        }
    }
}

struct RecordedQuinn {
    inner: QuinnBackend,
    log: Arc<Mutex<EventLog>>,
}

impl RecordedQuinn {
    fn new(inner: QuinnBackend) -> (Self, Arc<Mutex<EventLog>>) {
        let log = Arc::new(Mutex::new(EventLog::default()));
        (
            Self {
                inner,
                log: Arc::clone(&log),
            },
            log,
        )
    }
}

impl QuicConnection for RecordedQuinn {
    type Error = QuinnError;

    const RETAINS_BUFFERS: bool = QuinnBackend::RETAINS_BUFFERS;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        match self.inner.poll_event(cx) {
            Poll::Pending => {
                self.log.lock().expect("event log").pending += 1;
                Poll::Pending
            }
            Poll::Ready(Ok(event)) => {
                self.log.lock().expect("event log").record(&event);
                Poll::Ready(Ok(event))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_transmit(cx, source)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.inner.poll_open_uni(cx)
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.inner.poll_open_bi(cx)
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.log
            .lock()
            .expect("event log")
            .local_resets
            .push((stream, code));
        self.inner.reset(stream, code)
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.log
            .lock()
            .expect("event log")
            .local_stops
            .push((stream, code));
        self.inner.stop_sending(stream, code)
    }

    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        self.inner.extend_credit(stream, bytes)
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error> {
        self.inner.close(code, reason)
    }

    fn now(&self) -> Timestamp {
        self.inner.now()
    }
}

/// Reads a body to the end, keeping any trailers it carried.
async fn read_body(mut body: IncomingBody) -> (Vec<u8>, Option<http::HeaderMap>) {
    let mut out = Vec::new();
    let mut trailers = None;
    loop {
        let frame = core::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            None | Some(Err(_)) => return (out, trailers),
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => out.extend_from_slice(&data),
                Err(frame) => {
                    if let Ok(map) = frame.into_trailers() {
                        trailers = Some(map);
                    }
                }
            },
        }
    }
}

/// A payload with a pattern that makes a misordered or duplicated run obvious.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Fails a test rather than hanging it, so a protocol bug is diagnosable.
const LIMIT: Duration = Duration::from_secs(20);

/// Runs a body of work with both drivers spawned, over a real connection.
async fn over_quic<F, Fut, T>(tuning: Tuning, handler: Responder, body: F) -> T
where
    F: FnOnce(ngnet_h3::http::SendRequest<Payload>) -> Fut,
    Fut: core::future::Future<Output = T>,
{
    // Held for the whole of this function so its endpoints -- and the UDP sockets they own
    // -- are released when it returns rather than leaked.
    let pair = ngnet_h3_tests::connected_pair(tuning)
        .await
        .expect("a connected pair");

    let (handle, client_driver) =
        handshake::<_, Payload>(QuinnBackend::new(pair.client.clone())).expect("handshake");
    let server_driver = serve(QuinnBackend::new(pair.server.clone()), handler).expect("serve");

    let client = tokio::task::spawn_local(async move {
        let _ = client_driver.await;
    });
    let server = tokio::task::spawn_local(async move {
        let _ = server_driver.await;
    });

    let outcome = tokio::time::timeout(LIMIT, body(handle))
        .await
        .expect("the exchange should not take this long");

    client.abort();
    server.abort();
    drop(pair);
    outcome
}

async fn over_recorded_quic<F, Fut, T>(tuning: Tuning, handler: Responder, body: F) -> T
where
    F: FnOnce(
        ngnet_h3::http::SendRequest<Payload>,
        Arc<Mutex<EventLog>>,
        Arc<Mutex<EventLog>>,
    ) -> Fut,
    Fut: core::future::Future<Output = T>,
{
    let pair = ngnet_h3_tests::connected_pair(tuning)
        .await
        .expect("a connected pair");

    let (client_backend, client_log) = RecordedQuinn::new(QuinnBackend::new(pair.client.clone()));
    let (server_backend, server_log) = RecordedQuinn::new(QuinnBackend::new(pair.server.clone()));
    let (handle, client_driver) = handshake::<_, Payload>(client_backend).expect("handshake");
    let server_driver = serve(server_backend, handler).expect("serve");

    let client = tokio::task::spawn_local(async move {
        let _ = client_driver.await;
    });
    let server = tokio::task::spawn_local(async move {
        let _ = server_driver.await;
    });

    let outcome = tokio::time::timeout(LIMIT, body(handle, client_log, server_log))
        .await
        .expect("the recorded exchange should not take this long");

    client.abort();
    server.abort();
    drop(pair);
    outcome
}

type Responder = Box<dyn FnMut(http::Request<IncomingBody>) -> Answer>;

/// A handler that echoes the request body back with a 200.
fn echo() -> Responder {
    Box::new(|request: http::Request<IncomingBody>| {
        Box::pin(async move {
            let (body, trailers) = read_body(request.into_body()).await;
            let response = match trailers {
                Some(map) => http::Response::builder()
                    .status(200)
                    .body(with_trailers(Bytes::from(body), map)),
                None => http::Response::builder()
                    .status(200)
                    .body(once(Bytes::from(body))),
            };
            response.expect("a response")
        }) as Answer
    })
}

/// Runs a future on a local set, since the backend is not `Send`.
fn run<F: core::future::Future>(future: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, future)
}

#[test]
fn a_request_and_response_cross_a_real_quic_connection() {
    run(over_quic(Tuning::roomy(), echo(), |handle| async move {
        let response = handle
            .send_request(
                http::Request::builder()
                    .uri("https://localhost/")
                    .body(empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(response.status(), 200);
    }));
}

#[test]
fn a_body_survives_a_real_round_trip_unchanged() {
    let payload = patterned(64 * 1024);
    let expected = payload.clone();
    run(over_quic(
        Tuning::roomy(),
        echo(),
        move |handle| async move {
            let response = handle
                .send_request(
                    http::Request::builder()
                        .method("POST")
                        .uri("https://localhost/")
                        .body(once(Bytes::from(payload)))
                        .expect("a request"),
                )
                .await
                .expect("a response");
            let (body, _) = read_body(response.into_body()).await;
            assert_eq!(body, expected, "the body was corrupted in flight");
        },
    ));
}

#[test]
fn a_megabyte_body_survives_a_real_round_trip() {
    // Large enough to span many packets, exhaust the initial flow-control window several
    // times over, and exercise the block-and-unblock path against a real congestion
    // controller rather than a cap a test chose.
    let payload = patterned(1024 * 1024);
    let expected = payload.clone();
    run(over_quic(
        Tuning::roomy(),
        echo(),
        move |handle| async move {
            let response = handle
                .send_request(
                    http::Request::builder()
                        .method("POST")
                        .uri("https://localhost/")
                        .body(once(Bytes::from(payload)))
                        .expect("a request"),
                )
                .await
                .expect("a response");
            let (body, _) = read_body(response.into_body()).await;
            assert_eq!(body.len(), expected.len());
            assert_eq!(body, expected);
        },
    ));
}

#[test]
fn trailers_cross_a_real_connection_in_both_directions() {
    let mut sent = http::HeaderMap::new();
    sent.insert("x-checksum", "deadbeef".parse().expect("a value"));

    run(over_quic(
        Tuning::roomy(),
        echo(),
        move |handle| async move {
            let response = handle
                .send_request(
                    http::Request::builder()
                        .method("POST")
                        .uri("https://localhost/")
                        .body(with_trailers(Bytes::from_static(b"body"), sent))
                        .expect("a request"),
                )
                .await
                .expect("a response");
            let (body, trailers) = read_body(response.into_body()).await;
            assert_eq!(&body[..], b"body");
            let trailers = trailers.expect("the response carried trailers");
            assert_eq!(trailers.get("x-checksum").expect("the trailer"), "deadbeef");
        },
    ));
}

#[test]
fn concurrent_requests_keep_their_bodies_apart_over_real_quic() {
    run(over_quic(Tuning::roomy(), echo(), |handle| async move {
        let mut futures = Vec::new();
        for i in 0..16u8 {
            let payload = vec![i; 4096];
            futures.push(
                handle.send_request(
                    http::Request::builder()
                        .method("POST")
                        .uri(format!("https://localhost/{i}"))
                        .body(once(Bytes::from(payload)))
                        .expect("a request"),
                ),
            );
        }

        for (i, future) in futures.into_iter().enumerate() {
            let response = future.await.expect("a response");
            let (body, _) = read_body(response.into_body()).await;
            assert_eq!(
                body,
                vec![i as u8; 4096],
                "response {i} carried another request's body"
            );
        }
    }));
}

#[test]
fn a_cramped_transport_produces_identical_results() {
    // Narrow windows force short writes and repeated blocking, which is where the
    // unblock path lives. The answer must be the same as with room to spare.
    let tuning = Tuning::cramped();
    let payload = patterned(96 * 1024);
    let expected = payload.clone();
    run(over_quic(tuning, echo(), move |handle| async move {
        let response = handle
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://localhost/")
                    .body(once(Bytes::from(payload)))
                    .expect("a request"),
            )
            .await
            .expect("a response");
        let (body, _) = read_body(response.into_body()).await;
        assert_eq!(body, expected, "a cramped transport changed the answer");
    }));
}

#[test]
fn an_empty_body_still_ends_the_stream_over_real_quic() {
    let responder: Responder = Box::new(|_request| {
        Box::pin(async move {
            http::Response::builder()
                .status(204)
                .body(empty())
                .expect("a response")
        }) as Answer
    });

    run(over_quic(Tuning::roomy(), responder, |handle| async move {
        let response = handle
            .send_request(
                http::Request::builder()
                    .uri("https://localhost/")
                    .body(empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(response.status(), 204);
        let (body, _) = read_body(response.into_body()).await;
        assert!(body.is_empty());
    }));
}

#[test]
fn the_https_scheme_is_carried_over_real_quic() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let recorder = Arc::clone(&seen);
    let responder: Responder = Box::new(move |request: http::Request<IncomingBody>| {
        *recorder.lock().expect("recorder") = request.uri().scheme_str().map(str::to_string);
        Box::pin(async move {
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as Answer
    });

    run(over_quic(Tuning::roomy(), responder, |handle| async move {
        let response = handle
            .send_request(
                http::Request::builder()
                    .uri("https://localhost/secure")
                    .body(empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(response.status(), 200);
    }));

    assert_eq!(
        seen.lock().expect("recorder").as_deref(),
        Some("https"),
        "the scheme did not survive the round trip"
    );
}

#[test]
fn a_handler_carries_a_cancellation_signal_over_real_quic() {
    let responder: Responder = Box::new(|request: http::Request<IncomingBody>| {
        let has_signal = request.extensions().get::<Cancelled>().is_some();
        Box::pin(async move {
            http::Response::builder()
                .status(if has_signal { 200 } else { 500 })
                .body(empty())
                .expect("a response")
        }) as Answer
    });

    run(over_quic(Tuning::roomy(), responder, |handle| async move {
        let response = handle
            .send_request(
                http::Request::builder()
                    .uri("https://localhost/")
                    .body(empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(
            response.status(),
            200,
            "the request reached the handler without a cancellation signal"
        );
    }));
}

#[test]
fn a_connection_outlives_the_endpoint_it_was_created_from() {
    // Pins the claim `ConnectedPair` makes about itself. An earlier version of the harness
    // leaked both endpoints to keep them alive, justified by the belief that a connection
    // dies with its endpoint. That belief was wrong -- quinn's endpoint driver shuts down
    // only once its reference count reaches zero *and* no connections remain -- and the leak
    // was buying nothing.
    //
    // Stated as a test because it is a claim about someone else's crate, which is exactly
    // the kind that rots silently on an upgrade.
    run(async {
        let pair = ngnet_h3_tests::connected_pair(Tuning::roomy())
            .await
            .expect("a connected pair");

        let client_quic = pair.client.clone();
        let server_quic = pair.server.clone();
        drop(pair.endpoints);

        let (handle, client_driver) =
            handshake::<_, Payload>(QuinnBackend::new(client_quic)).expect("handshake");
        let server_driver = serve(QuinnBackend::new(server_quic), echo()).expect("serve");

        let client = tokio::task::spawn_local(async move {
            let _ = client_driver.await;
        });
        let server = tokio::task::spawn_local(async move {
            let _ = server_driver.await;
        });

        let response = tokio::time::timeout(
            LIMIT,
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://localhost/")
                    .body(once(Bytes::from_static(b"still here")))
                    .expect("a request"),
            ),
        )
        .await
        .expect("the exchange should not take this long")
        .expect("a response");

        assert_eq!(response.status(), 200);
        let (body, _) = read_body(response.into_body()).await;
        assert_eq!(&body[..], b"still here");

        client.abort();
        server.abort();
    });
}

#[test]
fn successful_streams_close_once_after_fin_without_retaining_history() {
    const STREAMS: usize = 1_000;

    run(over_recorded_quic(
        Tuning::roomy(),
        echo(),
        |handle, client_log, server_log| async move {
            for _ in 0..STREAMS {
                let response = handle
                    .send_request(
                        http::Request::builder()
                            .uri("https://localhost/")
                            .body(empty())
                            .expect("a request"),
                    )
                    .await
                    .expect("a response");
                let (body, trailers) = read_body(response.into_body()).await;
                assert!(body.is_empty());
                assert!(trailers.is_none());
            }

            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let client_closed = client_log.lock().expect("client log").closes.len();
                    let server_closed = server_log.lock().expect("server log").closes.len();
                    if client_closed == STREAMS && server_closed == STREAMS {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all successful streams should close");

            for (side, log) in [("client", client_log), ("server", server_log)] {
                let log = log.lock().expect("event log");
                assert_eq!(log.closes.len(), STREAMS, "{side} closure count");
                assert_eq!(log.fins.len(), STREAMS, "{side} FIN count");
                for (stream, (close_pending, rx_code, tx_code, count)) in &log.closes {
                    assert_eq!(*count, 1, "{side} closed stream {stream} more than once");
                    assert_eq!(*rx_code, None, "{side} receive direction was not clean");
                    assert_eq!(*tx_code, None, "{side} send direction was not clean");
                    let fin_pending = log
                        .fins
                        .get(stream)
                        .unwrap_or_else(|| panic!("{side} closed stream {stream} before FIN"));
                    assert!(
                        close_pending > fin_pending,
                        "{side} closed stream {stream} without a Pending batch boundary"
                    );
                }
            }
        },
    ));
}

#[test]
fn dropping_a_request_stops_both_quinn_directions_and_closes_once() {
    let started = Arc::new(tokio::sync::Notify::new());
    let handler_started = Arc::clone(&started);
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handler_cancelled = Arc::clone(&cancelled);
    let responder: Responder = Box::new(move |request: http::Request<IncomingBody>| {
        let signal = request
            .extensions()
            .get::<Cancelled>()
            .cloned()
            .expect("a cancellation signal");
        handler_started.notify_one();
        let cancelled = Arc::clone(&handler_cancelled);
        Box::pin(async move {
            signal.cancelled().await;
            cancelled.store(true, std::sync::atomic::Ordering::Release);
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as Answer
    });

    run(over_recorded_quic(
        Tuning::roomy(),
        responder,
        move |handle, client_log, server_log| async move {
            let request = handle.send_request(
                http::Request::builder()
                    .uri("https://localhost/cancel")
                    .body(empty())
                    .expect("a request"),
            );
            let mut request = Box::pin(request);
            tokio::select! {
                _ = started.notified() => {}
                result = &mut request => panic!("response settled before cancellation: {result:?}"),
            }
            drop(request);

            let settled = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let handler_saw_cancel = cancelled.load(std::sync::atomic::Ordering::Acquire);
                    let client_closed = client_log.lock().expect("client log").closes.len();
                    let server_closed = server_log.lock().expect("server log").closes.len();
                    if handler_saw_cancel && client_closed == 1 && server_closed == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if settled.is_err() {
                let client = client_log.lock().expect("client log");
                let server = server_log.lock().expect("server log");
                panic!(
                    "cancel did not settle: handler={}, client closes={}, resets={}, stops={}; \
                     server closes={}, peer resets={}",
                    cancelled.load(std::sync::atomic::Ordering::Acquire),
                    client.closes.len(),
                    client.local_resets.len(),
                    client.local_stops.len(),
                    server.closes.len(),
                    server.peer_resets.len(),
                );
            }

            let cancelled_code = ErrorCode::new(0x10c);
            let client = client_log.lock().expect("client log");
            assert_eq!(client.local_resets.len(), 1);
            assert_eq!(client.local_stops.len(), 1);
            assert_eq!(client.local_resets[0].1, cancelled_code);
            assert_eq!(client.local_stops[0].1, cancelled_code);
            let (_, client_rx, client_tx, client_count) =
                client.closes.values().next().expect("a client close");
            assert_eq!(
                (*client_rx, *client_tx),
                (Some(cancelled_code), None),
                "the already-finished request send direction must stay clean"
            );
            assert_eq!(*client_count, 1);
            drop(client);

            let server = server_log.lock().expect("server log");
            assert!(
                server.peer_resets.is_empty(),
                "an already-finished request must not acquire a peer reset"
            );
            let (_, server_rx, server_tx, server_count) =
                server.closes.values().next().expect("a server close");
            assert_eq!(
                (*server_rx, *server_tx),
                (None, Some(cancelled_code)),
                "the server read the request cleanly before its send was stopped"
            );
            assert_eq!(*server_count, 1);
        },
    ));
}
