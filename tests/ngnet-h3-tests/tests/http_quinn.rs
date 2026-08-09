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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use ngnet_h3::http::{Cancelled, IncomingBody, handshake, serve};
use ngnet_h3_tests::Tuning;
use ngnet_h3_tests::quic_backend::QuinnBackend;

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
