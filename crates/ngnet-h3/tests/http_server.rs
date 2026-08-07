#![cfg(feature = "http")]
//! The asynchronous server.
//!
//! Both ends of every exchange here are this crate's own layer: `handshake` on one side and
//! `serve` on the other, over the in-memory backend. That is the first end-to-end use of the
//! whole thing, and it is what the quinn integration will repeat over a real network.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::{Cancelled, Config, IncomingBody, handshake, serve, serve_with};

mod support;
use support::{Payload, empty, once};

/// A handler that answers every request the same way.
fn always(
    status: u16,
    body: &'static [u8],
) -> impl FnMut(http::Request<IncomingBody>) -> BoxedAnswer {
    move |_request| {
        let response = http::Response::builder()
            .status(status)
            .body(once(Bytes::from_static(body)))
            .expect("a response");
        Box::pin(async move { response })
    }
}

type BoxedAnswer = std::pin::Pin<Box<dyn core::future::Future<Output = http::Response<Payload>>>>;

#[test]
fn a_request_reaches_a_handler_and_its_answer_reaches_the_caller() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, always(200, b"answered")).expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    })
    .expect("a response");

    assert_eq!(response.status(), 200);
    let body = support::read_body(response.into_body());
    assert_eq!(&body[..], b"answered");
}

#[test]
fn a_handler_sees_the_request_it_was_sent() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        recorder
            .lock()
            .expect("recorder")
            .push((request.method().clone(), request.uri().path().to_string()));
        let response = http::Response::builder()
            .status(200)
            .body(empty())
            .expect("a response");
        Box::pin(async move { response }) as BoxedAnswer
    })
    .expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/things")
                .body(empty())
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    let seen = seen.lock().expect("recorder");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, http::Method::POST);
    assert_eq!(seen[0].1, "/things");
}

#[test]
fn a_handler_can_read_the_request_body() {
    let echoed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = Arc::clone(&echoed);

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let recorder = Arc::clone(&recorder);
        let body = request.into_body();
        Box::pin(async move {
            let bytes = support::read_body_async(body).await;
            recorder.lock().expect("recorder").extend_from_slice(&bytes);
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from_static(b"the request body")))
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert_eq!(&echoed.lock().expect("recorder")[..], b"the request body");
}

#[test]
fn several_handlers_make_progress_independently() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, |request: http::Request<IncomingBody>| {
        let path = request.uri().path().to_string();
        Box::pin(async move {
            http::Response::builder()
                .status(200)
                .body(once(Bytes::from(path.into_bytes())))
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let futures: Vec<_> = (0..8)
        .map(|i| {
            handle.send_request(
                http::Request::builder()
                    .uri(format!("https://example.test/{i}"))
                    .body(empty())
                    .expect("a request"),
            )
        })
        .collect();

    let answers = support::both_ends_many(client, server, futures);
    for (i, answer) in answers.into_iter().enumerate() {
        let body = support::read_body(answer.expect("a response").into_body());
        assert_eq!(String::from_utf8_lossy(&body), format!("/{i}"));
    }
}

#[test]
fn one_pending_handler_does_not_prevent_the_others_completing() {
    // The point of holding handlers rather than serialising them. If the driver waited on
    // each in turn, one that never finishes would take the connection with it.
    let gate = support::Gate::new();
    let held = gate.clone();

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let stall = request.uri().path() == "/slow";
        let gate = held.clone();
        Box::pin(async move {
            if stall {
                gate.wait().await;
            }
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let slow = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/slow")
            .body(empty())
            .expect("a request"),
    );
    let quick = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/quick")
            .body(empty())
            .expect("a request"),
    );

    let mut pump = support::BothEnds::new(client, server);
    let mut slow = Box::pin(slow);
    let mut quick = Box::pin(quick);

    let quick_answer = pump
        .rounds(200, &mut quick)
        .expect("the quick request should not wait for the slow one");
    assert!(quick_answer.is_ok());
    assert!(
        pump.peek(&mut slow).is_none(),
        "the stalled handler answered without being released"
    );

    gate.open();
    let slow_answer = pump
        .rounds(200, &mut slow)
        .expect("the released handler should answer");
    assert!(slow_answer.is_ok());
}

#[test]
fn a_handler_learns_its_exchange_was_abandoned() {
    let noticed = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&noticed);
    let gate = support::Gate::new();
    let held = gate.clone();

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let cancelled = request
            .extensions()
            .get::<Cancelled>()
            .cloned()
            .expect("every request carries a cancellation signal");
        let counter = Arc::clone(&counter);
        let gate = held.clone();
        Box::pin(async move {
            gate.wait().await;
            if cancelled.is_cancelled() {
                counter.fetch_add(1, Ordering::Release);
            }
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let future = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );

    let mut pump = support::BothEnds::new(client, server);
    let mut future = Box::pin(future);
    pump.rounds(20, &mut future);

    // The caller gives up while the handler is still waiting.
    drop(future);
    let mut nothing = Box::pin(core::future::pending::<support::Answer>());
    pump.rounds(60, &mut nothing);

    gate.open();
    pump.rounds(60, &mut nothing);

    assert_eq!(
        noticed.load(Ordering::Acquire),
        1,
        "the handler was never told its exchange had gone"
    );
}

#[test]
fn a_request_carries_a_cancellation_signal_that_stays_clear_when_nothing_goes_wrong() {
    let cancelled_at_end = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&cancelled_at_end);

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let cancelled = request
            .extensions()
            .get::<Cancelled>()
            .cloned()
            .expect("a cancellation signal");
        let counter = Arc::clone(&counter);
        Box::pin(async move {
            if cancelled.is_cancelled() {
                counter.fetch_add(1, Ordering::Release);
            }
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert_eq!(
        cancelled_at_end.load(Ordering::Acquire),
        0,
        "a healthy exchange reported itself cancelled"
    );
}

#[test]
fn exceeding_the_concurrency_limit_refuses_rather_than_queues() {
    // The limit has to refuse. Queueing instead would make the backlog an allocation the
    // peer controls, which is the same thing as no limit at all.
    let started = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&started);
    let gate = support::Gate::new();
    let held = gate.clone();

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve_with(
        server_side,
        move |_request: http::Request<IncomingBody>| {
            counter.fetch_add(1, Ordering::Release);
            let gate = held.clone();
            Box::pin(async move {
                gate.wait().await;
                http::Response::builder()
                    .status(200)
                    .body(empty())
                    .expect("a response")
            }) as BoxedAnswer
        },
        Config::default().max_concurrent_streams(2),
    )
    .expect("serve");

    let futures: Vec<_> = (0..6)
        .map(|i| {
            handle.send_request(
                http::Request::builder()
                    .uri(format!("https://example.test/{i}"))
                    .body(empty())
                    .expect("a request"),
            )
        })
        .collect();

    let mut pump = support::BothEnds::new(client, server);
    let mut pinned: Vec<_> = futures.into_iter().map(Box::pin).collect();
    for _ in 0..200 {
        for future in &mut pinned {
            let _ = pump.peek(future);
        }
        pump.round();
    }

    assert!(
        started.load(Ordering::Acquire) <= 2,
        "more handlers ran at once than the limit allowed: {}",
        started.load(Ordering::Acquire)
    );
    gate.open();
}

#[test]
fn a_response_with_no_body_still_ends_its_stream() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, always(204, b"")).expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    })
    .expect("a response");

    assert_eq!(response.status(), 204);
    assert!(support::read_body(response.into_body()).is_empty());
}

#[test]
fn a_handler_that_ignores_the_request_body_still_answers() {
    // The asymmetry with a client's response body: dropping an unread *request* body must
    // not abandon the exchange, because the handler still owes an answer.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, |request: http::Request<IncomingBody>| {
        // Dropped, deliberately, without reading a byte.
        drop(request.into_body());
        Box::pin(async move {
            http::Response::builder()
                .status(200)
                .body(once(Bytes::from_static(b"ignored your body")))
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from(vec![0xab; 16 * 1024])))
                .expect("a request"),
        )
    })
    .expect("a response");

    assert_eq!(response.status(), 200);
    assert_eq!(
        &support::read_body(response.into_body())[..],
        b"ignored your body"
    );
}

#[test]
fn a_body_round_trips_between_this_crate_at_both_ends() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, |request: http::Request<IncomingBody>| {
        let body = request.into_body();
        Box::pin(async move {
            let bytes = support::read_body_async(body).await;
            http::Response::builder()
                .status(200)
                .body(once(Bytes::from(bytes)))
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let sent = payload.clone();

    let response = support::both_ends(client, server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from(sent)))
                .expect("a request"),
        )
    })
    .expect("a response");

    assert_eq!(support::read_body(response.into_body()), payload);
}
