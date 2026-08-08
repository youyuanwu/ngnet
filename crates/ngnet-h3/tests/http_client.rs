#![cfg(feature = "http")]
//! The asynchronous client, driven over the in-memory backend.
//!
//! Both ends of every exchange here are this crate: a client driver on one side, and a
//! deliberately minimal hand-driven server on the other, so that a failure points at the
//! client rather than at a second layer of the same code.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ngnet_h3::http::testing::block_on;
use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::testing::http_body_crate::Body;
use ngnet_h3::http::{Config, ErrorKind, handshake, handshake_with};

mod support;
use support::{Server, empty, once};

#[test]
fn a_get_completes_over_the_backend() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    });

    let response = response.expect("a response");
    assert_eq!(response.status(), 200);
}

#[test]
fn the_three_unidirectional_streams_are_bound_without_the_caller_naming_them() {
    // The whole point of the layer: HTTP/3 cannot move a byte until a control stream and two
    // QPACK streams exist and are declared, and no test here mentions one.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert!(
        server.saw_unidirectional_streams() >= 3,
        "the peer never saw the control and QPACK streams"
    );
}

#[test]
fn a_response_body_is_read_to_completion() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.answer_with_body(b"hello world".to_vec());

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    })
    .expect("a response");

    let body = block_on(support::collect(response.into_body())).expect("a body");
    assert_eq!(&body[..], b"hello world");
}

#[test]
fn a_request_body_reaches_the_peer_byte_exact() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let sent = payload.clone();

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from(sent)))
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert_eq!(server.received_body(), payload);
}

#[test]
fn concurrent_requests_are_matched_to_their_own_responses() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.echo_path_in_body();

    let futures: Vec<_> = (0..16)
        .map(|i| {
            handle.send_request(
                http::Request::builder()
                    .uri(format!("https://example.test/{i}"))
                    .body(empty())
                    .expect("a request"),
            )
        })
        .collect();

    let responses = support::exchange_many(driver, &mut server, futures);

    for (i, response) in responses.into_iter().enumerate() {
        let response = response.expect("a response");
        let body = block_on(support::collect(response.into_body())).expect("a body");
        assert_eq!(
            String::from_utf8_lossy(&body),
            format!("/{i}"),
            "response {i} carried another request's body"
        );
    }
}

#[test]
fn a_handle_is_cloneable_and_the_clones_share_one_connection() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.echo_path_in_body();

    let second = handle.clone();
    let futures = vec![
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/one")
                .body(empty())
                .expect("a request"),
        ),
        second.send_request(
            http::Request::builder()
                .uri("https://example.test/two")
                .body(empty())
                .expect("a request"),
        ),
    ];

    let responses = support::exchange_many(driver, &mut server, futures);
    let bodies: Vec<String> = responses
        .into_iter()
        .map(|response| {
            let body = block_on(support::collect(response.expect("a response").into_body()))
                .expect("a body");
            String::from_utf8_lossy(&body).into_owned()
        })
        .collect();
    assert_eq!(bodies, vec!["/one".to_string(), "/two".to_string()]);
}

#[test]
fn a_never_polled_driver_sends_nothing() {
    // The trap the `#[must_use]` exists for, stated as a test rather than only as prose.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let _future = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );

    // The server is driven; the client's driver is not.
    server.pump();
    assert_eq!(
        server.requests_seen(),
        0,
        "a request moved without the driver being polled"
    );
    drop(driver);
}

#[test]
fn dropping_the_driver_fails_every_request_in_flight() {
    let (client_side, _server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");

    let future = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );

    drop(driver);

    let error = block_on(future).expect_err("the driver is gone");
    assert_eq!(error.kind(), ErrorKind::Closed);
    assert!(error.is_closed());
}

#[test]
fn a_request_submitted_after_the_driver_is_gone_fails_immediately() {
    let (client_side, _server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    drop(driver);

    let future = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );
    let error = block_on(future).expect_err("the connection is gone");
    assert_eq!(error.kind(), ErrorKind::Closed);
    assert!(handle.is_closed());
}

#[test]
fn a_request_with_an_invalid_head_fails_that_request_alone() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    // No authority anywhere, which HTTP/3 will not carry.
    let bad = handle.send_request(
        http::Request::builder()
            .uri("/no-authority")
            .body(empty())
            .expect("a request"),
    );
    let good = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );

    let mut responses = support::exchange_many(driver, &mut server, vec![bad, good]);
    let good = responses.pop().expect("two responses");
    let bad = responses.pop().expect("two responses");

    assert_eq!(
        bad.expect_err("a head with no authority").kind(),
        ErrorKind::Protocol
    );
    assert!(
        good.is_ok(),
        "one caller's bad head must not disturb another's exchange"
    );
}

#[test]
fn shutdown_refuses_new_requests_retriably() {
    let (client_side, _server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");

    handle.shutdown();
    assert!(handle.is_refusing());

    let future = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );
    let error = block_on(future).expect_err("refused");
    assert_eq!(error.kind(), ErrorKind::Refused);
    assert!(
        error.is_retriable(),
        "a refused exchange was never looked at, so retrying it cannot duplicate anything"
    );
    drop(driver);
}

#[test]
fn a_configured_connection_still_completes_an_exchange() {
    // The settings reach nghttp3, so a value it rejects would surface here rather than at
    // some later point where the cause is unrecoverable.
    let (client_side, server_side, _knobs) = support::pair();
    let config = Config::default()
        .max_concurrent_streams(4)
        .max_field_section_size(8 * 1024)
        .qpack_max_dtable_capacity(0)
        .qpack_blocked_streams(0)
        .events_per_pass(8);
    let (handle, driver) =
        handshake_with::<_, support::Payload>(client_side, config).expect("handshake");
    let mut server = Server::new(server_side);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    });
    assert!(response.is_ok());
}

#[test]
fn a_body_is_polled_only_while_the_driver_runs() {
    // Bodies are pulled by the state machine from inside the write path, so a body that is
    // polled without the driver running would mean something else is driving it.
    let polls = Arc::new(AtomicUsize::new(0));
    let counting = support::counting(Bytes::from_static(b"payload"), Arc::clone(&polls));

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let future = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/")
            .body(counting)
            .expect("a request"),
    );
    assert_eq!(polls.load(Ordering::Relaxed), 0);

    let response = support::exchange_many(driver, &mut server, vec![future]);
    assert!(response[0].is_ok());
    assert!(polls.load(Ordering::Relaxed) > 0);
}

#[test]
fn an_empty_body_still_ends_the_stream() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.answer_with_status(204);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    })
    .expect("a response");

    assert_eq!(response.status(), 204);
    let mut body = response.into_body();
    assert!(
        Body::is_end_stream(&body)
            || block_on(support::collect(&mut body))
                .expect("a body")
                .is_empty()
    );
}
