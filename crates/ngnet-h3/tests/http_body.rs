#![cfg(feature = "http")]
//! The retain contract, under conditions chosen to break it.
//!
//! nghttp3 does not copy outgoing body data. It borrows the application's buffers and reads
//! through them on every write until the transport reports them released, and `delete_outq`
//! deliberately leaks the alien ones — so release on close and on drop is mandatory, not
//! tidy-up. A use-after-free was already found in this area once, and its cause was an
//! ownership path that looked symmetric and was not.
//!
//! Exactly three things release a retained buffer: acknowledgement, stream close, and
//! dropping the connection. Every test here is about one of those, or about something that
//! must *not* release.
//!
//! The in-memory backend is what makes this measurable. It declares `RETAINS_BUFFERS = true`,
//! so it must report release explicitly, and it can be told to withhold that report — which
//! is the only way to prove *when* a buffer is freed rather than merely that it eventually
//! is. A copying transport cannot exercise this at all, which is why the quinn integration
//! is not where this belongs.
//!
//! Release is observed through the buffer's owner rather than through a counter inside the
//! crate. `Bytes::from_owner` keeps the owner alive for exactly as long as any reference to
//! its bytes exists — including the ones nghttp3 is reading through — so the owner's `Drop`
//! firing *is* the release, with nothing to keep in step.

use ngnet_h3::http::testing::block_on;
use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::{ErrorKind, handshake};

mod support;
use support::{Gate, Probe, Pump, Server, empty, failing, gated, once, tracked, with_trailers};

/// A payload with a pattern that makes a misordered or duplicated run obvious.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[test]
fn a_megabyte_body_arrives_byte_exact() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let payload = patterned(1024 * 1024);
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
    assert_eq!(
        server.received_body().len(),
        payload.len(),
        "the body changed length in flight"
    );
    assert_eq!(server.received_body(), payload, "the body was corrupted");
}

#[test]
fn a_body_larger_than_the_transport_will_take_at_once_still_completes() {
    // The unblock path. nghttp3 unschedules a blocked stream and will not offer it again of
    // its own accord, so without an explicit unblock this stalls — silently, which is why it
    // earns a test rather than being assumed.
    let (client_side, server_side, knobs) = support::pair();
    knobs.accept_at_most(1024);
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let payload = patterned(64 * 1024);
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

    assert!(response.is_ok(), "a capped transport stalled the exchange");
    assert_eq!(server.received_body(), payload);
    assert!(
        knobs.writes() > payload.len() / 1024,
        "the test did not non-vacuously retry the short-written HTTP/3 stream"
    );
}

#[test]
fn a_buffer_is_not_released_while_acknowledgement_is_withheld() {
    // The central claim of the retain contract, as an experiment rather than as prose: with
    // the transport refusing to say a byte is free, nothing is freed. If this fails, a
    // buffer nghttp3 is still reading through has been handed back to the allocator.
    let (client_side, server_side, knobs) = support::pair();
    knobs.withhold_release();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let server = Server::new(server_side);

    let probe = Probe::new();
    let body = tracked(Bytes::from(patterned(32 * 1024)), probe.clone());

    let mut pump = Pump::new(driver, server);
    let mut future = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(body)
                .expect("a request"),
        ),
    );

    pump.rounds(30, &mut future);

    assert!(
        !probe.freed(),
        "a buffer was released while the transport was withholding acknowledgement"
    );
}

#[test]
fn a_buffer_is_released_once_acknowledgement_arrives() {
    // The other half of the pair above. Withholding proves nothing on its own — a layer that
    // never released anything would pass it — so the same setup runs to completion with
    // acknowledgement allowed.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let probe = Probe::new();
    let body = tracked(Bytes::from(patterned(32 * 1024)), probe.clone());

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(body)
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert!(
        probe.freed(),
        "acknowledgement arrived and the buffer was never released"
    );
}

#[test]
fn dropping_the_connection_mid_body_releases_what_it_was_holding() {
    // The third release trigger, and the one that cannot be skipped: `delete_outq` leaks
    // alien buffers deliberately, so a connection that did not release on drop would simply
    // lose the memory.
    let (client_side, server_side, knobs) = support::pair();
    knobs.withhold_release();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let server = Server::new(server_side);

    let probe = Probe::new();
    let body = tracked(Bytes::from(patterned(32 * 1024)), probe.clone());

    let mut pump = Pump::new(driver, server);
    let mut future = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(body)
                .expect("a request"),
        ),
    );
    pump.rounds(30, &mut future);
    assert!(!probe.freed(), "acknowledgement was not actually withheld");

    drop(future);
    drop(pump);
    drop(handle);

    assert!(
        probe.freed(),
        "dropping the connection leaked the buffers nghttp3 was holding"
    );
}

#[test]
fn an_undelivered_release_does_not_fail_the_connection() {
    // msquic reports exactly this: the buffer is the application's again, but the data was
    // cancelled. It frees, and it must not reach the state machine as acknowledgement —
    // that would claim more arrived than ever did, which the offset accounting rejects and
    // which is the shape of an early free.
    let (client_side, server_side, knobs) = support::pair();
    knobs.report_undelivered();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let server = Server::new(server_side);

    let mut pump = Pump::new(driver, server);
    let mut future = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from(patterned(4096))))
                .expect("a request"),
        ),
    );
    pump.rounds(40, &mut future);

    assert!(
        !pump.driver_failed(),
        "an undelivered release was reported as acknowledgement and failed the connection"
    );
}

#[test]
fn a_failing_body_leaves_the_connection_usable_for_other_exchanges() {
    // The state machine's own body-failure signal is connection-fatal. Using it for a
    // caller's error would let one file read going wrong take down every unrelated exchange
    // sharing the connection, so the layer must not use it.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.echo_path_in_body();

    let failing_request = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/fails")
            .body(failing())
            .expect("a request"),
    );
    let healthy = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/fine")
            .body(empty())
            .expect("a request"),
    );

    let mut answers = support::exchange_many(driver, &mut server, vec![failing_request, healthy]);
    let healthy = answers.pop().expect("two answers");
    let failed = answers.pop().expect("two answers");

    assert!(
        healthy.is_ok(),
        "one caller's body failure took down an unrelated exchange"
    );
    if let Err(error) = failed {
        assert_ne!(
            error.kind(),
            ErrorKind::Protocol,
            "a caller's body failure is not a protocol violation"
        );
    }
}

#[test]
fn trailers_arrive_after_the_body() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-checksum", "deadbeef".parse().expect("a value"));

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(with_trailers(Bytes::from_static(b"body"), trailers))
                .expect("a request"),
        )
    });

    assert!(response.is_ok());
    assert_eq!(server.received_body(), b"body");
    assert_eq!(
        server.received_trailer("x-checksum").as_deref(),
        Some("deadbeef"),
        "the trailing field section did not arrive, or arrived as a header"
    );
}

#[test]
fn a_deferred_body_completes_once_it_is_resumed() {
    // Deferral and congestion are different mechanisms and conflating them livelocks:
    // treating a body with nothing to say as a busy transport waits for a transport that is
    // perfectly willing, and the exchange never finishes.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let server = Server::new(server_side);

    let gate = Gate::new();
    let body = gated(Bytes::from_static(b"eventually"), gate.clone());

    let mut pump = Pump::new(driver, server);
    let mut future = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(body)
                .expect("a request"),
        ),
    );

    assert!(
        pump.rounds(30, &mut future).is_none(),
        "the exchange finished while its body was still holding everything back"
    );

    gate.open();
    let answer = pump
        .rounds(400, &mut future)
        .expect("the body was resumed, so the exchange should finish");
    assert!(answer.is_ok());
    assert_eq!(pump.into_server().received_body(), b"eventually");
}

#[test]
fn an_unread_response_body_can_be_dropped_without_panicking() {
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.answer_with_body(patterned(8 * 1024));

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        )
    })
    .expect("a response");

    // Dropped without a byte being read. The unread bytes are credited back and the peer is
    // told to stop; what is asserted here is only that neither path panics.
    drop(response.into_body());
}

#[test]
fn several_bodies_in_flight_keep_their_buffers_apart() {
    // Retained buffers are keyed by stream, and a registry that mixed them up would release
    // one exchange's memory on another's acknowledgement.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.echo_path_in_body();

    let futures: Vec<_> = (0..8)
        .map(|i| {
            let payload = vec![i as u8; 4096];
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri(format!("https://example.test/{i}"))
                    .body(once(Bytes::from(payload)))
                    .expect("a request"),
            )
        })
        .collect();

    let answers = support::exchange_many(driver, &mut server, futures);
    for (i, answer) in answers.into_iter().enumerate() {
        assert!(answer.is_ok(), "exchange {i} failed");
    }
}

#[test]
fn a_body_of_exactly_nothing_still_ends_its_stream() {
    // The empty final write: an offer carrying the end and no bytes. Committing zero for it
    // would tell the state machine the stream ended without the transport ever sending the
    // end, and the peer would wait forever.
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, driver) = handshake::<_, support::Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::new()))
                .expect("a request"),
        )
    });

    assert!(response.is_ok(), "an empty body never ended its stream");
    assert_eq!(server.requests_seen(), 1);
}

#[test]
fn the_layer_needs_no_runtime() {
    // Everything above runs on a parker inside the crate. Stated once, here, because every
    // other test depends on it and none of them says so.
    assert_eq!(block_on(async { 1 + 1 }), 2);
}
