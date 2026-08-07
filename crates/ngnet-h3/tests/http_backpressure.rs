#![cfg(feature = "http")]
//! Boundedness and fairness, which the driver asserts about itself and nothing else checks.
//!
//! The read side of the backend trait is one connection-level event stream, and that shape
//! has an obvious failure mode: a stream producing data faster than the driver consumes it
//! keeps the source perpetually ready, starving writes, handlers, releases and resets. Two
//! things stop it, and both are testable.
//!
//! The transport must not read beyond the credit the layer has extended — so the memory
//! bound stays in QUIC's flow control rather than moving into the process. And the driver
//! takes a bounded number of events per pass and applies control-plane news ahead of bulk
//! data — so a reset behind a megabyte of body bytes is acted on now rather than after the
//! megabyte has been parsed.

use ngnet_h3::http::testing::{ScriptedSource, block_on, held_bytes, loopback};
use ngnet_h3::http::{Config, QuicConnection, QuicEvent, StreamSource, handshake};
use ngnet_h3::{ErrorCode, StreamId};

mod support;
use ngnet_h3::http::testing::bytes_crate::Bytes;
use support::{Payload, Server, empty, once};

fn next_event(endpoint: &mut impl QuicConnection) -> QuicEvent {
    block_on(core::future::poll_fn(|cx| endpoint.poll_event(cx)))
        .unwrap_or_else(|_| panic!("the loopback should not fail here"))
}

fn transmit(endpoint: &mut impl QuicConnection, source: &mut impl StreamSource) {
    let outcome = block_on(core::future::poll_fn(|cx| {
        endpoint.poll_transmit(cx, source)
    }));
    assert!(outcome.is_ok());
}

fn open_uni(endpoint: &mut impl QuicConnection) -> StreamId {
    block_on(core::future::poll_fn(|cx| endpoint.poll_open_uni(cx)))
        .unwrap_or_else(|_| panic!("opening a stream"))
}

#[test]
fn a_hot_stream_cannot_make_the_transport_buffer_without_bound() {
    // The obligation `extend_credit` exists to express. An endpoint that read whatever
    // arrived would hold it all; one that reads only what it has been credited for stops.
    let (mut writer, mut reader, _knobs) = loopback();
    let stream = open_uni(&mut writer);

    // Far more than the initial allowance, written in one go.
    let mut source = ScriptedSource::new([(stream, vec![0xab; 1024 * 1024], false)]);
    transmit(&mut writer, &mut source);

    let held = held_bytes(&reader);
    assert!(
        held > 0,
        "everything was delivered at once; the transport read past its credit"
    );

    // Reading is what returns credit, so nothing more arrives until it does.
    let mut delivered = 0usize;
    for _ in 0..4 {
        if let QuicEvent::Data { bytes, .. } = next_event(&mut reader) {
            delivered += bytes.len();
        }
    }
    assert!(
        delivered <= 64 * 1024,
        "more was handed over than the initial allowance: {delivered}"
    );
    assert!(
        held_bytes(&reader) > 0,
        "the backlog vanished without credit being extended"
    );
}

#[test]
fn extending_credit_is_what_releases_the_backlog() {
    let (mut writer, mut reader, _knobs) = loopback();
    let stream = open_uni(&mut writer);

    let mut source = ScriptedSource::new([(stream, vec![0xcd; 256 * 1024], false)]);
    transmit(&mut writer, &mut source);

    let before = held_bytes(&reader);
    assert!(before > 0, "nothing was held, so this proves nothing");

    reader
        .extend_credit(Some(stream), before as u64)
        .expect("extending credit");

    assert_eq!(
        held_bytes(&reader),
        0,
        "credit was extended and the backlog was not released"
    );
}

#[test]
fn a_reset_is_applied_ahead_of_a_backlog_of_body_bytes() {
    // Fairness on the read side. A driver that drained data before control-plane news would
    // learn about an abandoned exchange only after parsing everything the peer had already
    // sent on it — which is exactly when it matters least.
    let (mut writer, mut reader, _knobs) = loopback();
    let stream = open_uni(&mut writer);

    let mut source = ScriptedSource::new([(stream, vec![0xab; 512 * 1024], false)]);
    transmit(&mut writer, &mut source);
    writer.reset(stream, ErrorCode::new(0x10c)).expect("reset");

    assert!(
        held_bytes(&reader) > 0,
        "this test needs a backlog for it to mean anything"
    );

    let mut data_seen = 0usize;
    loop {
        match next_event(&mut reader) {
            QuicEvent::Data { bytes, .. } => {
                data_seen += bytes.len();
                assert!(
                    data_seen <= 64 * 1024,
                    "the reset waited behind more than the credited backlog"
                );
            }
            QuicEvent::Reset { .. } => break,
            other => panic!("expected data or the reset, got {other:?}"),
        }
    }
}

#[test]
fn a_release_is_not_stuck_behind_a_backlog_either() {
    // Releases free memory. Queueing them behind body data the layer has not been credited
    // for would hold retained buffers for no reason at all.
    let (mut writer, mut reader, _knobs) = loopback();
    let stream = open_uni(&mut writer);

    // Fill the reader's allowance so subsequent data is held.
    let mut flood = ScriptedSource::new([(stream, vec![0xab; 512 * 1024], false)]);
    transmit(&mut writer, &mut flood);
    assert!(held_bytes(&reader) > 0);

    // The *writer* is the one that gets releases, so send from the other side and look at
    // the writer's own event stream.
    let back = open_uni(&mut reader);
    let mut answer = ScriptedSource::new([(back, b"small".to_vec(), false)]);
    transmit(&mut reader, &mut answer);

    // The claim is not that a release jumps ahead of data the layer has already been
    // credited for — that data is legitimately queued. It is that a release does not wait
    // behind data being *held* for want of credit, which could be held indefinitely.
    let mut saw_release = false;
    for _ in 0..16 {
        if let QuicEvent::Released { .. } = next_event(&mut reader) {
            saw_release = true;
            break;
        }
    }
    assert!(saw_release, "the release never arrived");
    assert!(
        held_bytes(&reader) > 0,
        "the backlog drained, so the release was not overtaking anything"
    );
}

#[test]
fn the_driver_takes_a_bounded_number_of_events_per_pass() {
    // The configured bound is what stops one stream's data monopolising a pass. Exercised
    // through a real exchange rather than by inspection: with a small budget the exchange
    // must still complete, which it cannot do if the driver never gets past reading.
    let (client_side, server_side, _knobs) = support::pair();
    let config = Config::default().events_per_pass(1);
    let (handle, driver) =
        ngnet_h3::http::handshake_with::<_, Payload>(client_side, config).expect("handshake");
    let mut server = Server::new(server_side);

    let response = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(once(Bytes::from(vec![0x11; 128 * 1024])))
                .expect("a request"),
        )
    });

    assert!(
        response.is_ok(),
        "an exchange stalled when the driver took one event per pass"
    );
}

#[test]
fn a_stalled_stream_does_not_prevent_another_completing() {
    // Write-side fairness. nghttp3 offers the highest-priority writable stream and keeps
    // offering it, so a stream the transport will not accept must be taken out of the
    // running or it starves every other one.
    let (client_side, server_side, knobs) = support::pair();
    let (handle, driver) = handshake::<_, Payload>(client_side).expect("handshake");
    let mut server = Server::new(server_side);
    server.echo_path_in_body();

    // Stream 0 is the first request; stall it and let the second through.
    knobs.stall(StreamId::new(0).expect("a stream"));

    let stalled = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/stalled")
            .body(empty())
            .expect("a request"),
    );
    let healthy = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/healthy")
            .body(empty())
            .expect("a request"),
    );

    let mut pump = support::Pump::new(driver, server);
    let mut stalled = Box::pin(stalled);
    let mut healthy = Box::pin(healthy);

    let answer = pump
        .rounds(400, &mut healthy)
        .expect("the healthy request should not wait for the stalled one");
    assert!(answer.is_ok());

    // And once the stall is lifted, the other one finishes too.
    knobs.unstall(StreamId::new(0).expect("a stream"));
    let recovered = pump
        .rounds(400, &mut stalled)
        .expect("the stalled request should finish once the transport accepts it");
    assert!(recovered.is_ok());
}
