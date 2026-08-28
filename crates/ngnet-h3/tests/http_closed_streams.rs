#![cfg(feature = "http")]
//! Closed-stream history behavior at the retention boundary.

use std::error::Error as _;

use ngnet_h3::http::{ErrorKind, QuicEvent, handshake};

mod support;
use support::{Log, Payload, Recorder, empty, request_stream};

const DRIVER_POLLS: usize = 128;

fn pump_driver<F>(driver: &mut std::pin::Pin<Box<F>>) -> Option<F::Output>
where
    F: core::future::Future,
{
    for _ in 0..DRIVER_POLLS {
        if let Some(outcome) = support::poll_now(driver) {
            return Some(outcome);
        }
    }
    None
}

fn closed(stream: ngnet_h3::StreamId) -> QuicEvent {
    QuicEvent::StreamClosed {
        stream,
        rx_code: None,
        tx_code: None,
    }
}

#[test]
fn retained_closed_stream_discards_a_late_release() {
    let log = Log::new();
    let (transport, controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");
    let mut driver = Box::pin(driver);
    assert!(support::poll_now(&mut driver).is_none());

    let stream = request_stream(2_000);
    controls.deliver(closed(stream));
    controls.deliver(QuicEvent::Released {
        stream,
        bytes: 1,
        delivered: true,
    });
    assert!(
        pump_driver(&mut driver).is_none(),
        "a retained tombstone did not discard its late release"
    );
    assert_eq!(
        controls.pending_events(),
        0,
        "the retained-release assertion ran before its events were consumed"
    );

    let _request = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/after")
            .body(empty())
            .expect("a request"),
    );
    assert!(
        pump_driver(&mut driver).is_none(),
        "the connection ended instead of carrying another exchange"
    );
    assert!(
        !log.offered(request_stream(0)).is_empty(),
        "the connection made no progress after the late release"
    );
}

#[test]
fn evicted_closed_stream_uses_the_non_tombstoned_release_path() {
    let (transport, controls) = support::stub();
    let (_handle, driver) = handshake::<_, Payload>(transport).expect("handshake");
    let mut driver = Box::pin(driver);
    assert!(support::poll_now(&mut driver).is_none());

    for index in 0..=1_024 {
        controls.deliver(closed(request_stream(index)));
    }
    let evicted = request_stream(0);
    controls.deliver(QuicEvent::Released {
        stream: evicted,
        bytes: 1,
        delivered: true,
    });

    let outcome = pump_driver(&mut driver).expect("the evicted release should end the connection");
    assert_eq!(
        controls.pending_events(),
        0,
        "the evicted-release assertion ran before its events were consumed"
    );
    let error = outcome.expect_err("the non-tombstoned release unexpectedly succeeded");
    assert_eq!(error.kind(), ErrorKind::Stream);
    assert!(
        error
            .source()
            .is_some_and(|source| source.to_string().contains("nothing outstanding")),
        "unexpected non-tombstoned release error: {error:?}"
    );
}
