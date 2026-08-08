#![cfg(feature = "http")]
//! The in-memory QUIC backend must behave like a QUIC connection.
//!
//! Everything in later suites is measured against this double, so a defect here would show
//! up as a mysterious failure somewhere else. These tests pin the double itself: stream
//! numbering, delivery, short writes, the release contract and the boundedness obligation.
//!
//! They are also the first evidence for the claim that the backend trait is not shaped
//! around one QUIC library. Nothing here derives from quinn; if the trait had a quinn-shaped
//! assumption in it, writing this would have been awkward.

use std::io::IoSlice;

use ngnet_h3::http::testing::{ScriptedSource, block_on, held_bytes, loopback};
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_h3::{Directionality, ErrorCode, Initiator, StreamId};

/// Polls a backend for one event, driving it to completion.
fn next_event(endpoint: &mut impl QuicConnection) -> QuicEvent {
    block_on(core::future::poll_fn(|cx| endpoint.poll_event(cx)))
        .unwrap_or_else(|_| panic!("the loopback should not fail here"))
}

/// Drains one transmit pass.
fn transmit(endpoint: &mut impl QuicConnection, source: &mut impl StreamSource) {
    let outcome = block_on(core::future::poll_fn(|cx| {
        endpoint.poll_transmit(cx, source)
    }));
    assert!(outcome.is_ok(), "the loopback should not fail here");
}

fn open_uni(endpoint: &mut impl QuicConnection) -> StreamId {
    block_on(core::future::poll_fn(|cx| endpoint.poll_open_uni(cx)))
        .unwrap_or_else(|_| panic!("opening a unidirectional stream"))
}

fn open_bi(endpoint: &mut impl QuicConnection) -> StreamId {
    block_on(core::future::poll_fn(|cx| endpoint.poll_open_bi(cx)))
        .unwrap_or_else(|_| panic!("opening a bidirectional stream"))
}

#[test]
fn opened_streams_are_numbered_the_way_http3_requires() {
    let (mut client, mut server, _knobs) = loopback();

    // Client-initiated unidirectional streams are 2, 6, 10 — the three HTTP/3 needs for its
    // control stream and the two QPACK streams.
    assert_eq!(open_uni(&mut client).get(), 2);
    assert_eq!(open_uni(&mut client).get(), 6);
    assert_eq!(open_uni(&mut client).get(), 10);

    // Client-initiated bidirectional streams are 0, 4, 8 — one per request.
    assert_eq!(open_bi(&mut client).get(), 0);
    assert_eq!(open_bi(&mut client).get(), 4);

    // The server's own streams never collide with the client's.
    assert_eq!(open_uni(&mut server).get(), 3);
    assert_eq!(open_bi(&mut server).get(), 1);
}

#[test]
fn stream_identifiers_carry_their_direction_and_initiator() {
    let (mut client, _server, _knobs) = loopback();

    let uni = open_uni(&mut client);
    assert_eq!(uni.directionality(), Directionality::Unidirectional);
    assert_eq!(uni.initiator(), Initiator::Client);

    let bi = open_bi(&mut client);
    assert_eq!(bi.directionality(), Directionality::Bidirectional);
    assert_eq!(bi.initiator(), Initiator::Client);
}

#[test]
fn bytes_written_on_one_side_arrive_on_the_other() {
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Data {
            stream: got,
            bytes,
            fin,
        } => {
            assert_eq!(got, stream);
            assert_eq!(&bytes[..], b"hello");
            assert!(!fin, "nothing said this was the end");
        }
        other => panic!("expected data, got {other:?}"),
    }
}

#[test]
fn the_end_of_a_stream_is_carried_with_its_last_bytes() {
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, b"done".to_vec(), true)]);
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Data { fin, .. } => assert!(fin, "the offer set fin and all of it was taken"),
        other => panic!("expected data, got {other:?}"),
    }
}

#[test]
fn a_stream_can_end_without_carrying_a_final_byte() {
    // The case that makes a separate `finish` method unnecessary, and the one a transport
    // is most likely to get wrong: an offer with `fin` and nothing in it is still progress,
    // and refusing it leaves the peer waiting for an end it was never told about.
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, Vec::new(), true)]);
    transmit(&mut client, &mut source);

    assert_eq!(
        source.outcomes,
        vec![WriteOutcome::Accepted(0)],
        "an empty final offer is accepted, not blocked"
    );
    match next_event(&mut server) {
        QuicEvent::Data { bytes, fin, .. } => {
            assert!(bytes.is_empty());
            assert!(fin);
        }
        other => panic!("expected an empty final data event, got {other:?}"),
    }
}

#[test]
fn a_capped_transport_takes_only_part_of_an_offer() {
    let (mut client, mut server, knobs) = loopback();
    knobs.accept_at_most(3);
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), true)]);
    transmit(&mut client, &mut source);

    assert_eq!(source.outcomes, vec![WriteOutcome::Accepted(3)]);
    match next_event(&mut server) {
        QuicEvent::Data { bytes, fin, .. } => {
            assert_eq!(&bytes[..], b"hel");
            assert!(
                !fin,
                "the end must not be signalled while bytes before it are still unsent"
            );
        }
        other => panic!("expected partial data, got {other:?}"),
    }
}

#[test]
fn a_stalled_stream_is_blocked_rather_than_failed() {
    let (mut client, _server, knobs) = loopback();
    let stream = open_uni(&mut client);
    knobs.stall(stream);

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    transmit(&mut client, &mut source);

    assert_eq!(
        source.outcomes,
        vec![WriteOutcome::Blocked],
        "congestion is not an error; the bytes are still owed"
    );
}

#[test]
fn writing_reports_the_bytes_back_as_released() {
    // This endpoint declares `RETAINS_BUFFERS = true`, so it owes an explicit release for
    // everything it takes. Without that report the layer would hold every body buffer it
    // ever sent until the connection was dropped.
    let (mut client, _server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    transmit(&mut client, &mut source);

    match next_event(&mut client) {
        QuicEvent::Released {
            stream: got,
            bytes,
            delivered,
        } => {
            assert_eq!(got, stream);
            assert_eq!(bytes, 5);
            assert!(delivered);
        }
        other => panic!("expected a release, got {other:?}"),
    }
}

#[test]
fn withheld_release_is_reported_only_once_it_is_allowed() {
    // The sharpest tool the double has: it is what lets a later suite prove *when* a buffer
    // is freed rather than merely that it eventually is.
    let (mut client, _server, knobs) = loopback();
    let stream = open_uni(&mut client);
    knobs.withhold_release();

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    transmit(&mut client, &mut source);

    // Nothing has been released, so the only thing waiting is nothing at all.
    let mut polled = false;
    let pending = core::future::poll_fn(|cx| {
        polled = true;
        match client.poll_event(cx) {
            core::task::Poll::Ready(event) => core::task::Poll::Ready(Some(event)),
            core::task::Poll::Pending => core::task::Poll::Ready(None),
        }
    });
    assert!(
        block_on(pending).is_none(),
        "release was reported despite being withheld"
    );

    knobs.release_everything();
    let mut idle = ScriptedSource::new([]);
    transmit(&mut client, &mut idle);

    match next_event(&mut client) {
        QuicEvent::Released { bytes, .. } => assert_eq!(bytes, 5),
        other => panic!("expected the withheld release, got {other:?}"),
    }
}

#[test]
fn an_undelivered_release_is_distinguishable_from_an_acknowledged_one() {
    // msquic reports exactly this: the buffer is the application's again, but the data was
    // cancelled. Telling the state machine those bytes arrived would be a protocol lie.
    let (mut client, _server, knobs) = loopback();
    let stream = open_uni(&mut client);
    knobs.report_undelivered();

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    transmit(&mut client, &mut source);

    match next_event(&mut client) {
        QuicEvent::Released { delivered, .. } => assert!(!delivered),
        other => panic!("expected a release, got {other:?}"),
    }
}

#[test]
fn a_peer_opened_bidirectional_stream_is_announced_before_its_bytes() {
    // The peer has to be able to route an answer, which means it needs the stream before it
    // needs the request on it. Getting this order wrong resets the stream under several real
    // QUIC libraries.
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_bi(&mut client);

    let mut source = ScriptedSource::new([(stream, b"request".to_vec(), true)]);
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Accepted { stream: got } => assert_eq!(got, stream),
        other => panic!("expected the stream to be announced first, got {other:?}"),
    }
    match next_event(&mut server) {
        QuicEvent::Data { bytes, .. } => assert_eq!(&bytes[..], b"request"),
        other => panic!("expected the request bytes, got {other:?}"),
    }
}

#[test]
fn a_unidirectional_stream_gets_no_announcement_of_its_own() {
    // Deliberate: nghttp3 reads the HTTP/3 stream-type prefix itself, so a peer-opened
    // unidirectional stream needs no event beyond its bytes.
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    let mut source = ScriptedSource::new([(stream, b"\x00".to_vec(), false)]);
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Data { .. } => {}
        other => panic!("expected data with no announcement, got {other:?}"),
    }
}

#[test]
fn resetting_and_stopping_reach_the_peer() {
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_bi(&mut client);

    client
        .reset(stream, ErrorCode::new(0x10c))
        .expect("resetting");
    match next_event(&mut server) {
        QuicEvent::Reset { stream: got, code } => {
            assert_eq!(got, stream);
            assert_eq!(code.get(), 0x10c);
        }
        other => panic!("expected a reset, got {other:?}"),
    }

    client
        .stop_sending(stream, ErrorCode::new(0x10c))
        .expect("stopping");
    match next_event(&mut server) {
        QuicEvent::StopSending { stream: got, code } => {
            assert_eq!(got, stream);
            assert_eq!(code.get(), 0x10c);
        }
        other => panic!("expected a stop-sending, got {other:?}"),
    }
}

#[test]
fn closing_carries_the_application_error_code_to_the_peer() {
    // HTTP/3 closes with codes of its own — `H3_NO_ERROR` for an orderly finish — and a
    // transport that dropped them would make every connection failure look the same.
    let (mut client, mut server, _knobs) = loopback();

    client
        .close(ErrorCode::new(0x100), b"done")
        .expect("closing");

    match next_event(&mut server) {
        QuicEvent::Closed { code } => assert_eq!(code.map(|c| c.get()), Some(0x100)),
        other => panic!("expected a close, got {other:?}"),
    }
}

#[test]
fn delivery_stops_when_credit_runs_out_and_resumes_when_it_is_extended() {
    // The boundedness obligation, which is the whole reason `extend_credit` exists as a
    // method rather than as a side effect of reading. An endpoint that never extends credit
    // must stop receiving, or a fast peer moves the memory bound out of QUIC and into the
    // process.
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    // The initial allowance is 64 KiB; write half again as much.
    let payload = vec![0xab; 96 * 1024];
    let mut source = ScriptedSource::new([(stream, payload, false)]);
    transmit(&mut client, &mut source);

    // Exactly the allowance is delivered, and the rest waits.
    let mut delivered = 0usize;
    while delivered < 64 * 1024 {
        match next_event(&mut server) {
            QuicEvent::Data { bytes, .. } => delivered += bytes.len(),
            other => panic!("expected data, got {other:?}"),
        }
    }
    assert_eq!(delivered, 64 * 1024, "more than the credit was handed over");
    assert_eq!(
        held_bytes(&server),
        32 * 1024,
        "the rest should be held, not delivered and not dropped"
    );

    server
        .extend_credit(Some(stream), 32 * 1024)
        .expect("extending credit");
    assert_eq!(
        held_bytes(&server),
        0,
        "extending credit did not release what was held"
    );

    while delivered < 96 * 1024 {
        match next_event(&mut server) {
            QuicEvent::Data { bytes, .. } => delivered += bytes.len(),
            other => panic!("expected the remaining data, got {other:?}"),
        }
    }
    assert_eq!(delivered, 96 * 1024, "bytes were lost while held");
}

#[test]
fn a_control_event_is_not_stuck_behind_held_data() {
    // Fairness: a reset must reach the layer even when a stream is flooding it. Holding
    // data for want of credit must not hold anything else, or a driver drowning in body
    // bytes would learn about a reset only after it finished reading them.
    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);

    // Enough to exhaust the allowance and leave some held.
    let mut source = ScriptedSource::new([(stream, vec![0xab; 96 * 1024], false)]);
    transmit(&mut client, &mut source);
    client.reset(stream, ErrorCode::new(0x10c)).expect("reset");

    assert!(
        held_bytes(&server) > 0,
        "this test needs data to be held for it to mean anything"
    );

    // Drain only what credit already allowed, then the reset must be next — ahead of every
    // held byte.
    let mut seen = 0usize;
    loop {
        match next_event(&mut server) {
            QuicEvent::Data { bytes, .. } => {
                seen += bytes.len();
                assert!(seen <= 64 * 1024, "delivery ran past the credit given");
            }
            QuicEvent::Reset { .. } => break,
            other => panic!("expected data or the reset, got {other:?}"),
        }
    }
}

#[test]
fn a_failing_transport_reports_its_failure_rather_than_hanging() {
    let (mut client, _server, knobs) = loopback();
    let stream = open_uni(&mut client);
    knobs.fail_writes_after(0);

    let mut source = ScriptedSource::new([(stream, b"hello".to_vec(), false)]);
    let outcome = block_on(core::future::poll_fn(|cx| {
        client.poll_transmit(cx, &mut source)
    }));
    assert!(outcome.is_err(), "the write limit should have failed");
}

#[test]
fn a_source_that_offers_nothing_completes_a_pass() {
    let (mut client, _server, _knobs) = loopback();
    let mut source = ScriptedSource::new([]);
    transmit(&mut client, &mut source);
    assert!(source.is_drained());
}

#[test]
fn the_clock_never_goes_backwards() {
    // nghttp3 rejects a reading lower than the last one, so a transport whose clock stepped
    // back would fail every subsequent read.
    let (client, _server, _knobs) = loopback();
    let first = client.now();
    let second = client.now();
    assert!(second >= first);
}

#[test]
fn a_write_spanning_several_vectors_is_delivered_in_order() {
    // nghttp3 offers up to sixteen vectors at a time and a transport must treat them as one
    // contiguous run, not as independent writes.
    struct Vectored(Option<StreamId>);
    impl StreamSource for Vectored {
        fn write_next(
            &mut self,
            write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
        ) -> bool {
            let Some(stream) = self.0.take() else {
                return false;
            };
            let slices = [
                IoSlice::new(b"one"),
                IoSlice::new(b"two"),
                IoSlice::new(b"three"),
            ];
            assert_eq!(write(stream, &slices, true), WriteOutcome::Accepted(11));
            true
        }
    }

    let (mut client, mut server, _knobs) = loopback();
    let stream = open_uni(&mut client);
    let mut source = Vectored(Some(stream));
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Data { bytes, fin, .. } => {
            assert_eq!(&bytes[..], b"onetwothree");
            assert!(fin);
        }
        other => panic!("expected the whole run, got {other:?}"),
    }
}

#[test]
fn a_capped_write_across_vectors_takes_a_prefix_of_the_run() {
    struct Vectored(Option<StreamId>);
    impl StreamSource for Vectored {
        fn write_next(
            &mut self,
            write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
        ) -> bool {
            let Some(stream) = self.0.take() else {
                return false;
            };
            let slices = [IoSlice::new(b"one"), IoSlice::new(b"two")];
            assert_eq!(write(stream, &slices, false), WriteOutcome::Accepted(4));
            true
        }
    }

    let (mut client, mut server, knobs) = loopback();
    knobs.accept_at_most(4);
    let stream = open_uni(&mut client);
    let mut source = Vectored(Some(stream));
    transmit(&mut client, &mut source);

    match next_event(&mut server) {
        QuicEvent::Data { bytes, .. } => assert_eq!(&bytes[..], b"onet"),
        other => panic!("expected a prefix, got {other:?}"),
    }
}
