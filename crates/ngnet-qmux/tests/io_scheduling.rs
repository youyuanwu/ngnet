//! Scheduling and backpressure: what the layer does when something cannot proceed.
//!
//! Every test here is about a wait. Three of them are about a wait *ending* -- a blocked open
//! that completes, a credit-exhausted write that resumes, a byte stream that refuses its first
//! attempt and is obeyed rather than failed. Three are about a wait that must not happen: an
//! idle connection that arms nothing, and two floods against a caller that does not keep up.
//!
//! # Why a counting waker, and not a timeout
//!
//! A connection returning [`Poll::Pending`] tells you nothing on its own. The interesting
//! question is what it did on the way: parked against an event, or woke itself and asked to be
//! polled again. The second is a busy loop that passes every functional test in this suite
//! while burning a core per stalled connection, and no assertion about the *result* of a poll
//! can see it. [`counting_waker`] can: an idle connection produces a count of zero across as
//! many polls as you care to make, and a self-woken one produces one wake per poll.
//!
//! That is why the wake counts here are asserted as exact equalities rather than as bounds.
//! `<= 1` would pass for a connection that woke itself once per poll and happened to be polled
//! once.
//!
//! # Why some tests drive the connections by hand
//!
//! [`run_pair`] fails a test whose futures stop making progress, which is exactly right for a
//! transfer and exactly wrong for a flood: a caller that never credits *should* stall, and the
//! stall is the assertion. Those tests step the two connections themselves, with the noop
//! waker [`poll_once`] supplies, and assert on what the layer stopped doing.

//! The whole file is gated: without the `io` feature there is no layer to schedule.

#![cfg(feature = "io")]

mod io_harness;

use std::task::{Context, Poll};

use io_harness::{
    announcement_record, announcement_record_configured, connected_pair, connected_pair_with,
    connection_with_peer_stream, counting_waker, exchange, flush, next_event, open_bidi,
    peer_writes, poll_once, run_pair, write_all,
};
use ngnet_qmux::io::testing::Fault;
use ngnet_qmux::io::{Config, Event, StreamOpen, StreamWrite};
use ngnet_qmux::{Directionality, Role, StreamId};

const REQUEST: &[u8] = b"the request that had to wait for stream capacity";
const RESPONSE: &[u8] = b"the answer, once there was a stream to send it on";

/// An open the peer forbids waits, and completes when the peer changes its mind (Spec SC-011).
///
/// The server advertises room for no bidirectional streams at all, so the client's open is
/// blocked by a limit it has been *told* about rather than by not yet having heard from its
/// peer -- the two are different waits and only the first is this one. The client's open is
/// polled until it completes and its result asserted to be a stream rather than an error,
/// which is the whole of the requirement: exhausted capacity is a condition, not a failure,
/// and a layer that reported it as one would leave a caller with nothing sensible to do.
#[test]
fn an_open_blocked_by_the_peers_limit_completes_once_the_limit_is_raised() {
    let (mut client, mut server) =
        connected_pair_with(Config::new(), Config::new().max_streams_bidi(0), |_| {});

    let client_side = async {
        let stream = io_harness::open_bidi(&mut client)
            .await
            .expect("the open reported an error rather than waiting");
        write_all(&mut client, stream, REQUEST, true)
            .await
            .expect("writing the request");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        // The client's announcement first: until it has arrived, both sides are still in the
        // first flight and a raised limit would prove nothing about a blocked open.
        loop {
            let event = next_event(&mut server)
                .await
                .expect("the server reported an error while waiting for the announcement");
            if matches!(event, Event::PeerTransportParams(_)) {
                break;
            }
        }
        server
            .extend_stream_limit(Directionality::Bidirectional, 1)
            .expect("raising the stream limit");
        io_harness::accept_stream(&mut server)
            .await
            .expect("accepting the stream the client finally opened")
    };

    let (opened, (stream, received)) = run_pair(client_side, server_side);
    assert_eq!(opened, stream);
    assert_eq!(
        received, REQUEST,
        "the stream opened after the wait carried its payload intact"
    );
    assert_eq!(
        stream.directionality(),
        Directionality::Bidirectional,
        "the wait produced the kind of stream that was asked for"
    );
}

/// A blocked open parks; it does not wake itself (Spec SC-012, and the reason for it).
///
/// The connection is given a peer announcement permitting no bidirectional streams and nothing
/// else, so its open can only wait. If the wait were a self-wake -- pending plus an immediate
/// wake, which looks identical to a caller -- the count below would rise by one per poll, and
/// the executor above would spin for as long as the peer took to raise the limit.
#[test]
fn an_immediate_blocked_open_does_not_wake_or_poll() {
    let (mut conn, mut far) = connection_with_peer_stream(Role::Client);
    peer_writes(
        &mut far,
        &announcement_record_configured(Role::Server, Config::new().max_streams_bidi(0)),
    );

    let (_waker, wakes) = counting_waker();

    for _ in 0..16 {
        assert!(
            matches!(conn.try_open_bidi(), Ok(StreamOpen::Blocked)),
            "the peer permits no bidirectional streams, so the open cannot complete"
        );
    }
    assert_eq!(
        wakes.count(),
        0,
        "the blocked open asked to be polled again rather than parking on the limit: that is a \
         busy loop, and it costs a core for every connection waiting on a slow peer"
    );
}

/// An idle connection is not woken until its byte stream has something to say (Spec SC-012).
///
/// Nothing arriving, nothing to send, nothing pending -- and the count stays at zero across
/// many polls, which is only possible if the connection registered the byte stream's waker and
/// armed nothing else. It also proves there is no timer here: a layer that had armed one would
/// eventually wake without the peer doing anything.
#[test]
fn an_idle_connection_is_not_woken_until_its_byte_stream_reports_readiness() {
    let (mut conn, mut far) = connection_with_peer_stream(Role::Client);
    let (waker, wakes) = counting_waker();
    let mut cx = Context::from_waker(&waker);

    for _ in 0..32 {
        assert!(
            matches!(conn.poll_next_event(&mut cx), Poll::Pending),
            "nothing has arrived, so there is nothing to report"
        );
    }
    assert_eq!(
        wakes.count(),
        0,
        "an idle connection asked to be polled again; it must consume nothing at all while \
         waiting for a peer that may take minutes to speak"
    );

    peer_writes(&mut far, &announcement_record(Role::Server));
    assert_eq!(
        wakes.count(),
        1,
        "the byte stream reporting readiness is what wakes an idle connection, and it is the \
         only thing that does"
    );

    assert!(
        matches!(
            conn.poll_next_event(&mut cx),
            Poll::Ready(Ok(Event::PeerTransportParams(_)))
        ),
        "the wake was followed by the event that caused it"
    );

    // Drained of everything that arrival produced, the connection is idle again.
    while conn.poll_next_event(&mut cx).is_ready() {}
    let settled = wakes.count();
    for _ in 0..32 {
        assert!(matches!(conn.poll_next_event(&mut cx), Poll::Pending));
    }
    assert_eq!(
        wakes.count(),
        settled,
        "having read everything there was, the connection went back to costing nothing"
    );
}

/// A write with no credit parks, and the peer's extension is what wakes it (Spec FR-013).
///
/// Two assertions in one test because they are two halves of the same claim. The wait must be
/// a real park -- no wake per poll -- and it must end when the peer extends the window, which
/// it can only do by sending a frame this side has to read. A layer that parked and had no
/// wakeup path would be worse than one that spun: the connection would never move again.
#[test]
fn a_credit_exhausted_write_parks_and_is_woken_by_the_extension() {
    const WINDOW: u64 = 2_048;
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW);
    let (mut client, mut server) = connected_pair(config);
    let payload = vec![0x33u8; 16 * 1_024];

    // Fill the window using the non-parking form, which reports exhaustion as a value; the
    // server is pumped alongside so the handshake completes, and it credits nothing.
    let mut stream: Option<StreamId> = None;
    let mut sent = 0usize;
    for _ in 0..64 {
        let _ = poll_once(|cx| client.poll_next_event(cx));
        let _ = poll_once(|cx| server.poll_next_event(cx));
        let _ = poll_once(|cx| client.poll_pump(cx));
        let _ = poll_once(|cx| server.poll_pump(cx));
        match stream {
            None => {
                if let StreamOpen::Opened(id) = client.try_open_bidi().expect("open outcome") {
                    stream = Some(id);
                }
            }
            Some(id) => match client
                .try_write_stream(id, &payload[sent..], false)
                .expect("a write outcome")
            {
                StreamWrite::Accepted(taken) => sent += taken,
                StreamWrite::Blocked => {}
                StreamWrite::Closed => panic!("the stream closed itself"),
            },
        }
        let _ = poll_once(|cx| client.poll_pump(cx));
    }
    let stream = stream.expect("the stream opened");
    assert!(
        sent > 0 && (sent as u64) <= WINDOW,
        "the window was filled and not exceeded: {sent} bytes against a window of {WINDOW}"
    );

    let (waker, wakes) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(
            client
                .poll_write_stream(&mut cx, stream, b"more", false)
                .is_pending(),
            "there is no credit, so there is nothing to write into"
        );
    }
    assert_eq!(
        wakes.count(),
        0,
        "the blocked write woke itself instead of parking on the peer's window"
    );

    server
        .extend_stream_credit(stream, WINDOW)
        .expect("extending the stream window");
    server
        .extend_connection_credit(WINDOW)
        .expect("extending the connection window");
    let _ = poll_once(|cx| server.poll_pump(cx));
    let _ = poll_once(|cx| client.poll_next_event(cx));

    assert!(
        matches!(
            client.poll_write_stream(&mut cx, stream, b"more", false),
            Poll::Ready(Ok(4))
        ),
        "the extension reached the sender and the write resumed"
    );
    assert!(
        wakes.count() >= 1,
        "the parked write was woken by the extension arriving, not by being polled on spec"
    );
}

/// A byte stream that cannot proceed at first still carries a transfer (Spec SC-021).
///
/// Both halves are told to refuse their first write, and the refusal is injected *before* the
/// connections exist because construction schedules the transport-parameter announcement --
/// the very first thing either side writes. The read direction needs no injection: the first
/// read on each side necessarily precedes the peer's first write, so both connections begin by
/// being unable to read and waiting to be told otherwise.
///
/// A layer that treated "not now" as "written" would lose its own announcement and both sides
/// would wait forever for parameters that were never sent.
#[test]
fn a_byte_stream_that_cannot_proceed_at_first_still_completes_a_transfer() {
    let (mut client, mut server) = connected_pair_with(Config::new(), Config::new(), |side| {
        side.inject(Fault::WriteNotNow);
    });

    exchange(&mut client, &mut server, REQUEST, RESPONSE);
}

/// A body far larger than the window transfers intact through repeated waits (Spec SC-003).
///
/// The window is a fraction of the body and of one record, so the sender exhausts its credit
/// many times over and every one of those is a park resumed by an extension from the receiver.
/// The bytes are a function of their offset rather than a repeated value, so a chunk delivered
/// twice or out of order fails the assertion instead of hiding in a run of identical bytes.
///
/// This is the layer's half of the criterion; the HTTP/3 form of it belongs to the join crate.
#[test]
fn a_body_many_times_the_flow_control_window_transfers_intact() {
    const WINDOW: u64 = 8 * 1_024;
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW);
    let (mut client, mut server) = connected_pair(config);

    // Ten times the maximum record size, and twenty-five times the window.
    let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let expected = body.clone();

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, &body, true)
            .await
            .expect("writing the body");
        flush(&mut client).await.expect("flushing");
    };

    let server_side = async {
        let mut received: Vec<u8> = Vec::new();
        loop {
            if let Event::StreamData {
                stream_id,
                data,
                fin,
                ..
            } = next_event(&mut server).await.expect("an event")
            {
                received.extend_from_slice(&data);
                if fin {
                    return received;
                }
                let consumed = data.len() as u64;
                server
                    .extend_stream_credit(stream_id, consumed)
                    .expect("extending the stream window");
                server
                    .extend_connection_credit(consumed)
                    .expect("extending the connection window");
            }
        }
    };

    let (_, received) = run_pair(client_side, server_side);
    assert_eq!(
        received.len(),
        expected.len(),
        "the transfer stopped short, which is what a truncated write looks like from here"
    );
    assert_eq!(
        received, expected,
        "every byte arrived once, in order, across many exhausted windows"
    );
}

/// How much read-ahead the flood below is allowed before the caller has said anything.
const ALLOWANCE: u64 = 8 * 1_024;

/// The protocol windows, set far above the allowance so that the bound under test is this
/// layer's own rather than the receive window restated.
const WINDOW: u64 = 256 * 1_024;

/// What the peer offers, and how much of it goes into each record.
const PAYLOAD: usize = 64 * 1_024;
const CHUNK: usize = 1_024;

/// Enough passes for the flood to reach a fixed point many times over.
const PASSES: usize = 2_000;

/// What a flood did.
struct Flooded {
    /// Bytes the peer handed to its byte stream.
    sent: usize,
    /// Bytes the layer delivered to the caller.
    delivered: u64,
    /// Bytes delivered by the halfway point, for comparison with the total.
    delivered_halfway: u64,
    /// The highest figure the layer reported for its own read-ahead.
    high_water: u64,
}

/// A peer sending continuously against a caller that credits `budget` bytes and then stops.
///
/// The caller drains every event the moment it is offered, which is what makes this a test of
/// the bound rather than of the queue: a layer measuring queue depth would see zero here
/// forever and read until the peer ran out. Within the budget each consumed byte is credited
/// **twice**, once naming the stream and once naming the connection, which is what the HTTP/3
/// layer above does and what the bound must not double-count.
fn flood(budget: u64) -> Flooded {
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW)
        .read_ahead(ALLOWANCE);
    let (mut client, mut server) = connected_pair(config);
    let payload = vec![0x5au8; PAYLOAD];

    let mut stream: Option<StreamId> = None;
    let mut sent = 0usize;
    let mut delivered = 0u64;
    let mut credited = 0u64;
    let mut high_water = 0u64;
    let mut delivered_halfway = 0u64;

    for pass in 0..PASSES {
        loop {
            match poll_once(|cx| server.poll_next_event(cx)) {
                Poll::Ready(Ok(Event::StreamData {
                    stream_id, data, ..
                })) => {
                    delivered += data.len() as u64;
                    let credit = (data.len() as u64).min(budget.saturating_sub(credited));
                    if credit > 0 {
                        credited += credit;
                        server
                            .extend_stream_credit(stream_id, credit)
                            .expect("extending the stream window");
                        server
                            .extend_connection_credit(credit)
                            .expect("extending the connection window");
                    }
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => panic!("the receiving connection ended: {error}"),
                Poll::Pending => break,
            }
        }
        high_water = high_water.max(server.read_ahead());
        let _ = poll_once(|cx| client.poll_next_event(cx));

        match stream {
            None => {
                if let StreamOpen::Opened(id) = client.try_open_bidi().expect("open outcome") {
                    stream = Some(id);
                }
            }
            Some(id) if sent < payload.len() => {
                let end = payload.len().min(sent + CHUNK);
                match client
                    .try_write_stream(id, &payload[sent..end], false)
                    .expect("a write outcome")
                {
                    StreamWrite::Accepted(taken) => sent += taken,
                    StreamWrite::Blocked => {}
                    StreamWrite::Closed => panic!("the stream closed itself"),
                }
            }
            Some(_) => {}
        }
        let _ = poll_once(|cx| client.poll_pump(cx));
        let _ = poll_once(|cx| server.poll_pump(cx));

        if pass == PASSES / 2 {
            delivered_halfway = delivered;
        }
    }

    Flooded {
        sent,
        delivered,
        delivered_halfway,
        high_water,
    }
}

/// How far past the bound one pass may carry the figure.
///
/// Events already queued when the bound was reached are still delivered, and the peer adds at
/// most one record of [`CHUNK`] bytes per pass while the caller drains every pass. Four chunks
/// is generous slack for that and still far below the sixteen kilobytes a double-counted
/// credit would buy in the test beneath.
const SLACK: u64 = 4 * CHUNK as u64;

/// A caller that never credits stops the layer reading (Spec SC-016).
///
/// The peer sends sixty-four kilobytes into a receive window that would happily take four
/// times that, and the caller consumes every event without ever reporting a byte consumed.
/// Delivery stops at the allowance and stays there for the rest of the run, with the peer's
/// bytes left sitting in the transport where they belong -- which is what backpressure is.
#[test]
fn a_caller_that_credits_nothing_stops_the_layer_reading_ahead() {
    let flooded = flood(0);

    assert_eq!(
        flooded.sent, PAYLOAD,
        "the peer was able to offer everything it had, so the layer's restraint is the layer's"
    );
    assert!(
        flooded.delivered < flooded.sent as u64,
        "the layer took everything the peer sent despite the caller never crediting a byte"
    );
    assert_eq!(
        flooded.delivered, flooded.delivered_halfway,
        "delivery was still growing halfway through the run: the bound is not a bound"
    );
    assert!(
        flooded.high_water <= ALLOWANCE + SLACK,
        "read-ahead reached {} against an allowance of {ALLOWANCE}",
        flooded.high_water
    );
}

/// The bound holds when every consumed byte is credited twice (Spec SC-030).
///
/// The caller credits a budget of sixteen kilobytes and then stops, and it credits each of
/// those bytes to both windows -- exactly what the HTTP/3 layer above does, because
/// stream-level credit does not imply connection-level credit. Only the connection-level
/// extension may adjust this layer's bound. If both were counted the caller would appear to
/// have consumed thirty-two kilobytes, delivery would run to the far side of forty, and the
/// assertion below fails; in the general case it would not fail at all, it would simply grow
/// until something else did.
#[test]
fn crediting_each_byte_twice_does_not_buy_twice_the_read_ahead() {
    const BUDGET: u64 = 16 * 1_024;
    let flooded = flood(BUDGET);

    assert!(
        flooded.delivered > BUDGET,
        "the credited budget was never even used up, so this run says nothing about the bound"
    );
    assert!(
        flooded.delivered <= BUDGET + ALLOWANCE + SLACK,
        "delivery reached {} bytes, past the {} the budget and the allowance permit: the \
         stream-level extension was counted as well as the connection-level one, and each \
         consumed byte bought two bytes of read-ahead",
        flooded.delivered,
        BUDGET + ALLOWANCE + SLACK
    );
    assert_eq!(
        flooded.delivered, flooded.delivered_halfway,
        "delivery was still growing halfway through the run: the bound is not a bound"
    );
    assert!(
        flooded.high_water <= ALLOWANCE + SLACK,
        "read-ahead reached {} against an allowance of {ALLOWANCE}",
        flooded.high_water
    );
    assert!(
        flooded.delivered < flooded.sent as u64,
        "the layer took everything the peer sent, so nothing was ever held back"
    );
}
