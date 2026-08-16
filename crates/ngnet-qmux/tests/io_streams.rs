//! Every direct stream operation, and the effect the peer observes.
//!
//! A stream operation that a caller can invoke but whose result nothing on the far side ever
//! sees is indistinguishable from a no-op, and a layer full of those passes a unit test suite
//! and fails in production. So each test here drives one operation on one connection and
//! asserts on what the *other* connection reports -- an event, a rejected write, or a stream
//! that turns out to exist. The two are always driven together, because an operation that
//! queues a record has not happened until the pump has carried it.

//! The whole file is gated: without the `io` feature there is no layer to test, and a test
//! target that failed to compile would make `--no-default-features` fail for a reason that has
//! nothing to do with the sans-I/O core.

#![cfg(feature = "io")]

mod io_harness;

use io_harness::{connected_pair, flush, next_event, open_bidi, open_uni, run_pair, write_all};
use ngnet_qmux::io::{Config, Event, StreamWrite};
use ngnet_qmux::{Directionality, Initiator, Role, Shutdown, StreamId};

/// An opened bidirectional stream is a stream the peer receives on and can answer on.
#[test]
fn opening_a_bidirectional_stream_is_observed_by_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"question", true)
            .await
            .expect("writing");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        loop {
            if let Event::StreamData { stream_id, fin, .. } =
                next_event(&mut server).await.expect("an event")
                && fin
            {
                return stream_id;
            }
        }
    };

    let (opened, observed) = run_pair(client_side, server_side);
    assert_eq!(
        opened, observed,
        "the peer saw the stream the client opened, under the same identifier"
    );
    assert_eq!(
        opened.directionality(),
        Directionality::Bidirectional,
        "a bidirectional open produced a bidirectional identifier"
    );
    assert_eq!(
        opened.initiator(),
        Initiator::Client,
        "the identifier records who opened it, and the client did"
    );
}

/// A unidirectional stream is distinguishable from a bidirectional one, on both sides.
///
/// Worth asserting separately because the two go through different state-machine entry points
/// and different identifier arithmetic; a layer that wired both to the bidirectional one would
/// pass every data-transfer test in this suite.
#[test]
fn opening_a_unidirectional_stream_is_observed_by_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());

    let client_side = async {
        let stream = open_uni(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"announcement", true)
            .await
            .expect("writing");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        loop {
            if let Event::StreamData {
                stream_id,
                data,
                fin,
                ..
            } = next_event(&mut server).await.expect("an event")
                && fin
            {
                return (stream_id, data);
            }
        }
    };

    let (opened, (observed, data)) = run_pair(client_side, server_side);
    assert_eq!(opened, observed);
    assert_eq!(data, b"announcement");
    assert_eq!(
        opened.directionality(),
        Directionality::Unidirectional,
        "a unidirectional open produced a unidirectional identifier"
    );
}

/// A write carrying the end of stream is delivered as data plus an end, not as data alone.
#[test]
fn a_write_with_an_end_of_stream_marker_ends_the_stream_for_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"final", true)
            .await
            .expect("writing");
        flush(&mut client).await.expect("flushing");
    };

    let server_side = async {
        let mut fins = 0usize;
        let mut body = Vec::new();
        loop {
            if let Event::StreamData { data, fin, .. } =
                next_event(&mut server).await.expect("an event")
            {
                body.extend_from_slice(&data);
                if fin {
                    fins += 1;
                    return (body, fins);
                }
            }
        }
    };

    let (_, (body, fins)) = run_pair(client_side, server_side);
    assert_eq!(body, b"final");
    assert_eq!(fins, 1, "exactly one delivery carried the end of stream");
}

/// Shutting down the read side asks the peer to stop sending, and the peer is told so.
///
/// The observable effect is a [`Event::StopSending`] on the far side carrying the code the
/// caller chose: an application that resets a stream for a reason needs that reason to survive
/// the trip, or the peer can only guess at what went wrong.
#[test]
fn shutting_down_the_read_side_is_observed_by_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());
    const CODE: u64 = 0x1234;

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"partial", false)
            .await
            .expect("writing");
        client
            .shutdown_stream(stream, Shutdown::Read, CODE)
            .expect("shutting down the read side");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        loop {
            if let Event::StopSending {
                stream_id,
                app_error_code,
            } = next_event(&mut server).await.expect("an event")
            {
                return (stream_id, app_error_code);
            }
        }
    };

    let (stream, (observed, code)) = run_pair(client_side, server_side);
    assert_eq!(stream, observed, "the peer was told which stream to stop");
    assert_eq!(code, CODE, "the application's reason survived the trip");
}

/// Shutting down the write side resets the stream, and the peer sees the reset and the code.
#[test]
fn shutting_down_the_write_side_is_observed_by_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());
    const CODE: u64 = 0x5678;

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"partial", false)
            .await
            .expect("writing");
        client
            .shutdown_stream(stream, Shutdown::Write, CODE)
            .expect("shutting down the write side");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        loop {
            if let Event::StreamReset {
                stream_id,
                final_size,
                app_error_code,
            } = next_event(&mut server).await.expect("an event")
            {
                return (stream_id, final_size, app_error_code);
            }
        }
    };

    let (stream, (observed, final_size, code)) = run_pair(client_side, server_side);
    assert_eq!(stream, observed, "the peer was told which stream was reset");
    assert_eq!(code, CODE, "the application's reason survived the trip");
    assert_eq!(
        final_size, 7,
        "the reset reported how long the stream turned out to be, so the peer knows nothing \
         was lost before it"
    );
}

/// Reporting bytes consumed lets the peer send more, which is the whole point of flow control.
///
/// The window is set small enough that the sender runs out with the payload half-sent, so the
/// transfer completes only if the credit extension actually reached the peer and reopened it.
/// Without the extension this test would stall, and the harness reports a stall as a stall.
#[test]
fn reporting_bytes_consumed_lets_the_peer_send_more() {
    const WINDOW: u64 = 4_096;
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW);
    let (mut client, mut server) = connected_pair(config);

    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 253) as u8).collect();
    let expected = payload.clone();

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, &payload, true)
            .await
            .expect("writing");
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
                // Both windows, because they are separate: extending only one leaves the
                // other to run out and the transfer stalls a few kilobytes later.
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
        received, expected,
        "the whole payload arrived, which it could only do if the credit extensions reached \
         the sender"
    );
}

/// The non-parking write form reports a blocked stream rather than waiting on it.
///
/// This is the form the HTTP/3 layer needs: it is handed a synchronous closure with no context
/// to park with, so a write that cannot proceed has to come back as a value. Here the window
/// is exhausted deliberately and the outcome asserted, along with the accepted count of the
/// write that fits -- a form that truncated silently would be worse than one that refused.
#[test]
fn the_non_parking_write_form_reports_being_blocked() {
    const WINDOW: u64 = 2_048;
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW);
    let (mut client, mut server) = connected_pair(config);

    let payload = vec![0xa5u8; 8_192];

    let ((sent, outcomes), ()) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            let mut outcomes = Vec::new();
            let mut sent = 0usize;
            loop {
                match client
                    .try_write_stream(stream, &payload[sent..], false)
                    .expect("a write outcome")
                {
                    StreamWrite::Accepted(taken) => {
                        sent += taken;
                        outcomes.push(StreamWrite::Accepted(taken));
                    }
                    other => {
                        outcomes.push(other);
                        break;
                    }
                }
                flush(&mut client).await.expect("flushing");
            }
            (sent, outcomes)
        },
        async {
            // The server exists to answer the handshake; it does not extend anything, so the
            // sender's window stays shut once it is exhausted.
            flush(&mut server).await.expect("flushing");
        },
    );

    assert!(
        sent > 0,
        "the writes that fit in the window were accepted rather than refused wholesale"
    );
    assert!(
        (sent as u64) <= WINDOW,
        "no more than the advertised window was accepted"
    );
    assert_eq!(
        outcomes.last(),
        Some(&StreamWrite::Blocked),
        "the write that did not fit came back as a value rather than parking, which is what a \
         synchronous caller with no context can act on"
    );
}

/// The roles are what the identifiers say they are, from both ends.
#[test]
fn each_endpoint_reports_its_own_role() {
    let (client, server) = connected_pair(Config::new());
    assert_eq!(client.role(), Role::Client);
    assert_eq!(server.role(), Role::Server);
    assert_eq!(
        StreamId::new(0).expect("a valid identifier").initiator(),
        Initiator::Client,
        "the first stream belongs to the client, which is what makes the roles asymmetric"
    );
}

/// A record that takes part of a payload and then runs out of window still reports the count.
///
/// The two facts a write returns -- how much was taken, and why no more was -- can both be
/// true at once, because one record can pack some of the payload and only then find the
/// window exhausted. Those bytes are in the record and the record goes to the peer, so a
/// caller told only "blocked" offers them a second time and the stream carries them twice.
/// The peer then reads a duplicated fragment inside a frame it was told the length of, which
/// is a protocol failure some distance from its cause.
///
/// The window is deliberately not a multiple of the offer size, so the boundary falls partway
/// through a write rather than neatly between two.
#[test]
fn a_partly_taken_write_reports_what_it_took_even_when_it_then_blocks() {
    const WINDOW: u64 = 3_000;
    const OFFER: usize = 700;
    let config = Config::new()
        .initial_max_stream_data(WINDOW)
        .initial_max_data(WINDOW);
    let (mut client, mut server) = connected_pair(config);

    let payload = vec![0x5au8; 8_192];

    let (sent, received) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            let mut sent = 0usize;
            loop {
                let end = (sent + OFFER).min(payload.len());
                match client
                    .try_write_stream(stream, &payload[sent..end], false)
                    .expect("a write outcome")
                {
                    StreamWrite::Accepted(taken) => sent += taken,
                    StreamWrite::Blocked | StreamWrite::Closed => break,
                }
                flush(&mut client).await.expect("flushing");
            }
            flush(&mut client).await.expect("flushing");
            sent
        },
        async {
            let mut received = 0usize;
            // Exactly the window's worth arrives and then nothing does, so the read stops
            // when the count reaches it rather than on an end this test never sends.
            while (received as u64) < WINDOW {
                if let Event::StreamData { data, .. } =
                    next_event(&mut server).await.expect("an event")
                {
                    received += data.len();
                }
            }
            received
        },
    );

    assert_eq!(
        sent as u64, WINDOW,
        "every byte the window allowed must be reported as accepted; a count dropped because \
         the same record also reported a block is a byte the caller will send again"
    );
    assert_eq!(
        received as u64, WINDOW,
        "and the peer must receive exactly those bytes, which is what a resend would break"
    );
}

/// Writing to a stream that no longer exists is a closed stream, not a dead connection.
///
/// A caller above this layer holds a queue of bytes per stream, and a stream can finish --
/// or be torn down by either end -- while that queue still has something in it. The offer
/// that follows names a stream the state machine has already forgotten and disposed of.
/// Treating that as a failed record would kill a connection carrying every other exchange
/// every time one exchange ended with bytes still queued behind it.
///
/// Both ends finish the stream here, which is what makes it *gone* rather than half-closed:
/// a stream with either half still open is still a stream the write path finds, and it
/// answers with a shut write side instead.
#[test]
fn writing_to_a_stream_that_is_gone_is_reported_rather_than_fatal() {
    let (mut client, mut server) = connected_pair(Config::new());

    let (outcome, answered) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            write_all(&mut client, stream, b"a question", true)
                .await
                .expect("writing");
            flush(&mut client).await.expect("flushing");

            // Reading the server's end-of-stream is what completes the stream, and a
            // completed stream is one the state machine disposes of.
            loop {
                if let Event::StreamData { fin, .. } =
                    next_event(&mut client).await.expect("an event")
                    && fin
                {
                    break;
                }
            }

            let outcome = client
                .try_write_stream(stream, b"an afterthought", true)
                .expect("a write to a departed stream is an outcome, not an error");

            // The connection has to still work, which a fatal ending would not allow.
            let another = open_bidi(&mut client)
                .await
                .expect("opening another stream");
            write_all(&mut client, another, b"still here", true)
                .await
                .expect("writing on a fresh stream");
            flush(&mut client).await.expect("flushing");
            outcome
        },
        async {
            let mut answered = None;
            loop {
                match next_event(&mut server).await.expect("an event") {
                    Event::StreamData { stream_id, fin, .. } if fin && answered.is_none() => {
                        write_all(&mut server, stream_id, b"an answer", true)
                            .await
                            .expect("answering");
                        flush(&mut server).await.expect("flushing");
                        answered = Some(stream_id);
                    }
                    Event::StreamData {
                        data, fin: true, ..
                    } if data == b"still here" => {
                        return answered.expect("the first stream was answered");
                    }
                    _ => {}
                }
            }
        },
    );

    assert_eq!(
        outcome,
        StreamWrite::Closed,
        "a stream the state machine no longer has takes no more bytes, and says so"
    );
    assert_eq!(
        answered.get(),
        0,
        "and the exchange that finished was the client's first stream"
    );
}
