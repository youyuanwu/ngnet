//! How a connection ends, which is the half of the design that is easy to get wrong quietly.
//!
//! A transfer that works proves very little on its own: every one of the endings below is
//! reachable from a working connection, and a layer that collapsed them into one "connection
//! error" would leave a caller unable to tell a peer that closed politely from a peer that
//! crashed, a truncated record from a protocol violation, or a broken socket from either. Each
//! test here provokes one ending and asserts the *distinct* outcome it produces, because the
//! distinctions are the deliverable.
//!
//! The endings are provoked from the far side of the byte stream rather than from inside the
//! connection. A test that reached into the connection to set its error state would assert
//! that the field can be written, which nothing doubts.

//! The whole file is gated: without the `io` feature there is no layer to test, and a test
//! target that failed to compile would make `--no-default-features` fail for a reason that has
//! nothing to do with the sans-I/O core.

#![cfg(feature = "io")]

mod io_harness;

use std::cell::Cell;
use std::error::Error as _;
use std::future::poll_fn;
use std::task::Poll;

use io_harness::{
    announcement_record, close, connected_pair, drain_to_ending, drain_written, exchange, flush,
    next_event, open_bidi, peer_writes, poll_once, run, run_pair, write_all,
};
use ngnet_qmux::io::testing::{Fault, TestClock, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Config, Connection, ErrorKind, Event, encode_close_record};
use ngnet_qmux::{CloseKind, CloseReason, Role, Shutdown, StreamId, Timestamp};

/// A byte stream that fails is a connection that fails, and it says so as a byte-stream fault.
#[test]
fn a_failing_byte_stream_ends_the_connection() {
    let (near, far) = stream_pair();
    // Injected before the connection takes ownership: there is no second handle afterwards,
    // and a fault injected later would be testing a stream the connection never used.
    near.inject(Fault::Broken);
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = match poll_once(|cx| conn.poll_next_event_buffered(cx)) {
        Poll::Ready(Err(error)) => error,
        other => panic!("the injected byte-stream failure was not reported: {other:?}"),
    };
    assert!(
        conn.queued_output() > 0,
        "the buffered error path must retain the announcement until finish"
    );
    assert_eq!(
        error.kind(),
        ErrorKind::ByteStream,
        "a transport failure is reported as one rather than as a protocol problem, which is \
         the difference between blaming the peer and blaming the socket"
    );
    assert!(
        error.source().is_some(),
        "the underlying failure is carried, not flattened into a message"
    );
    assert!(
        !error.kind().is_orderly(),
        "a broken transport is not an orderly ending"
    );

    let finish_error = match poll_once(|cx| conn.poll_finish(cx)) {
        Poll::Ready(Err(error)) => error,
        other => panic!("finishing a broken transport did not report its failure: {other:?}"),
    };
    assert_eq!(finish_error.kind(), ErrorKind::ByteStream);
    assert!(
        conn.queued_output() > 0,
        "a failed transport cannot accept the retained output; dropping the ended connection \
         releases its bounded buffer"
    );
    drop((conn, far));
}

/// A peer that stops between records has ended the stream, and nothing was lost.
#[test]
fn a_byte_stream_that_ends_between_records_reports_the_end_of_the_stream() {
    let (near, far) = stream_pair();
    near.deliver(&announcement_record(Role::Server));
    let mut far = far;
    let shutdown = poll_once(|cx| far.poll_shutdown(cx));
    assert!(matches!(shutdown, Poll::Ready(Ok(()))));

    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = drain_to_ending(&mut conn);
    assert_eq!(
        error.kind(),
        ErrorKind::EndOfStream,
        "the record before the end was whole, so this is an ending and not a truncation"
    );
    assert!(
        error.kind().is_orderly(),
        "an ending on a record boundary lost nothing and is reported as orderly"
    );
}

/// A peer that stops midway through a record has truncated it, which is a different fault.
///
/// Distinguishable from the previous case only because the framer knows whether it is holding
/// a partial record. A layer that reported both as "the stream ended" would hide the one case
/// where bytes provably went missing.
#[test]
fn a_byte_stream_that_ends_mid_record_reports_a_truncated_record() {
    let record = announcement_record(Role::Server);
    assert!(record.len() > 2, "a record long enough to cut in half");

    let (near, far) = stream_pair();
    near.deliver(&record[..record.len() - 1]);
    let mut far = far;
    let shutdown = poll_once(|cx| far.poll_shutdown(cx));
    assert!(matches!(shutdown, Poll::Ready(Ok(()))));

    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = match poll_once(|cx| conn.poll_next_event_buffered(cx)) {
        Poll::Ready(Err(error)) => error,
        other => panic!("the truncated record was not reported: {other:?}"),
    };
    assert!(
        conn.queued_output() > 0,
        "the buffered truncation path must retain the announcement until finish"
    );
    assert_eq!(
        error.kind(),
        ErrorKind::TruncatedRecord,
        "the stream ended with a record half delivered, and that is not an orderly ending"
    );
    assert!(!error.kind().is_orderly());

    assert!(matches!(
        poll_once(|cx| conn.poll_finish(cx)),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(conn.queued_output(), 0);
    assert!(
        !drain_written(&mut far).is_empty(),
        "truncation completion discarded the retained announcement"
    );
}

/// A peer sending something the protocol does not allow ends the connection as a violation.
#[test]
fn a_peer_protocol_violation_ends_the_connection_as_one() {
    let (near, far) = stream_pair();
    // A well-framed record carrying a frame type the protocol does not define. The framer
    // accepts it -- the length prefix is honest -- and the state machine rejects it, which is
    // exactly the division of labour being tested: framing faults and protocol faults are
    // found in different places and reported differently.
    near.deliver(&[0x01, 0x0a]);
    drop(far);

    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = drain_to_ending(&mut conn);
    assert_eq!(
        error.kind(),
        ErrorKind::Protocol,
        "the peer broke the protocol, which is neither a transport failure nor an ending"
    );
    assert!(!error.kind().is_orderly());
}

/// A peer's close is reported as a close, with everything the peer said in it.
#[test]
fn a_peer_close_is_reported_with_its_reason() {
    let reason = CloseReason::transport(0x0d, b"a stated reason");

    let (near, far) = stream_pair();
    // The announcement first: a close is a frame like any other, and a state machine that has
    // not yet been told who it is talking to rejects the record rather than reading it.
    near.deliver(&announcement_record(Role::Server));
    near.deliver(&encode_close_record(&reason));
    drop(far);

    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = drain_to_ending(&mut conn);
    assert_eq!(error.kind(), ErrorKind::PeerClosed);
    assert!(
        error.kind().is_orderly(),
        "a peer that closed deliberately ended the connection in an orderly way"
    );

    let close = error.close_reason().expect("the close the peer sent");
    assert_eq!(close.kind(), CloseKind::Transport);
    assert_eq!(close.error_code(), 0x0d);
    assert_eq!(close.frame_type(), reason.frame_type());
    assert_eq!(close.reason(), b"a stated reason");
    assert_eq!(
        close, &reason,
        "every field the peer encoded came back, rather than a close with the details lost"
    );
}

/// A close sent by this endpoint arrives at the peer with all four fields intact.
///
/// The round trip is the point. Encoding and decoding in this crate could agree with each
/// other and disagree with the wire; here one connection encodes, the byte stream carries it,
/// and the *state machine* on the far side is what decides it is a close.
#[test]
fn a_local_close_is_observed_by_the_peer() {
    let (mut client, mut server) = connected_pair(Config::new());
    let reason = CloseReason::application(0x9001, b"the application is finished");

    let (_, error) = run_pair(
        async {
            // Something has to have been exchanged first, or the close is the only record the
            // peer ever sees and the ordering claim -- queued records leave before the close
            // -- is not being tested at all.
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            assert!(matches!(
                client.try_write_stream(stream, b"before the close", true),
                Ok(ngnet_qmux::io::StreamWrite::Accepted(16))
            ));
            assert!(
                client.queued_output() > 0,
                "the close must begin with a retained record to test ordering"
            );
            close(&mut client, &reason).await.expect("closing");
        },
        async {
            let mut received = Vec::new();
            loop {
                match next_event(&mut server).await {
                    Ok(Event::StreamData { data, .. }) => received.extend_from_slice(&data),
                    Ok(_) => {}
                    Err(error) => return (received, error),
                }
            }
        },
    );

    let (received, error) = error;
    assert_eq!(
        received, b"before the close",
        "the record queued before the close went out before it, rather than being overtaken"
    );
    assert_eq!(error.kind(), ErrorKind::PeerClosed);

    let close = error.close_reason().expect("the close this endpoint sent");
    assert_eq!(close.kind(), CloseKind::Application);
    assert_eq!(close.error_code(), 0x9001);
    assert_eq!(close.frame_type(), reason.frame_type());
    assert_eq!(close.reason(), b"the application is finished");
}

/// An ending still emits the reset it had queued (Spec SC-005).
///
/// The frames that explain an ending are queued *inside the state machine* rather than in the
/// outbound buffer: `shutdown_stream` takes no context and writes nothing, so a RESET_STREAM
/// exists only as an intention until something produces a record for it. Closing without
/// producing would therefore leave a peer with a stream that simply stopped and no reason for
/// it, which is why `poll_close` drains through `drain_pending` rather than flushing what
/// happens to be there.
///
/// Coalescing makes this worth restating rather than merely keeping. The path that used to
/// flush after every record now accumulates, so "what is already in the buffer" and "what the
/// connection owes" have come further apart, and the ending is where that difference is fatal:
/// there is no later pass to carry it.
///
/// Nothing flushes between the shutdown and the close, deliberately. A flush there would test
/// the flush.
#[test]
fn an_ending_still_emits_the_reset_it_had_queued() {
    const CODE: u64 = 0x4242;
    let (mut client, mut server) = connected_pair(Config::new());
    let reason = CloseReason::application(0x9002, b"finished, and one stream failed");

    let (_, observed) = run_pair(
        async {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            write_all(&mut client, stream, b"before the reset", false)
                .await
                .expect("writing");
            client
                .shutdown_stream(stream, Shutdown::Write, CODE)
                .expect("resetting the write side");
            client
                .shutdown_stream(stream, Shutdown::Read, CODE)
                .expect("asking the peer to stop sending");
            close(&mut client, &reason).await.expect("closing");
        },
        async {
            let mut reset = None;
            let mut stop = None;
            loop {
                match next_event(&mut server).await {
                    Ok(Event::StreamReset {
                        stream_id,
                        app_error_code,
                        ..
                    }) => reset = Some((stream_id, app_error_code)),
                    Ok(Event::StopSending {
                        stream_id,
                        app_error_code,
                    }) => stop = Some((stream_id, app_error_code)),
                    Ok(_) => {}
                    Err(error) => return (reset, stop, error),
                }
            }
        },
    );

    let (reset, stop, error) = observed;
    let (reset_stream, reset_code) = reset.expect(
        "the ending was reported with no reset before it: the RESET_STREAM queued in the state \
         machine was never produced, so the peer's stream stopped without an explanation",
    );
    let (stop_stream, stop_code) =
        stop.expect("the STOP_SENDING queued alongside the reset never reached the peer either");
    assert_eq!(
        reset_code, CODE,
        "the application's reason survived the trip"
    );
    assert_eq!(stop_code, CODE, "and so did the one on the stop-sending");
    assert_eq!(
        reset_stream, stop_stream,
        "both frames name the stream the caller shut down"
    );
    assert_eq!(
        error.kind(),
        ErrorKind::PeerClosed,
        "the close still arrived after the frames that explain it"
    );
}

/// A peer that disappears without closing is an ending, and is not blamed for a violation.
///
/// The distinction a caller acts on: a connection whose peer simply went away needs no
/// incident report, while a protocol violation does. Reporting this as a violation would make
/// every ordinary disconnection look like a misbehaving peer, and reporting it as a peer close
/// would invent a close nobody sent.
#[test]
fn a_peer_that_disappears_without_closing_is_an_ordinary_ending() {
    let (near, far) = stream_pair();
    near.deliver(&announcement_record(Role::Server));
    let mut far = far;

    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    // A conversation gets under way first, so what is being observed is a peer that vanished
    // mid-connection rather than one that was never there.
    io_harness::run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        write_all(&mut conn, stream, b"a request nobody will answer", true)
            .await
            .expect("writing");
    });

    // The peer goes away, having sent no close.
    let shutdown = poll_once(|cx| far.poll_shutdown(cx));
    assert!(matches!(shutdown, Poll::Ready(Ok(()))));

    let error = drain_to_ending(&mut conn);
    assert_eq!(
        error.kind(),
        ErrorKind::EndOfStream,
        "a peer that stopped talking ended the connection; it did not break the protocol"
    );
    assert!(error.kind().is_orderly());
    assert!(
        error.close_reason().is_none(),
        "there is no close to report, because the peer never sent one"
    );
}

/// The connection's timestamps are the caller's clock's, not something it reads itself.
///
/// Nothing in the layer calls a system clock -- a structural test enforces that -- and this is
/// the behavioural half of the same claim: a caller that controls the clock controls what the
/// connection believes the time is, which is what makes an idle timeout testable without
/// waiting for one.
#[test]
fn the_connection_reports_the_clock_it_was_given() {
    let clock = TestClock::new();
    clock.set(Timestamp::from_nanos(7_000_000_000));

    let (near, _far) = stream_pair();
    let mut conn =
        Connection::client(near, clock.clone(), Config::new()).expect("constructing a client");

    assert_eq!(
        conn.now(),
        Timestamp::from_nanos(7_000_000_000),
        "the connection reads the clock it was handed"
    );

    clock.set(Timestamp::from_nanos(9_500_000_000));
    let pumped = poll_once(|cx| conn.poll_pump(cx));
    assert!(matches!(pumped, Poll::Ready(Ok(()))));

    assert_eq!(conn.now(), Timestamp::from_nanos(9_500_000_000));
    assert_eq!(
        conn.timestamp(),
        Timestamp::from_nanos(9_500_000_000),
        "the state machine was driven with the caller's time, so the connection and its caller \
         share one timescale rather than each keeping its own"
    );
}

/// Both roles are built from a byte stream that is already established, and only that way.
///
/// The layer does not dial, listen, resolve, or configure a socket. That is not an omission to
/// be filled in later: a connection that established its own transport would have to know what
/// kind of transport it is, and the whole point of the seam is that it does not.
#[test]
fn a_connection_is_created_in_each_role_from_an_established_stream() {
    let (near, far) = stream_pair();
    let client = Connection::client(near, TestClock::new(), Config::new()).expect("a client");
    let server = Connection::server(far, TestClock::new(), Config::new()).expect("a server");

    assert_eq!(client.role(), Role::Client);
    assert_eq!(server.role(), Role::Server);
}

/// And there is no way to establish one: the layer's source names no transport at all.
#[test]
fn the_crate_offers_no_way_to_establish_a_byte_stream() {
    let layer = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/io");
    let forbidden = [
        "TcpStream",
        "TcpListener",
        "UdpSocket",
        "UnixStream",
        "ToSocketAddrs",
        "std::net",
    ];

    let mut scanned = 0usize;
    for entry in std::fs::read_dir(&layer).expect("the layer's source directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        scanned += 1;
        for name in forbidden {
            assert!(
                !source.contains(name),
                "{} names {name}; establishing a transport is the caller's job, and a layer \
                 that did it would be committing every caller to one kind of socket",
                path.display()
            );
        }
    }
    assert!(
        scanned > 0,
        "the scan found no source to check, which would make this test assert nothing"
    );
}

/// A connection configured with nothing at all completes a transfer.
///
/// This is the trap the state machine sets: its default transport parameters are all zero, and
/// a connection built from them can open no stream and send no byte, while reporting no error
/// -- it simply never proceeds. The layer must supply working values of its own. The test
/// configures *nothing*; setting a limit here would test the setter and leave the defaults
/// unexercised, which is precisely the failure mode.
#[test]
fn a_connection_built_from_the_defaults_completes_a_transfer() {
    let (near, far) = stream_pair();
    let mut client =
        Connection::client(near, TestClock::new(), Config::default()).expect("a client");
    let mut server =
        Connection::server(far, TestClock::new(), Config::default()).expect("a server");

    exchange(
        &mut client,
        &mut server,
        b"a request sent under the defaults",
        b"a response sent under the defaults",
    );
}

/// Once a connection has ended, it keeps saying so rather than appearing to recover.
#[test]
fn an_ended_connection_reports_the_same_ending_again() {
    let (near, _far) = stream_pair();
    near.inject(Fault::Broken);
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let first = drain_to_ending(&mut conn);
    let again = match poll_once(|cx| conn.poll_next_event(cx)) {
        Poll::Ready(Err(error)) => error,
        other => panic!("an ended connection answered with {other:?} instead of its ending"),
    };

    assert_eq!(
        first.kind(),
        again.kind(),
        "the ending is latched: a caller that polls again learns the same thing, rather than \
         being told the connection is fine and then failing on the next write"
    );

    let write = conn.try_write_stream(
        StreamId::new(0).expect("a valid identifier"),
        b"nothing doing",
        false,
    );
    assert!(
        write.is_err(),
        "an operation on an ended connection fails rather than pretending to have queued \
         something that will never be sent"
    );
}

/// Several records reach a slow peer whole, unduplicated and in order (Spec SC-002).
///
/// This test used to pin a stronger rule than the layer now keeps. While a record was produced
/// only into an empty outbound buffer, a partial accept could stop only *between* records, and
/// one record written a byte at a time was the whole of the case. Coalescing overturned that
/// deliberately -- see `docs/qmux/design.md`, "Produce up to the ceiling, write once, then
/// read" -- so a write is now offered several records at once and a partial accept can stop
/// anywhere, including in the middle of a length prefix. That is a state the old arrangement
/// made unreachable, and it is the one this test now exists to reach.
///
/// The transport is as hostile as this harness can make it and hostile in two independent ways,
/// because the two catch different bugs. One byte accepted per call makes every accept partial,
/// which catches a resume that assumes a write took everything it was offered. A pipe that
/// holds only a few hundred bytes until the far end reads makes the write stop and be resumed
/// *across* calls, which catches a resume point that is recomputed rather than remembered --
/// the failure the single `written` cursor exists to prevent.
///
/// The expected bytes are the same workload over a generous byte stream rather than a recorded
/// literal: what is being asserted is that the shape of the writes does not change the octets,
/// and a literal would additionally pin the transport-parameter encoding, which is not this
/// test's claim.
#[test]
fn a_backed_up_transport_does_not_lose_a_record() {
    /// Several records' worth, so the failure has somewhere to hide: one record cannot be
    /// duplicated over its neighbour, and cannot be delivered out of order.
    const PAYLOAD: usize = 50_000;
    /// Not a divisor of anything: a pipe whose bound fell on a record boundary would leave the
    /// mid-record case untested by accident.
    const PIPE: usize = 700;

    let data: Vec<u8> = (0..PAYLOAD).map(|i| (i % 251) as u8).collect();
    let expected = written_over_a_generous_stream(&data);

    let (near, far) = stream_pair();
    near.set_write_cap(Some(1));
    near.set_capacity(Some(PIPE));
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");
    let mut far = far;
    peer_writes(&mut far, &announcement_record(Role::Server));

    let sending_done = Cell::new(false);
    let sender = async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        write_all(&mut conn, stream, &data, true)
            .await
            .expect("writing the payload");
        flush(&mut conn).await.expect("flushing the payload");
        sending_done.set(true);
    };
    let consumer = async {
        let mut received = Vec::new();
        let mut buffer = [0u8; 64];
        loop {
            let taken = poll_fn(|cx| match far.poll_read(cx, &mut buffer) {
                Poll::Pending if sending_done.get() => Poll::Ready(0),
                Poll::Pending => Poll::Pending,
                Poll::Ready(outcome) => Poll::Ready(outcome.expect("the byte stream failed")),
            })
            .await;
            if taken == 0 {
                return received;
            }
            received.extend_from_slice(&buffer[..taken]);
        }
    };

    let (_, received) = run_pair(sender, consumer);

    assert_eq!(
        received.len(),
        expected.len(),
        "the slow transport received {} bytes where the generous one received {}: a partial \
         accept either lost bytes or sent some of them twice",
        received.len(),
        expected.len()
    );
    assert_eq!(
        received, expected,
        "the octets differ from the same workload over a generous byte stream, so a write that \
         stopped part way through a record was resumed at the wrong offset"
    );
}

/// The same payload over a byte stream that accepts everything, for comparison.
///
/// Deliberately not a second implementation of the encoding: it is the *same* connection code
/// over a byte stream with nothing in its way, which is what makes the comparison a statement
/// about the writes rather than about the framing.
fn written_over_a_generous_stream(data: &[u8]) -> Vec<u8> {
    let (near, far) = stream_pair();
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");
    let mut far = far;
    peer_writes(&mut far, &announcement_record(Role::Server));
    run(async {
        let stream = open_bidi(&mut conn).await.expect("opening a stream");
        write_all(&mut conn, stream, data, true)
            .await
            .expect("writing the payload");
        flush(&mut conn).await.expect("flushing the payload");
    });
    drain_written(&mut far)
}

/// Finishing without a close still shuts the write side down.
///
/// The counterpart to `poll_close` for an ending that has nothing to say: the caller failed,
/// or went away, and the peer needs to learn that nothing more is coming.
///
/// Flushing alone would look sufficient against a socket, whose drop emits a FIN whatever the
/// program does. It is not sufficient for the byte streams this seam exists to accept. A
/// buffered writer or a TLS session holds bytes of its own that only a shutdown flushes, so a
/// finish that skipped it would discard exactly the bytes it had just been asked to deliver.
///
/// The test byte stream has no `Drop`, and reports end of stream only once the writer has
/// shut down, so the peer reaching an orderly ending is the evidence that it happened.
#[test]
fn finishing_without_a_close_shuts_the_write_side_down() {
    let (near, far) = stream_pair();
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let buffered = poll_once(|cx| conn.poll_pump_buffered(cx));
    assert!(matches!(buffered, Poll::Ready(Ok(()))));
    assert!(
        conn.queued_output() > 0,
        "the finish must start with retained output to exercise its flush obligation"
    );

    let finished = poll_once(|cx| conn.poll_finish(cx));
    assert!(
        matches!(finished, Poll::Ready(Ok(()))),
        "finishing a connection with nothing outstanding completes at once: {finished:?}"
    );

    let mut far = far;
    let mut buffer = [0u8; 512];
    let mut announced = 0usize;
    loop {
        match poll_once(|cx| far.poll_read(cx, &mut buffer)) {
            Poll::Ready(Ok(0)) => break,
            Poll::Ready(Ok(read)) => announced += read,
            other => panic!("the peer should read the announcement and then the end: {other:?}"),
        }
    }

    assert!(
        announced > 0,
        "finishing flushed nothing, so the transport parameters never left the buffer"
    );
}

#[test]
fn an_orderly_inbound_end_does_not_strand_retained_output() {
    let (near, mut far) = stream_pair();
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    assert!(matches!(
        poll_once(|cx| conn.poll_pump_buffered(cx)),
        Poll::Ready(Ok(()))
    ));
    assert!(conn.queued_output() > 0);
    assert!(matches!(
        poll_once(|cx| far.poll_shutdown(cx)),
        Poll::Ready(Ok(()))
    ));

    let ending = match poll_once(|cx| conn.poll_next_event_buffered(cx)) {
        Poll::Ready(Err(error)) => error,
        other => panic!("the peer's orderly end was not reported: {other:?}"),
    };
    assert_eq!(ending.kind(), ErrorKind::EndOfStream);
    assert!(
        conn.queued_output() > 0,
        "the buffered form unexpectedly forced output while reporting EOF"
    );

    assert!(matches!(
        poll_once(|cx| conn.poll_finish(cx)),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(conn.queued_output(), 0);

    let mut received = 0usize;
    let mut buffer = [0_u8; 512];
    loop {
        match poll_once(|cx| far.poll_read(cx, &mut buffer)) {
            Poll::Ready(Ok(0)) => break,
            Poll::Ready(Ok(read)) => received += read,
            other => panic!("the peer should receive the retained output and EOF: {other:?}"),
        }
    }
    assert!(received > 0, "the retained announcement was discarded");
}
