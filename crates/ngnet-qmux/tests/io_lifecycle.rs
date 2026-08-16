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

use std::error::Error as _;
use std::task::Poll;

use io_harness::{
    announcement_record, close, connected_pair, drain_to_ending, drain_written, exchange,
    next_event, open_bidi, poll_once, run_pair, write_all,
};
use ngnet_qmux::io::testing::{Fault, TestClock, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Config, Connection, ErrorKind, Event, encode_close_record};
use ngnet_qmux::{CloseKind, CloseReason, Role, StreamId, Timestamp};

/// A byte stream that fails is a connection that fails, and it says so as a byte-stream fault.
#[test]
fn a_failing_byte_stream_ends_the_connection() {
    let (near, _far) = stream_pair();
    // Injected before the connection takes ownership: there is no second handle afterwards,
    // and a fault injected later would be testing a stream the connection never used.
    near.inject(Fault::Broken);
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let error = drain_to_ending(&mut conn);
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

    let error = drain_to_ending(&mut conn);
    assert_eq!(
        error.kind(),
        ErrorKind::TruncatedRecord,
        "the stream ended with a record half delivered, and that is not an orderly ending"
    );
    assert!(!error.kind().is_orderly());
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
            write_all(&mut client, stream, b"before the close", true)
                .await
                .expect("writing");
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

/// A connection still flushes what it is given even when the peer is slow to read.
#[test]
fn a_backed_up_transport_does_not_lose_a_record() {
    let (near, far) = stream_pair();
    // A byte stream that accepts one byte per call is the least generous transport a
    // connection can be handed, and a record that survives it survives any of them.
    near.set_write_cap(Some(1));
    let mut conn =
        Connection::client(near, TestClock::new(), Config::new()).expect("constructing a client");

    let mut far = far;
    let expected = announcement_record(Role::Client);

    let mut written = Vec::new();
    for _ in 0..expected.len() * 4 {
        let _ = poll_once(|cx| conn.poll_pump(cx));
        written.extend_from_slice(&drain_written(&mut far));
        if written.len() >= expected.len() {
            break;
        }
    }

    assert_eq!(
        written, expected,
        "the announcement arrived whole and unduplicated despite being written one byte per call"
    );
}
