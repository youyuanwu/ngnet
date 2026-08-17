//! A transfer either arrives whole and in order or the layer is not worth having.
//!
//! Everything here runs the same body: a client opens a stream, sends, and reads back what the
//! server sends in reply. What changes between the tests is the shape of the byte stream
//! underneath -- generous, one byte at a time, or asked to carry far more than fits in a
//! record -- because those are the conditions under which a connection that "works" quietly
//! loses or reorders data. The body lives in a shared module rather than being copied, so a
//! later runtime can rerun exactly these expectations over a real socket and the two cannot
//! drift apart.

//! The whole file is gated: without the `io` feature there is no layer to test, and a test
//! target that failed to compile would make `--no-default-features` fail for a reason that has
//! nothing to do with the sans-I/O core.

#![cfg(feature = "io")]

mod io_harness;

use std::task::Poll;

use io_harness::{
    announcement_record, client_exchange, connected_pair, connected_pair_one_byte_at_a_time,
    connected_pair_with, drain_written, exchange, flush, next_event, open_bidi, poll_once, run,
    run_pair, server_exchange, write_all,
};
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection, Event};
use ngnet_qmux::{Role, StreamId};

const REQUEST: &[u8] = b"the client's half of the conversation";
const RESPONSE: &[u8] = b"and the server's answer to it";

#[test]
fn a_client_and_a_server_complete_a_bidirectional_transfer() {
    let (mut client, mut server) = connected_pair(Config::new());
    exchange(&mut client, &mut server, REQUEST, RESPONSE);
}

/// The same exchange with the byte stream refusing to move more than one byte per call.
///
/// This is the property that separates a connection from a serialiser with a socket attached.
/// Every record is split across many reads and many writes, so every partial write has to be
/// resumed at the right offset and every partial record has to be retained until it completes.
/// A layer that assumed a write takes everything it is given, or that a read yields a whole
/// record, passes the generous test and fails this one.
#[test]
fn a_transfer_survives_one_byte_per_call() {
    let (mut client, mut server) = connected_pair_one_byte_at_a_time(Config::new());
    exchange(&mut client, &mut server, REQUEST, RESPONSE);
}

/// A megabyte through a byte stream that stops part way through records (Spec SC-002).
///
/// The case coalescing created. While a record was produced only into an empty outbound
/// buffer, a partial accept could stop only *between* records: the write side offered exactly
/// one record and resumed inside it, and the record boundary and the buffer boundary were the
/// same place. A write is now offered everything that has accumulated, so an accept stops
/// wherever the transport felt like stopping -- inside a length prefix, one byte before the end
/// of a record, in the middle of the third of four. Nothing above the cursor knows or cares,
/// which is the claim, and byte-identity over a megabyte is the evidence for it.
///
/// Four caps rather than one, because the interesting stops are at different distances from a
/// record boundary and a single cap exercises one arithmetic. Seven is small and coprime with
/// everything; a thousand divides no record; 16381 is one byte short of the record limit, which
/// is the boundary a fencepost error lands on; 40000 spans more than two records, which is the
/// case where one accept covers several boundaries at once. Each is paired with a pipe that
/// holds a few times the cap, so the write also has to stop and be resumed *across* calls
/// rather than only within one.
///
/// The bytes are a function of their offset, so a duplicated or reordered chunk fails the
/// comparison rather than hiding in a run of identical values.
#[test]
fn a_megabyte_survives_writes_that_stop_inside_records() {
    const BODY: usize = 1 << 20;
    let request: Vec<u8> = (0..BODY).map(|i| (i % 251) as u8).collect();
    let response: Vec<u8> = (0..4_096).map(|i| (i % 241) as u8).collect();

    // Windows above the body, so what is being tested is the write path rather than flow
    // control, which has tests of its own in `io_scheduling.rs`.
    let config = Config::new()
        .initial_max_stream_data(2 << 20)
        .initial_max_data(4 << 20);

    for cap in [7usize, 1_000, 16_381, 40_000] {
        let (mut client, mut server) = connected_pair_with(config, config, |side| {
            side.set_write_cap(Some(cap));
            side.set_capacity(Some(cap * 3 + 1));
        });
        let (received_response, received_request) = run_pair(
            client_exchange(&mut client, &request),
            server_exchange(&mut server, &response),
        );
        assert_eq!(
            received_request.len(),
            request.len(),
            "with {cap} bytes accepted per write the transfer stopped {} bytes short, which is \
             what a resume from the wrong offset looks like from here",
            request.len() - received_request.len()
        );
        assert_eq!(
            received_request, request,
            "the body did not survive writes capped at {cap} bytes: some record was resumed at \
             the wrong offset, or part of one was sent twice"
        );
        assert_eq!(
            received_response, response,
            "the answer did not survive the same treatment in the other direction"
        );
    }
}

/// A payload far larger than one record, which is the case a single record cannot serve.
///
/// The payload is not a repeated byte: a reordering or a duplicated chunk in a run of the same
/// value is invisible. Each byte is a function of its offset, so the assertion catches order
/// and not merely length.
#[test]
fn a_payload_far_larger_than_one_record_arrives_intact_and_in_order() {
    let request: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let response: Vec<u8> = (0..150_000u32).map(|i| (i % 241) as u8).collect();

    let config = Config::new()
        .initial_max_stream_data(1 << 20)
        .initial_max_data(1 << 21);
    let (mut client, mut server) = connected_pair(config);

    exchange(&mut client, &mut server, &request, &response);
}

/// An end of stream carrying nothing, which is the case a data-carrying `fin` hides.
///
/// A layer that only marks the end of a stream while packing stream data has no way to send
/// this, and a caller that has finished writing would hang waiting for a peer that is waiting
/// for the end. The peer must see a final event with no bytes in it.
#[test]
fn a_zero_length_end_of_stream_is_delivered() {
    let (mut client, mut server) = connected_pair(Config::new());

    let client_side = async {
        let stream = open_bidi(&mut client).await.expect("opening a stream");
        write_all(&mut client, stream, b"body", false)
            .await
            .expect("writing the body");
        write_all(&mut client, stream, &[], true)
            .await
            .expect("writing the end of stream");
        flush(&mut client).await.expect("flushing");
        stream
    };

    let server_side = async {
        let mut received = Vec::new();
        loop {
            if let Event::StreamData { data, fin, .. } =
                next_event(&mut server).await.expect("an event")
            {
                if fin {
                    return (received, data.is_empty());
                }
                received.extend_from_slice(&data);
            }
        }
    };

    let (_, (received, empty_fin)) = run_pair(client_side, server_side);
    assert_eq!(received, b"body", "the body arrived before the end");
    assert!(
        empty_fin,
        "the end of stream arrived as its own event carrying no data, which is the only way a \
         caller that has already written its whole body can signal it"
    );
}

/// Several events produced by one read, delivered as one ordered sequence.
///
/// A read that yields three records must not lose two of them, and must not deliver them in
/// whichever order a hash map iterates. The client sends on three streams without flushing
/// between them, and the server asserts it sees all three in the order they were sent.
#[test]
fn several_events_from_one_read_arrive_as_one_ordered_sequence() {
    let (mut client, mut server) = connected_pair(Config::new());

    let bodies: [&[u8]; 3] = [b"first", b"second", b"third"];

    let client_side = async {
        let mut opened = Vec::new();
        for body in bodies {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            write_all(&mut client, stream, body, true)
                .await
                .expect("writing a body");
            opened.push(stream);
        }
        flush(&mut client).await.expect("flushing");
        opened
    };

    let server_side = async {
        // Collected in first-seen order across all three streams at once. Reading them one
        // stream at a time would discard the events belonging to the others, which is exactly
        // the failure this test exists to catch.
        let mut seen: Vec<(StreamId, Vec<u8>)> = Vec::new();
        let mut finished = 0usize;
        while finished < bodies.len() {
            if let Event::StreamData {
                stream_id,
                data,
                fin,
                ..
            } = next_event(&mut server).await.expect("an event")
            {
                match seen.iter_mut().find(|(id, _)| *id == stream_id) {
                    Some((_, body)) => body.extend_from_slice(&data),
                    None => seen.push((stream_id, data.to_vec())),
                }
                if fin {
                    finished += 1;
                }
            }
        }
        seen
    };

    let (opened, seen) = run_pair(client_side, server_side);

    let expected: Vec<_> = opened
        .into_iter()
        .zip(bodies)
        .map(|(stream, body)| (stream, body.to_vec()))
        .collect();
    assert_eq!(
        seen, expected,
        "the events arrived in the order they were produced, on the streams they were produced on"
    );
}

/// One read carrying several records yields every event it contained, in order.
///
/// Stronger than the test above, and deliberately so: here the records are handed over as a
/// single chunk of bytes and the connection is polled exactly once before the events are
/// counted. A layer that surfaced the first frame of a chunk and dropped the rest, or that
/// needed another read per event, fails this and passes the looser version.
#[test]
fn a_single_read_carrying_several_events_yields_them_as_one_sequence() {
    let bodies: [&[u8]; 3] = [b"first", b"second", b"third"];

    // Real traffic, produced by a real client: the announcement it sends unprompted followed
    // by three streams' worth of records, all collected off the wire as one run of bytes.
    let (near, far) = stream_pair();
    near.deliver(&announcement_record(Role::Server));
    let mut far = far;
    let mut client = Connection::client(near, TestClock::new(), Config::new()).expect("a client");
    let opened = run(async {
        let mut opened = Vec::new();
        for body in bodies {
            let stream = open_bidi(&mut client).await.expect("opening a stream");
            write_all(&mut client, stream, body, true)
                .await
                .expect("writing a body");
            opened.push(stream);
        }
        opened
    });
    let traffic = drain_written(&mut far);

    // Delivered in one go, and read in one pump.
    let (server_near, _server_far) = stream_pair();
    server_near.deliver(&traffic);
    let mut server =
        Connection::server(server_near, TestClock::new(), Config::new()).expect("a server");
    let pumped = poll_once(|cx| server.poll_pump(cx));
    assert!(matches!(pumped, Poll::Ready(Ok(()))));

    let mut events = Vec::new();
    while let Poll::Ready(Ok(event)) = poll_once(|cx| server.poll_next_event(cx)) {
        events.push(event);
    }

    assert!(
        matches!(events.first(), Some(Event::PeerTransportParams(_))),
        "the peer's parameters came first, because they were the first thing it sent"
    );

    let delivered: Vec<(StreamId, Vec<u8>, bool)> = events
        .into_iter()
        .filter_map(|event| match event {
            Event::StreamData {
                stream_id,
                data,
                fin,
                ..
            } => Some((stream_id, data.to_vec(), fin)),
            _ => None,
        })
        .collect();

    let expected: Vec<(StreamId, Vec<u8>, bool)> = opened
        .into_iter()
        .zip(bodies)
        .map(|(stream, body)| (stream, body.to_vec(), true))
        .collect();
    assert_eq!(
        delivered, expected,
        "one read produced every event the bytes contained, in the order the peer produced \
         them, with none collapsed and none held back for a later read"
    );
}

/// The generic bodies are what a later runtime reruns, so they are exercised as such here.
#[test]
fn the_shared_exchange_bodies_run_over_the_in_memory_pair() {
    let (mut client, mut server) = connected_pair(Config::new());
    let (response, request) = run_pair(
        client_exchange(&mut client, REQUEST),
        server_exchange(&mut server, RESPONSE),
    );
    assert_eq!(request, REQUEST);
    assert_eq!(response, RESPONSE);
}
