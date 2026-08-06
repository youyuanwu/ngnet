//! Arbitrary inbound chunk boundaries, and what the returned credit actually accounts for.
//!
//! A QUIC stack delivers whatever it happens to have. These tests feed the *same* bytes to
//! a connection split in every way that matters — one byte at a time, in irregular runs,
//! all at once — and require the result to be identical each time.
//!
//! They also pin the meaning of the value `read_stream` returns, which is the single
//! easiest thing to get wrong here. It is not "how many of your bytes I consumed": all of
//! them always are, and re-presenting a supposed remainder would duplicate data. It is how
//! much QUIC flow-control credit may now be extended, and it deliberately excludes body
//! payload, which the caller credits itself once it has handled the chunks delivered to it.

use ngnet_h3::{Conn, ConnBuilder, FieldAction, FieldSection, Header, Role, StreamId, Timestamp};

const CLIENT_CONTROL: i64 = 2;
const CLIENT_QPACK_ENCODER: i64 = 6;
const CLIENT_QPACK_DECODER: i64 = 10;
const SERVER_CONTROL: i64 = 3;
const SERVER_QPACK_ENCODER: i64 = 7;
const SERVER_QPACK_DECODER: i64 = 11;

fn id(raw: i64) -> StreamId {
    StreamId::new(raw).expect("valid stream id")
}

#[derive(Default, Debug, PartialEq, Eq)]
struct Received {
    fields: Vec<(i64, FieldSection, Vec<u8>, Vec<u8>)>,
    body: Vec<(i64, Vec<u8>)>,
}

/// Totals used to check the credit accounting adds up.
#[derive(Default, Debug)]
struct Accounting {
    received: Received,
    body_bytes: u64,
    deferred: u64,
}

fn server() -> Conn<Accounting> {
    let mut conn = ConnBuilder::<Accounting>::new(Role::Server)
        .on_field(
            |acc: &mut Accounting, stream, section, _token, name, value| {
                acc.received
                    .fields
                    .push((stream.get(), section, name.to_vec(), value.to_vec()));
                FieldAction::Continue
            },
        )
        .on_data(|acc: &mut Accounting, stream, chunk| {
            acc.body_bytes += chunk.len() as u64;
            acc.received.body.push((stream.get(), chunk.to_vec()));
        })
        .on_deferred_consume(|acc: &mut Accounting, _stream, consumed| {
            acc.deferred += consumed;
        })
        .build()
        .expect("server");
    conn.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();
    conn
}

fn client() -> Conn<()> {
    let mut conn = ConnBuilder::<()>::new(Role::Client)
        .build()
        .expect("client");
    conn.bind_control_stream(id(CLIENT_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_DECODER))
        .unwrap();
    conn
}

/// Everything a client produces for one request, grouped by stream in emission order.
fn request_bytes() -> Vec<(i64, Vec<u8>)> {
    let mut client = client();
    client
        .submit_request(
            id(0),
            &[
                Header::new(":method", "GET").unwrap(),
                Header::new(":scheme", "https").unwrap(),
                Header::new(":path", "/fragmented").unwrap(),
                Header::new(":authority", "example.test").unwrap(),
                Header::new("accept", "text/plain").unwrap(),
            ],
            None,
        )
        .unwrap();

    let mut out: Vec<(i64, Vec<u8>)> = Vec::new();
    let mut drained = false;
    for _ in 0..64 {
        let Some(send) = client.writev_stream(&mut ()).unwrap() else {
            drained = true;
            break;
        };
        let stream = send.stream().get();
        let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
        let taken = bytes.len();
        send.commit(taken).unwrap();
        if taken == 0 {
            drained = true;
            break;
        }
        match out.last_mut() {
            Some((last, buffer)) if *last == stream => buffer.extend_from_slice(&bytes),
            _ => out.push((stream, bytes)),
        }
    }
    // Truncated traffic would make every split below a split of the wrong bytes.
    assert!(drained, "the client never stopped producing bytes");
    out
}

/// Delivers each stream's bytes in runs of `chunk`, or all at once when `chunk` is zero.
fn deliver(
    conn: &mut Conn<Accounting>,
    acc: &mut Accounting,
    data: &[(i64, Vec<u8>)],
    chunk: usize,
) -> u64 {
    let mut credit = 0u64;
    for (stream, bytes) in data {
        if chunk == 0 {
            credit += conn
                .read_stream(id(*stream), bytes, false, Timestamp::from_nanos(1), acc)
                .expect("read")
                .bytes();
            continue;
        }
        for piece in bytes.chunks(chunk) {
            credit += conn
                .read_stream(id(*stream), piece, false, Timestamp::from_nanos(1), acc)
                .expect("read")
                .bytes();
        }
    }
    credit
}

#[test]
fn the_same_bytes_split_any_way_produce_the_same_result() {
    let data = request_bytes();
    assert!(
        !data.is_empty(),
        "the client should have produced something"
    );

    let mut baseline = Accounting::default();
    let mut conn = server();
    let baseline_credit = deliver(&mut conn, &mut baseline, &data, 0);
    drop(conn);

    assert!(
        baseline
            .received
            .fields
            .iter()
            .any(|(_, _, name, _)| name == b":path"),
        "the baseline delivery should have produced the request's fields"
    );

    // One byte at a time is the pathological case: every frame header, every varint and
    // every QPACK instruction is split across calls.
    for chunk in [1usize, 2, 3, 5, 7, 13, 64] {
        let mut acc = Accounting::default();
        let mut conn = server();
        let credit = deliver(&mut conn, &mut acc, &data, chunk);
        drop(conn);

        assert_eq!(
            acc.received, baseline.received,
            "delivering in chunks of {chunk} changed what was received"
        );
        assert_eq!(
            credit, baseline_credit,
            "delivering in chunks of {chunk} changed the credit reported"
        );
    }
}

#[test]
fn the_credit_reported_accounts_for_every_delivered_byte() {
    let data = request_bytes();
    let total: u64 = data.iter().map(|(_, b)| b.len() as u64).sum();

    let mut acc = Accounting::default();
    let mut conn = server();
    let credit = deliver(&mut conn, &mut acc, &data, 0);

    // The three parts that must add up: what was creditable immediately, the body payload
    // the caller credits itself, and anything reported late for a stream that was blocked.
    assert_eq!(
        credit + acc.body_bytes + acc.deferred,
        total,
        "credit ({credit}) + body ({}) + deferred ({}) should account for all {total} \
         delivered bytes exactly once",
        acc.body_bytes,
        acc.deferred
    );
}

#[test]
fn every_supplied_byte_is_consumed_so_there_is_no_remainder() {
    // The value returned is credit, not a consumed count, so it can legitimately be less
    // than what was supplied -- and a caller re-presenting the difference would duplicate
    // data. Delivering the same bytes twice must therefore look different from delivering
    // them once, which is what proves they were fully consumed the first time.
    let data = request_bytes();

    let mut once = Accounting::default();
    let mut conn = server();
    deliver(&mut conn, &mut once, &data, 0);
    let fields_once = once.received.fields.len();
    drop(conn);

    let mut twice = Accounting::default();
    let mut conn = server();
    deliver(&mut conn, &mut twice, &data, 0);

    // Re-present only the request stream. Re-presenting the control stream would error on
    // its first byte and short-circuit the check, leaving the interesting case untested.
    let request_stream: Vec<(i64, Vec<u8>)> = data
        .iter()
        .filter(|(stream, _)| *stream == 0)
        .cloned()
        .collect();
    assert!(
        !request_stream.is_empty(),
        "the request stream should be among the emitted buffers"
    );
    let rejected = deliver_expecting_failure(&mut conn, &mut twice, &request_stream);

    // Either outcome is acceptable; silently delivering the same fields twice is not.
    assert_eq!(
        twice.received.fields.len(),
        fields_once,
        "re-presenting consumed bytes delivered the same fields again (rejected: {rejected})"
    );
}

/// Delivers again, reporting whether the connection rejected it.
fn deliver_expecting_failure(
    conn: &mut Conn<Accounting>,
    acc: &mut Accounting,
    data: &[(i64, Vec<u8>)],
) -> bool {
    for (stream, bytes) in data {
        if conn
            .read_stream(id(*stream), bytes, false, Timestamp::from_nanos(2), acc)
            .is_err()
        {
            return true;
        }
    }
    false
}
