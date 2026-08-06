//! Outgoing bodies, and the contract that keeps their buffers alive.
//!
//! nghttp3 has no copying data source: the vectors a body hands over point at the
//! application's own memory, and nghttp3 reads through those pointers on every write until
//! the peer acknowledges the bytes. Everything here exists because getting that wrong is a
//! use-after-free rather than a wrong answer, so the tests are about *when* buffers are
//! released as much as about bytes arriving.
//!
//! The load-bearing fact, verified in `lib/nghttp3_stream.c`, is that
//! `nghttp3_stream_update_ack_offset` is reachable only from the acknowledgement entry
//! points. Reporting bytes *written* never releases anything.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ngnet_h3::{
    BodyOutcome, BodySource, Conn, ConnBuilder, ErrorKind, FieldAction, FieldSection, FixedBody,
    Header, RetainedBytes, Role, StreamId, Timestamp,
};

const CLIENT_CONTROL: i64 = 2;
const CLIENT_QPACK_ENCODER: i64 = 6;
const CLIENT_QPACK_DECODER: i64 = 10;
const SERVER_CONTROL: i64 = 3;
const SERVER_QPACK_ENCODER: i64 = 7;
const SERVER_QPACK_DECODER: i64 = 11;

fn id(raw: i64) -> StreamId {
    StreamId::new(raw).expect("literal is a valid stream id")
}

fn request_fields(path: &'static str) -> [Header<'static>; 4] {
    [
        Header::new(":method", "POST").unwrap(),
        Header::new(":scheme", "https").unwrap(),
        Header::new(":path", path).unwrap(),
        Header::new(":authority", "example.test").unwrap(),
    ]
}

/// What one side observed.
#[derive(Default, Debug)]
struct Seen {
    /// Body bytes per stream, in arrival order.
    body: HashMap<i64, Vec<u8>>,
    /// Trailing field names per stream, so a trailer can be told from a header.
    trailers: HashMap<i64, Vec<Vec<u8>>>,
    /// How many separate chunks the data handler was called with, per stream.
    chunks: HashMap<i64, usize>,
    ended: Vec<i64>,
}

fn observer(role: Role) -> Conn<Seen> {
    let mut conn = ConnBuilder::<Seen>::new(role)
        .on_field(|seen: &mut Seen, stream, section, _token, name, _value| {
            if section == FieldSection::Trailers {
                seen.trailers
                    .entry(stream.get())
                    .or_default()
                    .push(name.to_vec());
            }
            FieldAction::Continue
        })
        .on_data(|seen: &mut Seen, stream, chunk| {
            seen.body
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
            *seen.chunks.entry(stream.get()).or_default() += 1;
        })
        .on_end_stream(|seen: &mut Seen, stream| seen.ended.push(stream.get()))
        .build()
        .expect("connection");

    let (control, encoder, decoder) = match role {
        Role::Client => (CLIENT_CONTROL, CLIENT_QPACK_ENCODER, CLIENT_QPACK_DECODER),
        Role::Server => (SERVER_CONTROL, SERVER_QPACK_ENCODER, SERVER_QPACK_DECODER),
    };
    conn.bind_control_stream(id(control)).unwrap();
    conn.bind_qpack_streams(id(encoder), id(decoder)).unwrap();
    conn
}

/// How much of each offer the pretend transport accepts, and whether it acknowledges.
struct Policy {
    /// Accept limits, cycled. `usize::MAX` means "everything".
    limits: Vec<usize>,
    /// Whether accepted bytes are reported acknowledged straight away.
    ///
    /// A real QUIC stack reports acknowledgement later; doing it immediately is sound here
    /// because nothing else touches the buffers in between, and it is the only way to
    /// observe release in a synchronous test.
    ack: bool,
    step: usize,
}

impl Policy {
    /// Accepts everything and acknowledges it.
    fn eager() -> Self {
        Self {
            limits: vec![usize::MAX],
            ack: true,
            step: 0,
        }
    }

    /// Accepts everything but never acknowledges anything.
    fn never_acknowledges() -> Self {
        Self {
            limits: vec![usize::MAX],
            ack: false,
            step: 0,
        }
    }

    /// Accepts a varying, deliberately awkward number of bytes each pass, including none.
    fn miserly() -> Self {
        Self {
            limits: vec![7, 0, 1, 23, 3, 0, 61, 2],
            ack: true,
            step: 0,
        }
    }

    fn next_limit(&mut self) -> usize {
        let limit = self.limits[self.step % self.limits.len()];
        self.step += 1;
        limit
    }
}

/// One side of the pretend transport, with the state its handlers mutate.
struct Side {
    conn: Conn<Seen>,
    seen: Seen,
    policy: Policy,
    /// Streams told to stop offering because the transport took nothing.
    blocked: Vec<StreamId>,
}

impl Side {
    fn new(role: Role, policy: Policy) -> Self {
        Self {
            conn: observer(role),
            seen: Seen::default(),
            policy,
            blocked: Vec::new(),
        }
    }
}

/// Drains one offer from `from` into `to`, returning whether anything was on offer.
fn transfer(from: &mut Side, to: &mut Side, now: u64) -> bool {
    // Clearing the blocked set first is what keeps a transport that sometimes takes
    // nothing from deadlocking, and is the distinction this exercises: blocking is about
    // the transport refusing bytes, not about a body having none.
    for stream in from.blocked.drain(..) {
        from.conn.unblock_stream(stream).expect("unblock");
    }

    let Some(send) = from
        .conn
        .writev_stream(&mut from.seen)
        .expect("collect data to send")
    else {
        return false;
    };
    let stream = send.stream();
    let fin = send.fin();
    let offered: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    let accepted = offered.len().min(from.policy.next_limit());
    send.commit(accepted).expect("commit");

    if from.policy.ack && accepted > 0 {
        from.conn
            .add_ack_offset(stream, accepted as u64, &mut from.seen)
            .expect("report acknowledgement");
    }

    if accepted == 0 && !offered.is_empty() {
        from.conn.block_stream(stream).expect("block");
        from.blocked.push(stream);
    }

    // The peer only learns the stream ended if it received all the bytes before the end.
    let deliver_fin = fin && accepted == offered.len();
    if accepted > 0 || deliver_fin {
        to.conn
            .read_stream(
                stream,
                &offered[..accepted],
                deliver_fin,
                Timestamp::from_nanos(now),
                &mut to.seen,
            )
            .expect("read stream data");
    }

    !offered.is_empty() || fin
}

/// Runs both sides against each other until neither has anything left to send.
fn pump(a: &mut Side, b: &mut Side, now: u64) {
    let mut settled = false;
    for _ in 0..4096 {
        let moved = transfer(a, b, now) | transfer(b, a, now);
        if !moved {
            settled = true;
            break;
        }
    }
    assert!(settled, "the two sides never stopped exchanging bytes");
}

/// A body that hands over a fixed list of pieces, `per_call` at a time.
struct ChunkedBody {
    pieces: VecDeque<RetainedBytes>,
    per_call: usize,
}

impl ChunkedBody {
    fn new(pieces: impl IntoIterator<Item = RetainedBytes>, per_call: usize) -> Self {
        Self {
            pieces: pieces.into_iter().collect(),
            per_call,
        }
    }
}

impl BodySource for ChunkedBody {
    fn next(&mut self) -> BodyOutcome {
        let take = self.per_call.min(self.pieces.len());
        let pieces: Vec<RetainedBytes> = self.pieces.drain(..take).collect();
        if self.pieces.is_empty() {
            BodyOutcome::Eof(pieces)
        } else {
            BodyOutcome::Wrote(pieces)
        }
    }
}

/// A body that withholds everything until a gate is opened.
struct GatedBody {
    gate: Arc<AtomicBool>,
    payload: Option<RetainedBytes>,
    /// How many times the source was asked while the gate was shut.
    deferrals: Arc<AtomicUsize>,
}

impl BodySource for GatedBody {
    fn next(&mut self) -> BodyOutcome {
        if !self.gate.load(Ordering::Relaxed) {
            self.deferrals.fetch_add(1, Ordering::Relaxed);
            return BodyOutcome::Defer;
        }
        match self.payload.take() {
            Some(payload) => BodyOutcome::Eof(vec![payload]),
            None => BodyOutcome::Eof(Vec::new()),
        }
    }
}

/// A body whose first answer is "nothing yet", said the wrong way round.
///
/// Reporting no bytes without an end is not expressible as an outcome, so this says
/// `Wrote` with nothing in it — which must be turned into a deferral rather than into a
/// zero-length data frame.
struct EmptyThenPayload {
    gate: Arc<AtomicBool>,
    payload: Option<RetainedBytes>,
}

impl BodySource for EmptyThenPayload {
    fn next(&mut self) -> BodyOutcome {
        if !self.gate.load(Ordering::Relaxed) {
            return BodyOutcome::Wrote(Vec::new());
        }
        match self.payload.take() {
            Some(payload) => BodyOutcome::Eof(vec![payload]),
            None => BodyOutcome::Eof(Vec::new()),
        }
    }
}

/// A body that ends its payload but keeps the stream open for trailers.
struct BodyThenTrailers {
    payload: Option<RetainedBytes>,
}

impl BodySource for BodyThenTrailers {
    fn next(&mut self) -> BodyOutcome {
        match self.payload.take() {
            Some(payload) => BodyOutcome::EofWithTrailers(vec![payload]),
            None => BodyOutcome::EofWithTrailers(Vec::new()),
        }
    }
}

/// Wraps a payload so the test can watch the allocation itself, not a copy of it.
fn shared(bytes: &[u8]) -> (Arc<[u8]>, RetainedBytes) {
    let arc: Arc<[u8]> = Arc::from(bytes);
    let retained = RetainedBytes::new(Arc::clone(&arc));
    (arc, retained)
}

#[test]
fn request_and_response_bodies_round_trip_in_order() {
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());
    let stream = id(0);

    let request_body: Vec<u8> = (0u8..=255).cycle().take(3000).collect();
    let response_body: Vec<u8> = (0u8..=255).rev().cycle().take(5000).collect();

    client
        .conn
        .submit_request(
            stream,
            &request_fields("/echo"),
            Some(Box::new(FixedBody::new(request_body.clone()))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(request_body.as_slice()),
        "the request body should arrive whole and in order"
    );
    assert_eq!(server.seen.ended, vec![0], "the body ends the stream");

    server
        .conn
        .submit_response(
            stream,
            &[Header::new(":status", "200").unwrap()],
            Some(Box::new(FixedBody::new(response_body.clone()))),
        )
        .expect("submit response");
    pump(&mut client, &mut server, 2);

    assert_eq!(
        client.seen.body.get(&0).map(Vec::as_slice),
        Some(response_body.as_slice())
    );
    assert_eq!(client.seen.ended, vec![0]);
}

#[test]
fn a_miserly_transport_produces_identical_bytes_and_starves_nobody() {
    // Ten streams, a transport that takes an awkward and sometimes zero number of bytes,
    // and blocking to stop the same stream being offered forever. The result must match
    // the transport that takes everything, byte for byte, on every stream.
    fn run(client_policy: Policy, server_policy: Policy) -> (HashMap<i64, Vec<u8>>, Vec<i64>) {
        let mut client = Side::new(Role::Client, client_policy);
        let mut server = Side::new(Role::Server, server_policy);

        for n in 0..10i64 {
            let payload: Vec<u8> = (0..600u32)
                .map(|b| (b as u8).wrapping_add(n as u8))
                .collect();
            client
                .conn
                .submit_request(
                    id(n * 4),
                    &request_fields("/concurrent"),
                    Some(Box::new(FixedBody::new(payload))),
                )
                .expect("submit request");
        }
        pump(&mut client, &mut server, 1);

        let mut ended = server.seen.ended.clone();
        ended.sort_unstable();
        (server.seen.body.clone(), ended)
    }

    let (whole, whole_ended) = run(Policy::eager(), Policy::eager());
    let (piecemeal, piecemeal_ended) = run(Policy::miserly(), Policy::miserly());

    assert_eq!(whole_ended, (0..10).map(|n| n * 4).collect::<Vec<_>>());
    assert_eq!(
        piecemeal_ended, whole_ended,
        "a transport that takes bytes grudgingly must still finish every stream"
    );
    assert_eq!(
        piecemeal, whole,
        "partial acceptance must not change a single byte"
    );
}

#[test]
fn nothing_is_released_until_acknowledgement_arrives() {
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());
    let (arc, retained) = shared(b"the payload that must be retained");

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/retain"),
            Some(Box::new(FixedBody::new(retained))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::len),
        Some(arc.len()),
        "the body was delivered, so its buffer really was handed over"
    );
    assert_eq!(
        client.conn.retained_body_buffers(),
        1,
        "reporting bytes written must not release anything"
    );
    assert_eq!(
        Arc::strong_count(&arc),
        2,
        "the test's handle, and the crate's"
    );

    // Now acknowledge everything that was written on that stream.
    let written = server.seen.body[&0].len() as u64;
    client
        .conn
        .add_ack_offset(id(0), written + 4096, &mut client.seen)
        .expect_err("more than was written must be refused");
    assert_eq!(
        client.conn.retained_body_buffers(),
        1,
        "a refused acknowledgement must not release anything"
    );

    ack_everything(&mut client, id(0));
    assert_eq!(client.conn.retained_body_buffers(), 0);
    assert_eq!(
        Arc::strong_count(&arc),
        1,
        "the crate must have dropped its handle"
    );
}

/// Reports every byte the stream ever wrote as acknowledged, one call.
///
/// The written total is not tracked by the test, so this walks up until the connection
/// refuses — which is exactly the bounds check being exercised, from the other side.
fn ack_everything(side: &mut Side, stream: StreamId) {
    let mut total = 0u64;
    while side.conn.add_ack_offset(stream, 1, &mut side.seen).is_ok() {
        total += 1;
        assert!(total < 1 << 20, "acknowledgement was never bounded");
    }
    assert!(total > 0, "nothing was ever written on that stream");
}

#[test]
fn only_application_buffers_are_reported_acknowledged() {
    // The stream's output is a header section and a data frame header, both of which
    // nghttp3 serialised into buffers it owns, followed by the one buffer this crate
    // supplied. Acknowledging every byte up to that last buffer must release nothing,
    // because nghttp3 restricts acknowledgement reporting to application-owned buffers.
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());
    let (arc, retained) = shared(b"abcde");

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/alien"),
            Some(Box::new(FixedBody::new(retained))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    let body_len = arc.len() as u64;
    // Everything the request stream wrote, found by walking the bounds check to its limit
    // on a throwaway connection would be circular, so it is derived instead: acknowledge
    // one byte at a time and record where the release happens.
    let mut acked = 0u64;
    let mut released_at = None;
    while client
        .conn
        .add_ack_offset(id(0), 1, &mut client.seen)
        .is_ok()
    {
        acked += 1;
        if client.conn.retained_body_buffers() == 0 && released_at.is_none() {
            released_at = Some(acked);
        }
    }
    let released_at = released_at.expect("the buffer was never released");
    assert_eq!(
        released_at, acked,
        "the body buffer is the last thing written, so it must be released by the very \
         last acknowledged byte -- releasing earlier would mean frame-header bytes had \
         been counted against it"
    );
    assert!(
        acked > body_len,
        "the stream wrote frame headers as well as the body, so more bytes were written \
         than the body contains"
    );
    assert_eq!(Arc::strong_count(&arc), 1);
}

#[test]
fn a_body_of_several_buffers_is_released_one_boundary_at_a_time() {
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());

    let (first, first_retained) = shared(b"aa");
    let (second, second_retained) = shared(b"bbb");
    let (third, third_retained) = shared(b"c");
    let body = ChunkedBody::new([first_retained, second_retained, third_retained], 3);

    client
        .conn
        .submit_request(id(0), &request_fields("/multi"), Some(Box::new(body)))
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(client.conn.retained_body_buffers(), 3);
    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(&b"aabbbc"[..])
    );

    // Walk the acknowledgement forward one byte at a time and record the queue length
    // after each step. The frame headers come first, so the queue stays at three until
    // the body's own bytes start being acknowledged.
    let mut lengths = Vec::new();
    while client
        .conn
        .add_ack_offset(id(0), 1, &mut client.seen)
        .is_ok()
    {
        lengths.push(client.conn.retained_body_buffers());
    }

    // The last six acknowledged bytes are the body: aa | bbb | c.
    let tail: Vec<usize> = lengths[lengths.len() - 6..].to_vec();
    assert_eq!(
        tail,
        vec![3, 2, 2, 2, 1, 0],
        "a buffer must be released exactly when its final byte is acknowledged, and a \
         delta landing inside one must release nothing"
    );
    assert_eq!(Arc::strong_count(&first), 1);
    assert_eq!(Arc::strong_count(&second), 1);
    assert_eq!(Arc::strong_count(&third), 1);
}

#[test]
fn a_body_spanning_several_calls_arrives_whole() {
    // One piece per call, so the data callback is entered repeatedly and the retain queue
    // grows across calls rather than within one.
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());

    let pieces: Vec<RetainedBytes> = (0u8..12)
        .map(|n| RetainedBytes::from(vec![n; 64].as_slice()))
        .collect();
    let expected: Vec<u8> = (0u8..12).flat_map(|n| vec![n; 64]).collect();

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/streamed"),
            Some(Box::new(ChunkedBody::new(pieces, 1))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(expected.as_slice())
    );
    assert_eq!(
        client.conn.retained_body_buffers(),
        0,
        "everything was acknowledged as it went, so nothing should still be held"
    );
}

#[test]
fn a_source_with_more_pieces_than_nghttp3_asks_for_loses_none_of_them() {
    // nghttp3 offers a fixed array of eight vectors per call. A source handing over twenty
    // pieces at once must have the surplus carried to the next call, not dropped.
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());

    let pieces: Vec<RetainedBytes> = (0u8..20)
        .map(|n| RetainedBytes::from(vec![n; 8].as_slice()))
        .collect();
    let expected: Vec<u8> = (0u8..20).flat_map(|n| vec![n; 8]).collect();

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/surplus"),
            Some(Box::new(ChunkedBody::new(pieces, 20))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(expected.as_slice()),
        "pieces beyond the eight nghttp3 asked for must be offered again, not dropped"
    );
}

#[test]
fn a_deferred_body_resumes_without_losing_or_duplicating_a_byte() {
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());
    let gate = Arc::new(AtomicBool::new(false));
    let deferrals = Arc::new(AtomicUsize::new(0));
    let payload: Vec<u8> = (0u8..=200).collect();

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/deferred"),
            Some(Box::new(GatedBody {
                gate: Arc::clone(&gate),
                payload: Some(RetainedBytes::from(payload.as_slice())),
                deferrals: Arc::clone(&deferrals),
            })),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert!(
        deferrals.load(Ordering::Relaxed) > 0,
        "the source should have been asked and have declined"
    );
    assert!(
        !server.seen.body.contains_key(&0),
        "a deferred body must not produce an empty data frame"
    );
    assert!(
        !client.conn.is_stream_writable(id(0)).expect("usable"),
        "a deferred stream is not writable until it is resumed"
    );

    gate.store(true, Ordering::Relaxed);
    client.conn.resume_stream(id(0)).expect("resume");
    pump(&mut client, &mut server, 2);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(payload.as_slice())
    );
    assert_eq!(server.seen.ended, vec![0]);
}

#[test]
fn a_source_offering_nothing_without_ending_defers_rather_than_writing_an_empty_frame() {
    // nghttp3 asserts that a callback returning no bytes has also signalled the end. An
    // assertion aborts where it is compiled in and writes a zero-length data frame where
    // it is not, so this is the case that must never reach it either way.
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());
    let gate = Arc::new(AtomicBool::new(false));

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/empty-then-payload"),
            Some(Box::new(EmptyThenPayload {
                gate: Arc::clone(&gate),
                payload: Some(RetainedBytes::from(&b"eventually"[..])),
            })),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert!(
        !server.seen.chunks.contains_key(&0),
        "no data frame at all should have been written, not even an empty one"
    );
    assert!(!client.conn.is_stream_writable(id(0)).expect("usable"));

    gate.store(true, Ordering::Relaxed);
    client.conn.resume_stream(id(0)).expect("resume");
    pump(&mut client, &mut server, 2);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(&b"eventually"[..])
    );
}

#[test]
fn a_body_can_be_followed_by_trailers() {
    // Only reachable now that bodies exist: a message with no body ends its stream at the
    // header section, leaving nothing for a trailer to follow.
    let mut client = Side::new(Role::Client, Policy::eager());
    let mut server = Side::new(Role::Server, Policy::eager());

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/trailed"),
            Some(Box::new(BodyThenTrailers {
                payload: Some(RetainedBytes::from(&b"checksummed"[..])),
            })),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(&b"checksummed"[..])
    );
    assert!(
        server.seen.ended.is_empty(),
        "the stream must stay open for the trailing section"
    );

    client
        .conn
        .submit_trailers(id(0), &[Header::new("x-checksum", "deadbeef").unwrap()])
        .expect("submit trailers");
    pump(&mut client, &mut server, 2);

    assert_eq!(
        server.seen.trailers.get(&0).map(Vec::as_slice),
        Some(&[b"x-checksum".to_vec()][..]),
        "the field must arrive as a trailer, not as a header"
    );
    assert_eq!(server.seen.ended, vec![0]);
}

#[test]
fn closing_a_stream_releases_a_body_that_was_never_acknowledged() {
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());
    let (arc, retained) = shared(b"abandoned mid-flight");

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/abandoned"),
            Some(Box::new(FixedBody::new(retained))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);
    assert_eq!(client.conn.retained_body_buffers(), 1);

    client
        .conn
        .close_stream(id(0), &mut client.seen)
        .expect("close the stream");

    assert_eq!(client.conn.retained_body_buffers(), 0);
    assert_eq!(
        Arc::strong_count(&arc),
        1,
        "closing must release, exactly once, without an acknowledgement ever arriving"
    );
    assert!(client.conn.is_usable());
}

#[test]
fn dropping_the_connection_releases_a_body_that_was_never_acknowledged() {
    // Mandatory rather than tidy: nghttp3's own teardown frees only the buffers it
    // allocated itself, and deliberately leaves application-owned ones alone.
    let (arc, retained) = shared(b"never acknowledged");
    {
        let mut client = Side::new(Role::Client, Policy::never_acknowledges());
        let mut server = Side::new(Role::Server, Policy::never_acknowledges());
        client
            .conn
            .submit_request(
                id(0),
                &request_fields("/dropped"),
                Some(Box::new(FixedBody::new(retained))),
            )
            .expect("submit request");
        pump(&mut client, &mut server, 1);
        assert_eq!(client.conn.retained_body_buffers(), 1);
        assert_eq!(Arc::strong_count(&arc), 2);
    }
    assert_eq!(
        Arc::strong_count(&arc),
        1,
        "dropping the connection must release every buffer it still held"
    );
}

#[test]
fn a_body_that_is_never_sent_is_released_when_the_connection_goes() {
    // Submitted, but the connection is dropped before anything is ever written, so the
    // release path here is the registry's rather than the acknowledgement accounting's.
    let (arc, retained) = shared(b"submitted but never written");
    {
        let mut client = Side::new(Role::Client, Policy::eager());
        client
            .conn
            .submit_request(
                id(0),
                &request_fields("/unsent"),
                Some(Box::new(FixedBody::new(retained))),
            )
            .expect("submit request");
        assert_eq!(Arc::strong_count(&arc), 2);
    }
    assert_eq!(Arc::strong_count(&arc), 1);
}

#[test]
fn a_failed_resubmission_does_not_release_the_body_already_in_flight() {
    // The rollback on a failed submission must undo only what that call attached. A stream
    // that already carries a body has buffers nghttp3 has queued and reads through on
    // every later write, and the failures that reach this path -- a stream already in use,
    // a connection that is closing -- are recoverable, so nothing poisons and the next
    // write would hand the caller freed memory.
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());
    let (arc, retained) = shared(b"in flight, and still pointed at");

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/in-flight"),
            Some(Box::new(FixedBody::new(retained))),
        )
        .expect("the first submission");
    pump(&mut client, &mut server, 1);
    assert_eq!(client.conn.retained_body_buffers(), 1);
    assert_eq!(Arc::strong_count(&arc), 2);

    // A second submission on the same stream, with no body of its own.
    let error = client
        .conn
        .submit_request(id(0), &request_fields("/again"), None)
        .expect_err("the stream is already in use");
    assert!(
        !error.is_fatal() && client.conn.is_usable(),
        "this failure is recoverable, which is exactly why the release would go unnoticed"
    );
    assert_eq!(
        client.conn.retained_body_buffers(),
        1,
        "the in-flight body must still be retained"
    );
    assert_eq!(
        Arc::strong_count(&arc),
        2,
        "the buffer nghttp3 still points at must not have been freed"
    );

    // And with a body of its own, which is refused before nghttp3 is reached at all.
    let (second, second_retained) = shared(b"never attached");
    let error = client
        .conn
        .submit_request(
            id(0),
            &request_fields("/again-with-body"),
            Some(Box::new(FixedBody::new(second_retained))),
        )
        .expect_err("a stream carries at most one body");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(client.conn.retained_body_buffers(), 1);
    assert_eq!(Arc::strong_count(&arc), 2);
    assert_eq!(
        Arc::strong_count(&second),
        1,
        "the refused body was never taken"
    );

    // The stream is still writable and its bytes are still the ones that were queued.
    pump(&mut client, &mut server, 2);
    assert_eq!(
        server.seen.body.get(&0).map(Vec::as_slice),
        Some(&arc[..]),
        "the peer must receive the body that was actually submitted"
    );
}

#[test]
fn acknowledging_a_stream_after_closing_it_is_refused() {
    // Closing discards the stream's accounting along with its buffers, so there is nothing
    // left for a later acknowledgement to release. nghttp3 would accept it silently; this
    // does not, because then an over-report -- the condition that makes early release
    // memory-unsafe -- would become silent the moment a stream closed. The contract is
    // therefore that a caller stops reporting once it closes a stream.
    let mut client = Side::new(Role::Client, Policy::never_acknowledges());
    let mut server = Side::new(Role::Server, Policy::never_acknowledges());

    client
        .conn
        .submit_request(
            id(0),
            &request_fields("/closed"),
            Some(Box::new(FixedBody::new(b"payload".to_vec()))),
        )
        .expect("submit request");
    pump(&mut client, &mut server, 1);

    // Before the close, acknowledgement is accepted.
    client
        .conn
        .add_ack_offset(id(0), 1, &mut client.seen)
        .expect("one byte was certainly written");

    client
        .conn
        .close_stream(id(0), &mut client.seen)
        .expect("close");

    let error = client
        .conn
        .add_ack_offset(id(0), 1, &mut client.seen)
        .expect_err("the stream's accounting is gone");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("already closed"),
        "the message should name the case, got: {error}"
    );
    assert!(client.conn.is_usable(), "a caller mistake is not fatal");
}

#[test]
fn acknowledging_a_stream_that_never_wrote_is_a_typed_error() {
    let mut client = Side::new(Role::Client, Policy::eager());
    let error = client
        .conn
        .add_ack_offset(id(0), 1, &mut client.seen)
        .expect_err("nothing has been written on that stream");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        !error.is_fatal() && client.conn.is_usable(),
        "a caller mistake must not take the connection down"
    );

    client
        .conn
        .add_ack_offset(id(0), 0, &mut client.seen)
        .expect("acknowledging nothing is always allowed");
}
