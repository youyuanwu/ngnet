//! Message bodies and flow control (Spec SC-001, SC-011, SC-013, SC-018, US-4).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use ngnet_h2::{
    BodyOutcome, BodySource, BytesBody, ErrorCode, ErrorKind, FrameInfo, Header, HeaderAction,
    Session, SessionBuilder, Setting, StreamId,
};

const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn drain<C>(session: &mut Session<C>, context: &mut C) -> Vec<u8> {
    let mut wire = Vec::new();
    while let Some(block) = session.send(context).expect("send failed") {
        wire.extend_from_slice(block);
    }
    wire
}

fn parse_frames(mut wire: &[u8]) -> Vec<(u8, u8, u32)> {
    if wire.starts_with(CLIENT_MAGIC) {
        wire = &wire[CLIENT_MAGIC.len()..];
    }
    let mut frames = Vec::new();
    while wire.len() >= 9 {
        let len = u32::from_be_bytes([0, wire[0], wire[1], wire[2]]) as usize;
        frames.push((
            wire[3],
            wire[4],
            u32::from_be_bytes([wire[5], wire[6], wire[7], wire[8]]) & 0x7fff_ffff,
        ));
        if wire.len() < 9 + len {
            break;
        }
        wire = &wire[9 + len..];
    }
    frames
}

/// What a peer accumulated during an exchange.
#[derive(Debug, Default)]
struct Seen {
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    closed: Vec<(i32, u32, Option<String>)>,
    begun: Vec<i32>,
}

fn recorder() -> SessionBuilder<Seen> {
    SessionBuilder::<Seen>::server()
        .on_begin_headers(|seen: &mut Seen, info: FrameInfo| {
            let stream = info.stream_id().get();
            if !seen.begun.contains(&stream) {
                seen.begun.push(stream);
            }
            HeaderAction::Continue
        })
        .on_header(
            |seen: &mut Seen, _info: FrameInfo, name: &[u8], value: &[u8]| {
                seen.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .on_data_chunk(|seen: &mut Seen, _stream: StreamId, chunk: &[u8]| {
            seen.body.extend_from_slice(chunk);
        })
        .on_stream_close(
            |seen: &mut Seen, stream: StreamId, code: ErrorCode, body_error| {
                seen.closed.push((
                    stream.get(),
                    code.get(),
                    body_error.map(|e| e.to_string()),
                ));
            },
        )
}

fn client_recorder() -> SessionBuilder<Seen> {
    SessionBuilder::<Seen>::client()
        .on_header(
            |seen: &mut Seen, _info: FrameInfo, name: &[u8], value: &[u8]| {
                seen.headers.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
                HeaderAction::Continue
            },
        )
        .on_data_chunk(|seen: &mut Seen, _stream: StreamId, chunk: &[u8]| {
            seen.body.extend_from_slice(chunk);
        })
        .on_stream_close(
            |seen: &mut Seen, stream: StreamId, code: ErrorCode, body_error| {
                seen.closed.push((
                    stream.get(),
                    code.get(),
                    body_error.map(|e| e.to_string()),
                ));
            },
        )
}

fn request_headers() -> Vec<Header<'static>> {
    vec![
        Header::new(":method", "POST"),
        Header::new(":scheme", "http"),
        Header::new(":authority", "example.test"),
        Header::new(":path", "/upload"),
    ]
}

/// Shuttles bytes between two sessions until neither has anything more to say.
fn pump(
    client: &mut Session<Seen>,
    client_seen: &mut Seen,
    server: &mut Session<Seen>,
    server_seen: &mut Seen,
) {
    for _ in 0..64 {
        let to_server = drain(client, client_seen);
        if !to_server.is_empty() {
            let consumed = server.recv(&to_server, server_seen).expect("server recv");
            assert_eq!(consumed, to_server.len(), "server should consume everything");
        }

        let to_client = drain(server, server_seen);
        if !to_client.is_empty() {
            let consumed = client.recv(&to_client, client_seen).expect("client recv");
            assert_eq!(consumed, to_client.len(), "client should consume everything");
        }

        if to_server.is_empty() && to_client.is_empty() {
            return;
        }
    }
    panic!("the exchange did not settle");
}

#[test]
fn a_multi_chunk_body_round_trips_intact() {
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), BytesBody::new(payload.clone()))
        .expect("submitting a request with a body should succeed");

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    assert_eq!(
        server_seen.body.len(),
        payload.len(),
        "the whole body should have arrived"
    );
    assert_eq!(
        server_seen.body, payload,
        "the body should arrive in order and unmodified"
    );
}

#[test]
fn a_response_body_reaches_the_client() {
    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client.submit_request(&request_headers()).unwrap();
    let opening = drain(&mut client, &mut client_seen);
    server.recv(&opening, &mut server_seen).unwrap();

    server
        .submit_response_with_body(
            StreamId::new(1),
            &[Header::new(":status", "200")],
            BytesBody::new(b"hello from the server".to_vec()),
        )
        .expect("submitting a response with a body should succeed");

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    assert_eq!(client_seen.body, b"hello from the server");
    assert!(
        client_seen
            .headers
            .contains(&(":status".into(), "200".into()))
    );
}

#[test]
fn an_empty_body_produces_a_well_formed_message() {
    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), BytesBody::new(Vec::new()))
        .unwrap();

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    assert!(server_seen.body.is_empty());
    assert!(
        server_seen
            .headers
            .contains(&(":path".into(), "/upload".into())),
        "the message itself should still be well formed"
    );
}

/// A body source that fails partway through.
struct FailingBody {
    written: usize,
}

impl BodySource for FailingBody {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        if self.written == 0 {
            let take = buf.len().min(16);
            buf[..take].fill(b'a');
            self.written += take;
            BodyOutcome::Wrote(take)
        } else {
            BodyOutcome::Fail(Box::new(std::io::Error::other("the disk caught fire")))
        }
    }
}

#[test]
fn a_body_failure_resets_the_stream_and_surfaces_the_error() {
    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), FailingBody { written: 0 })
        .unwrap();

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    let failure = client_seen
        .closed
        .iter()
        .find(|(stream, _, _)| *stream == 1)
        .expect("the stream should have closed");

    assert_eq!(
        failure.2.as_deref(),
        Some("the disk caught fire"),
        "the caller's own error should be handed back, saw {:?}",
        failure
    );
}

/// A body that announces trailers when it ends.
struct TrailingBody {
    sent: bool,
}

impl BodySource for TrailingBody {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        if self.sent {
            return BodyOutcome::EofWithTrailers(0);
        }
        let body = b"checksummed payload";
        let take = body.len().min(buf.len());
        buf[..take].copy_from_slice(&body[..take]);
        self.sent = true;
        BodyOutcome::Wrote(take)
    }
}

#[test]
fn trailers_arrive_after_the_final_body_chunk() {
    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    let stream = client
        .submit_request_with_body(&request_headers(), TrailingBody { sent: false })
        .unwrap();

    // Drain until the body has ended and the trailer window has opened. The session stops
    // wanting to write at that point even though the stream is still open, which is why
    // draining alone is not enough to know the exchange is finished.
    for _ in 0..16 {
        let out = drain(&mut client, &mut client_seen);
        if !out.is_empty() {
            server.recv(&out, &mut server_seen).unwrap();
        }
        if client.trailers_ready(stream) {
            break;
        }
    }

    assert!(
        client.trailers_ready(stream),
        "the body should have announced that trailers may follow"
    );

    client
        .submit_trailer(stream, &[Header::new("x-checksum", "deadbeef")])
        .expect("trailers should be legal once the body has ended without closing");

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    assert_eq!(server_seen.body, b"checksummed payload");
    assert!(
        server_seen
            .headers
            .contains(&("x-checksum".into(), "deadbeef".into())),
        "the trailer should have arrived, saw {:?}",
        server_seen.headers
    );
}

#[test]
fn consume_is_rejected_on_a_session_that_manages_windows_itself() {
    let mut server = recorder().build().unwrap();

    let error = server
        .consume(StreamId::new(1), 100)
        .expect_err("a default session replenishes windows itself");

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("manual_flow_control"));
}

#[test]
fn a_default_session_completes_without_ever_consuming() {
    // The counterpart to the rejection above: callers who never think about flow control
    // must not stall.
    let payload: Vec<u8> = vec![b'z'; 200_000];

    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), BytesBody::new(payload.clone()))
        .unwrap();

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    assert_eq!(
        server_seen.body.len(),
        payload.len(),
        "a body larger than the initial window must still complete without consume()"
    );
}

#[test]
fn manual_flow_control_withholds_data_until_consumption_is_reported() {
    let payload: Vec<u8> = vec![b'q'; 200_000];

    let mut client = client_recorder().build().unwrap();
    let mut server = SessionBuilder::<Seen>::server()
        .manual_flow_control(true)
        .setting(Setting::InitialWindowSize(16_384))
        .on_data_chunk(|seen: &mut Seen, _stream: StreamId, chunk: &[u8]| {
            seen.body.extend_from_slice(chunk);
        })
        .build()
        .unwrap();

    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), BytesBody::new(payload.clone()))
        .unwrap();

    // Shuttle without ever reporting consumption: the window must run out.
    for _ in 0..32 {
        let to_server = drain(&mut client, &mut client_seen);
        if to_server.is_empty() {
            break;
        }
        server.recv(&to_server, &mut server_seen).unwrap();
        let to_client = drain(&mut server, &mut server_seen);
        if !to_client.is_empty() {
            client.recv(&to_client, &mut client_seen).unwrap();
        }
    }

    let stalled_at = server_seen.body.len();
    assert!(
        stalled_at < payload.len(),
        "without reporting consumption the transfer should have stalled, got {stalled_at} \
         of {}",
        payload.len()
    );

    assert!(
        stalled_at > 0,
        "some data should have arrived before the stall"
    );

    // Now report consumption and the rest must flow to completion.
    let mut total_received = server_seen.body.len();
    for _ in 0..512 {
        let outstanding = server_seen.body.len();
        if outstanding > 0 {
            server
                .consume(StreamId::new(1), outstanding)
                .expect("consume should succeed on a manual-flow-control session");
            server_seen.body.clear();
        }

        let to_client = drain(&mut server, &mut server_seen);
        if !to_client.is_empty() {
            client.recv(&to_client, &mut client_seen).unwrap();
        }
        let to_server = drain(&mut client, &mut client_seen);
        if to_server.is_empty() && server_seen.body.is_empty() {
            break;
        }
        if !to_server.is_empty() {
            server.recv(&to_server, &mut server_seen).unwrap();
        }
        total_received += server_seen.body.len();
    }
    total_received += server_seen.body.len();

    assert_eq!(
        total_received,
        payload.len(),
        "reporting consumption must let the whole body through; got {total_received} of {}",
        payload.len()
    );
}

/// Counts its own drops, so body release can be observed rather than assumed.
struct CountedBody {
    inner: BytesBody,
    dropped: Arc<AtomicUsize>,
}

impl BodySource for CountedBody {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        self.inner.fill(buf)
    }
}

impl Drop for CountedBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

#[test]
fn a_body_source_is_dropped_when_its_stream_closes() {
    // Bodies are owned by the session for exactly the life of their stream, so they must
    // be released as each stream closes rather than accumulating until teardown.
    let dropped = Arc::new(AtomicUsize::new(0));

    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    const BODIES: usize = 64;
    for _ in 0..BODIES {
        let stream = client
            .submit_request_with_body(
                &request_headers(),
                CountedBody {
                    inner: BytesBody::new(vec![b'x'; 1024]),
                    dropped: Arc::clone(&dropped),
                },
            )
            .unwrap();

        pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

        // The server must actually answer: a stream stays open awaiting a response, and
        // an unanswered stream never closes, so nothing would be released.
        server
            .submit_response(stream, &[Header::new(":status", "200")])
            .expect("responding should succeed");

        pump(&mut client, &mut client_seen, &mut server, &mut server_seen);
        server_seen.body.clear();
    }

    assert_eq!(
        dropped.load(AtomicOrdering::Relaxed),
        BODIES,
        "every body should have been released as its stream closed, not held until teardown"
    );

    // The sessions' Drop additionally asserts every native allocation was released.
    drop(client);
    drop(server);
}

#[test]
fn a_body_may_not_be_attached_to_a_stream_that_is_not_open() {
    // Without an open stream there is nothing whose closure releases the body entry, and
    // libnghttp2 would already be holding its address in a queued item.
    let mut server = recorder().build().unwrap();

    let error = server
        .submit_response_with_body(
            StreamId::new(99),
            &[Header::new(":status", "200")],
            BytesBody::new(b"body".to_vec()),
        )
        .expect_err("a body needs an open stream");

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("not open"));
}

#[test]
fn frames_show_the_body_split_across_data_frames() {
    let mut client = client_recorder().build().unwrap();
    let mut client_seen = Seen::default();

    client
        .submit_request_with_body(&request_headers(), BytesBody::new(vec![b'x'; 40_000]))
        .unwrap();

    let wire = drain(&mut client, &mut client_seen);
    let frames = parse_frames(&wire);
    let data_frames = frames.iter().filter(|(kind, _, _)| *kind == 0x00).count();

    assert!(
        data_frames > 1,
        "a body larger than the maximum frame size should span several DATA frames, saw \
         {data_frames}"
    );
}

/// Reads the buffer it is handed before writing, to prove nothing leaks through.
struct Peeking {
    saw_nonzero: Arc<AtomicUsize>,
}

impl BodySource for Peeking {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        // The buffer libnghttp2 hands over is reused between frames. If it were not
        // cleared, this would be a window onto whatever the previous frame left there,
        // including another stream's body.
        let nonzero = buf.iter().filter(|b| **b != 0).count();
        self.saw_nonzero.fetch_add(nonzero, AtomicOrdering::Relaxed);
        BodyOutcome::Eof(0)
    }
}

#[test]
fn a_body_source_never_sees_residue_from_earlier_frames() {
    let saw_nonzero = Arc::new(AtomicUsize::new(0));

    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    // Push a recognisable payload through first, so there is something to leak.
    let secret = b"SUPERSECRETTOKEN".repeat(512);
    client
        .submit_request_with_body(&request_headers(), BytesBody::new(secret))
        .unwrap();
    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);
    server_seen.body.clear();

    // Then hand a source that inspects whatever it is given.
    for _ in 0..8 {
        client
            .submit_request_with_body(
                &request_headers(),
                Peeking {
                    saw_nonzero: Arc::clone(&saw_nonzero),
                },
            )
            .unwrap();
        pump(&mut client, &mut client_seen, &mut server, &mut server_seen);
    }

    assert_eq!(
        saw_nonzero.load(AtomicOrdering::Relaxed),
        0,
        "a body source must be handed a cleared buffer, never residue from earlier frames"
    );
}

/// Claims to have written far more than it was given.
struct Overrunning;

impl BodySource for Overrunning {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        BodyOutcome::Wrote(buf.len() + 1_000_000)
    }
}

#[test]
fn a_body_source_claiming_to_overrun_is_caught() {
    // Trusting the claim would make libnghttp2 read past its buffer, and an absurd value
    // could also cast onto a negative control code and stall the stream silently.
    let mut client = client_recorder().build().unwrap();
    let mut server = recorder().build().unwrap();
    let (mut client_seen, mut server_seen) = (Seen::default(), Seen::default());

    client
        .submit_request_with_body(&request_headers(), Overrunning)
        .unwrap();

    pump(&mut client, &mut client_seen, &mut server, &mut server_seen);

    let failure = client_seen
        .closed
        .iter()
        .find(|(stream, _, _)| *stream == 1)
        .expect("the stream should have been terminated");
    assert!(
        failure.2.as_deref().is_some_and(|e| e.contains("more than")),
        "the overrun should be reported to the caller, saw {failure:?}"
    );
}

#[test]
fn consume_rejects_a_length_it_could_not_account_for() {
    let mut server = SessionBuilder::<Seen>::server()
        .manual_flow_control(true)
        .build()
        .unwrap();

    let error = server
        .consume(StreamId::new(1), usize::MAX)
        .expect_err("a length libnghttp2 would truncate must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn trailers_are_rejected_without_an_open_trailer_window() {
    // libnghttp2 accepts this and emits nothing, which reads as success and is not.
    let mut client = client_recorder().build().unwrap();
    let mut client_seen = Seen::default();

    let stream = client
        .submit_request_with_body(&request_headers(), BytesBody::new(b"no trailers".to_vec()))
        .unwrap();
    let _ = drain(&mut client, &mut client_seen);

    let error = client
        .submit_trailer(stream, &[Header::new("x-checksum", "deadbeef")])
        .expect_err("a body that did not announce trailers leaves no window for them");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("EofWithTrailers"));
}

/// A body that has nothing to give until it is told otherwise.
///
/// Counts how often it is consulted, which is what makes the deferral assertions exact:
/// the point of deferring is not merely that no data is produced, but that the source is
/// left alone until it is resumed.
struct DeferringBody {
    ready: Arc<AtomicUsize>,
    payload: &'static [u8],
    sent: usize,
    fills: Arc<AtomicUsize>,
}

impl BodySource for DeferringBody {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        self.fills.fetch_add(1, AtomicOrdering::SeqCst);

        if self.ready.load(AtomicOrdering::SeqCst) == 0 {
            return BodyOutcome::Defer;
        }
        if self.sent == self.payload.len() {
            return BodyOutcome::Eof(0);
        }

        let take = (self.payload.len() - self.sent).min(buf.len());
        buf[..take].copy_from_slice(&self.payload[self.sent..self.sent + take]);
        self.sent += take;
        BodyOutcome::Eof(take)
    }
}

#[test]
fn a_deferred_body_stalls_its_stream_without_emitting_or_being_asked_again() {
    // FR-029, and the precursor to SC-008. Three properties matter and each is asserted
    // separately, because an implementation could satisfy any one of them while failing
    // the others: no DATA frames are produced, the source is not consulted again, and the
    // stream still completes once resumed.
    let ready = Arc::new(AtomicUsize::new(0));
    let fills = Arc::new(AtomicUsize::new(0));

    let mut client = SessionBuilder::<()>::client().build().unwrap();
    let stream = client
        .submit_request_with_body(
            &[
                Header::new(":method", "POST"),
                Header::new(":scheme", "http"),
                Header::new(":authority", "example.test"),
                Header::new(":path", "/deferred"),
            ],
            DeferringBody {
                ready: Arc::clone(&ready),
                payload: b"eventually",
                sent: 0,
                fills: Arc::clone(&fills),
            },
        )
        .expect("submitting the request");

    // First drain: headers go out, the body defers.
    let wire = drain(&mut client, &mut ());
    let after_first = fills.load(AtomicOrdering::SeqCst);
    assert_eq!(
        after_first, 1,
        "the body should have been consulted exactly once before deferring"
    );
    assert!(
        !parse_frames(&wire).iter().any(|(kind, _, _)| *kind == 0),
        "a deferred body must emit no DATA frames, not even empty ones"
    );

    // Further passes must not touch the body at all. This is the property that separates
    // deferral from returning `Wrote(0)`, which would emit an empty DATA frame and
    // reschedule the stream on every pass.
    for _ in 0..8 {
        let idle = drain(&mut client, &mut ());
        assert!(
            parse_frames(&idle).iter().all(|(kind, _, _)| *kind != 0),
            "a deferred stream must stay silent across repeated sends"
        );
    }
    assert_eq!(
        fills.load(AtomicOrdering::SeqCst),
        after_first,
        "a deferred body must not be consulted again until its stream is resumed"
    );

    // Resuming puts the DATA item back on the queue, and only then is the body asked.
    ready.store(1, AtomicOrdering::SeqCst);
    client.resume_body(stream).expect("resuming the stream");

    let wire = drain(&mut client, &mut ());
    assert!(
        fills.load(AtomicOrdering::SeqCst) > after_first,
        "resuming should let the body be consulted again"
    );
    let data: Vec<_> = parse_frames(&wire)
        .into_iter()
        .filter(|(kind, _, _)| *kind == 0)
        .collect();
    assert!(!data.is_empty(), "the resumed body should produce DATA");
    assert!(
        data.iter().any(|(_, flags, _)| flags & 0x01 != 0),
        "the resumed body should end its stream"
    );
}

#[test]
fn resuming_a_stream_with_nothing_deferred_is_reported_as_invalid_input() {
    // The stale-notification case. An asynchronous body may signal readiness just as its
    // stream is reset, so callers need this to be a typed, benign outcome rather than a
    // fault they cannot distinguish.
    let mut client = SessionBuilder::<()>::client().build().unwrap();

    let error = client
        .resume_body(StreamId::new(99))
        .expect_err("no such stream");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    let stream = client
        .submit_request(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/"),
        ])
        .expect("submitting");

    // The stream exists but nothing is deferred on it, which libnghttp2 reports the same
    // way — so callers cannot tell the two apart, and must not need to.
    let error = client.resume_body(stream).expect_err("nothing deferred");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}
