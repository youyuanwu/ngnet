//! Receiving a message body: zero copy, and backpressure driven by consumption.
//!
//! Two properties are asserted here that cannot be seen from anywhere else. The first is
//! that a delivered chunk *is* the driver's read buffer rather than a copy of it, and
//! stays valid for as long as the caller holds it — checked by address, because that is
//! what the claim means. The second is that the peer is credited when the application
//! reads, not when octets arrive, which is the whole of this crate's backpressure.
//!
//! Everything runs on one task, as elsewhere in this suite: no runtime, no spawning.

#![cfg(feature = "http")]

use core::pin::Pin;
use core::task::Poll;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nghttp2::http::testing::{
    BufferLog, Empty, alongside, block_on, bytes_crate as bytes, duplex,
    http_body_crate as http_body, http_crate as http, serve,
};
use nghttp2::http::{Error, IncomingBody};
use nghttp2::{
    BodyOutcome, BodySource, FrameType, Header, HeaderAction, HeaderCategory, Session,
    SessionBuilder, StreamId,
};

use bytes::Bytes;
use http_body::{Body, Frame};

/// The receive window HTTP/2 gives a stream and a connection until either side says
/// otherwise. Nothing configures it here, so it is what bounds an unread body.
const DEFAULT_WINDOW: usize = 65_535;

// ---------------------------------------------------------------------------
// The peer
// ---------------------------------------------------------------------------

/// What the peer server has been asked for, and what it is still to answer.
#[derive(Debug, Default)]
struct Peer {
    paths: BTreeMap<i32, String>,
    pending: Vec<i32>,
    /// Streams answered with a body that promised trailers.
    trailing: Vec<i32>,
}

/// A response body of known content, reporting how much the session has taken.
///
/// The count is what makes "the peer stalled" observable: flow control stops libnghttp2
/// asking for octets, so a body that is never asked never advances.
struct Canned {
    data: Arc<Vec<u8>>,
    offset: usize,
    sent: Arc<AtomicUsize>,
    trailers: bool,
}

impl BodySource for Canned {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        let take = buf.len().min(self.data.len() - self.offset);
        buf[..take].copy_from_slice(&self.data[self.offset..self.offset + take]);
        self.offset += take;
        self.sent.fetch_add(take, Ordering::AcqRel);

        if self.offset < self.data.len() {
            BodyOutcome::Wrote(take)
        } else if self.trailers {
            BodyOutcome::EofWithTrailers(take)
        } else {
            BodyOutcome::Eof(take)
        }
    }
}

fn peer_session() -> Session<Peer> {
    SessionBuilder::<Peer>::server()
        .on_header(|peer: &mut Peer, frame, name: &[u8], value: &[u8]| {
            if name == b":path" {
                peer.paths.insert(
                    frame.stream_id().get(),
                    String::from_utf8_lossy(value).into_owned(),
                );
            }
            HeaderAction::Continue
        })
        .on_frame(|peer: &mut Peer, frame| {
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && frame.category() == Some(HeaderCategory::Request)
            {
                peer.pending.push(frame.stream_id().get());
            }
        })
        .build()
        .expect("building the peer session")
}

/// What the peer answers each path with.
struct Answers {
    bodies: BTreeMap<String, Arc<Vec<u8>>>,
    trailers: bool,
    sent: Arc<AtomicUsize>,
}

impl Answers {
    fn one(path: &str, payload: Vec<u8>) -> Self {
        Self {
            bodies: [(path.to_owned(), Arc::new(payload))].into_iter().collect(),
            trailers: false,
            sent: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_trailers(mut self) -> Self {
        self.trailers = true;
        self
    }

    fn and(mut self, path: &str, payload: Vec<u8>) -> Self {
        self.bodies.insert(path.to_owned(), Arc::new(payload));
        self
    }

    /// One pass of the peer: answer what is outstanding, then trail what is ready.
    fn step(&self, session: &mut Session<Peer>, peer: &mut Peer) {
        for stream in core::mem::take(&mut peer.pending) {
            let path = peer.paths.get(&stream).cloned().unwrap_or_default();
            let data = Arc::clone(
                self.bodies
                    .get(&path)
                    .unwrap_or_else(|| panic!("the test scripted no answer for {path}")),
            );
            session
                .submit_response_with_body(
                    StreamId::new(stream),
                    &[Header::new(":status", "200"), Header::new("x-path", &path)],
                    Canned {
                        data,
                        offset: 0,
                        sent: Arc::clone(&self.sent),
                        trailers: self.trailers,
                    },
                )
                .expect("submitting a response");
            if self.trailers {
                peer.trailing.push(stream);
            }
        }

        let ready: Vec<i32> = peer
            .trailing
            .iter()
            .copied()
            .filter(|stream| session.trailers_ready(StreamId::new(*stream)))
            .collect();
        for stream in ready {
            peer.trailing.retain(|held| *held != stream);
            session
                .submit_trailer(
                    StreamId::new(stream),
                    &[Header::new("x-checksum", "deadbeef")],
                )
                .expect("submitting trailers");
        }
    }
}

// ---------------------------------------------------------------------------
// Client-side helpers
// ---------------------------------------------------------------------------

fn request(path: &str) -> http::Request<Empty> {
    http::Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(Empty)
        .expect("building a request")
}

/// The next frame of a received body.
///
/// `http_body` deliberately ships no combinators, and this crate takes no
/// dev-dependencies, so the poll is written out.
async fn next_frame(body: &mut IncomingBody) -> Option<Result<Frame<Bytes>, Error>> {
    core::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

/// Yields once, so everything else on the task gets a full poll.
async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// A payload with no repeating structure, so a misplaced chunk shows up as a mismatch.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Reads a whole body, checking every chunk against `log` as it goes.
async fn drain(
    body: &mut IncomingBody,
    log: Option<&BufferLog>,
) -> (Vec<u8>, Option<http::HeaderMap>) {
    let mut received = Vec::new();
    let mut trailers = None;

    while let Some(frame) = next_frame(body).await {
        let frame = frame.expect("a body frame");
        if let Some(data) = frame.data_ref() {
            if let Some(log) = log {
                assert!(
                    log.holds(data),
                    "a delivered chunk was a copy, not a view of the read buffer",
                );
            }
            received.extend_from_slice(data);
        } else if let Ok(map) = frame.into_trailers() {
            trailers = Some(map);
        }
    }

    (received, trailers)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn a_delivered_chunk_is_a_view_of_the_drivers_read_buffer() {
    // Spec SC-016. Asserted by address: "no copy" is a claim about where the octets are,
    // and comparing contents would pass just as happily against a memcpy.
    let expected = payload(40_000);
    let answers = Answers::one("/zero-copy", expected.clone());

    let (client_side, server_side) = duplex(false);
    let log = client_side.buffer_log();
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/zero-copy"));

    let exchange = async {
        let mut body = response.await.expect("a response").into_body();
        let (received, _) = drain(&mut body, Some(&log)).await;
        drop(requests);
        received
    };

    let received = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert_eq!(received, expected);
    assert!(log.reads() > 1, "the payload arrived in a single read");
}

#[test]
fn a_retained_chunk_outlives_the_reads_that_follow_it() {
    // The other half of SC-016. A view is only useful if it stays valid, so the buffer it
    // came from must stay out of the driver's pool while the caller holds it — otherwise
    // a later read would quietly rewrite octets someone is still looking at.
    let expected = payload(200_000);
    let answers = Answers::one("/retain", expected.clone());

    let (client_side, server_side) = duplex(false);
    let log = client_side.buffer_log();
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/retain"));

    let exchange = async {
        let mut body = response.await.expect("a response").into_body();

        // Hold the first chunk, and note how many reads had happened when it was taken.
        let held = loop {
            let frame = next_frame(&mut body).await.expect("a first frame");
            if let Ok(data) = frame.expect("a body frame").into_data() {
                break data;
            }
        };
        let mark = log.reads();

        let (rest, _) = drain(&mut body, None).await;
        drop(requests);
        (held, mark, rest)
    };

    let (held, mark, rest) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert!(
        log.reads() > mark + 4,
        "not enough reads followed for the buffer to have been reused",
    );
    assert!(
        log.reuses() > 0,
        "no read buffer was ever recycled, so nothing was held back from anything",
    );
    assert!(
        !log.overwrote(&held, mark),
        "a later read wrote over a chunk the caller was still holding",
    );
    assert_eq!(
        held.as_ref(),
        &expected[..held.len()],
        "a retained chunk changed underneath its holder",
    );

    let mut whole = held.to_vec();
    whole.extend_from_slice(&rest);
    assert_eq!(whole, expected);
}

#[test]
fn a_four_hundred_kilobyte_body_arrives_intact() {
    // Spec SC-007. Large enough to cross every boundary that matters: many DATA frames,
    // many reads, many window updates, and more than one refill of the read pool.
    let expected = payload(400 * 1024);
    let answers = Answers::one("/large", expected.clone());

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/large"));

    let exchange = async {
        let mut body = response.await.expect("a response").into_body();
        let (received, _) = drain(&mut body, None).await;
        drop(requests);
        received
    };

    let received = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert_eq!(received.len(), expected.len());
    assert_eq!(received, expected);
}

#[test]
fn an_unread_body_stalls_the_peer_and_resumes_once_it_is_read() {
    // Spec SC-009. This is the whole of the backpressure claim: capacity is returned when
    // the application reads, so a body nobody reads closes the window and the peer stops.
    let expected = payload(300_000);
    let answers = Answers::one("/backpressure", expected.clone());
    let sent = Arc::clone(&answers.sent);

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/backpressure"));

    let exchange = async {
        let mut body = response.await.expect("a response").into_body();

        // Deliberately read nothing for a while. The peer has an entire payload ready and
        // a transport that never blocks, so anything it does not send is flow control.
        for _ in 0..64 {
            yield_now().await;
        }
        let stalled = sent.load(Ordering::Acquire);

        let (received, _) = drain(&mut body, None).await;
        drop(requests);
        (stalled, received)
    };

    let (stalled, received) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert!(
        stalled <= DEFAULT_WINDOW,
        "the peer sent {stalled} octets into a {DEFAULT_WINDOW}-octet window",
    );
    assert!(stalled > 0, "the peer sent nothing at all");
    assert_eq!(received, expected, "reading did not let the rest through");
}

#[test]
fn an_unread_body_does_not_hold_up_another_stream() {
    // Spec SC-031. Flow control is per stream as well as per connection, so one caller
    // ignoring its body must not become every other caller's problem.
    let ignored = payload(20_000);
    let wanted = payload(4_000);
    let answers = Answers::one("/ignored", ignored).and("/wanted", wanted.clone());

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let first = requests.send_request(request("/ignored"));
    let second = requests.send_request(request("/wanted"));

    let exchange = async {
        // Held, never read. Dropping it would return the window and prove nothing.
        let neglected = first.await.expect("a response").into_body();

        let mut body = second.await.expect("a response").into_body();
        let (received, _) = drain(&mut body, None).await;

        drop((neglected, requests));
        received
    };

    let received = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert_eq!(received, wanted);
}

#[test]
fn trailers_arrive_as_trailers_and_not_as_headers() {
    // Spec SC-004. A trailing block is part of the body, not part of the head — folding
    // it into the response headers would silently rewrite a message that had already been
    // handed to the caller.
    let expected = payload(5_000);
    let answers = Answers::one("/trailers", expected.clone()).with_trailers();

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/trailers"));

    let exchange = async {
        let response = response.await.expect("a response");
        let head_fields = response.headers().clone();
        let mut body = response.into_body();
        let (received, trailers) = drain(&mut body, None).await;
        drop(requests);
        (head_fields, received, trailers)
    };

    let (head_fields, received, trailers) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert_eq!(received, expected);
    assert!(
        head_fields.get("x-checksum").is_none(),
        "a trailer was reported as a response header",
    );
    let trailers = trailers.expect("a trailers frame");
    assert_eq!(
        trailers.get("x-checksum").map(http::HeaderValue::as_bytes),
        Some(b"deadbeef".as_slice()),
    );
}

#[test]
fn the_head_is_readable_while_the_body_is_still_arriving() {
    // Spec SC-003. The response future resolves on the header block, not on the end of
    // the message, which is what makes a streaming response usable at all.
    let expected = payload(120_000);
    let answers = Answers::one("/streaming", expected.clone());

    let (client_side, server_side) = duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();
    let response = requests.send_request(request("/streaming"));

    let exchange = async {
        let response = response.await.expect("a response");
        let status = response.status();
        let path = response
            .headers()
            .get("x-path")
            .map(|value| value.as_bytes().to_vec());

        // One chunk only. The rest of the message is still in flight, and the head was
        // readable before any of it was.
        let mut body = response.into_body();
        let first = loop {
            let frame = next_frame(&mut body).await.expect("a first frame");
            if let Ok(data) = frame.expect("a body frame").into_data() {
                break data;
            }
        };

        let complete = body.is_end_stream();
        drop((body, requests));
        (status, path, first, complete)
    };

    let (status, path, first, complete) = block_on(alongside(
        alongside(exchange, connection),
        serve(server_side, peer_session(), &mut peer, |session, peer| {
            answers.step(session, peer);
        }),
    ));

    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(path.as_deref(), Some(b"/streaming".as_slice()));
    assert!(!first.is_empty());
    assert_eq!(first.as_ref(), &expected[..first.len()]);
    assert!(
        !complete,
        "the whole body had arrived, so nothing was proven about arriving early",
    );
}
