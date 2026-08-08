//! Cancellation, teardown, and telling failures apart.
//!
//! Everything here is about a connection *ending* — on purpose, or not. The properties are
//! easy to state and easy to get subtly wrong: a caller who walks away must stop the peer
//! working, a wind-down must let what is in flight finish, and a caller who has to decide
//! whether to retry must be able to decide it from the error alone.
//!
//! Everything runs on one task, as elsewhere in this suite.

#![cfg(feature = "http")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ngnet_h2::http::testing::bytes_crate::{Bytes, BytesMut};
use ngnet_h2::http::testing::{
    Empty, alongside, block_on, duplex, failing, http_crate as http, serve as drive_peer,
};
use ngnet_h2::http::{
    Cancelled, ErrorKind, IncomingBody, Transport, TransportRead, TransportWrite, server,
};
use ngnet_h2::{
    BodyOutcome, BodySource, ErrorCode, FrameType, Header, HeaderAction, HeaderCategory, Session,
    SessionBuilder, StreamId,
};

// ---------------------------------------------------------------------------
// The peer server
// ---------------------------------------------------------------------------

/// What the peer observed, and what it still owes.
#[derive(Debug, Default)]
struct Peer {
    paths: BTreeMap<i32, String>,
    pending: Vec<i32>,
    /// Streams that closed, with the code they closed under.
    closed: BTreeMap<i32, u32>,
}

/// A response body that never produces anything and never ends.
struct NeverEnds;

impl BodySource for NeverEnds {
    fn fill(&mut self, _buf: &mut [u8]) -> BodyOutcome {
        BodyOutcome::Defer
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
        .on_stream_close(|peer: &mut Peer, stream, code, _failure| {
            peer.closed.insert(stream.get(), code.get());
        })
        .build()
        .expect("building the peer session")
}

/// Answers every outstanding request with a body that never ends, holding the stream open.
fn answer_and_stall(session: &mut Session<Peer>, peer: &mut Peer) {
    for stream in core::mem::take(&mut peer.pending) {
        session
            .submit_response_with_body(
                StreamId::new(stream),
                &[Header::new(":status", "200")],
                NeverEnds,
            )
            .expect("submitting a response");
    }
}

/// Answers every outstanding request plainly, ending the stream at once.
fn answer_plainly(session: &mut Session<Peer>, peer: &mut Peer) {
    for stream in core::mem::take(&mut peer.pending) {
        session
            .submit_response(StreamId::new(stream), &[Header::new(":status", "200")])
            .expect("submitting a response");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn request(path: &str) -> http::Request<Empty> {
    http::Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(Empty)
        .expect("building a request")
}

async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(move |cx| {
        if yielded {
            core::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[test]
fn dropping_a_pending_request_resets_its_stream() {
    // Spec SC-011, first half, asserted from the peer's side — which is the only side that
    // can tell the difference between "cancelled" and "forgotten about".
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let response = requests.send_request(request("/abandoned"));
        // Let the request reach the peer, which never answers — so nothing but the drop
        // below can close this stream.
        for _ in 0..16 {
            yield_now().await;
        }
        drop(response);
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
    };

    block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, |_session, _peer| {}),
    ));

    assert_eq!(
        peer.closed.get(&1).copied(),
        Some(ErrorCode::CANCEL.get()),
        "the peer did not see the abandoned request cancelled",
    );
}

#[test]
fn dropping_an_unread_response_body_resets_its_stream() {
    // Spec SC-011, second half. Returning the window without stopping the peer would
    // invite it to send the rest of something nobody will ever read.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let response = requests
            .send_request(request("/streaming"))
            .await
            .expect("a response");
        // The head has arrived; the body never will, because the peer's body never ends.
        drop(response);
        for _ in 0..16 {
            yield_now().await;
        }
        drop(requests);
    };

    block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, answer_and_stall),
    ));

    assert_eq!(
        peer.closed.get(&1).copied(),
        Some(ErrorCode::CANCEL.get()),
        "the peer went on sending a body that had been dropped",
    );
}

#[test]
fn a_completed_response_body_is_not_reset_when_dropped() {
    // The other side of the rule. A stream that already ended has nothing left to stop,
    // and resetting it would tell the peer something went wrong when nothing did.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let response = requests
            .send_request(request("/complete"))
            .await
            .expect("a response");
        for _ in 0..8 {
            yield_now().await;
        }
        drop(response);
        for _ in 0..8 {
            yield_now().await;
        }
        drop(requests);
    };

    block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(
        peer.closed.get(&1).copied(),
        Some(ErrorCode::NO_ERROR.get()),
        "a response that had already ended was reset on drop",
    );
}

#[test]
fn a_servers_unread_request_body_is_not_reset_and_its_answer_still_arrives() {
    // Spec SC-011's exception. Dropping a *request* body says nothing about wanting the
    // exchange to stop: a handler that ignores the body is entitled to answer anyway, and
    // resetting would destroy the response it is about to send.
    let (server_side, client_side) = duplex();
    let connection = server::serve(server_side, |request: http::Request<IncomingBody>| {
        // Dropped, unread, immediately.
        drop(request.into_body());
        async move {
            http::Response::builder()
                .status(200)
                .header("x-answered", "yes")
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut client = ClientPeer::default();
    client.outgoing.push("/ignored".to_owned());

    let driving = async {
        for _ in 0..32 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, client_peer_session(), &mut client, ask),
    ));

    assert_eq!(
        client.status(1),
        Some("200"),
        "a handler that ignored its request body lost its response",
    );
    assert_eq!(client.head(1, "x-answered"), Some("yes"));
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

#[test]
fn a_graceful_shutdown_finishes_what_is_in_flight_and_refuses_what_is_not() {
    // Spec SC-024. The distinction that matters: in-flight work is not collateral damage,
    // and what is turned away is turned away in a way the caller can act on.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let first = requests.send_request(request("/in-flight"));
        requests.shutdown();

        let later = requests.send_request(request("/too-late"));
        let refused = later.await.expect_err("a request after shutdown");

        let answered = first.await;
        drop(requests);
        (answered, refused)
    };

    let (answered, refused) = block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(
        answered.expect("the in-flight request").status(),
        http::StatusCode::OK,
        "a shutdown cancelled work that was already under way",
    );
    assert_eq!(refused.kind(), ErrorKind::Refused);
    assert!(
        refused.is_retriable(),
        "a request that was never begun was not reported as retriable",
    );
    assert_eq!(peer.paths.len(), 1, "the refused request reached the peer");
}

#[test]
fn a_peer_going_away_refuses_the_streams_it_never_began() {
    // Spec SC-014. A `GOAWAY` names the last stream the peer looked at; everything above it
    // was never begun, which is the one failure safe to retry without knowing anything
    // about the request.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let handle = requests.clone();
    let exchange = async {
        let first = requests.send_request(request("/kept"));
        let second = requests.send_request(request("/abandoned"));
        for _ in 0..32 {
            yield_now().await;
        }
        let outcome = second.await;
        drop((first, requests));
        outcome
    };

    // Waits until both requests have arrived, answers the first, and says it will do
    // nothing above it. Triggered by what it has received rather than by a pass count: the
    // peer harness steps until it has nothing more to send, so passes are not exchanges.
    let mut gone = false;
    let step = move |session: &mut Session<Peer>, peer: &mut Peer| {
        if gone || peer.pending.len() < 2 {
            return;
        }
        let honoured = peer.pending[0];
        peer.pending.clear();
        session
            .submit_response_with_body(
                StreamId::new(honoured),
                &[Header::new(":status", "200")],
                NeverEnds,
            )
            .expect("submitting a response");
        session
            .shutdown(StreamId::new(honoured), ErrorCode::NO_ERROR)
            .expect("going away");
        gone = true;
    };

    let outcome = block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, step),
    ));

    let refused = outcome.expect_err("a stream the peer never began");
    assert_eq!(refused.kind(), ErrorKind::Refused);
    assert!(refused.is_retriable());
    assert_eq!(
        refused.reason(),
        Some(ErrorCode::NO_ERROR),
        "the peer's own reason was not carried",
    );
    assert!(
        handle.is_refusing(),
        "the handle went on accepting requests after the peer left",
    );
}

// ---------------------------------------------------------------------------
// Telling failures apart
// ---------------------------------------------------------------------------

#[test]
fn a_peer_reset_carries_the_peers_reason() {
    // Spec SC-013. "The peer refused this" and "we could not send it" are different
    // things to a caller, and the reason code is what separates them.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let response = requests.send_request(request("/refused"));
        let outcome = response.await;
        drop(requests);
        outcome
    };

    let step = |session: &mut Session<Peer>, peer: &mut Peer| {
        for stream in core::mem::take(&mut peer.pending) {
            session
                .reset_stream(StreamId::new(stream), ErrorCode::REFUSED_STREAM)
                .expect("resetting");
        }
    };

    let outcome = block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, step),
    ));

    let error = outcome.expect_err("a stream the peer reset");
    assert_eq!(error.kind(), ErrorKind::Stream);
    assert_eq!(error.reason(), Some(ErrorCode::REFUSED_STREAM));
    assert!(
        !error.is_retriable(),
        "a reset stream was reported as blindly retriable",
    );
    assert!(
        error.to_string().contains("REFUSED_STREAM"),
        "the reason did not reach the message: {error}",
    );
}

#[test]
fn a_transport_failure_is_identifiable_as_one() {
    // Spec SC-026. A caller deciding whether to reconnect needs to know the socket broke
    // rather than the peer having said something invalid.
    // The budget differs by direction because the two are not symmetric: a client
    // coalesces its preface, settings and first request into a single write, so a second
    // write only ever happens if there is something further to send. Two reads, one write.
    for (on_read, after) in [(true, 2), (false, 1)] {
        let direction = if on_read { "reading" } else { "writing" };
        let (client_side, server_side) = failing(after, on_read);
        let (requests, connection) =
            ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

        let mut peer = Peer::default();
        // The *connection* is what reports a broken transport. The request future only
        // ever learns that its connection went away, which is true of every failure and so
        // distinguishes nothing — asserting there would have been an assertion about
        // nothing at all.
        let exchange = async {
            let outcome = requests.send_request(request("/doomed")).await;
            drop(requests);
            outcome
        };

        let outcome = block_on(alongside(
            alongside(connection, exchange),
            drive_peer(server_side, peer_session(), &mut peer, answer_plainly),
        ));

        let error = outcome.expect_err("a transport that broke");
        assert_eq!(
            error.kind(),
            ErrorKind::Transport,
            "a transport that broke while {direction} reported something else: {error}",
        );
        let cause = std::error::Error::source(&error).expect("the underlying failure");
        assert!(
            cause.to_string().contains("scripted transport"),
            "the transport's own error was lost while {direction}: {cause}",
        );
    }
}

#[test]
fn end_of_file_part_way_through_a_frame_is_a_connection_error() {
    // Spec SC-030. A truncated frame is not a clean close: treating it as one would let a
    // connection cut mid-message look like a connection that finished.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let response = requests.send_request(request("/truncated"));

    // A frame header claiming an eight-octet payload, followed by two octets and then
    // nothing. Cut inside the payload rather than between frames, which is the case a
    // clean-close check cannot tell apart on its own.
    let peer = async {
        let (mut reader, mut writer) = server_side.split();
        // Read what the client sends first, so its preface does not sit unread.
        let (_result, _buf) = reader.read(BytesMut::with_capacity(4096)).await;
        let truncated = Bytes::from_static(&[
            0x00, 0x00, 0x08, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        ]);
        let (_result, _buf) = writer.write(truncated).await;
        drop(writer);
        core::future::pending::<()>().await;
    };

    let outcome = block_on(alongside(
        alongside(connection, async {
            let _ = response.await;
            drop(requests);
        }),
        peer,
    ));

    let error = outcome.expect_err("a frame the peer cut in half");
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert!(
        error.to_string().contains("part-way"),
        "the truncation was not named: {error}",
    );
}

// ---------------------------------------------------------------------------
// The server's loss signal
// ---------------------------------------------------------------------------

#[test]
fn a_handler_learns_its_stream_was_lost_even_with_no_body_to_read() {
    // The gap the request body cannot cover. A bodyless request that is reset leaves the
    // body with nothing to report — it ended legitimately — so without a separate signal a
    // handler would go on working for a peer that had left.
    let observed = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&observed);

    let (server_side, client_side) = duplex();
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let counter = Arc::clone(&counter);
        let lost = request.extensions().get::<Cancelled>().cloned();
        async move {
            // Awaited rather than checked first: the reset may already have arrived in the
            // same read as the request, and either way the handler learns.
            let lost = lost.expect("every request carries the signal");
            lost.cancelled().await;
            counter.fetch_add(1, Ordering::AcqRel);
            http::Response::builder()
                .status(200)
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut client = ClientPeer::default();
    client.outgoing.push("/doomed".to_owned());

    let driving = async {
        for _ in 0..40 {
            yield_now().await;
        }
    };

    // Reset once the request has actually gone out, rather than after a fixed number of
    // passes: the peer harness steps until it has nothing more to send, so passes are not
    // exchanges and counting them is how this test would come to pass by accident.
    let mut sent = false;
    let mut reset = false;
    let step = move |session: &mut Session<ClientPeer>, peer: &mut ClientPeer| {
        // On the pass *after* the request went out, not the same one: a reset submitted
        // alongside the headers cancels them before they are ever serialised, and the
        // server would never see the request at all.
        if sent && !reset {
            reset = true;
            session
                .reset_stream(StreamId::new(1), ErrorCode::CANCEL)
                .expect("resetting");
        }
        let had_work = !peer.outgoing.is_empty();
        ask(session, peer);
        sent |= had_work;
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, client_peer_session(), &mut client, step),
    ));

    assert_eq!(
        observed.load(Ordering::Acquire),
        1,
        "the handler was never told its stream had gone",
    );
    assert!(
        !client.heads.contains_key(&1),
        "a response was sent on a stream the peer had reset",
    );
}

#[test]
fn a_request_dropped_before_it_is_sent_never_reaches_the_peer() {
    // A response future dropped before the driver has run has no stream to reset, and
    // sending the request only to take it back would be work the peer has to do for
    // nothing. It is simply never sent.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        // Dropped before anything is polled, so the driver has not seen it.
        drop(requests.send_request(request("/never-sent")));
        let kept = requests.send_request(request("/sent"));
        kept.await.expect("a response");
        drop(requests);
    };

    block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(
        peer.paths.values().cloned().collect::<Vec<_>>(),
        ["/sent"],
        "a request nobody was waiting for was sent anyway",
    );
}

#[test]
fn a_client_going_away_does_not_discard_the_responses_a_server_owes_it() {
    // A `GOAWAY` names the last stream *its sender* acted on, so what it abandons is the
    // work the receiver started. A client's ordinary wind-down names zero, and a server
    // reading that as "discard everything in flight" would drop responses its peer is
    // still waiting for — including for this crate talking to itself.
    let (server_side, client_side) = duplex();
    let connection = server::serve(server_side, |request: http::Request<IncomingBody>| {
        let lost = request.extensions().get::<Cancelled>().cloned();
        async move {
            assert!(
                !lost.expect("the signal").is_cancelled(),
                "a client's wind-down cancelled a handler that was still wanted",
            );
            http::Response::builder()
                .status(200)
                .header("x-answered", "yes")
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut client = ClientPeer::default();
    client.outgoing.push("/in-flight".to_owned());

    let driving = async {
        for _ in 0..32 {
            yield_now().await;
        }
    };

    // Says it is going away the moment the request is out, naming stream zero as a client
    // that accepts no pushes must.
    let mut sent = false;
    let mut gone = false;
    let step = move |session: &mut Session<ClientPeer>, peer: &mut ClientPeer| {
        if sent && !gone {
            gone = true;
            session
                .shutdown(StreamId::new(0), ErrorCode::NO_ERROR)
                .expect("going away");
        }
        let had_work = !peer.outgoing.is_empty();
        ask(session, peer);
        sent |= had_work;
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, client_peer_session(), &mut client, step),
    ));

    assert_eq!(
        client.status(1),
        Some("200"),
        "the server discarded a response its client was still waiting for",
    );
    assert_eq!(client.head(1, "x-answered"), Some("yes"));
}

#[test]
fn shutting_down_twice_is_no_worse_than_once() {
    // Documented as idempotent, so it has to be.
    let (client_side, server_side) = duplex();
    let (requests, connection) =
        ngnet_h2::http::handshake::<_, Empty>(client_side).expect("handshake");

    let mut peer = Peer::default();

    let exchange = async {
        let first = requests.send_request(request("/in-flight"));
        requests.shutdown();
        requests.shutdown();
        requests.shutdown();
        let answered = first.await;
        drop(requests);
        answered
    };

    let answered = block_on(alongside(
        alongside(exchange, connection),
        drive_peer(server_side, peer_session(), &mut peer, answer_plainly),
    ));

    assert_eq!(
        answered.expect("the in-flight request").status(),
        http::StatusCode::OK,
    );
}

// ---------------------------------------------------------------------------
// A peer that is a client, for the server-side tests
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ClientPeer {
    outgoing: Vec<String>,
    heads: BTreeMap<i32, Vec<(String, String)>>,
    opening: BTreeMap<i32, Vec<(String, String)>>,
}

impl ClientPeer {
    fn head(&self, stream: i32, name: &str) -> Option<&str> {
        self.heads
            .get(&stream)?
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }

    fn status(&self, stream: i32) -> Option<&str> {
        self.head(stream, ":status")
    }
}

fn client_peer_session() -> Session<ClientPeer> {
    SessionBuilder::<ClientPeer>::client()
        .on_begin_headers(|peer: &mut ClientPeer, frame| {
            if frame.category() == Some(HeaderCategory::Response) {
                peer.opening.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|peer: &mut ClientPeer, frame, name: &[u8], value: &[u8]| {
            if let Some(fields) = peer.opening.get_mut(&frame.stream_id().get()) {
                fields.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
            }
            HeaderAction::Continue
        })
        .on_frame(|peer: &mut ClientPeer, frame| {
            if frame.kind() == FrameType::HEADERS
                && frame.is_end_headers()
                && let Some(fields) = peer.opening.remove(&frame.stream_id().get())
            {
                peer.heads.insert(frame.stream_id().get(), fields);
            }
        })
        .build()
        .expect("building the peer session")
}

fn ask(session: &mut Session<ClientPeer>, peer: &mut ClientPeer) {
    for path in core::mem::take(&mut peer.outgoing) {
        session
            .submit_request(&[
                Header::new(":method", "GET"),
                Header::new(":scheme", "http"),
                Header::new(":authority", "example.test"),
                Header::new(":path", &path),
            ])
            .expect("submitting");
    }
}
