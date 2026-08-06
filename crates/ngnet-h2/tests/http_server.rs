//! Serving requests: many handlers on one connection, none spawned.
//!
//! The client tests prove a connection can ask. These prove it can answer, and that
//! answering many things at once needs no runtime — the handlers are futures the driver
//! holds, and the concurrency comes from each having its own waker rather than from
//! anything being spawned.
//!
//! Everything runs on one task, as elsewhere in this suite.

#![cfg(feature = "http")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ngnet_h2::http::testing::{
    Empty, Full, alongside, block_on, duplex, http_crate as http, scripted, serve as drive_peer,
};
use ngnet_h2::http::{IncomingBody, server};
use ngnet_h2::{
    BodyOutcome, BodySource, ErrorCode, FrameType, Header, HeaderAction, HeaderCategory, Session,
    SessionBuilder, StreamId,
};

// ---------------------------------------------------------------------------
// The peer client
// ---------------------------------------------------------------------------

/// What the peer client observed of each response.
#[derive(Debug, Default)]
struct Peer {
    /// Streams still to be opened, as method/path/payload.
    outgoing: Vec<(&'static str, String, Option<Vec<u8>>)>,
    /// Response fields received, by stream.
    heads: BTreeMap<i32, Vec<(String, String)>>,
    /// Blocks still arriving, by stream.
    opening: BTreeMap<i32, Vec<(String, String)>>,
    /// Response payload, by stream.
    bodies: BTreeMap<i32, Vec<u8>>,
    /// Trailing header blocks received, by stream.
    trailers: BTreeMap<i32, Vec<(String, String)>>,
    /// Streams whose trailers arrived only after some data had, the ordering the wire
    /// requires.
    trailers_after_data: std::collections::BTreeSet<i32>,
    /// Streams that closed, with the code they closed under.
    closed: BTreeMap<i32, u32>,
}

impl Peer {
    fn head(&self, stream: i32, name: &str) -> Option<&str> {
        self.heads
            .get(&stream)?
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A request body of known content.
struct Canned {
    data: Vec<u8>,
    offset: usize,
}

impl BodySource for Canned {
    fn fill(&mut self, buf: &mut [u8]) -> BodyOutcome {
        let take = buf.len().min(self.data.len() - self.offset);
        buf[..take].copy_from_slice(&self.data[self.offset..self.offset + take]);
        self.offset += take;
        if self.offset < self.data.len() {
            BodyOutcome::Wrote(take)
        } else {
            BodyOutcome::Eof(take)
        }
    }
}

/// A request body that never produces anything and never ends.
///
/// Holds a request open under its handler, which is what makes "the peer reset it
/// part-way" a thing that can happen at all.
struct NeverEnds;

impl BodySource for NeverEnds {
    fn fill(&mut self, _buf: &mut [u8]) -> BodyOutcome {
        BodyOutcome::Defer
    }
}

fn peer_session() -> Session<Peer> {
    SessionBuilder::<Peer>::client()
        .on_begin_headers(|peer: &mut Peer, frame| {
            if frame.category() == Some(HeaderCategory::Response) || frame.is_trailers() {
                peer.opening.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|peer: &mut Peer, frame, name: &[u8], value: &[u8]| {
            if let Some(fields) = peer.opening.get_mut(&frame.stream_id().get()) {
                fields.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
            }
            HeaderAction::Continue
        })
        .on_frame(|peer: &mut Peer, frame| {
            if frame.kind() == FrameType::HEADERS && frame.is_end_headers() {
                if let Some(fields) = peer.opening.remove(&frame.stream_id().get()) {
                    let stream = frame.stream_id().get();
                    if frame.is_trailers() {
                        // Witnessed before the block is stored, so "trailers followed data"
                        // is read off what had already arrived rather than off arrival
                        // order alone.
                        if peer
                            .bodies
                            .get(&stream)
                            .is_some_and(|body| !body.is_empty())
                        {
                            peer.trailers_after_data.insert(stream);
                        }
                        peer.trailers.insert(stream, fields);
                    } else {
                        peer.heads.insert(stream, fields);
                    }
                }
            }
        })
        .on_data_chunk(|peer: &mut Peer, stream, chunk: &[u8]| {
            peer.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_stream_close(|peer: &mut Peer, stream, code, _failure| {
            peer.closed.insert(stream.get(), code.get());
        })
        .build()
        .expect("building the peer session")
}

/// Opens whatever requests are queued, one per pass.
fn ask(session: &mut Session<Peer>, peer: &mut Peer) {
    for (method, path, payload) in core::mem::take(&mut peer.outgoing) {
        let target = format!("http://example.test{path}");
        let fields = [
            Header::new(":method", method),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", &path),
            Header::new("x-asked", &target),
        ];
        match payload {
            None => session.submit_request(&fields).expect("submitting"),
            // An empty payload means "a body that never ends", which is how a request is
            // held open long enough for the peer to change its mind about it.
            Some(data) if data.is_empty() => session
                .submit_request_with_body(&fields, NeverEnds)
                .expect("submitting"),
            Some(data) => session
                .submit_request_with_body(&fields, Canned { data, offset: 0 })
                .expect("submitting"),
        };
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Yields once, so everything else on the task gets a full poll.
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

/// Reads a whole request body inside a handler.
async fn drain(mut body: IncomingBody) -> Result<Vec<u8>, ngnet_h2::http::Error> {
    use http_body::Body as _;

    let mut received = Vec::new();
    while let Some(frame) =
        core::future::poll_fn(|cx| core::pin::Pin::new(&mut body).poll_frame(cx)).await
    {
        if let Some(data) = frame?.data_ref() {
            received.extend_from_slice(data);
        }
    }
    Ok(received)
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn a_handler_sees_the_whole_request_head() {
    // The request line HTTP/2 splits across four pseudo-headers has to come back together
    // as the method and URI an `http::Request` carries, or a handler cannot route at all.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        recorder.lock().expect("record").push((
            request.method().clone(),
            request.uri().clone(),
            request
                .headers()
                .get("x-asked")
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned()),
        ));
        async move {
            http::Response::builder()
                .status(http::StatusCode::CREATED)
                .header("x-answered", "yes")
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing
        .push(("PUT", "/things/7?q=1".to_owned(), None));

    let driving = async {
        for _ in 0..24 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    let seen = seen.lock().expect("record");
    let (method, uri, asked) = seen.first().expect("the handler ran");
    assert_eq!(method, http::Method::PUT);
    assert_eq!(uri.path(), "/things/7");
    assert_eq!(uri.query(), Some("q=1"));
    assert_eq!(
        uri.authority().map(http::uri::Authority::as_str),
        Some("example.test")
    );
    assert_eq!(uri.scheme_str(), Some("http"));
    assert_eq!(asked.as_deref(), Some("http://example.test/things/7?q=1"));

    assert_eq!(peer.head(1, ":status"), Some("201"));
    assert_eq!(peer.head(1, "x-answered"), Some("yes"));
}

#[test]
fn a_handler_reads_the_request_body() {
    let expected = payload(50_000);
    let received = Arc::new(Mutex::new(None));
    let recorder = Arc::clone(&received);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let recorder = Arc::clone(&recorder);
        async move {
            let body = drain(request.into_body()).await.expect("a request body");
            let len = body.len();
            *recorder.lock().expect("record") = Some(body);
            http::Response::builder()
                .status(200)
                .header("x-received", len.to_string())
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing
        .push(("POST", "/upload".to_owned(), Some(expected.clone())));

    let driving = async {
        for _ in 0..256 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    assert_eq!(received.lock().expect("record").as_ref(), Some(&expected));
    assert_eq!(
        peer.head(1, "x-received"),
        Some(expected.len().to_string().as_str())
    );
}

#[test]
fn a_handler_sends_a_response_body() {
    let expected = payload(200_000);
    let answer = expected.clone();

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |_request: http::Request<IncomingBody>| {
        let answer = answer.clone();
        async move {
            http::Response::builder()
                .status(200)
                .body(Full::new(answer))
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/download".to_owned(), None));

    let driving = async {
        for _ in 0..512 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    assert_eq!(peer.bodies.get(&1).map(Vec::len), Some(expected.len()));
    assert_eq!(peer.bodies.get(&1), Some(&expected));
}

#[test]
fn a_slow_handler_delays_no_other_stream() {
    // Spec SC-006. The whole point of holding handlers as futures rather than running them
    // in turn: one that is not ready must cost the others nothing.
    //
    // Asserted by ordering, not by outcome. A server that ran handlers to completion in
    // arrival order would still answer both eventually — what it could not do is answer
    // the second *before* the first was released, which is what the log below records.
    let gate = Arc::new(Gate::default());
    let opened = Arc::clone(&gate);
    let log = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&log);
    let observer = Arc::clone(&log);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let gate = Arc::clone(&gate);
        let recorder = Arc::clone(&recorder);
        let slow = request.uri().path() == "/slow";
        async move {
            if slow {
                gate.wait().await;
                recorder.lock().expect("log").push("slow answered");
            } else {
                recorder.lock().expect("log").push("quick answered");
            }
            http::Response::builder()
                .status(200)
                .header("x-path", if slow { "/slow" } else { "/quick" })
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/slow".to_owned(), None));
    peer.outgoing.push(("GET", "/quick".to_owned(), None));

    let driving = async {
        for _ in 0..32 {
            yield_now().await;
        }
        observer.lock().expect("log").push("released");
        opened.open();
        for _ in 0..32 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    assert_eq!(
        *log.lock().expect("log"),
        ["quick answered", "released", "slow answered"],
        "the quick request waited behind the slow one",
    );
    assert_eq!(peer.head(3, "x-path"), Some("/quick"));
    assert_eq!(peer.head(1, "x-path"), Some("/slow"));
}

#[test]
fn a_parked_handler_is_not_polled_by_another_streams_traffic() {
    // The other half of the concurrency claim, and the reason each handler carries its own
    // waker. A connection carrying several streams must poll one future when one of them
    // becomes ready — not every future every time anything happens.
    //
    // The gate counts its own polls, so a re-poll is visible. Other streams run throughout,
    // so "it was not polled" means something: without per-handler wakers, every response
    // that went out on those streams would have polled this one too.
    let gate = Arc::new(Gate::default());
    let watcher = Arc::clone(&gate);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let gate = Arc::clone(&gate);
        let watched = request.uri().path() == "/watched";
        async move {
            if watched {
                gate.wait().await;
            }
            http::Response::builder()
                .status(200)
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/watched".to_owned(), None));

    let driving = async {
        for _ in 0..8 {
            yield_now().await;
        }
        let parked = watcher.polls();

        for _ in 0..40 {
            yield_now().await;
        }
        let after = watcher.polls();

        watcher.open();
        for _ in 0..16 {
            yield_now().await;
        }
        (parked, after, watcher.polls())
    };

    // Six more requests, answered while the watched handler sits parked.
    let mut passes = 0;
    let busywork = move |session: &mut Session<Peer>, peer: &mut Peer| {
        passes += 1;
        if (2..8).contains(&passes) {
            peer.outgoing
                .push(("GET", format!("/other/{passes}"), None));
        }
        ask(session, peer);
    };

    let (parked, after, finally) = block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, busywork),
    ));

    assert_eq!(parked, 1, "the handler did not park where expected");
    assert_eq!(
        after, parked,
        "a parked handler was polled again by another stream's traffic",
    );
    assert_eq!(
        finally, 2,
        "the handler was never resumed, or was resumed twice"
    );
    assert!(
        peer.heads.len() > 4,
        "not enough other streams completed for the claim to mean anything: {:?}",
        peer.heads.keys().collect::<Vec<_>>(),
    );
}

#[test]
fn a_reset_stream_discards_its_response_without_failing_the_connection() {
    // Spec SC-028. Four things at once: the response is discarded, the connection reports
    // no error, another stream still completes, and the handler can tell it happened.
    let gate = Arc::new(Gate::default());
    let opened = Arc::clone(&gate);
    let noticed = Arc::new(Mutex::new(None));
    let recorder = Arc::clone(&noticed);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let gate = Arc::clone(&gate);
        let recorder = Arc::clone(&recorder);
        let watched = request.uri().path() == "/doomed";
        let body = request.into_body();
        async move {
            if watched {
                gate.wait().await;
                // The request body is the channel a handler learns through: the stream it
                // was reading from is gone, and reading says so.
                *recorder.lock().expect("record") = Some(drain(body).await.is_err());
            }
            http::Response::builder()
                .status(200)
                .header("x-path", if watched { "/doomed" } else { "/fine" })
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    // Watched rather than awaited: a server connection ends when its peer goes away, so
    // awaiting it here would simply hang. What matters is that it did not *fail*.
    let failed = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&failed);
    let connection = async move {
        *sink.lock().expect("outcome") = Some(connection.await.is_err());
    };

    let mut peer = Peer::default();
    // A body that never ends, so the stream is still open when the peer changes its mind.
    peer.outgoing
        .push(("POST", "/doomed".to_owned(), Some(Vec::new())));

    let driving = async {
        for _ in 0..16 {
            yield_now().await;
        }
        opened.open();
        for _ in 0..48 {
            yield_now().await;
        }
    };

    // The peer resets the first stream once it is under way, then asks for another on the
    // same connection.
    let mut passes = 0;
    let step = move |session: &mut Session<Peer>, peer: &mut Peer| {
        ask(session, peer);
        passes += 1;
        if passes == 3 {
            session
                .reset_stream(StreamId::new(1), ErrorCode::CANCEL)
                .expect("resetting");
            peer.outgoing.push(("GET", "/fine".to_owned(), None));
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, step),
    ));

    assert_ne!(
        *failed.lock().expect("outcome"),
        Some(true),
        "one reset stream failed the whole connection",
    );
    assert_eq!(
        peer.heads.get(&1),
        None,
        "a response was sent on a stream the peer had reset",
    );
    assert_eq!(
        peer.head(3, "x-path"),
        Some("/fine"),
        "the other stream did not complete",
    );
    assert_eq!(
        peer.closed.get(&1).copied(),
        Some(ErrorCode::CANCEL.get()),
        "the reset stream did not close under the peer's own code",
    );
    assert_eq!(
        *noticed.lock().expect("record"),
        Some(true),
        "the handler could not tell its stream had gone",
    );
}

#[test]
fn a_retained_handler_still_counts_against_the_concurrency_cap() {
    // SF-3 / TO-4. The concurrency cap must be a structural bound on handler futures, not
    // only advice the peer may ignore. libnghttp2 drops a reset stream from the count it
    // enforces, but this crate deliberately keeps that stream's handler running — so the
    // cap has to be re-enforced here against the running handlers. With a cap of one, a
    // peer that resets its first stream and opens a second must be refused, because the
    // retained handler still holds the only slot.
    let invocations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&invocations);

    let (server_side, client_side) = duplex(false);
    let config = ngnet_h2::http::Config::default().max_concurrent_streams(1);
    let connection = server::serve_with(
        server_side,
        move |_request: http::Request<IncomingBody>| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                // Never finishes: a handler that outlives the reset of its stream, still
                // occupying its slot against the cap.
                core::future::pending::<()>().await;
                #[allow(unreachable_code)]
                http::Response::builder()
                    .status(200)
                    .body(Empty)
                    .expect("a response")
            }
        },
        config,
    )
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/first".to_owned(), None));

    let driving = async {
        for _ in 0..128 {
            yield_now().await;
        }
    };

    // Sequenced on the observed handler count, not a pass tally: the reset must follow the
    // first handler actually starting. The reset and the second request go out together —
    // they name different streams, so the reset cannot cancel the second request's headers
    // — and the reset's own output keeps the peer's send loop running long enough to
    // serialise that second request.
    let observed = Arc::clone(&invocations);
    let mut done = false;
    let step = move |session: &mut Session<Peer>, peer: &mut Peer| {
        ask(session, peer);
        if !done && observed.load(Ordering::SeqCst) >= 1 {
            session
                .reset_stream(StreamId::new(1), ErrorCode::CANCEL)
                .expect("resetting");
            peer.outgoing.push(("GET", "/second".to_owned(), None));
            done = true;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, step),
    ));

    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "the capped second request started a handler, so a retained handler did not hold its slot",
    );
    assert_eq!(
        peer.closed.get(&3).copied(),
        Some(ErrorCode::REFUSED_STREAM.get()),
        "the capped stream was not refused with REFUSED_STREAM",
    );
    assert_eq!(
        peer.heads.get(&3),
        None,
        "a refused stream still received a response head",
    );
}

#[test]
fn a_handler_sends_trailers_after_its_body() {
    // SF-7. Server response bodies are inserted with `slot: None`, so a client-side test
    // proves nothing about them. A handler that sends data and then trailers must reach
    // the peer as exactly that: the data first, the trailing block after it, in the order
    // the wire requires. `trailers_after_data` records that ordering as it was observed,
    // not merely that both arrived.
    let expected = payload(4_000);
    let answer = expected.clone();

    let (body, handle) = scripted();
    handle.send(answer);
    let mut sent = http::HeaderMap::new();
    sent.insert("x-trailer", http::HeaderValue::from_static("checksum"));
    handle.finish_with_trailers(sent);

    let body_slot = Arc::new(Mutex::new(Some(body)));
    let taker = Arc::clone(&body_slot);
    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |_request: http::Request<IncomingBody>| {
        let taker = Arc::clone(&taker);
        async move {
            let body = taker.lock().expect("body").take().expect("one request");
            http::Response::builder()
                .status(200)
                .body(body)
                .expect("a response")
        }
    })
    .expect("serving");

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/report".to_owned(), None));

    let driving = async {
        for _ in 0..64 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    assert_eq!(peer.head(1, ":status"), Some("200"));
    assert_eq!(
        peer.bodies.get(&1),
        Some(&expected),
        "the handler's body did not arrive intact",
    );
    assert_eq!(
        peer.trailers
            .get(&1)
            .and_then(|fields| fields.iter().find(|(name, _)| name == "x-trailer"))
            .map(|(_, value)| value.as_str()),
        Some("checksum"),
        "the handler's trailers did not arrive",
    );
    assert!(
        peer.trailers_after_data.contains(&1),
        "the trailers did not follow the data on the wire",
    );
}

#[test]
fn a_failed_response_body_resets_only_its_own_stream() {
    // SF-7. `fail_stream` early-returns for a server stream because it has no slot, so a
    // handler whose body fails part-way exercises code no client-side test can. The peer
    // must see that one stream reset, while a second request on the same connection still
    // completes and the connection itself reports no error.
    let (doomed_body, doomed) = scripted();
    doomed.send(payload(2_000));
    doomed.fail("the body gave out");
    let (fine_body, fine) = scripted();
    fine.finish();

    let mut bodies = BTreeMap::new();
    bodies.insert("/doomed".to_owned(), doomed_body);
    bodies.insert("/fine".to_owned(), fine_body);
    let bodies = Arc::new(Mutex::new(bodies));
    let taker = Arc::clone(&bodies);

    let (server_side, client_side) = duplex(false);
    let connection = server::serve(server_side, move |request: http::Request<IncomingBody>| {
        let taker = Arc::clone(&taker);
        let path = request.uri().path().to_owned();
        async move {
            let body = taker
                .lock()
                .expect("body")
                .remove(&path)
                .expect("a scripted body for the path");
            http::Response::builder()
                .status(200)
                .body(body)
                .expect("a response")
        }
    })
    .expect("serving");

    let failed = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&failed);
    let connection = async move {
        *sink.lock().expect("outcome") = Some(connection.await.is_err());
    };

    let mut peer = Peer::default();
    peer.outgoing.push(("GET", "/doomed".to_owned(), None));
    peer.outgoing.push(("GET", "/fine".to_owned(), None));

    let driving = async {
        for _ in 0..64 {
            yield_now().await;
        }
    };

    block_on(alongside(
        alongside(driving, connection),
        drive_peer(client_side, peer_session(), &mut peer, ask),
    ));

    let doomed_close = peer.closed.get(&1).copied();
    assert!(
        doomed_close.is_some_and(|code| code != ErrorCode::NO_ERROR.get()),
        "the failed stream did not reset with an error (closed under {doomed_close:?})",
    );
    assert_eq!(
        peer.head(3, ":status"),
        Some("200"),
        "the healthy stream did not complete alongside the failed one",
    );
    assert_ne!(
        *failed.lock().expect("outcome"),
        Some(true),
        "one failed response body failed the whole connection",
    );
}

/// A future that resolves only when opened, so a handler can be held at a known point.
///
/// Counts how often it is polled, which is what makes "a parked handler is not polled
/// again" observable: counting around the `await` would only ever see the two ends of it.
#[derive(Debug, Default)]
struct Gate {
    state: Mutex<(bool, Option<core::task::Waker>)>,
    polls: AtomicUsize,
}

impl Gate {
    fn open(&self) {
        let waker = {
            let mut state = self.state.lock().expect("gate");
            state.0 = true;
            state.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn polls(&self) -> usize {
        self.polls.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        core::future::poll_fn(|cx| {
            self.polls.fetch_add(1, Ordering::AcqRel);
            let mut state = self.state.lock().expect("gate");
            if state.0 {
                core::task::Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                core::task::Poll::Pending
            }
        })
        .await;
    }
}
