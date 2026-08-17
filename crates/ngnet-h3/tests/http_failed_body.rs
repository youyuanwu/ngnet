#![cfg(feature = "http")]
//! What the stack asks a transport to do when a caller's outgoing body fails.
//!
//! A body that failed and a body that ended used to look identical on the wire: both marked
//! the stream finished, and the reset that was supposed to signal the failure arrived
//! afterwards, about a stream the peer already considered complete. Which of the two
//! statements the peer believed depended on how much happened to be queued behind the end
//! marker, which is to say on nothing the caller could observe or control. A truncated
//! message arrived looking whole.
//!
//! These tests are therefore mostly not about what a peer received. They are about what this
//! endpoint *said*: a recording transport sits between the driver and the transport
//! underneath and keeps every write, every end-of-stream marker, every reset and every
//! transmit pass, so the claim "no end marker is ever produced for a failed body" is
//! asserted where the decision is made rather than inferred from what a particular peer
//! happened to make of it. That is what makes it a claim about every transport this stack
//! runs over rather than about the in-memory one.
//!
//! Two of them run against a transport that says nothing at all. A failure must reach the
//! peer without the peer prompting it, so the interesting question is what the driver does
//! when there is nothing to react to — and the answer has to be "not go idle", which is only
//! observable by driving it by hand and watching for a `Pending` that must not come.

use ngnet_h3::ErrorCode;
use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::{ErrorKind, IncomingBody, QuicEvent, handshake, serve};

mod support;
use support::{
    Gate, Log, Payload, Pump, Recorder, Server, empty, failing, failing_after,
    failing_after_resuming, gated, once, request_stream, with_trailers,
};

/// `H3_REQUEST_CANCELLED`, written out rather than imported.
///
/// The constant inside the crate is private, and that is just as well: this is the number
/// RFC 9114 §4.1.1 names for abandoning a message part-way, and a test that read it from the
/// implementation would agree with whatever the implementation happened to say.
const REQUEST_CANCELLED: u64 = 0x10c;

/// How many hand-driven polls a test allows before declaring the driver stuck.
///
/// A bound rather than a timeout: a failure to make progress should fail a test rather than
/// hang one.
const POLLS: usize = 40;

type BoxedAnswer = std::pin::Pin<Box<dyn core::future::Future<Output = http::Response<Payload>>>>;

/// A payload with a pattern that is recognisable wherever it turns up.
fn patterned(seed: &[u8], repeats: usize) -> Vec<u8> {
    seed.iter()
        .copied()
        .cycle()
        .take(seed.len() * repeats)
        .collect()
}

/// Whether `haystack` contains `needle` anywhere.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Reads a whole response, however the abandonment of it reaches the caller.
///
/// An abandoned message can surface in either of two places, and which one depends on
/// whether the reset caught up with the head before the peer got round to reading its
/// inbox: the driver acts on control-plane news ahead of body data, so a reset sitting in
/// the same batch of events as the head it followed fails the exchange before the head is
/// ever dispatched. The caller then sees a failed response rather than a failed body read.
/// Both are the same answer — this message is not complete — and the specification admits
/// either; what neither may be is a body that ends normally.
fn whole_response<C, S>(
    pump: &mut support::BothEnds<C, S>,
    request: ngnet_h3::http::ResponseFuture,
) -> Result<Vec<u8>, ngnet_h3::http::Error>
where
    C: core::future::Future<Output = Result<(), ngnet_h3::http::Error>>,
    S: core::future::Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    let mut request = Box::pin(request);
    let head = pump
        .rounds(400, &mut request)
        .expect("the request should settle")?;
    let mut read = Box::pin(support::collect(head.into_body()));
    pump.rounds(400, &mut read)
        .expect("the body read should settle")
}

/// Drives a driver by hand until it settles or the bound is reached.
///
/// Every poll is on a no-op waker and on this thread, so nothing at all happens between one
/// poll and the next.
fn pump_driver<F>(driver: &mut std::pin::Pin<Box<F>>) -> Option<F::Output>
where
    F: core::future::Future,
{
    for _ in 0..POLLS {
        if let Some(outcome) = support::poll_now(driver) {
            return Some(outcome);
        }
    }
    None
}

#[test]
fn a_failed_body_produces_no_end_of_stream_marker_and_exactly_one_reset() {
    // The whole change, stated at the seam it is made above. Zero end markers, because an
    // end marker is a statement that the message is complete and it is not; exactly one
    // reset, because that reset is now the only thing the peer will ever be told about how
    // this message ended (SC-003).
    let (client_side, server_side, _knobs) = support::pair();
    let log = Log::new();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(client_side, log.clone())).expect("handshake");
    let mut server = Server::new(server_side);

    let answer = support::exchange(driver, &mut server, || {
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(failing_after(Bytes::from(patterned(
                    b"\xde\xad\xbe\xef",
                    256,
                ))))
                .expect("a request"),
        )
    });

    let stream = request_stream(0);
    assert!(answer.is_err(), "a request whose body failed succeeded");
    assert_eq!(
        log.end_markers(stream),
        0,
        "a failed body's stream was offered an end-of-stream marker: {:#?}",
        log.calls()
    );
    assert_eq!(
        log.resets(stream),
        vec![REQUEST_CANCELLED],
        "a failed body should be answered by exactly one reset"
    );
}

#[test]
fn the_reset_after_a_failed_body_carries_request_cancelled_in_both_roles() {
    // The code is not changed by this work; the assertion exists so that it stays that way.
    // RFC 9114 §4.1.1 names H3_REQUEST_CANCELLED for abandoning a message part-way in either
    // direction, and both roles must make the same decision about a body they supplied
    // (SC-004).
    let client_log = Log::new();
    {
        let (client_side, server_side, _knobs) = support::pair();
        let (handle, driver) =
            handshake::<_, Payload>(Recorder::new(client_side, client_log.clone()))
                .expect("handshake");
        let mut server = Server::new(server_side);

        let answer = support::exchange(driver, &mut server, || {
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://example.test/")
                    .body(failing())
                    .expect("a request"),
            )
        });
        assert!(answer.is_err());
    }

    let server_log = Log::new();
    {
        let (client_side, server_side, _knobs) = support::pair();
        let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
        let server = serve(
            Recorder::new(server_side, server_log.clone()),
            |_request: http::Request<IncomingBody>| {
                Box::pin(async {
                    http::Response::builder()
                        .status(200)
                        .body(failing())
                        .expect("a response")
                }) as BoxedAnswer
            },
        )
        .expect("serve");

        let answer = support::both_ends(client, server, || {
            handle.send_request(
                http::Request::builder()
                    .uri("https://example.test/")
                    .body(empty())
                    .expect("a request"),
            )
        });
        // Whether the head outlives the reset that treads on its heels is a race neither end
        // controls — `whole_response` says why — and is not this test's subject. The code on
        // the reset is, and it is the same number whichever way the exchange settled.
        drop(answer);
    }

    let stream = request_stream(0);
    assert_eq!(
        client_log.resets(stream),
        vec![REQUEST_CANCELLED],
        "a client's failed request body"
    );
    assert_eq!(
        server_log.resets(stream),
        vec![REQUEST_CANCELLED],
        "a server's failed response body"
    );
}

#[test]
fn the_bytes_of_a_failing_pull_are_never_written() {
    // One pull of a body drains it until it defers, ends or fails, so a pull that fails may
    // be carrying bytes it already gathered. Those bytes go with the message they can no
    // longer finish: the stream is being reset, so nothing on it is anything the peer may
    // act on (SC-005).
    //
    // The seam carries framed bytes — a QPACK-encoded head and DATA frame headers around the
    // payload — so this looks for the patterns rather than counting. Both halves matter: the
    // first pattern must be there, or the test would pass just as well against a stack that
    // wrote nothing at all.
    let written = patterned(b"\xde\xad\xbe\xef", 256);
    let discarded = patterned(b"\xca\xfe\xba\xbe", 256);

    let (client_side, server_side, _knobs) = support::pair();
    let log = Log::new();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(client_side, log.clone())).expect("handshake");
    let server = Server::new(server_side);

    let gate = Gate::new();
    let body = failing_after_resuming(
        Bytes::from(written.clone()),
        Bytes::from(discarded.clone()),
        gate.clone(),
    );

    let mut pump = Pump::new(driver, server);
    let mut future = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(body)
                .expect("a request"),
        ),
    );
    assert!(
        pump.rounds(30, &mut future).is_none(),
        "the exchange settled while its body was still waiting"
    );

    gate.open();
    let answer = pump
        .rounds(200, &mut future)
        .expect("the body failed, so the exchange should settle");
    assert!(answer.is_err());

    let stream = request_stream(0);
    let offered = log.offered(stream);
    assert!(
        contains(&offered, &written[..16]),
        "the bytes from the pull that succeeded were never written"
    );
    assert!(
        !contains(&offered, &discarded[..16]),
        "bytes gathered by the pull that failed were written anyway"
    );
    assert_eq!(log.end_markers(stream), 0);
}

#[test]
fn the_driver_never_parks_between_a_body_failing_and_its_reset() {
    // A park here would leave the peer holding a message that neither ended nor was
    // abandoned until the peer happened to say something of its own accord — and against a
    // peer that never does, forever (SC-009).
    //
    // A park cannot be recognised from the recorded calls: the driver asks the transport for
    // events at the top of every pass, so a pass that parks and a pass that works look the
    // same from outside. It is observable where it actually happens, though — the driver
    // future answering `Poll::Pending`. Against a stub that always transmits and always
    // opens a stream, nothing in a pass is ever waited on: the only `await` left is the
    // park's own `poll_fn`, and the stub's silence only becomes `Pending` there. So a first
    // poll that comes back `Pending` with the reset already recorded is a first poll during
    // which no park happened before it.
    let log = Log::new();
    let (transport, _controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let _request = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/")
            .body(failing_after(Bytes::from(patterned(
                b"\xde\xad\xbe\xef",
                64,
            ))))
            .expect("a request"),
    );

    let mut driver = Box::pin(driver);
    assert!(
        support::poll_now(&mut driver).is_none(),
        "the driver finished rather than parking, so this proves nothing"
    );
    assert_eq!(
        log.resets(request_stream(0)),
        vec![REQUEST_CANCELLED],
        "the driver went idle before it had told the transport about a failed body: {:#?}",
        log.calls()
    );
}

#[test]
fn a_reset_reaches_a_transport_that_reports_nothing_at_all() {
    // The obligation that comes with expressing "abandoned" as "nothing further, ever": a
    // stream that is never resumed and never reset hangs, and it hangs silently. So the
    // failure has to reach the transport with no input from the peer whatsoever — no
    // acknowledgement, no answer, not even a close (SC-008).
    let log = Log::new();
    let (transport, _controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let _request = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/")
            .body(failing())
            .expect("a request"),
    );

    let mut driver = Box::pin(driver);
    pump_driver(&mut driver);

    assert_eq!(
        log.resets(request_stream(0)),
        vec![REQUEST_CANCELLED],
        "a silent peer left the failure undelivered: {:#?}",
        log.calls()
    );
}

#[test]
fn the_reset_follows_the_failing_body_on_the_very_next_transmit() {
    // A body is pulled during a transmit and the record of how it ended is read at the top of
    // the next pass, above everything that could wait, so the reset is drained onto the
    // transport before that pass transmits anything of its own. The window is therefore not
    // merely bounded but empty: no transmit separates the failure from the reset. Asserting
    // the two-pass bound SC-008 asks for would leave room for a delay the design does not
    // have, and a regression that reintroduced one would pass.
    let log = Log::new();
    let (transport, _controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let _request = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/")
            // The failure happens inside a transmit, where nothing the transport is called
            // with says so; the body notes the pass it happened on instead.
            .body(
                failing_after(Bytes::from(patterned(b"\xde\xad\xbe\xef", 64))).marking(log.clone()),
            )
            .expect("a request"),
    );

    let mut driver = Box::pin(driver);
    pump_driver(&mut driver);

    let marks = log.marks();
    assert_eq!(marks.len(), 1, "the body should have failed exactly once");
    let failed_on = marks[0];
    let reset_on = log
        .transmits_before_reset(request_stream(0))
        .expect("a failed body should be reset");
    assert_eq!(
        reset_on, failed_on,
        "the body failed on transmit {failed_on} and the reset went out on {reset_on}, so a \
         transmit passed in between and the reset waited on something"
    );
}

#[test]
fn a_failed_bodys_reset_is_written_even_while_a_new_stream_is_refused() {
    // The stall this design has to avoid, and the reason the ending is read early in a pass
    // rather than half way down it. A pass that wants a stream the transport will not open
    // waits there indefinitely — that is what an exhausted peer stream limit looks like — and
    // it waits *before* the point at which the role would otherwise notice a body had failed.
    // Under the old behaviour that only delayed a reset the peer had already been told to
    // ignore; under this one it would leave a stream that neither ends nor resets, which is
    // worse than the defect being removed (Spec C-4).
    let log = Log::new();
    let (transport, controls) = support::stub();
    // One stream, which the failing request takes. Everything after it waits forever.
    controls.open_at_most(1);
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let _failing = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/fails")
            .body(failing_after(Bytes::from(patterned(
                b"\xde\xad\xbe\xef",
                64,
            ))))
            .expect("a request"),
    );
    let _queued = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/waits")
            .body(empty())
            .expect("a request"),
    );

    let mut driver = Box::pin(driver);
    pump_driver(&mut driver);

    assert_eq!(
        log.resets(request_stream(0)),
        vec![REQUEST_CANCELLED],
        "the reset was lost to a pass that stopped to wait for a stream: {:#?}",
        log.calls()
    );
    assert_eq!(
        log.end_markers(request_stream(0)),
        0,
        "a failed body's stream was ended after all"
    );
}

#[test]
fn a_body_that_ends_is_untouched_however_it_ends() {
    // Only the failure path moves. A body that ends normally, one that ends with a trailing
    // field section and one that had nothing to say for a while and then ended must all
    // still produce an end-of-stream marker and no reset — which is the same pair of
    // observations as before the change, made at the same seam (SC-015).
    let clean = Log::new();
    {
        let (client_side, server_side, _knobs) = support::pair();
        let (handle, driver) =
            handshake::<_, Payload>(Recorder::new(client_side, clean.clone())).expect("handshake");
        let mut server = Server::new(server_side);
        let answer = support::exchange(driver, &mut server, || {
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://example.test/")
                    .body(once(Bytes::from_static(b"all of it")))
                    .expect("a request"),
            )
        });
        assert!(answer.is_ok());
    }

    let trailered = Log::new();
    {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-checksum", "deadbeef".parse().expect("a value"));

        let (client_side, server_side, _knobs) = support::pair();
        let (handle, driver) =
            handshake::<_, Payload>(Recorder::new(client_side, trailered.clone()))
                .expect("handshake");
        let mut server = Server::new(server_side);
        let answer = support::exchange(driver, &mut server, || {
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://example.test/")
                    .body(with_trailers(Bytes::from_static(b"all of it"), trailers))
                    .expect("a request"),
            )
        });
        assert!(answer.is_ok());
        assert_eq!(
            server.received_trailer("x-checksum").as_deref(),
            Some("deadbeef")
        );
    }

    let resumed = Log::new();
    {
        let (client_side, server_side, _knobs) = support::pair();
        let (handle, driver) = handshake::<_, Payload>(Recorder::new(client_side, resumed.clone()))
            .expect("handshake");
        let server = Server::new(server_side);

        let gate = Gate::new();
        let mut pump = Pump::new(driver, server);
        let mut future = Box::pin(
            handle.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://example.test/")
                    .body(gated(Bytes::from_static(b"eventually"), gate.clone()))
                    .expect("a request"),
            ),
        );
        assert!(pump.rounds(30, &mut future).is_none());
        gate.open();
        let answer = pump
            .rounds(400, &mut future)
            .expect("a resumed body should finish");
        assert!(answer.is_ok());
    }

    let stream = request_stream(0);
    for (what, log) in [
        ("a clean ending", &clean),
        ("a trailer ending", &trailered),
        ("a resumed ending", &resumed),
    ] {
        assert_eq!(
            log.end_markers(stream),
            1,
            "{what} should end its stream exactly once: {:#?}",
            log.calls()
        );
        assert!(
            log.resets(stream).is_empty(),
            "{what} was reset: {:#?}",
            log.calls()
        );
    }
}

#[test]
fn a_failing_response_body_leaves_the_clients_read_in_an_error() {
    // The whole point of the change, seen from the far end: a client reading a truncated
    // response must be told, whether the body managed to produce anything before it failed or
    // not, and without the length it may never have been given (SC-001).
    //
    // Read here rather than with the helper the rest of the suite uses, because that one
    // swallows a body error and returns what it had — which is exactly the mistake this
    // whole change is about.
    let gate = Gate::new();
    let held = gate.clone();
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let path = request.uri().path().to_owned();
        let gate = held.clone();
        Box::pin(async move {
            let body = match path.as_str() {
                "/nothing" => failing(),
                "/some" => failing_after(Bytes::from(patterned(b"\xde\xad\xbe\xef", 64))),
                _ => failing_after_resuming(
                    Bytes::from(patterned(b"\xde\xad\xbe\xef", 64)),
                    Bytes::from_static(b"never sent"),
                    gate,
                ),
            };
            http::Response::builder()
                .status(200)
                .body(body)
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    for path in ["/nothing", "/some"] {
        let request = handle.send_request(
            http::Request::builder()
                .uri(format!("https://example.test{path}"))
                .body(empty())
                .expect("a request"),
        );
        assert!(
            whole_response(&mut pump, request).is_err(),
            "{path} read as a complete message when it had been abandoned"
        );
    }

    // And once more with a body that pauses after its first chunk, which puts the head into
    // the client's hands a batch of events before the reset and so pins the failure to where
    // the criterion says it belongs: in the read of the body rather than in the head.
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/partway")
                .body(empty())
                .expect("a request"),
        ),
    );
    let response = pump
        .rounds(400, &mut request)
        .expect("/partway never answered")
        .expect("/partway's head should arrive ahead of the failure");
    let mut read = Box::pin(support::collect(response.into_body()));
    assert!(
        pump.rounds(50, &mut read).is_none(),
        "/partway's read ended while its body was still paused"
    );
    gate.open();
    let outcome = pump
        .rounds(400, &mut read)
        .expect("/partway's body read never settled");
    assert!(
        outcome.is_err(),
        "/partway read as a complete message when it had been abandoned part-way"
    );
}

#[test]
fn a_failing_request_body_leaves_the_handlers_read_in_an_error_and_tells_the_client() {
    // The other direction, where believing a truncated message is worse still: a server can
    // act on a request body it thinks is whole. The client is separately owed the truth about
    // its own body, and owed it as *its* failure rather than as the peer's (SC-006).
    let read = std::sync::Arc::new(std::sync::Mutex::new(None));
    let recorder = std::sync::Arc::clone(&read);

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let recorder = std::sync::Arc::clone(&recorder);
        let body = request.into_body();
        Box::pin(async move {
            let outcome = support::collect(body).await;
            *recorder.lock().expect("the recorder") = Some(outcome.is_err());
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let gate = Gate::new();
    // The body pauses after its first chunk rather than failing on the first pull: an upload
    // whose source dies part-way is the realistic shape of the failure, and it puts the
    // handler's read of an already-started body squarely in the middle of it. The other
    // shape — a body that fails before it has produced anything, whose reset catches the head
    // up in the same batch of events — is a case of its own, and is
    // `a_request_body_that_fails_on_its_first_pull_still_fails_the_handlers_read`.
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/upload")
                .body(failing_after_resuming(
                    Bytes::from(patterned(b"\xde\xad\xbe\xef", 64)),
                    Bytes::from_static(b"never sent"),
                    gate.clone(),
                ))
                .expect("a request"),
        ),
    );
    assert!(
        pump.rounds(50, &mut request).is_none(),
        "the request settled while its body was still paused"
    );
    gate.open();
    let answer = pump
        .rounds(400, &mut request)
        .expect("the request should settle");

    let error = answer.expect_err("a request whose body failed succeeded");
    assert_eq!(
        error.kind(),
        ErrorKind::Body,
        "the caller was told something other than that its own body failed"
    );

    let mut nothing = Box::pin(core::future::pending::<()>());
    pump.rounds(200, &mut nothing);
    assert_eq!(
        *read.lock().expect("the recorder"),
        Some(true),
        "the handler read a truncated upload as a complete one"
    );
}

#[test]
fn a_request_body_that_fails_on_its_first_pull_still_fails_the_handlers_read() {
    // The narrowest version of the same obligation, and the one removing the end marker made
    // reachable. A body that fails before it has produced anything writes nothing and ends
    // nothing, so its reset follows the head down the wire with no bytes in between and
    // arrives in the same batch of events as the head it is about. Control-plane news is
    // acted on ahead of body data, so at the moment the reset is applied the server has never
    // heard of the stream; a reset dropped there leaves the head to start a handler on an
    // exchange the peer abandoned before it began, holding a request body that can no longer
    // end and can no longer fail. The handler then reads it forever (SC-006).
    //
    // Which is why every drive here is bounded: an unfixed endpoint does not fail this test
    // slowly, it fails to finish at all, and a bound is the difference between a red test and
    // a wedged one.
    let read = std::sync::Arc::new(std::sync::Mutex::new(None));
    let recorder = std::sync::Arc::clone(&read);

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let recorder = std::sync::Arc::clone(&recorder);
        let body = request.into_body();
        Box::pin(async move {
            let outcome = support::collect(body).await;
            *recorder.lock().expect("the recorder") = Some(outcome.is_err());
            http::Response::builder()
                .status(200)
                .body(empty())
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/upload")
                .body(failing())
                .expect("a request"),
        ),
    );
    let answer = pump
        .rounds(400, &mut request)
        .expect("the request should settle");
    assert!(answer.is_err(), "a request whose body failed succeeded");

    // Nothing left to wait for, so the rounds are spent on the connection itself: whatever
    // the handler is going to learn, it has learnt by the end of them.
    let mut nothing = Box::pin(core::future::pending::<()>());
    pump.rounds(200, &mut nothing);
    assert_eq!(
        *read.lock().expect("the recorder"),
        Some(true),
        "the handler was left reading an upload that can no longer end or fail"
    );
}

#[test]
fn a_body_that_fails_behind_a_backlog_still_leaves_the_clients_read_in_an_error() {
    // The case that already worked, because a backlog meant the reset overtook bytes the peer
    // had not seen yet and the end marker went with them. It has to keep working: the fix
    // removes the end marker, and a change that only helped the unqueued case while breaking
    // the queued one would be no better than the defect (SC-002).
    let (client_side, server_side, knobs) = support::pair();
    // A transport that takes very little at a time is what keeps a backlog in existence long
    // enough for the body to fail behind it.
    knobs.accept_at_most(1024);

    let gate = Gate::new();
    let held = gate.clone();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |_request: http::Request<IncomingBody>| {
        let gate = held.clone();
        Box::pin(async move {
            http::Response::builder()
                .status(200)
                .body(failing_after_resuming(
                    Bytes::from(patterned(b"\xde\xad\xbe\xef", 16 * 1024)),
                    Bytes::from_static(b"never sent"),
                    gate,
                ))
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/")
                .body(empty())
                .expect("a request"),
        ),
    );
    let response = pump
        .rounds(400, &mut request)
        .expect("the request should settle")
        .expect("a response head");

    // Only a few rounds, so most of the body is still queued when the gate opens.
    let mut read = Box::pin(support::collect(response.into_body()));
    pump.rounds(5, &mut read);
    gate.open();

    let outcome = pump
        .rounds(800, &mut read)
        .expect("the read should settle once the body fails");
    assert!(
        outcome.is_err(),
        "a response truncated behind a backlog read as a complete one"
    );
}

#[test]
fn a_failing_body_disturbs_neither_the_exchange_beside_it_nor_the_one_after() {
    // A failure is one exchange's. Everything else on the connection has to finish
    // byte-for-byte, and the connection has to stay usable for whatever is started next
    // (SC-007).
    let payload = patterned(b"\x01\x02\x03\x04", 4 * 1024);
    let answer = payload.clone();

    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");
    let server = serve(server_side, move |request: http::Request<IncomingBody>| {
        let fails = request.uri().path() == "/fails";
        let answer = answer.clone();
        Box::pin(async move {
            let body = if fails {
                failing_after(Bytes::from_static(b"never arrives"))
            } else {
                once(Bytes::from(answer))
            };
            http::Response::builder()
                .status(200)
                .body(body)
                .expect("a response")
        }) as BoxedAnswer
    })
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let mut failing_request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/fails")
                .body(empty())
                .expect("a request"),
        ),
    );
    let mut healthy_request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/works")
                .body(empty())
                .expect("a request"),
        ),
    );

    let failing_response = pump.rounds(400, &mut failing_request);
    let healthy_response = pump
        .rounds(400, &mut healthy_request)
        .expect("the healthy exchange should settle")
        .expect("a response head");

    // Whether the abandoned exchange failed at its head or in its body is the race
    // `whole_response` describes; either way it must not have read as a whole message.
    let failed = match failing_response.expect("the failing exchange should settle") {
        Err(_) => Err(()),
        Ok(response) => {
            let mut read = Box::pin(support::collect(response.into_body()));
            pump.rounds(400, &mut read)
                .expect("the abandoned read should settle")
                .map_err(|_| ())
        }
    };
    let mut healthy_read = Box::pin(support::collect(healthy_response.into_body()));
    let healthy = pump
        .rounds(400, &mut healthy_read)
        .expect("the intact read should settle");

    assert!(failed.is_err(), "the abandoned message read as a whole one");
    assert_eq!(
        healthy.expect("the intact exchange failed"),
        payload,
        "an unrelated exchange lost or corrupted bytes"
    );

    let mut later = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/works")
                .body(empty())
                .expect("a request"),
        ),
    );
    let later_response = pump
        .rounds(400, &mut later)
        .expect("an exchange begun afterwards should settle")
        .expect("a response head");
    let mut later_read = Box::pin(support::collect(later_response.into_body()));
    let later_body = pump
        .rounds(400, &mut later_read)
        .expect("the later read should settle")
        .expect("the later exchange failed");
    assert_eq!(
        later_body, payload,
        "the connection was no longer usable after a body failed"
    );
}

#[test]
fn a_caller_that_lets_go_of_the_connection_as_its_body_fails_still_has_its_reset_delivered() {
    // With the last handle gone the driver may close the connection as soon as nothing is in
    // flight, and a failed body removes the last thing in flight. The reset is queued and
    // drained before that check is reached, so the peer is told before the connection goes —
    // which is the difference between an abandoned message and a message the peer never hears
    // anything more about.
    let log = Log::new();
    let (transport, _controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let _request = handle.send_request(
        http::Request::builder()
            .method("POST")
            .uri("https://example.test/")
            .body(failing_after(Bytes::from(patterned(
                b"\xde\xad\xbe\xef",
                64,
            ))))
            .expect("a request"),
    );
    // Nobody will ask for anything else, so the driver is free to finish as soon as it can.
    drop(handle);

    let mut driver = Box::pin(driver);
    let outcome = pump_driver(&mut driver);
    assert!(
        matches!(outcome, Some(Ok(()))),
        "the driver should have finished cleanly, got {outcome:?}"
    );

    let calls = log.calls();
    let reset = calls
        .iter()
        .position(|call| matches!(call, support::Call::Reset { .. }))
        .unwrap_or_else(|| panic!("the reset was never handed to the transport: {calls:#?}"));
    let closed = calls
        .iter()
        .position(|call| matches!(call, support::Call::Close { .. }))
        .expect("the connection should have been closed");
    assert!(
        reset < closed,
        "the connection closed before the failure was delivered: {calls:#?}"
    );
    assert_eq!(log.resets(request_stream(0)), vec![REQUEST_CANCELLED]);
}

#[test]
fn a_body_that_fails_after_the_peer_reset_the_stream_is_absorbed() {
    // The exchange is already over, so there is nothing to tell anyone and nothing to go
    // wrong. What must not happen is the failure escaping as a connection error or the driver
    // stopping: an endpoint that fell over because a body it had already given up on then
    // failed would be trading a small problem for the largest one.
    //
    // A client, because the client is the end that can be asserted about here: a response
    // future settling is this endpoint's own answer, whereas a server's equivalent is only
    // visible as bytes. The two roles do agree, though — `ClientRole::closed` does nothing
    // and `ServerRole::closed` no longer discards that stream's ending either, so both
    // settle the ending and emit a reset for a stream the peer has already abandoned:
    // harmless, and the server's side of it is
    // `a_response_body_that_fails_after_the_peer_reset_the_stream_is_still_reset`.
    let log = Log::new();
    let (transport, controls) = support::stub();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(transport, log.clone())).expect("handshake");

    let gate = Gate::new();
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/")
                .body(failing_after_resuming(
                    Bytes::from_static(b"some of it"),
                    Bytes::from_static(b"and no more"),
                    gate.clone(),
                ))
                .expect("a request"),
        ),
    );

    let mut driver = Box::pin(driver);
    assert!(
        support::poll_now(&mut driver).is_none(),
        "the driver finished before the exchange had begun"
    );

    // The peer gives up on the stream first, and only then does the body fail.
    let stream = request_stream(0);
    controls.deliver(QuicEvent::Reset {
        stream,
        code: ErrorCode::new(REQUEST_CANCELLED),
    });
    gate.open();
    let outcome = pump_driver(&mut driver);
    assert!(
        outcome.is_none(),
        "a body failing on a stream the peer had already reset ended the connection: \
         {outcome:?}"
    );

    let error = support::poll_now(&mut request)
        .expect("the exchange should have settled")
        .expect_err("a reset exchange succeeded");
    assert_eq!(
        error.kind(),
        ErrorKind::Stream,
        "the failure should be attributed to the peer, which reset first"
    );

    // And the connection carries on: a request begun afterwards still reaches the transport.
    let _later = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/after")
            .body(empty())
            .expect("a request"),
    );
    pump_driver(&mut driver);
    assert!(
        !log.offered(request_stream(1)).is_empty(),
        "the connection stopped carrying exchanges: {:#?}",
        log.calls()
    );
}

#[test]
fn a_response_body_that_fails_after_the_peer_reset_the_stream_is_still_reset() {
    // The server's half of the asymmetry the test above describes, which removing the end
    // marker turned from harmless into a stream that is never terminated at all. Applying a
    // peer's reset shuts the read side down and nothing more, so the response is still this
    // endpoint's to finish; if its body then fails and that ending is discarded — which is
    // what dropping the stream's ending slot on close amounted to — no reset is queued, no
    // end marker is written either, and the response is left suspended for the life of the
    // connection.
    //
    // Asserted at the transport seam, because "the peer was told" is a claim about what went
    // out, and because a suspended stream is not something the endpoint's own state will
    // confess to.
    let log = Log::new();
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");

    let gate = Gate::new();
    let held = gate.clone();
    let server = serve(
        Recorder::new(server_side, log.clone()),
        move |request: http::Request<IncomingBody>| {
            let abandoned = request.uri().path() == "/abandoned";
            let gate = held.clone();
            Box::pin(async move {
                let body = if abandoned {
                    failing_after_resuming(
                        Bytes::from_static(b"some of it"),
                        Bytes::from_static(b"never sent"),
                        gate,
                    )
                } else {
                    once(Bytes::from_static(b"all of it"))
                };
                http::Response::builder()
                    .status(200)
                    .body(body)
                    .expect("a response")
            }) as BoxedAnswer
        },
    )
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let mut request = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/abandoned")
                .body(empty())
                .expect("a request"),
        ),
    );
    let response = pump
        .rounds(400, &mut request)
        .expect("the request should settle")
        .expect("a response head");

    // The caller gives up on the exchange while the response body is still paused, which is
    // the only ordering that matters here: the peer's reset has to be applied before the
    // body fails, or there is nothing asymmetric left to get wrong.
    drop(response);
    let mut nothing = Box::pin(core::future::pending::<()>());
    pump.rounds(20, &mut nothing);
    gate.open();
    pump.rounds(200, &mut nothing);

    let stream = request_stream(0);
    assert_eq!(
        log.resets(stream),
        vec![REQUEST_CANCELLED],
        "a response body that failed on an abandoned stream was neither reset nor ended: \
         {:#?}",
        log.calls()
    );
    assert_eq!(
        log.end_markers(stream),
        0,
        "a failed body's stream was ended after all: {:#?}",
        log.calls()
    );

    // And the connection carries on serving, which is the other half of "one exchange's
    // failure": the pruning that keeps the ending out of the way must not take a live one
    // with it.
    let mut later = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/later")
                .body(empty())
                .expect("a request"),
        ),
    );
    let later_response = pump
        .rounds(400, &mut later)
        .expect("an exchange begun afterwards should settle")
        .expect("a response head");
    let mut read = Box::pin(support::collect(later_response.into_body()));
    let body = pump
        .rounds(400, &mut read)
        .expect("the later read should settle")
        .expect("the later exchange failed");
    assert_eq!(
        body, b"all of it",
        "the connection was no longer usable after an abandoned response body failed"
    );
}

#[test]
fn two_bodies_failing_in_one_pass_each_get_their_own_reset() {
    // Endings are read in a batch, so two of them arriving together is the case where one
    // could shadow the other. Each stream owes its peer its own reset and neither may be
    // ended.
    //
    // A server rather than a client, because only a server can have two bodies fail in one
    // pass at all: a client is handed one new stream per pass and so submits one request per
    // pass, and a body that fails offers nothing, which ends that pass's round of writes
    // before any other stream is asked. A server submits everything its handlers have
    // finished, and each of those messages has a head to write, so the round of writes
    // carries on past the first failure to the next.
    let log = Log::new();
    let (client_side, server_side, _knobs) = support::pair();
    let (handle, client) = handshake::<_, Payload>(client_side).expect("handshake");

    // A gate each, held until both handlers are waiting on one and then opened one after the
    // other, so both answers are queued by the same pass. (A single shared gate would not do
    // it: it remembers one waker, and the second waiter displaces the first, which is then
    // never woken at all.)
    let gates: Vec<Gate> = (0..2).map(|_| Gate::new()).collect();
    let held = gates.clone();
    let server = serve(
        Recorder::new(server_side, log.clone()),
        move |request: http::Request<IncomingBody>| {
            let index: usize = request
                .uri()
                .path()
                .trim_start_matches('/')
                .parse()
                .expect("a numbered path");
            let gate = held[index].clone();
            Box::pin(async move {
                gate.wait().await;
                http::Response::builder()
                    .status(200)
                    .body(failing())
                    .expect("a response")
            }) as BoxedAnswer
        },
    )
    .expect("serve");

    let mut pump = support::BothEnds::new(client, server);
    let mut requests: Vec<_> = (0..2)
        .map(|index| {
            Box::pin(
                handle.send_request(
                    http::Request::builder()
                        .uri(format!("https://example.test/{index}"))
                        .body(empty())
                        .expect("a request"),
                ),
            )
        })
        .collect();
    for request in &mut requests {
        assert!(
            pump.rounds(50, request).is_none(),
            "an exchange settled while its handler was still waiting"
        );
    }

    for gate in &gates {
        gate.open();
    }
    for request in &mut requests {
        pump.rounds(200, request);
    }

    let first = request_stream(0);
    let second = request_stream(1);
    assert_eq!(log.resets(first), vec![REQUEST_CANCELLED]);
    assert_eq!(log.resets(second), vec![REQUEST_CANCELLED]);
    assert_eq!(log.end_markers(first), 0, "{:#?}", log.calls());
    assert_eq!(log.end_markers(second), 0, "{:#?}", log.calls());
    assert_eq!(
        log.transmits_before_reset(first),
        log.transmits_before_reset(second),
        "the two resets should have gone out on the same pass: {:#?}",
        log.calls()
    );
}

#[test]
fn a_caller_abandoning_an_exchange_resets_it_exactly_as_it_always_did() {
    // The reset a failed body owes is drained through the same loop as the reset a caller
    // asks for by dropping a response future, and that loop now also tells the state machine
    // the write side is finished. Every caller of it is abandoning its own send side, so the
    // claim is sound — but it is a claim about someone else's path, so it is asserted rather
    // than reasoned about: the same code, the same stop-sending, and a connection that still
    // works afterwards.
    let (client_side, server_side, _knobs) = support::pair();
    let log = Log::new();
    let (handle, driver) =
        handshake::<_, Payload>(Recorder::new(client_side, log.clone())).expect("handshake");
    let server = Server::new(server_side);

    // A body that never gives anything up, so the request never ends and the peer never
    // answers: the caller gives up first.
    let gate = Gate::new();
    let mut abandoned = Box::pin(
        handle.send_request(
            http::Request::builder()
                .method("POST")
                .uri("https://example.test/abandoned")
                .body(gated(Bytes::from_static(b"never"), gate))
                .expect("a request"),
        ),
    );

    let mut pump = Pump::new(driver, server);
    assert!(
        pump.rounds(20, &mut abandoned).is_none(),
        "the exchange settled without the peer ever answering"
    );
    drop(abandoned);

    let mut later = Box::pin(
        handle.send_request(
            http::Request::builder()
                .uri("https://example.test/after")
                .body(empty())
                .expect("a request"),
        ),
    );
    let answer = pump
        .rounds(400, &mut later)
        .expect("an exchange begun after an abandonment should settle");
    assert!(answer.is_ok());

    let stream = request_stream(0);
    assert_eq!(
        log.resets(stream),
        vec![REQUEST_CANCELLED],
        "an abandoned exchange should be reset once, with the code it always carried"
    );
    assert_eq!(
        log.stops(stream),
        vec![REQUEST_CANCELLED],
        "an abandoned exchange should still ask the peer to stop"
    );
}
