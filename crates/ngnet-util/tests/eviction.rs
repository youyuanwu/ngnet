//! Eviction: what the pool does when a connection stops being one it should use again.
//!
//! These tests use the hand-written peer in `support::raw` rather than hyper's server, for
//! the reason given there: the frames involved are ones a well-behaved server will not send.

mod support;

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::{Client, ErrorKind};
use support::raw::{Behaviour, RawPeer};
use support::{get, until, within};

#[tokio::test]
async fn a_request_is_answered_by_the_hand_written_peer() {
    // The peer's own smoke test. Every other test in this file asserts something about a
    // peer that misbehaves, and a peer that cannot answer a request at all would make all of
    // them pass for the wrong reason.
    let peer = RawPeer::start(Behaviour::Answer).await;
    let client: Client<Full<Bytes>> = Client::new();

    let response = within("the request", client.request(get(peer.uri("/one"))))
        .await
        .expect("the peer answers");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(peer.accepts(), 1);
    assert_eq!(peer.connections()[0].requests, 1);
}

#[tokio::test]
async fn a_goaway_evicts_the_connection_and_the_next_request_redials() {
    // One request answered, then `GOAWAY`. The peer keeps the socket open afterwards, which
    // is what makes this a test of the pool noticing the *frame* rather than the pool
    // noticing a disconnection — a much weaker property that a far worse implementation has.
    let peer = RawPeer::start(Behaviour::AnswerThenGoAway {
        answer: 1,
        last_stream: 1,
    })
    .await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first request", client.request(get(peer.uri("/one"))))
        .await
        .expect("the peer answers the first");
    assert_eq!(peer.accepts(), 1);

    // Waiting on the actual state change rather than on a guess about how long it takes.
    // The peer cannot tell us the client has read its `GOAWAY`; it learns nothing after
    // sending one.
    until("the client to observe the GOAWAY", || {
        !ngnet_util::testing::has_eligible_connection(&client, &peer.authority())
    })
    .await;

    let _ = within("the second request", client.request(get(peer.uri("/two"))))
        .await
        .expect("the second request gets a fresh connection");

    assert_eq!(
        peer.accepts(),
        2,
        "a request arriving after a GOAWAY must be carried on a new connection"
    );
    let connections = peer.connections();
    assert_eq!(connections[0].requests, 1);
    assert_eq!(connections[1].requests, 1);
}

#[tokio::test]
async fn one_eviction_produces_one_replacement_however_many_callers_notice_it() {
    // The race the sequential test above cannot reach. Ten callers all find the same dead
    // connection at the same moment; a pool that replaces it per caller opens ten sockets and
    // still answers every request, so only the accept count catches it.
    let peer = RawPeer::start(Behaviour::FirstRetires {
        answer: 1,
        last_stream: 1,
    })
    .await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first request", client.request(get(peer.uri("/warm"))))
        .await
        .expect("the peer answers the first");

    until("the client to observe the GOAWAY", || {
        !ngnet_util::testing::has_eligible_connection(&client, &peer.authority())
    })
    .await;

    let mut tasks = Vec::new();
    for index in 0..10 {
        let client = client.clone();
        let uri = peer.uri(&format!("/replaced-{index}"));
        tasks.push(tokio::spawn(async move {
            client.request(get(uri)).await.map(|_| ())
        }));
    }
    for task in tasks {
        within("a concurrent replacement request", task)
            .await
            .expect("the task completes")
            .expect("the request succeeds");
    }

    assert_eq!(
        peer.accepts(),
        2,
        "ten callers finding one dead connection must produce one replacement"
    );
}

#[tokio::test]
async fn a_stream_the_peer_refused_is_reported_as_retriable() {
    // `GOAWAY(0)` says no stream was ever processed, so a request on stream 1 was provably
    // never acted on. That is the only circumstance in which replaying it would be safe, and
    // the error has to say so — even though this crate deliberately does not act on it.
    //
    // It does not act on it because `send_request` consumes the request and returns only a
    // response future: there is no error path that hands it back. Retrying would mean copying
    // every request against the chance that one was refused. The flag is reported so the
    // caller, who still has the request, can decide.
    let peer = RawPeer::start(Behaviour::RefuseEverything).await;
    let client: Client<Full<Bytes>> = Client::new();

    let error = within("the request", client.request(get(peer.uri("/refused"))))
        .await
        .expect_err("the peer refuses everything");

    assert_eq!(error.kind(), ErrorKind::Exchange);
    assert!(
        error.is_retriable(),
        "a stream refused by GOAWAY(0) was never acted on, so it is safe to replay: {error}"
    );
}

#[tokio::test]
async fn a_refusing_connection_is_not_handed_to_the_next_caller() {
    // The eviction rule as seen from a caller rather than from the pool. A connection that
    // refused a request will refuse the next one too, so handing it out again converts one
    // failure into an unbounded run of them.
    let peer = RawPeer::start(Behaviour::RefuseEverything).await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first request", client.request(get(peer.uri("/first"))))
        .await
        .expect_err("the peer refuses everything");

    until("the client to retire the refusing connection", || {
        !ngnet_util::testing::has_eligible_connection(&client, &peer.authority())
    })
    .await;

    let _ = within(
        "the second request",
        client.request(get(peer.uri("/second"))),
    )
    .await
    .expect_err("the fresh connection refuses too");

    assert_eq!(
        peer.accepts(),
        2,
        "the second request must not be sent down a connection already known to refuse"
    );
}
