//! Shutdown: saying goodbye, and knowing when the goodbye is over.
//!
//! The interesting property is not that shutdown ends the connections — it is that when
//! `shutdown` *returns*, they are already gone. A pool that sets a flag and returns has
//! reported a drain it did not observe, and the only way to tell the two apart is to ask the
//! peer what it received before the call came back.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use ngnet_util::{Client, ErrorKind};
use support::raw::{Behaviour, RawPeer};
use support::{TestServer, get, until, within};

#[tokio::test]
async fn shutdown_says_goodbye_on_the_wire() {
    // Asserted at the peer, on the wire, because at this end "the connection ended" and "the
    // connection was ended politely" look identical. A pool that dropped its sockets would
    // pass every other test in this file.
    let peer = RawPeer::start(Behaviour::Answer).await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the request", client.request(get(peer.uri("/one"))))
        .await
        .expect("the peer answers");

    within("shutdown", client.shutdown()).await;

    // Polled, and the first draft of this test was not — it asserted the frame was already
    // recorded the instant `shutdown` returned, and failed. The reason is worth keeping: the
    // bytes really are written before `shutdown` resolves, because it awaits the driver that
    // writes them, but *reading* them is the peer's own task's work and that task had not
    // been scheduled yet. The claim being made is about what the client sent. Asserting on
    // when a second task happened to notice would have been asserting on the scheduler.
    //
    // Note also that no further client activity happens after this point. If the `GOAWAY`
    // were not already on the wire, nothing would ever put it there and this would time out.
    until("the peer to read the GOAWAY", || {
        peer.connections()[0].saw_goaway()
    })
    .await;
}

#[tokio::test]
async fn shutdown_waits_for_an_exchange_already_in_flight() {
    // The claim `shutdown` actually makes: not that the connections were told to go, but
    // that they have *gone* — including the work that was running on them. A pool that set
    // its flag and returned would pass every other test here.
    //
    // The peer holds the response body back, so there is a real exchange in flight at the
    // moment shutdown is called, and the flag says whether it had finished when shutdown
    // came back.
    let peer = RawPeer::start(Behaviour::AnswerInTwoParts { delay_ms: 100 }).await;
    let client: Client<Full<Bytes>> = Client::new();

    let response = within("the response head", client.request(get(peer.uri("/slow"))))
        .await
        .expect("the head arrives");

    let finished = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&finished);
    let reader = tokio::spawn(async move {
        let body = response
            .into_body()
            .collect()
            .await
            .expect("the body completes")
            .to_bytes();
        flag.store(true, Ordering::SeqCst);
        body
    });

    within("shutdown", client.shutdown()).await;

    assert!(
        finished.load(Ordering::SeqCst),
        "shutdown returned while an exchange was still running"
    );
    assert_eq!(
        within("the reader", reader)
            .await
            .expect("the task completes"),
        Bytes::from_static(b"late")
    );
}

#[tokio::test]
async fn a_request_offered_after_shutdown_is_refused_as_closed() {
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first request", client.request(get(server.uri("/one"))))
        .await
        .expect("succeeds");

    within("shutdown", client.shutdown()).await;

    let error = within("the late request", client.request(get(server.uri("/two"))))
        .await
        .expect_err("the client is closed");

    assert_eq!(error.kind(), ErrorKind::Closed);
    assert!(
        !error.is_retriable(),
        "retrying against a closed client would fail identically for ever"
    );
    assert_eq!(
        server.accepts(),
        1,
        "a request refused for being late must not have dialled on its way to being refused"
    );
}

#[tokio::test]
async fn a_request_to_a_cold_origin_after_shutdown_dials_nothing() {
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    within("shutdown", client.shutdown()).await;

    let error = within("the request", client.request(get(server.uri("/one"))))
        .await
        .expect_err("the client is closed");

    assert_eq!(error.kind(), ErrorKind::Closed);
    assert_eq!(server.accepts(), 0);
}

#[tokio::test]
async fn shutdown_on_one_clone_closes_every_clone() {
    // Clones share a pool, which means they share a shutdown. This is a property worth
    // pinning rather than an implementation detail: a caller who clones a client into ten
    // tasks needs to know whether that made ten clients or one.
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();
    let clone = client.clone();

    let _ = within("the first request", client.request(get(server.uri("/one"))))
        .await
        .expect("succeeds");

    within("shutdown on the original", client.shutdown()).await;

    assert!(clone.is_closed());
    let error = within(
        "the clone's request",
        clone.request(get(server.uri("/two"))),
    )
    .await
    .expect_err("the clone is closed too");
    assert_eq!(error.kind(), ErrorKind::Closed);
}

#[tokio::test]
async fn concurrent_shutdowns_all_report_the_completed_drain() {
    // The second caller must not return early on the strength of the flag already being set.
    // Every one of these has to observe the drain, not merely learn that one is happening —
    // so every one of them must find the `GOAWAY` already sent when it returns.
    let peer = RawPeer::start(Behaviour::Answer).await;
    let client: Arc<Client<Full<Bytes>>> = Arc::new(Client::new());

    let _ = within("the request", client.request(get(peer.uri("/one"))))
        .await
        .expect("the peer answers");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client = Arc::clone(&client);
        tasks.push(tokio::spawn(async move { client.shutdown().await }));
    }

    for task in tasks {
        within("a concurrent shutdown", task)
            .await
            .expect("the task completes");
    }

    until("the peer to read the GOAWAY", || {
        peer.connections()[0].saw_goaway()
    })
    .await;
    assert!(client.is_closed());
}

#[tokio::test]
async fn shutdown_with_nothing_pooled_returns() {
    // Trivial, and the sort of thing that deadlocks: the leader waits for an acquire count
    // that is already zero and for a drain over an empty map, and then every caller waits on
    // a completion the leader has to remember to publish.
    let client: Client<Full<Bytes>> = Client::new();
    within("shutdown", client.shutdown()).await;
    within("the second shutdown", client.shutdown()).await;
    assert!(client.is_closed());
}

#[tokio::test]
async fn dropping_the_client_does_not_cancel_an_exchange_in_flight() {
    // The reason the pool keeps `Vec<JoinHandle>` and not a `JoinSet`. `JoinSet` aborts its
    // tasks when dropped, so a pool holding one would cancel the driver — and with it any
    // response still arriving — the moment the last `Client` went away.
    //
    // The peer sends the head at once and the body a moment later, which is what makes the
    // difference observable. With the whole response already buffered, an aborted driver and
    // a live one look the same.
    let peer = RawPeer::start(Behaviour::AnswerInTwoParts { delay_ms: 50 }).await;
    let client: Client<Full<Bytes>> = Client::new();

    let response = within("the response head", client.request(get(peer.uri("/slow"))))
        .await
        .expect("the head arrives");

    // Every user-facing handle to the pool is gone from here on. Only the exchange remains.
    drop(client);

    let body = within("the rest of the body", response.into_body().collect())
        .await
        .expect("the body completes after the client was dropped")
        .to_bytes();
    assert_eq!(body, Bytes::from_static(b"late"));
}
