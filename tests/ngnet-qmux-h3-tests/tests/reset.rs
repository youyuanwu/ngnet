//! A stream the peer resets mid-body, and a connection that survives it.
//!
//! Two claims, and they fail in opposite directions.
//!
//! The first is the whole point of running HTTP/3 over a multiplexed transport: one exchange
//! going wrong is one exchange going wrong. A join that turned a stream reset into a
//! connection failure would take every other request down with it, and the symptom — an
//! unrelated request failing at the same moment — is the kind that gets blamed on the
//! network.
//!
//! The second is that a message the sender abandoned never looks finished to the receiver. A
//! response body that fails is reset rather than ended, so the caller's read fails too; the
//! failure that matters here is the quiet one, where a truncated body arrives as a complete
//! one and, with no content-length to check it against, nothing downstream can tell.
//!
//! Both are asserted with and without a backlog behind the failure, because that is the
//! difference between the two: with bytes still queued the reset has something to discard and
//! the truncation is visible, and with none it is not.

use bytes::Bytes;
use http_body_util::BodyExt;
use ngnet_h3::http::IncomingBody;
use ngnet_qmux_h3_tests::{Failing, LIMIT, drain, get, memory_pair, ok, pattern};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// The backlog the response offers before it fails.
///
/// Sixteen mebibytes against a one-mebibyte connection window cannot all be in flight, so a
/// backlog exists for as long as the caller has not drained it, and the reset that ends the
/// exchange has queued bytes to discard. That is the path this case is here to cover — the
/// one where the peer can see for itself that the message stopped short.
const CHUNK: usize = 256 * 1024;
const CHUNKS: usize = 64;

/// A body with no backlog behind it at all.
///
/// Far below the smallest window either end advertises, so every byte of it is written and
/// gone before the body fails. Nothing is queued when the failure arrives and nothing is
/// discarded by the reset, which is precisely why this case used to pass silently: the
/// caller had already been handed a complete-looking message.
const SMALL: usize = 512;

/// How long the caller waits before reading, so the backlog is still there when it does.
///
/// Comfortably longer than the body's own pause. The dependence is in the safe direction:
/// the case needs the failure to happen *before* the caller drains, and making that margin
/// larger makes it more reliable rather than less. An earlier version read immediately and
/// passed only in debug builds — a release build drained all sixteen mebibytes inside the
/// fifty milliseconds the body waits, which quietly turned this into the small-body case
/// below rather than the backlogged one it is named for.
const SETTLE: core::time::Duration = core::time::Duration::from_millis(500);

/// Reads a body until it ends or fails, reporting how much arrived first.
///
/// The byte count is diagnostic, not a claim. How much of an abandoned body happens to reach
/// the caller before the reset does is a race between the transport and the producer; what
/// these tests assert is how the read *ended*, which is the part the caller acts on.
async fn read_until_failure(
    body: IncomingBody,
) -> (usize, Option<Box<dyn core::error::Error + Send + Sync>>) {
    let mut body = core::pin::pin!(body);
    let mut read = 0;
    loop {
        match body.as_mut().frame().await {
            None => return (read, None),
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    read += data.len();
                }
            }
            Some(Err(error)) => return (read, Some(error.into())),
        }
    }
}

/// Asserts that a body read ended in the peer abandoning the exchange.
fn expect_reset(read: usize, failure: Option<Box<dyn core::error::Error + Send + Sync>>) {
    let Some(error) = failure else {
        panic!(
            "a response body that failed must reach the caller as a failed read; ending it \
             normally hands over the {read} bytes that did arrive as though they were the \
             whole answer, and the caller has no way to know otherwise",
        );
    };
    assert!(
        error.to_string().contains("reset"),
        "and it must say the peer reset the exchange, because that is what tells the caller \
         the answer was abandoned rather than merely interrupted: {error}",
    );
}

#[tokio::test]
async fn a_peer_reset_mid_body_fails_only_its_own_request() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                if request.uri().path() == "/broken" {
                    // Headers, then chunks, then a failure. The HTTP/3 layer answers a
                    // failed response body by resetting that one stream, which is the peer
                    // reset this test is about.
                    return http::Response::builder()
                        .status(200)
                        .body(Failing::new(pattern(CHUNK), CHUNKS).boxed())
                        .expect("a response");
                }
                let (parts, body) = ok("a whole answer").into_parts();
                let body = body.map_err(|never| match never {}).boxed();
                http::Response::from_parts(parts, body)
            });

            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/broken")))
                .await
                .expect("the broken request must not hang")
                .expect("its headers arrive before its body fails");
            assert_eq!(response.status(), 200);

            // Deliberately not read yet. The response body is offered faster than the
            // connection window allows, so a backlog builds; leaving it there until the
            // handler's body has failed is what puts this case on the path where the reset
            // has queued bytes to discard.
            tokio::time::sleep(SETTLE).await;

            let (read, failure) = timeout(LIMIT, read_until_failure(response.into_body()))
                .await
                .expect("the broken body must not hang");
            expect_reset(read, failure);

            // The claim this test is named for. This request is made *after* the reset, on
            // the same connection, and has to work.
            let later = timeout(LIMIT, sender.send_request(get("https://qmux.test/fine")))
                .await
                .expect("the later request must not hang")
                .expect("the connection must still be usable");
            assert_eq!(later.status(), 200);
            let body = timeout(LIMIT, drain(later.into_body()))
                .await
                .expect("the later body must not hang")
                .expect("a body");
            assert_eq!(body, Bytes::from_static(b"a whole answer"));
        })
        .await;
}

#[tokio::test]
async fn a_response_body_that_fails_with_nothing_queued_behind_it_still_fails_the_callers_read() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                if request.uri().path() == "/broken" {
                    // One chunk small enough to be written and gone, and then a failure with
                    // nothing left behind it. Everything the peer will ever receive on this
                    // stream has already reached it by the time the body fails, so the only
                    // thing that can tell the peer the message is not the whole message is
                    // the reset itself.
                    return http::Response::builder()
                        .status(200)
                        .body(Failing::new(pattern(SMALL), 1).boxed())
                        .expect("a response");
                }
                let (parts, body) = ok("a whole answer").into_parts();
                let body = body.map_err(|never| match never {}).boxed();
                http::Response::from_parts(parts, body)
            });

            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/broken")))
                .await
                .expect("the broken request must not hang")
                .expect("its headers arrive before its body fails");
            assert_eq!(response.status(), 200);

            // Read straight away, unlike the backlogged case: waiting would only give the
            // transport time it does not need, and there is nothing to hold back.
            let (read, failure) = timeout(LIMIT, read_until_failure(response.into_body()))
                .await
                .expect("the broken body must not hang");
            expect_reset(read, failure);

            // A stream abandoned this way is left suspended until the reset goes out, so a
            // later request is also the check that nothing is stuck waiting on it: if the
            // reset never reached the transport, this is where it would show, as a request
            // that never completes rather than as a wrong answer.
            let later = timeout(LIMIT, sender.send_request(get("https://qmux.test/fine")))
                .await
                .expect("the later request must not hang")
                .expect("the connection must still be usable");
            assert_eq!(later.status(), 200);
            let body = timeout(LIMIT, drain(later.into_body()))
                .await
                .expect("the later body must not hang")
                .expect("a body");
            assert_eq!(body, Bytes::from_static(b"a whole answer"));
        })
        .await;
}
