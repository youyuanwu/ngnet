//! A stream the peer resets mid-body, and a connection that survives it.
//!
//! The distinction this test draws is the whole point of running HTTP/3 over a multiplexed
//! transport: one exchange going wrong is one exchange going wrong. A join that turned a
//! stream reset into a connection failure would take every other request down with it, and
//! the symptom — an unrelated request failing at the same moment — is the kind that gets
//! blamed on the network.

use bytes::Bytes;
use http_body_util::BodyExt;
use ngnet_h3::http::IncomingBody;
use ngnet_qmux_h3_tests::{Failing, LIMIT, drain, get, memory_pair, ok, pattern};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// The backlog the response offers before it fails.
///
/// The reset only discards something while there is something to discard, and that is what
/// makes the failure land *mid-body*. Sixteen mebibytes against a one-mebibyte connection
/// window cannot all be in flight, so a backlog exists for as long as the caller has not
/// drained it.
///
/// That last clause is the whole reason [`SETTLE`] exists below. An earlier version of this
/// test read the body immediately and passed only in debug builds: a release build drained
/// all sixteen mebibytes inside the fifty milliseconds the body waits before failing, leaving
/// no backlog and therefore no mid-body reset. The test was timing-dependent in the unsafe
/// direction — it needed the transport to be *slower* than the producer.
const CHUNK: usize = 256 * 1024;
const CHUNKS: usize = 64;
const OFFERED: usize = CHUNK * CHUNKS;

/// How long the caller waits before reading, so the reset provably precedes the read.
///
/// Comfortably longer than the body's own pause. The dependence is now in the safe direction:
/// the test needs the failure to happen *before* the caller drains, and making that margin
/// larger makes the test more reliable rather than less.
const SETTLE: core::time::Duration = core::time::Duration::from_millis(500);

/// Reads a body until it ends or fails, reporting how much arrived first.
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

#[tokio::test]
async fn a_peer_reset_mid_body_fails_only_its_own_request() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                if request.uri().path() == "/broken" {
                    // Headers, then a chunk, then a failure. The HTTP/3 layer answers a
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
            // handler's body has failed is what makes this a reset with something to discard
            // rather than a reset after a complete delivery.
            tokio::time::sleep(SETTLE).await;

            let (read, failure) = timeout(LIMIT, read_until_failure(response.into_body()))
                .await
                .expect("the broken body must not hang");
            let error = failure.expect(
                "a reset mid-body must reach the caller as a failed body; reporting a short \
                 body as a success would have the caller act on half an answer",
            );
            assert!(
                error.to_string().contains("reset"),
                "and it must say the peer reset the exchange: {error}",
            );
            assert!(
                read > 0 && read < OFFERED,
                "the failure must arrive partway through the body, not before it or after \
                 it: {read} of {OFFERED} bytes",
            );

            // The claim under test. This request is made *after* the reset, on the same
            // connection, and has to work.
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
