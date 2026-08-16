//! Many exchanges at once on one connection.

use bytes::Bytes;
use ngnet_qmux_h3_tests::{LIMIT, drain, memory_pair, ok, post};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// Enough exchanges to prove the connection multiplexes rather than serialises.
const REQUESTS: u32 = 8;

#[tokio::test]
async fn eight_concurrent_requests_each_receive_their_own_response() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                let path = request.uri().path().to_owned();
                let (_parts, incoming) = request.into_parts();
                let body = drain(incoming).await.expect("the request body");
                // The responses are answered in the reverse of the order the requests were
                // made. A server that handled one exchange at a time would deadlock on the
                // first, and a transport that muddled its streams would deliver the wrong
                // answer to a request that is still waiting -- which the echo catches,
                // because every response names the request that produced it.
                let index: u32 = path
                    .trim_start_matches("/r")
                    .parse()
                    .expect("a numbered path");
                tokio::time::sleep(core::time::Duration::from_millis(u64::from(
                    (REQUESTS - index) * 5,
                )))
                .await;
                let mut answer = path.into_bytes();
                answer.push(b':');
                answer.extend_from_slice(&body);
                ok(Bytes::from(answer))
            });

            // Every request is submitted before any answer is awaited, so all eight streams
            // are open on the connection at once.
            let mut pending = Vec::new();
            for index in 0..REQUESTS {
                let request = post(
                    &format!("https://qmux.test/r{index}"),
                    format!("body-{index}"),
                );
                pending.push((index, sender.send_request(request)));
            }

            for (index, response) in pending {
                let response = timeout(LIMIT, response)
                    .await
                    .expect("no request may hang")
                    .expect("a response");
                assert_eq!(response.status(), 200);
                let body = timeout(LIMIT, drain(response.into_body()))
                    .await
                    .expect("no body may hang")
                    .expect("a body");
                assert_eq!(
                    body.as_ref(),
                    format!("/r{index}:body-{index}").as_bytes(),
                    "response {index} must be the answer to request {index}"
                );
            }
        })
        .await;
}
