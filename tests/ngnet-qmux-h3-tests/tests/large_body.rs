//! A body far larger than one record and far larger than the initial windows.
//!
//! Three bounds are crossed deliberately. The payload is more than ten times the largest
//! record QMux will produce, so it is fragmented rather than sent whole; more than twice the
//! per-stream flow-control window, so finishing it requires the receiver to return credit
//! mid-transfer; and more than twice the connection window, so the credit has to be returned
//! at both levels. A join that extended only one of the two stalls here and nowhere else.

use ngnet_qmux_h3_tests::{LIMIT, drain, memory_pair, ok, pattern, post};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// 2.5 MiB: ten times the 16382-byte record limit over and over, twice the 1 MiB connection
/// window, and ten times the 256 KiB stream window.
const SIZE: usize = 2_621_440;

#[tokio::test]
async fn a_body_larger_than_the_windows_completes_in_both_directions() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                let (_parts, incoming) = request.into_parts();
                let received = drain(incoming).await.expect("the request body");
                assert_eq!(received.len(), SIZE, "the request body must arrive whole");
                assert_eq!(received, pattern(SIZE), "and byte for byte");
                ok(pattern(SIZE))
            });

            let response = timeout(
                LIMIT,
                sender.send_request(post("https://qmux.test/large", pattern(SIZE))),
            )
            .await
            .expect("the request must not hang")
            .expect("a response");

            assert_eq!(response.status(), 200);
            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");

            assert_eq!(body.len(), SIZE, "the response body must arrive whole");
            assert_eq!(body, pattern(SIZE), "and byte for byte");
        })
        .await;
}
