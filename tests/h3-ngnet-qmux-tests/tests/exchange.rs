use bytes::Bytes;
use h3_ngnet_qmux_tests::{LIMIT, exchange, memory_pair};
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn first_empty_request_on_a_fresh_connection_completes() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let (response, body, id) = timeout(LIMIT, exchange(&sender, Bytes::new()))
                .await
                .expect("exchange timeout");
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers()["content-type"],
                "application/octet-stream"
            );
            assert_eq!(response.headers()["x-qmux-test"], "round-trip");
            assert!(body.is_empty());
            assert_eq!(id.into_inner(), 0);
        })
        .await;
}

#[tokio::test]
async fn request_and_response_preserve_a_nonempty_body_exactly() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let expected = Bytes::from_static(b"the exact request and response body");
            let (response, body, _) = timeout(LIMIT, exchange(&sender, expected.clone()))
                .await
                .expect("exchange timeout");
            assert_eq!(response.status(), 200);
            assert_eq!(body, expected);
        })
        .await;
}
