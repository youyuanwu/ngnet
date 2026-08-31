use bytes::Bytes;
use h3::error::Code;
use h3_ngnet_qmux_tests::{LIMIT, exchange, memory_pair};
use http::Request;
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn abandoned_request_stream_does_not_break_an_unrelated_sibling() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let mut opener = sender.clone();
            let abandoned = opener
                .send_request(
                    Request::builder()
                        .method("POST")
                        .uri("https://qmux.test/abandoned")
                        .body(())
                        .expect("request"),
                )
                .await
                .expect("open abandoned request");
            drop(abandoned);

            let expected = Bytes::from_static(b"unaffected sibling");
            let (_, body, _) = timeout(LIMIT, exchange(&sender, expected.clone()))
                .await
                .expect("sibling exchange");
            assert_eq!(body, expected);
        })
        .await;
}

#[tokio::test]
async fn explicit_stop_and_split_drop_leave_a_stable_stream_outcome() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let mut opener = sender.clone();
            let mut stream = opener
                .send_request(
                    Request::builder()
                        .method("POST")
                        .uri("https://qmux.test/stopped")
                        .body(())
                        .expect("request"),
                )
                .await
                .expect("open request");
            stream
                .send_data(Bytes::from_static(b"partial"))
                .await
                .expect("data");
            let (send, mut recv) = stream.split();
            recv.stop_sending(Code::H3_REQUEST_CANCELLED);
            drop(recv);
            drop(send);

            let (_, body, _) =
                timeout(LIMIT, exchange(&sender, Bytes::from_static(b"still works")))
                    .await
                    .expect("sibling exchange");
            assert_eq!(body, b"still works"[..]);
        })
        .await;
}
