use bytes::Bytes;
use h3::error::Code;
use h3_ngnet_qmux_tests::{LIMIT, MemoryIoConfig, exchange, memory_pair, memory_pair_with};
use http::Request;
use ngnet_qmux::io::Config;
use ngnet_qmux::io::testing::Fault;
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

#[tokio::test]
async fn peer_reset_preserves_the_exact_application_code_and_a_sibling_survives() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let mut opener = sender.clone();
            let mut reset = opener
                .send_request(
                    Request::builder()
                        .method("POST")
                        .uri("https://qmux.test/reset")
                        .header("x-qmux-test", "reset")
                        .body(())
                        .expect("request"),
                )
                .await
                .expect("open reset request");
            reset.finish().await.expect("finish reset request");
            let error = reset
                .recv_response()
                .await
                .expect_err("server resets response");
            assert!(matches!(
                error,
                h3::error::StreamError::RemoteTerminate { code }
                    if code == Code::H3_REQUEST_CANCELLED
            ));

            let (_, body, _) = exchange(&sender, Bytes::from_static(b"sibling")).await;
            assert_eq!(body, b"sibling"[..]);
        })
        .await;
}

#[tokio::test]
async fn graceful_peer_close_is_observed_after_the_final_response() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let mut opener = sender.clone();
            let mut closing = opener
                .send_request(
                    Request::builder()
                        .method("POST")
                        .uri("https://qmux.test/close")
                        .header("x-qmux-test", "close")
                        .body(())
                        .expect("request"),
                )
                .await
                .expect("open closing request");
            closing.finish().await.expect("finish request");
            let response = closing.recv_response().await.expect("final response");
            assert_eq!(response.status(), 200);
            while closing.recv_data().await.expect("final data").is_some() {}

            let mut next = sender.clone();
            let result = timeout(
                LIMIT,
                next.send_request(
                    Request::builder()
                        .method("GET")
                        .uri("https://qmux.test/after-close")
                        .body(())
                        .expect("request"),
                ),
            )
            .await
            .expect("close observation");
            assert!(matches!(result, Err(h3::error::StreamError::RemoteClosing)));
        })
        .await;
}

#[tokio::test]
async fn lower_io_failure_fans_out_as_a_stable_connection_error() {
    LocalSet::new()
        .run_until(async {
            let (sender, client_fault, _) =
                memory_pair_with(Config::new(), MemoryIoConfig::default()).await;
            let _ = exchange(&sender, Bytes::new()).await;
            client_fault.inject(Fault::Broken);

            // Handles are deliberately lower-I/O-free: the first immediate operation may be
            // accepted into Core before the central driver observes the injected substrate
            // failure. It exists to wake that driver; every operation after the driver turn
            // must see the same terminal category.
            let mut trigger = sender.clone();
            if let Ok(stream) = trigger
                .send_request(
                    Request::builder()
                        .method("GET")
                        .uri("https://qmux.test/failure-trigger")
                        .body(())
                        .expect("request"),
                )
                .await
            {
                drop(stream);
            }
            tokio::task::yield_now().await;

            let mut category = None;
            for attempt in 0..2 {
                let mut next = sender.clone();
                let result = timeout(
                    LIMIT,
                    next.send_request(
                        Request::builder()
                            .method("GET")
                            .uri("https://qmux.test/failure")
                            .body(())
                            .expect("request"),
                    ),
                )
                .await
                .expect("failure observation");
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("lower failure is terminal on observation {attempt}"),
                };
                let current = std::mem::discriminant(&error);
                if let Some(category) = category {
                    assert_eq!(current, category);
                } else {
                    category = Some(current);
                }
            }
        })
        .await;
}
