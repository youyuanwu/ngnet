use bytes::Bytes;
use h3_ngnet_qmux_tests::{LIMIT, exchange, memory_pair};
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn consecutive_request_streams_have_exact_ids_and_independent_completion() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let first = exchange(&sender, Bytes::from_static(b"first"));
            let second = exchange(&sender, Bytes::from_static(b"second"));
            let ((_, first_body, first_id), (_, second_body, second_id)) =
                timeout(LIMIT, async { tokio::join!(first, second) })
                    .await
                    .expect("concurrent exchanges");
            assert_eq!(first_body, b"first"[..]);
            assert_eq!(second_body, b"second"[..]);
            assert_ne!(first_id, second_id);
            assert_eq!(first_id.into_inner() & 0x3, 0);
            assert_eq!(second_id.into_inner() & 0x3, 0);
        })
        .await;
}

#[tokio::test]
async fn upstream_control_uni_streams_and_data_first_bidi_stream_complete_together() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let (_, body, id) = timeout(
                LIMIT,
                exchange(&sender, Bytes::from_static(b"data-first request")),
            )
            .await
            .expect("exchange");
            assert_eq!(body, b"data-first request"[..]);
            assert_eq!(id.into_inner(), 0);
        })
        .await;
}
