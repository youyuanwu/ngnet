use bytes::Bytes;
use h3_ngnet_qmux_tests::{LIMIT, exchange, exchange_with, memory_pair, socket_pair};
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn non_send_memory_exchange_runs_on_a_local_set() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let (_, body, _) = timeout(LIMIT, exchange(&sender, Bytes::from_static(b"local set")))
                .await
                .expect("local exchange");
            assert_eq!(body, b"local set"[..]);
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sendable_socket_exchange_runs_on_a_work_stealing_runtime() {
    let sender = socket_pair().await;
    let expected = Bytes::from_static(b"work stealing");
    let (_, body, _) = timeout(LIMIT, exchange_with(&sender, expected.clone()))
        .await
        .expect("socket exchange");
    assert_eq!(body, expected);
}
