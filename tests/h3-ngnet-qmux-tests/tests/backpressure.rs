use bytes::Bytes;
use h3_ngnet_qmux_tests::{LIMIT, MemoryIoConfig, exchange, memory_pair_with};
use ngnet_qmux::io::Config;
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn fragmented_partial_lower_io_preserves_a_body_larger_than_both_windows() {
    LocalSet::new()
        .run_until(async {
            let transport = Config::new()
                .initial_max_stream_data(257)
                .initial_max_data(509)
                .read_ahead(509);
            let io = MemoryIoConfig {
                read_cap: Some(31),
                write_cap: Some(37),
                capacity: Some(257),
            };
            let (sender, _, _) = memory_pair_with(transport, io).await;
            let expected = Bytes::from(
                (0..4_097)
                    .map(|index| (index % 251) as u8)
                    .collect::<Vec<_>>(),
            );
            let (_, body, _) = timeout(LIMIT, exchange(&sender, expected.clone()))
                .await
                .expect("fragmented exchange");
            assert_eq!(body, expected);
        })
        .await;
}

#[tokio::test]
async fn separate_stream_and_connection_windows_restore_without_loss() {
    LocalSet::new()
        .run_until(async {
            for transport in [
                Config::new()
                    .initial_max_stream_data(64)
                    .initial_max_data(4096),
                Config::new()
                    .initial_max_stream_data(4096)
                    .initial_max_data(64),
            ] {
                let (sender, _, _) =
                    memory_pair_with(transport.read_ahead(4096), MemoryIoConfig::default()).await;
                let expected = Bytes::from(vec![0x5a; 16_385]);
                let (_, body, _) = timeout(LIMIT, exchange(&sender, expected.clone()))
                    .await
                    .expect("window restoration");
                assert_eq!(body, expected);
            }
        })
        .await;
}
