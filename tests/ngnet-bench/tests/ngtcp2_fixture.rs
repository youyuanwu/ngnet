use bytes::Bytes;
use ngnet_bench::NgnetNgtcpH3;

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_echoes_the_complete_body() {
    let body = Bytes::from(vec![0x5a; 64 * 1024]);
    let fixture = NgnetNgtcpH3::establish().await;

    assert_eq!(fixture.round_trip(body.clone()).await, body.len());
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_reuses_more_than_the_initial_stream_limit() {
    let fixture = NgnetNgtcpH3::establish().await;

    for exchange in 0..125 {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            fixture.round_trip(Bytes::new()),
        )
        .await
        .unwrap_or_else(|_| panic!("exchange {exchange} stalled after stream credit ran out"));
    }
}
