use bytes::Bytes;
use ngnet_bench::{NgnetH3Quinn, UpstreamH3Quinn};

#[tokio::test(flavor = "current_thread")]
async fn both_quinn_fixtures_echo_the_same_body() {
    let body = Bytes::from(vec![0x5a; 64 * 1024]);

    let ngnet = NgnetH3Quinn::establish().await;
    assert_eq!(ngnet.round_trip(body.clone()).await, body.len());

    let upstream = UpstreamH3Quinn::establish().await;
    assert_eq!(upstream.round_trip(body).await, 64 * 1024);
}
