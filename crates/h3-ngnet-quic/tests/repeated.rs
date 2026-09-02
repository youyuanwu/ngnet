//! Many exchanges on one connection.
//!
//! # Why this file exists
//!
//! Repetition on one connection used to stall this adapter intermittently: the client parked
//! waiting for a response body that never ended, and the connection sat until its 30-second
//! idle timeout. Roughly two runs in five of 200 x 1 KiB exchanges failed that way, at a
//! random exchange index, which made it a race rather than state that accumulated.
//!
//! It was a lost FIN. `ngtcp2_conn_writev_stream` may return a packet that contains no STREAM
//! frame at all, and says so by leaving `*pdatalen` at `-1`; it may also serialise a
//! *zero-length* STREAM frame, which it does exactly when the offer carries nothing but
//! `fin`, and says *that* with `*pdatalen == 0`. The transport wrapper clamped the sign away,
//! so a packet that had gone to an acknowledgement was indistinguishable from one carrying
//! the FIN, and `poll_finish` recorded the stream as ended. Nothing was in flight, so nothing
//! was ever retransmitted. See `crates/ngnet-quic/tests/fin_delivery.rs`, which reproduces
//! that decision deterministically.
//!
//! These tests are the end-to-end gate on it. They are not `#[ignore]`d: a reliability defect
//! that only shows up under repetition is exactly the kind CI has to run.

mod common;

use common::{Pair, body_of, drain_body, within};
use ngnet_quic::OsslSession;

async fn echo_server(connection: h3_ngnet_quic::Connection<OsslSession>) {
    let mut builder = h3::server::builder();
    builder.send_grease(false);
    let mut connection = builder
        .build::<_, bytes::Bytes>(connection)
        .await
        .expect("a hyperium server");
    loop {
        let resolver = match connection.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) | Err(_) => return,
        };
        let (_request, mut stream) = match resolver.resolve_request().await {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        let body = drain_body!(stream);
        if stream
            .send_response(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(())
                    .expect("a response head"),
            )
            .await
            .is_err()
        {
            continue;
        }
        if !body.is_empty() && stream.send_data(body).await.is_err() {
            continue;
        }
        let _ = stream.finish().await;
    }
}

async fn repeated(count: usize, size: usize) {
    let mut pair = Pair::new().await;
    let (client, server) = pair.split();
    let serving = tokio::spawn(echo_server(server));

    let mut builder = h3::client::builder();
    builder.send_grease(false);
    let (mut driver, sender) = builder.build(client).await.expect("a hyperium client");
    let driving = tokio::spawn(async move {
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let body = body_of(size);
    for i in 0..count {
        // Cloned per exchange, exactly as the benchmark fixture does, because hyperium's
        // `send_request` takes `&mut self`. This is not incidental: the shared-handle form of
        // this loop passes while this one does not.
        let mut sender = sender.clone();
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("https://localhost/echo")
            .body(())
            .expect("a request head");
        let mut stream = within(&format!("send_request #{i}"), sender.send_request(request))
            .await
            .unwrap_or_else(|e| panic!("exchange {i}: send_request: {e:?}"));
        if !body.is_empty() {
            within(&format!("send_data #{i}"), stream.send_data(body.clone()))
                .await
                .unwrap_or_else(|e| panic!("exchange {i}: send_data: {e:?}"));
        }
        within(&format!("finish #{i}"), stream.finish())
            .await
            .unwrap_or_else(|e| panic!("exchange {i}: finish: {e:?}"));
        within(&format!("recv_response #{i}"), stream.recv_response())
            .await
            .unwrap_or_else(|e| panic!("exchange {i}: recv_response: {e:?}"));
        let echoed = within(&format!("drain #{i}"), async { drain_body!(stream) }).await;
        assert_eq!(echoed.len(), body.len(), "exchange {i} length");
    }

    serving.abort();
    driving.abort();
}

#[tokio::test]
async fn two_hundred_small_exchanges_on_one_connection() {
    repeated(200, 1024).await;
}

#[tokio::test]
async fn two_hundred_empty_exchanges_on_one_connection() {
    repeated(200, 0).await;
}
