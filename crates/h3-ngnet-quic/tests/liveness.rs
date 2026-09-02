//! The connection keeps driving itself when no request task is alive.
//!
//! This is the regression test for the crate's one genuinely subtle mechanism. The adapter
//! exposes no driver future, so the connection's expiry timer has no obviously-stable task to
//! be armed under. If it were armed under whichever transient task happened to pump last,
//! that task could finish and leave the timer bound to a waker nobody polls — and during a
//! quiet period, with no inbound datagram to rescue it, loss recovery and the idle timeout
//! would never fire. The core owns a stable waker instead; this test is what proves it.
//!
//! The shape matters: the timeout must be observed by a *fresh* poll of the connection, after
//! the task that did the last real work is gone.

use std::sync::Arc;
use std::time::Duration;

use h3::quic::Connection as _;
use ngnet_quic::endpoint::{Config, Endpoint, EndpointBuilder, TokioClock, TokioSocket};
use ngnet_quic::{Duration as QuicDuration, OsslBackend, OsslSession, Role};
use ngnet_quic_h3_tests::{Credentials, H3_ALPN, TEST_SERVER_NAME, TestEntropy};

/// How long the connections under test tolerate silence.
const IDLE: Duration = Duration::from_millis(600);

/// An endpoint pair whose connections lapse quickly, so the timer can be observed.
async fn short_idle_pair() -> (
    h3_ngnet_quic::Connection<OsslSession>,
    h3_ngnet_quic::Connection<OsslSession>,
    Vec<tokio::task::JoinHandle<()>>,
    (Endpoint<OsslSession>, Endpoint<OsslSession>),
) {
    let credentials = Arc::new(Credentials::generate());

    let config = || {
        Config::new()
            .handshake_timeout(QuicDuration::from_nanos(5_000_000_000))
            .max_idle_timeout(QuicDuration::from_nanos(
                u64::try_from(IDLE.as_nanos()).expect("a representable idle timeout"),
            ))
    };

    let server_socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a server socket");
    let address = server_socket.inner().local_addr().expect("a bound address");
    let server_backend = OsslBackend::builder(Role::Server)
        .alpn(H3_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");
    let seeds = Arc::new(std::sync::atomic::AtomicU64::new(0xC0FFEE));
    let (server, server_driver) =
        EndpointBuilder::new(server_socket, TokioClock::new(), server_backend)
            .accepts(true)
            .config(config())
            .entropy(move || {
                TestEntropy::new(seeds.fetch_add(0x9E37_79B9, std::sync::atomic::Ordering::Relaxed))
            })
            .build_detachable()
            .expect("a server endpoint");

    let client_socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a client socket");
    let client_backend = OsslBackend::builder(Role::Client)
        .alpn(H3_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");
    let seeds = Arc::new(std::sync::atomic::AtomicU64::new(0xD15EA5E));
    let (client, client_driver) =
        EndpointBuilder::new(client_socket, TokioClock::new(), client_backend)
            .config(config())
            .entropy(move || {
                TestEntropy::new(seeds.fetch_add(0x9E37_79B9, std::sync::atomic::Ordering::Relaxed))
            })
            .build_detachable()
            .expect("a client endpoint");

    let tasks = vec![
        tokio::spawn(async move {
            let _ = server_driver.await;
        }),
        tokio::spawn(async move {
            let _ = client_driver.await;
        }),
    ];

    let accepting = server.clone();
    let accept = tokio::spawn(async move { accepting.accept_detached().await });
    let connecting = client
        .connect_detached(address, Some(TEST_SERVER_NAME))
        .await
        .expect("a detached client connection");
    let mut client_connection = h3_ngnet_quic::from_detached(connecting);
    tokio::pin!(accept);
    let accepted = std::future::poll_fn(|cx| {
        let _ = client_connection.poll_accept_bidi(cx);
        accept.as_mut().poll(cx)
    })
    .await
    .expect("the accept task")
    .expect("a detached server connection");

    (
        client_connection,
        h3_ngnet_quic::from_detached(accepted),
        tasks,
        (client, server),
    )
}

/// A connection left completely quiet still fires its own idle timeout.
///
/// Nothing arrives on this connection and no task holds a stream, so the *only* thing that can
/// end it is its own expiry timer. If the timer were bound to a dead waker the poll below
/// would stay pending forever and the test would fail on its bound.
#[tokio::test]
async fn a_quiet_connection_still_fires_its_own_idle_timeout() {
    let (mut client, server, tasks, _endpoints) = short_idle_pair().await;

    // Drop the peer's adapter so nothing answers, and let the connection go silent. The peer
    // endpoint stays up, so this is silence rather than a closed socket.
    drop(server);

    let ended = tokio::time::timeout(
        IDLE * 12,
        std::future::poll_fn(|cx| client.poll_accept_bidi(cx)),
    )
    .await
    .expect("the idle timeout must fire without any external event");

    let err = ended.expect_err("a lapsed connection must not yield a stream");
    assert!(
        matches!(err, h3::quic::ConnectionErrorIncoming::Timeout),
        "a connection that lapsed on silence must be reported as a timeout, got {err:?}"
    );
    assert_eq!(
        client.failure(),
        Some(h3_ngnet_quic::Error::IdleTimeout),
        "the adapter must classify the same event as an idle timeout"
    );

    for task in tasks {
        task.abort();
    }
}

/// The timer survives the task that armed it.
///
/// The first poll comes from a task that then goes away; the timeout must still be observed by
/// a later, different poll. This is the exact scenario a per-caller timer waker would break,
/// and it is why the sleep is polled under the core's own waker.
#[tokio::test]
async fn the_expiry_timer_outlives_the_task_that_armed_it() {
    let (client, server, tasks, _endpoints) = short_idle_pair().await;
    drop(server);

    // A short-lived task arms the timer and then finishes.
    let client = Arc::new(tokio::sync::Mutex::new(client));
    {
        let armed = Arc::clone(&client);
        tokio::spawn(async move {
            let mut guard = armed.lock().await;
            let _ = std::future::poll_fn(|cx| {
                let poll = guard.poll_accept_bidi(cx);
                // One pump is all this task does; it must not keep polling.
                std::task::Poll::Ready(poll)
            })
            .await;
        })
        .await
        .expect("the arming task");
    }

    // Nothing polls the connection for a while: only the core's own waker can carry the timer.
    tokio::time::sleep(IDLE * 3).await;

    let mut guard = client.lock().await;
    let ended = tokio::time::timeout(
        IDLE * 12,
        std::future::poll_fn(|cx| guard.poll_accept_bidi(cx)),
    )
    .await
    .expect("the idle timeout must still be observable from a different task");
    let err = ended.expect_err("a lapsed connection must not yield a stream");
    assert!(
        matches!(err, h3::quic::ConnectionErrorIncoming::Timeout),
        "expected a timeout, got {err:?}"
    );

    for task in tasks {
        task.abort();
    }
}
