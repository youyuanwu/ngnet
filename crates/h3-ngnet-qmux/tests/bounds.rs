mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use h3::quic;
use h3_ngnet_qmux::from_qmux;
use ngnet_qmux::io::Connection as QmuxConnection;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux::io::{Config, OUTBOUND_CEILING};

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn pending_accepts_never_exceed_the_configured_limit() {
    let adapter = h3_ngnet_qmux::AdapterConfig::new().pending_accept_limit(2);
    let (mut client, mut client_driver, server, mut server_driver) =
        common::pair_with(Config::new(), adapter, adapter);
    let client_task = async {
        let mut first = common::open_bidi(&mut client).await;
        let mut second = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut first, Bytes::from_static(b"a")).await;
        common::send_all_unframed(&mut second, Bytes::from_static(b"b")).await;
        (first, second)
    };
    let server_task = async {
        std::future::poll_fn(|cx| {
            if server.snapshot().pending_accepts == 2 {
                std::task::Poll::Ready(server.snapshot())
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await
    };
    let ((first, second), snapshot) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    drop((first, second));
    assert_eq!(snapshot.pending_accepts, 2);
    assert_eq!(snapshot.streams, 2);
}

#[test]
fn receive_and_lower_output_accounting_stay_within_documented_bounds() {
    let lower = Config::new()
        .initial_max_stream_data(32)
        .initial_max_data(32)
        .read_ahead(32);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(&[7; 96])).await;
        common::finish(&mut stream).await;
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        let body = common::receive_all(&mut stream).await;
        (body, server.snapshot())
    };
    let (_, (body, snapshot)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(body, vec![7; 96]);
    assert!(snapshot.receive_bytes <= 32 + ngnet_qmux::DEFAULT_MAX_RECORD_SIZE as usize);
    assert!(snapshot.lower_queued_output <= OUTBOUND_CEILING);
}

#[test]
fn eligible_stream_entries_are_reclaimed_after_both_halves_drop() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::finish(&mut stream).await;
        drop(stream);
        client.snapshot()
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        let _ = common::receive_all(&mut stream).await;
        drop(stream);
        server.snapshot()
    };
    let (_client_snapshot, _server_snapshot) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
    }
    let client_snapshot = client.snapshot();
    let server_snapshot = server.snapshot();
    assert_eq!(client_snapshot.streams, 0);
    assert_eq!(server_snapshot.streams, 0);
    assert_eq!(client_snapshot.pending_accepts, 0);
    assert_eq!(server_snapshot.pending_accepts, 0);
}

#[test]
#[cfg(debug_assertions)]
fn one_driver_poll_routes_at_most_sixty_four_events_from_one_lower_batch() {
    let (client_io, server_io) = stream_pair();
    let reads = server_io.read_log();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server");
    let (mut client, mut client_driver) = from_qmux::<Bytes, _, _>(client_lower);
    let (server, mut server_driver) = from_qmux::<Bytes, _, _>(server_lower);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
    }
    let mut streams = Vec::new();
    for value in 0..100_u8 {
        let mut stream = loop {
            match quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut cx) {
                Poll::Ready(Ok(stream)) => break stream,
                Poll::Ready(Err(error)) => panic!("open failed: {error}"),
                Poll::Pending => {
                    let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
                }
            }
        };
        let mut byte = Bytes::copy_from_slice(&[value]);
        assert!(matches!(
            quic::SendStreamUnframed::poll_send(&mut stream, &mut cx, &mut byte),
            Poll::Ready(Ok(1))
        ));
        streams.push(stream);
    }

    reads.clear();
    let routed_before = server.snapshot().routed_events;
    let continuation = Arc::new(Count::default());
    let driver_waker = Waker::from(Arc::clone(&continuation));
    let mut driver_cx = Context::from_waker(&driver_waker);
    assert!(matches!(
        std::pin::Pin::new(&mut server_driver).poll(&mut driver_cx),
        Poll::Pending
    ));
    let snapshot = server.snapshot();
    assert_eq!(reads.reads(), 1);
    assert_eq!(snapshot.routed_events - routed_before, 64);
    assert!(snapshot.pending_accepts <= 64);
    assert_eq!(
        continuation.0.load(Ordering::SeqCst),
        1,
        "positive progress at the routing budget schedules one continuation"
    );
    drop(streams);
}

#[test]
fn dropping_with_more_than_one_route_budget_in_flight_does_not_rediscover_the_stream() {
    let (client_io, server_io) = stream_pair();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server");
    let (mut client, mut client_driver) = from_qmux::<Bytes, _, _>(client_lower);
    let (mut server, mut server_driver) = from_qmux::<Bytes, _, _>(server_lower);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
    }

    let mut stream = loop {
        match quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut cx) {
            Poll::Ready(Ok(stream)) => break stream,
            Poll::Ready(Err(error)) => panic!("open failed: {error}"),
            Poll::Pending => {
                let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
            }
        }
    };
    for value in 0..100_u8 {
        let mut byte = Bytes::copy_from_slice(&[value]);
        assert!(matches!(
            quic::SendStreamUnframed::poll_send(&mut stream, &mut cx, &mut byte),
            Poll::Ready(Ok(1))
        ));
    }
    loop {
        match quic::SendStream::poll_finish(&mut stream, &mut cx) {
            Poll::Ready(Ok(())) => break,
            Poll::Ready(Err(error)) => panic!("finish failed: {error}"),
            Poll::Pending => {
                let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
            }
        }
    }

    let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
    let mut accepted = match quic::Connection::<Bytes>::poll_accept_bidi(&mut server, &mut cx) {
        Poll::Ready(Ok(stream)) => stream,
        Poll::Ready(Err(error)) => panic!("accept failed: {error}"),
        Poll::Pending => panic!("expected routed peer stream"),
    };
    assert!(matches!(
        quic::RecvStream::poll_data(&mut accepted, &mut cx),
        Poll::Ready(Ok(Some(_)))
    ));
    drop(accepted);
    drop(stream);

    for _ in 0..64 {
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
    }
    assert_eq!(server.snapshot().pending_accepts, 0);
    assert_eq!(server.snapshot().streams, 0);
}
