#![cfg(feature = "diagnostics")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use bytes::{Buf, Bytes};
use h3::proto::frame::Frame;
use h3::quic;
use h3_ngnet_qmux::diagnostics::{self, observe};
use h3_ngnet_qmux::from_qmux;
use ngnet_qmux::io::testing::{Fault, TestClock, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Config, Connection as QmuxConnection, Written};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct OracleCounts {
    read_calls: u64,
    read_bytes: u64,
    write_calls: u64,
    write_bytes: u64,
    write_not_now: u64,
    shutdown_calls: u64,
    failures: u64,
}

struct Oracle<S> {
    inner: S,
    counts: Arc<Mutex<OracleCounts>>,
}

#[derive(Default)]
struct TestWake(AtomicUsize);

impl Wake for TestWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl<S: AsyncByteStream> AsyncByteStream for Oracle<S> {
    type Error = S::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let result = self.inner.poll_read(cx, buffer);
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts.read_calls += 1;
        match &result {
            Poll::Ready(Ok(bytes)) => counts.read_bytes += *bytes as u64,
            Poll::Ready(Err(_)) => counts.failures += 1,
            Poll::Pending => {}
        }
        result
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        let result = self.inner.poll_write(cx, buffer);
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts.write_calls += 1;
        match &result {
            Poll::Ready(Ok(Written::Accepted(bytes))) => counts.write_bytes += *bytes as u64,
            Poll::Ready(Ok(Written::NotNow)) => counts.write_not_now += 1,
            Poll::Ready(Err(_)) => counts.failures += 1,
            Poll::Pending => {}
        }
        result
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_shutdown(cx);
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts.shutdown_calls += 1;
        if matches!(result, Poll::Ready(Err(_))) {
            counts.failures += 1;
        }
        result
    }
}

#[test]
fn lower_oracle_reconciles_calls_bytes_partial_not_now_and_shutdown() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (near, _far) = stream_pair();
    near.set_read_cap(Some(2));
    near.set_capacity(Some(2));
    near.deliver(b"abcdef");
    let fault = near.fault_control();
    let read_log = near.read_log();
    let write_log = near.write_log();
    let oracle = Arc::new(Mutex::new(OracleCounts::default()));
    let (mut stream, handle) = observe(Oracle {
        inner: near,
        counts: Arc::clone(&oracle),
    });
    handle.arm(true);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut buffer = [0_u8; 8];
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(2))
    ));
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(2))
    ));
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(2))
    ));
    assert!(matches!(
        stream.poll_write(&mut cx, b"offered"),
        Poll::Ready(Ok(Written::Accepted(2)))
    ));
    assert!(matches!(
        stream.poll_write(&mut cx, b"blocked"),
        Poll::Ready(Ok(Written::NotNow))
    ));
    fault.inject(Fault::Broken);
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(stream.poll_shutdown(&mut cx), Poll::Ready(Err(_))));

    let snapshot = handle.snapshot();
    assert_eq!(snapshot.lower_read_calls, 4);
    assert_eq!(snapshot.lower_read_bytes, 6);
    assert_eq!(snapshot.lower_write_calls, 2);
    assert_eq!(snapshot.lower_write_bytes, 2);
    assert_eq!(snapshot.lower_write_not_now, 1);
    assert_eq!(snapshot.lower_shutdown_calls, 1);
    assert_eq!(snapshot.lower_failures, 2);
    let oracle = *oracle.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(snapshot.lower_read_calls, oracle.read_calls);
    assert_eq!(snapshot.lower_read_bytes, oracle.read_bytes);
    assert_eq!(snapshot.lower_write_calls, oracle.write_calls);
    assert_eq!(snapshot.lower_write_bytes, oracle.write_bytes);
    assert_eq!(snapshot.lower_write_not_now, oracle.write_not_now);
    assert_eq!(snapshot.lower_shutdown_calls, oracle.shutdown_calls);
    assert_eq!(snapshot.lower_failures, oracle.failures);
    assert_eq!(read_log.reads() as u64, oracle.read_calls);
    assert_eq!(write_log.writes(), 1);
}

#[test]
fn unarmed_wrapper_and_adapter_are_protocol_inert() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (near, _far) = stream_pair();
    near.deliver(b"unchanged");
    let (mut stream, handle) = observe(near);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut buffer = [0_u8; 16];
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(9))
    ));
    assert_eq!(&buffer[..9], b"unchanged");
    assert!(matches!(
        stream.poll_write(&mut cx, b"abc"),
        Poll::Ready(Ok(Written::Accepted(3)))
    ));
    assert_eq!(handle.snapshot(), diagnostics::Snapshot::default());
}

#[test]
fn observed_test_stream_pair_reconciles_lower_oracle_while_driving_the_adapter() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (client_raw, server_raw) = stream_pair();
    client_raw.set_read_cap(Some(17));
    client_raw.set_write_cap(Some(19));
    server_raw.set_read_cap(Some(17));
    server_raw.set_write_cap(Some(19));
    let client_oracle = Arc::new(Mutex::new(OracleCounts::default()));
    let server_oracle = Arc::new(Mutex::new(OracleCounts::default()));
    let (client_io, client_handle) = observe(Oracle {
        inner: client_raw,
        counts: Arc::clone(&client_oracle),
    });
    let (server_io, server_handle) = observe(Oracle {
        inner: server_raw,
        counts: Arc::clone(&server_oracle),
    });
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server");
    let (mut client, mut client_driver) = from_qmux(client_lower, 128);
    let (mut server, mut server_driver) = from_qmux(server_lower, 128);
    let _handles = (client_handle, server_handle);
    diagnostics::arm(true);

    let client_task = async {
        let mut stream =
            std::future::poll_fn(|cx| quic::OpenStreams::poll_open_bidi(&mut client, cx))
                .await
                .expect("open");
        let mut data = Bytes::from_static(b"observed adapter body");
        while data.has_remaining() {
            std::future::poll_fn(|cx| {
                quic::SendStreamUnframed::poll_send(&mut stream, cx, &mut data)
            })
            .await
            .expect("send");
        }
        std::future::poll_fn(|cx| quic::SendStream::poll_finish(&mut stream, cx))
            .await
            .expect("finish");
    };
    let server_task = async {
        let mut stream =
            std::future::poll_fn(|cx| quic::Connection::poll_accept_bidi(&mut server, cx))
                .await
                .expect("accept");
        let mut body = Vec::new();
        while let Some(chunk) =
            std::future::poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
                .await
                .expect("receive")
        {
            body.extend_from_slice(&chunk);
        }
        body
    };
    let (_, body) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(body, b"observed adapter body");

    let client = *client_oracle.lock().unwrap_or_else(PoisonError::into_inner);
    let server = *server_oracle.lock().unwrap_or_else(PoisonError::into_inner);
    let snapshot = diagnostics::snapshot();
    assert_eq!(
        snapshot.lower_read_calls,
        client.read_calls + server.read_calls
    );
    assert_eq!(
        snapshot.lower_read_bytes,
        client.read_bytes + server.read_bytes
    );
    assert_eq!(
        snapshot.lower_write_calls,
        client.write_calls + server.write_calls
    );
    assert_eq!(
        snapshot.lower_write_bytes,
        client.write_bytes + server.write_bytes
    );
    assert_eq!(
        snapshot.lower_write_not_now,
        client.write_not_now + server.write_not_now
    );
    assert_eq!(snapshot.lower_failures, client.failures + server.failures);
    assert!(snapshot.adapter_polls > 0);
    assert!(snapshot.routed_events > 0);
}

#[test]
fn adapter_counters_reconcile_routing_credit_retention_wakes_and_cleanup() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    diagnostics::arm(true);
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let first_waker = Waker::from(Arc::new(TestWake::default()));
    let second_waker = Waker::from(Arc::new(TestWake::default()));
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut first_cx),
        Poll::Pending
    ));
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut second_cx),
        Poll::Pending
    ));
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"diagnostic body")).await;
        common::finish(&mut stream).await;
        drop(stream);
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        assert_eq!(common::receive_all(&mut stream).await, b"diagnostic body");
        drop(stream);
    };
    common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    let snapshot = diagnostics::snapshot();
    assert!(snapshot.adapter_polls > 0);
    assert!(snapshot.driver_polls > 0);
    assert!(snapshot.pump_attempts > 0);
    assert!(snapshot.productive_turns > 0);
    assert_eq!(
        snapshot.pump_attempts,
        snapshot.productive_turns + snapshot.no_progress_polls
    );
    assert!(snapshot.routed_events >= snapshot.stream_events);
    assert!(snapshot.stream_credit_applications > 0);
    assert_eq!(
        snapshot.stream_credit_applications,
        snapshot.connection_credit_applications
    );
    assert!(snapshot.waiter_registrations > 0);
    assert!(snapshot.waiter_replacements > 0);
    assert!(snapshot.wake_deliveries > 0);
    assert_eq!(snapshot.retained_send_bytes, 0);
    assert_eq!(snapshot.retained_receive_bytes, 0);
    assert!(snapshot.cleanups > 0);
}

#[test]
fn lower_failure_counts_once_and_fans_out_one_terminal() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (near, _far) = stream_pair();
    let fault = near.fault_control();
    let (observed, handle) = observe(near);
    let lower = QmuxConnection::client(observed, TestClock::new(), Config::new()).expect("client");
    let (_connection, mut driver) = from_qmux(lower, 128);
    let _handle = handle;
    diagnostics::arm(true);
    fault.inject(Fault::Broken);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        std::future::Future::poll(std::pin::Pin::new(&mut driver), &mut cx),
        Poll::Ready(Err(_))
    ));
    let snapshot = diagnostics::snapshot();
    assert_eq!(snapshot.lower_failures, 1);
    assert_eq!(snapshot.terminal_fanouts, 1);
}

#[test]
fn drain_preserves_live_retained_gauge_and_reseeds_interval_high_water() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    diagnostics::arm(true);
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        quic::SendStream::send_data(&mut stream, Frame::Data(Bytes::from(vec![0x5a; 1024])))
            .expect("retain framed body");
        let drained = diagnostics::drain();
        let reseeded = diagnostics::snapshot();
        std::future::poll_fn(|cx| quic::SendStream::poll_ready(&mut stream, cx))
            .await
            .expect("drain framed body");
        common::finish(&mut stream).await;
        (drained, reseeded)
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        common::receive_all(&mut stream).await
    };
    let ((drained, reseeded), received) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert!(drained.retained_send_bytes >= 1024);
    assert_eq!(
        drained.retained_send_high_water,
        drained.retained_send_bytes
    );
    assert_eq!(reseeded.retained_send_bytes, drained.retained_send_bytes);
    assert_eq!(
        reseeded.retained_send_high_water,
        drained.retained_send_bytes
    );
    assert_eq!(&received[received.len() - 1024..], vec![0x5a; 1024]);
    let final_interval = diagnostics::drain();
    assert_eq!(final_interval.retained_send_bytes, 0);
    assert!(final_interval.retained_send_high_water >= drained.retained_send_bytes);
}

#[test]
fn drain_resets_interval_counters_and_overflow() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    diagnostics::force_overflow_for_test();
    let first = diagnostics::drain();
    assert!(first.overflowed);
    assert_eq!(first.adapter_polls, u64::MAX);
    let second = diagnostics::drain();
    assert!(!second.overflowed);
    assert_eq!(second.adapter_polls, 0);
}
