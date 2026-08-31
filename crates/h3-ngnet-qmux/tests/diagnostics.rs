#![cfg(feature = "diagnostics")]

mod common;

use std::collections::VecDeque;
use std::convert::Infallible;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use h3_ngnet_qmux::diagnostics::{self, observe};
use ngnet_qmux::io::{AsyncByteStream, Config, Written};

struct Scripted {
    reads: VecDeque<Vec<u8>>,
    writes: VecDeque<Written>,
}

impl AsyncByteStream for Scripted {
    type Error = Infallible;

    fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let Some(bytes) = self.reads.pop_front() else {
            return Poll::Pending;
        };
        let count = bytes.len().min(buffer.len());
        buffer[..count].copy_from_slice(&bytes[..count]);
        Poll::Ready(Ok(count))
    }

    fn poll_write(
        &mut self,
        _cx: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        Poll::Ready(Ok(self.writes.pop_front().unwrap_or(Written::NotNow)))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn lower_oracle_reconciles_calls_bytes_partial_not_now_and_shutdown() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (mut stream, handle) = observe(Scripted {
        reads: VecDeque::from([b"abc".to_vec(), b"defgh".to_vec()]),
        writes: VecDeque::from([Written::Accepted(2), Written::NotNow]),
    });
    handle.arm(true);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut buffer = [0_u8; 8];
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(3))
    ));
    assert!(matches!(
        stream.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(5))
    ));
    assert!(matches!(
        stream.poll_write(&mut cx, b"offered"),
        Poll::Ready(Ok(Written::Accepted(2)))
    ));
    assert!(matches!(
        stream.poll_write(&mut cx, b"blocked"),
        Poll::Ready(Ok(Written::NotNow))
    ));
    assert!(matches!(stream.poll_shutdown(&mut cx), Poll::Ready(Ok(()))));

    let snapshot = handle.snapshot();
    assert_eq!(snapshot.lower_read_calls, 2);
    assert_eq!(snapshot.lower_read_bytes, 8);
    assert_eq!(snapshot.lower_write_calls, 2);
    assert_eq!(snapshot.lower_write_bytes, 2);
    assert_eq!(snapshot.lower_write_not_now, 1);
    assert_eq!(snapshot.lower_shutdown_calls, 1);
    assert_eq!(snapshot.lower_failures, 0);
}

#[test]
fn unarmed_wrapper_and_adapter_are_protocol_inert() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    let (mut stream, handle) = observe(Scripted {
        reads: VecDeque::from([b"unchanged".to_vec()]),
        writes: VecDeque::from([Written::Accepted(3)]),
    });
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
fn adapter_counters_reconcile_routing_credit_retention_wakes_and_cleanup() {
    let _serial = diagnostics::lock_for_test();
    diagnostics::reset();
    diagnostics::arm(true);
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
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
    assert!(snapshot.routed_events >= snapshot.stream_events);
    assert!(snapshot.stream_credit_applications > 0);
    assert_eq!(
        snapshot.stream_credit_applications,
        snapshot.connection_credit_applications
    );
    assert!(snapshot.waiter_registrations > 0);
    assert!(snapshot.wake_deliveries > 0);
    assert_eq!(snapshot.retained_send_bytes, 0);
    assert_eq!(snapshot.retained_receive_bytes, 0);
    assert!(snapshot.cleanups > 0);
}

#[test]
fn drain_preserves_live_gauges_and_resets_interval_high_water_and_overflow() {
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
