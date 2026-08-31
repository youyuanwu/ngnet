#![cfg(feature = "diagnostics")]

use std::sync::{Arc, Barrier, Mutex};

use ngnet_quic::Role;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn reset_excludes_recorders_from_the_previous_armed_generation() {
    let _guard = TEST_LOCK.lock().unwrap();
    ngnet_quic::diagnostics::reset();
    ngnet_quic::diagnostics::arm(true);
    let started = Arc::new(Barrier::new(2));
    let recorder_started = Arc::clone(&started);
    let recorder = std::thread::spawn(move || {
        recorder_started.wait();
        for _ in 0..10_000 {
            ngnet_quic::diagnostics::record_packet(12, Role::Client, true);
        }
    });
    started.wait();
    ngnet_quic::diagnostics::reset();
    recorder.join().expect("diagnostic recorder thread");
    assert_eq!(
        ngnet_quic::diagnostics::snapshot(),
        ngnet_quic::diagnostics::Snapshot::default()
    );
    assert!(ngnet_quic::diagnostics::take_attempts().is_empty());
    assert!(ngnet_quic::diagnostics::take_liveness_events().is_empty());
}

#[test]
fn exclusive_drain_partitions_concurrent_records_without_loss() {
    let _guard = TEST_LOCK.lock().unwrap();
    ngnet_quic::diagnostics::reset();
    ngnet_quic::diagnostics::arm(true);
    let started = Arc::new(Barrier::new(2));
    let recorder_started = Arc::clone(&started);
    let recorder = std::thread::spawn(move || {
        recorder_started.wait();
        for _ in 0..10_000 {
            ngnet_quic::diagnostics::record_release(12, Role::Client, 1);
        }
    });

    started.wait();
    let first = ngnet_quic::diagnostics::drain();
    recorder.join().expect("diagnostic recorder thread");
    let second = ngnet_quic::diagnostics::drain();
    assert_eq!(
        first.snapshot.client.release_event_bytes + second.snapshot.client.release_event_bytes,
        10_000,
        "a recorder interleaving with exclusive drain was lost or counted twice"
    );
    ngnet_quic::diagnostics::reset();
}
