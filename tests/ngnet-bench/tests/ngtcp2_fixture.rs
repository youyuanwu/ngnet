use bytes::Bytes;
use ngnet_bench::{CheckedIntegrity, CheckedPhase, CheckedProgress, NgnetNgtcpH3};
use std::sync::{Arc, Mutex};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn phase_name(phase: CheckedPhase) -> &'static str {
    match phase {
        CheckedPhase::ResponseHead => "response-head",
        CheckedPhase::BodyDrain => "body-drain",
        CheckedPhase::TerminalWait => "terminal-wait",
        CheckedPhase::Complete => "complete",
    }
}

fn integrity_name(integrity: CheckedIntegrity) -> &'static str {
    match integrity {
        CheckedIntegrity::ExactSoFar => "exact-so-far",
        CheckedIntegrity::ContentMismatch => "content-mismatch",
        CheckedIntegrity::LengthMismatch => "length-mismatch",
    }
}

fn establishment_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(if cfg!(debug_assertions) { 60 } else { 30 })
}

fn exchange_timeout(size: usize) -> std::time::Duration {
    let mib = size.div_ceil(1024 * 1024);
    let (base, per_started_mib) = if cfg!(debug_assertions) {
        (15, 75)
    } else {
        (5, 55)
    };
    std::time::Duration::from_secs(base + (mib as u64).saturating_mul(per_started_mib))
}

async fn establish_fixture() -> NgnetNgtcpH3 {
    tokio::time::timeout(establishment_timeout(), NgnetNgtcpH3::establish())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "ngtcp2 fixture establishment exceeded {} ms",
                establishment_timeout().as_millis()
            )
        })
}

#[cfg(feature = "diagnostics")]
fn assert_bounded_attempts(
    attempts: &[ngnet_quic::diagnostics::Attempt],
    stricter_limit: Option<u64>,
) -> u64 {
    use std::collections::BTreeMap;

    let mut staged = 0u64;
    let mut accepted = 0u64;
    let mut partial_allowance = 0u64;
    let mut next_offsets: BTreeMap<(u64, i64), u64> = BTreeMap::new();
    let mut saw_truncated = false;
    let mut saw_fin = false;

    for attempt in attempts {
        let limit = stricter_limit.map_or(attempt.sampled_payload_limit, |limit| {
            attempt.sampled_payload_limit.min(limit)
        });
        assert!(attempt.accepted_prefix <= attempt.prepared_backing_capacity);
        assert!(attempt.prepared_backing_capacity <= attempt.offered_bytes.min(limit));
        if let Some(expected) = next_offsets.insert(
            (attempt.connection_id, attempt.stream_id),
            attempt.stream_offset + attempt.accepted_prefix,
        ) {
            assert_eq!(
                attempt.stream_offset, expected,
                "an unaccepted suffix was not reoffered from the exact prior offset"
            );
        }
        if attempt.prepared_backing_capacity < attempt.offered_bytes {
            saw_truncated = true;
            assert!(
                !attempt.fin_offered,
                "a bounded prefix omitting a caller suffix must suppress FIN"
            );
        }
        saw_fin |= attempt.fin_offered;
        staged = staged.saturating_add(attempt.prepared_backing_capacity);
        accepted = accepted.saturating_add(attempt.accepted_prefix);
        if attempt.accepted_prefix < attempt.prepared_backing_capacity
            || (attempt.accepted_prefix == 0 && attempt.offered_bytes > 0)
        {
            partial_allowance = partial_allowance.saturating_add(limit);
        }
        assert_eq!(attempt.direction, "outbound");
    }

    assert!(saw_truncated, "the body must exercise bounded staging");
    assert!(saw_fin, "the exact exchange must exercise a final suffix");
    assert!(
        staged <= accepted.saturating_add(partial_allowance),
        "staged={staged}, accepted={accepted}, partial_allowance={partial_allowance}"
    );
    staged
}

#[cfg(feature = "diagnostics")]
fn assert_zero_accept_retry_reconciliation(drained: &ngnet_quic::diagnostics::DrainedDiagnostics) {
    if drained.snapshot.dropped_liveness_records != 0 {
        return;
    }
    for (role, snapshot) in [
        (ngnet_quic::Role::Client, drained.snapshot.client),
        (ngnet_quic::Role::Server, drained.snapshot.server),
    ] {
        let retries_without_enable = drained
            .liveness
            .iter()
            .filter(|event| {
                event.role == role
                    && event.kind == ngnet_quic::diagnostics::LivenessKind::Retry
                    && event.reason == "zero-accept"
                    && event.enabling_sequence.is_none()
            })
            .count() as u64;
        assert_eq!(
            snapshot.zero_accept_retries_without_enable, retries_without_enable,
            "zero-accept retries must report real external/sendability evidence"
        );
    }
}

async fn repeated_exact_echo(size: usize, exchanges: usize) {
    let fixture = establish_fixture().await;
    let body = Bytes::from(
        (0..size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );

    for exchange in 1..=exchanges {
        let emitted = Arc::new(Mutex::new((CheckedPhase::Complete, usize::MAX)));
        let progress = CheckedProgress::observed({
            let emitted = Arc::clone(&emitted);
            move |snapshot| {
                let bucket = snapshot.received / (64 * 1024);
                let mut prior = emitted.lock().expect("fixture checkpoint mutex poisoned");
                if prior.0 == snapshot.phase && prior.1 == bucket {
                    return;
                }
                *prior = (snapshot.phase, bucket);
                eprintln!(
                    "S9-FIXTURE-CHECKPOINT size={size} exchange={exchange} phase={} \
                     received_bytes={} integrity={} terminal={}",
                    phase_name(snapshot.phase),
                    snapshot.received,
                    integrity_name(snapshot.integrity),
                    snapshot.phase == CheckedPhase::Complete,
                );
            }
        });
        eprintln!(
            "S9-FIXTURE-CHECKPOINT size={size} exchange={exchange} last_completed={} \
             phase=response-head received_bytes=0",
            exchange - 1
        );
        let (received, exact) = tokio::time::timeout(
            exchange_timeout(size),
            fixture.try_round_trip_checked_observed(body.clone(), &progress),
        )
        .await
        .unwrap_or_else(|_| {
            let snapshot = progress.snapshot();
            panic!(
                "{size}-byte exchange {exchange} stalled; last completed exchange was {}; \
                 phase={} received_bytes={} integrity={} terminal={}",
                exchange - 1,
                phase_name(snapshot.phase),
                snapshot.received,
                integrity_name(snapshot.integrity),
                snapshot.phase == CheckedPhase::Complete,
            )
        })
        .unwrap_or_else(|error| {
            let snapshot = progress.snapshot();
            panic!(
                "{size}-byte exchange {exchange} failed; last completed exchange was {}; \
                 phase={} received_bytes={} integrity={} terminal={} error={error}",
                exchange - 1,
                phase_name(snapshot.phase),
                snapshot.received,
                integrity_name(snapshot.integrity),
                snapshot.phase == CheckedPhase::Complete,
            )
        });
        if received != size || !exact {
            eprintln!(
                "S9-FIXTURE-FAIL size={size} exchange={exchange} last_completed={} \
                 phase=complete received_bytes={received} integrity={} terminal=true \
                 classifier={}",
                exchange - 1,
                if received != size {
                    "length-mismatch"
                } else {
                    "content-mismatch"
                },
                if received != size {
                    "wrong-length"
                } else {
                    "wrong-content"
                }
            );
        }
        assert_eq!(
            received, size,
            "{size}-byte exchange {exchange} did not echo exactly"
        );
        assert!(
            exact,
            "{size}-byte exchange {exchange} returned corrupted content"
        );
        eprintln!(
            "S9-FIXTURE-CHECKPOINT size={size} exchange={exchange} completed={exchange} \
             phase=complete received_bytes={received}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_echoes_the_complete_body() {
    let _guard = TEST_LOCK.lock().await;
    let body = Bytes::from(vec![0x5a; 64 * 1024]);
    let fixture = establish_fixture().await;

    let (received, exact) = tokio::time::timeout(
        exchange_timeout(body.len()),
        fixture.try_round_trip_checked(body.clone()),
    )
    .await
    .expect("the complete-body exchange exceeded its body/build-scaled timeout")
    .expect("the complete-body exchange failed before an exact response");
    assert_eq!(received, body.len());
    assert!(exact);
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_reuses_more_than_the_initial_stream_limit() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = establish_fixture().await;

    for exchange in 0..125 {
        tokio::time::timeout(exchange_timeout(0), fixture.round_trip(Bytes::new()))
            .await
            .unwrap_or_else(|_| panic!("exchange {exchange} stalled after stream credit ran out"));
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "long-running S9 stress; run through the documented process-group supervisor"]
async fn ngtcp2_fixture_repeats_16_kib_exactly() {
    let _guard = TEST_LOCK.lock().await;
    repeated_exact_echo(16 * 1024, 125).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "long-running S9 stress; run through the documented process-group supervisor"]
async fn ngtcp2_fixture_repeats_1_mib_exactly() {
    let _guard = TEST_LOCK.lock().await;
    repeated_exact_echo(1024 * 1024, 125).await;
}

#[cfg(feature = "diagnostics")]
#[tokio::test(flavor = "current_thread")]
async fn unarmed_and_armed_diagnostics_preserve_and_reconcile_echoes() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = establish_fixture().await;
    ngnet_quic::diagnostics::reset();

    let unarmed_body = Bytes::from(vec![0x7a; 1024]);
    let (unarmed_received, unarmed_exact) = fixture.round_trip_checked(unarmed_body.clone()).await;
    assert_eq!(unarmed_received, unarmed_body.len());
    assert!(unarmed_exact);
    assert!(!ngnet_quic::diagnostics::is_armed());
    assert_eq!(
        ngnet_quic::diagnostics::snapshot(),
        ngnet_quic::diagnostics::Snapshot::default()
    );
    assert!(ngnet_quic::diagnostics::take_attempts().is_empty());

    ngnet_quic::diagnostics::set_test_staging_limit(Some(1024));
    ngnet_quic::diagnostics::arm(true);

    let body = Bytes::from(vec![0x6d; 16 * 1024]);
    for exchange in 1..=3 {
        let (received, exact) = tokio::time::timeout(
            exchange_timeout(body.len()),
            fixture.round_trip_checked(body.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("diagnostic exchange {exchange} stalled"));
        assert_eq!(received, body.len());
        assert!(exact, "diagnostic exchange {exchange} content differed");
    }

    ngnet_quic::diagnostics::arm(false);
    let drained = ngnet_quic::diagnostics::drain();
    let attempts = &drained.attempts;
    assert!(
        !attempts.is_empty(),
        "the armed fixture recorded no attempts"
    );
    assert_bounded_attempts(attempts, Some(1024));

    assert_zero_accept_retry_reconciliation(&drained);
    let snapshot = drained.snapshot;
    for role in [snapshot.client, snapshot.server] {
        assert!(role.offered_bytes > 0);
        assert_eq!(role.accepted_bytes, role.release_event_bytes);
        assert!(
            role.accepted_bytes >= (body.len() * 3) as u64,
            "transport stream bytes must include every application body byte"
        );
        assert_eq!(
            role.produced_packets,
            role.transport_only_packets + role.stream_carrying_packets
        );
        assert_eq!(role.inbound_drops, 0);
    }
    assert!(!snapshot.overflowed);
    assert!(!snapshot.retransmissions_available);
    ngnet_quic::diagnostics::reset();
}
