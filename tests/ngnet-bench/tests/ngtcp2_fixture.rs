use bytes::Bytes;
use ngnet_bench::NgnetNgtcpH3;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
async fn diagnostic_staged_total(size: usize, stricter_limit: Option<usize>) -> u64 {
    let fixture = NgnetNgtcpH3::establish().await;
    ngnet_quic::diagnostics::reset();
    ngnet_quic::diagnostics::set_test_staging_limit(stricter_limit);
    ngnet_quic::diagnostics::arm(true);

    let body = Bytes::from(vec![0x4d; size]);
    let (received, exact) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fixture.round_trip_checked(body),
    )
    .await
    .expect("diagnostic exact exchange stalled");
    assert_eq!(received, size);
    assert!(exact);

    ngnet_quic::diagnostics::arm(false);
    let attempts = ngnet_quic::diagnostics::take_attempts();
    let staged = assert_bounded_attempts(
        &attempts,
        stricter_limit.and_then(|limit| u64::try_from(limit).ok()),
    );
    let snapshot = ngnet_quic::diagnostics::snapshot();
    for role in [snapshot.client, snapshot.server] {
        assert_eq!(role.accepted_bytes, role.release_event_bytes);
        assert_eq!(
            role.produced_packets,
            role.transport_only_packets + role.stream_carrying_packets
        );
        assert_eq!(role.zero_accept_retries_without_enable, 0);
        assert_eq!(role.inbound_drops, 0);
    }
    assert!(!snapshot.overflowed);
    ngnet_quic::diagnostics::reset();
    staged
}

async fn repeated_exact_echo(size: usize, exchanges: usize) {
    let fixture = NgnetNgtcpH3::establish().await;
    let body = Bytes::from(
        (0..size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );

    for exchange in 1..=exchanges {
        let (received, exact) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.round_trip_checked(body.clone()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{size}-byte exchange {exchange} stalled; last completed exchange was {}",
                exchange - 1
            )
        });
        assert_eq!(
            received, size,
            "{size}-byte exchange {exchange} did not echo exactly"
        );
        assert!(
            exact,
            "{size}-byte exchange {exchange} returned corrupted content"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_echoes_the_complete_body() {
    let _guard = TEST_LOCK.lock().await;
    let body = Bytes::from(vec![0x5a; 64 * 1024]);
    let fixture = NgnetNgtcpH3::establish().await;

    let (received, exact) = fixture.round_trip_checked(body.clone()).await;
    assert_eq!(received, body.len());
    assert!(exact);
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_reuses_more_than_the_initial_stream_limit() {
    let _guard = TEST_LOCK.lock().await;
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

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_repeats_16_kib_exactly() {
    let _guard = TEST_LOCK.lock().await;
    repeated_exact_echo(16 * 1024, 125).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ngtcp2_fixture_repeats_1_mib_exactly() {
    let _guard = TEST_LOCK.lock().await;
    repeated_exact_echo(1024 * 1024, 125).await;
}

#[cfg(feature = "diagnostics")]
#[tokio::test(flavor = "current_thread")]
async fn unarmed_and_armed_diagnostics_preserve_and_reconcile_echoes() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = NgnetNgtcpH3::establish().await;
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
            std::time::Duration::from_secs(5),
            fixture.round_trip_checked(body.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("diagnostic exchange {exchange} stalled"));
        assert_eq!(received, body.len());
        assert!(exact, "diagnostic exchange {exchange} content differed");
    }

    ngnet_quic::diagnostics::arm(false);
    let attempts = ngnet_quic::diagnostics::take_attempts();
    assert!(
        !attempts.is_empty(),
        "the armed fixture recorded no attempts"
    );
    assert_bounded_attempts(&attempts, Some(1024));

    let liveness = ngnet_quic::diagnostics::take_liveness_events();
    let snapshot = ngnet_quic::diagnostics::snapshot();
    for role in [snapshot.client, snapshot.server] {
        assert!(role.offered_bytes > 0);
        assert_eq!(role.accepted_bytes, role.release_event_bytes);
        assert_eq!(
            role.produced_packets,
            role.transport_only_packets + role.stream_carrying_packets
        );
        assert_eq!(
            role.zero_accept_retries_without_enable, 0,
            "a blocked stream must wait for an enabling event before retrying: {liveness:#?}"
        );
        assert_eq!(role.inbound_drops, 0);
    }
    assert!(!snapshot.overflowed);
    assert!(!snapshot.retransmissions_available);
    ngnet_quic::diagnostics::reset();
}

#[cfg(feature = "diagnostics")]
#[tokio::test(flavor = "current_thread")]
async fn production_staging_is_payload_bounded_and_scales_linearly() {
    let _guard = TEST_LOCK.lock().await;

    let production = diagnostic_staged_total(64 * 1024, None).await;
    assert!(production > 64 * 1024);

    let one = diagnostic_staged_total(64 * 1024, Some(1024)).await;
    let two = diagnostic_staged_total(128 * 1024, Some(1024)).await;
    eprintln!(
        "staged totals: production_64k={production}, fixed_1024_64k={one}, \
         fixed_1024_128k={two}"
    );
    assert!(
        two.saturating_mul(10) <= one.saturating_mul(21),
        "doubling the body staged more than 2.1x: one={one}, two={two}"
    );
}
