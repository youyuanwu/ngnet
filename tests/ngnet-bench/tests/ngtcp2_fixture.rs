use bytes::Bytes;
use ngnet_bench::NgnetNgtcpH3;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
#[ignore = "Phase 1 reproduces native corruption/SIGSEGV; Phase 2 owns the staging repair"]
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
    assert!(
        attempts.iter().any(|attempt| attempt.accepted_prefix > 0
            && attempt.accepted_prefix < attempt.offered_bytes),
        "a 16 KiB body must exercise native partial acceptance"
    );
    assert!(
        attempts.iter().any(|attempt| attempt.fin_offered),
        "the exact echo must exercise a final stream offer"
    );
    for attempt in attempts {
        assert!(attempt.accepted_prefix <= attempt.prepared_backing_capacity);
        assert!(attempt.prepared_backing_capacity <= attempt.offered_bytes);
        assert_eq!(attempt.direction, "outbound");
    }

    let snapshot = ngnet_quic::diagnostics::snapshot();
    for role in [snapshot.client, snapshot.server] {
        assert!(role.offered_bytes > 0);
        assert_eq!(role.accepted_bytes, role.release_event_bytes);
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
