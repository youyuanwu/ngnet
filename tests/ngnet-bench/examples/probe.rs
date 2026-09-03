//! A single-arm driver for profiling. Not a benchmark: it exists so `perf` and `strace` can be
//! pointed at exactly one fixture, which Criterion's multi-arm process makes impossible.
//!
//! Usage: `probe <arm> <workload> <param> <iters> [timing|reliability|diagnostic]`
//!   arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket |
//!              ngnet-qmux-matched-duplex | ngnet-qmux-matched-socket |
//!              h3-qmux-duplex | h3-qmux-socket |
//!              ngnet-h3-quinn | ngnet-quic-h3 | h3-quinn |
//!              ngnet-quic-h3-matched | h3-ngnet-quic
//!   workload = body | concurrent
//!   param    = body size in bytes, or stream count
//!
//! The QUIC arms support `body` only. Diagnostic mode uses bench-local symmetric counters for
//! the four QMux arms. `ngnet-quic-h3 body` additionally requires `--features diagnostics`;
//! reliability and timing modes are always unarmed.

use std::hint::black_box;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ngnet_bench::{
    CheckedFailure, CheckedFailureKind, CheckedIntegrity, CheckedPhase, CheckedProgress,
    CheckedProgressSnapshot, NgnetH2, NgnetH3Quinn, NgnetNgtcpH3, NgnetNgtcpH3Matched, NgnetQmuxH3,
    NgnetQmuxH3Matched, NgnetQmuxH3MatchedSocket, NgnetQmuxH3Socket, TokioSocket, UpstreamH3Ngtcp,
    UpstreamH3Qmux, UpstreamH3QmuxSocket, UpstreamH3Quinn, body_of, current_thread_runtime,
};

enum Arm {
    H2Duplex(NgnetH2),
    H2Socket(TokioSocket),
    QmuxDuplex(NgnetQmuxH3),
    QmuxSocket(NgnetQmuxH3Socket),
    NgnetQmuxMatchedDuplex(NgnetQmuxH3Matched),
    NgnetQmuxMatchedSocket(NgnetQmuxH3MatchedSocket),
    UpstreamQmuxDuplex(UpstreamH3Qmux),
    UpstreamQmuxSocket(UpstreamH3QmuxSocket),
    NgnetH3Quinn(NgnetH3Quinn),
    NgnetNgtcpH3(NgnetNgtcpH3),
    UpstreamH3Quinn(UpstreamH3Quinn),
    /// The QPACK-matched native arm of the ngtcp2 HTTP/3 comparison.
    NgnetNgtcpH3Matched(NgnetNgtcpH3Matched),
    /// Hyperium H3 over the same ngtcp2 transport, through `h3-ngnet-quic`.
    UpstreamH3Ngtcp(UpstreamH3Ngtcp),
}

impl Arm {
    async fn round_trip(&self, body: bytes::Bytes) -> usize {
        match self {
            Arm::H2Duplex(a) => a.round_trip(body).await,
            Arm::H2Socket(a) => a.round_trip(body).await,
            Arm::QmuxDuplex(a) => a.round_trip(body).await,
            Arm::QmuxSocket(a) => a.round_trip(body).await,
            Arm::NgnetQmuxMatchedDuplex(a) => a.round_trip(body).await,
            Arm::NgnetQmuxMatchedSocket(a) => a.round_trip(body).await,
            Arm::UpstreamQmuxDuplex(a) => a.round_trip(body).await,
            Arm::UpstreamQmuxSocket(a) => a.round_trip(body).await,
            Arm::NgnetH3Quinn(a) => a.round_trip(body).await,
            Arm::NgnetNgtcpH3(a) => a.round_trip(body).await,
            Arm::UpstreamH3Quinn(a) => a.round_trip(body).await,
            Arm::NgnetNgtcpH3Matched(a) => a.round_trip(body).await,
            Arm::UpstreamH3Ngtcp(a) => a.round_trip(body).await,
        }
    }

    async fn round_trip_checked(
        &self,
        body: bytes::Bytes,
        progress: &CheckedProgress,
    ) -> Result<(usize, bool), CheckedFailure> {
        match self {
            Arm::NgnetNgtcpH3(a) => a.try_round_trip_checked_observed(body, progress).await,
            Arm::NgnetNgtcpH3Matched(a) => a.try_round_trip_checked_observed(body, progress).await,
            Arm::UpstreamH3Ngtcp(a) => a
                .try_round_trip_checked_observed(body, progress)
                .await
                .map_err(CheckedFailure::other),
            Arm::NgnetQmuxMatchedDuplex(a) => a
                .try_round_trip_checked(body)
                .await
                .map_err(CheckedFailure::other),
            Arm::NgnetQmuxMatchedSocket(a) => a
                .try_round_trip_checked(body)
                .await
                .map_err(CheckedFailure::other),
            Arm::UpstreamQmuxDuplex(a) => a
                .try_round_trip_checked(body)
                .await
                .map_err(CheckedFailure::other),
            Arm::UpstreamQmuxSocket(a) => a
                .try_round_trip_checked(body)
                .await
                .map_err(CheckedFailure::other),
            _ => unreachable!("request validation restricts diagnostic mode to supported arms"),
        }
    }

    async fn concurrent(&self, n: usize) {
        match self {
            Arm::H2Duplex(a) => a.concurrent(n).await,
            Arm::H2Socket(a) => a.concurrent(n).await,
            Arm::QmuxDuplex(a) => a.concurrent(n).await,
            Arm::QmuxSocket(a) => a.concurrent(n).await,
            Arm::NgnetQmuxMatchedDuplex(_)
            | Arm::NgnetQmuxMatchedSocket(_)
            | Arm::UpstreamQmuxDuplex(_)
            | Arm::UpstreamQmuxSocket(_)
            | Arm::NgnetH3Quinn(_)
            | Arm::NgnetNgtcpH3(_)
            | Arm::UpstreamH3Quinn(_)
            | Arm::NgnetNgtcpH3Matched(_)
            | Arm::UpstreamH3Ngtcp(_) => {
                unreachable!("QUIC arms reject concurrent before setup")
            }
        }
    }

    fn arm_symmetric_counters(&self) {
        match self {
            Arm::NgnetQmuxMatchedDuplex(a) => a.arm_counters(),
            Arm::NgnetQmuxMatchedSocket(a) => a.arm_counters(),
            Arm::UpstreamQmuxDuplex(a) => a.arm_counters(),
            Arm::UpstreamQmuxSocket(a) => a.arm_counters(),
            _ => {}
        }
    }

    fn emit_symmetric_counters(&self, exchange: usize, scope: &str) {
        let snapshot = match self {
            Arm::NgnetQmuxMatchedDuplex(a) => a.counter_snapshot(),
            Arm::NgnetQmuxMatchedSocket(a) => a.counter_snapshot(),
            Arm::UpstreamQmuxDuplex(a) => a.counter_snapshot(),
            Arm::UpstreamQmuxSocket(a) => a.counter_snapshot(),
            _ => return,
        };
        eprintln!(
            "PROBE-SYMMETRIC-QMUX exchange={exchange} scope={scope} \
             lower_read_calls={} lower_read_bytes={} lower_write_calls={} \
             lower_write_bytes={} lower_write_not_now={} endpoint_polls={} overflow={}",
            snapshot.lower_read_calls,
            snapshot.lower_read_bytes,
            snapshot.lower_write_calls,
            snapshot.lower_write_bytes,
            snapshot.lower_write_not_now,
            snapshot.endpoint_polls,
            snapshot.overflowed,
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Timing,
    Reliability,
    Diagnostic,
}

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

fn timeout_classifier(snapshot: CheckedProgressSnapshot) -> &'static str {
    match snapshot.integrity {
        CheckedIntegrity::ContentMismatch => return "wrong-content",
        CheckedIntegrity::LengthMismatch => return "wrong-length",
        CheckedIntegrity::ExactSoFar => {}
    }
    match snapshot.phase {
        CheckedPhase::ResponseHead => "s9-response-head-timeout",
        CheckedPhase::BodyDrain => "s9-body-drain-timeout",
        CheckedPhase::TerminalWait => "unclassified-terminal-wait",
        CheckedPhase::Complete => "unclassified-after-complete",
    }
}

fn error_classifier(
    snapshot: CheckedProgressSnapshot,
    error: &CheckedFailure,
    lost_fin_proof: bool,
) -> &'static str {
    if lost_fin_proof {
        return "lost-fin-signature";
    }
    match snapshot.integrity {
        CheckedIntegrity::ContentMismatch => return "wrong-content",
        CheckedIntegrity::LengthMismatch => return "wrong-length",
        CheckedIntegrity::ExactSoFar => {}
    }
    let closed = error.kind() == CheckedFailureKind::Closed;
    match (snapshot.phase, closed) {
        (CheckedPhase::ResponseHead, true) => "s9-unexpected-close-response-head",
        (CheckedPhase::BodyDrain, true) => "s9-unexpected-close-body-drain",
        (CheckedPhase::TerminalWait, true) => "unclassified-terminal-wait",
        (CheckedPhase::Complete, true) => "unclassified-after-complete",
        (CheckedPhase::ResponseHead, false) => "request-error-response-head",
        (CheckedPhase::BodyDrain, false) => "body-error",
        (CheckedPhase::TerminalWait, false) => "terminal-error",
        (CheckedPhase::Complete, false) => "error-after-complete",
    }
}

fn process_timeout(mode: Mode) -> Option<Duration> {
    match mode {
        Mode::Timing => None,
        Mode::Reliability => Some(Duration::from_secs(135)),
        Mode::Diagnostic => Some(Duration::from_secs(600)),
    }
}

fn flush_stderr() {
    std::io::stderr().flush().expect("flushing probe output");
}

fn exchange_timeout(body_size: usize) -> Duration {
    let mib = body_size.div_ceil(1024 * 1024);
    let (base, per_started_mib) = if cfg!(debug_assertions) {
        (15, 75)
    } else {
        (5, 55)
    };
    Duration::from_secs(base + (mib as u64).saturating_mul(per_started_mib))
}

fn establishment_timeout() -> Duration {
    Duration::from_secs(if cfg!(debug_assertions) { 60 } else { 30 })
}

fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn emit_rss(boundary: &str, exchange: usize, sample: Option<u64>) {
    match sample {
        Some(rss) => {
            eprintln!("PROBE-RSS boundary={boundary} exchange={exchange} rss_kib={rss}")
        }
        None => {
            eprintln!("PROBE-RSS boundary={boundary} exchange={exchange} rss_kib=unavailable")
        }
    }
}

#[cfg(feature = "diagnostics")]
fn emit_diagnostics(
    arm_name: &str,
    exchange: usize,
    scope: &str,
    application_body_bytes: Option<usize>,
) {
    if matches!(
        arm_name,
        "ngnet-qmux-matched-duplex"
            | "ngnet-qmux-matched-socket"
            | "h3-qmux-duplex"
            | "h3-qmux-socket"
    ) {
        return;
    }
    let drained = ngnet_quic::diagnostics::drain();
    let attempts = drained.attempts;
    let mut staged = 0u64;
    let mut accepted = 0u64;
    let mut partial_allowance = 0u64;
    for (attempt_index, attempt) in attempts.iter().enumerate() {
        assert!(
            attempt.accepted_prefix <= attempt.prepared_backing_capacity
                && attempt.prepared_backing_capacity
                    <= attempt.offered_bytes.min(attempt.sampled_payload_limit),
            "diagnostic attempt invariant failed at exchange {exchange}, attempt {attempt_index}"
        );
        staged = staged.saturating_add(attempt.prepared_backing_capacity);
        accepted = accepted.saturating_add(attempt.accepted_prefix);
        if attempt.accepted_prefix < attempt.prepared_backing_capacity
            || (attempt.accepted_prefix == 0 && attempt.offered_bytes > 0)
        {
            partial_allowance = partial_allowance.saturating_add(attempt.sampled_payload_limit);
        }
        if attempt.prepared_backing_capacity < attempt.offered_bytes {
            assert!(
                !attempt.fin_offered,
                "diagnostic attempt {attempt_index} attached FIN before the true final suffix"
            );
        }
        eprintln!(
            "PROBE-DIAGNOSTIC exchange={exchange} attempt={attempt_index} sequence={} \
             connection_id={} role={:?} direction={} stream_id={} stream_offset={} \
             connection_credit_before={} stream_credit_before={} congestion_credit_before={} \
             now={} expiry_before={} expiry_after={} offered={} sampled_payload_limit={} \
             prepared_backing_capacity={} \
             accepted_prefix={} fin={} zero_acceptance={} logical_retained={} \
             retained_backing_capacity={} outcome={:?}",
            attempt.sequence,
            attempt.connection_id,
            attempt.role,
            attempt.direction,
            attempt.stream_id,
            attempt.stream_offset,
            attempt.connection_credit_before,
            attempt.stream_credit_before,
            attempt.congestion_credit_before,
            attempt.now,
            attempt
                .expiry_before
                .map_or_else(|| "none".to_string(), |expiry| expiry.to_string()),
            attempt
                .expiry_after
                .map_or_else(|| "none".to_string(), |expiry| expiry.to_string()),
            attempt.offered_bytes,
            attempt.sampled_payload_limit,
            attempt.prepared_backing_capacity,
            attempt.accepted_prefix,
            attempt.fin_offered,
            attempt.zero_acceptance,
            attempt.logical_retained_bytes,
            attempt.retained_backing_capacity,
            attempt.outcome,
        );
    }
    assert!(
        staged <= accepted.saturating_add(partial_allowance),
        "diagnostic aggregate staging bound failed at exchange {exchange}: staged={staged}, \
         accepted={accepted}, partial_allowance={partial_allowance}"
    );
    for event in &drained.liveness {
        eprintln!(
            "PROBE-LIVENESS exchange={exchange} sequence={} connection_id={} role={:?} \
             kind={:?} reason={} attempt_sequence={} parked_attempt_sequence={} \
             enabling_sequence={}",
            event.sequence,
            event.connection_id,
            event.role,
            event.kind,
            event.reason,
            event
                .attempt_sequence
                .map_or_else(|| "unavailable".to_string(), |value| value.to_string()),
            event
                .parked_attempt_sequence
                .map_or_else(|| "unavailable".to_string(), |value| value.to_string()),
            event
                .enabling_sequence
                .map_or_else(|| "unavailable".to_string(), |value| value.to_string()),
        );
    }
    let snapshot = drained.snapshot;
    for (role, values) in [("client", snapshot.client), ("server", snapshot.server)] {
        assert_eq!(
            values.accepted_bytes, values.release_event_bytes,
            "{role} accepted/release reconciliation failed at exchange {exchange}"
        );
        assert_eq!(
            values.produced_packets,
            values.transport_only_packets + values.stream_carrying_packets,
            "{role} packet reconciliation failed at exchange {exchange}"
        );
        let retries_without_enable = drained
            .liveness
            .iter()
            .filter(|event| {
                format!("{:?}", event.role).eq_ignore_ascii_case(role)
                    && event.kind == ngnet_quic::diagnostics::LivenessKind::Retry
                    && event.reason == "zero-accept"
                    && event.enabling_sequence.is_none()
            })
            .count() as u64;
        if snapshot.dropped_liveness_records == 0 {
            assert_eq!(
                values.zero_accept_retries_without_enable, retries_without_enable,
                "{role} zero-accept retry/event reconciliation failed at exchange {exchange}"
            );
        }
        assert_eq!(
            values.inbound_drops, 0,
            "{role} observed an unexpected inbound drop at exchange {exchange}"
        );
        let (application_body_bytes, framing_overhead_bytes) = application_body_bytes.map_or_else(
            || ("unavailable".to_string(), "unavailable".to_string()),
            |application_body_bytes| {
                let application_body_bytes = application_body_bytes as u64;
                assert!(
                    values.accepted_bytes >= application_body_bytes,
                    "{role} accepted fewer transport stream bytes than the drained application \
                     body at exchange {exchange}: transport_stream_bytes={}, \
                     application_body_bytes={application_body_bytes}",
                    values.accepted_bytes
                );
                (
                    application_body_bytes.to_string(),
                    (values.accepted_bytes - application_body_bytes).to_string(),
                )
            },
        );
        eprintln!(
            "PROBE-SNAPSHOT exchange={exchange} scope={scope} role={role} \
             transport_stream_offered={} prepared_backing_capacity={} \
             transport_stream_accepted={} application_body_bytes={application_body_bytes} \
             framing_overhead_bytes={framing_overhead_bytes} zero_acceptances={} \
             logical_retained={} logical_retained_high_water={} retained_backing_capacity={} \
             retained_backing_high_water={} transport_stream_release_bytes={} \
             acknowledged_bytes={} \
             released_backing_capacity={} produced_packets={} transport_only_packets={} \
             stream_carrying_packets={} timer_rearms={} timer_fires={} timer_kicks={} \
             wake_registrations={} inbound_wakes={} \
             capacity_registrations={} capacity_wakes={} retries={} parks={} \
             zero_accept_retries={} zero_accept_retries_without_enable={} \
             inbound_queue_depth={} inbound_queue_high_water={} inbound_drops={} \
             outbound_queue_depth={} outbound_queue_high_water={} \
             outbound_capacity_transitions={} terminal_discarded_inbound={} \
             terminal_discarded_outbound={} stream_credit_bytes={} \
             connection_credit_bytes={} dropped_attempt_records={} \
             dropped_liveness_records={} retransmissions=unavailable overflow={}",
            values.offered_bytes,
            values.prepared_backing_capacity,
            values.accepted_bytes,
            values.zero_acceptances,
            values.logical_retained_bytes,
            values.logical_retained_high_water,
            values.retained_backing_capacity,
            values.retained_backing_high_water,
            values.release_event_bytes,
            values.acknowledged_bytes,
            values.released_backing_capacity,
            values.produced_packets,
            values.transport_only_packets,
            values.stream_carrying_packets,
            values.timer_rearms,
            values.timer_fires,
            values.timer_kicks,
            values.wake_registrations,
            values.inbound_wakes,
            values.capacity_registrations,
            values.capacity_wakes,
            values.retries,
            values.parks,
            values.zero_accept_retries,
            values.zero_accept_retries_without_enable,
            values.inbound_queue_depth,
            values.inbound_queue_high_water,
            values.inbound_drops,
            values.outbound_queue_depth,
            values.outbound_queue_high_water,
            values.outbound_capacity_transitions,
            values.terminal_discarded_inbound,
            values.terminal_discarded_outbound,
            values.stream_credit_bytes,
            values.connection_credit_bytes,
            snapshot.dropped_attempt_records,
            snapshot.dropped_liveness_records,
            snapshot.overflowed,
        );
    }
}

#[cfg(not(feature = "diagnostics"))]
fn emit_diagnostics(
    arm_name: &str,
    _exchange: usize,
    _scope: &str,
    _application_body_bytes: Option<usize>,
) {
    assert!(
        matches!(
            arm_name,
            "ngnet-qmux-matched-duplex"
                | "ngnet-qmux-matched-socket"
                | "h3-qmux-duplex"
                | "h3-qmux-socket"
        ),
        "ngnet-quic-h3 diagnostic mode requires --features diagnostics"
    );
}

fn validate_request(
    arm_name: &str,
    workload: &str,
    param: usize,
    iters: usize,
    mode: Mode,
) -> Result<(), String> {
    if iters == 0 {
        return Err("iters must be non-zero".to_string());
    }
    if matches!(
        arm_name,
        "ngnet-h3-quinn"
            | "ngnet-quic-h3"
            | "h3-quinn"
            | "ngnet-quic-h3-matched"
            | "h3-ngnet-quic"
            | "ngnet-qmux-matched-duplex"
            | "ngnet-qmux-matched-socket"
            | "h3-qmux-duplex"
            | "h3-qmux-socket"
    ) && workload != "body"
    {
        return Err(format!(
            "the {arm_name} arm supports the body workload only"
        ));
    }
    if arm_name == "ngnet-quic-h3" && !matches!(param, 0 | 1024 | 16384 | 1_048_576) {
        return Err(
            "ngnet-quic-h3 fixed-count probes support 0, 1024, 16384, or 1048576 bytes".to_string(),
        );
    }
    if mode == Mode::Diagnostic
        && (!matches!(
            arm_name,
            "ngnet-quic-h3"
                | "ngnet-quic-h3-matched"
                | "h3-ngnet-quic"
                | "ngnet-qmux-matched-duplex"
                | "ngnet-qmux-matched-socket"
                | "h3-qmux-duplex"
                | "h3-qmux-socket"
        ) || workload != "body")
    {
        return Err(
            "diagnostic mode supports only the ngtcp2 HTTP/3 arms or the four matched QMux \
             body arms"
                .to_string(),
        );
    }
    if mode == Mode::Reliability
        && (!matches!(
            arm_name,
            "ngnet-quic-h3" | "ngnet-quic-h3-matched" | "h3-ngnet-quic"
        ) || workload != "body")
    {
        return Err("reliability mode supports only the three ngtcp2 HTTP/3 body arms".to_string());
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arm_name = args.get(1).expect("arm").clone();
    let workload = args.get(2).expect("workload").clone();
    let param: usize = args.get(3).expect("param").parse().expect("a number");
    let iters: usize = args.get(4).expect("iters").parse().expect("a number");
    let mode = match args.get(5).map(String::as_str).unwrap_or("timing") {
        "timing" => Mode::Timing,
        "reliability" => Mode::Reliability,
        "diagnostic" => Mode::Diagnostic,
        other => panic!("unknown mode {other}; expected timing, reliability, or diagnostic"),
    };
    validate_request(&arm_name, &workload, param, iters, mode)
        .unwrap_or_else(|message| panic!("{message}"));
    #[cfg(not(feature = "diagnostics"))]
    assert!(
        mode != Mode::Diagnostic || arm_name != "ngnet-quic-h3",
        "ngnet-quic-h3 diagnostic mode requires `cargo build -p ngnet-bench --example probe \
         --release --features diagnostics`"
    );

    eprintln!(
        "PROBE-CHECKPOINT exchange=0 phase=setup received_bytes=0 \
         integrity=exact-so-far terminal=false"
    );
    flush_stderr();
    let rt = current_thread_runtime();
    rt.block_on(async move {
        let arm = match arm_name.as_str() {
            "h2-duplex" => Arm::H2Duplex(NgnetH2::establish().await),
            "h2-socket" => Arm::H2Socket(TokioSocket::establish().await),
            "qmux-duplex" => Arm::QmuxDuplex(NgnetQmuxH3::establish().await),
            "qmux-socket" => Arm::QmuxSocket(NgnetQmuxH3Socket::establish().await),
            "ngnet-qmux-matched-duplex" => {
                Arm::NgnetQmuxMatchedDuplex(NgnetQmuxH3Matched::establish().await)
            }
            "ngnet-qmux-matched-socket" => {
                Arm::NgnetQmuxMatchedSocket(NgnetQmuxH3MatchedSocket::establish().await)
            }
            "h3-qmux-duplex" => Arm::UpstreamQmuxDuplex(UpstreamH3Qmux::establish().await),
            "h3-qmux-socket" => Arm::UpstreamQmuxSocket(UpstreamH3QmuxSocket::establish().await),
            "ngnet-h3-quinn" => Arm::NgnetH3Quinn(NgnetH3Quinn::establish().await),
            "ngnet-quic-h3" => Arm::NgnetNgtcpH3(
                tokio::time::timeout(establishment_timeout(), NgnetNgtcpH3::establish())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "ngnet-quic-h3 establishment exceeded {} ms",
                            establishment_timeout().as_millis()
                        )
                    }),
            ),
            "h3-quinn" => Arm::UpstreamH3Quinn(UpstreamH3Quinn::establish().await),
            // The two arms of the ngtcp2 HTTP/3 comparison. Both are bounded the same way as
            // `ngnet-quic-h3` above, because both run the transport with the known large-body
            // liveness defect and an establishment that hangs must be reported, not waited on.
            "ngnet-quic-h3-matched" => Arm::NgnetNgtcpH3Matched(
                tokio::time::timeout(establishment_timeout(), NgnetNgtcpH3Matched::establish())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "ngnet-quic-h3-matched establishment exceeded {} ms",
                            establishment_timeout().as_millis()
                        )
                    }),
            ),
            "h3-ngnet-quic" => Arm::UpstreamH3Ngtcp(
                tokio::time::timeout(establishment_timeout(), UpstreamH3Ngtcp::establish())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "h3-ngnet-quic establishment exceeded {} ms",
                            establishment_timeout().as_millis()
                        )
                    }),
            ),
            other => panic!("unknown arm {other}"),
        };

        // Setup plus an empty persistent exchange stays before readiness and every
        // observation. Keeping the warm-up fixed matters for failure routing: if a large
        // workload fails, it does so after readiness and is classified as workload failure
        // rather than disappearing inside setup.
        eprintln!(
            "PROBE-CHECKPOINT exchange=0 phase=warmup received_bytes=0 \
             integrity=exact-so-far terminal=false"
        );
        flush_stderr();
        let warmup_received = tokio::time::timeout(
            exchange_timeout(0),
            arm.round_trip(body_of(0)),
        )
        .await
        .expect("warm-up exceeded its workload-scaled timeout");
        assert_eq!(warmup_received, 0, "warm-up response was not exact");

        eprintln!(
            "PROBE-METADATA arm={arm_name} workload={workload} param={param} count={iters} \
             warmup=1-explicit mode={} os={} arch={} build={} pid={} host={}",
            match mode {
                Mode::Timing => "timing",
                Mode::Reliability => "reliability",
                Mode::Diagnostic => "diagnostic",
            },
            std::env::consts::OS,
            std::env::consts::ARCH,
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
            std::process::id(),
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unavailable".to_string()),
        );
        eprintln!("PROBE-READY");
        flush_stderr();

        #[cfg(feature = "diagnostics")]
        {
            ngnet_quic::diagnostics::reset();
            ngnet_quic::diagnostics::arm(mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3");
        }

        if mode == Mode::Diagnostic {
            arm.arm_symmetric_counters();
            emit_rss("ready", 0, rss_kib());
            flush_stderr();
        }

        let body_payload = (workload == "body").then(|| body_of(param));
        let started = (mode == Mode::Timing).then(Instant::now);
        match workload.as_str() {
            "body" => {
                let payload = body_payload.expect("body workload has a payload");
                let active_exchange = Arc::new(AtomicUsize::new(1));
                let emitted = Arc::new(Mutex::new((
                    CheckedPhase::Complete,
                    usize::MAX,
                    CheckedIntegrity::ExactSoFar,
                )));
                let progress = CheckedProgress::observed({
                    let active_exchange = Arc::clone(&active_exchange);
                    let emitted = Arc::clone(&emitted);
                    move |snapshot| {
                        let bucket = snapshot.received / (64 * 1024);
                        let mut prior = emitted.lock().expect("probe checkpoint mutex poisoned");
                        if prior.0 == snapshot.phase
                            && prior.1 == bucket
                            && prior.2 == snapshot.integrity
                        {
                            return;
                        }
                        *prior = (snapshot.phase, bucket, snapshot.integrity);
                        eprintln!(
                            "PROBE-CHECKPOINT exchange={} phase={} received_bytes={} \
                             integrity={} terminal={}",
                            active_exchange.load(Ordering::Acquire),
                            phase_name(snapshot.phase),
                            snapshot.received,
                            integrity_name(snapshot.integrity),
                            snapshot.phase == CheckedPhase::Complete,
                        );
                        flush_stderr();
                    }
                });
                let workload = async {
                    for exchange in 1..=iters {
                        active_exchange.store(exchange, Ordering::Release);
                        if mode != Mode::Timing {
                            eprintln!(
                                "PROBE-CHECKPOINT exchange={exchange} last_completed={} \
                                 phase={} received_bytes=0 integrity=exact-so-far terminal=false \
                                 diagnostics_armed={}",
                                exchange - 1,
                                phase_name(CheckedPhase::ResponseHead),
                                mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3"
                            );
                            flush_stderr();
                        }
                        let (received, exact) = if mode == Mode::Timing {
                            (arm.round_trip(payload.clone()).await, true)
                        } else {
                            match tokio::time::timeout(
                                exchange_timeout(param),
                                arm.round_trip_checked(payload.clone(), &progress),
                            )
                            .await
                            {
                                Ok(Ok(received)) => received,
                                Ok(Err(error)) => {
                                    let failure_rss = rss_kib();
                                    let snapshot = progress.snapshot();
                                    eprintln!(
                                        "PROBE-FAIL exchange={exchange} last_completed={} \
                                         phase={} received_bytes={} integrity={} terminal={} classifier={} \
                                         supervisor=inner-error diagnostics_armed={} error={error:?}",
                                        exchange - 1,
                                        phase_name(snapshot.phase),
                                        snapshot.received,
                                        integrity_name(snapshot.integrity),
                                        snapshot.phase == CheckedPhase::Complete,
                                        error_classifier(
                                            snapshot,
                                            &error,
                                            false,
                                        ),
                                        mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3"
                                    );
                                    if mode == Mode::Diagnostic {
                                        emit_rss("failure-request-or-body", exchange, failure_rss);
                                        emit_diagnostics(
                                            &arm_name,
                                            exchange,
                                            "failure-request-or-body",
                                            None,
                                        );
                                        arm.emit_symmetric_counters(
                                            exchange,
                                            "failure-request-or-body",
                                        );
                                    }
                                    flush_stderr();
                                    panic!("exchange {exchange} failed before an exact response");
                                }
                                Err(_) => {
                                    let failure_rss = rss_kib();
                                    let snapshot = progress.snapshot();
                                    eprintln!(
                                        "PROBE-FAIL exchange={exchange} last_completed={} \
                                         phase={} received_bytes={} integrity={} terminal={} classifier={} \
                                         supervisor=inner-timeout diagnostics_armed={} timeout_ms={}",
                                        exchange - 1,
                                        phase_name(snapshot.phase),
                                        snapshot.received,
                                        integrity_name(snapshot.integrity),
                                        snapshot.phase == CheckedPhase::Complete,
                                        timeout_classifier(snapshot),
                                        mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3",
                                        exchange_timeout(param).as_millis()
                                    );
                                    if mode == Mode::Diagnostic {
                                        emit_rss("failure-timeout", exchange, failure_rss);
                                        emit_diagnostics(
                                            &arm_name,
                                            exchange,
                                            "failure-timeout",
                                            None,
                                        );
                                        arm.emit_symmetric_counters(exchange, "failure-timeout");
                                    }
                                    flush_stderr();
                                    panic!(
                                        "exchange {exchange} exceeded its workload-scaled timeout"
                                    );
                                }
                            }
                        };
                        let post_drain_rss = (mode == Mode::Diagnostic).then(rss_kib).flatten();
                        if received != param || !exact {
                            eprintln!(
                                "PROBE-FAIL exchange={exchange} last_completed={} phase=complete \
                                 received_bytes={received} integrity={} terminal=true classifier={} supervisor=inner-error \
                                 diagnostics_armed={} expected_bytes={param}",
                                exchange - 1,
                                if received != param {
                                    "length-mismatch"
                                } else {
                                    "content-mismatch"
                                },
                                if received != param {
                                    "wrong-length"
                                } else {
                                    "wrong-content"
                                },
                                mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3"
                            );
                            if mode == Mode::Diagnostic {
                                emit_rss("failure-response-drained", exchange, post_drain_rss);
                                emit_diagnostics(
                                    &arm_name,
                                    exchange,
                                    "failure-response-drained",
                                    None,
                                );
                                arm.emit_symmetric_counters(
                                    exchange,
                                    "failure-response-drained",
                                );
                            }
                            flush_stderr();
                            panic!("exchange {exchange} response was not exact");
                        }
                        black_box(received);
                        if mode != Mode::Timing {
                            eprintln!(
                                "PROBE-CHECKPOINT exchange={exchange} completed={exchange} \
                                 phase=complete received_bytes={received} integrity=exact-so-far \
                                 terminal=true classifier=completed \
                                 diagnostics_armed={}",
                                mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3"
                            );
                            flush_stderr();
                        }
                        if mode == Mode::Diagnostic {
                            tokio::task::yield_now().await;
                            emit_rss("response-drained", exchange, post_drain_rss);
                            emit_diagnostics(&arm_name, exchange, "both-endpoints", Some(param));
                            arm.emit_symmetric_counters(exchange, "both-endpoints");
                        }
                    }
                };
                if let Some(limit) = process_timeout(mode) {
                    if tokio::time::timeout(limit, workload).await.is_err() {
                        let exchange = active_exchange.load(Ordering::Acquire);
                        let snapshot = progress.snapshot();
                        eprintln!(
                            "PROBE-FAIL exchange={exchange} last_completed={} phase={} \
                             received_bytes={} integrity={} terminal={} classifier={} supervisor=process-timeout \
                             diagnostics_armed={} timeout_ms={}",
                            exchange.saturating_sub(1),
                            phase_name(snapshot.phase),
                            snapshot.received,
                            integrity_name(snapshot.integrity),
                            snapshot.phase == CheckedPhase::Complete,
                            timeout_classifier(snapshot),
                            mode == Mode::Diagnostic && arm_name == "ngnet-quic-h3",
                            limit.as_millis()
                        );
                        if mode == Mode::Diagnostic {
                            emit_diagnostics(
                                &arm_name,
                                exchange,
                                "failure-process-timeout",
                                None,
                            );
                        }
                        flush_stderr();
                        panic!("probe body workload exceeded its process deadline");
                    }
                } else {
                    workload.await;
                }
            }
            "concurrent" => {
                for exchange in 1..=iters {
                    arm.concurrent(param).await;
                    if mode == Mode::Diagnostic {
                        eprintln!("PROBE-PROGRESS exchange={exchange} completed={exchange}");
                        flush_stderr();
                    }
                }
            }
            other => panic!("unknown workload {other}"),
        }

        if let Some(started) = started {
            let elapsed = started.elapsed();
            if workload == "body" {
                let application_bytes = param.saturating_mul(iters).saturating_mul(2);
                eprintln!(
                    "PROBE-TIMING elapsed_ns={} application_bytes={application_bytes}",
                    elapsed.as_nanos()
                );
            } else {
                eprintln!(
                    "PROBE-TIMING elapsed_ns={} concurrent_rounds={iters} \
                     streams_per_round={param}",
                    elapsed.as_nanos()
                );
            }
        } else if mode == Mode::Diagnostic {
            emit_diagnostics(&arm_name, iters, "final", None);
            arm.emit_symmetric_counters(iters, "final");
            emit_rss("final", iters, rss_kib());
        }
        eprintln!("PROBE-DONE completed={iters}");
        flush_stderr();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_mode_is_restricted_to_the_supported_target_body_arm() {
        assert!(validate_request("ngnet-quic-h3", "body", 1024, 1, Mode::Diagnostic).is_ok());
        assert!(validate_request("h3-qmux-duplex", "body", 1024, 1, Mode::Diagnostic).is_ok());
        assert!(validate_request("h3-qmux-socket", "body", 1024, 1, Mode::Diagnostic).is_ok());
        assert!(
            validate_request(
                "ngnet-qmux-matched-duplex",
                "body",
                1024,
                1,
                Mode::Diagnostic
            )
            .is_ok()
        );
        assert!(
            validate_request(
                "ngnet-qmux-matched-socket",
                "body",
                1024,
                1,
                Mode::Diagnostic
            )
            .is_ok()
        );
        assert!(validate_request("ngnet-h3-quinn", "body", 1024, 1, Mode::Diagnostic).is_err());
        assert!(validate_request("ngnet-quic-h3", "concurrent", 1, 1, Mode::Diagnostic).is_err());
    }

    #[test]
    fn timeouts_scale_with_body_size_and_build_profile() {
        assert!(exchange_timeout(1024 * 1024) > exchange_timeout(0));
        if cfg!(debug_assertions) {
            assert!(exchange_timeout(1024 * 1024) >= Duration::from_secs(90));
        } else {
            assert!(exchange_timeout(1024 * 1024) >= Duration::from_secs(60));
        }
    }

    #[test]
    fn reliability_mode_and_process_deadlines_are_explicit() {
        assert!(
            validate_request("ngnet-quic-h3", "body", 1_048_576, 125, Mode::Reliability).is_ok()
        );
        assert!(
            validate_request(
                "ngnet-quic-h3-matched",
                "body",
                1_048_576,
                30,
                Mode::Reliability
            )
            .is_ok()
        );
        assert!(validate_request("ngnet-h3-quinn", "body", 1024, 1, Mode::Reliability).is_err());
        assert_eq!(process_timeout(Mode::Timing), None);
        assert_eq!(
            process_timeout(Mode::Reliability),
            Some(Duration::from_secs(135))
        );
        assert_eq!(
            process_timeout(Mode::Diagnostic),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn terminal_wait_is_not_assumed_to_be_the_lost_fin_defect() {
        let snapshot = |phase, integrity| CheckedProgressSnapshot {
            phase,
            received: 0,
            integrity,
        };
        let terminal = snapshot(CheckedPhase::TerminalWait, CheckedIntegrity::ExactSoFar);
        assert_eq!(phase_name(CheckedPhase::TerminalWait), "terminal-wait");
        assert_eq!(timeout_classifier(terminal), "unclassified-terminal-wait");
        assert_eq!(
            error_classifier(terminal, &CheckedFailure::closed("connection ended"), false,),
            "unclassified-terminal-wait"
        );
        assert_eq!(
            error_classifier(terminal, &CheckedFailure::other("not a close"), true),
            "lost-fin-signature"
        );
        assert_eq!(
            error_classifier(
                snapshot(CheckedPhase::BodyDrain, CheckedIntegrity::ExactSoFar),
                &CheckedFailure::other("frame decode failed"),
                false,
            ),
            "body-error"
        );
        assert_eq!(
            timeout_classifier(snapshot(
                CheckedPhase::BodyDrain,
                CheckedIntegrity::ContentMismatch,
            )),
            "wrong-content"
        );
        assert_eq!(
            error_classifier(
                snapshot(CheckedPhase::BodyDrain, CheckedIntegrity::LengthMismatch,),
                &CheckedFailure::closed("connection ended"),
                false,
            ),
            "wrong-length"
        );
    }
}
