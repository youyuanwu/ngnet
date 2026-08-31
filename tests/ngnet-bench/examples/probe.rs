//! A single-arm driver for profiling. Not a benchmark: it exists so `perf` and `strace` can be
//! pointed at exactly one fixture, which Criterion's multi-arm process makes impossible.
//!
//! Usage: `probe <arm> <workload> <param> <iters> [timing|diagnostic]`
//!   arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket |
//!              ngnet-h3-quinn | ngnet-quic-h3 | h3-quinn
//!   workload = body | concurrent
//!   param    = body size in bytes, or stream count
//!
//! The QUIC arms support `body` only. Diagnostic mode is supported only by
//! `ngnet-quic-h3 body` and additionally requires `--features diagnostics`; timing mode is
//! always unarmed.

use std::hint::black_box;
use std::io::Write;
use std::time::{Duration, Instant};

use ngnet_bench::{
    NgnetH2, NgnetH3Quinn, NgnetNgtcpH3, NgnetQmuxH3, NgnetQmuxH3Socket, TokioSocket,
    UpstreamH3Quinn, body_of, current_thread_runtime,
};

enum Arm {
    H2Duplex(NgnetH2),
    H2Socket(TokioSocket),
    QmuxDuplex(NgnetQmuxH3),
    QmuxSocket(NgnetQmuxH3Socket),
    NgnetH3Quinn(NgnetH3Quinn),
    NgnetNgtcpH3(NgnetNgtcpH3),
    UpstreamH3Quinn(UpstreamH3Quinn),
}

impl Arm {
    async fn round_trip(&self, body: bytes::Bytes) -> usize {
        match self {
            Arm::H2Duplex(a) => a.round_trip(body).await,
            Arm::H2Socket(a) => a.round_trip(body).await,
            Arm::QmuxDuplex(a) => a.round_trip(body).await,
            Arm::QmuxSocket(a) => a.round_trip(body).await,
            Arm::NgnetH3Quinn(a) => a.round_trip(body).await,
            Arm::NgnetNgtcpH3(a) => a.round_trip(body).await,
            Arm::UpstreamH3Quinn(a) => a.round_trip(body).await,
        }
    }

    async fn round_trip_checked(&self, body: bytes::Bytes) -> (usize, bool) {
        match self {
            Arm::NgnetNgtcpH3(a) => a.round_trip_checked(body).await,
            _ => unreachable!("request validation restricts diagnostic mode to ngnet-quic-h3"),
        }
    }

    async fn concurrent(&self, n: usize) {
        match self {
            Arm::H2Duplex(a) => a.concurrent(n).await,
            Arm::H2Socket(a) => a.concurrent(n).await,
            Arm::QmuxDuplex(a) => a.concurrent(n).await,
            Arm::QmuxSocket(a) => a.concurrent(n).await,
            Arm::NgnetH3Quinn(_) | Arm::NgnetNgtcpH3(_) | Arm::UpstreamH3Quinn(_) => {
                unreachable!("QUIC arms reject concurrent before setup")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Timing,
    Diagnostic,
}

fn flush_stderr() {
    std::io::stderr().flush().expect("flushing probe output");
}

fn exchange_timeout(body_size: usize) -> Duration {
    let mib = body_size.div_ceil(1024 * 1024);
    let (base, per_started_mib) = if cfg!(debug_assertions) {
        (15, 30)
    } else {
        (5, 10)
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
fn emit_diagnostics(exchange: usize, scope: &str, application_body_bytes: Option<usize>) {
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
             offered={} sampled_payload_limit={} \
             prepared_backing_capacity={} \
             accepted_prefix={} fin={} zero_acceptance={} logical_retained={} \
             retained_backing_capacity={} outcome={:?}",
            attempt.sequence,
            attempt.connection_id,
            attempt.role,
            attempt.direction,
            attempt.stream_id,
            attempt.stream_offset,
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
        assert_eq!(
            values.zero_accept_retries_without_enable, retries_without_enable,
            "{role} zero-accept retry/event reconciliation failed at exchange {exchange}"
        );
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
             stream_carrying_packets={} timer_rearms={} timer_fires={} wake_registrations={} inbound_wakes={} \
             capacity_registrations={} capacity_wakes={} retries={} parks={} \
             zero_accept_retries={} zero_accept_retries_without_enable={} \
             inbound_queue_depth={} inbound_queue_high_water={} inbound_drops={} \
             outbound_queue_depth={} outbound_queue_high_water={} \
             outbound_capacity_transitions={} terminal_discarded_inbound={} \
             terminal_discarded_outbound={} retransmissions=unavailable overflow={}",
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
            snapshot.overflowed,
        );
    }
}

#[cfg(not(feature = "diagnostics"))]
fn emit_diagnostics(_exchange: usize, _scope: &str, _application_body_bytes: Option<usize>) {
    unreachable!("diagnostic mode is rejected before setup")
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
    if matches!(arm_name, "ngnet-h3-quinn" | "ngnet-quic-h3" | "h3-quinn") && workload != "body" {
        return Err(format!(
            "the {arm_name} arm supports the body workload only"
        ));
    }
    if arm_name == "ngnet-quic-h3" && !matches!(param, 0 | 1024 | 16384 | 1_048_576) {
        return Err(
            "ngnet-quic-h3 fixed-count probes support 0, 1024, 16384, or 1048576 bytes".to_string(),
        );
    }
    if mode == Mode::Diagnostic && (arm_name != "ngnet-quic-h3" || workload != "body") {
        return Err("diagnostic mode supports only `ngnet-quic-h3 body`".to_string());
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
        "diagnostic" => Mode::Diagnostic,
        other => panic!("unknown mode {other}; expected timing or diagnostic"),
    };
    validate_request(&arm_name, &workload, param, iters, mode)
        .unwrap_or_else(|message| panic!("{message}"));
    #[cfg(not(feature = "diagnostics"))]
    assert!(
        mode == Mode::Timing,
        "diagnostic mode requires `cargo build -p ngnet-bench --example probe --release \
         --features diagnostics`"
    );

    let rt = current_thread_runtime();
    rt.block_on(async move {
        let arm = match arm_name.as_str() {
            "h2-duplex" => Arm::H2Duplex(NgnetH2::establish().await),
            "h2-socket" => Arm::H2Socket(TokioSocket::establish().await),
            "qmux-duplex" => Arm::QmuxDuplex(NgnetQmuxH3::establish().await),
            "qmux-socket" => Arm::QmuxSocket(NgnetQmuxH3Socket::establish().await),
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
            other => panic!("unknown arm {other}"),
        };

        // Setup plus an empty persistent exchange stays before readiness and every
        // observation. Keeping the warm-up fixed matters for failure routing: if a large
        // workload fails, it does so after readiness and is classified as workload failure
        // rather than disappearing inside setup.
        let warmup_received = arm.round_trip(body_of(0)).await;
        assert_eq!(warmup_received, 0, "warm-up response was not exact");

        eprintln!(
            "PROBE-METADATA arm={arm_name} workload={workload} param={param} count={iters} \
             warmup=1-explicit mode={} os={} arch={} build={} pid={} host={}",
            if mode == Mode::Timing {
                "timing"
            } else {
                "diagnostic"
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
            ngnet_quic::diagnostics::arm(mode == Mode::Diagnostic);
        }

        if mode == Mode::Diagnostic {
            emit_rss("ready", 0, rss_kib());
            flush_stderr();
        }

        let body_payload = (workload == "body").then(|| body_of(param));
        let started = (mode == Mode::Timing).then(Instant::now);
        match workload.as_str() {
            "body" => {
                let payload = body_payload.expect("body workload has a payload");
                for exchange in 1..=iters {
                    let (received, exact) = if mode == Mode::Timing {
                        (arm.round_trip(payload.clone()).await, true)
                    } else {
                        match tokio::time::timeout(
                            exchange_timeout(param),
                            arm.round_trip_checked(payload.clone()),
                        )
                        .await
                        {
                            Ok(received) => received,
                            Err(_) => {
                                let failure_rss = rss_kib();
                                eprintln!(
                                    "PROBE-FAIL exchange={exchange} last_completed={} \
                                     reason=timeout timeout_ms={}",
                                    exchange - 1,
                                    exchange_timeout(param).as_millis()
                                );
                                emit_rss("failure-timeout", exchange, failure_rss);
                                emit_diagnostics(exchange, "failure-timeout", None);
                                flush_stderr();
                                panic!("exchange {exchange} exceeded its workload-scaled timeout");
                            }
                        }
                    };
                    // Capture immediately after the response drain, before exactness or
                    // diagnostic formatting can perturb the resident-set observation.
                    let post_drain_rss = (mode == Mode::Diagnostic).then(rss_kib).flatten();
                    if received != param || !exact {
                        eprintln!(
                            "PROBE-FAIL exchange={exchange} last_completed={} reason={} \
                             expected={param} actual={received}",
                            exchange - 1,
                            if received != param {
                                "wrong-length"
                            } else {
                                "wrong-content"
                            }
                        );
                        if mode == Mode::Diagnostic {
                            emit_rss("failure-response-drained", exchange, post_drain_rss);
                            emit_diagnostics(exchange, "failure-response-drained", None);
                        }
                        flush_stderr();
                        panic!("exchange {exchange} response was not exact");
                    }
                    black_box(received);
                    if mode == Mode::Diagnostic {
                        emit_rss("response-drained", exchange, post_drain_rss);
                        emit_diagnostics(exchange, "both-endpoints", Some(param));
                        eprintln!(
                            "PROBE-PROGRESS exchange={exchange} completed={exchange} \
                             expected_bytes={param} received_bytes={received}"
                        );
                        flush_stderr();
                    }
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
        } else {
            emit_diagnostics(iters, "final", None);
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
        assert!(validate_request("ngnet-h3-quinn", "body", 1024, 1, Mode::Diagnostic).is_err());
        assert!(validate_request("ngnet-quic-h3", "concurrent", 1, 1, Mode::Diagnostic).is_err());
    }

    #[test]
    fn timeouts_scale_with_body_size_and_build_profile() {
        assert!(exchange_timeout(1024 * 1024) > exchange_timeout(0));
        if cfg!(debug_assertions) {
            assert!(exchange_timeout(1024 * 1024) >= Duration::from_secs(45));
        } else {
            assert!(exchange_timeout(1024 * 1024) >= Duration::from_secs(15));
        }
    }
}
