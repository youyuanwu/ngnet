//! A single-arm driver for profiling. Not a benchmark: it exists so `perf` and `strace` can be
//! pointed at exactly one fixture, which Criterion's multi-arm process makes impossible.
//!
//! Usage: `probe <arm> <workload> <param> <iters> [timing|diagnostic]`
//!   arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket |
//!              ngnet-h3-quinn | ngnet-quic-h3 | h3-quinn
//!   workload = body | concurrent
//!   param    = body size in bytes, or stream count
//!
//! The QUIC arms support `body` only. Diagnostic mode additionally requires
//! `--features diagnostics`; timing mode is always unarmed.

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
    async fn round_trip(&self, body: bytes::Bytes) -> (usize, bool) {
        match self {
            Arm::H2Duplex(a) => (a.round_trip(body).await, true),
            Arm::H2Socket(a) => (a.round_trip(body).await, true),
            Arm::QmuxDuplex(a) => (a.round_trip(body).await, true),
            Arm::QmuxSocket(a) => (a.round_trip(body).await, true),
            Arm::NgnetH3Quinn(a) => (a.round_trip(body).await, true),
            Arm::NgnetNgtcpH3(a) => a.round_trip_checked(body).await,
            Arm::UpstreamH3Quinn(a) => (a.round_trip(body).await, true),
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Timing,
    Diagnostic,
}

fn flush_stderr() {
    std::io::stderr().flush().expect("flushing probe output");
}

fn exchange_timeout(body_size: usize) -> Duration {
    let mib = body_size.div_ceil(1024 * 1024);
    Duration::from_secs(2 + (mib as u64).saturating_mul(3))
}

fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

#[cfg(feature = "diagnostics")]
fn emit_diagnostics(exchange: usize, scope: &str) {
    let attempts = ngnet_quic::diagnostics::take_attempts();
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
    for event in ngnet_quic::diagnostics::take_liveness_events() {
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
    let snapshot = ngnet_quic::diagnostics::snapshot();
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
        assert_eq!(
            values.inbound_drops, 0,
            "{role} observed an unexpected inbound drop at exchange {exchange}"
        );
        eprintln!(
            "PROBE-SNAPSHOT exchange={exchange} scope={scope} role={role} \
             offered={} prepared_backing_capacity={} accepted={} zero_acceptances={} \
             logical_retained={} logical_retained_high_water={} retained_backing_capacity={} \
             retained_backing_high_water={} release_bytes={} acknowledged_bytes={} \
             released_backing_capacity={} produced_packets={} transport_only_packets={} \
             stream_carrying_packets={} timer_rearms={} timer_fires={} wake_registrations={} inbound_wakes={} \
             capacity_registrations={} capacity_wakes={} retries={} parks={} \
             zero_accept_retries={} zero_accept_retries_without_enable={} \
             inbound_queue_depth={} inbound_queue_high_water={} inbound_drops={} \
             outbound_queue_depth={} outbound_queue_high_water={} \
             outbound_capacity_transitions={} retransmissions=unavailable overflow={}",
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
            snapshot.overflowed,
        );
    }
}

#[cfg(not(feature = "diagnostics"))]
fn emit_diagnostics(_exchange: usize, _scope: &str) {
    unreachable!("diagnostic mode is rejected before setup")
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
    assert!(iters > 0, "iters must be non-zero");
    if matches!(
        arm_name.as_str(),
        "ngnet-h3-quinn" | "ngnet-quic-h3" | "h3-quinn"
    ) && workload != "body"
    {
        panic!("the {arm_name} arm supports the body workload only");
    }
    if arm_name == "ngnet-quic-h3" && !matches!(param, 0 | 1024 | 16384 | 1_048_576) {
        panic!("ngnet-quic-h3 fixed-count probes support 0, 1024, 16384, or 1048576 bytes");
    }
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
            "ngnet-quic-h3" => Arm::NgnetNgtcpH3(NgnetNgtcpH3::establish().await),
            "h3-quinn" => Arm::UpstreamH3Quinn(UpstreamH3Quinn::establish().await),
            other => panic!("unknown arm {other}"),
        };

        // Setup plus an empty persistent exchange stays before readiness and every
        // observation. Keeping the warm-up fixed matters for failure routing: if a large
        // workload fails, it does so after readiness and is classified as workload failure
        // rather than disappearing inside setup.
        let (warmup_received, warmup_exact) = arm.round_trip(body_of(0)).await;
        assert_eq!(warmup_received, 0, "warm-up response was not exact");
        assert!(warmup_exact, "warm-up response bytes were not exact");

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
            match rss_kib() {
                Some(rss) => eprintln!("PROBE-RSS boundary=ready exchange=0 rss_kib={rss}"),
                None => eprintln!("PROBE-RSS boundary=ready exchange=0 rss_kib=unavailable"),
            }
            flush_stderr();
        }

        let body_payload = (workload == "body").then(|| body_of(param));
        let started = (mode == Mode::Timing).then(Instant::now);
        match workload.as_str() {
            "body" => {
                let payload = body_payload.expect("body workload has a payload");
                for exchange in 1..=iters {
                    let (received, exact) = if mode == Mode::Timing {
                        arm.round_trip(payload.clone()).await
                    } else {
                        match tokio::time::timeout(
                            exchange_timeout(param),
                            arm.round_trip(payload.clone()),
                        )
                        .await
                        {
                            Ok(received) => received,
                            Err(_) => {
                                eprintln!(
                                    "PROBE-FAIL exchange={exchange} last_completed={} \
                                     reason=timeout timeout_ms={}",
                                    exchange - 1,
                                    exchange_timeout(param).as_millis()
                                );
                                flush_stderr();
                                panic!("exchange {exchange} exceeded its workload-scaled timeout");
                            }
                        }
                    };
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
                            emit_diagnostics(exchange, "failure");
                        }
                        flush_stderr();
                        panic!("exchange {exchange} response was not exact");
                    }
                    black_box(received);
                    if mode == Mode::Diagnostic {
                        emit_diagnostics(exchange, "both-endpoints");
                        match rss_kib() {
                            Some(rss) => eprintln!(
                                "PROBE-RSS boundary=exchange exchange={exchange} rss_kib={rss}"
                            ),
                            None => eprintln!(
                                "PROBE-RSS boundary=exchange exchange={exchange} \
                                 rss_kib=unavailable"
                            ),
                        }
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
            let application_bytes = param.saturating_mul(iters).saturating_mul(2);
            eprintln!(
                "PROBE-TIMING elapsed_ns={} application_bytes={application_bytes}",
                elapsed.as_nanos()
            );
        } else {
            emit_diagnostics(iters, "final");
            match rss_kib() {
                Some(rss) => eprintln!("PROBE-RSS boundary=final exchange={iters} rss_kib={rss}"),
                None => {
                    eprintln!("PROBE-RSS boundary=final exchange={iters} rss_kib=unavailable")
                }
            }
        }
        eprintln!("PROBE-DONE completed={iters}");
        flush_stderr();
    });
}
