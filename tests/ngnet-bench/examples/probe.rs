//! A single-arm driver for profiling. Not a benchmark: it exists so `perf` and `strace` can be
//! pointed at exactly one fixture, which Criterion's multi-arm process makes impossible.
//!
//! Usage: `probe <arm> <workload> <param> <iters>`
//!   arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket |
//!              ngnet-h3-quinn | h3-quinn
//!   workload = body | concurrent
//!   param    = body size in bytes, or stream count
//!
//! The Quinn arms support `body` only. Their fixtures expose the persistent serial
//! round-trip used by the Quinn benchmarks, not the unrelated concurrency workload.

use std::hint::black_box;

use ngnet_bench::{
    NgnetH2, NgnetH3Quinn, NgnetQmuxH3, NgnetQmuxH3Socket, TokioSocket, UpstreamH3Quinn, body_of,
    current_thread_runtime,
};

enum Arm {
    H2Duplex(NgnetH2),
    H2Socket(TokioSocket),
    QmuxDuplex(NgnetQmuxH3),
    QmuxSocket(NgnetQmuxH3Socket),
    NgnetH3Quinn(NgnetH3Quinn),
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
            Arm::UpstreamH3Quinn(a) => a.round_trip(body).await,
        }
    }

    async fn concurrent(&self, n: usize) {
        match self {
            Arm::H2Duplex(a) => a.concurrent(n).await,
            Arm::H2Socket(a) => a.concurrent(n).await,
            Arm::QmuxDuplex(a) => a.concurrent(n).await,
            Arm::QmuxSocket(a) => a.concurrent(n).await,
            Arm::NgnetH3Quinn(_) | Arm::UpstreamH3Quinn(_) => {
                unreachable!("Quinn arms reject concurrent before setup")
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arm_name = args.get(1).expect("arm").clone();
    let workload = args.get(2).expect("workload").clone();
    let param: usize = args.get(3).expect("param").parse().expect("a number");
    let iters: usize = args.get(4).expect("iters").parse().expect("a number");
    if matches!(arm_name.as_str(), "ngnet-h3-quinn" | "h3-quinn") && workload != "body" {
        panic!("the {arm_name} arm supports the body workload only");
    }

    let rt = current_thread_runtime();
    rt.block_on(async move {
        let arm = match arm_name.as_str() {
            "h2-duplex" => Arm::H2Duplex(NgnetH2::establish().await),
            "h2-socket" => Arm::H2Socket(TokioSocket::establish().await),
            "qmux-duplex" => Arm::QmuxDuplex(NgnetQmuxH3::establish().await),
            "qmux-socket" => Arm::QmuxSocket(NgnetQmuxH3Socket::establish().await),
            "ngnet-h3-quinn" => Arm::NgnetH3Quinn(NgnetH3Quinn::establish().await),
            "h3-quinn" => Arm::UpstreamH3Quinn(UpstreamH3Quinn::establish().await),
            other => panic!("unknown arm {other}"),
        };

        // A warm-up exchange outside the measured region, matching what the fixtures do.
        arm.round_trip(body_of(0)).await;

        eprintln!("PROBE-READY");
        match workload.as_str() {
            "body" => {
                let payload = body_of(param);
                for _ in 0..iters {
                    black_box(arm.round_trip(payload.clone()).await);
                }
            }
            "concurrent" => {
                for _ in 0..iters {
                    arm.concurrent(param).await;
                }
            }
            other => panic!("unknown workload {other}"),
        }
        eprintln!("PROBE-DONE");
    });
}
