//! The shared-body comparison on a real socket: does handing bodies over actually beat
//! copying them?
//!
//! This is the benchmark the no-copy work is judged on. Each arm is paired with its own twin,
//! identical in every respect but the connection entry point — `handshake_shared_with` versus
//! `handshake_with` — so a difference between a pair is the body strategy or it is drift.
//!
//! # Reading this honestly
//!
//! `docs/benchmarks/controls.md` records that grouped A/B designs are untrustworthy on the
//! host these results were taken on: unchanged control arms drifted 5–11% inside a single
//! session. Three things guard against mistaking that drift for a result.
//!
//! 1. **The pairs are adjacent.** Within each size, `push` and `shared` for a transport run
//!    back to back, so the two halves of a comparison sit as close together in time as
//!    Criterion allows. Sizes are the outer loop, arms the inner one — never all of one arm
//!    and then all of the other. This is adjacency, not sample-level interleaving: Criterion
//!    samples one benchmark to completion before starting the next, and no arrangement of
//!    `bench_with_input` calls can change that. Replication is what covers the rest — the
//!    recorded result aggregates paired deltas over ten independent runs of this file, so a
//!    slow drift must bias every one of them in the same direction to survive.
//! 2. **`hyper-tokio` is carried as a drift control.** Nothing in this work touches hyper, so
//!    whatever it does between runs is the session's noise floor. A shared-versus-push
//!    difference smaller than the control's own movement is not a result.
//! 3. **The 0-byte point is a second control, and a mechanistic one.** With no body there is
//!    no memset, no source copy and no coalescing copy to remove, so the shared path can only
//!    be *level* with the push path there. If 0 B shows a difference, the harness is measuring
//!    something other than what it claims.
//!
//! A gain that shows up here but not in the duplex family (`shared_body.rs`), with no mechanism
//! to explain the difference, is drift. Two apparent regressions in PR #7 dissolved exactly
//! that way.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{
    CompioSharedSocket, CompioSocket, HyperSocket, TokioSharedSocket, TokioSocket, body_of,
    compio_runtime, current_thread_runtime,
};

/// 0 B is the mechanistic control described in the module docs; the rest are the sweep the
/// other families use, so results are comparable in shape.
const SIZES: [usize; 4] = [0, 1024, 64 * 1024, 1024 * 1024];

fn transport_shared_body(c: &mut Criterion) {
    // One runtime per arm, as the other transport benches do: sharing a runtime between arms
    // would let one arm's leftover wakeups land inside another's measurement.
    let compio_push = compio_runtime();
    let compio_push_socket = compio_push.block_on(CompioSocket::establish());

    let compio_shared = compio_runtime();
    let compio_shared_socket = compio_shared.block_on(CompioSharedSocket::establish());

    let tokio_push = current_thread_runtime();
    let tokio_push_socket = tokio_push.block_on(TokioSocket::establish());

    let tokio_shared = current_thread_runtime();
    let tokio_shared_socket = tokio_shared.block_on(TokioSharedSocket::establish());

    let hyper = current_thread_runtime();
    let hyper_socket = hyper.block_on(HyperSocket::establish());

    let mut group = c.benchmark_group("transport_shared_body");
    for size in SIZES {
        // `Throughput::Bytes(0)` would report a meaningless MB/s, so the empty-body control is
        // reported per-iteration instead.
        if size == 0 {
            group.throughput(Throughput::Elements(1));
        } else {
            group.throughput(Throughput::Bytes(size as u64));
        }
        let payload = body_of(size);

        // The completion pair first. That ordering was chosen because this pair was predicted
        // to show the largest effect — the push path here pays a coalescing copy the readiness
        // paths do not. The prediction was wrong: it measures the *smallest* effect, because
        // the coalescing path had already collapsed a pass into one write and so had no
        // syscall prize left to win. The order is kept anyway, since changing it would make
        // these results incomparable with the ones already recorded.
        group.bench_with_input(BenchmarkId::new("compio-push", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&compio_push)
                .iter(|| async { black_box(compio_push_socket.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("compio-shared", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&compio_shared).iter(|| async {
                black_box(compio_shared_socket.round_trip(payload.clone()).await)
            });
        });

        group.bench_with_input(BenchmarkId::new("tokio-push", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&tokio_push)
                .iter(|| async { black_box(tokio_push_socket.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("tokio-shared", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&tokio_shared).iter(|| async {
                black_box(tokio_shared_socket.round_trip(payload.clone()).await)
            });
        });

        // The drift control. Untouched by this work, so its movement between runs is the
        // session's noise floor and the bar any claimed gain has to clear.
        group.bench_with_input(BenchmarkId::new("hyper-tokio", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&hyper)
                .iter(|| async { black_box(hyper_socket.round_trip(payload.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, transport_shared_body);
criterion_main!(benches);
