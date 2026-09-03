//! Matched ngnet and hyperium H3 body throughput over the *same* ngtcp2 QUIC transport.
//!
//! The companion to `quic_stack_h3_serial_latency`: same two arms, same equalisation, with a
//! body to echo. Throughput counts both directions; establishment and warm-up stay outside the
//! measured closures.
//!
//! # Why only 1 KiB
//!
//! Not because larger bodies are uninteresting, but because this transport has a known,
//! unresolved intermittent connection-ending stall under repeated 16 KiB and 1 MiB workloads —
//! review finding S9, recorded in `docs/quic-h3/invariants.md` and
//! `docs/benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md`. The existing
//! `quic_stack_body_throughput` target restricts itself to 1 KiB for exactly this reason, and
//! a committed sweep that intermittently kills its own connection would produce numbers nobody
//! should trust.
//!
//! Larger payloads are not simply dropped: they are run as supervised, reportable probes on
//! **both** arms through the `probe` example, so the stall's effect on payload coverage is
//! stated from evidence rather than assumed. Nothing here retries a failed pass into a clean
//! result.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ngnet_bench::{NgnetNgtcpH3Matched, UpstreamH3Ngtcp, body_of, current_thread_runtime};

const SIZES: &[usize] = &[1024];

fn quic_stack_h3_body_throughput(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetNgtcpH3Matched::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Ngtcp::establish());

    let mut group = c.benchmark_group("quic_stack_h3_body_throughput");
    for &size in SIZES {
        let body = body_of(size);
        group.throughput(Throughput::Bytes((size * 2) as u64));
        group.bench_with_input(BenchmarkId::new("ngnet-quic-h3", size), &body, |b, body| {
            b.to_async(&ngnet_runtime)
                .iter(|| async { black_box(ngnet.round_trip(body.clone()).await) });
        });
        group.bench_with_input(BenchmarkId::new("h3-ngnet-quic", size), &body, |b, body| {
            b.to_async(&upstream_runtime)
                .iter(|| async { black_box(upstream.round_trip(body.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, quic_stack_h3_body_throughput);
criterion_main!(benches);
