//! Persistent-connection HTTP/3 body round trips across complete QUIC stacks.
//!
//! Each server fully collects and echoes the same 1 KiB body. Throughput counts both
//! directions; setup and warm-up remain outside the measured closures.
//!
//! The existing Quinn-only target retains its 16 KiB and 1 MiB cases. They are deliberately
//! absent here: repeated 16 KiB exchanges can stall or close the current ngtcp2 path, while
//! repeated 1 MiB exchanges can crash in native code. Including either point would prevent
//! the multi-arm process from reliably producing a measurement.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ngnet_bench::{NgnetH3Quinn, NgnetNgtcpH3, UpstreamH3Quinn, body_of, current_thread_runtime};

const SIZES: &[usize] = &[1024];

fn quic_stack_body_throughput(c: &mut Criterion) {
    let quinn_runtime = current_thread_runtime();
    let quinn = quinn_runtime.block_on(NgnetH3Quinn::establish());
    let ngtcp2_runtime = current_thread_runtime();
    let ngtcp2 = ngtcp2_runtime.block_on(NgnetNgtcpH3::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Quinn::establish());

    let mut group = c.benchmark_group("quic_stack_body_throughput");
    for &size in SIZES {
        let body = body_of(size);
        group.throughput(Throughput::Bytes((size * 2) as u64));
        group.bench_with_input(
            BenchmarkId::new("ngnet-h3-quinn", size),
            &body,
            |b, body| {
                b.to_async(&quinn_runtime)
                    .iter(|| async { black_box(quinn.round_trip(body.clone()).await) });
            },
        );
        group.bench_with_input(BenchmarkId::new("ngnet-quic-h3", size), &body, |b, body| {
            b.to_async(&ngtcp2_runtime)
                .iter(|| async { black_box(ngtcp2.round_trip(body.clone()).await) });
        });
        group.bench_with_input(BenchmarkId::new("h3-quinn", size), &body, |b, body| {
            b.to_async(&upstream_runtime)
                .iter(|| async { black_box(upstream.round_trip(body.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, quic_stack_body_throughput);
criterion_main!(benches);
