//! Persistent-connection HTTP/3 body round trips over Quinn loopback.
//!
//! Sizes are the outer loop and the two implementations run adjacently at each size. The
//! payload is reference-counted outside the measured closure; both servers fully collect it,
//! echo it in one body, and both clients drain that response before an iteration completes.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ngnet_bench::{NgnetH3Quinn, UpstreamH3Quinn, body_of, current_thread_runtime};

const SIZES: &[usize] = &[16 * 1024, 1024 * 1024];

fn quinn_body_throughput(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetH3Quinn::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Quinn::establish());

    let mut group = c.benchmark_group("quinn_body_throughput");
    for &size in SIZES {
        let body = body_of(size);
        group.throughput(Throughput::Bytes((size * 2) as u64));
        group.bench_with_input(
            BenchmarkId::new("ngnet-h3-quinn", size),
            &body,
            |b, body| {
                b.to_async(&ngnet_runtime)
                    .iter(|| async { black_box(ngnet.round_trip(body.clone()).await) });
            },
        );
        group.bench_with_input(BenchmarkId::new("h3-quinn", size), &body, |b, body| {
            b.to_async(&upstream_runtime)
                .iter(|| async { black_box(upstream.round_trip(body.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, quinn_body_throughput);
criterion_main!(benches);
