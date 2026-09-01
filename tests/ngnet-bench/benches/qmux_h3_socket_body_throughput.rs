//! Matched ngnet and hyperium H3 body round trips over QMux and loopback TCP.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ngnet_bench::{
    NgnetQmuxH3MatchedSocket, UpstreamH3QmuxSocket, body_of, current_thread_runtime,
};

const SIZES: &[usize] = &[0, 1024, 64 * 1024, 1024 * 1024, 8 * 1024 * 1024];

fn qmux_h3_socket_body_throughput(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetQmuxH3MatchedSocket::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3QmuxSocket::establish());
    let mut group = c.benchmark_group("qmux_h3_socket_body_throughput");

    for &size in SIZES {
        let body = body_of(size);
        group.throughput(Throughput::Bytes((size * 2) as u64));
        group.bench_with_input(BenchmarkId::new("ngnet-qmux-h3", size), &body, |b, body| {
            b.to_async(&ngnet_runtime)
                .iter(|| async { black_box(ngnet.round_trip(body.clone()).await) });
        });
        group.bench_with_input(BenchmarkId::new("h3-ngnet-qmux", size), &body, |b, body| {
            b.to_async(&upstream_runtime)
                .iter(|| async { black_box(upstream.round_trip(body.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, qmux_h3_socket_body_throughput);
criterion_main!(benches);
