//! Matched ngnet and hyperium H3 serial latency over QMux and Tokio duplex.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ngnet_bench::{NgnetQmuxH3Matched, UpstreamH3Qmux, current_thread_runtime};

fn qmux_h3_serial_latency(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetQmuxH3Matched::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Qmux::establish());

    let mut group = c.benchmark_group("qmux_h3_serial_latency");
    group.bench_function("ngnet-qmux-h3", |b| {
        b.to_async(&ngnet_runtime)
            .iter(|| async { black_box(ngnet.round_trip(Bytes::new()).await) });
    });
    group.bench_function("h3-ngnet-qmux", |b| {
        b.to_async(&upstream_runtime)
            .iter(|| async { black_box(upstream.round_trip(Bytes::new()).await) });
    });
    group.finish();
}

criterion_group!(benches, qmux_h3_serial_latency);
criterion_main!(benches);
