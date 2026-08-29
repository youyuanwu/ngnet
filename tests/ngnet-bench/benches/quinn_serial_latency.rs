//! Persistent-connection HTTP/3 serial latency over Quinn loopback.
//!
//! Both arms use the same Quinn version, Tokio runtime shape, TLS identity, ALPN, request,
//! response, and full-body drain. The differing axis is the HTTP/3 implementation and its
//! Quinn adapter: `ngnet-h3` + `ngnet-h3-quinn` versus upstream `h3` + `h3-quinn`.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ngnet_bench::{NgnetH3Quinn, UpstreamH3Quinn, current_thread_runtime};

fn quinn_serial_latency(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetH3Quinn::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Quinn::establish());

    let mut group = c.benchmark_group("quinn_serial_latency");
    group.bench_function("ngnet-h3-quinn", |b| {
        b.to_async(&ngnet_runtime)
            .iter(|| async { black_box(ngnet.round_trip(Bytes::new()).await) });
    });
    group.bench_function("h3-quinn", |b| {
        b.to_async(&upstream_runtime)
            .iter(|| async { black_box(upstream.round_trip(Bytes::new()).await) });
    });
    group.finish();
}

criterion_group!(benches, quinn_serial_latency);
criterion_main!(benches);
