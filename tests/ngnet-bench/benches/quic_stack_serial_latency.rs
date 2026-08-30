//! Persistent-connection HTTP/3 serial latency across complete QUIC stacks.
//!
//! All arms use current-thread Tokio runtimes, loopback UDP, the `h3` ALPN, equivalent
//! self-signed certificate trust, the same request and echo handler, warmed persistent
//! connections, and a full response drain. The ngtcp2/OpenSSL arm necessarily varies the
//! QUIC and TLS implementation in addition to the transport adapter.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ngnet_bench::{NgnetH3Quinn, NgnetNgtcpH3, UpstreamH3Quinn, current_thread_runtime};

fn quic_stack_serial_latency(c: &mut Criterion) {
    let quinn_runtime = current_thread_runtime();
    let quinn = quinn_runtime.block_on(NgnetH3Quinn::establish());
    let ngtcp2_runtime = current_thread_runtime();
    let ngtcp2 = ngtcp2_runtime.block_on(NgnetNgtcpH3::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Quinn::establish());

    let mut group = c.benchmark_group("quic_stack_serial_latency");
    group.bench_function("ngnet-h3-quinn", |b| {
        b.to_async(&quinn_runtime)
            .iter(|| async { black_box(quinn.round_trip(Bytes::new()).await) });
    });
    group.bench_function("ngnet-quic-h3", |b| {
        b.to_async(&ngtcp2_runtime)
            .iter(|| async { black_box(ngtcp2.round_trip(Bytes::new()).await) });
    });
    group.bench_function("h3-quinn", |b| {
        b.to_async(&upstream_runtime)
            .iter(|| async { black_box(upstream.round_trip(Bytes::new()).await) });
    });
    group.finish();
}

criterion_group!(benches, quic_stack_serial_latency);
criterion_main!(benches);
