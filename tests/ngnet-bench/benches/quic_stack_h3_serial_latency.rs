//! Matched ngnet and hyperium H3 serial latency over the *same* ngtcp2 QUIC transport.
//!
//! The question is narrow: with the transport, its credentials, its configuration and the task
//! topology all held equal, what does changing the HTTP/3 implementation and its adapter cost?
//! Both arms run `ngnet-quic` over a real loopback socket; one drives it with `ngnet-h3`
//! through `ngnet-quic-h3`, the other with hyperium `h3` through `h3-ngnet-quic`.
//!
//! Each arm gets its own current-thread runtime, a persistent connection, one spawned endpoint
//! driver plus one spawned HTTP/3 driver per endpoint, and one explicit empty warm-up inside
//! `establish()` and therefore outside the measured closure. The native arm is the
//! QPACK-matched fixture, not the default one: hyperium 0.0.8 has no dynamic table and
//! `ngnet-h3` defaults to 4 KiB, so the default pair would differ in header state.
//!
//! Five asymmetries could not be removed. They are enumerated next to the fixtures in
//! `ngnet-bench`'s library, and repeated with the results in
//! `docs/benchmarks/cases/quic-h3-comparison.md`. The one to keep in mind while reading a
//! number: `ngnet-h3` advances its state machine in its spawned driver task, while hyperium
//! advances a request stream from the task polling it — which here is the task inside the
//! measured closure. UDP I/O is shared and symmetric; the h3-to-stream driving is not.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ngnet_bench::{NgnetNgtcpH3Matched, UpstreamH3Ngtcp, current_thread_runtime};

fn quic_stack_h3_serial_latency(c: &mut Criterion) {
    let ngnet_runtime = current_thread_runtime();
    let ngnet = ngnet_runtime.block_on(NgnetNgtcpH3Matched::establish());
    let upstream_runtime = current_thread_runtime();
    let upstream = upstream_runtime.block_on(UpstreamH3Ngtcp::establish());

    let mut group = c.benchmark_group("quic_stack_h3_serial_latency");
    group.bench_function("ngnet-quic-h3", |b| {
        b.to_async(&ngnet_runtime)
            .iter(|| async { black_box(ngnet.round_trip(Bytes::new()).await) });
    });
    group.bench_function("h3-ngnet-quic", |b| {
        b.to_async(&upstream_runtime)
            .iter(|| async { black_box(upstream.round_trip(Bytes::new()).await) });
    });
    group.finish();
}

criterion_group!(benches, quic_stack_h3_serial_latency);
criterion_main!(benches);
