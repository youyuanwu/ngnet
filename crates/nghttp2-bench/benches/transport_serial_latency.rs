//! Serial latency on a real socket: one request in flight at a time on a persistent
//! connection, empty body. Three arms, to be read pairwise — `ngrs-compio` against
//! `ngrs-tokio` isolates the I/O model (completion against readiness, same stack);
//! `ngrs-tokio` against `hyper-tokio` isolates the HTTP/2 stack (this crate against the
//! reference implementation, same I/O); `ngrs-compio` against `hyper-tokio` varies both and
//! is attributable to neither.
//!
//! Empty body, so what is timed is the per-request round trip through the kernel and back,
//! which is exactly where a completion runtime differs from a readiness one.
//!
//! The two runtimes cannot nest, but they never have to: each connection is stood up once
//! outside the timed closure on its own runtime, and Criterion drives the bench functions
//! one after another, each on the runtime its arm was established on. See
//! `docs/benchmarks.md` for the confounds this comparison controls and the ones it cannot.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use nghttp2_bench::{
    CompioSocket, HyperSocket, TokioSocket, compio_runtime, current_thread_runtime,
};

fn transport_serial_latency(c: &mut Criterion) {
    let compio = compio_runtime();
    let compio_socket = compio.block_on(CompioSocket::establish());

    // One runtime per arm, so neither tokio arm's connection drivers share a scheduler with
    // the other's. Criterion runs the arms one at a time, but an idle connection's driver
    // task is still registered, and the cheapest way to keep it out of the measurement is to
    // keep it out of the runtime.
    let tokio = current_thread_runtime();
    let tokio_socket = tokio.block_on(TokioSocket::establish());

    let hyper = current_thread_runtime();
    let hyper_socket = hyper.block_on(HyperSocket::establish());

    let mut group = c.benchmark_group("transport_serial_latency");

    group.bench_function("ngrs-compio", |b| {
        b.to_async(&compio)
            .iter(|| async { black_box(compio_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("ngrs-tokio", |b| {
        b.to_async(&tokio)
            .iter(|| async { black_box(tokio_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("hyper-tokio", |b| {
        b.to_async(&hyper)
            .iter(|| async { black_box(hyper_socket.round_trip(Bytes::new()).await) });
    });

    group.finish();
}

criterion_group!(benches, transport_serial_latency);
criterion_main!(benches);
