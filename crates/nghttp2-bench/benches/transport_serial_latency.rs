//! Serial latency, completion vs readiness: one request in flight at a time on a persistent
//! connection, over a real loopback socket. The HTTP/2 stack is `nghttp2` on both arms; only
//! the transport differs — `CompioSocket` over io_uring against `TokioSocket` over epoll — so
//! what this isolates is the I/O model, not the stack. Empty body, so what is timed is the
//! per-request round trip through the kernel and back, which is exactly where a completion
//! runtime differs from a readiness one.
//!
//! The two runtimes cannot nest, but they never have to: each connection is stood up once
//! outside the timed closure on its own runtime, and Criterion drives the two bench functions
//! one after another, each on the runtime its arm was established on. See `docs/benchmarks.md`
//! for the confounds this comparison controls and the ones it cannot.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use nghttp2_bench::{CompioSocket, TokioSocket, compio_runtime, current_thread_runtime};

fn transport_serial_latency(c: &mut Criterion) {
    let compio = compio_runtime();
    let compio_socket = compio.block_on(CompioSocket::establish());

    let tokio = current_thread_runtime();
    let tokio_socket = tokio.block_on(TokioSocket::establish());

    let mut group = c.benchmark_group("transport_serial_latency");

    group.bench_function("compio", |b| {
        b.to_async(&compio)
            .iter(|| async { black_box(compio_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("tokio", |b| {
        b.to_async(&tokio)
            .iter(|| async { black_box(tokio_socket.round_trip(Bytes::new()).await) });
    });

    group.finish();
}

criterion_group!(benches, transport_serial_latency);
criterion_main!(benches);
