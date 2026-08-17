//! Serial latency on a real socket: one request in flight at a time on a persistent
//! connection, empty body. Four arms, to be read pairwise — `ngnet-h2-compio` against
//! `ngnet-h2-tokio` isolates the I/O model (completion against readiness, same stack);
//! `ngnet-h2-tokio` against `ngnet-qmux-h3-tokio` isolates the protocol stack (HTTP/2 against
//! HTTP/3-over-QMux, same I/O and same runtime arrangement);
//! `ngnet-h2-tokio` against `hyper-tokio` isolates the HTTP/2 stack (this crate against the
//! reference implementation, same I/O); `ngnet-h2-compio` against `hyper-tokio` varies both and
//! is attributable to neither.
//!
//! The QMux arm is paired with `ngnet-h2-tokio` and not with `ngnet-h2-compio` because it is
//! tokio-based: there is no completion-transport QMux arm to pair with the compio one, so
//! `ngnet-h2-tokio` is the only arm that differs from it in protocol alone. It is registered
//! immediately after `ngnet-h2-tokio` for the same reason — Criterion emits in registration
//! order, and putting `hyper-tokio` between the two halves of the cross-protocol comparison
//! would break the adjacency `docs/benchmarks/controls.md` treats as a control.
//!
//! Empty body, so what is timed is the per-request round trip through the kernel and back,
//! which is exactly where a completion runtime differs from a readiness one.
//!
//! The two runtimes cannot nest, but they never have to: each connection is stood up once
//! outside the timed closure on its own runtime, and Criterion drives the bench functions
//! one after another, each on the runtime its arm was established on. See
//! `docs/benchmarks/controls.md` for the confounds this comparison controls and the ones
//! it cannot.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use ngnet_bench::{
    CompioSocket, HyperSocket, NgnetQmuxH3Socket, TokioSocket, compio_runtime,
    current_thread_runtime,
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

    // The QMux arm gets its own runtime for exactly the reason above, and a current-thread one
    // so it is given no more parallelism than any other arm here.
    let qmux = current_thread_runtime();
    let qmux_socket = qmux.block_on(NgnetQmuxH3Socket::establish());

    let hyper = current_thread_runtime();
    let hyper_socket = hyper.block_on(HyperSocket::establish());

    let mut group = c.benchmark_group("transport_serial_latency");

    group.bench_function("ngnet-h2-compio", |b| {
        b.to_async(&compio)
            .iter(|| async { black_box(compio_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("ngnet-h2-tokio", |b| {
        b.to_async(&tokio)
            .iter(|| async { black_box(tokio_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("ngnet-qmux-h3-tokio", |b| {
        b.to_async(&qmux)
            .iter(|| async { black_box(qmux_socket.round_trip(Bytes::new()).await) });
    });

    group.bench_function("hyper-tokio", |b| {
        b.to_async(&hyper)
            .iter(|| async { black_box(hyper_socket.round_trip(Bytes::new()).await) });
    });

    group.finish();
}

criterion_group!(benches, transport_serial_latency);
criterion_main!(benches);
