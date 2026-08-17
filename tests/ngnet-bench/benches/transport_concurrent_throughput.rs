//! Concurrent throughput on a real socket: `N` requests issued together on one connection and
//! awaited as a group, so Criterion's per-iteration time covers `N` whole exchanges.
//! `Throughput::Elements` turns that into requests/sec. `N` sweeps the same 1, 8, 64 as the
//! duplex family, so the two are comparable in shape.
//!
//! Three arms, read pairwise: `ngnet-h2-compio` against `ngnet-h2-tokio` isolates the I/O model,
//! `ngnet-h2-tokio` against `hyper-tokio` isolates the HTTP/2 stack, and `ngnet-h2-compio` against
//! `hyper-tokio` varies both. One worker thread each (compio single-threaded, tokio
//! `current_thread`), so no arm gets to spread over cores the others cannot.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{CompioSocket, HyperSocket, TokioSocket, compio_runtime, current_thread_runtime};

const CONCURRENCY: [usize; 3] = [1, 8, 64];

fn transport_concurrent_throughput(c: &mut Criterion) {
    let compio = compio_runtime();
    let compio_socket = compio.block_on(CompioSocket::establish());

    // One runtime per arm; see `transport_serial_latency` for why.
    let tokio = current_thread_runtime();
    let tokio_socket = tokio.block_on(TokioSocket::establish());

    let hyper = current_thread_runtime();
    let hyper_socket = hyper.block_on(HyperSocket::establish());

    let mut group = c.benchmark_group("transport_concurrent_throughput");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("ngnet-h2-compio", n), &n, |b, &n| {
            b.to_async(&compio).iter(|| compio_socket.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("ngnet-h2-tokio", n), &n, |b, &n| {
            b.to_async(&tokio).iter(|| tokio_socket.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("hyper-tokio", n), &n, |b, &n| {
            b.to_async(&hyper).iter(|| hyper_socket.concurrent(n));
        });
    }
    group.finish();
}

criterion_group!(benches, transport_concurrent_throughput);
criterion_main!(benches);
