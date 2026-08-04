//! Concurrent throughput, completion vs readiness: `N` requests issued together on one real
//! loopback connection and awaited as a group, so Criterion's per-iteration time covers `N`
//! whole exchanges. `Throughput::Elements` turns that into requests/sec. `N` sweeps the same
//! 1, 8, 64 as the duplex family, so the two are comparable in shape.
//!
//! Only the transport differs between the two arms — `CompioSocket` over io_uring against
//! `TokioSocket` over epoll — with `nghttp2` held constant on both. One worker thread each
//! (compio single-threaded, tokio `current_thread`), so this measures the I/O model rather
//! than one scheduler doing work the other's is spared.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use nghttp2_bench::{CompioSocket, TokioSocket, compio_runtime, current_thread_runtime};

const CONCURRENCY: [usize; 3] = [1, 8, 64];

fn transport_concurrent_throughput(c: &mut Criterion) {
    let compio = compio_runtime();
    let compio_socket = compio.block_on(CompioSocket::establish());

    let tokio = current_thread_runtime();
    let tokio_socket = tokio.block_on(TokioSocket::establish());

    let mut group = c.benchmark_group("transport_concurrent_throughput");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("compio", n), &n, |b, &n| {
            b.to_async(&compio).iter(|| compio_socket.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("tokio", n), &n, |b, &n| {
            b.to_async(&tokio).iter(|| tokio_socket.concurrent(n));
        });
    }
    group.finish();
}

criterion_group!(benches, transport_concurrent_throughput);
criterion_main!(benches);
