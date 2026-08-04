//! Body throughput on a real socket: a request/response body sweep on a persistent connection,
//! with `Throughput::Bytes` so Criterion reports MB/s. The server echoes the body, so each
//! iteration moves `size` bytes up and `size` back; throughput is normalised to one body's
//! worth. The sweep reuses the duplex family's points so the two are comparable in shape.
//!
//! Three arms, read pairwise: `ngrs-compio` against `ngrs-tokio` isolates the I/O model,
//! `ngrs-tokio` against `hyper-tokio` isolates the HTTP/2 stack, and `ngrs-compio` against
//! `hyper-tokio` varies both.
//!
//! This is where the write-path asymmetry named in `docs/benchmarks.md` bites hardest: the two
//! readiness arms buffer or borrow outbound bytes in ways the completion arm structurally
//! cannot, so a large-body difference is partly write strategy and not purely I/O model or
//! stack.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use nghttp2_bench::{
    CompioSocket, HyperSocket, TokioSocket, body_of, compio_runtime, current_thread_runtime,
};

/// 0 B exercises the headers-only path; the rest climb until the initial window and the
/// buffer pool dominate. The same points as the duplex `body_throughput` bench.
const SIZES: [usize; 4] = [0, 1024, 64 * 1024, 1024 * 1024];

fn transport_body_throughput(c: &mut Criterion) {
    let compio = compio_runtime();
    let compio_socket = compio.block_on(CompioSocket::establish());

    // One runtime per arm; see `transport_serial_latency` for why.
    let tokio = current_thread_runtime();
    let tokio_socket = tokio.block_on(TokioSocket::establish());

    let hyper = current_thread_runtime();
    let hyper_socket = hyper.block_on(HyperSocket::establish());

    let mut group = c.benchmark_group("transport_body_throughput");
    for size in SIZES {
        // `Throughput::Bytes(0)` would report a meaningless MB/s, so the empty-body point is
        // reported per-iteration instead. Every non-empty size is reported as bytes/sec.
        if size == 0 {
            group.throughput(Throughput::Elements(1));
        } else {
            group.throughput(Throughput::Bytes(size as u64));
        }
        let payload = body_of(size);

        group.bench_with_input(BenchmarkId::new("ngrs-compio", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&compio)
                .iter(|| async { black_box(compio_socket.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("ngrs-tokio", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&tokio)
                .iter(|| async { black_box(tokio_socket.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("hyper-tokio", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&hyper)
                .iter(|| async { black_box(hyper_socket.round_trip(payload.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, transport_body_throughput);
criterion_main!(benches);
