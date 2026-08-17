//! Body throughput: a request/response body sweep on a persistent connection, with
//! `Throughput::Bytes` so Criterion reports MB/s. The server echoes the body, so each
//! iteration moves `size` bytes up and `size` back; throughput is normalised to one body's
//! worth, which is the number reported.
//!
//! The sweep is where flow control and the read-buffer pool start to matter: at 1 MiB the
//! 64 KiB initial window (matched between the two stacks) forces repeated `WINDOW_UPDATE`
//! round trips, so this is as much a flow-control benchmark as a copy benchmark.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, body_of, current_thread_runtime};

/// 0 B exercises the headers-only path; the rest climb until the initial window and the
/// buffer pool dominate.
const SIZES: [usize; 4] = [0, 1024, 64 * 1024, 1024 * 1024];

fn body_throughput(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("body_throughput");
    for size in SIZES {
        // `Throughput::Bytes(0)` would report a meaningless MB/s, so the empty-body point is
        // reported per-iteration instead. Every non-empty size is reported as bytes/sec.
        if size == 0 {
            group.throughput(Throughput::Elements(1));
        } else {
            group.throughput(Throughput::Bytes(size as u64));
        }
        let payload = body_of(size);

        group.bench_with_input(BenchmarkId::new("ngnet-h2", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(ngnet_h2.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("hyper", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(hyper.round_trip(payload.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, body_throughput);
criterion_main!(benches);
