//! Concurrent throughput: `N` requests issued together on one connection and awaited as a
//! group, so Criterion's per-iteration time covers `N` whole exchanges. `Throughput::Elements`
//! turns that into requests/sec. `N` is swept over 1, 8, 64 to show multiplexing that serial
//! latency cannot.
//!
//! Two groups run the same sweep. The single-threaded one is the deterministic headline: no
//! syscalls means a multi-threaded scheduler would only add cross-thread wakeup noise. The
//! multi-threaded one is kept separate and clearly named for anyone who wants to see what a
//! four-worker scheduler does to the same work.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use nghttp2_bench::{Hyper, Ngrs, current_thread_runtime, multi_thread_runtime};

const CONCURRENCY: [usize; 3] = [1, 8, 64];

fn concurrent_throughput(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngrs = runtime.block_on(Ngrs::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("concurrent_throughput");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("ngrs", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| ngrs.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("hyper", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| hyper.concurrent(n));
        });
    }
    group.finish();
}

fn concurrent_throughput_multi_thread(c: &mut Criterion) {
    let runtime = multi_thread_runtime(4);
    let ngrs = runtime.block_on(Ngrs::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("concurrent_throughput_multi_thread");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("ngrs", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| ngrs.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("hyper", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| hyper.concurrent(n));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    concurrent_throughput,
    concurrent_throughput_multi_thread
);
criterion_main!(benches);
