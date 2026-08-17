//! Concurrent throughput: `N` requests issued together on one connection and awaited as a
//! group, so Criterion's per-iteration time covers `N` whole exchanges. `Throughput::Elements`
//! turns that into requests/sec. `N` is swept over 1, 8, 64 to show multiplexing that serial
//! latency cannot.
//!
//! Two groups run the same sweep. The single-threaded one is the deterministic headline: no
//! syscalls means a multi-threaded scheduler would only add cross-thread wakeup noise. The
//! multi-threaded one is kept separate and clearly named for anyone who wants to see what a
//! four-worker scheduler does to the same work.
//!
//! The two groups do not carry the same arms, and the asymmetry is deliberate.
//! `concurrent_throughput` has three: `ngnet-h2` against `ngnet-qmux-h3` varies only the
//! protocol stack and is the cross-protocol comparison, `ngnet-h2` against `hyper` varies only
//! the HTTP/2 implementation and is carried unchanged from before the HTTP/3 arm existed, and
//! `ngnet-qmux-h3` against `hyper` varies both and attributes to neither. They are registered
//! `ngnet-h2`, `ngnet-qmux-h3`, `hyper` inside the concurrency loop, so at each `N` the two
//! halves of the cross-protocol comparison are emitted back to back — concurrency is the outer
//! loop and the arms are the inner one, the arrangement `docs/benchmarks/controls.md` fixes as
//! a control. `concurrent_throughput_multi_thread` has only the two HTTP/2 arms, for the
//! reason given at that function.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, NgnetQmuxH3, current_thread_runtime, multi_thread_runtime};

const CONCURRENCY: [usize; 3] = [1, 8, 64];

fn concurrent_throughput(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    // Established and warmed on the same runtime as the other duplex arms; the QMux fixture
    // checks each requested concurrency against the limit both stacks are configured with
    // before anything reaches the wire, so an inadmissible `N` fails loudly at setup of the
    // iteration rather than resetting exchanges mid-sample.
    let qmux_h3 = runtime.block_on(NgnetQmuxH3::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("concurrent_throughput");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("ngnet-h2", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| ngnet_h2.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("ngnet-qmux-h3", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| qmux_h3.concurrent(n));
        });

        group.bench_with_input(BenchmarkId::new("hyper", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| hyper.concurrent(n));
        });
    }
    group.finish();
}

/// The same sweep on a four-worker scheduler — and, unlike every other group this work
/// touched, with **no HTTP/3-over-QMux arm**. That omission is deliberate and must not be
/// "fixed".
///
/// The QMux join hangs, rather than fails, at high concurrency on a multi-worker runtime over
/// a duplex: measured with the flow-control windows and the stream allowance raised out of the
/// way, concurrency 64 wedged on roughly three attempts in four at both two and four workers,
/// typically after about 55 of the 64 requests had completed. Concurrency 1 and 8 complete on
/// every runtime, a current-thread runtime completes at every point, and loopback TCP is clean
/// throughout — so the arms this group's sibling and the `transport_*` targets carry are not
/// affected.
///
/// Intermittence is what makes the omission necessary rather than merely tidy. A reliably
/// hanging arm would be caught the first time anyone ran the suite; one that hangs three times
/// in four is a CI job that occasionally never returns, and nothing in
/// `cargo bench -- --test` imposes a timeout to turn that into a failure.
///
/// The defect is recorded rather than fixed — fixing the join is outside this work's scope —
/// on `docs/qmux-h3/pending-work.md`, and the omission itself is accounted for in
/// `docs/benchmarks/README.md` alongside the two `shared_body` groups, whose lack of a QMux
/// arm has an entirely different cause. Before adding an arm here, read both.
fn concurrent_throughput_multi_thread(c: &mut Criterion) {
    let runtime = multi_thread_runtime(4);
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("concurrent_throughput_multi_thread");
    for n in CONCURRENCY {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("ngnet-h2", n), &n, |b, &n| {
            b.to_async(&runtime).iter(|| ngnet_h2.concurrent(n));
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
