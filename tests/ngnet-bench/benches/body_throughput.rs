//! Body throughput: a request/response body sweep on a persistent connection, with
//! `Throughput::Bytes` so Criterion reports MB/s. The server echoes the body, so each
//! iteration moves `size` bytes up and `size` back; throughput is normalised to one body's
//! worth, which is the number reported.
//!
//! The sweep is where flow control and the read-buffer pool start to matter: at 1 MiB and
//! especially 8 MiB, the 64 KiB initial window (matched between the two stacks) forces repeated
//! `WINDOW_UPDATE` round trips, so this is as much a flow-control benchmark as a copy benchmark.
//!
//! Three arms, to be read pairwise. `ngnet-h2` against `ngnet-qmux-h3` varies only the
//! protocol stack and is the cross-protocol comparison; `ngnet-h2` against `hyper` varies only
//! the HTTP/2 implementation and is carried unchanged from before the HTTP/3 arm existed;
//! `ngnet-qmux-h3` against `hyper` varies both and attributes to neither. The arms are
//! registered `ngnet-h2`, `ngnet-qmux-h3`, `hyper` inside the size loop, so at each size the
//! two halves of the cross-protocol comparison are emitted back to back — size is the outer
//! loop and the arms are the inner one, which is the arrangement
//! `docs/benchmarks/controls.md` fixes as a control rather than a presentational choice.
//!
//! The flow-control sentence above is the one that most needs reading twice on the QMux arm.
//! Both stacks are given 65535 bytes of credit per stream and 65535 at the connection level,
//! so the 1 MiB and 8 MiB points cost each of them repeated credit extensions rather than one
//! uninterrupted copy — but the two are not extending the same quantity. HTTP/2's control
//! frames do not consume connection credit where QMux's unidirectional streams do, so the
//! connection-level figures are equal in number and not quite in meaning.
//! `docs/benchmarks/configuration.md` accounts for that and for the settings neither stack
//! exposes.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, NgnetQmuxH3, body_of, current_thread_runtime};

/// 0 B exercises the headers-only path; the rest climb until the initial window and the
/// buffer pool dominate.
const SIZES: [usize; 5] = [0, 1024, 64 * 1024, 1024 * 1024, 8 * 1024 * 1024];

fn body_throughput(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    // Established on the same runtime as the other duplex arms, and warmed there too, so no
    // size's first iteration pays for a handshake.
    let qmux_h3 = runtime.block_on(NgnetQmuxH3::establish());
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

        group.bench_with_input(BenchmarkId::new("ngnet-qmux-h3", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(qmux_h3.round_trip(payload.clone()).await) });
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
