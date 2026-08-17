//! The shared-body comparison over a duplex: the same question as `transport_shared_body.rs`,
//! asked without a socket in the way.
//!
//! The duplex family exists to isolate protocol and body handling from the kernel. There is no
//! `writev`, no io_uring and no loopback here, so the only thing handing a body over can
//! remove is CPU: the memset of libnghttp2's frame buffer and the source-side copy into it.
//! That makes this family the mechanism check for the socket family's result — a gain that
//! appears there and not here needs a socket-level explanation, and a gain that appears here
//! and not there needs an explanation for why the socket swallowed it. Without one, it is
//! drift.
//!
//! Arms are paired and interleaved, and `hyper-tokio` is carried as an untouched drift
//! control, for the reasons set out at length in `transport_shared_body.rs`.
//!
//! This group carries **no HTTP/3-over-QMux arm**, and for a different reason than
//! `concurrent_throughput_multi_thread` does: there is nothing to compare. What is measured
//! here is an HTTP/2 body-handover entry point against its copying twin, and no counterpart
//! mechanism exists on the QMux stack, so a QMux arm would be a third quantity beside a
//! two-sided comparison rather than a half of one. Note that this makes the group
//! single-*protocol* and not single-stack — it does carry a `hyper-tokio` arm.
//! `docs/benchmarks/README.md` records the two distinct reasons a group may lack a QMux arm.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, NgnetH2Shared, body_of, current_thread_runtime};

/// The same sweep as the socket family, so the two are comparable in shape. 0 B is the
/// mechanistic control: with no body there is nothing for the shared path to avoid copying.
const SIZES: [usize; 4] = [0, 1024, 64 * 1024, 1024 * 1024];

fn shared_body(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let push = runtime.block_on(NgnetH2::establish());
    let shared = runtime.block_on(NgnetH2Shared::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("shared_body");
    for size in SIZES {
        if size == 0 {
            group.throughput(Throughput::Elements(1));
        } else {
            group.throughput(Throughput::Bytes(size as u64));
        }
        let payload = body_of(size);

        group.bench_with_input(BenchmarkId::new("ngnet-h2-push", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(push.round_trip(payload.clone()).await) });
        });

        group.bench_with_input(BenchmarkId::new("ngnet-h2-shared", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(shared.round_trip(payload.clone()).await) });
        });

        // Untouched by this work: its movement is the session's noise floor.
        group.bench_with_input(BenchmarkId::new("hyper-tokio", size), &size, |b, _| {
            let payload = payload.clone();
            b.to_async(&runtime)
                .iter(|| async { black_box(hyper.round_trip(payload.clone()).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, shared_body);
criterion_main!(benches);
