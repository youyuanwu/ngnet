//! Serial latency: one request in flight at a time on a persistent connection.
//!
//! This is Criterion's home ground — it gives mean/median with confidence intervals and
//! outlier detection. The body is empty, so what is timed is the per-request headers round
//! trip and the wrapper work around it, not payload movement. The connection is stood up
//! once, outside the timed closure; each iteration issues one request on it and drains the
//! response.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, current_thread_runtime};

fn serial_latency(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("serial_latency");

    group.bench_function("ngnet-h2", |b| {
        b.to_async(&runtime)
            .iter(|| async { black_box(ngnet_h2.round_trip(Bytes::new()).await) });
    });

    group.bench_function("hyper", |b| {
        b.to_async(&runtime)
            .iter(|| async { black_box(hyper.round_trip(Bytes::new()).await) });
    });

    group.finish();
}

criterion_group!(benches, serial_latency);
criterion_main!(benches);
