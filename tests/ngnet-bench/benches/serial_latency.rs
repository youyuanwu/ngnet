//! Serial latency: one request in flight at a time on a persistent connection.
//!
//! This is Criterion's home ground — it gives mean/median with confidence intervals and
//! outlier detection. The body is empty, so what is timed is the per-request headers round
//! trip and the wrapper work around it, not payload movement. The connection is stood up
//! once, outside the timed closure; each iteration issues one request on it and drains the
//! response.
//!
//! Three arms, to be read pairwise, and the two pairs answer different questions.
//! `ngnet-h2` against `ngnet-qmux-h3` varies only the protocol stack — HTTP/2 against
//! HTTP/3-over-QMux, same substrate, same runtime, same request and same drain — so it is the
//! cross-protocol comparison this suite exists to make. `ngnet-h2` against `hyper` varies only
//! the HTTP/2 implementation, and is the comparison that predates the HTTP/3 arm; it is
//! carried unchanged so measurements taken before the QMux arm existed stay comparable with
//! ones taken after. `ngnet-qmux-h3` against `hyper` varies both protocol and implementation
//! and so attributes to neither.
//!
//! The arms are registered `ngnet-h2`, `ngnet-qmux-h3`, `hyper`, and the order is deliberate:
//! Criterion emits measurements in registration order, so the two halves of the
//! cross-protocol comparison run back to back rather than with an unrelated arm timed between
//! them. `docs/benchmarks/controls.md` treats that adjacency as a methodological device rather
//! than a presentational one, which is why it is worth stating here.
//!
//! What the cross-protocol pair does *not* control is the layering: the QMux arm carries a
//! stream-multiplexing transport underneath its HTTP framing where the HTTP/2 arms carry only
//! framing over a byte stream. That extra layer is part of what is being compared and not a
//! flaw in the comparison — see `docs/benchmarks/configuration.md` for the settings the two
//! protocols hold in common, what each is set to, and which of them cannot be matched.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use ngnet_bench::{Hyper, NgnetH2, NgnetQmuxH3, current_thread_runtime};

fn serial_latency(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let ngnet_h2 = runtime.block_on(NgnetH2::establish());
    // Established on the same runtime as the other duplex arms, exactly as they are
    // established: the connection, both its drivers and the warm-up exchange are all standing
    // before Criterion times anything.
    let qmux_h3 = runtime.block_on(NgnetQmuxH3::establish());
    let hyper = runtime.block_on(Hyper::establish());

    let mut group = c.benchmark_group("serial_latency");

    group.bench_function("ngnet-h2", |b| {
        b.to_async(&runtime)
            .iter(|| async { black_box(ngnet_h2.round_trip(Bytes::new()).await) });
    });

    group.bench_function("ngnet-qmux-h3", |b| {
        b.to_async(&runtime)
            .iter(|| async { black_box(qmux_h3.round_trip(Bytes::new()).await) });
    });

    group.bench_function("hyper", |b| {
        b.to_async(&runtime)
            .iter(|| async { black_box(hyper.round_trip(Bytes::new()).await) });
    });

    group.finish();
}

criterion_group!(benches, serial_latency);
criterion_main!(benches);
