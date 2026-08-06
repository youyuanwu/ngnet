//! The benchmark fixtures must move the bytes they claim to move.
//!
//! A benchmark arm that quietly transfers less than its twin is faster for the wrong reason,
//! and the shared-body arms were fast enough on the readiness transport to make that worth
//! ruling out rather than assuming. Each fixture here runs the same round trip the benchmark
//! times and asserts the echoed body came back whole, at every size in the sweep.
//!
//! This is a correctness check on the *harness*, not on the crate — the crate's own body
//! integrity is pinned by `ngnet-h2`'s test suite. What it rules out is a measurement artefact.

use bytes::Bytes;

use ngnet_h2_bench::{
    CompioSharedSocket, CompioSocket, Hyper, NgnetH2, NgnetH2Shared, TokioSharedSocket, TokioSocket,
    body_of, compio_runtime, current_thread_runtime,
};

/// The benchmark sweep, plus a size that is not a multiple of the 16 KiB frame payload so a
/// final short frame is exercised too.
const SIZES: [usize; 5] = [0, 1024, 64 * 1024, 1024 * 1024, 100_003];

/// Asserts one fixture echoes every size in the sweep back at its exact length.
macro_rules! echoes_whole {
    ($name:ident, $fixture:ty, $runtime:expr) => {
        #[test]
        fn $name() {
            let runtime = $runtime;
            let fixture = runtime.block_on(<$fixture>::establish());
            for size in SIZES {
                let echoed = runtime.block_on(fixture.round_trip(body_of(size)));
                assert_eq!(
                    echoed,
                    size,
                    "{} echoed {} bytes for a {}-byte body: an arm that moves fewer bytes than \
                     its twin is faster for a reason that is not the one under test",
                    stringify!($fixture),
                    echoed,
                    size
                );
            }
        }
    };
}

echoes_whole!(ngnet_h2_push_echoes_whole, NgnetH2, current_thread_runtime());
echoes_whole!(
    ngnet_h2_shared_echoes_whole,
    NgnetH2Shared,
    current_thread_runtime()
);
echoes_whole!(hyper_echoes_whole, Hyper, current_thread_runtime());
echoes_whole!(
    tokio_push_echoes_whole,
    TokioSocket,
    current_thread_runtime()
);
echoes_whole!(
    tokio_shared_echoes_whole,
    TokioSharedSocket,
    current_thread_runtime()
);
echoes_whole!(compio_push_echoes_whole, CompioSocket, compio_runtime());
echoes_whole!(
    compio_shared_echoes_whole,
    CompioSharedSocket,
    compio_runtime()
);

/// The bodies really are distinct objects, so `body_of` is not handing every arm the same
/// shared allocation and letting one arm's work be attributed to another.
#[test]
fn the_sweep_builds_independent_bodies() {
    let first = body_of(1024);
    let second = body_of(1024);
    assert_eq!(first, second, "the same contents");
    assert_ne!(
        first.as_ptr(),
        second.as_ptr(),
        "but distinct allocations, so no arm can be measuring a body another arm warmed"
    );
    assert_eq!(body_of(0), Bytes::new(), "and the control point is empty");
}
