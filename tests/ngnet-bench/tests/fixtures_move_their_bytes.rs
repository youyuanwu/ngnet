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

use ngnet_bench::{
    CompioSharedSocket, CompioSocket, Hyper, NgnetH2, NgnetH2Shared, NgnetQmuxH3,
    NgnetQmuxH3Matched, NgnetQmuxH3MatchedSocket, NgnetQmuxH3Socket, TokioSharedSocket,
    TokioSocket, UpstreamH3Qmux, UpstreamH3QmuxSocket, body_of, compio_runtime,
    current_thread_runtime,
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

echoes_whole!(
    ngnet_h2_push_echoes_whole,
    NgnetH2,
    current_thread_runtime()
);
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
// The cross-protocol arms, held to exactly the same standard and for a sharper reason: a
// QMux arm that moved fewer bytes than its HTTP/2 counterpart would not merely be fast for
// the wrong reason, it would be evidence for a cross-protocol claim that was never measured.
// The 1 MiB point also happens to be where the matched 65535-byte credit is exercised hardest
// — it takes sixteen window-sized instalments — so an arm that echoes it whole is an arm
// whose flow control is being extended rather than one that got lucky with a large window.
echoes_whole!(
    qmux_h3_duplex_echoes_whole,
    NgnetQmuxH3,
    current_thread_runtime()
);
echoes_whole!(
    qmux_h3_socket_echoes_whole,
    NgnetQmuxH3Socket,
    current_thread_runtime()
);
echoes_whole!(
    matched_ngnet_qmux_h3_duplex_echoes_whole,
    NgnetQmuxH3Matched,
    current_thread_runtime()
);
echoes_whole!(
    matched_ngnet_qmux_h3_socket_echoes_whole,
    NgnetQmuxH3MatchedSocket,
    current_thread_runtime()
);
echoes_whole!(
    upstream_h3_qmux_duplex_echoes_whole,
    UpstreamH3Qmux,
    current_thread_runtime()
);
echoes_whole!(
    upstream_h3_qmux_socket_echoes_whole,
    UpstreamH3QmuxSocket,
    current_thread_runtime()
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

#[test]
fn matched_qmux_fixtures_use_symmetric_per_instance_counters() {
    let runtime = current_thread_runtime();
    let body = body_of(100_003);

    let ngnet = runtime.block_on(NgnetQmuxH3Matched::establish());
    ngnet.arm_counters();
    assert_eq!(runtime.block_on(ngnet.round_trip(body.clone())), body.len());
    let ngnet_memory = ngnet.counter_snapshot();

    let upstream = runtime.block_on(UpstreamH3Qmux::establish());
    upstream.arm_counters();
    assert_eq!(
        runtime.block_on(upstream.round_trip(body.clone())),
        body.len()
    );
    let upstream_memory = upstream.counter_snapshot();

    let ngnet_socket = runtime.block_on(NgnetQmuxH3MatchedSocket::establish());
    ngnet_socket.arm_counters();
    assert_eq!(
        runtime.block_on(ngnet_socket.round_trip(body.clone())),
        body.len()
    );
    let ngnet_socket = ngnet_socket.counter_snapshot();

    let upstream_socket = runtime.block_on(UpstreamH3QmuxSocket::establish());
    upstream_socket.arm_counters();
    assert_eq!(runtime.block_on(upstream_socket.round_trip(body)), 100_003);
    let upstream_socket = upstream_socket.counter_snapshot();

    for snapshot in [ngnet_memory, upstream_memory, ngnet_socket, upstream_socket] {
        assert!(snapshot.lower_read_calls > 0);
        assert!(snapshot.lower_write_calls > 0);
        assert!(snapshot.lower_read_bytes >= 200_006);
        assert!(snapshot.lower_write_bytes >= snapshot.lower_read_bytes);
        assert!(snapshot.endpoint_polls > 0);
        assert!(!snapshot.overflowed);
    }
}
