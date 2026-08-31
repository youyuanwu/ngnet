# 27 — Does packet-bounded borrowing staging repair persistent ngtcp2 bodies?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; CPU 3
**Date:** 2026-08-30
**Source:** unrecoverable uncommitted working tree based on `e4815f7`; `9010a5b` is the
subsequent committed representation of the repair, not proof of measured-tree identity
**Purpose:** historical correctness/resource qualification and report-only origin context
**Exclusions:** no failed, slow, or signalled system-under-test attempt was excluded

**Superseded status:** [run 30](30-ngtcp2-final-review-resolution.md) reproduces later armed
and unarmed release-mode timeouts and withdraws current persistent-stability/RSS qualification.

## Repair

The borrowing stream-write path now samples the current maximum transmit UDP payload and
stages at most that prefix. It preserves source-slice order and the retained allocation's
address, suppresses FIN when any caller suffix was omitted, and reports only ngtcp2's accepted
prefix.

The first bounded large-body runs exposed two adjacent correctness conditions:

1. A fully acknowledged chunk queue removed its stream entry and reset the retention offset.
   Later data on the still-open stream was recorded from offset zero, so a cumulative
   acknowledgement could release a newly staged pointer before ngtcp2 finished with it.
   Retention now keeps the cumulative offset until stream close.
2. One endpoint poll could route up to eight receive batches before a detached connection
   owner ran. A 1 MiB diagnostic observed 73 unexpected inbound drops. The endpoint now yields
   after a receive batch and schedules its continuation; the 64-datagram queue and explicit
   overflow rule are unchanged. A transport-only packet with zero accepted stream bytes also
   ends the current transmit drain before the prefix is retried.

## Deterministic bounds

The feature-gated fixture retains a stricter 1024-byte test seam. Zero, prefix, slice-boundary,
multi-slice, complete, blocked, empty-FIN, delayed-FIN, stable-address, acknowledgement, and
release behavior are covered by retention, stream, diagnostic, allocation, and end-to-end
tests.

The historical live-loopback release scaling test reported:

| Run | Aggregate prepared backing |
| --- | ---: |
| Production limit, 64 KiB body | 354,907 B |
| Fixed 1024-byte limit, 64 KiB body | 322,691 B |
| Fixed 1024-byte limit, 128 KiB body | 652,388 B |

The fixed-limit doubling ratio was `652388 / 322691 = 2.021×`. This is retained as recorded
evidence only: the flaky live-loopback `2.1×` automated gate was removed in favor of
deterministic controlled staging coverage. Every recorded attempt was asserted to satisfy
`accepted <= staged <= min(offered, sampled_payload_limit)`. Each diagnostic batch also
asserts aggregate staged bytes are no greater than accepted bytes plus one effective payload
limit for each partial or zero-accept attempt. Accepted and HTTP/3 release-event bytes matched
exactly for both roles. Those fields are transport stream bytes, including HTTP/3
framing/control data; exact echoed application-body bytes were checked separately.

## Persistent exactness

Commands:

```sh
cargo test -p ngnet-bench --test ngtcp2_fixture \
  ngtcp2_fixture_repeats_1_mib_exactly -- --exact --nocapture
cargo test -p ngnet-bench --test ngtcp2_fixture --release \
  ngtcp2_fixture_repeats_1_mib_exactly -- --exact --nocapture
cargo test -p ngnet-bench --test ngtcp2_fixture --release \
  ngtcp2_fixture_repeats_16_kib_exactly -- --exact --nocapture
```

Two debug 125 × 1 MiB runs, three release 125 × 1 MiB runs, and three release
125 × 16 KiB runs completed byte-exactly after the complete repair. The activated ordinary
fixture covers both 125-exchange sizes in debug and release.

Intermediate investigation attempts were preserved as failures rather than discarded:

- payload-bounded staging alone still corrupted a 1 MiB response;
- disabling acknowledgement reclamation made it pass, localizing the premature release;
- before receive-batch yielding, the diagnostic recorded 73 unexpected inbound drops;
- before the final fairness/zero-progress combination, release exact runs stalled at
  exchanges 1, 6, and 94;
- the accepted checkout's repeated runs completed with status 0.

## Diagnostic and memory qualification

The release diagnostic binary was built with:

```sh
cargo build -p ngnet-bench --example probe --release --features diagnostics
```

Each fresh process used this shape, with `COUNT` set to 125, 250, or 500:

```sh
taskset -c 3 ./target/release/examples/probe \
  ngnet-quic-h3 body 1048576 COUNT diagnostic
```

The three 125-exchange runs and the single 250/500 runs all completed exactly, reported zero
inbound drops, no counter overflow, and accepted transport stream bytes equal to release-event
bytes. The historical zero-accept retry result is not accepted as liveness proof: local
transport-packet production and generic driver wakes were then misclassified as enabling
events, allowing the diagnostic to satisfy itself.

| Exchanges | Ready RSS | Maximum sampled post-boundary `VmRSS` | Final RSS | Maximum increase |
| ---: | ---: | ---: | ---: | ---: |
| 125 (1) | 13,852 KiB | 18,392 KiB | 17,364 KiB | 4,540 KiB |
| 125 (2) | 13,844 KiB | 22,460 KiB | 21,432 KiB | 8,616 KiB |
| 125 (3) | 13,900 KiB | 19,140 KiB | 18,112 KiB | 5,240 KiB |
| 250 | 13,880 KiB | 21,704 KiB | 20,676 KiB | 7,824 KiB |
| 500 | 13,772 KiB | 18,632 KiB | 17,604 KiB | 4,860 KiB |

The 125-run envelope is 8,616 KiB. Its tolerance is 2,048 KiB (larger than 5%), for a
10,664 KiB limit. The 250- and 500-exchange increases remain below it.

The first 125-run final snapshots reported client/server prepared backing of
136,632,712/138,899,725 bytes for 131,075,908/131,073,658 accepted bytes. This is
1.042×/1.060× accepted progress rather than Phase 1's 17.13×/7.58× three-exchange
complete-offer amplification.

## Historical origin characterization

The predecessor is a correctness-failing reference, not a 1 MiB throughput baseline. The
repaired checkout was therefore characterized as a new origin, interleaving three unarmed
target passes with three unchanged `h3-quinn` control passes:

```sh
cargo build -p ngnet-bench --example probe --release
taskset -c 3 ./target/release/examples/probe ARM body SIZE 125 timing
```

| Size | `ngnet-quic-h3` elapsed ns | `h3-quinn` control elapsed ns |
| --- | --- | --- |
| 16 KiB | 177,161,434; 157,359,464; 167,938,735 | 15,623,801; 13,666,918; 13,683,753 |
| 1 MiB | 5,833,162,457; 5,107,401,231; 3,877,749,160 | 775,944,379; 666,550,713; 635,099,036 |

Every target and control process completed 125 exchanges. Span was 11.79% target and 14.30%
control at 16 KiB, and 38.29% target and 21.13% control at 1 MiB. The target arm performed
byte-exact comparison while the control arm only drained/counts bytes, and the probe had not
passed its required 1 KiB Criterion calibration. These values are therefore historical,
non-comparable observations only; they do not establish a calibrated performance origin or a
directional performance claim.

## Verification and limitations

These commands passed:

```sh
cargo test -p ngnet-quic --all-features
cargo test -p ngnet-quic -p ngnet-quic-h3 -p ngnet-quic-h3-tests --release
cargo test -p ngnet-bench --test ngtcp2_fixture --release --features diagnostics
cargo test -p ngnet-quic-h3-tests --test zero_alloc --release --all-features
cargo test --workspace --all-features
cargo test --workspace
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo bench --workspace -- --test
RUSTDOCFLAGS="-D warnings" cargo doc \
  -p ngnet-quic -p ngnet-quic-h3 --all-features --no-deps
```

The broader `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`
remains host/repository-limited by pre-existing rustdoc warnings in generated
`ngnet-h2-sys` bindings and broken intra-doc links in the private `ngnet-h2-tests` crate; the
Phase 2 QUIC API documentation command above passes.

Matched repair/predecessor empty and 1 KiB measurements were not collected: the predecessor
branch was deleted after its intermediate PR closed, and switching this execution checkout
away from the uncommitted repair would violate the local workflow contract. Historical values
are not substituted for a same-session matched control.

Packet ordering was not changed. Diagnostic snapshots show recurring transport-only packets,
but current timing spread is too large and no transport-first-attributed target gap has been
declared. Phase 3 therefore remains gated.

The exact measured source tree cannot now be reconstructed. No dirty-tree diff, source
archive, measured binary hash, or raw log was retained. Git proves that `e4815f7` has tree
`ca5b4d189a8a6ad02021f743ebb8e06b2ce2e19a`, while the later Phase 2 commit `9010a5b` has
tree `358699c20f97565e15cb119cb1116c3d764a7524`; those trees differ across the repair code and
tests. `9010a5b` is the likely durable representation, but the stronger identity claim is not
recoverable and is not guessed.
