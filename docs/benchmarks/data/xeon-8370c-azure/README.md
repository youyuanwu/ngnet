# `xeon-8370c-azure`

**Status: historical Azure-VM label; current captures use the migrated CPU described below.**
Runs have been collected in this directory since 2026-08-16.

**First run:** 2026-08-16 · **Last run:** 2026-08-30

## Hardware and system

| | |
| --- | --- |
| CPU | Intel Xeon Platinum 8370C @ 2.80 GHz (Ice Lake-SP) |
| Cores / threads | 4 cores, 8 threads, one socket, one NUMA node |
| Memory | 31 GiB |
| Virtualised | yes — Azure, Microsoft hypervisor |
| Kernel | `6.17.0-1015-azure` |
| Distribution | Ubuntu 24.04.4 LTS |
| io_uring | available — `/proc/sys/kernel/io_uring_disabled` is `0`, and both compio arms obtained `DriverType::IoUring` |
| Frequency scaling | `performance` governor, `intel_pstate` with `no_turbo=0`. Turbo is **not** disabled, and a guest cannot pin the frequency, so some drift is expected. |
| Rust toolchain | 1.97.1, from `rust-toolchain.toml` |

> **Host migration note, runs 12–21.** The VM now reports an Intel Xeon Platinum 8573C,
> kernel `6.17.0-1022-azure`, and Rust 1.98.0 rather than the 8370C/kernel/toolchain recorded
> above. The directory remains the historical VM label. These runs use within-run controls and
> never treat absolute differences across the migration as controlled hardware A/B evidence.
>
> **Second migration note, run 24.** The VM again reports an Intel Xeon Platinum 8370C,
> now with kernel `7.0.0-1012-azure` and Rust 1.98.0. Run 24 therefore uses its own fresh
> interleaved baseline; neither the original 8370C runs nor the intervening 8573C runs are
> treated as absolute controls.

**A shared virtual machine is not a benchmark rig.** Turbo cannot be disabled from inside the
guest, the host's other tenants are invisible from here, and hyper-threading means a "pinned"
core is half a physical one. Nothing on this host will produce trustworthy absolute figures;
what it can produce is paired deltas against drift controls measured in the same session, which
is what [`../../controls.md`](../../controls.md) requires anyway. Record the controls' movement
in every run.

## Measurement conditions

- **Pinning:** `taskset -c 3`, matching the legacy host's convention. Sibling thread 7 shares
  the physical core, so keep the machine otherwise idle.
- **Otherwise idle?** To be recorded per run. Runs 01–03 had the machine to themselves, and
  `04` records the same for its two passes. `05` and `06` do **not** state it, so it should not
  be assumed of them: their control movements — 0.73% mean for `05`, 1.06% mean and 4.47% worst
  for `06` — are the only evidence available about how quiet the host was, and they are the
  right thing to size those runs' deltas against rather than the 1% figure below.
- **Observed drift — the important number. About 1%.** Across 78 benchmarks run twice with no
  code change between, median |drift| was **0.90%**, mean 1.23%, and only one benchmark
  exceeded 5%. See [01-drift-baseline](01-drift-baseline.md) — that is the bar every result
  from this host is sized against, and it is roughly an order of magnitude tighter than the
  legacy host's.

  The one exception worth remembering: `shared_body/hyper-tokio/1048576` moved **+11.48%**
  between identical passes. A duplex 1 MiB hyper figure needs replication here.

## Arms this machine can run

All twelve bench targets. `cargo bench -p ngnet-bench -- --test` completes with `Success` for
every arm, including `compio-push` and `compio-shared`, which abort unless they obtain
`DriverType::IoUring` — so io_uring is genuinely available here and not silently falling back
to a polling driver.

`ngnet-bench` needs the `crates/ngnet-h2-sys/vendor/nghttp2`,
`crates/ngnet-h3-sys/vendor/nghttp3` (with its nested `lib/sfparse`),
`crates/ngnet-qmux-sys/vendor/dwnx`, and `crates/ngnet-quic-sys/vendor/ngtcp2` submodules.
The complete ngtcp2 HTTP/3 arm also requires OpenSSL 3.5 or newer. The pinned toolchain, a C
compiler, CMake, and libclang are common prerequisites.
[`../../running.md`](../../running.md) states the current requirement in full.

> **Historical prerequisite note.** Runs 01–03 predate both the QMux and QUIC-stack families
> and needed fewer submodules; runs through 24 did not reach ngtcp2/OpenSSL. The list above is
> intentionally the requirement for the current twelve-target suite, including the ngtcp2 arm,
> rather than the smaller environment in which those historical runs were captured.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| [01-drift-baseline](01-drift-baseline.md) | 2026-08-16 | `e75118e` | Two identical passes: what an unchanged arm does here — **~1%** |
| [02-first-survey](02-first-survey.md) | 2026-08-16 | `e75118e` | Where the arms stand, which legacy conclusions carried over, and where this stack beats hyper |
| [03-shared-body](03-shared-body.md) | 2026-08-16 | `e75118e` | Handing bodies over, five replicates — **settled the compio verdict** |
| [04-qmux-drift-baseline](04-qmux-drift-baseline.md) | 2026-08-17 | `524fa54` | Drift for the QMux arms, which `01` predates — socket **0.67%**, duplex **1.55%** |
| [05-qmux-delivery-aliasing](05-qmux-delivery-aliasing.md) | 2026-08-17 | `223960d` against `9f97334` | A two-orders-of-magnitude allocation cut that was **2.5–4.8% slower** — reverted |
| [06-qmux-write-path](06-qmux-write-path.md) | 2026-08-17 | `524fa54` against `a54ea43` | The write-path set end to end — **−30% at 1 MiB**, −8.5% at concurrency 64 on a socket |
| [07-qmux-per-commit-attribution](07-qmux-per-commit-attribution.md) | 2026-08-17 | seven commits, each against its predecessor | Coalescing is **−21.7%** at 1 MiB; four of the six are inside their step's controls — duplex only |
| [08-qmux-against-h2](08-qmux-against-h2.md) | 2026-08-18 | `c525aa1` | **QMux against HTTP/2**, five passes — fixed +19–34 µs per exchange, **0.86×** per byte over a socket |
| [09-qmux-h2-mechanisms](09-qmux-h2-mechanisms.md) | 2026-08-27 | `dc922be` | **Why**, by counting — 68 writes against 189 at 1 MiB, `2n + 2` against 2 at concurrency, and 45% of the fixed cost inside `ngnet-h3` |
| [10-h3-closed-stream-lookup](10-h3-closed-stream-lookup.md) | 2026-08-27 | `6d13712` against `419a774` | Constant-time closed-stream lookup — **−13–18% duplex, −7–13% socket** |
| [11-qmux-flush-decoupling](11-qmux-flush-decoupling.md) | 2026-08-28 | `736b460` against `b6c76d6` | QMux concurrent socket writes collapse from `2n + 2` to **~3**, improving n=64 by **24.6%** without a serial-latency blocker |
| [12-apply-events-reprofile](12-apply-events-reprofile.md) | 2026-08-28 | `700bfa6` | Fresh inclusive `apply_events` attribution is **0.74–1.05% serial, 1.86–1.89% at n=64**; run 09's 8.1% was flat/self, and scratch reuse was rejected before implementation |
| [13-qmux-h3-current-bottlenecks](13-qmux-h3-current-bottlenecks.md) | 2026-08-28 | `5477450` | Current bottlenecks — a duplicate event-path pump is **−5.4% duplex / −3.7% socket serial** in a diagnostic |
| [14-qmux-h3-one-pump](14-qmux-h3-one-pump.md) | 2026-08-28 | `c6f1191` against `5477450` | Production one-pump rule — **96 → 73 reads; −5.26% duplex / −2.37% socket serial** |
| [15-qmux-h3-shared-snapshot](15-qmux-h3-shared-snapshot.md) | 2026-08-28 | `0b6dcbd` against `c6f1191`, then `5477450` | Coherent shared work — **155 → 29 operations; −7.59% / −5.68% incremental serial** |
| [16-qmux-h3-baseline-and-pump-attribution](16-qmux-h3-baseline-and-pump-attribution.md) | 2026-08-28 | `364dbb2` | Fresh post-PR-45 baseline on the migrated 8573C; exact **70-pump** source split with zero residual |
| [17-qmux-h3-candidate-a-read-pump-amplification](17-qmux-h3-candidate-a-read-pump-amplification.md) | 2026-08-28 | `4e91115` against `43b7da0` | A2 removed **33/70 pumps**, but socket-serial timing did not clear spread/2% — reverted |
| [18-qmux-h3-candidate-b-delivery-ownership](18-qmux-h3-candidate-b-delivery-ownership.md) | 2026-08-28 | `96a20e6` against `0104e85` | B3 removed **160 allocations**, but improved duplex by <1.1% and was flat on socket — reverted |
| [19-qmux-h3-candidate-c-fixed-header-work](19-qmux-h3-candidate-c-fixed-header-work.md) | 2026-08-28 | `c188758` against `c7c95d9` | C1–C3 removed **20 mallocs and 3 reallocs**, but timing was inconsistent and failed both-substrate gates — reverted |
| [20-qmux-h3-candidate-d-event-queue](20-qmux-h3-candidate-d-event-queue.md) | 2026-08-28 | `6bff8ee` | Pops remain **23 = 23 fill iterations**, including 16 empty; queue-local options cannot reduce the registered count — closed |
| [21-qmux-h3-combined-final-matrix](21-qmux-h3-combined-final-matrix.md) | 2026-08-28 | `0984e1c` against `364dbb2` | Final binaries are hash-identical to the merged predecessor; fresh 16-case QMux/H3 ÷ H2 ratios range from **0.845× to 2.063×** |
| [22-qmux-h3-at-8-mib](22-qmux-h3-at-8-mib.md) | 2026-08-29 | `cae8330` | At 8 MiB, QMux/H3 is **1.182× over a duplex and 0.919× over a socket** |
| [23-ngnet-h3-quinn-vs-h3-quinn](23-ngnet-h3-quinn-vs-h3-quinn.md) | 2026-08-29 | `feature/ngnet-h3-quinn` based on `eeddf04` | Quinn-matched HTTP/3 comparison: **2.684× empty, 1.463× at 16 KiB, 1.129× at 1 MiB** |
| [24-ngnet-h3-quinn-lifecycle](24-ngnet-h3-quinn-lifecycle.md) | 2026-08-29 | `bd83f18` against `7da1866` | Bounded Quinn lifecycle cleanup — **−24.50% adjusted empty serial, 223,968 → 7,300 KiB aged RSS** |
| [25-ngtcp2-quic-stack-comparison](25-ngtcp2-quic-stack-comparison.md) | 2026-08-30 | working tree based on `7422956` | Historical pre-repair ngtcp2/OpenSSL context — **1.513× empty, 1.401× at 1 KiB; larger bodies were unstable** |
| [26-ngtcp2-phase1-diagnostics](26-ngtcp2-phase1-diagnostics.md) | 2026-08-30 | historical Phase 1 tree, reachable as `8515702` | Diagnostic foundation — **1 MiB failure reproduced; 16 KiB prepares 7.58–17.13× accepted bytes** |
| [27-ngtcp2-packet-bounded-staging](27-ngtcp2-packet-bounded-staging.md) | 2026-08-30 | unrecoverable working tree based on `e4815f7`; later represented by `9010a5b` | Packet-bounded borrowing repair — **historical 125/250/500 × 1 MiB exact with sampled RSS** |
| [28-ngtcp2-stream-first-gate](28-ngtcp2-stream-first-gate.md) | 2026-08-30 | `383082f` | Stream-first packet production — **not attributable with current diagnostics; deferred unchanged** |
| [29-ngtcp2-residual-eligibility](29-ngtcp2-residual-eligibility.md) | 2026-08-30 | `6a7af57` | Residual partition — **all six candidates deferred; no stable attributed gap beyond drift** |
| [30-ngtcp2-final-review-resolution](30-ngtcp2-final-review-resolution.md) | 2026-08-30 | `d8d9d90` through `8964a97` | Final-review evidence — **final calibration passes; release repetition remains flaky at 15 s and 60 s; 3 diagnostic timeouts preserved; RSS/stability gate unmet** |

Still outstanding, in the order they are worth doing:

1. **Force `TokioWriter::is_write_vectored` to `false` and re-run
   `transport_concurrent_throughput`.** This is the direct test of the mechanism the whole
   write-path finding rests on, and the one thing
   [02-first-survey](02-first-survey.md) could not settle from a standing survey.
2. ~~**A write count per megabyte for both stacks.**~~ **Done — it is
   [`09`](09-qmux-h2-mechanisms.md), and it answered more than it was asked.** QMux issues 68
   writes per megabyte-exchange where HTTP/2 issues 189, because HTTP/2 caps a write at one
   16 KiB frame and QMux empties a 64 KiB buffer. That is the mechanism behind the 61% kernel
   path [`08`](08-qmux-against-h2.md) could only compute. The same run explained the concurrency
   inversion as `2n + 2` writes against HTTP/2's constant two. [`11`](11-qmux-flush-decoupling.md)
   then decoupled internal event batches from actual task suspension and reduced the QMux count
   to approximately three at 1, 8, and 64 streams without removing the ordering boundary.
3. **Replicate the duplex 1 MiB arms**, the only place this host is noisy — and note that `04`
   sharpens this: `body_throughput/ngnet-qmux-h3/1048576` drifts **10.42%**, the worst
   identifier in the suite, so the QMux arm needs this more than the others do.
4. Everything else listed under *What a new machine should reproduce* in
   [`../../findings/`](../../findings/).
