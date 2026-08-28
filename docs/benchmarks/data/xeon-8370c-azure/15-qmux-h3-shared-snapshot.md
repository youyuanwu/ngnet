# 15 — Does one coherent shared-work snapshot beat repeated locking?

**Machine:** historical [`xeon-8370c-azure`](README.md) VM label; the current VM reports an **Intel Xeon Platinum 8573C**
**Date:** 2026-08-28
**Commit(s):** retained phase 1 `c6f119107d5bb6058015cb60530ae61c3d2639e0` against snapshot prototype/final `c171f4f77a4ae787ac828582a3e0209ed75b4a30`; fresh final comparison against base `54774504ca968140f3c25ce9b3bfc84d06a2bb6e`
**Cases:** exact empty duplex shared-operation counts; duplex and loopback-socket serial; concurrency 1, 8 and 64; 1 MiB duplex/socket regression guards; matching HTTP/2 controls
**Commands:** exact detached revisions were built with `cargo build --release -p ngnet-bench --benches --example probe`; copied executables ran as `taskset -c 3 <binary> --bench ngnet --save-baseline <pass> --noplot`. Count-only debug executables from both exact revisions retained uninlined method symbols and ran under elevated `perf` uprobes as `taskset -c 3 <probe-count> qmux-duplex body 0 <N>`.
**Repetitions:** Criterion phase 1 → prototype → phase 1 → prototype, then fresh base → final → base → final; 100 samples per benchmark per pass. Counts used `N=100` and `3N=300`, reported as `(count(300) - count(100)) / 200`.
**Controls:** unchanged matching HTTP/2 implementations were present in every binary and pass; movements are reported per row
**Exclusions:** none. No sample, benchmark, pass, or count was discarded. The 1 MiB cases were pre-registered as regression guards, not improvement targets.

The VM reports an Intel Xeon Platinum 8573C under the historical 8370C directory name.
Only interleaved comparisons within this run are controlled evidence; absolute timings are not
compared with runs from before the host migration.

## What was being asked

Run 13 counted 155 entries into the HTTP/3 `Shared` drain, readiness, and waker-refresh methods
per empty QMux/H3 exchange. The fields already share one mutex but the driver drained five
categories separately and queried each category separately around idle decisions. This run asks
whether transferring all five categories under one lock and making coherent idle, completion,
and under-waker probes both reduces those operations and improves controlled time.

The pre-registered plan called the old manifest “ten legacy take/readiness entries,” but the
historical total of 155 also includes `refresh_driver`: five takes, five readiness methods, and
one refresh method, or eleven entries. This run counts all eleven rather than silently omitting
the refresh from one side while counting its combined replacement on the other.

## Exact shared-operation counts

The release build inlines these small methods. Count-only binaries therefore used the plan's
same-revision debug-symbol fallback on both sides; Criterion continued to use untouched release
binaries. Raw totals include setup and teardown.

| Operation class | phase 1 raw N / 3N | prototype raw N / 3N | phase 1 per exchange | prototype per exchange |
| --- | ---: | ---: | ---: | ---: |
| destructive drains | five × 1,428 / 4,228 | 1,428 / 4,228 | 70 | **14** |
| idle/readiness predicates | five × 1,528 / 4,528 | 510 / 1,510 | 75 | **5** |
| normal-completion predicate | included above | 0 / 0 | — | **0** |
| waker refresh/recheck | 1,018 / 3,018 | 1,018 / 3,018 | 10 | **10** |
| **all counted operations** | **15,798 / 46,798** | **2,956 / 8,756** | **155** | **29** |

The snapshot removes 126 entries per exchange, an 81.3% reduction. Every productive driver pass
still performs one drain (14 per exchange); only five eligible idle decisions need the coherent
idle predicate; the ten park attempts each use one combined waker refresh and readiness recheck.
This workload ends through the peer-gone path, so it creates no normal-completion candidate.

## Incremental phase 1 → snapshot timing

Median microseconds; lower is better. Each change compares the arithmetic mean of two medians
per side. Spread is `(max / min) - 1` within a side.

| Benchmark | phase 1 1/2 | snapshot 1/2 | change | phase 1 / snapshot spread | H2 control |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 22.759 / 22.625 | 20.963 / 20.953 | **−7.64%** | 0.59% / 0.05% | −0.24% |
| socket serial | 33.871 / 34.046 | 31.966 / 31.711 | **−6.24%** | 0.52% / 0.80% | −0.49% |
| duplex concurrency 1 | 23.197 / 23.158 | 21.944 / 21.644 | **−5.97%** | 0.17% / 1.38% | +0.77% |
| duplex concurrency 8 | 125.743 / 124.794 | 124.175 / 121.822 | −1.81% | 0.76% / 1.93% | +0.59% |
| duplex concurrency 64 | 1008.080 / 999.117 | 1007.301 / 984.658 | −0.76% | 0.90% / 2.30% | +0.40% |
| socket concurrency 1 | 34.586 / 35.104 | 32.700 / 32.743 | **−6.09%** | 1.50% / 0.13% | +0.10% |
| socket concurrency 8 | 136.539 / 137.642 | 133.137 / 132.379 | **−3.16%** | 0.81% / 0.57% | −1.06% |
| socket concurrency 64 | 1016.295 / 1025.490 | 993.798 / 989.387 | **−2.87%** | 0.90% / 0.45% | −1.09% |
| duplex 1 MiB guard | 641.433 / 641.638 | 600.647 / 596.856 | −6.67% | 0.03% / 0.64% | +0.60% |
| socket 1 MiB guard | 1089.646 / 1084.635 | 1053.916 / 1051.753 | −3.16% | 0.46% / 0.21% | −1.94% |

Both serial targets and both concurrency-1 targets clear matching controls and both side spreads,
so the pre-registered phase-two positive gate passes. Socket concurrency 8 and 64 also clear
those bars. Duplex concurrency 8 and 64 do not clear snapshot-side spread and remain unclaimed.
The bulk arms show no regression but remain guards rather than optimization claims.

## Fresh base → retained-final timing

| Benchmark | base 1/2 | final 1/2 | change | base / final spread | H2 control |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 23.731 / 23.677 | 20.875 / 20.868 | **−11.95%** | 0.23% / 0.04% | +1.16% |
| socket serial | 34.870 / 34.474 | 31.972 / 31.821 | **−8.00%** | 1.15% / 0.47% | −0.78% |
| duplex concurrency 1 | 24.452 / 24.264 | 21.677 / 21.672 | **−11.01%** | 0.77% / 0.02% | +1.23% |
| duplex concurrency 8 | 128.829 / 129.541 | 121.864 / 123.025 | **−5.22%** | 0.55% / 0.95% | −0.16% |
| duplex concurrency 64 | 1030.822 / 1031.671 | 992.406 / 996.493 | **−3.57%** | 0.08% / 0.41% | +0.74% |
| socket concurrency 1 | 35.710 / 35.472 | 32.928 / 32.812 | **−7.65%** | 0.67% / 0.35% | −0.23% |
| socket concurrency 8 | 138.462 / 139.296 | 133.197 / 132.959 | **−4.18%** | 0.60% / 0.18% | +0.42% |
| socket concurrency 64 | 1019.885 / 1027.763 | 997.249 / 1015.024 | −1.73% | 0.77% / 1.78% | −0.51% |
| duplex 1 MiB guard | 661.435 / 644.691 | 615.811 / 607.712 | −6.32% | 2.60% / 1.33% | +2.78% |
| socket 1 MiB guard | 1107.142 / 1103.656 | 1062.722 / 1081.329 | −3.02% | 0.32% / 1.75% | +0.53% |

The retained final is faster on every target. Socket concurrency 64 does not clear the 2% floor
or final-side spread and is unclaimed. Both bulk arms remain regression guards; the duplex guard
does not clear the pre-registered 10.42% historical within-host drift for that identifier.

## Exact-revision binaries

| Binary | base | phase 1 | snapshot/final |
| --- | --- | --- | --- |
| `probe` | `53ee9ed95756034043982a268ab80e42c700dce102a47f431b3e3b396a63296f` | `2cf914d6d8d3a9ed6d4ae491ae42cf72ba54646d9d72cf22d9d82d3f0556da51` | `03ec82e18678f96051b2fefdffe78642928209940143eb503735315a6f5f4503` |
| count-only `probe` | — | `e746f29b97d5b2d9192698ba0f94c3b10f822793cca84198ffc042de9ecd0e38` | `cb012b51e107f9f0a5c62d85fcb02588347bdda2264d133288a486e8cf06e04e` |
| `serial_latency` | `8fceeea596c3b26062a59d32e5a2204ee0f3c835aeadb374819aa1a5d3fcbd11` | `c2cce20ec4ab0a73550ef849ccc3b9eb3e2d0d6e0d57a99d3e7b8efa3b5ad3c0` | `5610172fd888bd6e3952e0395b1751560606a68a8bf1e8b6a8c8742ccff847ad` |
| `transport_serial_latency` | `0f332aec4984f2fbb3fefc045ae5c3cc946b32332dded94820ac3c8e30158f2a` | `573a443ae8d4eb097c73543cc23a686653bd0580afec4373440ddd6d9847130e` | `cf6c517f145eaf529ea0da804327c84eee1fd036ab7da8da4d5396b7d149b163` |

## Verdict

The prototype passes both required gates: counted shared operations fall 155 → 29 and the
incremental predicted targets improve 5.97–7.64% beyond controls and spread. Phase 2 is retained.
Together with phase 1, the fresh base-to-final comparison is 11.95% faster duplex serial and
8.00% faster socket serial, with no target regression.

## What this does not establish

- Unclaimed rows are not evidence of improvement merely because their point estimate is negative.
- Bulk guards are not a bulk-throughput optimization claim.
- The 155 → 29 count is from matched count-only builds, not the release timing executables.
- This run does not compare absolute latency with a pre-migration run, profile a real network, or
  validate the six local OpenSSL-dependent QUIC members.
