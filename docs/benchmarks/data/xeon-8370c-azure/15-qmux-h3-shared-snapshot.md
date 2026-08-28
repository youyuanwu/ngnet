# 15 — Does one coherent shared-work snapshot beat repeated locking?

**Machine:** historical [`xeon-8370c-azure`](README.md) VM label; the current VM reports an **Intel Xeon Platinum 8573C**
**Date:** 2026-08-28
**Commit(s):** retained phase 1 `c6f119107d5bb6058015cb60530ae61c3d2639e0` against final `0b6dcbd64bc6085e4dc11b732739627563041379`; fresh final comparison against base `54774504ca968140f3c25ce9b3bfc84d06a2bb6e`
**Cases:** exact empty duplex shared-operation counts; duplex and loopback-socket serial; concurrency 1, 8 and 64; 1 MiB duplex/socket regression guards; matching HTTP/2 controls
**Commands:** exact detached revisions were built with `cargo build --release -p ngnet-bench --benches --example probe`; copied executables ran as `taskset -c 3 <binary> --bench ngnet --save-baseline <pass> --noplot`. Count-only debug executables from both exact revisions retained uninlined method symbols and ran under elevated `perf` uprobes as `taskset -c 3 <probe-count> qmux-duplex body 0 <N>`.
**Repetitions:** Criterion phase 1 → final → phase 1 → final, then fresh base → final → base → final; 100 samples per benchmark per pass. Counts used `N=100` and `3N=300`, reported as `(count(300) - count(100)) / 200`.
**Controls:** unchanged matching HTTP/2 implementations were present in every binary and pass; movements are reported per row
**Exclusions:** none. No sample, benchmark, pass, or count was discarded. The 1 MiB cases were pre-registered as regression guards, not improvement targets.

The VM reports an Intel Xeon Platinum 8573C under the historical 8370C directory name.
Only interleaved comparisons within this run are controlled evidence; absolute timings are not
compared with runs from before the host migration.

As in run 14, exact counts use run 13's empty-serial `body 0` workload rather than the plan's
mistaken `concurrent 1` spelling. The phase-one recount reproduces the established empty-exchange
unit. Positive claims below use a conservative 2% reporting floor in addition to clearing the
absolute matching-control movement and both within-side spreads; the retention decision itself
uses the pre-registered serial and concurrency-1 targets.

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

| Operation class | phase 1 raw N / 3N | final raw N / 3N | phase 1 per exchange | final per exchange |
| --- | ---: | ---: | ---: | ---: |
| destructive drains | five × 1,428 / 4,228 | 1,428 / 4,228 | 70 | **14** |
| idle/readiness predicates | five × 1,528 / 4,528 | 510 / 1,510 | 75 | **5** |
| normal-completion predicate | none (new class) | 0 / 0 | — | **0** |
| waker refresh/recheck | 1,018 / 3,018 | 1,018 / 3,018 | 10 | **10** |
| **all counted operations** | **15,798 / 46,798** | **2,956 / 8,756** | **155** | **29** |

The snapshot removes 126 entries per exchange, an 81.3% reduction. Every productive driver pass
still performs one drain (14 per exchange); only five eligible idle decisions need the coherent
idle predicate; the ten park attempts each use one combined waker refresh and readiness recheck.
This workload ends through the peer-gone path, so it creates no normal-completion candidate.
The completion probe has no phase-one predecessor; its cost and extra-pass behavior are covered
by the driven `shared_snapshot.rs` tests rather than this peer-gone count workload. All five
legacy drain symbols and all five legacy readiness symbols recorded their respective collapsed
raw pairs above: every counted readiness decision found all categories empty, and no park took
the blocked-stream short-circuit.

## Incremental phase 1 → snapshot timing

Median microseconds; lower is better. Each change compares the arithmetic mean of two medians
per side. Spread is `(max / min) - 1` within a side.

| Benchmark | phase 1 1/2 | snapshot 1/2 | change | phase 1 / snapshot spread | H2 control |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 22.665 / 22.769 | 21.074 / 20.913 | **−7.59%** | 0.46% / 0.77% | −0.55% |
| socket serial | 33.663 / 33.976 | 31.973 / 31.821 | **−5.68%** | 0.93% / 0.48% | −0.60% |
| duplex concurrency 1 | 23.201 / 23.782 | 21.576 / 21.716 | **−7.86%** | 2.51% / 0.65% | +0.42% |
| duplex concurrency 8 | 125.039 / 125.398 | 122.409 / 122.344 | **−2.27%** | 0.29% / 0.05% | −0.95% |
| duplex concurrency 64 | 1002.527 / 1008.878 | 989.488 / 993.814 | −1.40% | 0.63% / 0.44% | −1.03% |
| socket concurrency 1 | 34.499 / 35.094 | 32.570 / 32.647 | **−6.29%** | 1.73% / 0.24% | −0.81% |
| socket concurrency 8 | 136.600 / 136.642 | 131.707 / 132.429 | **−3.33%** | 0.03% / 0.55% | −1.11% |
| socket concurrency 64 | 1006.176 / 1014.665 | 988.335 / 988.660 | **−2.17%** | 0.84% / 0.03% | −0.65% |
| duplex 1 MiB guard | 639.451 / 627.840 | 599.416 / 602.803 | −5.13% | 1.85% / 0.57% | −0.64% |
| socket 1 MiB guard | 1086.744 / 1080.890 | 1054.700 / 1050.382 | −2.89% | 0.54% / 0.41% | +1.42% |

Both serial targets and both concurrency-1 targets clear matching controls and both side spreads,
so the pre-registered phase-two positive gate passes. Duplex concurrency 8 and socket concurrency
8/64 also clear those bars and the 2% reporting floor. Duplex concurrency 64 clears control and
spread but remains unclaimed because its 1.40% point estimate is below that floor.
The bulk arms show no regression but remain guards rather than optimization claims.

## Fresh base → retained-final timing

| Benchmark | base 1/2 | final 1/2 | change | base / final spread | H2 control |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 23.741 / 23.650 | 20.963 / 20.860 | **−11.75%** | 0.38% / 0.49% | +0.65% |
| socket serial | 35.351 / 34.410 | 31.920 / 31.848 | **−8.59%** | 2.73% / 0.23% | −1.59% |
| duplex concurrency 1 | 24.755 / 24.701 | 21.867 / 21.797 | **−11.71%** | 0.22% / 0.32% | +0.00% |
| duplex concurrency 8 | 129.141 / 129.032 | 121.965 / 122.556 | **−5.29%** | 0.08% / 0.48% | −0.07% |
| duplex concurrency 64 | 1035.326 / 1031.154 | 987.303 / 995.782 | **−4.04%** | 0.40% / 0.86% | +0.48% |
| socket concurrency 1 | 35.475 / 35.298 | 32.599 / 32.452 | **−8.09%** | 0.50% / 0.46% | +0.02% |
| socket concurrency 8 | 138.056 / 141.183 | 133.008 / 132.236 | **−5.01%** | 2.26% / 0.58% | −0.41% |
| socket concurrency 64 | 1030.318 / 1029.242 | 989.423 / 991.718 | **−3.81%** | 0.10% / 0.23% | −0.44% |
| duplex 1 MiB guard | 650.296 / 642.948 | 597.839 / 598.908 | −7.46% | 1.14% / 0.18% | −0.26% |
| socket 1 MiB guard | 1104.832 / 1093.831 | 1049.412 / 1047.984 | −4.61% | 1.01% / 0.14% | −0.45% |

The retained final is faster on every target, and every serial/concurrency row clears controls
and both side spreads. Both bulk arms remain regression guards; the duplex guard does not clear
the pre-registered 10.42% historical within-host drift for that identifier.

## Exact-revision binaries

| Binary | base | phase 1 | snapshot/final |
| --- | --- | --- | --- |
| `probe` | `53ee9ed95756034043982a268ab80e42c700dce102a47f431b3e3b396a63296f` | `2cf914d6d8d3a9ed6d4ae491ae42cf72ba54646d9d72cf22d9d82d3f0556da51` | `0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` |
| count-only `probe` | — | `e746f29b97d5b2d9192698ba0f94c3b10f822793cca84198ffc042de9ecd0e38` | `978d63ee7ebec03a790ca6cf72b7ecb0a4d75021258d57419d0bae84612c16d6` |
| `serial_latency` | `8fceeea596c3b26062a59d32e5a2204ee0f3c835aeadb374819aa1a5d3fcbd11` | `c2cce20ec4ab0a73550ef849ccc3b9eb3e2d0d6e0d57a99d3e7b8efa3b5ad3c0` | `294ec76d110f77eeb86b2c222fa27cc22e6462df6cd6e4077bf55231f368776a` |
| `transport_serial_latency` | `0f332aec4984f2fbb3fefc045ae5c3cc946b32332dded94820ac3c8e30158f2a` | `573a443ae8d4eb097c73543cc23a686653bd0580afec4373440ddd6d9847130e` | `1a704657a524a6677a960c36f7d1ba98df66fdced24376ee1481009de9e66c82` |

## Verdict

The prototype passes both required gates: counted shared operations fall 155 → 29 and the
incremental predicted targets improve 5.68–7.86% beyond controls and spread. Phase 2 is retained.
Together with phase 1, the fresh base-to-final comparison is 11.75% faster duplex serial and
8.59% faster socket serial, with no target regression.

## What this does not establish

- Unclaimed rows are not evidence of improvement merely because their point estimate is negative.
- Bulk guards are not a bulk-throughput optimization claim.
- The 155 → 29 count is from matched count-only builds, not the release timing executables.
- This run does not compare absolute latency with a pre-migration run, profile a real network, or
  validate the six local OpenSSL-dependent QUIC members.
