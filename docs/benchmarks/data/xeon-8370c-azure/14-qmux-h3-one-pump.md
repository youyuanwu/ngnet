# 14 — Does the production one-pump rule reproduce the diagnostic?

**Machine:** historical [`xeon-8370c-azure`](README.md) VM label; the current VM reports an **Intel Xeon Platinum 8573C**
**Date:** 2026-08-28
**Commit(s):** base `54774504ca968140f3c25ce9b3bfc84d06a2bb6e` against phase 1 `c6f119107d5bb6058015cb60530ae61c3d2639e0`
**Cases:** exact empty duplex exchange counts; duplex and loopback-socket serial; concurrency 1, 8 and 64; 1 MiB duplex/socket regression guards; matching HTTP/2 controls
**Commands:** separate detached checkouts were built with `cargo build --release -p ngnet-bench --benches --example probe`; each copied executable was run as `taskset -c 3 <binary> --bench ngnet --save-baseline <pass> --noplot`. Exact counts used elevated `perf` uprobes around `taskset -c 3 <probe> qmux-duplex body 0 <N>`.
**Repetitions:** Criterion base → phase 1 → base → phase 1; 100 samples per Criterion benchmark per pass. Counts used `N=100` and `3N=300`, reported as `(count(300) - count(100)) / 200`.
**Controls:** the unchanged matching HTTP/2 implementation was present in every binary and pass; movements are reported per row below
**Exclusions:** none. No sample, benchmark, pass, or count was discarded. The 1 MiB cases were pre-registered as regression guards, not improvement targets.

The VM now reports an Intel Xeon Platinum 8573C under the historical 8370C directory name.
Only the interleaved comparisons within this run are controlled evidence. Absolute timings are
not compared with pre-migration runs.

## What was being asked

Run 13 isolated one duplicate: when no release or translated event was queued,
`QmuxConnection::poll_event` pumped explicitly and then called the lower buffered event poll,
which pumped again. Its one-line diagnostic reduced transport reads from 96 to 73 and improved
small exchanges. This run asks whether the production implementation realizes that exact count
change, preserves the existing event-pass count, and clears the pre-registered timing gates with
independently built exact-revision binaries.

## Exact empty-exchange counts

Raw uprobe totals include connection setup and teardown; the reported per-exchange column removes
that constant with the two-point formula above.

| Operation | base raw N / 3N | phase 1 raw N / 3N | base per exchange | phase 1 per exchange | change |
| --- | ---: | ---: | ---: | ---: | ---: |
| transport `poll_read` | 9,854 / 29,054 | 7,501 / 22,101 | 96 | **73** | −23 |
| QMux pump | 9,547 / 28,147 | 7,194 / 21,194 | 93 | **70** | −23 |
| Tokio task-waker clone | 9,651 / 28,451 | 7,298 / 21,498 | 94 | **71** | −23 |
| Tokio task-waker drop | 9,241 / 27,241 | 6,888 / 20,288 | 90 | **67** | −23 |
| HTTP/3 `poll_event` | 3,074 / 9,074 | 3,074 / 9,074 | 30 | 30 | 0 |
| HTTP/3 `poll_transmit` | 1,428 / 4,228 | 1,428 / 4,228 | 14 | 14 | 0 |

The production source therefore reproduces the diagnostic's 96 → 73 read count exactly. It
removes one pump, one read, one clone and one drop on each of the 23 affected event-poll
decisions rather than reducing HTTP/3 driver passes.

## Controlled Criterion timing

Median microseconds; lower is better. Each change compares the arithmetic mean of the two
medians on each side. Spread is `(max / min) - 1` within one side. A positive claim must exceed
the absolute matching control movement and both side spreads; the no-regression threshold is
the largest of those quantities and 2%.

| Benchmark | base 1/2 | phase 1 1/2 | change | base / phase spread | H2 control |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 24.384 / 23.952 | 22.909 / 22.883 | **−5.26%** | 1.80% / 0.11% | +0.74% |
| socket serial | 35.429 / 35.045 | 34.753 / 34.051 | **−2.37%** | 1.10% / 2.06% | −1.03% |
| duplex concurrency 1 | 24.794 / 24.687 | 24.265 / 23.179 | −4.12% | 0.44% / 4.68% | +0.06% |
| duplex concurrency 8 | 131.292 / 130.215 | 130.540 / 124.387 | −2.52% | 0.83% / 4.95% | +0.47% |
| duplex concurrency 64 | 1053.850 / 1048.271 | 1029.532 / 1004.466 | **−3.24%** | 0.53% / 2.50% | +0.10% |
| socket concurrency 1 | 36.287 / 35.531 | 35.398 / 34.837 | **−2.20%** | 2.13% / 1.61% | +0.24% |
| socket concurrency 8 | 141.387 / 139.331 | 139.138 / 136.426 | −1.84% | 1.48% / 1.99% | +0.90% |
| socket concurrency 64 | 1046.000 / 1027.911 | 1044.266 / 1017.129 | −0.60% | 1.76% / 2.67% | +1.31% |
| duplex 1 MiB guard | 659.204 / 652.356 | 645.991 / 639.381 | −2.00% | 1.05% / 1.03% | −0.01% |
| socket 1 MiB guard | 1133.079 / 1112.034 | 1109.548 / 1086.011 | −2.21% | 1.89% / 2.17% | −0.38% |

Both serial targets clear their controls and within-side spreads. Duplex concurrency 64 and
socket concurrency 1 also clear those bars. Duplex concurrency 1/8 and socket concurrency 8/64
do not clear phase-side spread or control movement and are unclaimed. The bulk guards show no
regression, but are not improvement claims.

## Exact-revision binaries

The copied executables were preserved before either side was run. Representative SHA-256 sums:

| Binary | base | phase 1 |
| --- | --- | --- |
| `probe` | `53ee9ed95756034043982a268ab80e42c700dce102a47f431b3e3b396a63296f` | `2cf914d6d8d3a9ed6d4ae491ae42cf72ba54646d9d72cf22d9d82d3f0556da51` |
| `serial_latency` | `8fceeea596c3b26062a59d32e5a2204ee0f3c835aeadb374819aa1a5d3fcbd11` | `c2cce20ec4ab0a73550ef849ccc3b9eb3e2d0d6e0d57a99d3e7b8efa3b5ad3c0` |
| `transport_serial_latency` | `0f332aec4984f2fbb3fefc045ae5c3cc946b32332dded94820ac3c8e30158f2a` | `573a443ae8d4eb097c73543cc23a686653bd0580afec4373440ddd6d9847130e` |

## What this establishes

- The production one-pump branch rule removes exactly 23 of 96 reads, 23 of 93 pumps, and 23
  waker clone/drop pairs per empty exchange without changing the 30 event polls or 14 transmit
  passes.
- It improves duplex serial by 5.26% and socket serial by 2.37% beyond controls and spread.
- The phase passes the pre-registered positive and no-regression gates and is retained.

## What it does not

- The unclaimed concurrency movements are not evidence of an improvement.
- The bulk guards are not a bulk-throughput optimization claim.
- The count reduction does not claim that the remaining 70 pumps or 73 reads are redundant.
- The run does not compare absolute latency with any pre-migration run, profile a real network,
  or validate the six local OpenSSL-dependent QUIC members.
