# 21 — Final QMux/H3 versus HTTP/2 matrix

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Final revision:** `0984e1c`
**Immediate merged predecessor:** `364dbb2`
**Production-code result:** byte-for-byte unchanged; every optimization candidate failed its
pre-registered elapsed gate and was reverted
**Cases:** 16 Criterion cases — duplex and loopback-socket substrates ×
{serial; concurrency 1, 8, 64; body 0, 1 KiB, 64 KiB, 1 MiB} — from the `serial_latency`,
`concurrent_throughput` and `body_throughput` targets and their `transport_*` socket twins, each
carrying its unchanged HTTP/2 arm
**Commands:** `cargo build --release -p ngnet-bench --benches --example probe`; each Criterion
binary as `taskset -c 3 <binary> --bench <stack-filter> --sample-size 50 --measurement-time 3
--warm-up-time 1 --save-baseline final-{1,2} --noplot`
**Repetitions:** three duplex passes and two socket passes of all 16 cases
**Controls:** unchanged H2 arm in every pass; within-pass QMux/H3 ÷ H2 ratios
**Exclusions:** none; all 80 stack/case/pass medians are reported

## What was being asked

After runs 17–20 rejected every optimization candidate, this run asks what the QMux/H3 stack
actually costs against HTTP/2 on this host across the full registered 16-case workload matrix, and
whether the final build differs in any measurable way from the merged predecessor `364dbb2` it was
branched from. Because no mechanism was retained, the second question reduces to a build-identity
check followed by a fresh, fully controlled ratio matrix.

## Revision identity

No production mechanism survived runs 17–20. Rebuilding at `0984e1c` produced the exact run-16
binary hashes built from merged revision `364dbb2`:

| Binary | SHA-256 |
| --- | --- |
| `probe` | `0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` |
| `serial_latency` | `294ec76d110f77eeb86b2c222fa27cc22e6462df6cd6e4077bf55231f368776a` |
| `concurrent_throughput` | `f3bed26412b5874711e55a141d2aa31ad255934fba2b713fa9e384e2941d299d` |
| `body_throughput` | `4910af4e89d7b9283ea90fe6611a52d57f4b948f535e3646c5f9f3abc7efbc18` |
| `transport_serial_latency` | `1a704657a524a6677a960c36f7d1ba98df66fdced24376ee1481009de9e66c82` |
| `transport_concurrent_throughput` | `01704c5524a71e2ef0148a7f884cdff412f9fed04f44e5cdbbfb8c1e4cc3f55d` |
| `transport_body_throughput` | `e2325e731a92f39aca2658d063bdd5e12dcb5e60ecc987bb83ae7f9755972af2` |

Thus the final build, the revision immediately before any attempted candidate, and `364dbb2`
collapse to the same executable comparison side. Running duplicate copies would compare a binary
with itself, so the fresh passes below are the direct final/predecessor/base measurement.
Differences from run 16 are machine drift and sampling spread, not a code effect.

## Final 16-case matrix

Median point estimates are microseconds; lower is better. Spread is
`(maximum - minimum) / minimum`. The final column is the ratio of the summed medians across all
passes, equivalent to the ratio of their arithmetic means.

| Case | H2 passes | H2 spread | QMux/H3 passes | QMux/H3 spread | Per-pass ratios | Final ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 10.090 / 10.077 / 10.297 | 2.19% | 20.696 / 20.743 / 21.455 | 3.67% | 2.051× / 2.058× / 2.084× | **2.065×** |
| duplex concurrency 1 | 10.842 / 10.772 / 11.215 | 4.11% | 21.640 / 21.626 / 21.723 | 0.45% | 1.996× / 2.008× / 1.937× | **1.980×** |
| duplex concurrency 8 | 65.206 / 64.875 / 65.352 | 0.73% | 122.376 / 121.982 / 123.153 | 0.96% | 1.877× / 1.880× / 1.884× | **1.880×** |
| duplex concurrency 64 | 549.192 / 549.982 / 564.534 | 2.79% | 1001.452 / 994.685 / 1022.770 | 2.82% | 1.823× / 1.809× / 1.812× | **1.815×** |
| duplex body 0 | 9.979 / 10.043 / 10.287 | 3.09% | 20.808 / 21.022 / 21.456 | 3.11% | 2.085× / 2.093× / 2.086× | **2.088×** |
| duplex body 1 KiB | 13.329 / 13.614 / 13.729 | 3.00% | 23.493 / 24.394 / 24.223 | 3.83% | 1.763× / 1.792× / 1.764× | **1.773×** |
| duplex body 64 KiB | 40.040 / 40.518 / 41.405 | 3.41% | 59.542 / 61.305 / 62.759 | 5.40% | 1.487× / 1.513× / 1.516× | **1.505×** |
| duplex body 1 MiB | 499.070 / 509.550 / 506.348 | 2.10% | 613.415 / 619.561 / 614.368 | 1.00% | 1.229× / 1.216× / 1.213× | **1.219×** |
| socket serial | 16.777 / 16.877 | 0.60% | 31.764 / 32.114 | 1.10% | 1.893× / 1.903× | **1.898×** |
| socket concurrency 1 | 17.480 / 17.605 | 0.71% | 33.019 / 32.927 | 0.28% | 1.889× / 1.870× | **1.880×** |
| socket concurrency 8 | 72.301 / 72.405 | 0.14% | 132.222 / 132.228 | 0.00% | 1.829× / 1.826× | **1.828×** |
| socket concurrency 64 | 555.937 / 557.170 | 0.22% | 995.508 / 999.732 | 0.42% | 1.791× / 1.794× | **1.792×** |
| socket body 0 | 16.871 / 16.882 | 0.07% | 32.660 / 32.052 | 1.90% | 1.936× / 1.899× | **1.917×** |
| socket body 1 KiB | 28.936 / 28.331 | 2.13% | 35.532 / 35.118 | 1.18% | 1.228× / 1.240× | **1.234×** |
| socket body 64 KiB | 93.311 / 93.308 | 0.00% | 90.898 / 91.712 | 0.89% | 0.974× / 0.983× | **0.979×** |
| socket body 1 MiB | 1246.008 / 1245.092 | 0.07% | 1048.103 / 1049.250 | 0.11% | 0.841× / 0.843× | **0.842×** |

Duplex concurrency remains reporting-only. On the socket, QMux/H3 remains slower for empty,
1 KiB and concurrency workloads, reaches parity around 64 KiB, and is about 15.8% faster at
1 MiB. The body and serial targets both include body size zero; their small difference is ordinary
separate-process spread and both are reported rather than combined.

## Comparison with the merged predecessor

Run 16 measured the same executable on this Xeon 8573C. The ten overlapping ratios compare as
follows:

| Case | Run 16 (`364dbb2`) | Run 21 (`0984e1c`) |
| --- | ---: | ---: |
| duplex serial | 2.07× | 2.065× |
| socket serial | 1.83× | 1.898× |
| duplex concurrency 1 / 8 / 64 | 1.99× / 1.89× / 1.81× | 1.980× / 1.880× / 1.815× |
| socket concurrency 1 / 8 / 64 | 1.79× / 1.84× / 1.78× | 1.880× / 1.828× / 1.792× |
| duplex 1 MiB | 1.24× | 1.219× |
| socket 1 MiB | 0.86× | 0.842× |

These movements are not optimization claims: hash identity proves a zero production-code delta.
They demonstrate why the candidate gates required matching controls and spreads.

## Drift controls in the same session

The unchanged HTTP/2 arm is the control in every pass, and every ratio is formed within its own
pass, so session drift cancels in the reported ratios.

| Control arm | Movement across passes |
| --- | --- |
| duplex H2 (8 cases, three passes) | 0.73–4.11% spread |
| socket H2 (8 cases, two passes) | 0.00–2.13% spread |
| duplex QMux/H3 (8 cases, three passes) | 0.45–5.40% spread |
| socket QMux/H3 (8 cases, two passes) | 0.00–1.90% spread |

No movement in this run is attributed to code, because there is no code delta to attribute it to.

## What this establishes

- The final branch head builds byte-identical benchmark binaries to merged revision `364dbb2`, so
  the production-code delta of this work is exactly zero and is provable from the hashes above.
- On this Xeon 8573C, in this session, QMux/H3 ÷ H2 spans 0.842× to 2.088× across the 16 registered
  cases: slower for empty, small-body and concurrent workloads, at parity near a 64 KiB body
  (0.979×), and faster only at a 1 MiB body on the socket (0.842×).
- Run 16's exact count vector still describes the measured executable, because the `probe` hash is
  unchanged, so the counts and the timings in this record refer to the same binary.
- Every candidate disposition recorded in runs 17–20 survives the combined check: there is no
  retained mechanism whose claim could have regressed, and no adverse interaction to isolate.

## Final exact state and disposition

Because the probe hash is also identical, run 16's lossless two-point final count vector applies
to the exact executable measured here: 73 reads, 70 pumps, 71 waker clones, 67 drops, 30 H3 event
polls, 14 transmit polls, 14 driver passes, 23 queue pops, seven pushes, 128 empty-exchange mallocs
and three socket writes. The 1 MiB allocation count remains 710 and socket writes remain 67.

Candidate A reduced work counts but missed the socket gate. Candidate B removed 160 mallocs but
did not improve elapsed time. Candidate C removed 20 mallocs and three reallocs but was unstable
and missed both-substrate repeatability. Candidate D had no queue-confined mechanism capable of
reducing its registered count. Every prototype was reverted, so the final PR carries measured
evidence and backlog closure rather than an unproven optimization.

## What it does not

This run does not compare absolute timings with the older Xeon 8370C, claim that rerun drift is a
code effect, or turn duplex concurrency into an improvement claim. It does not sweep header
cardinality, QPACK blocking patterns, non-empty trailers, alternate runtimes, or network latency.
Those are separate workloads, not missing controls for the 16 registered cases.
