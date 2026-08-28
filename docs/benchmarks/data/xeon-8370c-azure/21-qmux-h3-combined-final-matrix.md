# 21 — Final QMux/H3 versus HTTP/2 matrix

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Commit(s):** final `0984e1c` against merged predecessor `364dbb2`
**Immediate merged predecessor:** `364dbb2`
**Production-code result:** byte-for-byte unchanged; Candidates A–C failed their pre-registered
elapsed gates and were reverted, while Candidate D was closed documentation-only as
gate-incompatible
**Cases:** 16 Criterion cases — duplex and loopback-socket substrates ×
{serial; concurrency 1, 8, 64; body 0, 1 KiB, 64 KiB, 1 MiB} — from the `serial_latency`,
`concurrent_throughput` and `body_throughput` targets and their `transport_*` socket twins, each
carrying its unchanged HTTP/2 arm
**Command:** `cargo build --release -p ngnet-bench --benches --example probe`; each Criterion
binary as `taskset -c 3 <binary> --bench <stack-filter> --sample-size 100 --measurement-time 3
--warm-up-time 1 --save-baseline final-{1,2} --noplot`
**Repetitions:** three duplex passes and two socket passes of all 16 cases
**Controls:** unchanged H2 arm in every pass; within-pass QMux/H3 ÷ H2 ratios
**Exclusions:** none; all 80 stack/case/pass medians are reported

## What was being asked

After runs 17–20 rejected three prototypes and closed one documentation-only candidate, this run asks what the QMux/H3 stack
actually costs against HTTP/2 on this host across the full registered 16-case workload matrix, and
whether the final build differs in any measurable way from the merged predecessor `364dbb2` it was
branched from. Because no mechanism was retained, the second question reduces to a build-identity
check followed by a fresh, fully controlled ratio matrix.

## Results

The exact revision identity and complete matrix follow.

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
| duplex serial | 10.003 / 10.199 / 10.082 | 1.95% | 20.741 / 20.675 / 20.729 | 0.32% | 2.073× / 2.027× / 2.056× | **2.052×** |
| duplex concurrency 1 | 10.731 / 10.785 / 10.752 | 0.51% | 21.672 / 21.647 / 21.528 | 0.67% | 2.020× / 2.007× / 2.002× | **2.010×** |
| duplex concurrency 8 | 64.873 / 65.191 / 64.866 | 0.50% | 122.621 / 122.902 / 122.294 | 0.50% | 1.890× / 1.885× / 1.885× | **1.887×** |
| duplex concurrency 64 | 545.775 / 548.287 / 546.753 | 0.46% | 992.122 / 996.066 / 990.917 | 0.52% | 1.818× / 1.817× / 1.812× | **1.816×** |
| duplex body 0 | 10.038 / 10.108 / 10.068 | 0.70% | 20.812 / 20.743 / 20.778 | 0.33% | 2.073× / 2.052× / 2.064× | **2.063×** |
| duplex body 1 KiB | 13.363 / 13.366 / 13.357 | 0.07% | 23.810 / 23.942 / 23.608 | 1.41% | 1.782× / 1.791× / 1.767× | **1.780×** |
| duplex body 64 KiB | 40.087 / 40.024 / 40.035 | 0.16% | 61.050 / 60.719 / 59.468 | 2.66% | 1.523× / 1.517× / 1.485× | **1.508×** |
| duplex body 1 MiB | 498.596 / 496.617 / 505.557 | 1.80% | 609.834 / 609.935 / 613.603 | 0.62% | 1.223× / 1.228× / 1.214× | **1.222×** |
| socket serial | 16.884 / 16.886 | 0.01% | 31.699 / 31.647 | 0.16% | 1.877× / 1.874× | **1.876×** |
| socket concurrency 1 | 17.638 / 17.616 | 0.12% | 32.654 / 32.708 | 0.17% | 1.851× / 1.857× | **1.854×** |
| socket concurrency 8 | 72.484 / 72.397 | 0.12% | 131.735 / 131.676 | 0.04% | 1.817× / 1.819× | **1.818×** |
| socket concurrency 64 | 558.096 / 557.143 | 0.17% | 989.328 / 991.497 | 0.22% | 1.773× / 1.780× | **1.776×** |
| socket body 0 | 16.946 / 16.899 | 0.28% | 31.790 / 31.766 | 0.07% | 1.876× / 1.880× | **1.878×** |
| socket body 1 KiB | 28.062 / 27.974 | 0.31% | 34.478 / 34.799 | 0.93% | 1.229× / 1.244× | **1.236×** |
| socket body 64 KiB | 91.630 / 91.953 | 0.35% | 91.180 / 93.411 | 2.45% | 0.995× / 1.016× | **1.005×** |
| socket body 1 MiB | 1242.383 / 1255.320 | 1.04% | 1049.331 / 1060.416 | 1.06% | 0.845× / 0.845× | **0.845×** |

Duplex concurrency remains reporting-only. On the socket, QMux/H3 remains slower for empty,
1 KiB and concurrency workloads, reaches parity around 64 KiB, and is about 15.5% faster at
1 MiB. The body and serial targets both include body size zero; their small difference is ordinary
separate-process spread and both are reported rather than combined.

## Comparison with the merged predecessor

Run 16 measured the same executable on this Xeon 8573C. The ten overlapping ratios compare as
follows. Run 16 and run 21 use their documented per-run aggregation methods, so this table is a
drift-oriented descriptive comparison rather than a code-effect estimate:

| Case | Run 16 (`364dbb2`) | Run 21 (`0984e1c`) |
| --- | ---: | ---: |
| duplex serial | 2.07× | 2.052× |
| socket serial | 1.83× | 1.876× |
| duplex concurrency 1 / 8 / 64 | 1.99× / 1.89× / 1.81× | 2.010× / 1.887× / 1.816× |
| socket concurrency 1 / 8 / 64 | 1.79× / 1.84× / 1.78× | 1.854× / 1.818× / 1.776× |
| duplex 1 MiB | 1.24× | 1.222× |
| socket 1 MiB | 0.86× | 0.845× |

These movements are not optimization claims: hash identity proves a zero production-code delta.
They demonstrate why the candidate gates required matching controls and spreads.

## Drift controls in the same session

The unchanged HTTP/2 arm is the control in every pass, and every ratio is formed within its own
pass, so session drift cancels in the reported ratios.

| Control arm | Movement across passes |
| --- | --- |
| duplex H2 (8 cases, three passes) | 0.07–1.95% spread |
| socket H2 (8 cases, two passes) | 0.01–1.04% spread |
| duplex QMux/H3 (8 cases, three passes) | 0.32–2.66% spread |
| socket QMux/H3 (8 cases, two passes) | 0.04–2.45% spread |

No movement in this run is attributed to code, because there is no code delta to attribute it to.

## What this establishes

- The final branch head builds byte-identical benchmark binaries to merged revision `364dbb2`, so
  the production-code delta of this work is exactly zero and is provable from the hashes above.
- On this Xeon 8573C, in this session, QMux/H3 ÷ H2 spans 0.845× to 2.063× across the 16 registered
  cases: slower for empty, small-body and concurrent workloads, at parity near a 64 KiB body
  (1.005×), and faster only at a 1 MiB body on the socket (0.845×).
- Run 16's exact count vector still describes the measured executable, because the `probe` hash is
  unchanged, so the counts and the timings in this record refer to the same binary.
- Every candidate disposition recorded in runs 17–20 survives the combined check: there is no
  retained mechanism whose claim could have regressed, and no adverse interaction to isolate.

## Final exact state and disposition

Because the probe hash is also identical, run 16's lossless two-point final count vector applies
to the exact executable measured here. The counts were not retaken in run 21: binary identity,
rather than a second sampling pass, is the equivalence proof. The vector is 73 reads, 70 pumps,
71 waker clones, 67 drops, 30 H3 event
polls, 14 transmit polls, 14 driver passes, 23 queue pops, seven pushes, 128 empty-exchange mallocs
and three socket writes. The 1 MiB allocation count remains 710 and socket writes remain 67.

Candidate A reduced work counts but missed the socket gate. Candidate B removed 160 mallocs but
did not improve elapsed time. Candidate C removed 20 mallocs and three reallocs but every
100-sample pass remained below the 2% retention floor. Candidate D had no queue-confined mechanism capable of
reducing its registered count. Every prototype was reverted, so the final PR carries measured
evidence and backlog closure rather than an unproven optimization.

## Validation

Focused H3, QMux and QMux/H3 suites passed in default, no-default-feature and release
configurations. The supported workspace suite, dependency-graph structural test, all-feature and
no-default-feature clippy with warnings denied, rustdoc with warnings denied, and benchmark smoke
all passed. `cargo check -p ngnet-quic --no-default-features` also passed.

The five OpenSSL-dependent QUIC surfaces were attempted and reached the documented host limit:
the installed OpenSSL 3.0.13 is below ngtcp2's required 3.5.0, beginning at
`ngnet-quic-sys`. CI is assigned `ngnet-quic-sys`, `ngnet-quic`, `ngnet-quic-h3`,
`ngnet-quic-h3-tests`, and `ngnet-quic-tests`; `ngnet-workspace-tests --no-run` and its
dependency-graph test passed locally.

## What it does not

This run does not compare absolute timings with the older Xeon 8370C, claim that rerun drift is a
code effect, or turn duplex concurrency into an improvement claim. It does not sweep header
cardinality, QPACK blocking patterns, non-empty trailers, alternate runtimes, or network latency.
Those are separate workloads, not missing controls for the 16 registered cases.
