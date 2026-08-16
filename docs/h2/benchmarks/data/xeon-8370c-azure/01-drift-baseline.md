# 01 — Drift baseline

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-16
**Commit:** `e75118e` — one commit, no code change between the two passes
**Cases:** all eight bench targets, 78 benchmarks
**Command:**

```sh
cargo build --benches -p ngnet-h2-bench --release   # pre-built, so compilation never contends
taskset -c 3 cargo bench -p ngnet-h2-bench -- --save-baseline r1
taskset -c 3 cargo bench -p ngnet-h2-bench -- --save-baseline r2
```

**Repetitions:** two full passes, back to back, 03:13–03:32 and 03:32–03:51 UTC, 18m42s each
**Controls:** the whole run is a control — nothing changed between the passes, so every number
below is drift by construction
**Exclusions:** none

## What was being asked

What an unchanged arm does on this host between two identical passes. Until that number
exists, no result measured here can be sized against anything: a 4% gain means one thing on a
machine that drifts 1% and nothing at all on a machine that drifts 15%. The legacy host's
answer was 5–15%, with one arm reaching 34.94%, and every method rule in
[`../../controls.md`](../../controls.md) was built to survive it.

Medians of Criterion's per-iteration timing, taken from the saved `r1` and `r2` baselines.

## Results

| | |
| --- | --- |
| Benchmarks compared | 78 |
| Median \|drift\| | **0.90%** |
| Mean \|drift\| | 1.23% |
| Over 5% | 1 of 78 |
| Over 10% | 1 of 78 |
| Largest | +11.48% — `shared_body/hyper-tokio/1048576` |
| Second largest | +4.19% — `transport_body_throughput/hyper-tokio/1024` |

Per-arm summary, mean of the absolute drift across that arm's benchmarks:

| Arm | Family | Benchmarks | Mean \|drift\| | Worst |
| --- | --- | --- | --- | --- |
| `ngnet-h2-compio` | socket | 8 | **0.52%** | +1.87% |
| `ngnet-h2-tokio` | socket | 8 | 1.08% | +2.05% |
| `hyper-tokio` | socket | 12 | 1.49% | +4.19% |
| `compio-push` / `compio-shared` | socket | 8 | 1.00% | −1.55% |
| `tokio-push` / `tokio-shared` | socket | 8 | 0.75% | −1.34% |
| `ngnet-h2` | duplex | 11 | 0.93% | +2.29% |
| `ngnet-h2-push` / `ngnet-h2-shared` | duplex | 8 | 1.12% | +2.38% |
| `hyper` | duplex | 11 | 1.11% | +2.88% |
| `hyper-tokio` | duplex | 4 | **4.97%** | +11.48% |

The full 78-row table is in [`01-drift-baseline-table.md`](01-drift-baseline-table.md).

Criterion's own dispersion within a pass, for comparison: median absolute deviation was
**under 1% of the median for 69 of 78 benchmarks**, and never above 2.7%. Within-pass noise and
between-pass drift are therefore the same order of magnitude here, which is the condition the
legacy host never reached.

## What this establishes

- **The drift bar on this machine is about 1%, and 2% covers all but one benchmark.** A paired
  delta of 5% or more, consistent in sign across passes, is a result here. On the legacy host
  the same claim needed 30%.
- **The socket family is the quieter one**, which is the opposite of what a syscall-heavy
  workload might suggest and is worth knowing before designing a run: the noisiest arms are the
  duplex hyper ones.
- **One benchmark is not usable at this bar without replication.**
  `shared_body/hyper-tokio/1048576` moved +11.48% between identical passes, ten times its own
  within-pass dispersion of 0.37%, and its socket counterpart moved +1.67% in the same session.
  A duplex 1 MiB hyper result needs more than two passes behind it.
- **The compio arms are as steady as the tokio ones** (0.52% on `ngnet-h2-compio`, the
  steadiest arm in the run), which the legacy host never showed — there `compio-push` was the
  arm that wandered 34.94% and cost a verdict.

## What it does not

- It does not establish drift over hours or across reboots. Both passes ran inside 40 minutes
  on a machine that was otherwise idle; a run competing with other tenants on the Azure host
  may see more, and this figure should be re-derived if results ever stop making sense.
- It does not license reading the absolute figures in this session against the legacy host's.
  They are a different machine's numbers and nothing normalises between them.
- Turbo is still enabled and cannot be disabled from inside the guest, and core 3 shares a
  physical core with the hyper-thread on core 7. The bar above is what this host achieves
  *with* those uncontrolled, not what it could achieve without them.

The survey the same two passes produced is [02-first-survey](02-first-survey.md).
