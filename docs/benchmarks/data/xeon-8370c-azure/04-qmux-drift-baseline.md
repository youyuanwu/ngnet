# 04 — What does an unchanged QMux arm do here, run to run?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-17
**Commit(s):** `524fa54` — a single sha, twice; no code changed between the passes
**Cases:** all eight bench targets, nine groups, 94 benchmark ids
**Command:** `cargo build --benches -p ngnet-bench --release`, then
`taskset -c 3 cargo bench -p ngnet-bench -- --save-baseline r1`, then the same with `r2`
**Repetitions:** two passes, back to back, not interleaved with anything — this run *is* the
drift measurement, so there is no other side to interleave with
**Controls:** none, and none apply. Every arm here is a control: nothing changed between the
passes, so every difference reported is drift by construction
**Exclusions:** none. No replicate was dropped, and no rule was needed, because there is no
result to protect from one

## What was being asked

[`01-drift-baseline`](01-drift-baseline.md) established what this machine's run-to-run variation
looks like, and it remains the reference for the arms it covered — but it was taken before the
QMux arms existed and covers none of them, which
[`data/README.md`](../README.md) has said ever since. Work is now beginning on the QMux write
path, and every figure it produces will be a paired difference on one of sixteen QMux benchmark
ids. A paired difference is only a result if it is larger than what the same id does when nothing
changes at all, and for these sixteen ids that quantity was unknown. This run measures it, on the
same machine, in the same state, with the same commands the earlier baseline used.

## Results

Across all 94 ids: median absolute drift **0.53%**, mean **1.11%**, worst **10.42%**. Twelve ids
exceeded 2% and three exceeded 5%. That is the same order of magnitude
[`01-drift-baseline`](01-drift-baseline.md) reported over 78 ids, which is the first thing worth
knowing: adding sixteen QMux ids did not change what this machine is like.

Per arm, mean of the absolute drift across that arm's benchmarks, with that arm's worst case
beside it — the presentation [`01-drift-baseline`](01-drift-baseline.md) uses:

| Arm | Benchmarks | Mean \|drift\| | Worst |
| --- | --- | --- | --- |
| `compio-push` | | 4 | 0.80% | 1.55% |
| `compio-shared` | | 4 | 1.86% | 3.43% |
| `hyper` | | 11 | 1.41% | 3.98% |
| `hyper-tokio` | | 16 | 1.73% | 9.35% |
| `ngnet-h2` | | 11 | 0.62% | 1.88% |
| `ngnet-h2-compio` | | 8 | 1.19% | 3.30% |
| `ngnet-h2-push` | | 4 | 0.32% | 0.50% |
| `ngnet-h2-shared` | | 4 | 0.81% | 1.62% |
| `ngnet-h2-tokio` | | 8 | 0.89% | 3.00% |
| `ngnet-qmux-h3` | | 8 | 1.55% | 10.42% |
| `ngnet-qmux-h3-tokio` | | 8 | 0.67% | 1.41% |
| `tokio-push` | | 4 | 0.30% | 0.42% |
| `tokio-shared` | | 4 | 0.82% | 1.03% |
The two QMux arms are not alike, and the difference is the point of this run. Per id:

| Benchmark id | r1 (µs) | r2 (µs) | drift |
| --- | --- | --- | --- |
| `body_throughput/ngnet-qmux-h3/0` | 28.6 | 28.4 | -0.50% |
| `body_throughput/ngnet-qmux-h3/1024` | 34.6 | 34.6 | -0.10% |
| `body_throughput/ngnet-qmux-h3/1048576` | 895.2 | 988.5 | +10.42% |
| `body_throughput/ngnet-qmux-h3/65536` | 88.7 | 88.3 | -0.38% |
| `concurrent_throughput/ngnet-qmux-h3/1` | 29.9 | 29.9 | +0.06% |
| `concurrent_throughput/ngnet-qmux-h3/64` | 1242.7 | 1242.6 | -0.01% |
| `concurrent_throughput/ngnet-qmux-h3/8` | 156.9 | 156.6 | -0.21% |
| `serial_latency/ngnet-qmux-h3` | 29.2 | 29.0 | -0.74% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/0` | 53.2 | 53.9 | +1.30% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/1024` | 67.3 | 67.6 | +0.48% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/1048576` | 1768.3 | 1793.3 | +1.41% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/65536` | 173.7 | 173.6 | -0.06% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/1` | 55.7 | 55.0 | -1.23% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/64` | 1850.2 | 1840.9 | -0.50% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/8` | 247.0 | 246.5 | -0.22% |
| `transport_serial_latency/ngnet-qmux-h3-tokio` | 52.6 | 52.7 | +0.15% |
## Drift controls in the same session

Every arm is a control here; the table above is the complete set. The one worth naming
separately is `ngnet-h2`, at **0.62%** mean and **1.88%** worst over eleven ids, because it is the
arm every QMux figure will be quoted against. A QMux/HTTP-2 ratio inherits both arms' variation,
so the bar for a *ratio* is wider than the bar for either side alone.

## What this establishes

- **The two identifiers a write-coalescing change needs are the steadiest QMux ids on the
  machine.** `transport_concurrent_throughput/ngnet-qmux-h3-tokio/8` and `/64` drifted **−0.22%**
  and **−0.50%**. These are the only arms combining several streams in flight with a real system
  call, which is where a write-count reduction can show as a syscall saving at all, and they will
  resolve an effect of a few percent. That is the single most useful thing this run says.
- **The socket QMux arm is quiet and the duplex QMux arm is not.** `ngnet-qmux-h3-tokio` averages
  **0.67%** with a worst case of **1.41%**; `ngnet-qmux-h3` averages **1.55%** with a worst case of
  **10.42%**. The expectation before the run was the reverse — a duplex has no kernel in it — and
  it was wrong.
- **`body_throughput/ngnet-qmux-h3/1048576` is the noisiest identifier in the entire suite**, at
  **+10.42%**, ahead of `shared_body/hyper-tokio/1048576` at +9.35%. It is also, at 895 µs, the
  slowest duplex id and the one doing the most allocation per iteration. Anything measured there
  needs an effect above ten percent, or many more replicates, to be a result at all.
- **Its 64 KiB neighbour is not noisy.** `body_throughput/ngnet-qmux-h3/65536` drifted **−0.38%**
  and `.../1024` **−0.10%**. The noise is specific to the megabyte point, not to the duplex family.
- **Adding sixteen QMux ids did not change what the machine is like.** Median 0.53% against the
  earlier baseline's 0.90%, mean 1.11% against 1.23%.

## What it does not

- **It does not establish drift over hours, across reboots, or against a busy machine.** Both
  passes ran back to back on an otherwise idle host, which is the condition every later run must
  also meet for this to be the right bar.
- **It says nothing about why the megabyte duplex point is noisy.** Three candidates are
  untested: the allocation volume at that size, the absence of a syscall to serialise the two
  peers against each other, and the run length itself. A later run that wanted to *use* that
  identifier would have to settle this first; the recommendation below is to use its 64 KiB
  neighbour instead and avoid the question.
- **It does not cover the three groups with no QMux arm** — `shared_body`,
  `transport_shared_body` and `concurrent_throughput_multi_thread`, 38 ids between them. Their
  arms are in the per-arm table because they ran, but no QMux figure will ever be quoted against
  them.
- **It is not a comparison of QMux against HTTP/2.** Both arms' absolute figures appear above only
  as the input to a drift calculation. Nothing here licenses a statement about which is faster.
- **Two repetitions is this suite's stated minimum, not its practice.** The settled verdicts on
  this machine used five and ten. A per-id figure here is one paired observation, so the per-arm
  means are better evidence than any single row.

## What this changes about the work in progress

Recorded here because it contradicts a plan written before the numbers existed. The screen that
preceded this run recommended `body_throughput/ngnet-qmux-h3/1048576` as the primary instrument
for the copy-removal and allocation-removal changes, on the reasoning that a duplex arm has no
kernel in the way and so shows a CPU change most clearly. That reasoning is sound and the choice
of identifier is not: that id is the noisiest of the 94. **Use `.../65536` as the primary
instrument instead**, where drift is −0.38%, and treat the megabyte point as confirmation only.
