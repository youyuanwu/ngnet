# 07 — Which of the changes produced the gain?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-17
**Commit(s):** seven, each against its predecessor —
`524fa54` → `c8fca5c` → `cad8975` → `223960d` → `9f97334` → `f3205c0` → `d25574b`
**Cases:** `serial_latency` and `body_throughput`, run whole. **Duplex only.** No `transport_`
target was run, so no socket identifier is measured per commit anywhere
**Command:** for each commit, `cargo build --benches -p ngnet-bench --release` then
`taskset -c 3 cargo bench -p ngnet-bench --bench serial_latency --bench body_throughput -- --save-baseline <name>`
**Repetitions:** two per commit, and interleaved in the only way a seven-point sequence can be —
every commit was measured once, in order, before any was measured twice
**Controls:** the ten `ngnet-h2` and `hyper` identifiers in the same two groups, untouched
throughout
**Exclusions:** none; nothing was dropped

## What was being asked

[`06`](06-qmux-write-path.md) measures the whole set of changes against the state before them and
says explicitly that it attributes nothing among them. This run asks which change produced what.
It exists because that question was being answered from figures that had no run behind them.

The answer is mostly **"this cannot say"**, and that is the useful part.

## Results

Percent change from the immediately preceding commit; negative is faster. The control columns are
the ten unchanged identifiers in the same session, and **a step smaller than its own row's control
worst is not a result**.

| Step | commits | `body_throughput/0` | `body_throughput/1024` | `body_throughput/1048576` | `body_throughput/65536` | `serial_latency/ngnet-qmux-h3` | control mean | control worst |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| coalesce | `524fa54`→`c8fca5c` | +3.3% | -3.4% | -21.7% | -8.6% | -0.3% | 1.59% | 4.47% |
| direct | `c8fca5c`→`cad8975` | -0.7% | -1.3% | -2.8% | -1.2% | +1.5% | 1.85% | 4.18% |
| scan | `cad8975`→`223960d` | +1.9% | +1.6% | -2.8% | -1.3% | +0.1% | 0.90% | 2.95% |
| alias | `223960d`→`9f97334` | +2.5% | +3.3% | -0.6% | +4.8% | +4.7% | 0.73% | 1.74% |
| vectored | `9f97334`→`f3205c0` | -0.0% | -2.9% | -0.7% | -7.7% | +1.3% | 1.84% | 5.18% |
| credit | `f3205c0`→`d25574b` | -0.7% | -1.2% | -1.0% | -2.3% | -0.1% | 1.42% | 3.64% |
## Drift controls in the same session

They are in the table above, per step, which is the only place they are useful — a control band
averaged across a seven-point sequence would hide that two steps ran in noticeably noisier
sessions than the others.

## What this establishes

- **Write coalescing is the change that produced the body gains.** −21.7% at a megabyte and −8.6%
  at 64 KiB, against a control worst of 4.47% in its own step. Nothing else here comes close.
- **Nothing else is resolved on these identifiers.** Direct serialisation, scanning in place and
  credit batching each move every identifier by less than their step's own control worst. They are
  **not** shown to be worth nothing; they are shown to be smaller than this instrument can see.
  Their establishment is by count, which is what the requirement they were made under provides
  for, and the counts are in the crates' own tests.
- **Vectored record input at 64 KiB is the one marginal case: −7.7% against a control worst of
  5.18%.** Outside the band, but not far outside, in the noisiest step of the seven. It had been
  predicted to be unresolvable, and this is weak evidence against that prediction rather than
  strong evidence for a gain. It deserves a run of its own before it is quoted as a figure.
- **Delivery aliasing is the only step that is unambiguously unfavourable**, and it is treated at
  length in [`05`](05-qmux-delivery-aliasing.md), which measures the same step with the same data.

## What it does not

- **It does not measure a single socket identifier.** Every figure here is duplex. The gains
  [`06`](06-qmux-write-path.md) records on `transport_*` arms — including the −8.5% at concurrency
  64 that is the clearest evidence of the coalescing mechanism — are **not** attributed to any
  commit by this run or any other. A reader wanting to know which change produced the socket gain
  has to be told that nobody has measured it.
- **It does not measure concurrency at all.** `concurrent_throughput` was not run, so the axis on
  which coalescing was predicted to matter most is absent from the attribution.
- **The steps are not independent of their order.** Each is measured on a build containing every
  change before it, and the fourth was later reverted, so the last two steps were measured on a
  build carrying a change that does not ship.
- **Two repetitions per side is the minimum this suite allows.** For the −21.7% step that is
  ample. For everything else in the table it is why the answer is "cannot say".
