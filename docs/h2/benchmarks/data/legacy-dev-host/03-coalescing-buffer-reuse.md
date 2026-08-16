# 03 — Reusing the coalescing buffer

**Machine:** [`legacy-dev-host`](README.md)
**Date:** 2026-08-04
**Commit:** #8 — *Reuse the coalescing buffer instead of rebuilding it every pass*
**Cases:** [`transport_body_throughput`](../../cases/transport-body-throughput.md), with
[`transport_concurrent_throughput`](../../cases/transport-concurrent-throughput.md) as a
cross-check
**Command:** `taskset -c 3`, against a saved Criterion baseline
**Repetitions:** two; the quoted run is the quieter of the two, and the reason is given below
**Controls:** `ngnet-h2-tokio` and `hyper-tokio` — neither takes the owned path
**Exclusions:** none

## What was being asked

`CompioIo` is the only shipped transport that takes the owned coalescing path, so removing
that path's per-pass allocation was measured on its own rather than folded into the gathering
work. The buffer had been a local handed away whole with `freeze()`, so every pass rebuilt it;
hoisting it and handing over `split().freeze()` lets `bytes` reclaim the capacity.

## Results

Negative is faster.

| Body | `ngnet-h2-compio` (changed) | `ngnet-h2-tokio` (control) | `hyper-tokio` (control) |
| --- | --- | --- | --- |
| 0 B | −5.9% | +2.0% | −0.9% |
| 1 KiB | −5.8% | +7.1% | −1.2% |
| 64 KiB | −3.8% | +0.9% | −0.4% |
| 1 MiB | −7.0% | +1.1% | +0.8% |

## Drift controls in the same session

| Control arm | Movement |
| --- | --- |
| `hyper-tokio` | within ±1.2% across all four sizes |
| `ngnet-h2-tokio` | +0.9% to +7.1% — investigated, see below |

`hyper-tokio` holding within ±1.2% is the quietest condition obtained for any measurement on
this host, which is why this run is quoted rather than an average of the two. The second run
agreed on direction for compio but was noisier throughout.

## The tokio arm reading positive

Chased down rather than waved through, since a regression on the default transport would
matter more than the gain. It does not reproduce: on `transport_concurrent_throughput` — the
workload that exercises the gathering path hardest — the same build measured **−5.1%, −0.2%
and +1.2%** at N=1/8/64, against a control that had itself moved −3.1% to −5.0%.

There is no mechanism for a cost on that arm, because the empty case is taken before the split
(see `flush`): a pass that never coalesces hands over `Bytes::new()` and touches the buffer not
at all. A cost appearing in one benchmark family and not the other, with no mechanism, is
drift.

## What this establishes

- **About 4–7% for the completion transport** across the body sweep, present at every size
  including 0 B — the buffer was rebuilt per pass whether or not there was a payload.
- The cost removed is **growth**, not allocation count: rebuilding from empty re-copies the
  contents at every doubling. Twelve `malloc`/`free` pairs under glibc's thread cache are well
  under a microsecond against a 62 µs pass.

## What it does not

- It does not show a cost or a gain on the readiness arms, which do not take the owned path.
- It says nothing about the pair of atomic refcount operations later removed from the readiness
  coalesced drain; no benchmark here isolates those, and that claim is structural.
  See [`../../allocation-counts.md`](../../allocation-counts.md).

Conclusion drawn in
[`../../findings/coalescing-buffer-reuse.md`](../../findings/coalescing-buffer-reuse.md).
