# 03 — Handing bodies over, re-measured

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-16
**Commit:** `e75118e`
**Case:** [`transport_shared_body`](../../cases/transport-shared-body.md)
**Command:** `taskset -c 3 cargo bench -p ngnet-bench --bench transport_shared_body --
--save-baseline <name>`, once per replicate, benchmarks pre-built
**Repetitions:** five — `r1` and `r2` from the [drift baseline](01-drift-baseline.md) session,
plus `s3`, `s4`, `s5` immediately after, 03:52–04:12 UTC
**Controls:** `hyper-tokio` and both untouched `*-push` twins; the 0-byte point as the
mechanistic control
**Exclusions:** the pre-registered rule — discard any replicate whose 0-byte paired delta
exceeds ±5% — was applied and **excluded none**. The largest 0-byte paired delta was +2.19%.

> **Editorial note, 2026-08-17.** The command above names the benchmark crate as it is
> called now. At commit `e75118e` it was still named for HTTP/2 alone; it was renamed to
> `ngnet-bench` when the suite stopped being an HTTP/2-only one, and the command was
> corrected here so that it still runs. It is otherwise the one that was run, and nothing
> below it has been touched.

## What was being asked

[`../../findings/handing-bodies-over.md`](../../findings/handing-bodies-over.md) records SC-005
as MET on the readiness transport and **NOT MET on the completion transport**, the latter only
because the legacy host's untouched `compio-push` control wandered 34.94% while the measured
gain was 4.07%. `docs/h2/pending-work.md` states what that needs: *"a quieter machine and a
pre-registered replicate count, not more argument."* This host drifts about 1%
([01-drift-baseline](01-drift-baseline.md)), so the question can be asked properly.

## Results

Paired delta, shared against push, per replicate. Negative is faster.

| Transport | Body | Mean | Range | Per replicate |
| --- | --- | --- | --- | --- |
| `tokio` | 0 B | −0.33% | −0.88 .. +0.16 | −0.51, −0.38, +0.16, −0.88, −0.06 |
| `tokio` | 1 KiB | **−29.24%** | −29.55 .. −28.37 | −29.54, −29.42, −29.55, −28.37, −29.31 |
| `tokio` | 64 KiB | **−22.83%** | −23.20 .. −22.45 | −23.02, −22.91, −22.45, −23.20, −22.58 |
| `tokio` | 1 MiB | **−24.33%** | −25.04 .. −23.85 | −24.08, −24.19, −23.85, −25.04, −24.50 |
| `compio` | 0 B | +1.23% | −0.23 .. +2.19 | +1.30, +0.99, +1.92, −0.23, +2.19 |
| `compio` | 1 KiB | **+1.99%** | +1.17 .. +3.61 | +1.71, +1.40, +2.08, +1.17, +3.61 |
| `compio` | 64 KiB | −2.26% | −3.99 .. −0.14 | −1.59, −3.57, −0.14, −3.99, −2.01 |
| `compio` | 1 MiB | **−4.55%** | −5.62 .. −3.18 | −3.83, −5.62, −4.57, −5.56, −3.18 |

## Drift controls in the same session

Spread across the five replicates, `(max − min) / min`:

| Arm | 0 B | 1 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- | --- |
| `hyper-tokio` (untouched) | 2.54% | 2.48% | 1.59% | 1.67% |
| `tokio-push` (untouched) | 1.44% | 1.29% | 1.15% | **0.64%** |
| `compio-push` (untouched) | 1.26% | 1.37% | 3.19% | **1.92%** |

The legacy host's equivalent figures were 15.14% and **34.94%** for `compio-push`. That arm is
the one that cost the original verdict, and here it is as steady as every other.

## What this establishes

- **SC-005 is MET on the readiness transport, decisively.** −24.33% at 1 MiB against a
  same-transport control spread of 0.64% at that size — a factor of 38. All five replicates
  agree in sign and fall inside 1.2 percentage points of each other, and the 0-byte control is
  level at −0.33%, exactly where the mechanism says there is nothing to win.
- **SC-005 is MET on the completion transport.** −4.55% at 1 MiB against `compio-push`'s own
  1.92% spread at that size, and 3.19% at its worst size. Every one of the five replicates is
  negative, ranging −3.18% to −5.62%. **This overturns the NOT MET verdict**, which failed on a
  misbehaving control rather than on its own delta — precisely the outcome
  `docs/h2/pending-work.md` said a quieter machine might produce. The gain is small, and it is
  a gain.
- **The completion transport is measurably *slower* with a handed-over body below 64 KiB.**
  +1.99% at 1 KiB, positive in all five replicates, and +1.23% at 0 B where the shared path
  should cost nothing at all. This is new — the legacy host read −0.9% at 1 KiB and called it
  noise — and it has a mechanism already on record: the shared path mints frame headers the
  copying path got for free, and on the completion transport there is no syscall saving to pay
  for them. Below the size where the copy dominates, the trade is net negative.
- **The mechanism's own prediction holds.** Gains track the write-count collapse
  (0 B `1→1`, 1 KiB `2→1`, 64 KiB `5→2`, 1 MiB `65→17`) rather than the byte counts, and vanish
  at 0 B where the ratio is 1. On the readiness transport, which has the syscalls to save, the
  gain is an order of magnitude larger than on the completion transport, which does not.

## What it does not

- **The replicate count was not pre-registered.** Two passes were run as part of
  [01-drift-baseline](01-drift-baseline.md), and three more were added *after* seeing that the
  first two agreed. That is the letter of what `docs/h2/pending-work.md` asked for left unmet,
  and it is recorded rather than glossed. What mitigates it: no replicate was discarded, the
  exclusion rule excluded none, and the five agree closely enough that no plausible stopping
  rule changes the verdict.
- **Magnitudes did not carry over from the legacy host, only the shape.** tokio at 1 MiB is
  −24.33% here against −30.6% there, and at 1 KiB −29.24% against −35.3%. Same sign, same
  ordering, smaller.
- The duplex family (`shared_body`) was measured in the same two passes and moved −7.07%
  (1 KiB), −8.55% (64 KiB), −8.23% (1 MiB), against −9.2/−9.7/−14.4 on the legacy host. It is
  reported here for corroboration only: its `hyper-tokio` control is the one arm this machine
  is noisy on (+11.48% between the two passes, see
  [01-drift-baseline](01-drift-baseline.md)), so the duplex 1 MiB figure should not be leaned
  on.
- Nothing here measures concurrency or latency with a handed-over body; only the body sweep
  was run.

Recorded in [`../../findings/handing-bodies-over.md`](../../findings/handing-bodies-over.md).
