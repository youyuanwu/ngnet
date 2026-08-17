# 02 — Gathering against the per-block drain

**Machine:** [`legacy-dev-host`](README.md)
**Date:** 2026-08-04
**Commits:** `main` @ `c8dd79c` against the gathering branch (#7)
**Cases:** [`transport_concurrent_throughput`](../../cases/transport-concurrent-throughput.md),
[`transport_body_throughput`](../../cases/transport-body-throughput.md),
[`transport_serial_latency`](../../cases/transport-serial-latency.md)
**Command:** `taskset -c 3`, benchmarks pre-built so compilation never contends with
measurement
**Repetitions:** two per side; run-to-run spread under 2.5% on the concurrency arms
**Controls:** `ngnet-h2-compio` and `hyper-tokio` — unchanged code, carried as drift controls.
Only `ngnet-h2-tokio` changed.
**Exclusions:** none

## What was being asked

Whether accumulating sub-threshold blocks into a driver-owned buffer and issuing one `writev`
per pass — gathering — beats the per-block drain the tokio adapter used, and by how much. The
baseline arm is the removed `PerRegion` shape: one `write(2)` per session block, no
accumulation.

## Results

Negative is faster.

| Measure | `ngnet-h2-tokio` before (per-block) | `ngnet-h2-tokio` after (gathering) | change |
| --- | --- | --- | --- |
| Concurrent, N=8 | 129.05 µs (62.0 Kelem/s) | 61.63 µs (**129.8 Kelem/s**) | **−52.2%** |
| Concurrent, N=64 | 937.32 µs (68.3 Kelem/s) | 385.51 µs (**166.0 Kelem/s**) | **−58.9%** |
| Concurrent, N=1 | 25.16 µs | 25.68 µs | +2.1%, within drift |
| Body 1 KiB | 52.33 µs (18.6 MiB/s) | 44.53 µs (21.9 MiB/s) | −14.9% |
| Body 64 KiB | 165.05 µs (379 MiB/s) | 141.33 µs (**442 MiB/s**) | −14.4% |
| Body 1 MiB | 2018.73 µs (495 MiB/s) | 1829.76 µs (547 MiB/s) | −9.4%, treat as neutral |

The other two arms in the same runs, for placement rather than comparison: `ngnet-h2-compio`
61.85 µs at N=8 and 379.83 µs at N=64; `hyper-tokio` 67.78 µs and 391.27 µs. At 1 MiB the three
measured 547 (gathering tokio), 531 (hyper) and 482 (coalescing compio) MiB/s — the first two
within this arm's own run-to-run spread of each other, so no ordering should be read into them.

## Drift controls in the same session

| Control arm | Movement |
| --- | --- |
| `ngnet-h2-compio` | −0.2% (N=8), −0.7% (N=64), +0.9% (serial), +0.2% (1 MiB) |
| `hyper-tokio` | 5.1% on serial latency and 9.9% at 1 MiB in one grouped session, code unchanged |
| 1 MiB arm, baseline against itself | 10.2% spread between two repetitions |

`ngnet-h2-compio` cannot implement `write_vectored` — it is a completion transport — so its
inertness is required rather than merely reassuring.

## Two apparent regressions that dissolved

Serial latency showed +6.8% and empty-body +5.1% under a design that ran both baseline
repetitions and then both branch repetitions. Re-measured interleaved (baseline, branch,
baseline, branch), serial latency moved +1.3% on the changed arm against +4.5% and +1.4% on the
two controls — the changed arm moved *less* than either unchanged one — and the empty-body sign
inverted to −4.7% against −0.9% and −0.6%. Neither regression survived.

## What this establishes

- Gathering is worth **−52.2% at N=8 and −58.9% at N=64** on this host, taking the tokio
  transport to parity with io_uring and slightly ahead of hyper.
- The gain is **absent at N=1**, exactly where there is nothing to gather, which is the control
  point the mechanism predicts.
- The body-sweep gains track the write-count reduction (50% at 1 KiB, 20% at 64 KiB, 1.5% at
  1 MiB) at the two smaller sizes.
- **Grouped A/B designs are untrustworthy on this machine.** That is the methodological result,
  and it changed how every later run was designed.

## What it does not

- **1 MiB is not a gain.** Only one syscall in sixty-five is saved there, and the arm's own
  spread is 10.2%. Treat it as neutral, which is what gathering was adopted to achieve at large
  bodies — avoiding the regression coalescing would have caused by copying, not producing a
  gain.
- **The compio side is unswept.** This run varied a readiness `TcpStream` only, so it says
  nothing about whether a completion transport would prefer coalescing.
- The 68.3 Kelem/s figure belongs to the **per-block** drain, not to emulated gathering.
  Emulation accumulates in the driver first, so the small blocks collapse into one region
  before the emulating loop sees them; see
  [`../../allocation-counts.md`](../../allocation-counts.md).

Conclusion drawn in
[`../../findings/write-path-and-gathering.md`](../../findings/write-path-and-gathering.md).
