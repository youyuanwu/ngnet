# 10 — What did constant-time closed-stream lookup buy?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-27
**Commit(s):** `6d13712` before, `419a774` after
**Cases:** the QMux arms in `serial_latency`, `concurrent_throughput`,
`transport_serial_latency` and `transport_concurrent_throughput`. The corresponding unchanged
HTTP/2 arms were run separately as drift controls
**Command:** after `cargo build --benches -p ngnet-bench --release`, pinned to core 3:
`taskset -c 3 cargo bench -p ngnet-bench --bench serial_latency
--bench concurrent_throughput --bench transport_serial_latency
--bench transport_concurrent_throughput -- ngnet-qmux-h3 --save-baseline <name>`.
The control series used the same command with `ngnet-h2` as the filter
**Repetitions:** two per side, interleaved before → after → before → after. The unchanged
HTTP/2 controls used the same interleaving immediately afterwards
**Controls:** corresponding HTTP/2 arms moved from −1.69% to +2.55%. The smallest QMux effect
was −7.14%; every effect is larger than its matching control movement and the tested arm's own
between-repetition spread
**Exclusions:** none; no sample or benchmark was discarded

## What was being asked

[`09`](09-qmux-h2-mechanisms.md) established that the linear scan of 1,024 closed-stream
tombstones was costly by shortening the retention window as a diagnostic. That was not a valid
fix. This run asks whether the shipped fix — preserving all 1,024 FIFO tombstones while moving
membership into a `HashSet` — realizes the predicted gain.

## Results

Criterion median per exchange, averaged across each side's two passes. Lower is better.

| Benchmark | before (µs) | after (µs) | change | within-side spread, before / after |
| --- | ---: | ---: | ---: | ---: |
| duplex serial | 28.81 | 24.89 | **−13.60%** | 0.10% / 1.06% |
| duplex concurrency 1 | 29.84 | 25.92 | **−13.14%** | 0.24% / 0.85% |
| duplex concurrency 8 | 159.64 | 130.41 | **−18.31%** | 1.06% / 0.43% |
| duplex concurrency 64 | 1265.59 | 1038.31 | **−17.96%** | 0.18% / 1.12% |
| socket serial | 54.42 | 49.31 | **−9.40%** | 1.12% / 0.33% |
| socket concurrency 1 | 56.23 | 52.21 | **−7.14%** | 1.27% / 0.65% |
| socket concurrency 8 | 236.48 | 207.41 | **−12.29%** | 0.75% / 0.29% |
| socket concurrency 64 | 1733.89 | 1515.49 | **−12.60%** | 2.91% / 0.11% |

## Drift controls

Only the matching HTTP/2 readiness arm is shown where the socket suite also carries compio.

| Control | movement |
| --- | ---: |
| duplex serial | +0.50% |
| duplex concurrency 1 / 8 / 64 | +2.55% / +1.15% / +1.12% |
| socket serial | −1.69% |
| socket concurrency 1 / 8 / 64 | +2.47% / +0.31% / +0.69% |

## What this establishes

- The valid constant-time implementation realizes the diagnostic's predicted gain without
  reducing the 1,024-entry retention window.
- The gain is **13–18% on the duplex arms** and **7–13% on the socket arms** tested.
- Concurrency benefits more than serial operation, consistent with one removed scan per stream
  close rather than a per-byte effect.
- Every result is at least 4.6 percentage points beyond its matching control and at least
  4.2 percentage points beyond the tested arm's larger within-side spread.

## What it does not

- Body-throughput arms were not run. The mechanism is per stream close, so a body-size sweep
  would mostly amortize the same fixed saving rather than test a new claim.
- This is loopback, tokio and a current-thread runtime. It does not measure a real network,
  compio or the QUIC join.
- The control series followed the QMux series rather than sharing each Criterion process.
  Both were interleaved across the same two commits and pinned to the same core, but session
  drift therefore remains visible rather than cancelling within a pass.
