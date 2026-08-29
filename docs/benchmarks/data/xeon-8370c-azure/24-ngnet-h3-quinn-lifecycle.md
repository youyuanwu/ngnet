# 24 — Does bounded Quinn lifecycle cleanup remove connection-history cost?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8370C
**Date:** 2026-08-29
**Baseline:** `7da1866` (Phase 1 harness only)
**Candidate:** `7d5057a` (bounded lifecycle plus final review fixes)
**CPU / system:** CPU 3, Linux 7.0.0-1012-azure, rustc 1.98.0
**Cases:** `quinn_serial_latency`, `quinn_body_throughput`, and the checked-in Quinn probe
**Criterion command:** `taskset -c 3 cargo bench -p ngnet-bench --bench
quinn_serial_latency --bench quinn_body_throughput -- --warm-up-time 1
--measurement-time 3 --sample-size 30 --noplot`
**Probe command:** `taskset -c 3 ./target/release/examples/probe <arm> body 0
<1250|12500|62500>`
**Repetitions:** three interleaved baseline/candidate passes; adjacent upstream `h3-quinn`
arm retained as drift control
**RSS:** `/proc/<probe-pid>/status` sampled every 10 ms after `PROBE-READY`; supplemental
`/usr/bin/time -v` for the 62,500-exchange run
**Sampler:** a Python 3 parent starts the command with `subprocess.Popen`, blocks on the
stderr `PROBE-READY` line, records `time.perf_counter()`, reads `VmRSS` and `VmHWM` from
`/proc/<pid>/status`, sleeps 10 ms, and repeats until exit; elapsed time divided by the
explicit iteration count is the repetition mean
**Otherwise idle:** yes; no build, test, or other benchmark ran concurrently
**Exclusions:** hardware/function profiling was unavailable because the host has
`perf_event_paranoid=4`; no timed repetition was discarded

Run 23 used a migrated 8573C host despite this directory's historical label. Its absolute
values are context only. Every conclusion below uses the fresh baseline and candidate built
from the same source tree and interleaved on the current 8370C host.

## Criterion results

Criterion median time per complete request/response exchange. Lower is better.

| Case | Revision | Pass 1 | Pass 2 | Pass 3 | Median |
| --- | --- | ---: | ---: | ---: | ---: |
| empty serial | baseline | 107.940 µs | 117.580 µs | 117.290 µs | **117.290 µs** |
| empty serial | lifecycle | 81.395 µs | 80.178 µs | 80.830 µs | **80.830 µs** |
| empty serial control | baseline | 42.461 µs | 41.481 µs | 40.534 µs | **41.481 µs** |
| empty serial control | lifecycle | 41.290 µs | 41.719 µs | 41.958 µs | **41.719 µs** |
| 16 KiB echo | baseline | 195.000 µs | 179.350 µs | 180.610 µs | **180.610 µs** |
| 16 KiB echo | lifecycle | 166.860 µs | 168.140 µs | 160.560 µs | **166.860 µs** |
| 16 KiB control | baseline | 124.590 µs | 113.440 µs | 107.860 µs | **113.440 µs** |
| 16 KiB control | lifecycle | 110.860 µs | 113.070 µs | 109.500 µs | **110.860 µs** |
| 1 MiB echo | baseline | 6.7610 ms | 6.0901 ms | 6.2450 ms | **6.2450 ms** |
| 1 MiB echo | lifecycle | 5.2254 ms | 5.2332 ms | 5.2688 ms | **5.2332 ms** |
| 1 MiB control | baseline | 5.1002 ms | 4.7967 ms | 4.6006 ms | **4.7967 ms** |
| 1 MiB control | lifecycle | 5.0463 ms | 5.1064 ms | 4.9054 ms | **5.0463 ms** |

Each lifecycle/baseline pair is normalized by its matching interleaved unchanged-arm pair;
the reported adjusted change is the median of those three paired ratios:

| Case | Raw median change | Adjusted passes | Adjusted median | Gate |
| --- | ---: | --- | ---: | --- |
| empty serial | −31.09% | −22.45%, −32.20%, −33.42% | **−32.20%** | measured and attributed to removing aged history |
| 16 KiB echo | −7.61% | −3.83%, −5.94%, −12.43% | **−5.94%** | pass: no regression over 5% |
| 1 MiB echo | −16.20% | −21.89%, −19.28%, −20.87% | **−20.87%** | pass: no regression over 5% |

## Probe latency and resident memory

Each cell lists the three post-ready mean latencies or sampled peak `VmRSS` values, followed
by their median.

| Exchanges | Revision | µs/exchange repetitions → median | peak RSS KiB repetitions → median |
| ---: | --- | --- | --- |
| 1,250 | baseline | 98.697, 89.525, 99.136 → **98.697** | 11,120, 10,964, 11,048 → **11,048** |
| 1,250 | lifecycle | 97.738, 89.584, 89.553 → **89.584** | 7,220, 7,156, 7,192 → **7,192** |
| 12,500 | baseline | 81.608, 84.849, 82.971 → **82.971** | 48,688, 48,688, 48,596 → **48,688** |
| 12,500 | lifecycle | 83.222, 82.118, 83.870 → **83.222** | 7,212, 7,272, 7,320 → **7,272** |
| 62,500 | baseline | 107.235, 105.761, 105.325 → **105.761** | 223,996, 223,984, 223,972 → **223,984** |
| 62,500 | lifecycle | 82.083, 81.272, 81.961 → **81.961** | 7,304, 7,256, 7,196 → **7,256** |

The lifecycle median elapsed windows were approximately 0.112, 1.040, and 5.123 seconds.
The same fixed counts intentionally took longer on the degraded aged baseline, reaching
approximately 0.123, 1.037, and 6.610 seconds at its medians.

The lifecycle 62,500-exchange median is 8.51% below its 1,250-exchange median, within the
10% stability gate. Its long-window median peak is only 64 KiB above the short-window median,
well inside the larger-of-25%-or-16-MiB gate and below twice the 12,500-exchange peak.
Supplemental whole-process maxima were 223,848 KiB for baseline and 7,104 KiB for lifecycle.

The upstream probe remained bounded at approximately 6.6 MiB. Its median latency was
48.902/41.524/41.968 µs for baseline-source runs and 48.671/41.696/40.762 µs for
candidate-source runs at 1,250/12,500/62,500 exchanges; this is context for shared-host drift,
not an implementation difference.

## Adjacent-release candidate

Adjacent same-stream immediate-release coalescing was implemented with an invariant test for
additive totals, stream isolation, `delivered=true`, release-before-close, and exactly-once
accounting. It was measured in the sequence candidate/control/candidate/control:

| Pair | Lifecycle empty / control | Coalesced empty / control | Ratio-adjusted change |
| ---: | ---: | ---: | ---: |
| 1 | 76.845 / 41.299 µs | 78.564 / 39.559 µs | **+6.73%** |
| 2 | 77.736 / 40.384 µs | 78.274 / 40.663 µs | **+0.001%** |

The candidate did not improve empty serial by the required 3% beyond drift and was reverted.
Body results were also noisy rather than a reason to override that gate: adjusted directions
changed sign between pairs. No coalescing source or test remains in the final revision.

## Profile evidence

Linux denied `perf` events at `perf_event_paranoid=4`. A scoped
`taskset -c 3 strace -f -c` run over 10,000 lifecycle exchanges reported 50.44% of observed
syscall time in 45,927 `sendmsg` calls, 34.40% in 67,848 `recvmmsg` calls, and 14.22% in
30,010 `epoll_wait` calls. That is syscall attribution only; it cannot assign user-space
driver, registry, FFI, write-slice, or task costs.

## What this establishes

- Completed bidirectional lifecycle state, not the checked-in probe or upstream stack, caused
  the connection-history RSS growth: the 62,500-exchange median fell from 223,984 to
  7,256 KiB with flat short/long candidate peaks.
- Aged serial latency is stable after cleanup, and the fresh Criterion matrix shows a large
  empty-serial improvement after unchanged-arm adjustment.
- Neither measured body case regressed. The lifecycle correction is accepted independently
  of optimization experiments.
- Immediate-release coalescing failed its predeclared gate and was correctly reverted.

## What it does not

- It does not compare absolute values with run 23 across the Azure CPU migration.
- It does not provide function-level CPU attribution; `perf` was unavailable.
- It says nothing about packet loss, internet latency, tail latency, multi-connection or
  multi-core scaling.
- It does not justify `ngnet-h3` scratch reuse, multi-slice Quinn writes, or reader pooling.
  Those need separate profiles, invariant tests, and plans.
