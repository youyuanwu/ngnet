# 24 — Does bounded Quinn lifecycle cleanup remove connection-history cost?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8370C
**Date:** 2026-08-29
**Baseline:** `7da1866` (Phase 1 harness only)
**Candidate:** `bd83f18` (bounded lifecycle plus final review fixes)
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
| empty serial | baseline | 105.810 µs | 105.760 µs | 109.980 µs | **105.810 µs** |
| empty serial | lifecycle | 78.961 µs | 78.386 µs | 78.963 µs | **78.961 µs** |
| empty serial control | baseline | 40.477 µs | 39.644 µs | 45.776 µs | **40.477 µs** |
| empty serial control | lifecycle | 40.007 µs | 39.936 µs | 39.558 µs | **39.936 µs** |
| 16 KiB echo | baseline | 179.510 µs | 174.590 µs | 176.120 µs | **176.120 µs** |
| 16 KiB echo | lifecycle | 162.300 µs | 158.750 µs | 157.960 µs | **158.750 µs** |
| 16 KiB control | baseline | 109.340 µs | 105.070 µs | 110.420 µs | **109.340 µs** |
| 16 KiB control | lifecycle | 108.900 µs | 107.430 µs | 107.350 µs | **107.430 µs** |
| 1 MiB echo | baseline | 6.0232 ms | 6.0982 ms | 5.9869 ms | **6.0232 ms** |
| 1 MiB echo | lifecycle | 6.0888 ms | 5.4089 ms | 6.0392 ms | **6.0392 ms** |
| 1 MiB control | baseline | 4.5591 ms | 4.5411 ms | 4.5273 ms | **4.5411 ms** |
| 1 MiB control | lifecycle | 4.8599 ms | 4.8964 ms | 4.7911 ms | **4.8599 ms** |

Each lifecycle/baseline pair is normalized by its matching interleaved unchanged-arm pair;
the reported adjusted change is the median of those three paired ratios:

| Case | Raw median change | Adjusted passes | Adjusted median | Gate |
| --- | ---: | --- | ---: | --- |
| empty serial | −25.38% | −24.50%, −26.43%, −16.92% | **−24.50%** | measured and attributed to removing aged history |
| 16 KiB echo | −9.86% | −9.22%, −11.07%, −7.75% | **−9.22%** | pass: no regression over 5% |
| 1 MiB echo | +0.27% | −5.17%, −17.74%, −4.68% | **−5.17%** | pass: no regression over 5% |

## Probe latency and resident memory

Each cell lists the three post-ready mean latencies or sampled peak `VmRSS` values, followed
by their median.

| Exchanges | Revision | µs/exchange repetitions → median | peak RSS KiB repetitions → median |
| ---: | --- | --- | --- |
| 1,250 | baseline | 89.309, 89.649, 89.681 → **89.649** | 10,960, 11,244, 10,952 → **10,960** |
| 1,250 | lifecycle | 90.620, 81.233, 81.188 → **81.233** | 7,216, 7,076, 7,244 → **7,216** |
| 12,500 | baseline | 82.738, 81.212, 80.525 → **81.212** | 48,856, 48,852, 48,672 → **48,852** |
| 12,500 | lifecycle | 78.991, 79.768, 81.630 → **79.768** | 7,212, 7,248, 7,256 → **7,248** |
| 62,500 | baseline | 102.687, 102.527, 103.611 → **102.687** | 224,000, 223,968, 223,944 → **223,968** |
| 62,500 | lifecycle | 80.216, 79.588, 83.686 → **80.216** | 7,300, 7,268, 7,340 → **7,300** |

The lifecycle median elapsed windows were approximately 0.102, 0.997, and 5.014 seconds.
The same fixed counts intentionally took longer on the degraded aged baseline, reaching
approximately 0.112, 1.015, and 6.418 seconds at its medians.

The lifecycle 62,500-exchange median is 1.25% below its 1,250-exchange median, within the
10% stability gate. Its long-window median peak is only 84 KiB above the short-window median,
well inside the larger-of-25%-or-16-MiB gate and below twice the 12,500-exchange peak.
Supplemental whole-process maxima were 223,812 KiB for baseline and 7,200 KiB for lifecycle.

The upstream probe remained bounded at approximately 6.6 MiB. Its median latency was
48.790/40.640/40.097 µs for baseline-source runs and 40.602/40.841/40.258 µs for
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
  the connection-history RSS growth: the 62,500-exchange median fell from 223,968 to
  7,300 KiB with flat short/long candidate peaks.
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
