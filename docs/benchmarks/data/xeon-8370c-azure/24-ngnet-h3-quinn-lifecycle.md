# 24 — Does bounded Quinn lifecycle cleanup remove connection-history cost?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8370C
**Date:** 2026-08-29
**Baseline:** `7da1866` (Phase 1 harness only)
**Candidate:** `6335077` (bounded lifecycle and review fixes)
**CPU / system:** CPU 3, Linux 7.0.0-1012-azure, rustc 1.98.0
**Cases:** `quinn_serial_latency`, `quinn_body_throughput`, and the checked-in Quinn probe
**Criterion command:** `taskset -c 3 cargo bench -p ngnet-bench --bench
quinn_serial_latency --bench quinn_body_throughput -- --warm-up-time 1
--measurement-time 3 --sample-size 30 --noplot`
**Probe command:** `taskset -c 3 ./target/release/examples/probe <arm> body 0
<1000|10000|50000>`
**Repetitions:** three interleaved baseline/candidate passes; adjacent upstream `h3-quinn`
arm retained as drift control
**RSS:** `/proc/<probe-pid>/status` sampled every 10 ms after `PROBE-READY`; supplemental
`/usr/bin/time -v` for the 50,000-exchange run
**Exclusions:** hardware/function profiling was unavailable because the host has
`perf_event_paranoid=4`; no timed repetition was discarded

Run 23 used a migrated 8573C host despite this directory's historical label. Its absolute
values are context only. Every conclusion below uses the fresh baseline and candidate built
from the same source tree and interleaved on the current 8370C host.

## Criterion results

Criterion median time per complete request/response exchange. Lower is better.

| Case | Revision | Pass 1 | Pass 2 | Pass 3 | Median |
| --- | --- | ---: | ---: | ---: | ---: |
| empty serial | baseline | 105.230 µs | 108.190 µs | 106.870 µs | **106.870 µs** |
| empty serial | lifecycle | 79.188 µs | 77.992 µs | 77.858 µs | **77.992 µs** |
| empty serial control | baseline | 39.038 µs | 40.066 µs | 40.046 µs | **40.046 µs** |
| empty serial control | lifecycle | 40.487 µs | 41.557 µs | 40.449 µs | **40.487 µs** |
| 16 KiB echo | baseline | 173.270 µs | 173.620 µs | 174.240 µs | **173.620 µs** |
| 16 KiB echo | lifecycle | 163.240 µs | 164.890 µs | 158.930 µs | **163.240 µs** |
| 16 KiB control | baseline | 105.430 µs | 107.420 µs | 110.540 µs | **107.420 µs** |
| 16 KiB control | lifecycle | 111.060 µs | 105.630 µs | 106.760 µs | **106.760 µs** |
| 1 MiB echo | baseline | 6.2205 ms | 6.3037 ms | 6.1804 ms | **6.2205 ms** |
| 1 MiB echo | lifecycle | 6.2546 ms | 5.1614 ms | 5.1437 ms | **5.1614 ms** |
| 1 MiB control | baseline | 4.5539 ms | 4.6302 ms | 4.5862 ms | **4.5862 ms** |
| 1 MiB control | lifecycle | 4.8057 ms | 4.8650 ms | 4.5972 ms | **4.8057 ms** |

Adjusting the baseline-to-candidate ratio by the matching unchanged-arm ratio gives:

| Case | Raw lifecycle change | Control-adjusted change | Gate |
| --- | ---: | ---: | --- |
| empty serial | −27.02% | **−27.82%** | measured and attributed to removing aged history |
| 16 KiB echo | −5.98% | **−5.40%** | pass: no regression over 5% |
| 1 MiB echo | −17.03% | **−20.82%** | pass: no regression over 5% |

## Probe latency and resident memory

Each cell lists the three post-ready mean latencies or sampled peak `VmRSS` values, followed
by their median.

| Exchanges | Revision | µs/exchange repetitions → median | peak RSS KiB repetitions → median |
| ---: | --- | --- | --- |
| 1,000 | baseline | 91.766, 81.273, 81.540 → **81.540** | 10,400, 10,256, 10,236 → **10,256** |
| 1,000 | lifecycle | 92.390, 81.258, 81.246 → **81.258** | 7,296, 7,228, 7,132 → **7,228** |
| 10,000 | baseline | 79.588, 79.331, 79.571 → **79.571** | 40,824, 40,772, 41,184 → **40,824** |
| 10,000 | lifecycle | 79.773, 79.596, 79.688 → **79.688** | 7,196, 7,112, 7,184 → **7,184** |
| 50,000 | baseline | 98.530, 96.747, 97.341 → **97.341** | 174,956, 174,988, 174,964 → **174,964** |
| 50,000 | lifecycle | 79.863, 79.355, 78.661 → **79.355** | 7,208, 7,244, 7,292 → **7,244** |

The lifecycle 50,000-exchange median is 2.34% below its 1,000-exchange median, within the
10% stability gate. Its long-window median peak is only 16 KiB above the short-window median,
well inside the larger-of-25%-or-16-MiB gate and below twice the 10,000-exchange peak.
Supplemental whole-process maxima were 174,980 KiB for baseline and 7,236 KiB for lifecycle.

The upstream probe remained bounded at approximately 6.7 MiB. Its median latency was
50.874/40.508/39.466 µs for baseline-source runs and 40.954/40.897/39.341 µs for
candidate-source runs at 1,000/10,000/50,000 exchanges; this is context for shared-host drift,
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
  the connection-history RSS growth: the 50,000-exchange median fell from 174,964 to
  7,244 KiB with flat short/long candidate peaks.
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
