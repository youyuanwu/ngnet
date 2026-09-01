# 32 — Hyperium QMux adapter progress ownership

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-09-01
**Candidate:** `bed0402` (driver-only lower I/O)
**Baseline:** `8d0479b` (bounded opportunistic handle help)
**Workloads:** empty body, 1,000 exchanges; 1 MiB body, 100 exchanges
**Method:** three pinned repetitions, alternating baseline then candidate in each pass

## Question and controls

This A/B asks only whether `h3-ngnet-qmux` should retain driver-only lower-I/O
ownership. Both revisions use the same fixed-`Bytes` adapter, QMux configuration,
one combined adapter/H3 task per endpoint, warm-up, request/echo/drain workload,
and bench-local lower-I/O counters. The ngnet H3 arm is rerun beside each revision
as an unchanged drift control. This does not compare or tune ngnet H3.

Each revision was checked out, built in release mode, and run on CPU 0. Within
each of three passes, duplex and loopback-socket empty/body probes ran for the
ngnet control and hyperium candidate:

```sh
for pass in 1 2 3; do
  git checkout --detach 8d0479b
  cargo build -p ngnet-bench --example probe --release
  for arm in ngnet-qmux-matched-duplex h3-qmux-duplex \
             ngnet-qmux-matched-socket h3-qmux-socket; do
    taskset -c 0 target/release/examples/probe "$arm" body 0 1000 timing
    taskset -c 0 target/release/examples/probe "$arm" body 1048576 100 timing
  done

  git checkout feature/h3-ngnet-qmux
  cargo build -p ngnet-bench --example probe --release
  for arm in ngnet-qmux-matched-duplex h3-qmux-duplex \
             ngnet-qmux-matched-socket h3-qmux-socket; do
    taskset -c 0 target/release/examples/probe "$arm" body 0 1000 timing
    taskset -c 0 target/release/examples/probe "$arm" body 1048576 100 timing
  done
done
```

## Timing results

Values are the median elapsed milliseconds across three runs. Delta is
driver-only relative to opportunistic; negative is faster.

| Baseline pass | ngnet duplex empty / 1 MiB | hyperium duplex empty / 1 MiB | ngnet socket empty / 1 MiB | hyperium socket empty / 1 MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 20.202 / 82.398 | 26.474 / 160.074 | 32.835 / 140.822 | 36.425 / 315.454 |
| 2 | 19.383 / 66.669 | 24.046 / 151.022 | 34.321 / 131.697 | 31.583 / 334.251 |
| 3 | 19.121 / 66.409 | 19.604 / 143.861 | 33.487 / 147.389 | 30.204 / 321.352 |

| Driver-only pass | ngnet duplex empty / 1 MiB | hyperium duplex empty / 1 MiB | ngnet socket empty / 1 MiB | hyperium socket empty / 1 MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 19.469 / 69.246 | 23.004 / 61.732 | 35.525 / 132.011 | 27.959 / 134.291 |
| 2 | 22.602 / 67.217 | 16.704 / 59.749 | 33.413 / 142.508 | 32.514 / 126.896 |
| 3 | 19.127 / 76.132 | 19.880 / 59.878 | 37.324 / 143.409 | 38.668 / 126.045 |

| Substrate / workload | Stack | Opportunistic | Driver-only | Delta |
| --- | --- | ---: | ---: | ---: |
| duplex empty | ngnet control | 19.383 | 19.469 | +0.4% |
| duplex empty | hyperium adapter | 24.046 | 19.880 | −17.3% |
| duplex 1 MiB | ngnet control | 66.669 | 69.246 | +3.9% |
| duplex 1 MiB | hyperium adapter | 151.022 | 59.878 | −60.4% |
| socket empty | ngnet control | 33.487 | 35.525 | +6.1% |
| socket empty | hyperium adapter | 31.583 | 32.514 | +2.9% |
| socket 1 MiB | ngnet control | 140.822 | 142.508 | +1.2% |
| socket 1 MiB | hyperium adapter | 321.352 | 126.896 | −60.5% |

Empty-workload movement is within the controls' direction-changing spread and
does not establish an improvement. The 1 MiB candidate change is much larger
than the unchanged-control movement on both substrates and has the same sign in
all six candidate runs. It is evidence for the ownership decision, not a general
claim that either complete H3 stack wins.

## Symmetric counter check

One exact diagnostic exchange per arm used only the common bench-local wrapper.
The values aggregate both endpoints of one fixture.

| Substrate / stack / body | Revision | reads | read bytes | writes | write bytes | endpoint polls |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| duplex ngnet / empty | both | 46 | 85 | 2 | 85 | 5 |
| duplex hyperium / empty | opportunistic | 35 | 109 | 2 | 109 | 5 |
| duplex hyperium / empty | driver-only | 7 | 109 | 2 | 109 | 7 |
| duplex ngnet / 1 MiB | both | 849 | 2,099,551 | 66 | 2,099,551 | 102 |
| duplex hyperium / 1 MiB | opportunistic | 2,728 | 2,100,156 | 75 | 2,100,156 | 1,177 |
| duplex hyperium / 1 MiB | driver-only | 311 | 2,099,823 | 70 | 2,099,823 | 326 |
| socket ngnet / empty | both | 46 | 85 | 2 | 85 | 5 |
| socket hyperium / empty | opportunistic | 35 | 109 | 2 | 109 | 5 |
| socket hyperium / empty | driver-only | 7 | 109 | 2 | 109 | 7 |
| socket ngnet / 1 MiB | both | 956 | 2,099,551 | 66 | 2,099,551 | 149 |
| socket hyperium / 1 MiB | opportunistic | 7,114 | 2,100,149 | 75 | 2,100,156 | 3,371 |
| socket hyperium / 1 MiB | driver-only | 311 | 2,099,823 | 70 | 2,099,823 | 326 |

All intervals completed exact request/echo/drain work without overflow or lower
write refusal. The changed byte totals reflect different H3/QMux record
boundaries under the new scheduling rule, so the timing comparison remains for
the complete hyperium-H3-plus-adapter pair.

## Decision

Retain driver-only lower-I/O ownership. Deterministic first-flight,
fragmentation, flow-control, output-ceiling, close-tail, failure-fan-out,
independent-waiter, no-spin, and 64-event fairness tests all pass. The candidate
also clears the plan's A/B gate on both 1 MiB substrates. No bounded
opportunistic fallback is needed.
