# 23 — How does ngnet-h3-quinn compare with h3-quinn?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-29
**Source:** `feature/ngnet-h3-quinn` working tree based on `eeddf04`
**Cases:** `quinn_serial_latency` and `quinn_body_throughput`
**Command:** three passes of `taskset -c 3 cargo bench --quiet -p ngnet-bench
--bench quinn_serial_latency --bench quinn_body_throughput --
--warm-up-time 1 --measurement-time 3 --sample-size 30
--save-baseline quinn-<pass> --noplot`
**Repetitions:** three passes; the two arms are adjacent within each case and size
**Controls:** same Quinn 0.11.11, separate current-thread Tokio runtimes, loopback UDP, rustls,
certificate, `h3` ALPN, warmed persistent connection, request, echo response, and full drain
**Exclusions:** none

## Results

Criterion median per complete request/response exchange. The 16 KiB and 1 MiB iterations send
the body in both directions. Lower time is better.

| Case | Pass | ngnet-h3-quinn | h3-quinn | ngnet ÷ h3-quinn |
| --- | ---: | ---: | ---: | ---: |
| empty serial | 1 | 92.365 µs | 33.583 µs | 2.750× |
| empty serial | 2 | 92.729 µs | 33.001 µs | 2.810× |
| empty serial | 3 | 82.781 µs | 33.230 µs | 2.491× |
| **empty serial mean** | | **89.292 µs** | **33.271 µs** | **2.684×** |
| 16 KiB echo | 1 | 144.080 µs | 100.410 µs | 1.435× |
| 16 KiB echo | 2 | 142.180 µs | 95.837 µs | 1.484× |
| 16 KiB echo | 3 | 142.930 µs | 97.038 µs | 1.473× |
| **16 KiB mean** | | **143.063 µs** | **97.762 µs** | **1.463×** |
| 1 MiB echo | 1 | 5.0153 ms | 4.5102 ms | 1.112× |
| 1 MiB echo | 2 | 5.0188 ms | 4.4227 ms | 1.135× |
| 1 MiB echo | 3 | 4.9810 ms | 4.3709 ms | 1.140× |
| **1 MiB mean** | | **5.0050 ms** | **4.4346 ms** | **1.129×** |

The corresponding mean Criterion throughput is 218.4 MiB/s against 319.8 MiB/s at 16 KiB,
and 399.6 MiB/s against 451.1 MiB/s at 1 MiB.

## What this establishes

- On this controlled loopback workload, upstream `h3-quinn` has lower median latency in all
  three passes at every measured size.
- The gap is largest for an empty exchange: `ngnet-h3-quinn` takes 2.491–2.810× as long across
  the three passes.
- The ratio narrows as payload work dominates: 1.435–1.484× at 16 KiB and 1.112–1.140× at
  1 MiB.
- At the aggregate means, `ngnet-h3-quinn` is 31.7% lower in reported throughput at 16 KiB
  and 11.4% lower at 1 MiB.

## What it does not

- It does not identify a mechanism. The narrowing ratio is consistent with a larger fixed
  per-stream or per-event cost, but CPU profiles and allocation counts are required before
  attributing that cost.
- It is a shared Azure VM, loopback UDP, one persistent connection, and a single-thread runtime.
  It says nothing about loss, internet latency, tail latency, multi-core scaling, CPU use, or
  memory use.
- The HTTP/3 implementations do not expose identical QPACK configuration. Quinn and all
  transport/TLS settings are matched; framing and header-compression behavior are part of the
  implementations being compared.
