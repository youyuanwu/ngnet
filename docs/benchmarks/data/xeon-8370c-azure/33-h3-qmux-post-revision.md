# 33 — Post-revision hyperium and ngnet H3 over QMux

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-09-01
**Commit:** `d8f0bec`
**Cases:** equal-topology duplex/socket empty latency and body throughput
**Controls:** one task per endpoint, common QMux configuration and lower seam,
per-fixture symmetric counters, explicit empty warm-up

## Scope

Both arms use the same QMux implementation, byte stream, Tokio current-thread
runtime, request/echo/drain workload, and pending/concurrency limit. The
comparison still varies the complete HTTP implementation plus adapter:
`ngnet-h3 + ngnet-qmux-h3` versus `hyperium h3 + h3-ngnet-qmux`. It does not
attribute a timing difference to either adapter alone.

Run 31 is historical: it used unequal endpoint task topology and removed
process-global adapter instrumentation. Run 32 separately records the decision
to retain driver-only lower-I/O ownership in `h3-ngnet-qmux`.

## Commands

```sh
cargo bench -p ngnet-bench --bench qmux_h3_serial_latency -- \
  --sample-size 20 --measurement-time 2 --warm-up-time 1
cargo bench -p ngnet-bench --bench qmux_h3_socket_serial_latency -- \
  --sample-size 20 --measurement-time 2 --warm-up-time 1
cargo bench -p ngnet-bench --bench qmux_h3_body_throughput -- \
  --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench -p ngnet-bench --bench qmux_h3_socket_body_throughput -- \
  --sample-size 10 --measurement-time 1 --warm-up-time 1

cargo build -p ngnet-bench --example probe --release
taskset -c 0 target/release/examples/probe <arm> body 0 1000 timing
taskset -c 0 target/release/examples/probe <arm> body 1048576 100 timing
taskset -c 0 target/release/examples/probe <arm> body <bytes> 1 diagnostic
```

Five pinned timing passes alternated arm order. No sample or Criterion outlier
was excluded.

## Criterion survey

Times are point estimates with 95% confidence intervals. Throughput counts the
request plus echoed response. Lower time is better.

| Substrate / body | ngnet H3 | hyperium H3 |
| --- | ---: | ---: |
| duplex serial empty | 20.100 µs [19.499, 20.700] | 15.559 µs [15.188, 15.931] |
| duplex body empty | 19.348 µs [19.228, 19.509] | 15.497 µs [15.066, 16.547] |
| duplex 1 KiB | 23.286 µs; 83.87 MiB/s | 18.859 µs; 103.57 MiB/s |
| duplex 64 KiB | 62.173 µs; 1.963 GiB/s | 55.154 µs; 2.213 GiB/s |
| duplex 1 MiB | 679.43 µs; 2.875 GiB/s | 605.80 µs; 3.224 GiB/s |
| duplex 8 MiB | 5.1383 ms; 3.041 GiB/s | 4.6649 ms; 3.350 GiB/s |
| socket serial empty | 34.574 µs [33.954, 35.239] | 28.222 µs [27.710, 28.960] |
| socket body empty | 34.406 µs [33.428, 35.553] | 30.343 µs [29.058, 31.793] |
| socket 1 KiB | 38.936 µs; 50.16 MiB/s | 32.683 µs; 59.76 MiB/s |
| socket 64 KiB | 115.30 µs; 1.059 GiB/s | 104.16 µs; 1.172 GiB/s |
| socket 1 MiB | 1.3919 ms; 1.403 GiB/s | 1.2008 ms; 1.627 GiB/s |
| socket 8 MiB | 10.718 ms; 1.458 GiB/s | 9.1347 ms; 1.711 GiB/s |

Criterion reported high-severe outliers in seven identifiers, including both
socket serial arms and the ngnet 1 MiB/8 MiB duplex cases. The survey's
consistent sign therefore needs the fixed-count drift check below.

## Pinned interleaved probes

Each value is elapsed milliseconds for 1,000 empty or 100 complete 1 MiB
exchanges.

| Pass | ngnet duplex empty | hyperium duplex empty | ngnet duplex 1 MiB | hyperium duplex 1 MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 19.833 | 16.209 | 66.761 | 60.547 |
| 2 | 22.654 | 16.072 | 66.469 | 61.384 |
| 3 | 24.087 | 24.591 | 70.254 | 67.369 |
| 4 | 50.008 | 15.978 | 88.624 | 90.438 |
| 5 | 20.476 | 24.403 | 76.376 | 65.444 |
| median | 22.654 | 16.209 | 70.254 | 65.444 |
| range | 19.833–50.008 | 15.978–24.591 | 66.469–88.624 | 60.547–90.438 |

| Pass | ngnet socket empty | hyperium socket empty | ngnet socket 1 MiB | hyperium socket 1 MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 34.987 | 30.803 | 130.436 | 154.420 |
| 2 | 37.030 | 28.240 | 135.462 | 130.400 |
| 3 | 33.402 | 27.920 | 150.846 | 128.160 |
| 4 | 51.911 | 28.179 | 141.757 | 159.995 |
| 5 | 33.291 | 35.184 | 141.074 | 122.929 |
| median | 34.987 | 28.240 | 141.074 | 130.400 |
| range | 33.291–51.911 | 27.920–35.184 | 130.436–150.846 | 122.929–159.995 |

Hyperium's median is lower by 28.4%/6.8% on duplex empty/body and
19.3%/7.6% on socket empty/body. Every pair of within-arm ranges overlaps,
including a direction reversal in socket-body pass 1. Shared-host drift is
therefore large enough that these probes do not establish a stable winner.

## Symmetric counter intervals

One exact exchange per arm was sampled after warm-up. Counters aggregate both
endpoints of one fixture and were applied identically to both adapters.

| Substrate / stack / body | reads | read bytes | writes | write bytes | endpoint polls |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex ngnet / empty | 46 | 85 | 2 | 85 | 5 |
| duplex hyperium / empty | 7 | 109 | 2 | 109 | 7 |
| duplex ngnet / 1 MiB | 849 | 2,099,551 | 66 | 2,099,551 | 102 |
| duplex hyperium / 1 MiB | 311 | 2,099,823 | 70 | 2,099,823 | 326 |
| socket ngnet / empty | 46 | 85 | 2 | 85 | 5 |
| socket hyperium / empty | 7 | 109 | 2 | 109 | 7 |
| socket ngnet / 1 MiB | 956 | 2,099,551 | 66 | 2,099,551 | 149 |
| socket hyperium / 1 MiB | 311 | 2,099,823 | 70 | 2,099,823 | 326 |

All intervals completed exact request/echo/drain work with zero write refusal
and no counter overflow. The different transport byte and call counts are
observations of the complete H3-plus-adapter pairs, not proof of where the
difference originates.

## Verdict

The revised fixtures are topology- and instrumentation-symmetric, and all
Criterion point estimates favor the hyperium pair on this run. The pinned
within-arm ranges overlap in every matched case and include substantial shared
host drift. The responsible whole-stack verdict remains **inconclusive**: no
stable winner is claimed.
