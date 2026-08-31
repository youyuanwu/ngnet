# 31 — Hyperium H3 and ngnet H3 over the same QMux transport

**Machine:** [`xeon-8370c-azure`](README.md)  
**Date:** 2026-08-31  
**Commit:** `87a2cff67af74bb8ee130e9ff97eec4fff8f1901`  
**Cases:** QMux duplex/socket serial latency and body throughput; focused adapter diagnostics  
**Repetitions:** three pinned, interleaved fixed-count timing rounds at 1 MiB; Criterion
surveys used 20 serial samples or 10 body samples  
**Exclusions:** no numerical sample was excluded. Criterion-reported outliers remain in its
confidence intervals. The unpinned Criterion survey is context, not the controlled verdict.

## Environment

- Intel Xeon Platinum 8370C, 4 cores / 8 threads, Microsoft Azure hypervisor.
- Linux `7.0.0-1012-azure`, x86_64, 31 GiB RAM.
- `rustc 1.98.0 (88d9e12ae 2026-08-18)`, Cargo 1.98.0.
- Gnuplot unavailable; Criterion used plotters.
- Pinned fixed-count probes used CPU 0. The shared VM was not otherwise controllable; host
  neighbours, turbo state, and frequency drift remain unknown.

## Question and controls

This run asks how complete `ngnet-h3 + ngnet-qmux-h3` and
`hyperium h3 + h3-ngnet-qmux` stacks compare when both use the same QMux implementation,
transport configuration, request/echo/drain workload, Tokio current-thread runtime, and either
the same Tokio duplex or the same loopback TCP helper.

| Control | Both arms |
| --- | --- |
| QMux stream / connection window | 65,535 / 65,535 bytes |
| QMux read-ahead | 65,535 bytes |
| QMux bidi / uni lifetime allowance | `2^40` / 16 |
| Pending/concurrent request policy | 128 |
| Substrate | `duplex(1 MiB)` or common `tokio_socket_pair()` with `TCP_NODELAY` |
| Request | POST `http://bench.local/bench`, content-type plus `x-bench: 1` |
| Response | 200, same content type, exact body echo and complete drain |
| H3 field section | 64 KiB |
| GREASE | disabled on hyperium; ngnet exposes no matching toggle |
| QPACK dynamic table | ngnet matched fixture set to zero; hyperium 0.0.8 exposes no control |
| Setup | connection construction and one explicit empty warm-up excluded |
| Measured bytes | request plus echoed response (`2 × body size`) |

Hyperium clones its `SendRequest` inside each round trip; the ngnet fixture's handle API does
not require that clone. This small measured asymmetry is disclosed rather than attributed.

## Exact commands

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
# Repeated three times in this order:
taskset -c 0 target/release/examples/probe ngnet-qmux-matched-duplex body 1048576 100 timing
taskset -c 0 target/release/examples/probe h3-qmux-duplex body 1048576 100 timing
taskset -c 0 target/release/examples/probe ngnet-qmux-matched-socket body 1048576 100 timing
taskset -c 0 target/release/examples/probe h3-qmux-socket body 1048576 100 timing

cargo build -p ngnet-bench --example probe --release --features diagnostics
taskset -c 0 target/release/examples/probe h3-qmux-duplex body 1048576 1 diagnostic
taskset -c 0 target/release/examples/probe h3-qmux-socket body 1048576 1 diagnostic
```

## Criterion survey

Times are Criterion point estimates with 95% confidence intervals; lower is better. Throughput
is derived from the same interval and counts request plus response bytes.

| Substrate / body | ngnet H3 | hyperium H3 |
| --- | ---: | ---: |
| duplex serial empty | 19.921 µs [19.109, 20.999] | 18.220 µs [17.919, 18.580] |
| duplex 1 KiB | 21.822 µs; 89.50 MiB/s | 23.147 µs; 84.38 MiB/s |
| duplex 64 KiB | 98.338 µs; 1.241 GiB/s | 62.501 µs; 1.953 GiB/s |
| duplex 1 MiB | 675.45 µs; 2.892 GiB/s | 600.71 µs; 3.251 GiB/s |
| duplex 8 MiB | 5.1997 ms; 3.005 GiB/s | 4.4576 ms; 3.505 GiB/s |
| socket serial empty | 49.341 µs [44.846, 52.887] | 46.709 µs [43.490, 50.493] |
| socket 1 KiB | 39.722 µs; 49.17 MiB/s | 54.469 µs; 35.86 MiB/s |
| socket 64 KiB | 110.47 µs; 1.105 GiB/s | 162.06 µs; 771.3 MiB/s |
| socket 1 MiB | 1.2902 ms; 1.514 GiB/s | 1.4776 ms; 1.322 GiB/s |
| socket 8 MiB | 10.656 ms; 1.466 GiB/s | 11.988 ms; 1.303 GiB/s |

The sign changes by substrate: hyperium is faster on the larger duplex bodies while ngnet is
faster on the larger socket bodies. This is whole-stack evidence only.

## Pinned, interleaved 1 MiB probes

Each value is elapsed milliseconds for 100 complete exchanges.

| Round | ngnet duplex | hyperium duplex | ngnet socket | hyperium socket |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 62.657 | 58.318 | 152.509 | 140.557 |
| 2 | 62.068 | 60.181 | 131.841 | 140.603 |
| 3 | 73.616 | 61.593 | 137.158 | 153.217 |
| median | 62.657 | 60.181 | 137.158 | 140.603 |

The median hyperium delta is −3.95% on duplex and +2.51% on sockets. Those deltas are smaller
than the within-arm ranges on this shared VM (5.6–18.6%), so the controlled result is
**inconclusive/noisy**, not a stable winner.

## Focused adapter diagnostics

Diagnostics were compiled and armed only after `PROBE-READY`; default timing probes were rebuilt
without the feature. Both substrates produced the same one-exchange 1 MiB structural counts:

| Counter | duplex | socket |
| --- | ---: | ---: |
| lower read calls / bytes | 399 / 2,100,021 | 399 / 2,100,021 |
| lower write calls / bytes | 190 / 2,100,028 | 190 / 2,100,028 |
| adapter polls / driver polls | 251 / 148 | 251 / 148 |
| pump attempts / productive turns | 399 / 185 | 399 / 185 |
| routed events | 237 | 237 |
| stream / connection credit applications | 160 / 160 | 160 / 160 |
| waiter registrations / delivered wakes | 74 / 74 | 74 / 74 |
| retained send high-water | 1,048,581 bytes | 1,048,581 bytes |
| retained receive high-water | 65,439 bytes | 65,439 bytes |
| final retained send / receive | 0 / 0 | 0 / 0 |
| overflow / lower failures | false / 0 | false / 0 |

These counters describe only `h3-ngnet-qmux`; the baseline exposes no matched internal counters.
They therefore support adapter invariants and candidate generation, not a numerical attribution
against `ngnet-qmux-h3`. The retained-send high-water is the caller-owned 1 MiB body plus H3
framing, not a second body-sized copy. The receive high-water remains within the configured
connection window.

## What this establishes

- All eight matched combinations execute complete request/echo/drain work.
- The result's sign differs between duplex and sockets.
- The controlled 1 MiB repetitions are too noisy to name a stable whole-stack winner.
- Armed adapter counters reconcile credit applications, wake delivery, final gauges, and exact
  application completion without overflow.

## What this does not establish

- It does not attribute a timing difference to the adapter, QPACK, H3 state machine, scheduler,
  or lower system calls.
- It does not compare adapter counters numerically with the baseline.
- It does not turn the unpinned Criterion survey into a controlled A/B result.
- It does not justify header/payload coalescing or any optimization; the focused evidence shows
  work counts but no isolated causal timing experiment.
