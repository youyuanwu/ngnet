# 11 — Decoupling QMux flushes from HTTP/3 event batches

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-28
**Commit(s):** `736b460` before, `b6c76d6` after
**Cases:** the HTTP/2 and QMux/H3 arms in `serial_latency`,
`concurrent_throughput`, `transport_serial_latency`, and
`transport_concurrent_throughput`, with concurrent parameters 1, 8, and 64
**Command:** exact-revision bench binaries were built from separate checkouts, preserved under
`target/paw-qmux-flush-measure/bin/`, and run with
`taskset -c 3 <binary> --bench <filter> --save-baseline <name> --noplot`.
Socket counts used `strace -c -f -e trace=write,writev,send,sendto,sendmsg taskset -c 3
<probe> <arm> concurrent <n> <iterations>`
**Repetitions:** two Criterion passes per side, interleaved before → after → before → after.
Each socket count was taken at 1,000 and 3,000 completed batches and reduced as
`(c(3000) - c(1000)) / 2000`
**Controls:** the unchanged HTTP/2 arm was measured beside QMux in every pass. Its averaged
movement ranged from −1.02% to +0.43%
**Exclusions:** none. Every command completed, no run was interrupted, and no sample or
benchmark was discarded

## What changed

The stream-ending `Poll::Pending` boundary remains: it starts a new HTTP/3 event batch so a
stream's last data is applied before its close. What changed is the flush boundary. QMux may
now retain its bounded output across productive internal driver passes and event-batch
boundaries, but the HTTP/3 driver explicitly flushes the transport immediately before any
operation can suspend its task. Capacity pressure and the connection's completion/error tail
remain independent forced-flush points.

This run asks both parts of the acceptance question: did the socket write count stop growing
with the number of streams, and did preserving serial latency require an unacceptable trade?

## Criterion results

Criterion median microseconds. `before 1/2` and `after 1/2` are the raw pass medians; the change
compares the arithmetic mean of each side's two passes. Spread is the full distance between a
side's two medians divided by that side's mean. Lower is better.

| Benchmark | before 1 | after 1 | before 2 | after 2 | change | spread before / after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 23.537 | 23.860 | 23.989 | 24.049 | +0.81% | 1.90% / 0.79% |
| duplex concurrent 1 | 24.501 | 24.470 | 24.359 | 24.635 | +0.50% | 0.58% / 0.67% |
| duplex concurrent 8 | 130.999 | 129.987 | 131.253 | 130.567 | −0.65% | 0.19% / 0.45% |
| duplex concurrent 64 | 1050.629 | 1036.058 | 1050.971 | 1046.125 | −0.92% | 0.03% / 0.97% |
| socket serial | 38.765 | 34.957 | 38.715 | 35.239 | **−9.40%** | 0.13% / 0.80% |
| socket concurrent 1 | 39.547 | 35.561 | 39.343 | 36.305 | **−8.90%** | 0.52% / 2.07% |
| socket concurrent 8 | 180.045 | 140.085 | 180.020 | 140.995 | **−21.94%** | 0.01% / 0.65% |
| socket concurrent 64 | 1375.041 | 1035.853 | 1379.770 | 1041.251 | **−24.60%** | 0.34% / 0.52% |

### Unchanged HTTP/2 controls

| Control | before 1 | after 1 | before 2 | after 2 | averaged movement |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 10.160 | 10.244 | 10.288 | 10.102 | −0.50% |
| duplex concurrent 1 | 10.873 | 10.965 | 10.916 | 10.772 | −0.24% |
| duplex concurrent 8 | 65.603 | 66.145 | 65.601 | 64.606 | −0.35% |
| duplex concurrent 64 | 557.217 | 558.909 | 558.900 | 552.528 | −0.42% |
| socket serial | 17.154 | 17.136 | 17.040 | 17.205 | +0.43% |
| socket concurrent 1 | 17.790 | 17.538 | 17.848 | 17.738 | −1.02% |
| socket concurrent 8 | 73.097 | 72.846 | 73.765 | 72.923 | −0.74% |
| socket concurrent 64 | 564.571 | 563.227 | 564.186 | 560.367 | −0.46% |

The pre-registered serial gate normalises QMux movement by the matching HTTP/2 movement inside
each paired pass. Duplex ratios were 1.0054 and 1.0210, whose two-pass median (the arithmetic
mean) is a 1.32% regression. The gate is 2%, the largest of 2%, absolute control movement, and
QMux within-side spread, so this is not a serial blocker. Socket ratios were 0.9027 and 0.9015,
a 9.79% normalised improvement.

## Exact socket write counts

Raw syscall counts include process setup and the probe's two diagnostic `write` calls. The
two-point subtraction removes both. HTTP/2 used `writev`; QMux used `sendto`; `send`, `sendmsg`,
and the other protocol's write primitive were zero throughout.

Run 09 summarised the old linear shape as `2n + 2`. The 64-stream observation here is about
two writes above that idealised formula. That small offset is scheduling-dependent, but this
aggregate syscall probe does not isolate its source; the per-stream term, rather than an exact
intercept, is the defect this run tests.

| Revision | arm | streams | `c(1000)` | `c(3000)` | writes per batch |
| --- | --- | ---: | ---: | ---: | ---: |
| before | HTTP/2 `writev` | 1 | 2005 | 6005 | 2 |
| before | HTTP/2 `writev` | 8 | 2005 | 6005 | 2 |
| before | HTTP/2 `writev` | 64 | 2005 | 6005 | 2 |
| before | QMux `sendto` | 1 | 4011 | 12011 | 4 |
| before | QMux `sendto` | 8 | 18015 | 54033 | 18.009 |
| before | QMux `sendto` | 64 | 132063 | 396166 | 132.052 |
| after | HTTP/2 `writev` | 1 | 2005 | 6005 | 2 |
| after | HTTP/2 `writev` | 8 | 2005 | 6005 | 2 |
| after | HTTP/2 `writev` | 64 | 2005 | 6005 | 2 |
| after | QMux `sendto` | 1 | 3008 | 9008 | 3 |
| after | QMux `sendto` | 8 | 3012 | 9030 | 3.009 |
| after | QMux `sendto` | 64 | 3058 | 9161 | 3.052 |

The after-side values satisfy all three pre-registered limits:
`w(8) <= w(1) + 2`, `w(64) <= w(1) + 4`, and `w(64) <= 12`. The small fractional increase is consistent with occasional scheduler slicing across 1,000
batches, though the aggregate probe does not attribute individual extra calls. It is not a
per-stream term. The deterministic
in-memory regression counts both endpoints and obtains exactly 5 writes at 1, 8, and 64. The
socket probe counts the process's socket syscalls after setup and obtains approximately 3; the
fixed offset differs, but both instruments now have a constant concurrency shape and both
would fail their bounds if the old linear term returned.

## What this establishes

- QMux/H3 socket writes no longer fit `2n + 2`. They are approximately three per concurrent
  batch at 1, 8, and 64 streams while the unchanged HTTP/2 control remains exactly two.
- Removing the per-ending writes improves loopback-socket concurrency by 8.9% at one stream,
  21.9% at eight, and 24.6% at sixty-four on this host.
- The duplex arms remain within their controls and within-side spread. In particular, the
  serial normalised regression is below the pre-registered 2% gate, so no batching-delay trade
  was paid to obtain the socket result.
- The improvement comes from scheduling and suspension semantics, not from a timer: a serial
  exchange still flushes before the same executor poll can park.

## What it does not

- This is loopback, tokio, and a current-thread runtime on a shared Azure VM. It does not
  measure a real network, another executor, compio, or the QUIC join.
- It does not revisit the closed-stream lookup measured in run 10, or any later item in the
  QMux/H3 performance backlog.
- `ngnet-quic-h3` implements the new required flush operation as an immediate no-op because its
  datagram path does not defer output. That implementation was statically audited but could not
  be compiled here: this host has OpenSSL 3.0.13 and `ngnet-quic-sys` requires OpenSSL 3.5 or
  newer. CI must compile it before merge.
