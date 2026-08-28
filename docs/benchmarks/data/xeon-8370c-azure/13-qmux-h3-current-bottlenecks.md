# 13 — Where is QMux/H3 slow now?

**Machine:** historical [`xeon-8370c-azure`](README.md) VM label; the current VM reports an
**Intel Xeon Platinum 8573C**, as it did for run 12
**Date:** 2026-08-28
**Commit(s):** `5477450` exactly, after the closed-stream and flush-decoupling work
**Cases:** the single-arm probe for empty serial exchanges, concurrency 64, and 1 MiB echo
exchanges, each over an in-memory duplex and a loopback socket, for both HTTP/2 and QMux/H3.
A one-line diagnostic was then compared with `5477450` in Criterion's serial and concurrency
suites
**Commands:** profiles used
`perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid -- taskset -c 3
target/release/examples/probe <arm> <workload> <parameter> <iterations>`.
Exact call and allocation counts used two-point uprobes and
`(count(3N) - count(N)) / 2N`. The diagnostic Criterion binaries were built from separate
checkouts and run directly with
`taskset -c 3 <binary> --bench ngnet --save-baseline <name> --noplot`
**Repetitions:** two profiles per arm and workload, 24 profiles total. The Criterion diagnostic
used two interleaved passes per side, baseline → diagnostic → baseline → diagnostic. Six
short pinned probe comparisons per side were used only to screen the diagnostic before Criterion
**Controls:** the unchanged HTTP/2 arm was present in each Criterion binary. Its movement was
+0.02% for duplex serial, −0.50% for socket serial, and −0.37% to +0.80% for the matching
duplex/socket concurrency controls
**Exclusions:** none. No profile, count, or Criterion sample was discarded

## What was being asked

Runs 10 and 11 removed two large costs, and run 12 showed that the old `apply_events` profile
was no longer current. This run asks the broader question rather than moving to the next old
symbol: what now accounts for QMux/H3's cost against HTTP/2, and which mechanism can be changed
without guessing?

## Whole-stack cost

Task-clock microseconds per completed probe workload, averaged over two profiles:

| Workload | substrate | HTTP/2 | QMux/H3 | QMux/H3 ÷ HTTP/2 |
| --- | --- | ---: | ---: | ---: |
| empty serial | duplex | 11.18 | 24.92 | 2.23× |
| empty serial | socket | 18.72 | 36.77 | 1.96× |
| concurrency 64 | duplex | 530.25 | 1026.29 | 1.94× |
| concurrency 64 | socket | 542.38 | 1019.46 | 1.88× |
| 1 MiB echo | duplex | 532.03 | 686.66 | 1.29× |
| 1 MiB echo | socket | 1295.00 | **1153.47** | **0.89×** |

These are profiler task-clock figures, not Criterion latency measurements. They reproduce the
shape established by runs 08 and 11: a large fixed CPU cost, a smaller per-byte CPU cost, and a
bulk-socket win from QMux's larger writes.

## The small-exchange bottleneck: repeated driver passes and transport pumps

Exact calls per empty exchange:

| Call | HTTP/2 | QMux/H3 |
| --- | ---: | ---: |
| transport `poll_read` | 7 | **96** |
| waker clone | 11 | **94** |
| waker drop | 3 | **90** |
| QMux pump | — | **93** |
| QMux `write_side` | — | **96** |
| HTTP/3 `poll_event` | — | **30** |
| HTTP/3 `poll_transmit` | — | **14** |
| HTTP/3 client + server `apply_events` | — | **14** |
| named `Shared` take/pending/readiness calls | analogous API | **155** |

The top QMux/H3 symbols agree with the counts: transport `poll_read` is 7.6% across the two
endpoints, waker clone/drop is 6.7%, and `poll_event` is 3.1%. No one expensive operation
dominates. The cost is the number of times the event loop asks.

One duplication is concrete. `QmuxConnection::poll_event` pumps the QMux connection, then when
no translated event is already held calls `fill()`. `fill()` calls
`Connection::poll_next_event_buffered`, which pumps the same connection again before looking at
the event queue. The diagnostic retained the explicit pump only when a release or translated
event was already queued; otherwise the event-poll operation performed the one required pump.
It changed no other code.

That removed **23 transport reads per empty exchange**, 96 → 73. It did not remove all repeated
reads, so this is one confirmed cost rather than a claim to have redesigned the event loop.
The focused `ngnet-qmux-h3` and `ngnet-qmux-h3-tests` suites passed unchanged.

### Controlled diagnostic timing

Criterion median microseconds. The change compares the arithmetic mean of each side's two
passes. Lower is better.

| Benchmark | baseline 1/2 | diagnostic 1/2 | change | baseline / diagnostic spread |
| --- | ---: | ---: | ---: | ---: |
| duplex serial | 24.01 / 24.32 | 22.72 / 23.00 | **−5.42%** | 1.29% / 1.23% |
| socket serial | 35.26 / 35.71 | 33.94 / 34.39 | **−3.71%** | 1.27% / 1.33% |
| duplex concurrency 1 | 24.58 / 24.78 | 23.62 / 24.69 | −2.11% | 0.84% / 4.53% |
| duplex concurrency 8 | 130.67 / 130.99 | 126.67 / 131.78 | −1.23% | 0.24% / 4.03% |
| duplex concurrency 64 | 1052.75 / 1052.58 | 1023.82 / 1061.69 | −0.94% | 0.02% / 3.70% |
| socket concurrency 1 | 35.63 / 35.75 | 35.00 / 35.15 | **−1.72%** | 0.33% / 0.41% |
| socket concurrency 8 | 140.36 / 139.27 | 137.52 / 137.51 | **−1.64%** | 0.78% / 0.01% |
| socket concurrency 64 | 1035.96 / 1045.28 | 1023.32 / 1026.87 | **−1.49%** | 0.90% / 0.35% |

The unchanged HTTP/2 controls moved +0.02% for duplex serial, −0.50% for socket serial,
+0.14%/+0.80%/−0.37% for duplex concurrency 1/8/64, and
+0.04%/+0.36%/+0.42% for socket concurrency 1/8/64.

Both serial results and all socket-concurrency results exceed their matching controls and
within-side spread. The duplex-concurrency results do not exceed diagnostic-side spread and are
not claimed. Six additional pinned probe comparisons put the exploratory 1 MiB duplex change at
−2.49% with every pass between −3.23% and −1.48%; no Criterion body run was taken, so that is a
lead rather than an acceptance result.

## The concurrency bottleneck: distributed protocol work plus allocation

At concurrency 64 over a duplex, QMux/H3 costs 496 microseconds more per batch. The differential
is distributed. The named rows below are the major attributable layers, not an exhaustive sum;
kernel/vDSO, fixture, and unresolved/support symbols account for the remainder:

| Layer | HTTP/2 | QMux/H3 | QMux/H3 minus HTTP/2 |
| --- | ---: | ---: | ---: |
| Rust HTTP driver | 170.2 µs | 248.4 µs | +78.2 µs |
| C HTTP library | 59.6 µs | 121.3 µs | +61.7 µs |
| tokio | 35.8 µs | 123.8 µs | +88.0 µs |
| libc allocation/memory | 188.9 µs | 249.1 µs | +60.2 µs |
| QMux join + transport + dwnx | — | 174.6 µs | +174.6 µs |

Allocator calls per batch are **7,960 for QMux/H3 against 5,670 for HTTP/2**, or about 124
against 89 per stream. This is a real differential, but it is only one eighth of the elapsed
gap and has no single owner: header fields, HTTP maps, task state, stream creation, event vectors,
and protocol-library callbacks all contribute. The profile does not support “replace the
allocator” or “reuse one vector” as the next fix.

The repeated-pass shape scales too: a 64-stream QMux/H3 batch makes 2,246 transport reads,
2,240 waker drops, 2,243 pumps, 852 `poll_event` calls, and 268 transmit passes. The confirmed
double-pump fix removes one part; further collapsing requires a driver/transport contract that
can remember read-pending within a productive turn without losing wake registration.

## The bulk bottleneck: one owned allocation per delivered record

At 1 MiB over a duplex, QMux/H3 costs 155 microseconds more. The QMux join, transport, and dwnx
account for 165 microseconds and libc for another 55 microseconds over HTTP/2, while the
HTTP/3 Rust driver is 91 microseconds cheaper than the HTTP/2 one. Smaller protocol-library,
tokio, kernel, and unresolved differences account for the rest rather than making those three
figures an exhaustive decomposition.

Exact allocator calls are **818 per QMux/H3 exchange against 205 for HTTP/2**. In the QMux/H3
malloc call stacks, 83% come from three views of the same delivery path:

- `RawVec::finish_grow`: 38.4%;
- `Bytes` shallow-clone backing: 22.4%;
- QMux's stream-data handler, which copies callback-borrowed bytes into an owned event: 22.2%.

This is the dominant bulk CPU mechanism, but not an immediately valid fix. Run 05 replaced the
copy with pooled reference-counted views, cut each delivery allocation from 8,216 bytes to 24,
and became 2.5–4.8% slower. The next attempt needs a cheaper ownership design, not another
generic pool or a claim that fewer allocations automatically means less time.

Over a socket QMux/H3 remains faster at 1 MiB because its larger writes save 68 microseconds of
kernel time and its HTTP/3 driver is cheaper per byte. That result does not make the delivery
allocation free; it means the avoided socket work is larger.

## What this establishes

- **The next implementable bottleneck is a duplicate QMux pump in the HTTP/3 event path.**
  Removing only that duplication improves serial latency by 5.4% on a duplex and 3.7% on a
  socket, with unchanged HTTP/2 controls, and reduces transport reads by 24%.
- **The broader small-request cost is event-loop amplification**, not `apply_events`: 14
  productive passes become 30 event polls, 93 pumps, 96 reads, and 184 waker clone/drops.
- **Shared-state probes are current but secondary.** The named H3 methods are 10.9% of QMux/H3
  serial CPU versus 10.4% for analogous H2 methods; their absolute cost is 2.71 versus 1.16
  microseconds. Collapsing their 155 calls may help, but it does not explain most of the gap.
- **Concurrency has no single allocation hotspot.** QMux/H3 allocates 40% more often, but
  allocation/memory accounts for about one eighth of the total differential.
- **Bulk delivery allocation is a genuine dominant mechanism**, but its known straightforward
  replacement is slower. It is a design problem, not a ready optimization.

## What it does not

- The diagnostic is not production code. It changes one pump decision and passed focused suites,
  but a real implementation still needs tests that pin progress, wake ownership, event ordering,
  buffered-output flushing, close/error tails, and both queued-event cases.
- Profiles compare current implementations, not protocols. HTTP/3 carries QPACK/control streams
  and QMux carries transport framing and flow control that HTTP/2 does not.
- The current VM reports a Xeon 8573C under the historical 8370C directory. Within-run ratios,
  repeated profiles, exact counts, and the interleaved A/B are usable; absolute comparison with
  old runs is not controlled.
- Malloc call graphs were sampled from 20–200 workloads after exact two-point counts established
  the totals. Percentages identify ownership; they are not allocation counts by themselves.
- No QUIC arm, real network, completion runtime, or multi-threaded runtime was profiled.
