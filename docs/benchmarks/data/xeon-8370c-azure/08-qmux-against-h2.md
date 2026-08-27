# 08 — What does HTTP/3 over QMux cost relative to HTTP/2?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-18
**Commit(s):** `c525aa1` — a single sha. This is a comparison of two *arms* in one build, not
of two builds
**Cases:** the six bench targets that carry a QMux arm — `serial_latency`,
`concurrent_throughput`, `body_throughput` and their three `transport_` counterparts, 62
benchmark ids. `shared_body`, `transport_shared_body` and `concurrent_throughput_multi_thread`
were not run: they have no QMux arm and can contribute nothing to a ratio
**Command:** `cargo build --benches -p ngnet-bench --release`, then five times
`taskset -c 3 cargo bench -p ngnet-bench --bench serial_latency --bench concurrent_throughput --bench body_throughput --bench transport_serial_latency --bench transport_concurrent_throughput --bench transport_body_throughput -- --save-baseline x<n>`
**Repetitions:** five passes. Interleaving does not apply and is not claimed: the two arms of
each comparison are registered adjacently inside one Criterion group, so they already run beside
each other within every pass
**Controls:** none, and none apply — every arm here is under test. What stands in their place is
that **the ratio is formed inside each pass, from that pass's own two medians**, and the five
resulting ratios are reported with their full range. Session drift moves both arms of a group
together and cancels in the quotient; a ratio of two figures taken in different sessions would
not have that property
**Exclusions:** none. The rule was fixed before the first pass: no replicate is excluded for its
value, a pass is discarded only if an arm fails to complete, and then the whole pass goes rather
than one arm from it. No pass was discarded

## What was being asked

The suite has carried a QMux arm beside an HTTP/2 arm since the arms were added, and
[`data/README.md`](../README.md) has said ever since that no recorded run compares them. Runs
`04` through `07` are all build-against-build: they measure what changed, not what the two stacks
cost relative to each other. This run asks the question the arms were built for.

The pre-registration is at `.paw/work/qmux-h3-perf/CrossProtocolPreregistration.md` in the branch
that produced this run.

## Results

A ratio above 1.00× means QMux is slower. **Bold** marks the two ends of the range — where QMux
is within half again of HTTP/2, and where it is more than three times.

### Over an in-memory duplex — no kernel, so this isolates processor cost

| Benchmark id | HTTP/2 (µs) | QMux (µs) | ratio | range over five passes |
| --- | --- | --- | --- | --- |
| `serial_latency` | 10.5 | 29.3 | 2.78× | 2.77–2.87× |
| `body_throughput/0` | 10.2 | 29.5 | 2.92× | 2.88–2.94× |
| `body_throughput/1024` | 13.4 | 33.2 | 2.47× | 2.45–2.50× |
| `body_throughput/65536` | 38.8 | 72.9 | 1.86× | 1.79–1.91× |
| `body_throughput/1048576` | 506.5 | 680.9 | **1.34×** | 1.33–1.51× |
| `concurrent_throughput/1` | 11.1 | 30.3 | 2.72× | 2.71–2.74× |
| `concurrent_throughput/8` | 64.1 | 158.9 | 2.48× | 2.44–2.50× |
| `concurrent_throughput/64` | 539.9 | 1259.6 | 2.33× | 2.32–2.35× |
### Over a loopback socket

| Benchmark id | HTTP/2 (µs) | QMux (µs) | ratio | range over five passes |
| --- | --- | --- | --- | --- |
| `transport_serial_latency` | 21.1 | 55.2 | 2.61× | 2.60–2.67× |
| `transport_body_throughput/0` | 21.1 | 55.1 | 2.61× | 2.56–2.67× |
| `transport_body_throughput/1024` | 35.2 | 59.8 | 1.70× | 1.68–1.74× |
| `transport_body_throughput/65536` | 106.1 | 127.9 | **1.21×** | 1.18–1.22× |
| `transport_body_throughput/1048576` | 1394.9 | 1242.2 | **0.89×** | 0.88–0.90× |
| `transport_concurrent_throughput/1` | 21.8 | 56.4 | 2.59× | 2.52–2.63× |
| `transport_concurrent_throughput/8` | 73.4 | 231.0 | **3.14×** | 3.09–3.20× |
| `transport_concurrent_throughput/64` | 546.1 | 1701.2 | **3.12×** | 3.08–3.14× |
## The decomposition, which is the useful part

Subtracting the empty-body point from the megabyte point separates a **fixed cost per exchange**
from a **marginal cost per byte**, for each stack and each substrate:

| | fixed, per exchange | marginal, per MiB of body |
| --- | --- | --- |
| HTTP/2 over a duplex | 10.2 µs | 496.3 µs |
| QMux over a duplex | 29.5 µs | 651.3 µs |
| **QMux ÷ HTTP/2, duplex** | **+19.4 µs** | **1.31×** |
| HTTP/2 over a socket | 21.1 µs | 1373.8 µs |
| QMux over a socket | 55.1 µs | 1187.1 µs |
| **QMux ÷ HTTP/2, socket** | **+34.0 µs** | **0.86×** |

The two components behave differently and the difference is the whole shape of the comparison.

## What this establishes

- **QMux costs about 2.6× to 2.9× more than HTTP/2 for an exchange with no body**, on either
  substrate, and this dominates every small-request figure in the table. In absolute terms it is a
  fixed **+19 µs** per exchange over a duplex and **+34 µs** over a socket. That is the extra
  layer: records, transport-level flow control, and a pump between the transport and the HTTP
  framing, none of which an HTTP/2 connection carries.
- **Per byte of body, QMux is 31% more expensive than HTTP/2 in processor terms** — the duplex
  figure, where no kernel is involved.
- **Per byte of body over a real socket, QMux is 14% *cheaper* than HTTP/2**, and this is the
  result worth noticing. It is not marginal: `transport_body_throughput/1048576` is **0.89×**,
  with all five passes between 0.88× and 0.90×, and it is the only identifier in the suite where a
  QMux arm beats its HTTP/2 counterpart.
- **The crossover sits between 64 KiB and 1 MiB over a socket** — 1.21× and 0.89× — as the fixed
  per-exchange cost is amortised and then overtaken.
- **Concurrency over a socket is QMux's worst case, at 3.12–3.14×**, and it is *worse* than the
  same parameter over a duplex (2.33×). Every other workload has a smaller ratio with a kernel in
  the way than without one; concurrency is the exception, on both 8 and 64 streams, in all five
  passes.

## What it does not

- **It does not explain why QMux moves bulk bytes more cheaply over a socket.** Combining the two
  families implies the kernel-path cost per megabyte is **878 µs for HTTP/2 and 536 µs for QMux**,
  which is 61%. That is arithmetic on measured numbers, not a mechanism. The obvious candidate is
  the number and size of writes per megabyte — QMux now fills a 64 KiB buffer and writes it once,
  and what HTTP/2's gathering path does per megabyte has not been counted here. **What would
  settle it:** a write count per megabyte for both arms, which both stacks already have the
  instrumentation to report.
- **It does not explain the concurrency inversion**, which remains the standing lead recorded in
  `docs/qmux-h3/pending-work.md`. This run does sharpen it: the inversion is now measured rather
  than suspected, it holds at both 8 and 64 streams, and the mechanism previously suspected — one
  write per offered slice — was removed in this branch and the inversion survived it.
- **It compares one implementation of each, not two protocols.** HTTP/2 here has had a write path,
  a body-handover path and a buffer-reuse path measured and tuned; QMux has had one round of the
  same. A reader wanting to know what the QMux *draft* costs should not read this table as that.
- **It says nothing about a real network, about compio, or about memory.** Loopback throughout,
  tokio on both sides of every pair quoted, and no resident-size measurement anywhere.
- **The megabyte duplex point is the least trustworthy row.** Its QMux arm varied 14.4% across the
  five passes, consistent with [`04`](04-qmux-drift-baseline.md) recording it as the noisiest
  identifier in the suite; its ratio range is 1.33–1.51× where every other row is within 0.06×.
- **Confounds that were not removed** are enumerated on [`../../configuration.md`](../../configuration.md)
  and [`../../controls.md`](../../controls.md) and all bias against QMux: the record size is 16382
  bytes against HTTP/2's 16384-byte frame payload, and HTTP/3's control and QPACK streams consume
  connection-level flow-control credit where HTTP/2's control frames sit outside it.
