# 06 — What did the QMux write-path work cost and save, end to end?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-17
**Commit(s):** `524fa54` against `a54ea43` — the instrumentation commit that precedes the first
optimization, against the final state. The baseline is deliberately *not* the branch point: the
instrumentation commit adds counters, and comparing against the branch point would have put them
on one side of the comparison only
**Cases:** the six bench targets that carry a QMux arm — `serial_latency`,
`concurrent_throughput`, `body_throughput` and their three `transport_` counterparts. All six were
run whole, so every arm in every group is present and the HTTP/2 and hyper arms in them are
controls. The three groups with no QMux arm (`shared_body`, `transport_shared_body`,
`concurrent_throughput_multi_thread`) were not run: nothing in them can move, and including them
would only have diluted the summary
**Command:** `cargo build --benches -p ngnet-bench --release`, then
`taskset -c 3 cargo bench -p ngnet-bench --bench serial_latency --bench concurrent_throughput --bench body_throughput --bench transport_serial_latency --bench transport_concurrent_throughput --bench transport_body_throughput -- --save-baseline <name>`
**Repetitions:** two per side, interleaved base → after → base → after
**Controls:** the 46 `ngnet-h2`, `ngnet-h2-tokio`, `ngnet-h2-compio`, `hyper` and `hyper-tokio`
identifiers in the same six targets, none of which this work touches
**Exclusions:** none, and no rule was needed; nothing was dropped

## What was being asked

Six changes were made to the QMux write and read paths: records now coalesce into a bounded
buffer and are written once instead of one per record; a record is serialised into that buffer
rather than through a staging copy; a record that arrives whole is scanned where it lies instead
of being copied to be examined; one record can carry a whole fragmented offer; and window
extensions are held for the length of a run rather than forwarded one at a time. A seventh — 
handing callers views of a pooled read buffer instead of copies — was made, measured, and
reverted; [`05-qmux-delivery-aliasing`](05-qmux-delivery-aliasing.md) is why. This run asks what
the set that shipped is worth.

## Results

Microseconds per iteration, lower is better. **Bold** marks a delta larger than the worst control
movement in the same session, which is the only bar this machine offers.

| Benchmark id | family | base (µs) | after (µs) | paired delta | spread |
| --- | --- | --- | --- | --- | --- |
| `body_throughput/ngnet-qmux-h3/0` | duplex | 28.8 | 29.9 | +3.91% | 1.63% |
| `body_throughput/ngnet-qmux-h3/1024` | duplex | 34.8 | 33.4 | -4.17% | 3.10% |
| `body_throughput/ngnet-qmux-h3/1048576` | duplex | 986.0 | 686.9 | **-30.33%** | 0.72% |
| `body_throughput/ngnet-qmux-h3/65536` | duplex | 87.9 | 72.5 | **-17.53%** | 2.88% |
| `concurrent_throughput/ngnet-qmux-h3/1` | duplex | 29.8 | 30.7 | +3.08% | 1.54% |
| `concurrent_throughput/ngnet-qmux-h3/64` | duplex | 1239.1 | 1261.8 | +1.83% | 1.34% |
| `concurrent_throughput/ngnet-qmux-h3/8` | duplex | 156.0 | 159.3 | +2.14% | 0.92% |
| `serial_latency/ngnet-qmux-h3` | duplex | 29.1 | 29.4 | +1.18% | 1.15% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/0` | socket | 54.1 | 55.1 | +1.73% | 2.14% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/1024` | socket | 67.7 | 59.7 | **-11.84%** | 1.80% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/1048576` | socket | 1785.2 | 1241.8 | **-30.44%** | 1.14% |
| `transport_body_throughput/ngnet-qmux-h3-tokio/65536` | socket | 173.4 | 128.5 | **-25.92%** | 2.10% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/1` | socket | 55.5 | 56.2 | +1.33% | 1.51% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/64` | socket | 1855.8 | 1697.9 | **-8.51%** | 1.25% |
| `transport_concurrent_throughput/ngnet-qmux-h3-tokio/8` | socket | 248.5 | 230.8 | **-7.12%** | 0.81% |
| `transport_serial_latency/ngnet-qmux-h3-tokio` | socket | 53.0 | 55.1 | +3.97% | 1.52% |
## Drift controls in the same session

| Control arm | Movement |
| --- | --- |
| `body_throughput/hyper/0` | +4.47% |
| `body_throughput/hyper/65536` | +4.37% |
| `body_throughput/hyper/1048576` | -2.93% |
| `concurrent_throughput/ngnet-h2/8` | +2.35% |
| `concurrent_throughput/ngnet-h2/64` | +2.30% |
| all 46 unchanged identifiers | mean **1.06%**, worst **4.47%** |
The control band is wider than [`04-qmux-drift-baseline`](04-qmux-drift-baseline.md) recorded for
the same arms, and the two largest movers are hyper arms. Nothing here is quoted against a control
band narrower than the one actually observed.

## What this establishes

- **Bodies got much faster, and more so the larger they are.** −30.4% at a megabyte over a socket,
  −30.3% over a duplex, −25.9% at 64 KiB over a socket, −17.5% at 64 KiB over a duplex, −11.8% at
  1 KiB over a socket. These are far outside the control band in a session where the controls were
  unusually wide.
- **Multiplexed exchanges over a real socket got faster: −8.5% at concurrency 64 and −7.1% at 8.**
  These two identifiers were predicted in advance to be the only ones where a write-count
  reduction could appear as a system-call saving, and they are.
- **The same two parameters over a duplex got slightly slower: +1.8% and +2.1%.** That is the same
  prediction seen from the other side. A duplex makes no system calls, so coalescing has no
  syscall to save there and only its bookkeeping to pay. The sign difference between the two
  families for the same parameter is the strongest single piece of evidence that the gain is the
  mechanism it is claimed to be, and not something else moving with it.
- **Small exchanges cost a little more.** Empty body +3.9% over a duplex, +1.7% over a socket;
  serial latency +1.2% over a duplex, +4.0% over a socket. Only the last exceeds the worst control
  movement, and only barely. The honest reading is a real but small per-exchange cost, of the
  order of a few percent, which the arms with no payload to amortise it over show and the others
  do not.
- **The largest per-identifier gain and the largest per-identifier cost are on the same arm
  family**, which is what a change to a fixed per-pass cost looks like when the payload varies.

## What it does not

- **It does not attribute the gain among the five changes that shipped.** This is one paired
  comparison of the whole set.
  [`findings/qmux-write-path.md`](../../findings/qmux-write-path.md) carries the per-commit
  attribution, taken on two of these targets.
- **It does not establish the small regressions as real.** Four of the six positive deltas are
  inside the worst control movement. They are reported because a result is not permitted to be
  quoted in one direction only, not because they are settled.
- **It does not cover compio, `shared_body`, or a multi-worker runtime.** No QMux arm exists in
  any of them.
- **It says nothing about memory.** Coalescing raised what a connection may hold awaiting a write
  from one record to about 80 KiB by design. Nothing here measures resident size.
- **Two repetitions per side is this suite's minimum.** The settled verdicts on this machine used
  five and ten. The large deltas here are many times the control band and do not need more; the
  small ones would.
