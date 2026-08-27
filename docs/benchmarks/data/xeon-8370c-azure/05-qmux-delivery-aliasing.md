# 05 — Does handing a caller a view of the read buffer beat copying it?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-17
**Commit(s):** `223960d` against `9f97334` — the delivery-aliasing change against its immediate
parent, so the only difference between the sides is that one copies each delivery into an owned
allocation and the other hands out a reference-counted view of a pooled read buffer
**Cases:** `serial_latency` and `body_throughput`, the two targets where a per-delivery cost can
appear at all; both were run whole, so every arm in them is present
**Command:** `cargo build --benches -p ngnet-bench --release`, then
`taskset -c 3 cargo bench -p ngnet-bench --bench serial_latency --bench body_throughput -- --save-baseline <name>`
**Repetitions:** two per side, interleaved across the whole sequence of seven commits this run was
cut from — every commit was measured once before any was measured twice
**Controls:** the `ngnet-h2` and `hyper` arms in the same groups, ten identifiers, untouched by
either side
**Exclusions:** none, and no rule was needed; nothing was dropped

## What was being asked

Every delivery of received stream data is copied out of the buffer dwnx is parsing into a fresh
allocation, because the borrow a handler is given is valid only inside the callback. The HTTP/2
stack solved the same problem by keeping the read buffer alive and handing out reference-counted
views into it, and the pending-work pages have asked since the layer was written what that copy
costs. This run answers a narrower and more useful question: not what the copy costs, but whether
removing it makes anything faster.

The prior expectation was that it would, and it is recorded here because it was wrong. An
allocation-counting harness on the same build shows per-delivery allocation falling from 8,216
bytes to 24 — a reduction of two orders of magnitude, achieved as designed.

## Results

Microseconds per iteration, lower is better. The paired delta is the aliasing build against its
parent; **bold** rows are the arms that changed.

| Benchmark id | with the copy (µs) | aliased (µs) | paired delta | within-side spread |
| --- | --- | --- | --- | --- |
| `body_throughput/ngnet-qmux-h3/0` | 29.9 | 30.7 | **+2.52%** | 0.64% |
| `body_throughput/ngnet-qmux-h3/1024` | 33.7 | 34.8 | **+3.29%** | 1.13% |
| `body_throughput/ngnet-qmux-h3/1048576` | 692.6 | 688.4 | **-0.61%** | 1.01% |
| `body_throughput/ngnet-qmux-h3/65536` | 78.6 | 82.3 | **+4.79%** | 3.87% |
| `serial_latency/ngnet-qmux-h3` | 29.2 | 30.6 | **+4.68%** | 0.29% |
| `body_throughput/hyper/0` | 9.4 | 9.4 | -0.23% | 0.49% |
| `body_throughput/hyper/1024` | 13.4 | 13.5 | +0.31% | 0.34% |
| `body_throughput/hyper/1048576` | 399.8 | 406.8 | +1.74% | 9.09% |
| `body_throughput/hyper/65536` | 35.5 | 36.0 | +1.29% | 1.09% |
| `body_throughput/ngnet-h2/0` | 10.3 | 10.4 | +0.32% | 0.44% |
| `body_throughput/ngnet-h2/1024` | 13.5 | 13.4 | -0.84% | 1.26% |
| `body_throughput/ngnet-h2/1048576` | 508.2 | 506.9 | -0.24% | 0.17% |
| `body_throughput/ngnet-h2/65536` | 39.3 | 39.4 | +0.23% | 1.91% |
| `serial_latency/hyper` | 9.4 | 9.3 | -1.01% | 0.93% |
| `serial_latency/ngnet-h2` | 10.3 | 10.2 | -1.07% | 0.44% |
## Drift controls in the same session

| Control arm | Movement |
| --- | --- |
| The ten `ngnet-h2` and `hyper` identifiers in the same two groups | mean **0.73%**, worst **1.74%** |

The changed arms moved by 2.5% to 4.8% against that. Only the megabyte point sits inside the
controls' own movement, at −0.61%.

## What this establishes

- **Aliasing deliveries is slower than copying them, at every payload size but one.** Empty body
  +2.52%, 1 KiB +3.29%, 64 KiB +4.79%, and serial latency +4.68%, against controls moving 0.73%
  on average. At 1 MiB it is −0.61%, which is inside the controls and is not a result.
- **A large reduction in allocation did not produce a reduction in time**, and by itself is not
  evidence that it would. This is the same lesson the HTTP/2 stack's own coalescing-buffer finding
  recorded from the other direction, and it is the reason this suite treats a count and a timing as
  different kinds of claim.
- **The mechanism is consistent across the sizes.** The cost is per delivery rather than per byte:
  it is largest relative to the work on the arms with the least work per delivery, and it
  disappears at the size where a single delivery carries the most. A pool's bookkeeping, a
  reference count taken and dropped, and the copy that a delivery below the aliasing threshold
  still needs, are together larger than one allocation of the size being avoided.

## What it does not

- **It does not show that no aliasing scheme could pay.** It shows that this one does not. A
  design with a cheaper reclamation check, or without the small-delivery copy-out that the pinning
  bound requires, is untested — and the pinning bound is not optional, so that particular variant
  is not available without giving something else up.
- **It does not cover the socket family.** `transport_body_throughput` and
  `transport_serial_latency` were not in this run. A kernel in the path adds a fixed cost to every
  exchange that would dilute this one, so the socket arms would be expected to show a *smaller*
  regression, not a different sign — but that is a prediction and not a measurement.
- **It does not cover concurrency.** `concurrent_throughput` was not run. More streams in flight
  means more deliveries per pass and more pool traffic, so the direction would be expected to hold
  or worsen; again, untested.
- **It says nothing about memory.** The aliasing build holds fewer allocations and pins more
  buffer. Nothing here measures resident size, and the two are not the same question.

## What was done about it

The change was reverted. The requirement it was made under says that a change whose measured
effect does not clear the bar is either removed or kept for a stated reason that is not its
timing, and there is no such reason here: the allocation count is the only thing it improves, and
the count is not what a caller waits for.
