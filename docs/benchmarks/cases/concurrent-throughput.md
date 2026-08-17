# `concurrent_throughput`

**Family:** duplex — `tests/ngnet-bench/benches/concurrent_throughput.rs`

`N` requests issued together on **one** connection per iteration and awaited as a group, so
Criterion's per-iteration time covers `N` whole exchanges.

```sh
cargo bench -p ngnet-bench --bench concurrent_throughput
```

## What it measures

Multiplexing that serial latency cannot show: how the per-exchange cost changes when `N`
streams are in flight on one connection. `Throughput::Elements(N)` turns the per-iteration
time into requests/sec.

## Arms and parameters

| Arm | Stack | Protocol |
| --- | --- | --- |
| `ngnet-h2` | this crate | HTTP/2 |
| `ngnet-qmux-h3` | this crate | HTTP/3 over QMux |
| `hyper` | hyper | HTTP/2 |

`N` sweeps **1, 8, 64**. 64 sits below the 128 concurrent streams both stacks are configured
for ([`../configuration.md`](../configuration.md)), and the QMux fixture refuses a concurrency
above that limit before offering it rather than discovering the answer on the wire.

Read pairwise: `ngnet-h2` against `ngnet-qmux-h3` varies the protocol, `ngnet-h2` against
`hyper` varies the HTTP/2 implementation, and `ngnet-qmux-h3` against `hyper` varies both and
attributes to neither. The arms are registered in that order inside the concurrency loop, so
`N` is the outer loop and the arms are the inner one — the arrangement
[`../controls.md`](../controls.md) fixes as a control.

Two separately named groups run the same sweep, and **they do not carry the same arms**:

- **`concurrent_throughput`** — one `current_thread` runtime, three arms as above. This is the
  deterministic headline: with no syscalls, a multi-threaded scheduler would only add
  cross-thread wakeup noise.
- **`concurrent_throughput_multi_thread`** — a four-worker runtime, and **only the two HTTP/2
  arms**. It exists to show what cross-thread scheduling does to the same work, **not** to
  replace the single-threaded numbers, and the two must not be tabulated as one series.

### Why the multi-threaded group has no QMux arm

This is not the reason [`shared_body`](shared-body.md) has none, and the two must not be filed
together. There, no counterpart mechanism exists to measure. Here the mechanism exists, the arm
was written, and it does not work: **the QMux join hangs at high concurrency on a multi-worker
runtime.** Measured with the flow-control windows and the stream allowance raised out of the
way, concurrency 64 wedged on roughly three attempts in four at both two and four workers,
typically after about 55 of the 64 requests had completed. Concurrency 1 and 8 complete on
every runtime, a current-thread runtime completes at every point, and loopback TCP is clean
throughout — so the sibling group above and every `transport_*` target are unaffected.

**Intermittence is what makes the omission necessary rather than merely tidy.** An arm that
hung every time would be caught by whoever added it. One that hangs three times in four is a CI
job that occasionally never returns, and `cargo bench -- --test` imposes no timeout that would
turn that into a failure.

The defect is recorded rather than fixed — fixing the join is outside this work's scope — on
[`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md). That record is what makes the
omission traceable to a known defect rather than indistinguishable from an oversight, and it is
what should be read before anyone adds the arm back.

## Reading it

- **Throughput does not scale with `N` here the way a networked server would.** On one core
  the per-request protocol CPU cost cannot be run in parallel, so multiplexing only amortises
  per-batch overhead. See [`../interpreting.md`](../interpreting.md).
- The duplex reports `is_write_vectored() == true`, so the `ngnet-h2` arm exercises the
  gathering drain — this case is not measuring the historical per-block write behaviour.
- **On the cross-protocol pair, N=1 is the control point and N=64 is the question.** At N=1
  there is nothing to multiplex, so the pair should reproduce
  [`serial-latency`](serial-latency.md) closely; a cross-protocol ratio that grows with `N` says
  something scales with in-flight streams rather than with exchanges, which is a different claim
  from a fixed per-exchange overhead and needs a different mechanism.
- This case was **blind to the effect that dominated the socket family**: a per-block drain
  costs nothing without a kernel. That is the clearest single illustration of what the duplex
  deletes; see [`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).
  The same caution applies to the QMux arm, whose own write path issues one write per `IoSlice`
  ([`../controls.md`](../controls.md)) — a cost the duplex family largely hides.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
