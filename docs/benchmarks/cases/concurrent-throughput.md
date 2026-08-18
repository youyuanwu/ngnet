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
was written, and it was left out because **the QMux join hangs at high concurrency on a
multi-worker runtime.** Measured with the flow-control windows and the stream allowance raised
out of the way, concurrency 64 wedged on roughly three attempts in four at both two and four
workers, typically after about 55 of the 64 requests had completed. Concurrency 1 and 8 complete
on every runtime, and a current-thread runtime completes at every point — so the sibling group
above and every `transport_*` target are unaffected.

**The reason for the omission has since shifted, and the arm should still not be added.** The
write-path work screened the defect and drove it deliberately: the benchmark fixtures themselves
completed 1,520 attempts out of 1,520, across both substrates and every worker count, because
`response_for` sets a `content-type` header on the response and that header takes the failure
rate from 100% to 0%. So the fixture this group would use does not hang, and the original
objection — an arm that occasionally never returns, under a `cargo bench -- --test` that imposes
no timeout to turn that into a failure — no longer applies on the evidence.

What replaces it has nothing to do with the defect. This is a **duplex** group, so it cannot
show a syscall saving; a QMux arm here would report the userspace bookkeeping its
single-threaded sibling already reports, plus the scheduling noise the group exists to display.
Adding an arm that sits one response header away from a deterministic wedge, to measure
something another arm measures more cleanly, is a bad trade. Anyone who wants it anyway should
read the fixture's header first.

The defect is recorded rather than fixed — fixing the join is outside this work's scope — on
[`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md), which also carries what the
screen found, including a correction to the claim that loopback TCP is clean throughout. That
record is what makes the omission traceable to a known defect rather than indistinguishable from
an oversight, and it is what should be read before anyone adds the arm back.

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
  The same caution used to apply to the QMux arm, whose write path issued one write per
  `IoSlice`. It no longer does: records now accumulate in a bounded buffer and leave together,
  and this group shows what that is worth without a kernel — which is *nothing*, and slightly
  worse than nothing. Its socket sibling gained 8.5% at N=64 from the same change while this
  group's arm lost 1.8%
  ([`../findings/qmux-write-path.md`](../findings/qmux-write-path.md)), which is the same
  blindness pointing the other way: the bookkeeping is visible here and what it buys is not.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
