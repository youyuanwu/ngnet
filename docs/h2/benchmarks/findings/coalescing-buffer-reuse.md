# Reusing the coalescing buffer

**Measurement:**
[`03-coalescing-buffer-reuse`](../data/legacy-dev-host/03-coalescing-buffer-reuse.md) — legacy
development host.

Removing the owned path's per-pass allocation was measured separately from the gathering work,
since `CompioIo` is the only shipped transport that takes that path.

**About 4–7% for the completion transport** across the body sweep, in a run where the unchanged
`hyper-tokio` control held within ±1.2% — the quietest conditions obtained for any measurement
on that host, which is why that run is quoted rather than an average. A second run agreed on
direction for compio but was noisier throughout.

## The tokio arm read slightly positive, and that was chased down

`ngnet-h2-tokio` reads +0.9% to +7.1% in the same run, and that was investigated rather than
waved through, since a regression on the default transport would matter more than the gain. It
does not reproduce: on `transport_concurrent_throughput`, the workload that exercises the
gathering path hardest, the same build measured −5.1%, −0.2% and +1.2% at N=1/8/64 against a
control that had itself moved −3.1% to −5.0%. A cost that appears in one benchmark family and
not the other, with no mechanism to explain it, is drift.

There is no mechanism because the empty case is taken before the split (see `flush`): a pass
that never coalesces hands over `Bytes::new()` and touches the buffer not at all. That guard is
load-bearing rather than cosmetic — without it, `split` on an already-shared buffer costs an
atomic increment and the dropped handle an atomic decrement, so the vectored and borrowed paths
would have paid two atomics per pass for a buffer they never fill. That cost was measured and
removed before these numbers were taken.

## Why the gain is this size and not larger

Twelve allocations per pass sounds substantial, but a same-size `malloc`/`free` pair under
glibc's thread cache is tens of nanoseconds, so twelve is well under a microsecond against a
62 µs pass. What actually costs something is the **growth**: rebuilding the buffer from empty
each pass re-copies its contents at every doubling, which is why the gain appears on the body
sweep and not in concurrency.

This is a good illustration of why the counts in [`../allocation-counts.md`](../allocation-counts.md)
are pinned as a *property* rather than treated as a proxy for time.

## What a new machine should reproduce

1. A gain on the **completion** arm across the body sweep, present at every size including
   0 B — the buffer is rebuilt per pass whether or not there is a payload.
2. Larger at the sizes where the buffer grows most, and **absent on the readiness arms**,
   which do not take the owned path at all.
3. Any movement on `ngnet-h2-tokio` in this benchmark should fail to reproduce in
   `transport_concurrent_throughput`. If it does reproduce, the drift explanation above is
   wrong and there is a mechanism to find.
