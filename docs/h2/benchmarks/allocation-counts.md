# Allocation, counted rather than timed

These are not timings and are not filed under [`data/`](data/): they are counts asserted by
tests, so they are a property of the code and identical on every machine. `cargo test -p
ngnet-h2` re-derives every figure on this page.

From `crates/ngnet-h2/tests/http_zero_alloc.rs`, exact counts per driver pass in steady state:

| Shape (write behaviour × declaration → drain) | Single upload | 8 multiplexed streams |
| --- | --- | --- |
| declares `false`, either model → coalesced | 0 allocs / 1 write | 0 allocs / 1 write |
| natively gathering, declares `true` → gathered | **0 allocs / 4 writes** | **0 allocs / 1 write** |
| emulating, declares `true` → gathered | **0 allocs / 4 writes** | **0 allocs / 1 write** |
| emulating, declares `false` → coalesced | **0 allocs / 1 write** *(was 4)* | **0 allocs / 1 write** |
| *(removed)* `PerRegion` — per-block, no accumulation | 0 allocs / 4 writes | 0 allocs / **513 writes** |

**One number in this table moved when `TransportWrite::is_write_vectored` replaced
`Config::write_policy`, and it is the fourth row's upload column: 4 → 1.** Its mechanism is the
drain switch and nothing else. Under the old design that row did not exist as a separate shape:
an emulating transport had no way to decline, so it took the gathered drain and measured
identically to the native one — which is now the *third* row, still measured, by a transport
that declares `true` against its own nature precisely so the old row survives. The fourth row
is the same transport telling the truth. It buys the one write with a copy of every outgoing
octet, where the gathered drain's emulation issued its writes without copying the regions, so
for an upload that was already one region this is a cost rather than a saving. It is pinned
under a name that says so — `an_honest_emulating_transport_now_costs_one_write_per_upload_pass`
— so the move cannot be quietly re-absorbed as a new baseline.

The multiplexed column moved nowhere, which is the more important half. A multiplexed pass is
hundreds of sub-threshold blocks that the driver accumulates into a *single region* before any
write, so gathered and coalesced both cost one write.

The last row is history, kept because the 513 is the number the whole gathering argument turns
on. That drain no longer exists, and **the 68.3 Kelem/s figure quoted in the gathering finding
belongs to it, not to emulated gathering**. Emulation is **not** the 513-write cliff:
accumulation happens in the driver *before* any write, so the 512 small blocks collapse into
one region and the emulating loop runs once. That is why the two `true`-declaring rows are
identical on both workloads, and it is the structural reason the provided gathering defaults
are affordable.

## How the coalesced row reached zero

The coalesced row read `4 allocs` and `12 allocs` when gathering was introduced, and that
recurring cost was part of the argument for it. It has since been removed in two steps.

First, the coalescing buffer was a local handed away whole with `freeze()`, so every pass
rebuilt it; hoisting it and handing over `split().freeze()` let `bytes` reclaim the capacity,
which is what brought the row to zero. What that was worth in time — about 4–7% for the
completion transport — is in
[`findings/coalescing-buffer-reuse.md`](findings/coalescing-buffer-reuse.md).

Second — and this changes no number in the table — the write primitive split by I/O model, and
the readiness coalesced drain stopped transferring ownership at all: it lends the driver's
buffer through `write_borrowed` and clears it, so there is no `split().freeze()` on that path
and no `is_empty()` guard around one. **The allocation count does not move**, because
`split().freeze()` never heap-allocated in steady state — it split a handle out of capacity
the buffer already had. What its removal drops is the pair of atomic refcount operations that
creating and dropping that handle cost, which an allocation counter cannot see and which no
benchmark here isolates. The claim is structural, not measured, and is recorded as such in
`the_coalesced_write_path_reuses_its_coalescing_buffer`.

The *completion* coalesced drain keeps both the transfer and the `is_empty()` guard, because a
completion transport genuinely needs to own the buffer for the duration of the operation. The
guard's original rationale — dodging atomics on passes that do not use the path — is therefore
still true where it now lives, and false only where it no longer is.

What the coalesced path still pays, inherently and on both models, is a **copy** of every
outgoing octet into the driver's buffer.

## Write counts for a handed-over body

Pinned by
`http_shared_body.rs::handing_a_body_over_collapses_the_write_count_on_the_gathering_path`,
for one upload, push path → shared path:

| Body | Push | Shared |
| --- | --- | --- |
| 0 B | 1 | 1 |
| 1 KiB | 2 | 1 |
| 64 KiB | 5 | 2 |
| 1 MiB | 65 | 17 |

These counts are the mechanism behind the timings in
[`findings/handing-bodies-over.md`](findings/handing-bodies-over.md), and they are the reason
that finding's gain vanishes exactly at 0 B, where the ratio is 1.

## Why counts rather than times

So the separating column is the write count, and it is a syscall count. Gathering reaches the
old borrowed path's zero allocation and zero copy of large blocks at the coalescing path's
write count.

The modest size of the coalescing-buffer gain is worth understanding rather than glossing.
Twelve allocations per pass sounds substantial, but a same-size `malloc`/`free` pair under
glibc's thread cache is tens of nanoseconds, so twelve is well under a microsecond against a
62 µs pass. What actually costs something is the *growth*: rebuilding the buffer from empty
each pass re-copies its contents at every doubling, which is why the gain appears on the body
sweep and not in concurrency. This is a good illustration of why the counts on this page are
pinned as a *property* rather than treated as a proxy for time — and why they belong here
rather than in a per-machine run file.
