# The write path, and how gathering closed the gap

**Measurements:** [`01-three-arm-baseline`](../data/legacy-dev-host/01-three-arm-baseline.md),
[`02-gathering-path`](../data/legacy-dev-host/02-gathering-path.md) — legacy development host.

Two things were measured here at different times, and the second changed the first. The
history is kept rather than overwritten, because the sequence is the point: a benchmark that
is only ever reported after the fact teaches nothing about how its conclusion was reached.

## The third arm overturned the previous conclusion

Before hyper was measured on a real socket, the compio-against-tokio pair looked like a clean
result about the I/O model: io_uring roughly 2.3× at N=64, therefore completion I/O
multiplexes better than readiness I/O.

`hyper-tokio` falsified that. hyper reached 143–159 Kelem/s at N=64 on *epoll* — within noise
of compio's 160 — so almost none of the gap could be the I/O model, because hyper closed
almost all of it without changing the I/O model at all.
([The run.](../data/legacy-dev-host/01-three-arm-baseline.md))

## What the gap actually was

**The number of write syscalls per pass.** The tokio transport, at the time, elected the
borrowed path and issued a `write(2)` per session block; the completion transport structurally
cannot borrow and so coalesced; hyper buffers outbound bytes and flushes in large writes, which
is the same strategy by another name. So the two fast arms were the two coalescing arms, and
the slow arm was the one writing per block — a cost invisible over a duplex, dominant over a
socket, and growing with the number of multiplexed streams because each stream adds blocks to
the pass.

This was confirmed directly rather than inferred. Flipping *only* `TokioWriter`'s borrowed
write off — at the time, by returning `None`; later, by setting `WritePolicy::Coalesced`
instead of the default `WritePolicy::Gathered`; and now, since the drain follows the transport,
by making `TokioWriter::is_write_vectored` return `false` — changing nothing else, moved
`ngnet-h2-tokio` by **+95% at N=8 and +128% at N=64** (to ~152 Kelem/s), putting it level with
compio and ahead of hyper. Those numbers are the original measurement and were not re-measured
when the way of reaching the same arm changed.

## The trade that turned out not to exist

The obvious reading — few syscalls or zero allocation, pick one — was recorded as an open
trade. It was **false**, and the reason given for it was false too: that gathering blocks into
one vectored write was "closed off by the session invalidating each block when the next is
requested". Two facts were conflated. libnghttp2 recycles its serialisation buffer at
frame-item boundaries, and `Session::send` hands back a slice borrowing the session, so at
most one block is live at a time. That forecloses gathering blocks **with each other** —
nothing more. A live block gathers perfectly well with memory the driver already owns.

`BorrowedWrite::write_vectored` does exactly that: small blocks accumulate into a
driver-owned buffer reused across passes, and a block at or above `VECTORED_THRESHOLD` goes
out as the second region of a two-region `writev`, never copied.

## Gathering, measured

Only `ngnet-h2-tokio` changed; the other two arms were unchanged code and served as drift
controls. Full tables in [the run](../data/legacy-dev-host/02-gathering-path.md); the headline
is **−52.2% at N=8** (62.0 → 129.8 Kelem/s) and **−58.9% at N=64** (68.3 → 166.0 Kelem/s),
with N=1 unmoved at +2.1%, within drift.

In the same runs `ngnet-h2-compio` measured 61.85 µs at N=8 and 379.83 µs at N=64, and
`hyper-tokio` 67.78 µs and 391.27 µs — so **the tokio transport ended at parity with io_uring
and slightly ahead of hyper**, having been 2.1× and 2.4× slower than compio at those points.

> **What the 68.3 is, and is not.** It is the *per-block* drain — the removed `PerRegion`
> shape, one `write(2)` per session block, no accumulation. It is **not** emulated gathering,
> and quoting it as gathering's cost is a mistake this documentation made once and corrects
> here and in `docs/h2/design.md`. Emulated gathering accumulates in the driver *first*, so
> the small blocks collapse into one region before the emulating loop sees them; that is why
> `http_zero_alloc.rs` measures the emulating and native rows identically.
>
> Read together with the 166.0 for gathering against ~152 for the coalescing arms, the
> ordering at N=64 on a natively-gathering `TcpStream` is: gathering **166.0**, coalescing
> ~152, per-block 68.3. **On this workload a natively-gathering readiness transport does not
> prefer coalescing**, which is why a natively-gathering transport is asked to declare itself
> and both shipped adapters do. That is one workload — 64 concurrent streams of small blocks
> over a loopback `TcpStream` — and it is the workload the declaration is *worth making* for,
> not a proof about every readiness transport or every traffic shape. Note also that this run
> swept a readiness `TcpStream` and never swept compio, so the completion side of the question
> is unmeasured.

## Why the body arms moved, and why 1 MiB should not be believed

The explanation first written was wrong and is worth recording as such: it claimed libnghttp2
emits each 9-byte DATA frame header as its own block, so that the borrowed path wrote header
and payload separately and gathering halved the count. Dumping the actual block sizes
falsifies it — libnghttp2 hands back the header *already joined* to its payload, as a single
16393-byte block (16384 + 9). There is no separate header write to fold.

The real arithmetic follows from the block distribution, which is sharply bimodal: control and
`HEADERS` blocks are ≤ ~73 bytes, DATA blocks are 16392–16393. Only the small ones accumulate,
so what gathering saves on a body upload is the *`HEADERS` block*, folded into the first DATA
frame's `writev`, and nothing else — every DATA block already exceeds the threshold and goes
out as its own single-region call either way:

| Body | Writes without accumulation | Gathering writes | Reduction |
| --- | --- | --- | --- |
| 1 KiB | 2 | **1** | 50% |
| 64 KiB | 5 | **4** | 20% |
| 1 MiB | 65 | **64** | 1.5% |

That matches the measured −14.9% and −14.4% at 1 KiB and 64 KiB. It does **not** explain
−9.4% at 1 MiB, where only one syscall in sixty-five is saved. That arm is also the noisiest in
the suite — 10.2% spread between two baseline repetitions alone — so the honest reading is that
**1 MiB is neutral, within noise**, which is exactly what gathering was adopted to achieve
there. The goal at large bodies was to avoid the regression coalescing would have caused by
copying, not to produce a gain, and a gain should not be claimed merely because the number came
out that way.

## Two apparent regressions that were drift

Serial latency showed +6.8% and empty-body +5.1% under a grouped A/B design. Neither survived
an interleaved re-measurement. That episode is what the measurement rules in
[`../controls.md`](../controls.md) are made of, and the lesson is recorded there rather than
the first numbers.

`ngnet-h2-compio`, which cannot implement `write_vectored` — it is a completion transport, and
the borrowed gathering write is unavailable to it — moved −0.2% (N=8), −0.7% (N=64), +0.9%
(serial) and +0.2% (1 MiB): inert, as required.

## What this leaves standing

- **The separating column is the write count, and it is a syscall count.** Gathering reaches
  the old borrowed path's zero allocation and zero copy of large blocks at the coalescing
  path's write count; the 513-to-1 collapse in
  [`../allocation-counts.md`](../allocation-counts.md) is the mechanism behind the −58.9% at
  N=64.
- **compio led on small and medium bodies** over hyper as well, so that lead was never merely
  a coalescing artefact.
- **The empty-body row was a near-tie across all three arms**, the reassuring control: with
  almost no I/O to do, three stacks and two I/O models converge, as they should.
- **`ngnet-h2-tokio` was the fastest arm for a single empty-body round trip**, and gathering
  did not disturb that — at N=1 there is nothing to gather, so the path costs nothing.

## What a new machine should reproduce

The percentages above belong to the legacy host. On new hardware the claim is falsifiable in
this shape:

1. At N=8 and N=64, `ngnet-h2-tokio` (gathering) sits **level with or ahead of** both
   `ngnet-h2-compio` and `hyper-tokio` — not 2× behind.
2. At N=1 the three arms are within noise of one another, and so is empty-body serial latency.
3. Forcing `is_write_vectored` to `false` on the tokio adapter costs a large, unmistakable
   fraction at N=64 and roughly nothing at N=1. This is the direct test of the mechanism, and
   is the single most valuable run to repeat on any new host.
4. On the body sweep, a write-side change moves 1 KiB most, 64 KiB less, and 1 MiB
   indistinguishably from zero — the ratios in the table above, not a uniform gain.
