# `transport_concurrent_throughput`

**Family:** real socket — `tests/ngnet-bench/benches/transport_concurrent_throughput.rs`

`N` requests issued together on one loopback TCP connection and awaited as a group, so
Criterion's per-iteration time covers `N` whole exchanges. `Throughput::Elements` turns that
into requests/sec.

```sh
taskset -c 3 cargo bench -p ngnet-bench --bench transport_concurrent_throughput
```

## What it measures

**Syscalls per pass, more than anything else.** Multiplexing `N` streams puts `N` streams'
worth of blocks into one driver pass, so a drain that writes per block pays `N` times over
while a drain that accumulates pays once. This is the case where that difference is visible,
and it is the case that produced the largest result in this suite — a 2.3× spread that had
nothing to do with the I/O model. See
[`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).

## Arms and parameters

| Arm | Stack | Protocol | I/O model |
| --- | --- | --- | --- |
| `ngnet-h2-compio` | this crate | HTTP/2 | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | HTTP/2 | tokio, epoll (readiness) |
| `ngnet-qmux-h3-tokio` | this crate | HTTP/3 over QMux | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | HTTP/2 | tokio, epoll (readiness) |

`N` sweeps **1, 8, 64** — the same points as the duplex family, so the two are comparable in
shape. One worker thread each (compio single-threaded, tokio `current_thread`), so no arm gets
to spread over cores the others cannot, and one runtime per arm. 64 sits below the 128
concurrent streams both stacks are configured for
([`../configuration.md`](../configuration.md)).

The QMux arm is registered immediately after `ngnet-h2-tokio` inside the concurrency loop, so
`N` is the outer loop, the arms are the inner one, and the two halves of the cross-protocol
pair are timed adjacently.

The single-threaded runtime here is not a matter of taste: the QMux join hangs at high
concurrency on a multi-worker runtime, which is why the duplex family's
[`concurrent_throughput_multi_thread`](concurrent-throughput.md) group carries no QMux arm at
all — that page records why the reason for the omission has since shifted while the omission
stands. Nothing in this file uses a multi-worker runtime, so nothing here is affected; the
defect is recorded on [`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md).

## Reading it

Pairwise, as in [`transport_serial_latency`](transport-serial-latency.md): compio against
tokio isolates the I/O model, `ngnet-h2-tokio` against `hyper-tokio` isolates the stack,
`ngnet-h2-tokio` against `ngnet-qmux-h3-tokio` isolates the protocol, and every other pair
varies two axes and is attributable to neither.

- **N=1 is the control point.** With one stream there is nothing to gather and nothing to
  amortise, so a write-side change should move it by roughly nothing; a change that moves N=1
  as much as N=64 is not doing what it claims. For the cross-protocol pair the same point does
  double duty: N=1 should reproduce
  [`transport_serial_latency`](transport-serial-latency.md), and a ratio that *grows* from
  there to N=64 is the interesting reading, because it says the difference scales with
  in-flight streams rather than with exchanges.
- **This is the case where the QMux write path was most exposed, and it is the case that has
  moved most.** The finding that gave this group its 2.3× spread was about syscalls per pass,
  and the QMux join used to offer one `IoSlice` per write to its writer — the exact pattern that
  finding identified as expensive with a kernel in the way and invisible without one. It no
  longer does, and the two families answered differently at the same parameters: this group's
  QMux arm gained 8.5% at N=64 and 7.1% at N=8 while the duplex
  [`concurrent_throughput`](concurrent-throughput.md) arm *lost* 1.8% and 2.1%
  ([`../findings/qmux-write-path.md`](../findings/qmux-write-path.md)). That sign flip is the
  clearest thing this pair of groups has produced, and it is what the group exists for. If the
  cross-protocol ratio here still exceeds the duplex family's at the same `N`, the write path is
  no longer the first place to look — the record count and the pump's fixed offer bound are, and
  [`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md) carries both.
  [`../controls.md`](../controls.md) records what is left of the confound with its direction,
  and it is disclosed rather than controlled.
- On one core, throughput does not multiply with `N`; see
  [`../interpreting.md`](../interpreting.md).

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md). Three now contain a QMux
arm — [`04-qmux-drift-baseline`](../data/xeon-8370c-azure/04-qmux-drift-baseline.md),
[`05-qmux-delivery-aliasing`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md) and
[`06-qmux-write-path`](../data/xeon-8370c-azure/06-qmux-write-path.md) — and all three are
paired comparisons of one QMux build against another, not of the QMux arm against an HTTP/2 one.
**No recorded run computes a cross-protocol ratio for this group under drift controls**, so the
readings above that turn on a ratio are still unmeasured; the HTTP/2 arms appear in those
sessions only as unchanged controls.
