# `transport_body_throughput`

**Family:** real socket — `tests/ngnet-bench/benches/transport_body_throughput.rs`

A request/response body sweep on a persistent loopback TCP connection, with
`Throughput::Bytes` so Criterion reports MB/s.

```sh
taskset -c 3 cargo bench -p ngnet-bench --bench transport_body_throughput
```

## What it measures

Payload movement with the kernel in the way: copies, frame serialisation, flow control, and
the write strategy each arm is capable of. The server echoes the body, so each iteration moves
`size` bytes up and `size` back; throughput is normalised to one body's worth. The sweep
reuses the duplex family's points so the two are comparable in shape.

## Arms and parameters

| Arm | Stack | Protocol | I/O model |
| --- | --- | --- | --- |
| `ngnet-h2-compio` | this crate | HTTP/2 | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | HTTP/2 | tokio, epoll (readiness) |
| `ngnet-qmux-h3-tokio` | this crate | HTTP/3 over QMux | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | HTTP/2 | tokio, epoll (readiness) |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**; 0 B is reported per-iteration rather than as a
meaningless `Throughput::Bytes(0)`. The QMux arm is registered immediately after
`ngnet-h2-tokio` inside the size loop, its counterpart on the protocol axis.

## Reading it

**This is where the write-path asymmetry bites hardest.** The two readiness arms buffer or
borrow outbound bytes in ways the completion arm structurally cannot, so a large-body
difference is partly write strategy and not purely I/O model or stack. The confound is set out
with its direction in [`../controls.md`](../controls.md).

What bounds how much any write-side change can move each point is arithmetic on the block
distribution rather than a matter of opinion — only sub-threshold blocks accumulate, and every
DATA block is already 16392–16393 bytes:

| Body | Writes without accumulation | Gathering writes | Reduction |
| --- | --- | --- | --- |
| 1 KiB | 2 | **1** | 50% |
| 64 KiB | 5 | **4** | 20% |
| 1 MiB | 65 | **64** | 1.5% |

So a write-side gain should be large at 1 KiB, moderate at 64 KiB and **absent at 1 MiB**. A
1 MiB gain therefore needs a different mechanism or it is drift — and the 1 MiB point is also
the noisiest in the suite, having shown 10.2% spread between two repetitions of an unchanged
arm. See [`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).

**That arithmetic does not carry across to the QMux arm, and reading it as though it does is
the mistake this paragraph exists to prevent.** The table above counts HTTP/2 DATA blocks. The
QMux arm's payload is cut twice: into HTTP/3 frames, then into QMux records whose maximum is
16382 bytes — a number [`../configuration.md`](../configuration.md) records as reachable from
neither stack, together with the direction it biases the comparison. So the QMux arm makes more
records for the same body than the HTTP/2 arms make frames. That used to be one of *two*
mechanisms growing with body size; the other — its join offering those records to the writer one
`IoSlice` at a time — is gone, and removing it is the largest single movement any QMux arm in
this suite has recorded for a QMux arm: −30.4% at 1 MiB and −25.9% at 64 KiB — a figure for the
whole write-path change set, not for that mechanism alone, which no run attributes on a socket arm
([`../findings/qmux-write-path.md`](../findings/qmux-write-path.md)). The record count remains
and still grows with body size. If a cross-protocol ratio here *grows* across the sweep while
the duplex family's [`body_throughput`](body-throughput.md) ratio falls, that is now the
mechanism to look at first rather than one of two;
[`../controls.md`](../controls.md) gives it a direction.

The pair that isolates the protocol is `ngnet-h2-tokio` against `ngnet-qmux-h3-tokio`, never
the compio arm against the QMux one — the latter differs in I/O model as well.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md). Three now contain a QMux
arm — [`04-qmux-drift-baseline`](../data/xeon-8370c-azure/04-qmux-drift-baseline.md),
[`05-qmux-delivery-aliasing`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md) and
[`06-qmux-write-path`](../data/xeon-8370c-azure/06-qmux-write-path.md) — and all three are
paired comparisons of one QMux build against another, not of the QMux arm against an HTTP/2 one.
**No recorded run computes a cross-protocol ratio for this group under drift controls**, so the
readings above that turn on a ratio are still unmeasured; the HTTP/2 arms appear in those
sessions only as unchanged controls.
