# `transport_body_throughput`

**Family:** real socket — `tests/ngnet-h2-bench/benches/transport_body_throughput.rs`

A request/response body sweep on a persistent loopback TCP connection, with
`Throughput::Bytes` so Criterion reports MB/s.

```sh
taskset -c 3 cargo bench -p ngnet-h2-bench --bench transport_body_throughput
```

## What it measures

Payload movement with the kernel in the way: copies, frame serialisation, flow control, and
the write strategy each arm is capable of. The server echoes the body, so each iteration moves
`size` bytes up and `size` back; throughput is normalised to one body's worth. The sweep
reuses the duplex family's points so the two are comparable in shape.

## Arms and parameters

| Arm | Stack | I/O model |
| --- | --- | --- |
| `ngnet-h2-compio` | this crate | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | tokio, epoll (readiness) |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**; 0 B is reported per-iteration rather than as a
meaningless `Throughput::Bytes(0)`.

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

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
