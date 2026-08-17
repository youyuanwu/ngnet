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

| Arm | Stack | I/O model |
| --- | --- | --- |
| `ngnet-h2-compio` | this crate | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | tokio, epoll (readiness) |

`N` sweeps **1, 8, 64** — the same points as the duplex family, so the two are comparable in
shape. One worker thread each (compio single-threaded, tokio `current_thread`), so no arm gets
to spread over cores the others cannot, and one runtime per arm.

## Reading it

Pairwise, as in [`transport_serial_latency`](transport-serial-latency.md): compio against
tokio isolates the I/O model, tokio against hyper isolates the stack, compio against hyper
varies both and is attributable to neither.

- **N=1 is the control point.** With one stream there is nothing to gather and nothing to
  amortise, so a write-side change should move it by roughly nothing; a change that moves N=1
  as much as N=64 is not doing what it claims.
- On one core, throughput does not multiply with `N`; see
  [`../interpreting.md`](../interpreting.md).

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
