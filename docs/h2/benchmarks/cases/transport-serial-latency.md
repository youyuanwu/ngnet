# `transport_serial_latency`

**Family:** real socket — `tests/ngnet-h2-bench/benches/transport_serial_latency.rs`

One request in flight at a time on a persistent loopback TCP connection, empty body.

```sh
taskset -c 3 cargo bench -p ngnet-h2-bench --bench transport_serial_latency
```

## What it measures

The per-request round trip through the kernel and back, which is exactly where a completion
runtime differs from a readiness one. Empty body, so no payload movement is timed.

## Arms

| Arm | Stack | I/O model |
| --- | --- | --- |
| `ngnet-h2-compio` | this crate | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | tokio, epoll (readiness) |

Each arm gets **its own runtime**, so no arm's idle connection driver sits registered in
another's scheduler. Criterion runs the arms one at a time, each on the runtime its connection
was established on; the two runtimes never nest.

The compio arm asserts it obtained `DriverType::IoUring` and aborts rather than publishing
numbers from anything else, and prints the backend alongside the results — a benchmark result
outlives the manifest that produced it.

## Reading it pairwise, never as a ranking

Only two of the three pairs isolate anything at all:

- **`ngnet-h2-compio` against `ngnet-h2-tokio`** — same stack, different I/O model. This is
  the completion-against-readiness question.
- **`ngnet-h2-tokio` against `hyper-tokio`** — same I/O model, different stack. This is the
  duplex family's question asked again with the kernel put back.
- **`ngnet-h2-compio` against `hyper-tokio`** — *both* differ. It is the honest end-to-end
  "fastest configuration here against the reference implementation" number, and nothing in it
  can be attributed to either axis alone.

Historically **the empty-body case is a near-tie across all three arms**, and that is its most
useful property: with almost no I/O to do, three stacks and two I/O models converge, as they
should. An empty-body result that is *not* a near-tie is a signal that something outside the
protocol is being measured.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
