# `concurrent_throughput`

**Family:** duplex — `tests/ngnet-h2-bench/benches/concurrent_throughput.rs`

`N` requests issued together on **one** connection per iteration and awaited as a group, so
Criterion's per-iteration time covers `N` whole exchanges.

```sh
cargo bench -p ngnet-h2-bench --bench concurrent_throughput
```

## What it measures

Multiplexing that serial latency cannot show: how the per-exchange cost changes when `N`
streams are in flight on one connection. `Throughput::Elements(N)` turns the per-iteration
time into requests/sec.

## Arms and parameters

| Arm | Stack |
| --- | --- |
| `ngnet-h2` | this crate |
| `hyper` | hyper |

`N` sweeps **1, 8, 64**.

Two separately named groups run the same sweep:

- **`concurrent_throughput`** — one `current_thread` runtime. This is the deterministic
  headline: with no syscalls, a multi-threaded scheduler would only add cross-thread wakeup
  noise.
- **`concurrent_throughput_multi_thread`** — a four-worker runtime. It exists to show what
  cross-thread scheduling does to the same work, **not** to replace the single-threaded
  numbers, and the two must not be tabulated as one series.

## Reading it

- **Throughput does not scale with `N` here the way a networked server would.** On one core
  the per-request protocol CPU cost cannot be run in parallel, so multiplexing only amortises
  per-batch overhead. See [`../interpreting.md`](../interpreting.md).
- The duplex reports `is_write_vectored() == true`, so the `ngnet-h2` arm exercises the
  gathering drain — this case is not measuring the historical per-block write behaviour.
- This case was **blind to the effect that dominated the socket family**: a per-block drain
  costs nothing without a kernel. That is the clearest single illustration of what the duplex
  deletes; see [`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
