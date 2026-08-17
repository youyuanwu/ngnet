# 01 — The three socket arms, before gathering existed

**Machine:** [`legacy-dev-host`](README.md)
**Date:** 2026-08-03
**Commit:** `c8dd79c` — *Benchmark hyper over a real socket, and correct what the last one
concluded* (#6)
**Cases:** [`transport_serial_latency`](../../cases/transport-serial-latency.md),
[`transport_concurrent_throughput`](../../cases/transport-concurrent-throughput.md),
[`transport_body_throughput`](../../cases/transport-body-throughput.md)
**Command:** `taskset -c 3 cargo bench -p ngnet-bench --bench transport_*`
**Repetitions:** two independent runs; the reported figures reproduced across both
**Controls:** none — this is a survey of three arms, not an A/B. Ranges are given instead of
point estimates for exactly that reason.
**Exclusions:** none

> **Editorial note, 2026-08-17.** The command above names the benchmark crate as it is
> called now. At commit `c8dd79c` it was still named for HTTP/2 alone; it was renamed to
> `ngnet-bench` when the suite stopped being an HTTP/2-only one, and the command was
> corrected here so that it still runs. It is otherwise the one that was run, and nothing
> below it has been touched.

## What was being asked

How the two I/O models and the two HTTP/2 stacks compare over a real loopback socket, with the
third arm — hyper on epoll — measured for the first time. The previous conclusion on record
had been drawn from the compio/tokio pair alone.

## Results

Medians, backend confirmed `IoUring`. `ngnet-h2-tokio` here elects the **borrowed** write path,
one `write(2)` per session block, which is what `main` did at the time. Bold marks the best arm
in each row.

| Measure | `ngnet-h2-compio` | `ngnet-h2-tokio` (borrowed) | `hyper-tokio` |
| --- | --- | --- | --- |
| Serial latency, empty body | 26.2 µs | **23.9 µs** | 26.1 µs |
| Concurrent, N=1 | 33–36 Kelem/s | **37–39 Kelem/s** | 36 Kelem/s |
| Concurrent, N=8 | **121–122 Kelem/s** | 59–62 Kelem/s | 111–114 Kelem/s |
| Concurrent, N=64 | **160–161 Kelem/s** | 65–67 Kelem/s | 143–159 Kelem/s |
| Body 1 KiB | **28–29 MiB/s** | 17–18 MiB/s | 22–24 MiB/s |
| Body 64 KiB | **411–415 MiB/s** | 352–360 MiB/s | 356–361 MiB/s |
| Body 1 MiB | 418–435 MiB/s | 449–481 MiB/s | **526–541 MiB/s** |

## A follow-up on the same host

Flipping *only* the tokio adapter's borrowed write off — everything else unchanged — moved
`ngnet-h2-tokio` **+95% at N=8 and +128% at N=64**, to ~152 Kelem/s: level with compio and
ahead of hyper. This is the direct confirmation that the gap was the write path and not the
I/O model.

## What this establishes

- **The third arm overturned the previous conclusion.** compio's ~2.3× lead over tokio at
  N=64 could not be the I/O model, because hyper reaches 143–159 Kelem/s on *epoll*, within
  noise of compio's 160.
- The separating variable is **write syscalls per pass**: the two fast arms were the two
  coalescing arms, and the slow arm was the one writing per block.
- The empty-body row is a near-tie across all three arms, which is the expected control
  behaviour when there is almost no I/O to do.

## What it does not

- **No drift controls.** Nothing here is an A/B against a saved baseline, so single-figure
  differences within a few percent carry no weight; the ranges are the honest reading.
- The 1 MiB row's ordering was later shown to sit inside this arm's run-to-run spread on this
  host. It should not be read as hyper leading at large bodies.
- Nothing here says what compio would do on a real NIC, where the costs io_uring exists to
  amortise are present. Loopback biases against it.

Superseded for the tokio arm by [02-gathering-path](02-gathering-path.md). Conclusion drawn in
[`../../findings/write-path-and-gathering.md`](../../findings/write-path-and-gathering.md).
