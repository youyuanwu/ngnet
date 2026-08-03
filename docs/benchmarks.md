# Benchmarks

`crates/nghttp2-bench` holds two [Criterion](https://bheisler.github.io/criterion.rs/)
comparisons, which answer different questions and must not be read as one:

- **This stack against [hyper](https://hyper.rs)**, both on tokio over a `tokio::io::duplex` —
  an in-memory pipe, no sockets. Varies the *HTTP/2 implementation*, holding I/O constant.
- **Completion I/O against readiness I/O** — this stack on compio's io_uring runtime against
  the same stack on tokio, over real loopback TCP. Varies the *transport*, holding the HTTP/2
  implementation constant. The `transport_*` benches.

In both, latency comes from Criterion's per-iteration timing, and throughput is derived by
putting a known number of requests or bytes in each iteration and declaring it with
`Throughput::Elements` / `Throughput::Bytes`.

The crate is `publish = false` and lives outside `nghttp2` for the same reason
`nghttp2-tests` does: the wrapper takes exactly one dependency and no dev-dependencies, so
anything needing a third-party stack — here, hyper — belongs in a crate of its own.

## Running

```sh
# Everything. The transport benches need the completion feature and a host with io_uring.
cargo bench -p nghttp2-bench

# Completion against readiness, on one pinned core so the numbers are comparable.
taskset -c 3 cargo bench -p nghttp2-bench --bench transport_concurrent_throughput

# One at a time.
cargo bench -p nghttp2-bench --bench serial_latency
cargo bench -p nghttp2-bench --bench concurrent_throughput
cargo bench -p nghttp2-bench --bench body_throughput
```

Comparing against a saved baseline is the point of running these at all — a single run's
absolute numbers say little (see the noise caveat below), but a before/after delta on the
same machine says a great deal:

```sh
# Record a baseline before a change.
cargo bench -p nghttp2-bench -- --save-baseline before

# After the change, compare against it. Criterion reports the delta and whether it clears
# the noise threshold.
cargo bench -p nghttp2-bench -- --baseline before
```

Pin to one core and keep the machine otherwise idle, or the delta will be buried:

```sh
taskset -c 2 cargo bench -p nghttp2-bench -- --baseline before
```

## The duplex benches: this stack against hyper

- **`serial_latency`** — one request in flight at a time on a persistent connection, empty
  body. Criterion's home ground: mean/median with confidence intervals and outlier
  detection. This times the per-request headers round trip and the wrapper work around it.

- **`concurrent_throughput`** — `N` requests issued together on **one** connection per
  iteration and awaited as a group, with `Throughput::Elements(N)` so the report is
  requests/sec. `N` sweeps 1, 8, 64. A second, separately named
  `concurrent_throughput_multi_thread` group runs the same sweep on a four-worker runtime;
  it exists to show what cross-thread scheduling does to the same work, not to replace the
  deterministic single-threaded numbers.

- **`body_throughput`** — a request/response body sweep (0 B, 1 KiB, 64 KiB, 1 MiB) with
  `Throughput::Bytes` so the report is MB/s. The server echoes the body, so each iteration
  moves the payload up and back; throughput is normalised to one body's worth. This is where
  flow control and the read-buffer pool start to matter: at 1 MiB the 64 KiB initial window
  forces repeated `WINDOW_UPDATE` round trips.

## The transport benches: completion against readiness

`transport_serial_latency`, `transport_concurrent_throughput` and `transport_body_throughput`
run the *same* HTTP/2 stack over two transports — `CompioIo` on compio's io_uring runtime, and
`TokioIo` on tokio — across real loopback TCP. Everything else is held still: same request,
same headers, same `Config`, same echo handler, same draining, same number of spawned tasks.
A difference in the numbers is therefore a difference in the I/O model, not in two libraries'
protocol code.

They require the `completion` feature and a host with io_uring. The compio arm asserts it
obtained `DriverType::IoUring` and aborts rather than publishing numbers from anything else,
and prints the backend alongside the results — a benchmark result outlives the manifest that
produced it.

### What was measured

Medians on one pinned core (`taskset -c 3`), backend confirmed `IoUring`, reproduced across
independent runs:

| Measure | compio (io_uring) | tokio (epoll) | |
| --- | --- | --- | --- |
| Serial latency, empty body | 33–38 µs | **29 µs** | tokio ~15–25% faster |
| Concurrent, N=1 | ~28 Kelem/s | ~33 Kelem/s | tokio ~10% faster |
| Concurrent, N=8 | **105–123 Kelem/s** | 50–62 Kelem/s | compio ~2× |
| Concurrent, N=64 | **129–155 Kelem/s** | 55–66 Kelem/s | compio ~2.3× |
| Body 1 KiB | **20–25 MiB/s** | 13–16 MiB/s | compio ahead |
| Body 64 KiB | **283–358 MiB/s** | 241–283 MiB/s | compio ahead |
| Body 1 MiB | 369–391 MiB/s | 329–398 MiB/s | tie |

**The crossover is the finding, not either endpoint.** io_uring is *slower* for a single
request in flight and roughly twice as fast once eight or more streams are multiplexed on the
connection. One mechanism explains both: a single request has nothing to batch, so it pays
io_uring's per-operation submission cost against epoll's cheaper single syscall; at high
concurrency the ring batches many submissions and completions into far fewer syscalls than a
readiness loop can. This was checked rather than assumed — the "it is really task-scheduling
overhead" explanation was falsified by the N=1 result, where tokio wins the identical spawn
pattern.

The 1 MiB tie is consistent with the write-path asymmetry described below: tokio's borrowed
zero-copy write roughly cancels io_uring's syscall advantage once bodies are large enough for
copying to dominate.

### Confounds, and which way each pushes

Three could not be eliminated. Each is named here with its direction, because a number without
its bias is not evidence:

- **The write-path asymmetry.** The tokio transport takes the borrowed write path — the
  session's own blocks handed over directly, no allocation, no copy. A completion transport
  structurally cannot: the kernel must own the buffer for the duration, so compio takes the
  coalescing owned path, trading an allocation and a copy for fewer, larger writes. This
  **favours compio on small bodies**, where syscall count dominates, and **favours tokio on
  large ones**, where the copy does. The 1 MiB tie is this confound becoming visible.
- **Loopback, not a network interface.** No real network latency, no device interrupts, no
  driver work — precisely the costs io_uring exists to amortise. This **biases against
  compio**; a real NIC would be expected to widen its lead rather than narrow it. Nothing here
  licenses a claim about what these transports do on a real network.
- **Scheduler non-separability.** compio is thread-per-core and `!Send`; tokio is
  work-stealing. Both arms are held to one worker thread and one pinned core, which is as
  close as the two can be brought, but the runtime and the I/O model are not separable in
  either — a residual scheduler difference is inseparably mixed into every number above.

Controlled rather than merely disclosed: `TCP_NODELAY` is set explicitly on all four endpoints
(both sides of both arms), since Nagle meeting delayed ACK would dominate a small-request
benchmark and say nothing about io_uring; each runtime gets exactly one worker thread; and
pinning is left to external `taskset` because compio can pin natively while tokio cannot, so
pinning one side would manufacture the asymmetry the control exists to remove.

## What these numbers do and do not mean

The duplex benches delete the kernel entirely, so they measure protocol and wrapper CPU work
rather than performance. The transport benches put the kernel back but keep it on loopback, so
they measure syscall and scheduling behaviour rather than anything a network would do. Neither
family measures CPU time or memory: Criterion reports wall-clock, so a stack burning more CPU
for the same wall time looks identical here.

Read them as a measure of **protocol and wrapper CPU work**, and nothing else.

- **The duplex removes the kernel.** No syscalls, no sockets, no network. Real-world
  performance is dominated by the things this harness deletes. A change that helps here may
  be invisible on a real socket, and a change that hurts here may not matter there.
- **Criterion reports wall-clock time.** A stack that burns more CPU to fill the same wall
  time looks identical. Two implementations that finish an in-memory exchange in the same
  microseconds can have very different CPU and allocation costs — which is exactly what this
  harness cannot see, and why CPU and memory are called out as out of scope below.
- **Serial latency is not tail latency under load.** It is the mean cost of one exchange on
  an otherwise idle connection. The behaviour that hurts real deployments — a slow stream
  behind a head-of-line block, a burst that overflows a buffer — is not what this measures.
- **Single-threaded throughput does not scale with `N` the way a networked server would.**
  On one core the per-request protocol CPU cost cannot be run in parallel, so multiplexing
  `N` streams only amortises per-batch overhead; it does not multiply throughput. That is a
  property of measuring CPU-bound work on one core, not a property of either HTTP/2 stack.

## Matched, and unmatched, configuration

Both stacks are pinned to libnghttp2's defaults, since `nghttp2`'s async layer advertises
only two settings of its own and leaves the rest at those defaults (`config.rs`,
`driver.rs`). hyper's builders default to much larger windows and header limits, so its
builders are dialled back to match. The flow-control windows matter most: a mismatched
initial window alone can move body throughput by 2x and say nothing about either
implementation.

| Setting | Value both stacks use | How |
| --- | --- | --- |
| `INITIAL_WINDOW_SIZE` (stream) | 65535 | libnghttp2 default; hyper `initial_stream_window_size` |
| Connection window | 65535 | libnghttp2 default; hyper `initial_connection_window_size` + `adaptive_window(false)` |
| `MAX_FRAME_SIZE` | 16384 | libnghttp2 default; hyper `max_frame_size` |
| HPACK table size | 4096 | libnghttp2 default; hyper `header_table_size` |
| `MAX_CONCURRENT_STREAMS` | 128 | `Config` default; hyper `max_concurrent_streams` |
| `MAX_HEADER_LIST_SIZE` | 64 KiB | `Config` default; hyper `max_header_list_size` |
| Response `Date` header | none | hyper's `auto_date_header(false)`; this crate adds none |

What could **not** be matched:

- **Outbound coalescing (`max_send_buf_size`).** hyper buffers outbound bytes and flushes in
  larger writes; the default is large. This crate has no equivalent knob — its tokio adapter
  takes the borrowed write path, which hands each of the session's own blocks to the
  transport separately (zero-copy, zero-alloc, but several small writes per pass; see
  `docs/design.md`). Over an in-memory duplex, where a write is cheap but not free, this
  biases **large-body** throughput toward hyper. The 1 MiB result is consistent with that.
- **Optimistic stream opening.** hyper's `initial_max_send_streams` lets it open streams
  before the peer's `SETTINGS` arrives; this crate waits. This only affects the first round
  trip, so on a persistent connection it is noise.

## The noise caveat

Without pinning to a core and disabling turbo and frequency scaling, run-to-run variance
routinely exceeds the difference being looked for. In development runs on a shared machine,
both stacks moved together by ~15% between two runs minutes apart — enough to flip the sign
of any close comparison. Treat a single run's absolute numbers as indicative only; trust
deltas measured back-to-back on a quiet, pinned core, and re-run anything whose confidence
intervals overlap before believing it.

## CPU and memory are deliberately not covered yet

Criterion gives neither, and this harness makes no attempt at them. The gap is known and
left open on purpose:

- **Allocation profile** — [`dhat-rs`](https://docs.rs/dhat) would give per-exchange
  allocation counts and peak heap, which is what actually distinguishes two stacks that post
  the same wall-clock time here. It also complements `tests/http_zero_alloc.rs`, which pins
  *steady-state* zero allocation but says nothing about the per-stream setup cost.
- **Throughput, tail latency, CPU and peak RSS under real concurrency** — `h2load` (already
  vendored under `deps/nghttp2/src/`) driving a real socket server, under `perf stat`, would
  measure all four under load the way this harness structurally cannot. That is the pass that
  would turn "faster over a duplex" into "faster on a wire."
