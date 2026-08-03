# Benchmarks

`crates/nghttp2-bench` compares this repo's async HTTP/2 client and server against
[hyper](https://hyper.rs)'s HTTP/2, both driven on tokio over a `tokio::io::duplex` — an
in-memory pipe, no sockets. It is a [Criterion](https://bheisler.github.io/criterion.rs/)
harness: latency comes from Criterion's per-iteration timing, and throughput is derived by
putting a known number of requests or bytes in each iteration and declaring it with
`Throughput::Elements` / `Throughput::Bytes`.

The crate is `publish = false` and lives outside `nghttp2` for the same reason
`nghttp2-tests` does: the wrapper takes exactly one dependency and no dev-dependencies, so
anything needing a third-party stack — here, hyper — belongs in a crate of its own.

## Running

```sh
# All three benches.
cargo bench -p nghttp2-bench

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

## The three benches

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

## What these numbers do and do not mean

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
