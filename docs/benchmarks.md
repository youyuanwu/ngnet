# Benchmarks

`crates/nghttp2-bench` holds two [Criterion](https://bheisler.github.io/criterion.rs/)
benchmark families, which answer different questions and must not be read as one:

- **The duplex family** — this stack against [hyper](https://hyper.rs), both on tokio over a
  `tokio::io::duplex`, an in-memory pipe with no sockets. Varies the *HTTP/2
  implementation*, holding I/O constant, and deletes the kernel entirely.
- **The real-socket family** — three arms over real loopback TCP, varying the HTTP/2
  implementation *and* the I/O model. The `transport_*` benches.

Between them they fill in the whole matrix of stack against I/O model:

| | duplex (no kernel) | tokio (epoll) | compio (io_uring) |
| --- | --- | --- | --- |
| **`nghttp2`** | `ngrs` | `ngrs-tokio` | `ngrs-compio` |
| **hyper** | `hyper` | `hyper-tokio` | n/a — hyper has no completion transport |

The empty cell is not an omission: hyper's connection types are built on tokio's
readiness-based `AsyncRead`/`AsyncWrite`, so there is no hyper-on-io_uring arm to run. The
duplex column has no compio entry for a different reason — a `tokio::io::duplex` has no file
descriptor, so no completion runtime can attach to one at all. That is precisely why the
second family uses real sockets.

**Only compare within a column, or within a row.** The two families measure different units
of work, so `ngrs` and `ngrs-tokio` are not two measurements of one thing and the duplex
numbers cannot be used to chain a comparison across to the socket ones.

In both families, latency comes from Criterion's per-iteration timing, and throughput is
derived by putting a known number of requests or bytes in each iteration and declaring it
with `Throughput::Elements` / `Throughput::Bytes`.

The crate is `publish = false` and lives outside `nghttp2` for the same reason
`nghttp2-tests` does: the wrapper takes exactly one dependency and no dev-dependencies, so
anything needing a third-party stack — here, hyper — belongs in a crate of its own.

## Running

```sh
# Everything. The real-socket benches need the completion feature and a host with io_uring.
cargo bench -p nghttp2-bench

# The three-arm real-socket comparison, on one pinned core so the numbers are comparable.
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

## The real-socket benches: three arms

`transport_serial_latency`, `transport_concurrent_throughput` and `transport_body_throughput`
run the same workload over a real loopback TCP connection with three arms:

| Arm | Stack | I/O model |
| --- | --- | --- |
| `ngrs-compio` | this crate | compio, io_uring (completion) |
| `ngrs-tokio` | this crate | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | tokio, epoll (readiness) |

**Read them pairwise, never as a ranking.** Each pair isolates something different, and only
two of the three pairs isolate anything at all:

- **`ngrs-compio` against `ngrs-tokio`** — same stack, different I/O model. This is the
  completion-against-readiness question.
- **`ngrs-tokio` against `hyper-tokio`** — same I/O model, different stack. This is the
  duplex family's question asked again with the kernel put back.
- **`ngrs-compio` against `hyper-tokio`** — *both* differ. It is the honest end-to-end "the
  fastest configuration here against the reference implementation" number, and nothing in it
  can be attributed to either axis alone.

Everything else is held still across all three: same request, same headers, same protocol
settings (the matched table below applies to `hyper-tokio` exactly as it does to the duplex
`hyper` arm — both go through the same builder helpers), same echo handler, same draining,
same number of spawned tasks, `TCP_NODELAY` on all six endpoints, one worker thread per arm.

They require the `completion` feature and a host with io_uring. The compio arm asserts it
obtained `DriverType::IoUring` and aborts rather than publishing numbers from anything else,
and prints the backend alongside the results — a benchmark result outlives the manifest that
produced it.

### What was measured

Two things were measured here at different times, and the second changed the first. The
history is kept rather than overwritten, because the sequence is the point: a benchmark that
is only ever reported after the fact teaches nothing about how its conclusion was reached.

#### The three-arm comparison, before gathering existed

Medians on one pinned core (`taskset -c 3`), backend confirmed `IoUring`, reproduced across
two independent runs. `ngrs-tokio` here elects the **borrowed** write path, which is what
`main` did at the time:

| Measure | `ngrs-compio` | `ngrs-tokio` (borrowed) | `hyper-tokio` |
| --- | --- | --- | --- |
| Serial latency, empty body | 26.2 µs | **23.9 µs** | 26.1 µs |
| Concurrent, N=1 | 33–36 Kelem/s | **37–39 Kelem/s** | 36 Kelem/s |
| Concurrent, N=8 | **121–122 Kelem/s** | 59–62 Kelem/s | 111–114 Kelem/s |
| Concurrent, N=64 | **160–161 Kelem/s** | 65–67 Kelem/s | 143–159 Kelem/s |
| Body 1 KiB | **28–29 MiB/s** | 17–18 MiB/s | 22–24 MiB/s |
| Body 64 KiB | **411–415 MiB/s** | 352–360 MiB/s | 356–361 MiB/s |
| Body 1 MiB | 418–435 MiB/s | 449–481 MiB/s | **526–541 MiB/s** |

**The third arm overturned this file's previous conclusion.** Before hyper was measured on a
real socket, the compio-against-tokio pair looked like a clean result about the I/O model:
io_uring roughly 2.3× at N=64, therefore completion I/O multiplexes better than readiness
I/O. `hyper-tokio` falsifies that. hyper reaches 143–159 Kelem/s at N=64 on *epoll* — within
noise of compio's 160 — so almost none of the gap can be the I/O model, because hyper closes
almost all of it without changing the I/O model at all.

What the gap actually is: **the number of write syscalls per pass.** The tokio transport
elected the borrowed path and issued a `write(2)` per session block; the completion transport
structurally cannot borrow and so coalesced; hyper buffers outbound bytes and flushes in large
writes, which is the same strategy by another name. So the two fast arms were the two
coalescing arms, and the slow arm was the one writing per block — a cost invisible over a
duplex, dominant over a socket, and growing with the number of multiplexed streams because
each stream adds blocks to the pass.

This was confirmed directly rather than inferred. Flipping *only* `TokioWriter::write_borrowed`
to return `None`, changing nothing else, moved `ngrs-tokio` by **+95% at N=8 and +128% at
N=64** (to ~152 Kelem/s), putting it level with compio and ahead of hyper.

#### What that framing got wrong, and the gathering path

The obvious reading — few syscalls or zero allocation, pick one — was recorded as an open
trade. It was **false**, and the reason given for it was false too: that gathering blocks into
one vectored write was "closed off by the session invalidating each block when the next is
requested". Two facts were conflated. libnghttp2 recycles its serialisation buffer at
frame-item boundaries, and `Session::send` hands back a slice borrowing the session, so at
most one block is live at a time. That forecloses gathering blocks **with each other** —
nothing more. A live block gathers perfectly well with memory the driver already owns.

`TransportWrite::write_vectored` does exactly that: small blocks accumulate into a
driver-owned buffer reused across passes, and a block at or above `VECTORED_THRESHOLD` goes
out as the second region of a two-region `writev`, never copied.

#### After: the gathering path measured

`main` @ `c8dd79c` against the gathering branch, `taskset -c 3`, benchmarks pre-built so
compilation never contends with measurement, two repetitions per side, run-to-run spread under
2.5% on the concurrency arms. Only `ngrs-tokio` changed; the other two arms are unchanged code
and serve as drift controls.

| Measure | `ngrs-tokio` before (borrowed) | `ngrs-tokio` after (gathering) | change |
| --- | --- | --- | --- |
| Concurrent, N=8 | 129.05 µs (62.0 Kelem/s) | 61.63 µs (**129.8 Kelem/s**) | **−52.2%** |
| Concurrent, N=64 | 937.32 µs (68.3 Kelem/s) | 385.51 µs (**166.0 Kelem/s**) | **−58.9%** |
| Concurrent, N=1 | 25.16 µs | 25.68 µs | +2.1%, within drift |
| Body 1 KiB | 52.33 µs (18.6 MiB/s) | 44.53 µs (21.9 MiB/s) | −14.9% |
| Body 64 KiB | 165.05 µs (379 MiB/s) | 141.33 µs (**442 MiB/s**) | −14.4% |
| Body 1 MiB | 2018.73 µs (495 MiB/s) | 1829.76 µs (547 MiB/s) | −9.4%, but see below — treat as neutral |

In the same runs `ngrs-compio` measured 61.85 µs at N=8 and 379.83 µs at N=64, and
`hyper-tokio` 67.78 µs and 391.27 µs — so **the tokio transport is now at parity with io_uring
and slightly ahead of hyper**, having been 2.1× and 2.4× slower than compio at those points.
At 1 MiB the three arms measured 547 (gathering tokio), 531 (hyper) and 482 (coalescing
compio) MiB/s in the same runs — but see the caveat below before reading an ordering into the
first two, which are within this arm's run-to-run spread of each other.

**Why the body arms move, and why the 1 MiB figure should not be believed.** The explanation
first written here was wrong and is worth recording as such: it claimed libnghttp2 emits each
9-byte DATA frame header as its own block, so that the borrowed path wrote header and payload
separately and gathering halved the count. Dumping the actual block sizes falsifies it —
libnghttp2 hands back the header *already joined* to its payload, as a single 16393-byte block
(16384 + 9). There is no separate header write to fold.

The real arithmetic follows from the block distribution, which is sharply bimodal: control and
`HEADERS` blocks are ≤ ~73 bytes, DATA blocks are 16392–16393. Only the small ones accumulate,
so what gathering saves on a body upload is the *`HEADERS` block*, folded into the first DATA
frame's `writev`, and nothing else — every DATA block already exceeds the threshold and goes
out as its own single-region call either way:

| Body | Borrowed writes | Gathering writes | Reduction |
| --- | --- | --- | --- |
| 1 KiB | 2 | **1** | 50% |
| 64 KiB | 5 | **4** | 20% |
| 1 MiB | 65 | **64** | 1.5% |

That matches the measured −14.9% and −14.4% at 1 KiB and 64 KiB. It does **not** explain
−9.4% at 1 MiB, where only one syscall in sixty-five is saved. That arm is also the noisiest in
the suite — 10.2% spread between the two baseline repetitions alone — so the honest reading is
that **1 MiB is neutral, within noise**, which is exactly what gathering was adopted to achieve
there. The goal at large bodies was to avoid the regression coalescing would have caused by
copying, not to produce a gain, and a gain should not be claimed merely because the number came
out that way.

**Two arms first appeared to regress, and both were drift.** Serial latency showed +6.8% and
empty-body +5.1% under a design that ran both baseline repetitions and then both branch
repetitions. That design cannot separate a real effect from machine drift, and this machine
drifts: across one such session `hyper-tokio` moved 5.1% on serial latency and 9.9% at 1 MiB
*without its code changing*. Re-measured with the branches interleaved (baseline, branch,
baseline, branch) and the unchanged arms used as controls, serial latency moved +1.3% on the
changed arm against +4.5% and +1.4% on the two controls — i.e. the changed arm moved *less*
than either unchanged one — and the empty-body sign inverted to −4.7% against −0.9% and −0.6%.
Neither regression survives. The lesson is recorded rather than the first numbers: **grouped
A/B designs are not trustworthy on this machine, and unchanged arms are the cheapest available
control.**

`ngrs-compio`, which does not implement `write_vectored`, moved −0.2% (N=8), −0.7% (N=64),
+0.9% (serial) and +0.2% (1 MiB) — inert, as required.

#### Allocation, counted rather than timed

From `crates/nghttp2/tests/http_zero_alloc.rs`, exact counts per driver pass in steady state:

| Strategy | Single upload | 8 multiplexed streams |
| --- | --- | --- |
| Owned (coalescing) | 4 allocs / 1 write | 12 allocs / 1 write |
| Borrowed | 0 allocs / 4 writes | 0 allocs / **513 writes** |
| **Gathering** | **0 allocs / 4 writes** | **0 allocs / 1 write** |

Gathering strictly dominates both: the borrowed path's zero allocation with the coalescing
path's write count. The 513-to-1 collapse is the mechanism behind the −58.9% at N=64. This is
also why the trade the previous section framed turned out not to exist — no values judgement
about the library was needed, because nothing had to be given up.

What survives from the original three-arm comparison:

- **compio still leads on small and medium bodies** over hyper as well, so that lead was never
  merely a coalescing artefact.
- **The empty-body row remains a near-tie across all three**, the reassuring control: with
  almost no I/O to do, three stacks and two I/O models converge, as they should.
- **`ngrs-tokio` remains the fastest arm for a single empty-body round trip**, and gathering
  did not disturb that — at N=1 there is nothing to gather, so the path costs nothing.

### Confounds, and which way each pushes

Each is named with its direction, because a number without its bias is not evidence:

- **The write-path asymmetry — was the dominant effect, and is now largely removed.** It is
  kept here because it is the reason the arms ever diverged, and because it still applies to
  the completion arm. The tokio transport now gathers (zero allocation, one `writev` per
  pass); the completion transport structurally cannot borrow or gather a session block, since
  the kernel must own the buffer for the duration, so it still coalesces and still pays a copy;
  hyper coalesces by buffering. Before gathering, this **favoured the coalescing arms wherever
  syscalls dominated** and accounted for the entire N=8/N=64 spread. With the tokio arm no
  longer writing per block, what remains of the confound is the **copy** the completion arm
  pays and the other two do not, which biases against compio on large bodies.
- **Loopback, not a network interface.** No real network latency, no device interrupts, no
  driver work — precisely the costs io_uring exists to amortise. This **biases against
  compio**; a real NIC would be expected to widen its lead rather than narrow it. Nothing
  here licenses a claim about what these transports do on a real network.
- **Scheduler non-separability.** compio is thread-per-core and `!Send`; tokio is
  work-stealing. All arms are held to one worker thread and one pinned core, which is as
  close as they can be brought, but the runtime and the I/O model are not separable in the
  compio arm — a residual scheduler difference is inseparably mixed into every compio number
  above. The two tokio arms share a runtime type, so the `ngrs-tokio`/`hyper-tokio` pair is
  free of this one, which is another reason that pair carries most of the weight here.

Controlled rather than merely disclosed: `TCP_NODELAY` is set explicitly on all six endpoints
(both sides of all three arms), since Nagle meeting delayed ACK would dominate a small-request
benchmark and say nothing about either axis; each runtime gets exactly one worker thread, and
each arm gets its own runtime so no arm's idle connection driver sits in another's scheduler;
and pinning is left to external `taskset` because compio can pin natively while tokio cannot,
so pinning one side would manufacture the asymmetry the control exists to remove.

## What these numbers do and do not mean

The duplex benches delete the kernel entirely, so they measure protocol and wrapper CPU work
rather than performance. The transport benches put the kernel back but keep it on loopback, so
they measure syscall and scheduling behaviour rather than anything a network would do. Neither
family measures CPU time or memory: Criterion reports wall-clock, so a stack burning more CPU
for the same wall time looks identical here.

Read them as a measure of **protocol, wrapper and syscall CPU work**, and nothing else.

- **The duplex removes the kernel.** No syscalls, no sockets, no network. Real-world
  performance is dominated by the things that family deletes — as the write-path finding
  above demonstrates concretely: a strategy that is free over a duplex costs a factor of two
  over a socket, and the duplex benches cannot see it. A change that helps there may be
  invisible on a real socket, and a change that hurts there may not matter.
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

- **Outbound write batching.** hyper buffers outbound bytes and flushes in large writes, sized
  by `max_send_buf_size`; this crate has no such knob, and reaches the same end differently.
  Until the gathering path existed, the tokio adapter wrote each session block separately —
  zero-copy and zero-alloc but several syscalls per pass — and this was **the** unmatched
  setting that mattered more than everything matched, accounting for the whole
  `ngrs-tokio`/`hyper-tokio` concurrency gap. It is now largely matched in effect if not in
  mechanism: this crate emits one `writev` per pass where hyper emits one buffered `write`,
  and hyper still chains large payloads uncopied much as gathering does. The residual
  difference is a threshold (`VECTORED_THRESHOLD` = 256 here, `CHAIN_THRESHOLD` = 256 in `h2`
  when vectored). Note that `tokio::io::duplex` also reports `is_write_vectored() == true`, so
  the duplex family exercises the gathering path too — its `ngrs` arm is not measuring the old
  per-block behaviour.
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
