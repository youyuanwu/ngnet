# What these numbers do and do not mean

The duplex benches delete the kernel entirely, so they measure protocol and wrapper CPU work
rather than performance. The transport benches put the kernel back but keep it on loopback, so
they measure syscall and scheduling behaviour rather than anything a network would do. Neither
family measures CPU time or memory: Criterion reports wall-clock, so a stack burning more CPU
for the same wall time looks identical here.

The Quinn HTTP/3 benches also use loopback, but over UDP with QUIC encryption and congestion
control active. They compare two complete implementations on one transport under a controlled
local workload; they do not predict internet latency, loss recovery, multi-core scaling, or
tail behaviour under load.

Read them as a measure of **protocol, wrapper and syscall CPU work**, and nothing else.

- **The duplex removes the kernel.** No syscalls, no sockets, no network. Real-world
  performance is dominated by the things that family deletes — as the write-path finding
  demonstrates concretely: a drain that is free over a duplex costs a factor of two over a
  socket, and the duplex benches cannot see it. A change that helps there may be invisible on
  a real socket, and a change that hurts there may not matter. See
  [`findings/write-path-and-gathering.md`](findings/write-path-and-gathering.md).
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
  property of measuring CPU-bound work on one core, not a property of any stack or protocol
  here.
- **A cross-protocol arm is one implementation of each protocol, not the protocols.** The
  `ngnet-qmux-h3` arms measure this workspace's HTTP/3-over-QMux join as it stands today,
  against this workspace's HTTP/2 stack, on a byte stream neither protocol was designed
  around. What that licenses and what it does not is set out in [`README.md`](README.md); what
  was and was not held equal between them is [`configuration.md`](configuration.md).

## The noise caveat

Without pinning to a core and disabling turbo and frequency scaling, run-to-run variance
routinely exceeds the difference being looked for. In development runs on a shared machine,
both stacks moved together by ~15% between two runs minutes apart — enough to flip the sign
of any close comparison. Treat a single run's absolute numbers as indicative only; trust
deltas measured back-to-back on a quiet, pinned core, and re-run anything whose confidence
intervals overlap before believing it.

This is not a caveat in the abstract. It is the reason the recorded results are paired deltas
with named drift controls rather than tables of absolute figures, and the reason two apparent
regressions in PR #7 turned out to be nothing at all. [`controls.md`](controls.md) sets out
the design that survived contact with it.

## Absolute numbers do not travel between machines

Every measurement in [`data/`](data/) is filed under the host that produced it, and the
figures in one host's directory may not be tabulated with another's. Nothing in this harness
is normalised for CPU model, kernel, or io_uring implementation, and the effects being looked
for here — syscall counts, scheduler wakeups, cache behaviour on a 16 KiB frame buffer — are
exactly the ones that move most between hosts.

What does travel is the *shape* of a result: a paired delta, its sign, its magnitude relative
to the drift controls measured in the same session, and the mechanism advanced to explain it.
That is what [`findings/`](findings/) records, and it is stated in a form a new host can
falsify.

## CPU and memory are deliberately not covered yet

Criterion gives neither, and this harness makes no attempt at them. The gap is known and
left open on purpose:

- **Allocation profile** — [`dhat-rs`](https://docs.rs/dhat) would give per-exchange
  allocation counts and peak heap, which is what actually distinguishes two stacks that post
  the same wall-clock time here. It also complements `tests/http_zero_alloc.rs`, which pins
  *steady-state* zero allocation but says nothing about the per-stream setup cost. The counts
  that test does pin are in [`allocation-counts.md`](allocation-counts.md).
- **Throughput, tail latency, CPU and peak RSS under real concurrency** — `h2load` (already
  vendored under `crates/ngnet-h2-sys/vendor/nghttp2/src/`) driving a real socket server, under `perf stat`, would
  measure all four under load the way this harness structurally cannot. That is the pass that
  would turn "faster over a duplex" into "faster on a wire."
