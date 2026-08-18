# `transport_serial_latency`

**Family:** real socket — `tests/ngnet-bench/benches/transport_serial_latency.rs`

One request in flight at a time on a persistent loopback TCP connection, empty body. Four arms:
two I/O models, two stacks and two protocols, varied one axis at a time.

```sh
taskset -c 3 cargo bench -p ngnet-bench --bench transport_serial_latency
```

## What it measures

The per-request round trip through the kernel and back, which is exactly where a completion
runtime differs from a readiness one. Empty body, so no payload movement is timed.

## Arms

| Arm | Stack | Protocol | I/O model |
| --- | --- | --- | --- |
| `ngnet-h2-compio` | this crate | HTTP/2 | compio, io_uring (completion) |
| `ngnet-h2-tokio` | this crate | HTTP/2 | tokio, epoll (readiness) |
| `ngnet-qmux-h3-tokio` | this crate | HTTP/3 over QMux | tokio, epoll (readiness) |
| `hyper-tokio` | hyper | HTTP/2 | tokio, epoll (readiness) |

Each arm gets **its own runtime**, so no arm's idle connection driver sits registered in
another's scheduler. Criterion runs the arms one at a time, each on the runtime its connection
was established on; the two runtimes never nest.

The compio arm asserts it obtained `DriverType::IoUring` and aborts rather than publishing
numbers from anything else, and prints the backend alongside the results — a benchmark result
outlives the manifest that produced it.

The QMux arm is registered **immediately after `ngnet-h2-tokio`**, the only arm it differs from
in protocol alone, so the two halves of the cross-protocol comparison are timed back to back;
[`../controls.md`](../controls.md) treats that adjacency as a control. It runs over the same
loopback TCP socket as the others — QMux is a stream-multiplexing layer over a reliable byte
stream, not a UDP transport, so nothing here swaps the socket type, and the kernel path is
common to all four arms.

The QMux arm completes one exchange inside `establish`, before anything is timed; the HTTP/2
arms do not. That keeps handshake cost out of the timed loop and is set out with its direction
of bias in [`../controls.md`](../controls.md).

## Reading it pairwise, never as a ranking

Four arms give six pairs, and only three of them isolate a single axis:

- **`ngnet-h2-compio` against `ngnet-h2-tokio`** — same stack, same protocol, different I/O
  model. This is the completion-against-readiness question.
- **`ngnet-h2-tokio` against `hyper-tokio`** — same I/O model, same protocol, different stack.
  This is the duplex family's question asked again with the kernel put back.
- **`ngnet-h2-tokio` against `ngnet-qmux-h3-tokio`** — same crate family, same runtime, same
  I/O model, same socket, different protocol *and* the layering that comes with it. This is the
  cross-protocol question, and `ngnet-h2-tokio` — not the compio arm — is its counterpart.
- Every other pair varies two axes at once. `ngnet-h2-compio` against `hyper-tokio` remains the
  honest end-to-end "fastest configuration here against the reference implementation" number,
  attributable to neither axis alone; `ngnet-qmux-h3-tokio` against `hyper-tokio` or against
  the compio arm is attributable to nothing at all.

Historically **the empty-body case is a near-tie across the three HTTP/2 arms**, and that is
its most useful property: with almost no I/O to do, two stacks and two I/O models converge, as
they should. An empty-body result that is *not* a near-tie across those three is a signal that
something outside the protocol is being measured.

**The QMux arm is not expected to join that tie, and its not doing so is not a defect.** This
is the case with the least payload to amortise a per-exchange cost over, so it is where the
extra layer shows most; see [`serial-latency`](serial-latency.md) for the same argument without
a kernel, and [`../README.md`](../README.md) for what the resulting gap does and does not
license. What is worth watching is the *relationship* between the two families' ratios: a
socket ratio close to the duplex ratio says the cost is CPU in the protocol layers, and a
socket ratio noticeably larger says something about the syscall pattern differs too. That
reading used to name a specific suspect — the QMux join offered one `IoSlice` at a time to its
writer — and the suspect is gone: records now accumulate in a bounded buffer and a pass writes
once. [`../controls.md`](../controls.md) still gives the residual confound its direction, and
what is left of it here is small, because this case has no payload for a coalesced write to
carry: this arm moved +4.0% when coalescing landed
([`../findings/qmux-write-path.md`](../findings/qmux-write-path.md)), the wrong way, which is
what a bounded buffer's bookkeeping costs when there is nothing to amortise it over.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md). Three now contain a QMux
arm — [`04-qmux-drift-baseline`](../data/xeon-8370c-azure/04-qmux-drift-baseline.md),
[`05-qmux-delivery-aliasing`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md) and
[`06-qmux-write-path`](../data/xeon-8370c-azure/06-qmux-write-path.md) — and all three are
paired comparisons of one QMux build against another, not of the QMux arm against an HTTP/2 one.
**No recorded run computes a cross-protocol ratio for this group under drift controls**, so the
readings above that turn on a ratio are still unmeasured; the HTTP/2 arms appear in those
sessions only as unchanged controls.
