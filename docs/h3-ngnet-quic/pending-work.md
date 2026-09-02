# Hyperium H3 over ngtcp2: pending work

## Open defect: intermittent stall under repeated exchanges

**This is the most important thing on this page, and it is this crate's own fault.**

The adapter intermittently stalls. A peer parks waiting for something that never arrives, and
the connection sits until its 30-second idle timeout ends it, surfacing as
`ConnectionErrorIncoming::Timeout`.

It was found with a repeated small-body workload, which provokes it most reliably, but it is
**not confined to repetition**: single-exchange tests in `tests/lifecycle.rs` have also been
observed to stall under CPU contention. Repetition multiplies the timing windows rather than
creating them. Because of that, **all of this crate's live-loopback test suites are
`#[ignore]`d** in ordinary runs — the same treatment `docs/quic-h3/invariants.md` gives the
transport's own unresolved liveness failure. Run them with
`cargo test -p h3-ngnet-quic -- --ignored` on an idle machine.

### Evidence

Measured on `epyc-7763-azure`, release build, pinned to one core with `taskset -c 3`, via
`tests/ngnet-bench/examples/probe.rs` and a purpose-built harness:

| Workload | Arm | Result |
| --- | --- | --- |
| 200 x 1 KiB exchanges, 10 runs | `h3-ngnet-quic` | 6 failures |
| 200 x 1 KiB exchanges, 10 runs | `ngnet-quic-h3` (native) | 0 failures |
| 200 x 1 KiB exchanges, 25 runs (after two fixes) | `h3-ngnet-quic` | 11 failures |

Every failure took 30.06–30.11 s wall clock — the idle timeout exactly. Successful runs took
109–215 ms. The failing exchange index is random (3, 8, 11, 13, 99, 113, 142, 186), so this is
a race, not state that accumulates.

### What is established about the failure state

From instrumented runs, captured at the moment of the stall:

- The request was fully delivered **and acknowledged**: the client's transport reported zero
  retained bytes.
- No inbound datagrams were dropped on either side.
- The server observed the stream open — `Opened` counts track the exchange count — and had
  returned to accepting, with an empty accept queue.
- The client was parked in `poll_data` on that stream.
- Both sides had their expiry timer armed.

So the request arrives, the server sees the stream, and the response never reaches the client.
The fault is somewhere between the server accepting the stream and the client observing a
response; it is not yet located more precisely than that.

### Not the known S9 stall

`ngnet-quic-h3` has its own unresolved large-body stall (below). This is a different defect,
and the distinction was decided by a rule fixed *before* measuring: a failure may be attributed
to S9 only if reproduced on both stacks. It was not — the native arm passed 10 out of 10 on the
identical workload — and S9 lives in `ngnet-h3`'s driver, which this crate does not use.

### Fixed along the way, without resolving it

Two genuine defects were found while chasing this. Both are real, both reduced the failure
rate, neither removed it:

- **The waker registries were single-slot** where two tasks legitimately wait: the HTTP/3
  driver parks in `poll_accept_*` while a request task parks in `poll_open_*`, and split
  stream halves park on one stream id from different tasks. A displaced task was then
  reachable only through `ngnet-quic`'s inbound waker list, which does not carry the expiry
  timer. Now both registries are lists (`core.rs`).
- **The expiry timer was armed before the caller's write rather than after it.** A write that
  returns `Blocked` is exactly what creates the pacing deadline that will unblock it, so
  arming beforehand left that deadline unwaited (`pump::rearm`).

### Reproducing it

`cargo test -p h3-ngnet-quic --release -- --ignored` on an idle machine;
`crates/h3-ngnet-quic/tests/repeated.rs` is the most reliable reproducer. Note that adding `eprintln!` tracing to the pump hides the failure — it is timing
sensitive — so in-memory counters dumped at the stall were what produced the evidence above.

### Consequence

The crate should not be used until this is fixed, and the benchmark comparison it exists to
enable cannot be run meaningfully against it. The comparison run record reports the numbers it
obtained and claims nothing from them.

CI therefore runs only this crate's deterministic tests — the trait assertions and the error
unit tests. Gating the shared workflow on a suite with a known timing-sensitive failure would
redden CI for unrelated changes. Re-enable the live suites with the fix; they are the regression
suite for it.

## Inherited: the large-body stall

`ngnet-quic-h3` has an unresolved intermittent connection-ending stall under repeated 16 KiB and
1 MiB exchanges — review finding S9. The evidence is in
[`../quic-h3/invariants.md`](../quic-h3/invariants.md) and
[`../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md`](../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md),
and the root-cause area recorded there is the outer HTTP/3 sendability-generation scheduling
interacting with `ngnet-quic`'s packet-bounded staging and its zero-acceptance re-offers.

That defect is in `ngnet-h3`'s driver, which this crate does not use. Whether the same workload
provokes something equivalent through hyperium's driver is a separate question, and it is not
answered by assuming either way. What can be said:

- No test in this crate provoked a stall. None is `#[ignore]`d for it.
- The committed body-throughput bench stays at 1 KiB, matching the restriction the existing
  `quic_stack_body_throughput` target already applies to this transport.
- Both arms are wired into `ngnet-bench`'s `probe` example so 16 KiB and 1 MiB can be run as
  supervised, reportable probes on each stack, rather than the effect on payload coverage being
  inferred from one stack's history.

What was actually observed on the current machine is in the run record under
[`../benchmarks/data/`](../benchmarks/data/). If a stall is ever seen only on this adapter and
not on the native one, it is this crate's defect and must be treated as one — not attributed to
S9.

## Zero-copy bodies

The adapter uses `write_stream_vectored`, which stages an internal copy. `ngnet-quic` also
offers `write_stream_owned`, which takes an `OwnedBytes` and hands back the unaccepted suffix as
a zero-copy handle into the same allocation, with release reported on acknowledgement rather
than immediately.

Routing bodies through it would remove a copy per packet. It is deliberately not done here, for
the same reason it is deferred in `ngnet-quic-h3`
([`../quic-h3/pending-work.md`](../quic-h3/pending-work.md)) and for one more: the two stacks are
benchmarked against each other, and taking a faster path on one side only would make the
comparison measure the path rather than the HTTP/3 implementation. If it is ever taken, it should
be taken on both.

## Send progress within a pass

`poll_retained` re-offers the remainder immediately after a partial acceptance and parks only on
zero acceptance, which matches `ngnet-quic-h3`'s `transmit::drain`. `h3-ngnet-qmux` parks on
every partial acceptance instead. Neither is a hang — the pacing and acknowledgement timers
re-drive both — but the two adapters differ here, and if the QMux comparison is ever revisited
it is worth knowing that this is a difference between the adapters and not between the
transports.

## Not implemented, and not planned

QUIC DATAGRAM frames, stream prioritisation, 0-RTT, connection migration and key update. The
first two because `ngnet-quic` exposes neither and hyperium H3 0.0.8 requires neither; the rest
because the transport does not implement them (`crates/ngnet-quic/src/lib.rs`).

## Publishing

`publish = false`. The crate is new, its transport is at 0.0.x, and hyperium `h3` is itself
pre-1.0 with a semver-hazard feature gate on the traits a third-party backend implements.
