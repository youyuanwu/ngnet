# Hyperium H3 over ngtcp2: pending work

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
