# The ngtcp2 HTTP/3 comparison

Two HTTP/3 implementations over one QUIC transport:

| Arm | HTTP/3 | Adapter | Transport |
| --- | --- | --- | --- |
| `ngnet-quic-h3` | `ngnet-h3` | `ngnet-quic-h3` | `ngnet-quic` |
| `h3-ngnet-quic` | hyperium `h3` 0.0.8 | `h3-ngnet-quic` | `ngnet-quic` |

Targets: `quic_stack_h3_serial_latency` (empty body) and `quic_stack_h3_body_throughput`
(1 KiB). The counterpart of [`qmux-h3-comparison.md`](qmux-h3-comparison.md), asking the same
question about the other transport family.

## What the pair changes, and what it holds

It changes the HTTP/3 implementation and the adapter that joins it to the transport. It changes
nothing else that could be held equal.

Each arm gets its own current-thread runtime, a persistent connection, one spawned endpoint
driver plus one spawned HTTP/3 driver per endpoint, and one explicit empty warm-up inside
`establish()` and therefore outside the measured closure. Both arms take their credentials,
endpoints, ALPN, server name and transport configuration from the same
`ngnet-quic-h3-tests` helpers, so the transport is identical by construction rather than by
inspection.

The native arm is a **purpose-built matched fixture**, `NgnetNgtcpH3Matched`, not the
pre-existing `NgnetNgtcpH3`. `ngnet-h3` defaults to a 4 KiB QPACK dynamic table and hyperium
0.0.8 has none at all, so the default pair would differ in header state and the difference would
be read as "the HTTP/3 implementation". The matched fixture zeroes the capacity and writes the
field-section bound explicitly, on client and server, on both arms. The QMux comparison had to
do the same thing for the same reason. `NgnetNgtcpH3` and the existing `quic_stack_*` targets are
left untouched so run 25's record stays reproducible.

Also held equal: a byte-identical request head, the same response status and headers, one copy
of the body into a contiguous buffer on each server, a full drain of every response byte inside
the measured region, identical pinning, identical timed-region boundaries, and unmodified
Criterion sampling on both sides.

## The asymmetries that could not be removed

Named here because a comparison that hides these is not one.

1. **Where HTTP/3 driving sits relative to the timed region.** `ngnet-h3` advances its state
   machine in its spawned driver task; hyperium advances a request stream from whichever task
   polls it, which here is the task inside the measured closure. UDP I/O is symmetric — both
   arms hand it to the shared endpoint driver — but this is not. It is inherent to comparing
   these two drivers.
2. **Two independently written QUIC pumps**, one per adapter. Differences between them are part
   of "the adapter", but they are not differences in the HTTP/3 state machine.
3. **Hyperium clones its request handle per exchange**, because `SendRequest::send_request`
   takes `&mut self`. Already disclosed for the QMux pair.
4. **More await points inside hyperium's timed region**: `send_request`, `send_data`, `finish`,
   `recv_response` and the `recv_data` loop, against the native arm's single `send_request` plus
   drain.
5. **Body chunking granularity** may differ for an identical payload; neither layer exposes a
   control that would equalise it.

One deliberate non-match: the native config sets `max_concurrent_streams` and hyperium 0.0.8 has
no equivalent. It does not reach the wire as a difference — concurrent streams are bounded by
the transport's `MAX_STREAMS`, identical on both arms — and the workload is serial.

## Why the body sweep stops at 1 KiB

Because this transport has an unresolved intermittent connection-ending stall under repeated
16 KiB and 1 MiB workloads (review finding S9), and the existing `quic_stack_body_throughput`
already restricts itself for that reason. A committed sweep that intermittently kills its own
connection produces numbers nobody should trust. Larger payloads are run as supervised probes
through `examples/probe.rs`, on both arms, rather than being inferred from one stack's history.

## What a result from this pair could support

A whole-stack statement — "this HTTP/3 implementation plus this adapter costs more or less than
that one, over this transport, at this payload" — and no per-layer attribution. The adapter and
the state machine move together and cannot be separated by these arms.

## Status

**No result yet.** The first run,
[`../data/epyc-7763-azure/01-h3-ngnet-quic-comparison.md`](../data/epyc-7763-azure/01-h3-ngnet-quic-comparison.md),
is inconclusive: an unchanged control arm drifted 4.2x within the session, roughly thirty times
the candidate effect, on a host that could not be quiesced. That run also found a liveness defect
in `h3-ngnet-quic` itself under repeated exchanges, which must be fixed before the comparison is
worth running again.
