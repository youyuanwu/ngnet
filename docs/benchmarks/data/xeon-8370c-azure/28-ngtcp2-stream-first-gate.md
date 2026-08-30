# 28 — Is stream-first ngtcp2 packet production evidenced?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; no new timing run
**Date:** 2026-08-30
**Source:** `383082f` (accepted Phase 2 origin)
**Purpose:** Phase 3 packet-order entry-gate decision
**Disposition:** not evidenced; deferred without a source behavior change

## Gate

The reviewed Phase 3 plan permits a stream-first packet-order experiment only when all of
these conditions already hold:

1. the Phase 2 origin is stable;
2. recurring ACK/control debt exists alongside stream data;
3. standalone transport packets are attributable specifically to transport-first ordering;
4. a target gap larger than matched drift is declared before measuring the candidate.

Phase 2 established the stable origin and observed recurring transport-only packets. It did
not establish conditions 2–4. In particular, the origin timing spans were 11.79% target and
14.30% control at 16 KiB, and 38.29% target and 21.13% control at 1 MiB. No packet-count or
latency target larger than those spans was declared before Phase 3.

## What the current diagnostics prove

The adapter currently produces transport work before asking the HTTP/3 source for stream
data. `pump` drains inbound datagrams, handles an expired timer, and calls `produce`;
`produce` repeatedly calls `write_pkt` and records each resulting datagram as not
stream-carrying
([`crates/ngnet-quic-h3/src/pump.rs:29-94`](../../../../crates/ngnet-quic-h3/src/pump.rs#L29-L94),
[`crates/ngnet-quic-h3/src/pump.rs:97-158`](../../../../crates/ngnet-quic-h3/src/pump.rs#L97-L158)).
Only after that pass does `poll_transmit` call `transmit::drain`
([`crates/ngnet-quic-h3/src/connection.rs:388-411`](../../../../crates/ngnet-quic-h3/src/connection.rs#L388-L411)).

`transmit::drain` calls `write_stream_vectored` and classifies the resulting packet from
whether it accepted stream bytes
([`crates/ngnet-quic-h3/src/transmit.rs:61-89`](../../../../crates/ngnet-quic-h3/src/transmit.rs#L61-L89),
[`crates/ngnet-quic-h3/src/transmit.rs:101-106`](../../../../crates/ngnet-quic-h3/src/transmit.rs#L101-L106)).
The aggregate packet counters therefore distinguish standalone production from a stream
write that accepted new bytes. Each transport-only packet also creates a sequenced liveness
event, and stream-write attempts use the same process-wide sequence, so the trace can place
those observations in temporal order
([`crates/ngnet-quic/src/diagnostics.rs:360-423`](../../../../crates/ngnet-quic/src/diagnostics.rs#L360-L423),
[`crates/ngnet-quic/src/diagnostics.rs:540-542`](../../../../crates/ngnet-quic/src/diagnostics.rs#L540-L542)).
It does not identify the transport frames in a packet or why ngtcp2 emitted it.

## Attribution limitation

The current hooks cannot establish or refute transport-first attribution:

- `record_packet` increments role totals and records transport-only packets as sequenced
  enabling events, but records no frame inventory, simultaneous pending-stream state, or
  coalescing eligibility
  ([`crates/ngnet-quic/src/diagnostics.rs:680-694`](../../../../crates/ngnet-quic/src/diagnostics.rs#L680-L694)).
- The public snapshot exposes only aggregate transport-only, stream-carrying, and produced
  packet counts; retransmission attribution is explicitly unavailable
  ([`crates/ngnet-quic/src/diagnostics.rs:89-161`](../../../../crates/ngnet-quic/src/diagnostics.rs#L89-L161),
  [`crates/ngnet-quic/src/diagnostics.rs:508-519`](../../../../crates/ngnet-quic/src/diagnostics.rs#L508-L519)).
- `StreamSource` has only a consuming `write_next` operation and no non-consuming pending-data
  query
  ([`crates/ngnet-h3/src/http/quic.rs:213-240`](../../../../crates/ngnet-h3/src/http/quic.rs#L213-L240)).
  Asking it for an offer before `write_pkt` would itself perform the ordering change under
  evaluation.

Consequently, another run of the existing probe could show recurring temporal adjacency
between transport-only events and later stream attempts, but it could not tell whether those
packets could have carried the pending stream data, were mandatory before stream production,
or would disappear under stream-first order. Temporal or same-poll correlation alone would
still not expose frame eligibility or causality. No low-perturbation use of the existing
hooks can establish or refute the required ordering attribution, while a controlled ordering
arm would cross the unsatisfied entry gate. No new runtime measurement was therefore
collected and no target was retrofitted after observing Phase 2 results.

## Decision

Phase 3 is **not evidenced and deferred**. Packet ordering remains unchanged, no candidate
measurement is claimed, and the accepted Phase 2 origin at `383082f` remains the baseline.
This is a terminal ordering disposition for entry into Phase 4's residual-attribution work;
it is not evidence that stream-first ordering is ineffective.
