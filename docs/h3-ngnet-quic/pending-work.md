# Hyperium H3 over ngtcp2: pending work

## Resolved: the intermittent stall under repeated exchanges

The adapter used to stall intermittently. A peer parked waiting for something that never
arrived, and the connection sat until its 30-second idle timeout ended it, surfacing as
`ConnectionErrorIncoming::Timeout`. Roughly two runs in five of 200 x 1 KiB exchanges failed
that way, at a random exchange index.

**It was a lost FIN, and the conflation that lost it was in `ngnet-quic`, not in this crate's
wake plumbing.**

### The mechanism

`ngtcp2_conn_writev_stream` reports what it serialised through `*pdatalen`
(`crates/ngnet-quic-sys/vendor/ngtcp2/lib/includes/ngtcp2/ngtcp2.h:5233-5243`):

- "The packet might not contain STREAM frame if other frames occupy the packet. In that case,
  `*pdatalen` would be -1."
- "Empty data is treated specially ... If 0 length STREAM frame is successfully serialized,
  `*pdatalen` would be 0."

On a `fin`-only write those two are opposites: `0` means the FIN is on the wire, `-1` means the
stream was not touched. `submit_one_vec` clamped the sign with `accepted.max(0)`, so both
arrived as `StreamWrite::Datagram { accepted: 0 }`. `SendStream::poll_finish` read that as
proof and set `send_finished`. The FIN was never serialised, so nothing was in flight, so loss
recovery had nothing to retransmit — and the peer waited for an end that would never come.

ngtcp2 skips the caller's stream whenever another stream is already queued for transmission or
a queued frame did not fit: `*pfrc == NULL && send_stream && ngtcp2_pq_empty(&conn->tx.strmq)`
guards the entire stream-writing block (`ngtcp2_conn.c:4251-4253`). An HTTP/3 connection is
full of occasions for that — control and QPACK streams, retransmissions, MAX_DATA and
MAX_STREAMS raised on the read path — which is why the failing exchange index was random.

### Why it hit this adapter and not the native one

Hyperium's `finish()` is a separate, pure-FIN write on every exchange, so this adapter offered
the ambiguous shape once per request. The native stack reached the same conflation less often,
and `ngnet-h3`'s driver already had the correct handling for a declined FIN — the arm at
`crates/ngnet-h3/src/http/driver.rs:293-298`, whose comment reads "a transport that declined it
leaves the peer waiting for an end that never comes" — but could never reach it, because the
transport reported `Accepted(0)` rather than a block.

### The fix

- `ngnet-quic` keeps the two apart with `StreamWrite::DatagramWithoutStream { len }`: a
  produced datagram that carried nothing of the offered stream. It must still be sent; the
  offer, `fin` included, must be made again.
- `ngnet-quic-h3` maps it to `WriteOutcome::Blocked`, which is what routes into the correct
  `ngnet-h3` arm above.
- `h3-ngnet-quic` maps it to `Offered::Displaced` and re-offers within its bounded loop instead
  of recording the stream as finished.
- The endpoint's own attached-connection write path requeues the whole offer for it.

### The regression tests, and neither is `#[ignore]`d

`crates/ngnet-quic/tests/fin_delivery.rs` reproduces ngtcp2's decision **deterministically** —
an in-memory pair, a hand-driven clock, and packets dropped by choice rather than by chance, so
that several packets' worth of rescheduled data occupies the send queue at the moment the FIN
is offered. It asserts the packet carried no FIN, that the outcome does not claim otherwise,
and that the FIN is still deliverable afterwards; it also asserts non-vacuity, so a change that
stopped triggering the case fails rather than passing for the wrong reason. Reverting the fix
to its previous form makes it fail with:

    the fin was not serialised, yet the write was reported as
    Datagram { len: 1444, accepted: 0 }, which tells a caller the stream ended

`crates/h3-ngnet-quic/tests/repeated.rs` is the end-to-end gate: the 200-exchange workload that
used to fail. **All of this crate's live-loopback suites are now enabled**, and CI runs them
through `cargo test -p h3-ngnet-quic --release`.

### Measured after the fix

On `epyc-7763-azure`, release build, pinned to one core with `taskset -c 0`. Taken in two
rounds, because a review of the first changed the native arm; the full tables, and what each
round's code was, are in
[`../benchmarks/data/epyc-7763-azure/02-h3-ngnet-quic-fin-fix.md`](../benchmarks/data/epyc-7763-azure/02-h3-ngnet-quic-fin-fix.md).

| Workload | Arm | Runs | Failures |
| --- | --- | ---: | ---: |
| 200 x 1 KiB exchanges | `h3-ngnet-quic` | 45 | 0 |
| 200 x 1 KiB exchanges | `ngnet-quic-h3` (native, matched) | 45 | 0 |
| 200 x 16 KiB exchanges | `h3-ngnet-quic` | 40 | 0 |
| 200 x 16 KiB exchanges | `ngnet-quic-h3` (native, matched) | 70 | 2 (both in round 1) |
| 30 x 1 MiB exchanges | `h3-ngnet-quic` | 20 | 0 |
| 30 x 1 MiB exchanges | `ngnet-quic-h3` (native, matched) | 20 | 0 |
| `tests/repeated.rs`, release, pinned | `h3-ngnet-quic` | 30 | 0 |
| Full crate suite, all tests enabled | `h3-ngnet-quic` | 18 | 0 |

Before the fix the same 1 KiB workload failed 6 runs in 10 on this adapter as first measured,
and 11 in 25 after the two waker and timer fixes below — that second figure is the code this
change replaced. The native arm failed 0 in 10.

The two 16 KiB failures are the native arm's and are the inherited defect below; both fell in
the first round, and the second round saw none in fifty. That is not a claim the inherited
defect is gone — see the run record for why fifty clean runs bound its rate loosely rather than
showing it is zero. The 1 MiB rows establish that those probes now run, not that 1 MiB is safe.

### Two earlier fixes, which were real but were not this

Found while chasing it, both genuine, both reduced the failure rate without removing it:

- **The waker registries were single-slot** where two tasks legitimately wait: the HTTP/3
  driver parks in `poll_accept_*` while a request task parks in `poll_open_*`, and split stream
  halves park on one stream id from different tasks. Now both registries are lists (`core.rs`).
- **The expiry timer was armed before the caller's write rather than after it.** A write that
  returns `Blocked` is exactly what creates the pacing deadline that will unblock it, so arming
  beforehand left that deadline unwaited (`pump::rearm`).

## Inherited history: the native stack's large-body stall

`ngnet-quic-h3` had an intermittent connection-ending failure under repeated 16 KiB and 1 MiB
exchanges — review finding S9. The historical evidence is in
[`../quic-h3/invariants.md`](../quic-h3/invariants.md) and
[`../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md`](../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md),
and the root-cause area recorded there is the outer HTTP/3 sendability-generation scheduling
interacting with `ngnet-quic`'s packet-bounded staging and its zero-acceptance re-offers.

**The FIN change did not resolve it, and that evidence was mixed rather than tidy.** The FIN change
touched this stack twice: `transmit::drain` reports a stream-less packet as `Blocked`, which
reaches the arm in `ngnet-h3`'s driver written for exactly that case, and — after review — it
no longer ends the drain pass on one, so the displaced stream is re-offered within the pass
instead of waiting for an event that a bare acknowledgement will not produce.

Measured on this host, release, pinned to one core, both arms over the same transport with the
same workload (200 x 16 KiB exchanges), in the two rounds the run record describes:

| Round | Arm | Runs | Failures | Failure mode |
| --- | --- | ---: | ---: | --- |
| 1 | `h3-ngnet-quic` | 20 | 0 | — |
| 1 | `ngnet-quic-h3` (native, matched) | 20 | 2 | `ErrorKind::Closed`, "the connection has ended" |
| 2 | `h3-ngnet-quic` | 20 | 0 | — |
| 2 | `ngnet-quic-h3` (native, matched) | 50 | 0 | — |

Round 1 attributes squarely: transport held fixed, HTTP/3 layer varied, failure on one arm and
not the other, so it belongs to the native stack. That is the same rule used the other way round
for the defect above, when this adapter failed 6 in 10 and the native arm 0 in 10 at 1 KiB.

Round 2 saw none. Fifty clean runs bounded the fault rather than disproving it. Later native
work first localized failures around an armed imminent timer, then captured the missing fact:
the timer was handled and replaced by the idle deadline without the blocked stream receiving
another transmit pass. The native adapter now preserves that one ready edge. A separate
pre-readiness 1 MiB failure blocks a full resolution claim; see
[`../benchmarks/data/epyc-7763-azure/04-native-h3-s9-root-cause.md`](../benchmarks/data/epyc-7763-azure/04-native-h3-s9-root-cause.md).

Consequences for this crate:

- No test here is `#[ignore]`d, and none of this crate's tests provoked the failure.
- The committed body-throughput bench stays at 1 KiB, matching the restriction the existing
  `quic_stack_body_throughput` target already applies to this transport.
- Both arms stay wired into `ngnet-bench`'s `probe` example so 16 KiB and 1 MiB can be run as
  supervised, reportable probes on each stack.
- `tests/ngnet-bench/tests/ngtcp2_fixture.rs` drives 16 KiB bodies through the native stack and
  can fail the same way. It was seen to once, under a contended full-workspace run; 64 further
  runs across this branch and unmodified `main`, isolated and under deliberate CPU contention,
  did not reproduce it on either. See
  [`../quic-h3/pending-work.md`](../quic-h3/pending-work.md).

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

`poll_retained` re-offers the remainder immediately after a partial acceptance, re-offers again
when a packet carried nothing of the stream at all, and parks only on a genuine block, which
matches `ngnet-quic-h3`'s `transmit::drain`. `h3-ngnet-qmux` parks on every partial acceptance
instead. Neither is a hang — the pacing and acknowledgement timers
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
