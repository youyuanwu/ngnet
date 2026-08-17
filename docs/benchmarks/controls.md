# Confounds and controls

## Confounds, and which way each pushes

Each is named with its direction, because a number without its bias is not evidence.

- **The write-path asymmetry — was the dominant effect, and is now largely removed.** It is
  kept here because it is the reason the arms ever diverged. The tokio transport now gathers
  (zero allocation, one `writev` per pass); the completion transport still cannot borrow or
  gather a *session block*, since the kernel must own the buffer for the duration, so a
  push-model exchange through it coalesces and pays a copy; hyper coalesces by buffering.
  Before gathering, this **favoured the coalescing arms wherever syscalls dominated** and
  accounted for the entire N=8/N=64 spread. With the tokio arm no longer writing per block,
  what remains of the confound is the **copy** the completion arm pays on pushed bodies and
  the other two do not, which biases against compio on large bodies. That copy is no longer
  structural: `NGHTTP2_DATA_FLAG_NO_COPY` is implemented, and a connection that hands its
  bodies over makes the payload the caller's own `Bytes`, which even the completion transport
  gathers as an owned region without copying. What that buys, measured, is in
  [`findings/handing-bodies-over.md`](findings/handing-bodies-over.md) — large on the
  readiness transport, and honestly below the drift bar on the completion transport, because
  the completion push path already coalesced a pass into one write, so there was never a
  syscall to save there, only the copy.
- **Loopback, not a network interface.** No real network latency, no device interrupts, no
  driver work — precisely the costs io_uring exists to amortise. This **biases against
  compio**; a real NIC would be expected to widen its lead rather than narrow it. Nothing
  here licenses a claim about what these transports do on a real network.
- **Scheduler non-separability.** compio is thread-per-core and `!Send`; tokio is
  work-stealing. All arms are held to one worker thread and one pinned core, which is as
  close as they can be brought, but the runtime and the I/O model are not separable in the
  compio arm — a residual scheduler difference is inseparably mixed into every compio number.
  The two tokio arms share a runtime type, so the `ngnet-h2-tokio`/`hyper-tokio` pair is free
  of this one, which is another reason that pair carries most of the weight.

Controlled rather than merely disclosed: `TCP_NODELAY` is set explicitly on both endpoints of
every socket arm — eight endpoints across the four, the three tokio-based ones through the same
socket-pair helper and the compio one through its completion-side twin — since Nagle meeting
delayed ACK would dominate a small-request benchmark and say nothing about either axis; each
runtime gets exactly one worker thread, and each arm gets its own runtime so no arm's idle
connection driver sits in another's scheduler; and pinning is left to external `taskset`
because compio can pin natively while tokio cannot, so pinning one side would manufacture the
asymmetry the control exists to remove.

## Confounds of the cross-protocol pair

These bear on `ngnet-h2` against `ngnet-qmux-h3` (and on `ngnet-h2-tokio` against
`ngnet-qmux-h3-tokio`) and on nothing else in the suite. The settings that *could* be brought
to the same value on both sides are on [`configuration.md`](configuration.md); what is here is
what remained after that was done.

- **The layering itself — the largest of these, and not a defect in the comparison.** The QMux
  arms carry a stream-multiplexing transport underneath their HTTP framing: records, frames,
  transport-level flow control, a pump between the two layers. The HTTP/2 arms carry framing
  over a byte stream and nothing else. This **biases against the QMux arms** wherever
  per-exchange overhead dominates, and it cannot be removed by configuration, because it is not
  a setting — it is what the comparison is *of*. A reader wanting to know what it costs to run
  HTTP/3 over a reliable byte stream is asking about exactly this term. The distinction worth
  holding is between the part of the gap that is the extra layer (structural, and the point)
  and the part that is this particular implementation of it (contingent, and the thing a later
  measurement could move) — nothing in this suite separates the two, and no arm here is
  positioned to.
- **The record size, reachable from neither stack.** dwnx caps a QMux record at 16382 bytes
  including its framing; libnghttp2 caps a DATA frame's payload at 16384 and neither value is
  on either crate's configuration surface. So the QMux arm puts strictly less payload on the
  wire per unit, and at 1 MiB needs one more unit than the 64 an HTTP/2 arm needs. This
  **biases against the QMux arms**, by a fraction of a percent of a body sweep's work — at or
  below this host's drift bar, and the wrong order of magnitude to explain any gap seen so far.
  It is here so that a later reader hunting the mechanism behind a 1–2% body-throughput
  difference finds it before inventing one. [`configuration.md`](configuration.md) has the
  arithmetic and why neither side can be moved.
- **QMux's unidirectional streams spend connection credit; HTTP/2's control frames do not.**
  Both arms are given 65535 bytes of connection-level credit, but HTTP/3's control and QPACK
  streams are ordinary QMux streams and consume from that allowance, where HTTP/2's `SETTINGS`,
  `PING` and `WINDOW_UPDATE` frames sit outside flow control entirely. So the two figures are
  equal in number and not quite in meaning, and the QMux arm has marginally less of its window
  available to bodies. This **biases against the QMux arms**, by a few hundred bytes over a
  connection's whole life against a window that is extended per consumed byte — which is to say
  by nothing measurable. It is disclosed rather than controlled because the alternative,
  granting the QMux arm a few hundred extra bytes to compensate, would replace an exactly
  stated asymmetry with an estimated one.
- **The warm-up asymmetry: the QMux fixtures complete an exchange during `establish` and the
  HTTP/2 fixtures do not.** It is worth being exact about why, because the obvious explanation
  is wrong. It is *not* that an HTTP/2 fixture handshakes during setup and a QMux one does not.
  `handshake_with` performs no I/O — it constructs a driver, and `ngnet-h2`'s own documentation
  says nothing moves until that driver is polled — and no fixture on either stack awaits
  anything after spawning its drivers. On a `current_thread` runtime, `block_on(establish())`
  therefore returns without either driver having run, and **both** stacks defer their handshake
  to the first execution of the timed closure.
  <br><br>
  What keeps that out of every reported number, on both stacks, is Criterion's warm-up phase:
  it runs the closure unmeasured for three seconds before sampling begins, which absorbs a
  first iteration that handshakes. The suite's standing claim in
  [`cases/README.md`](cases/README.md) — that handshake cost is in no number here — rests on
  that, and always has.
  <br><br>
  The QMux warm-up is defence in depth rather than the mechanism. What it defends against is
  the size of the deferred work on this stack: a transport-parameter exchange that only leaves
  on the first pump, and until the peer's parameters arrive every limit is zero and no stream
  can be opened; three unidirectional streams the HTTP/3 driver opens before anything else; and
  a SETTINGS exchange on top. Against a run given a short `--warm-up-time`, or a sample count
  low enough that one anomalous iteration tells, that is a much larger thing to leave to
  chance than HTTP/2's preface and SETTINGS. The residual, disclosed: a QMux arm's first timed
  iteration meets a connection that has already carried one exchange where an HTTP/2 arm's
  meets one that has carried none, which **biases towards the QMux arms** by an amount that is
  not measurable across thousands of iterations. Do not delete the warm-up as redundant — it is
  redundant only for as long as the warm-up phase is left at its default.
- **One implementation, never optimised.** `ngnet-qmux-h3` had no benchmark before this suite
  and no measurement has been acted on since; the join is known to write one `IoSlice` at a
  time and to copy inbound bodies twice below it, both recorded on
  [`../qmux-h3/pending-work.md`](../qmux-h3/pending-work.md) and neither addressed. This
  **biases against the QMux arms** and is contingent rather than structural, which is the whole
  reason a cross-protocol figure licenses a statement about these two stacks today and not
  about the two protocols.
- **A multi-slice offer is written one slice at a time.** Named separately from the point above
  because it is the same *kind* of effect as the write-path asymmetry at the top of this page,
  and that one turned out to account for an entire 2.3× spread. `ngnet-h2`'s tokio transport
  emits one `writev` per driver pass; the QMux join issues one write per `IoSlice` and stops at
  the first not fully accepted, and the layer below refuses a second production while a record
  is outstanding — so a fragmented offer costs one record and one pump pass per slice. This
  **biases against the QMux arms, and biases them more on the socket family than on the duplex
  family**, because that is where a write is a syscall. It is the first thing to test against
  if a socket-family gap is ever found to exceed its duplex counterpart.
- **Buffering: one record outstanding, against a 1 MiB pipe.** A QMux connection holds two
  16382-byte buffers and permits at most one record outstanding at a time, where the duplex
  family's in-memory pipe is 1 MiB deep and the HTTP/2 arm may fill it. The pipe is sized so
  that it is not the bottleneck for the HTTP/2 arms; for the QMux arms the record discipline is
  the bottleneck long before the pipe is. This **biases against the QMux arms on the duplex
  family** and is a harness-visible consequence of the protocol's own design rather than a
  choice the harness made — the pipe's capacity is equal for both arms, and equalising the
  *effect* would mean shrinking the pipe until it constrained the HTTP/2 arm too, which would
  change arms whose measurements are already recorded.

Controlled rather than merely disclosed, on the cross-protocol pair specifically: both arms run
on a single-worker runtime with drivers on plain `tokio::spawn` — the QMux join imposes no
`Send` bound and needs no `LocalSet`, so the runtime arrangement is identical rather than
merely similar, and the two arms of a cross-protocol pair always share whichever arrangement
their family uses (see the runtime row in
[`configuration.md`](configuration.md), which differs by family); `TCP_NODELAY` on the socket family is set by the same helper for both; and the
request, body, echo handler and drain are one shared definition that both fixtures call, so
"the two stacks ran the same workload" is a property of there being one definition rather than
an assertion about two.

**The drain is part of that, and is the one shared helper worth naming.** The two stacks defer
different amounts of work until a response body is actually read, so an arm that took the
response head and dropped the body would be measuring almost nothing on one stack and rather
more on the other — and the gap would look like a protocol difference. Every arm, on both
stacks, reads every response to its end through the same function, and the server side collects
every request body through the same one. Neither is a property either stack can opt out of by
being lazier than the other.

## Drift, and the design that survives it

**Grouped A/B designs are not trustworthy.** Serial latency once showed +6.8% and empty-body
+5.1% under a design that ran both baseline repetitions and then both branch repetitions. That
design cannot separate a real effect from machine drift, and the machine drifted: across one
such session `hyper-tokio` moved 5.1% on serial latency and 9.9% at 1 MiB *without its code
changing*. Re-measured with the branches interleaved (baseline, branch, baseline, branch) and
the unchanged arms used as controls, serial latency moved +1.3% on the changed arm against
+4.5% and +1.4% on the two controls — the changed arm moved *less* than either unchanged one —
and the empty-body sign inverted to −4.7% against −0.9% and −0.6%. Neither regression survived.
The lesson is recorded rather than the first numbers: **interleave, and unchanged arms are the
cheapest available control.**

Four devices follow from that, and every recorded run uses them:

1. **Unchanged arms are carried as drift controls.** `hyper-tokio` is touched by none of this
   work, so whatever it does between runs is the session's noise floor. In the shared-body
   families the untouched `*-push` twins serve the same purpose for their own transport.
   A claimed gain smaller than the controls' own movement is not a result. The HTTP/2 arms play
   this role for the cross-protocol comparison too: nothing about them changed when the QMux
   arms were added — same group names, same benchmark ids, same sweeps, same registration order
   relative to one another — so a run that moves an HTTP/2 arm has moved for a reason outside
   this suite, and a QMux figure is read against an HTTP/2 figure taken minutes earlier on the
   same machine rather than against anything absolute.
2. **A mechanistic control is preferred to a statistical one where the mechanism allows.**
   The 0-byte point in the body sweeps is one: with no body there is nothing to copy and
   nothing to gather, so two arms that differ only in body strategy *cannot* legitimately
   differ there. If 0 B moves, the harness is measuring something other than what it claims.
3. **Pairs run adjacent, sizes are the outer loop.** Within each size, an arm and its twin run
   back to back, so the two halves of a comparison sit as close together in time as Criterion
   allows. This is adjacency, not sample-level interleaving — Criterion samples one benchmark
   to completion before starting the next, and no arrangement of `bench_with_input` calls
   changes that. Replication covers the remainder. The cross-protocol arms obey this by
   registration order: each QMux arm is registered **immediately after its HTTP/2
   counterpart** — `ngnet-h2` then `ngnet-qmux-h3`, with `hyper` after both, and on the socket
   family after `ngnet-h2-tokio` specifically, the only arm differing from it in protocol
   alone. Appending them at the end of each loop body would have put an unrelated arm between
   the two halves of the comparison this work exists to make. No pre-existing registration
   moved relative to another, because emission order is a control here and the runs already
   filed under [`data/`](data/) were taken in it.
4. **Replication, with the exclusion rule fixed in advance.** The shared-body verdict
   aggregates paired deltas over ten independent runs, so a slow drift has to bias every one
   of them the same way to survive. Its exclusion rule — discard any replicate whose 0-byte
   paired delta exceeds ±5% — was fixed before the results were seen, and the result is
   reported both with and without it.

## Judging against controls: whose controls?

When a session's controls disagree with each other, the choice of which to judge against has
to be made on evidence and recorded, not made silently. It came up once, in the shared-body
run: `compio-push` wandered 24–42% in three replicates where `tokio`'s own 0-byte control moved
at most 4.6%. The disturbance was a property of the compio arms, not a session-wide noise
floor, so each transport was judged against the controls on its own transport — which is what
made the tokio verdict MET and the compio verdict NOT MET in the same run. See
[`findings/handing-bodies-over.md`](findings/handing-bodies-over.md).

**The sequel is the more useful half of the lesson.** Re-measured on a host that drifts about
1%, where `compio-push` is as steady as every other arm, the compio delta was essentially
unchanged (−4.55% against −4.07%) and the verdict inverted to MET. The judgement call above
did not change what was measured; it only ever decided what could be *claimed* from it. When
controls disagree, the honest move is to report the delta, name the control that blocks the
claim, and go and find a quieter machine — not to pick the control that gives the answer one
wants.
