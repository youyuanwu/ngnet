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

Controlled rather than merely disclosed: `TCP_NODELAY` is set explicitly on all six endpoints
(both sides of all three arms), since Nagle meeting delayed ACK would dominate a small-request
benchmark and say nothing about either axis; each runtime gets exactly one worker thread, and
each arm gets its own runtime so no arm's idle connection driver sits in another's scheduler;
and pinning is left to external `taskset` because compio can pin natively while tokio cannot,
so pinning one side would manufacture the asymmetry the control exists to remove.

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
   A claimed gain smaller than the controls' own movement is not a result.
2. **A mechanistic control is preferred to a statistical one where the mechanism allows.**
   The 0-byte point in the body sweeps is one: with no body there is nothing to copy and
   nothing to gather, so two arms that differ only in body strategy *cannot* legitimately
   differ there. If 0 B moves, the harness is measuring something other than what it claims.
3. **Pairs run adjacent, sizes are the outer loop.** Within each size, an arm and its twin run
   back to back, so the two halves of a comparison sit as close together in time as Criterion
   allows. This is adjacency, not sample-level interleaving — Criterion samples one benchmark
   to completion before starting the next, and no arrangement of `bench_with_input` calls
   changes that. Replication covers the remainder.
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
