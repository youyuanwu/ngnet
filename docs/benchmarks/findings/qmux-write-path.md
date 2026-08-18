# The QMux write path, and what a count does not tell you

**Measurements:** [`04-qmux-drift-baseline`](../data/xeon-8370c-azure/04-qmux-drift-baseline.md),
[`05-qmux-delivery-aliasing`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md),
[`06-qmux-write-path`](../data/xeon-8370c-azure/06-qmux-write-path.md) — all on
`xeon-8370c-azure`.

The QMux stack was built to be correct and never made fast. It alternated strictly between
producing one protocol record and writing that one record, so a driver turn moving a megabyte made
sixty-five trips through the byte stream and one carrying sixty-four small streams made seventy,
totalling 1,922 bytes — about twenty-seven bytes a write. Sixty-four of those seventy are the
exchanges themselves and carry 1,342 bytes between them, twenty-one bytes each; the remaining six
are the connection preamble and setup, which are larger. The two averages are quoted here with
their denominators because they are easy to confuse and only the second is the quantity a
per-call cost is paid on. This is what changing that was worth, what it cost, and the one change
that was made, measured and thrown away.

## What was expected, and why it was half wrong

The argument for the work was an analogy: the HTTP/2 stack measured
[a large gain](write-path-and-gathering.md) from writing once per pass rather than once per
protocol unit, and QMux was in the shape HTTP/2 had been in. The analogy held, but it was attached
to the wrong workload. That finding records the megabyte body point as *neutral within noise* and
predicts a write-side change "moves 1 KiB most, 64 KiB less, and 1 MiB indistinguishably from
zero"; the case where it paid was a *multiplexed* pass, 513 writes collapsing to one.

So before any code was written, the write count was measured directly against the number of
streams in flight. It grew one for one — 7, 14 and 70 writes at concurrency 1, 8 and 64 with an
empty body — which located the gain on the concurrency axis, and a check of the benchmark arms
located it further: of sixteen QMux identifiers, eight run over an in-memory duplex where there
are no system calls to save at all. Two identifiers were named in advance as the only place a
write-count reduction could appear as a syscall saving. Both moved, and nothing else in their
family did.

## What it was worth

Paired against the same commit, interleaved, with 46 unchanged identifiers as controls moving
1.06% on average:

| Workload | duplex | real socket |
| --- | --- | --- |
| 1 MiB body | **−30.3%** | **−30.4%** |
| 64 KiB body | **−17.5%** | **−25.9%** |
| 1 KiB body | −4.2% | **−11.8%** |
| concurrency 64 | +1.8% | **−8.5%** |
| concurrency 8 | +2.1% | **−7.1%** |
| empty body | +3.9% | +1.7% |
| serial latency | +1.2% | +4.0% |

## The sign flip is the evidence

The most useful row is concurrency, because the two families disagree about its sign. Over a real
socket, concurrency 64 is 8.5% faster; over a duplex, the same parameter is 1.8% slower. That is
what a syscall reduction looks like when you take the syscalls away: the duplex arm pays the
bookkeeping and gets nothing back, because it never made the calls being saved.

Had both families moved the same way, the gain would have been consistent with almost anything.
Moving in opposite directions, on the same parameter, in the same session, is hard to explain with
anything other than the mechanism claimed.

## Where the gain came from

[`07-qmux-per-commit-attribution`](../data/xeon-8370c-azure/07-qmux-per-commit-attribution.md)
measures each commit against its predecessor, on two duplex targets, with the ten unchanged
identifiers in the same session as controls. Its answer is mostly *this cannot say*, and that is
worth stating plainly:

- **Write coalescing is the change that produced the body gains**, and it is the only step
  resolved on those identifiers: −21.7% at a megabyte and −8.6% at 64 KiB, against a control worst
  of 4.47% in its own step.
- **Direct serialisation, scanning in place and credit batching each move every identifier by less
  than their step's own control movement.** They are not shown to be worth nothing; they are
  smaller than this instrument can see. Their establishment is by count, which is what the
  requirement they were made under provides for.
- **Vectored record input at 64 KiB is marginal: −7.7% against a control worst of 5.18%**, in the
  noisiest step of the seven. It had been predicted unresolvable; this is weak evidence against
  that prediction, not a figure to quote.

**No socket identifier is attributed to any commit, by that run or any other.** The −8.5% at
concurrency 64 that is this finding's best evidence for the mechanism is a whole-set figure. Which
change produced it has not been measured, and the sign flip below is an argument about mechanism
rather than an attribution.

## The one that was thrown away

Handing callers reference-counted views of a pooled read buffer, instead of copying each delivery,
cut per-delivery allocation from **8,216 bytes to 24** — two orders of magnitude, exactly as
designed — and was **2.5% to 4.8% slower** at every payload size but one, against controls moving
0.73%. It was reverted. [`05`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md) has the
figures.

The mechanism is worth keeping even though the code is not. The cost is per delivery, not per
byte: largest on the arms doing least per delivery, absent at the size where one delivery carries
most. A pool's bookkeeping, a reference count taken and dropped, and the copy that a delivery
below the aliasing threshold still needs — and that copy is not optional, because without it one
retained byte pins a whole buffer — together cost more than the single allocation they replace.

This is the second time this workspace has recorded that a large allocation reduction did not
become a time reduction. The first was
[coalescing and buffer reuse](coalescing-buffer-reuse.md), where the gain turned out to be buffer
*growth* rather than allocation count. The lesson is the same one from a different angle, and it
is why this suite treats a count and a timing as different kinds of claim: a count is a property
of the code and true on every machine, and it is not evidence about time.

## What a new machine should reproduce

The percentages above belong to `xeon-8370c-azure` and to one pair of builds. On new hardware
the claim is falsifiable in this shape, and the ordering matters more than any single number:

1. **The counts, exactly, on any machine.** These are properties of the code, not of the host.
   The *after* halves are asserted by tests in this tree and a machine that does not reproduce
   them has a different build, not a different result: 3/10/66 writes per driver turn at
   concurrency 1/8/64 with an empty body and the same 3/10/66 at 64 KiB, 16/108/848 at 1 MiB,
   and zero bytes copied through the framer for a send of any size. They are asserted by
   `tests/ngnet-qmux-h3-tests/tests/concurrent_driver_writes.rs` and
   `crates/ngnet-qmux/tests/io_writes.rs`. The credit figures are a partial exception and are
   worth stating as such: `tests/ngnet-qmux-h3-tests/tests/credit_batching.rs` asserts a
   *relation* — that a run of window reports becomes strictly fewer and at most half as many
   extensions — rather than the exact 31 applications and 10 connection extensions observed for
   eight concurrent downloads, because an exact count there would pin the harness's scheduling
   rather than the batching. Reproduce the relation; treat the two numbers as an observation.
   The *before* halves — 7/14/70, 12/54/390,
   72/534/4230, 1,049,226 bytes copied, 42 credit applications and 21 extensions — cannot be
   re-taken from this tree, because the code that produced them is gone; they were measured by
   the same counters at `524fa54` and are recorded in the commits that removed them and in
   `.paw/work/qmux-h3-perf/Phase2Screen.md`. Treat them as a record rather than as something to
   verify.
2. **The sign flip on concurrency, before any magnitude.** `transport_concurrent_throughput`'s
   QMux arm improves at N=8 and N=64 and the duplex `concurrent_throughput` arm at the same
   parameters does not. If both families move the same way, the mechanism claimed here is not
   what is being measured, whatever the sizes are.
3. **The ordering across the body sweep, which is the reverse of the HTTP/2 finding's.** The
   gain grows with payload — largest at 1 MiB, smaller at 64 KiB, smaller again at 1 KiB, and
   negative with an empty body. [`write-path-and-gathering.md`](write-path-and-gathering.md)
   predicts the opposite ordering for a write-side change, and the reason the two differ is
   that this change also removed a per-record copy, which the HTTP/2 one did not. A new host
   that reproduces the HTTP/2 ordering here is measuring coalescing alone.
4. **The empty-body and serial-latency arms moving the wrong way, by a few percent.** They
   should be slightly worse, not neutral: there is no payload to amortise the buffer's
   bookkeeping over. A host on which they improve has something else going on.
5. **The drift controls first.** Before any of the above is read, the 46 unchanged identifiers
   in the session must move less than the effects being claimed —
   [`04`](../data/xeon-8370c-azure/04-qmux-drift-baseline.md) is what that looks like on this
   host, and `body_throughput/ngnet-qmux-h3/1048576` drifts far more than any other identifier
   in the suite. Read the 64 KiB point in preference to it wherever both are available.

The single most valuable run to repeat is the concurrency pair in step 2, because it is the only
one where the mechanism predicts two different signs and so cannot be satisfied by accident.

## What this does not establish

- **Nothing about compio, `shared_body`, or a multi-worker runtime.** No QMux arm exists in any of
  them, and the multi-worker group has none because of a recorded hang — which, incidentally, did
  not reproduce in 1520 attempts through the fixtures while this work was being done.
- **The small regressions are settled on one family and not the other.** All eight positive
  deltas are inside their session's control band; against each arm's own figure from `04`, the two
  socket ones (+1.7% and +4.0%) are outside its 1.41% and the four duplex ones are well inside its
  10.42%. So there is a small per-exchange cost on the socket arms and no established one on the
  duplex arms. Reported because a result may not be quoted in one direction only.
- **Nothing about memory.** Coalescing deliberately raised what a connection may hold awaiting a
  write from one record to about 80 KiB. That is a cost, it is by design, and it is not measured
  here.
- **Nothing about a real network.** Loopback throughout, which is where a syscall saving shows
  most cleanly and a latency change shows least.
