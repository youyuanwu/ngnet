# HTTP/3 over QMux: pending work

What is missing, and what would settle each.

## Performance work, in priority order

This is the implementation backlog produced by
[`09-qmux-h2-mechanisms`](../benchmarks/data/xeon-8370c-azure/09-qmux-h2-mechanisms.md).
The order is measured payoff divided by implementation risk, not source order.

Items 1, 2, 4, 5, 6, 8, 9 and 10 are settled; item 3 is closed without implementation after
reprofiling. Item 7 is the only open performance item.

1. **Replace `ngnet-h3`'s linear closed-stream lookup — settled.** `Driver::close_stream` scanned a
   1024-entry tombstone `Vec` on every close after a connection reaches steady state. A
   diagnostic that shortened the list reduced empty-exchange time by **11.7%** on a duplex and
   **6.9%** on a socket, and reduced the 64-stream duplex arm by **19.9%**. The fix kept the
   tombstone bound and insertion-order eviction, made membership constant-time, and did not ship
   the diagnostic value of sixteen. A synchronized hash index and FIFO queue now preserve the
   1024-entry semantics without the scan. The shipped implementation measures **13–18% faster
   on the tested duplex arms and 7–13% faster on the socket arms** in
   [`run 10`](../benchmarks/data/xeon-8370c-azure/10-h3-closed-stream-lookup.md). This is shared
   HTTP/3 work rather than QMux-specific work, and its verified resolution is in
   [`../h3/pending-work.md`](../h3/pending-work.md#a-linear-scan-on-every-stream-close).
2. **Decouple QMux flushing from HTTP/3 event-batch boundaries — settled.** QMux may now retain
   bounded output across productive internal driver passes, while a required transport
   operation flushes immediately before the HTTP/3 task can actually suspend. The
   correctness-required stream-ending boundary is unchanged. Exact socket counts at
   concurrency 1, 8, and 64 moved from 4/18.009/132.052 writes per batch to
   **3/3.009/3.052**, while HTTP/2 remained exactly two. Loopback concurrency improved
   **8.9–24.6%**, and the pre-registered serial-latency gate passed. The implementation and
   controlled evidence are recorded under
   [the write-count entry](#something-scales-with-in-flight-streams-on-a-real-socket--it-is-the-write-count)
   and in [`run 11`](../benchmarks/data/xeon-8370c-azure/11-qmux-flush-decoupling.md).
3. **Reuse `ngnet-h3::Driver::apply_events` scratch storage — closed without implementation.**
   Run 09's 2.39 microseconds / 8.1% attribution became stale after items 1 and 2 landed. Fresh
   profiling on their merged result, `700bfa6`, places inclusive `apply_events` across both roles at
   **0.259 microseconds / 1.04–1.05% on a duplex** and
   **0.268–0.280 microseconds / 0.74–0.78% on a socket** for an empty serial exchange.
   Concurrency 64 is 0.300–0.303 microseconds per exchange / 1.86–1.89%. Run 09's 2.39
   microseconds / 8.1% was a flat/self symbol bucket; the like-for-like fresh self costs are
   lower still. Exact call-site uprobes
   count twelve scratch-vector allocations per serial exchange, but the combined
   `take_events` plus `apply_events` path is only 0.365–0.548 microseconds per serial exchange.
   Reuse could remove only part of it and would put two-sweep ordering, same-batch reset replay,
   early-error cleanup, payload ownership, and retained-capacity bounds at risk for a whole-path
   inclusive bound of at most 2.28%, only a strict subset of which reuse could remove. The
   profile-first gate therefore rejected implementation before a prototype was created; see
   [`run 12`](../benchmarks/data/xeon-8370c-azure/12-apply-events-reprofile.md).
4. **Remove the duplicate QMux pump in `QmuxConnection::poll_event` — settled.** The production
   branch now pumps explicitly only when a release or translated event is already queued;
   otherwise `fill()` performs the one initial lower poll. Deterministic tests pin the queued
   release, held event, direct empty, terminal-error and pending-wake paths. Exact empty-exchange
   counts reproduce the diagnostic: reads fall **96 → 73**, pumps **93 → 70**, waker clones
   **94 → 71** and drops **90 → 67**, while 30 event polls and 14 transmit passes are unchanged.
   Controlled timing is **5.26% faster duplex serial and 2.37% faster socket serial** beyond
   matching controls and spread; duplex concurrency 64 and socket concurrency 1 also clear those
   bars. See [`run 14`](../benchmarks/data/xeon-8370c-azure/14-qmux-h3-one-pump.md).
5. **Collapse the HTTP/3 driver's repeated shared-state probes — settled.** One pass now drains
   ready, reset, credit, action and shutdown work under one lock, then processes it outside the
   lock; idle, completion and under-waker checks each use one coherent predicate. Exact matched
   count builds reduce the eleven old take/readiness/refresh entries from **155 → 29** per empty
   exchange. Controlled phase-1-to-snapshot timing improves duplex/socket serial by
   **7.59% / 5.68%** and both concurrency-1 targets by **7.86% / 6.29%**, beyond controls and
   spread. Fresh base-to-final serial timing is **11.75% / 8.59%** faster. The timing gate,
   rather than the count alone, retains the change; see
   [`run 15`](../benchmarks/data/xeon-8370c-azure/15-qmux-h3-shared-snapshot.md).
6. **Design a cheaper ownership path for delivered record data — closed after refreshed
   structural accounting.** The post-PR-45 baseline is 710 allocator calls at 1 MiB against
   HTTP/2's 194.5, refreshed from the 818-versus-205 counts that opened this item. Each
   exchange has 193 productive reads, 162 callback copies and 162 first-`Bytes` promotions;
   158 of 160 H3 body views cover their whole parent. A safe non-pooled read/record owner needs
   at least 193 owner allocations plus 162 `Bytes` control blocks, replacing only 324 current
   allocations, so it cannot satisfy the count gate; a per-record owner merely breaks even at
   324 before bookkeeping. Constructing `Bytes` in QMux violates its one-dependency boundary.
   The safe B3 range-deferral prototype did remove 160 calls (**710 → 550**) and reduced the
   sampled delivery share from 86.01% to 81.08%. Three 100-sample controlled passes improved
   duplex by only 0.43–1.09%; socket changed from a 0.31% improvement to a 0.20% regression,
   below the 2% and spread gates. The apparent
   `RawVec` opportunity is not framer retention—fresh counters observed zero framer growth and
   attributed 242 H3 growths to transport/event batches. Run 05's pooled negative remains
   valid; do not retry delivery ownership without a changed lower-layer owner API or a newly
   measured cost large enough to clear the elapsed gate. See
   [`run 18`](../benchmarks/data/xeon-8370c-azure/18-qmux-h3-candidate-b-delivery-ownership.md).
7. **Test HTTP/2 coalescing beyond one 16 KiB frame.** This is comparative work, not a QMux
   defect. At a 1 MiB exchange HTTP/2 issues 189 writes and QMux issues 67; the measured HTTP/2
   maximum is exactly 16 KiB while QMux can empty a 64 KiB buffer. A prototype must establish
   whether coalescing frames recovers the kernel-path gap without regressing latency or
   concurrency before it becomes an HTTP/2 change.
8. **Collapse the remaining QMux event-loop pumps — closed after measured implementation.**
   Fresh post-PR-45 attribution reconciled every empty-exchange pump: 23 from event filling,
   seven before queued events, three around open, 32 in transmit drains, and five forced flushes.
   A safe source-collapse implementation removed the duplicate open pre-pump and every
   unconditional transmit pump, retaining only a buffer-capacity pump. Exact counts improved
   from **70 → 37 pumps** and **73 → 40 reads**, with event/transmit/driver passes unchanged at
   30/14/14. Despite that 47% count reduction, three controlled passes improved duplex serial
   by 5.62% but socket serial by only **1.13%**, below both the 2% floor and the candidate's
   2.87% spread, so the code was reverted. Pending-read caching remains unsafe without a new
   guaranteed wake source, and driver-pass suppression lacks an outer turn boundary in the
   transport interface. Do not retry these mechanisms from counts alone; see
   [`run 17`](../benchmarks/data/xeon-8370c-azure/17-qmux-h3-candidate-a-read-pump-amplification.md).
9. **Reduce fixed HTTP/3 header/QPACK overhead — measured and reverted.** A complete bounded
   prototype inlined small received fields with heap fallback, reserved known field capacities,
   and removed redundant validation/submission vectors without changing public APIs or validation.
   It reduced exact per-exchange allocator activity from **128.02/6.02/128.02** to
   **108.02/3.02/108.02 malloc/realloc/free**. Timing remained below the retention gate: duplex passes changed
   −1.30% to −1.92%, while socket changed −0.74% to −0.88%, across three 100-sample passes.
   No raw result cleared 2%. The code was reverted. The per-section slot scan remained
   below profile resolution, and Registry/Tasks map work was too small to rescue the failed socket
   gate. Do not retry header-storage or registry changes from allocation counts alone. Native QPACK
   algorithm changes or a concrete concurrency-only mechanism would need independent gates. See
   [`run 19`](../benchmarks/data/xeon-8370c-azure/19-qmux-h3-candidate-c-fixed-header-work.md).
10. **Reduce QMux event-queue traffic independently — closed as coupled to Candidate A.** Fresh
    counts still show **23 pops = 23 `Inner::fill` iterations**, seven pushes and 16 empty pops per
    exchange. An uncontended locked empty pop costs 16.09–16.26 ns, so skipping all empty locks is
    only about 0.26 µs and still leaves 23 registered pop calls. Atomic emptiness hints and storage
    changes therefore fail the count gate; wholesale drain violates the lower queue's read-ahead
    accounting boundary. Reducing caller invocations requires changing the fill/driver schedule,
    which is the already measured and reverted Candidate A mechanism. See
    [`run 20`](../benchmarks/data/xeon-8370c-azure/20-qmux-h3-candidate-d-event-queue.md).

The duplicate-pump portion of the former 91-read observation is resolved in item 4; the remaining
reads are not presumed redundant. The 16382-byte record payload and the fixed 64-offer yield were
eliminated as causes of the concurrency inversion. Do not pursue allocation counts without a
timing hypothesis—the delivery-aliasing experiment already showed that a large allocation
reduction can be slower—and do not modify `deps/dwnx` as part of this backlog.

## Interoperability is proven against nothing

Everything here runs against this workspace's own stack: `ngnet-h3` over `ngnet-qmux` over an
in-memory byte-stream pair or a loopback TCP socket. Both ends of every test are this code.

That is a weaker position than the QUIC join is in, which at least runs against quinn. QMux is
an unratified draft and no other implementation is known to exist, so there is currently
nothing to interoperate *with* — the gap is real and it is not closable by effort here.

**What would settle it:** a second QMux implementation appearing, or dwnx's own example client
and server being driven against this stack. The latter is possible today and has not been done.

*The write-path work learned nothing about this.* It changed record boundaries, write
boundaries and the timing of window extensions — all of them things a conforming peer must
already tolerate, and none of them a reason to expect a different answer from an
interoperability run that has never been attempted.

## There is no structural test suite

`ngnet-quic-h3` ships `tests/invariants.rs`, which reads its own source and asserts that
nothing here names a socket or a thread, that module files are flat, that nothing is
`include!`d, and that the manifest declares exactly what it should. This crate ships no
equivalent, so the claims on `invariants.md` that would belong to such a suite are marked there
as resting on the compiler or on review instead.

**What would settle it:** the same suite, with the forbidden names adjusted — `TcpStream` is a
plausible thing for a QMux-adjacent crate to acquire by accident in a way it is not for a QUIC
one, so the list is not a copy.

*The write-path work learned nothing about this.* It added tests, several of them, but all of
them are behavioural — write counts, copy counts, allocation counts, credit applications — and
a behavioural test says nothing about whether the crate's shape is what it claims to be. If
anything the case for the suite is a little stronger than it was, because the work introduced
two new public constants and three new public methods, which is more surface for a manifest or
a module-shape assertion to have opinions about.

## The connection is configurable, but not adjustable once it is up

`connect_with` and `serve_with` take a `TransportConfig` and an `HttpConfig`, and
`QmuxConnection::client_with`/`server_with` take the transport half, so flow-control windows,
the stream allowances, the read-ahead budget, the idle timeout and the HTTP/3 layer's own
settings are all reachable from a caller. `connect` and `serve` remain, forwarding the
defaults, so nothing that compiled before needs a configuration it does not care about. That
closes the entry this section used to hold.

What is *not* settled is everything after construction. A `Config` is consumed when the
connection is built and there is no way to change any of it afterwards — which matters most
for the stream allowance, since the whole point of QMux's cumulative stream budget is that it
is meant to be extended over the life of a connection. That is the separate entry below, and
it is a defect rather than a deliberate narrowing.

Two smaller gaps remain here:

- **Not every field of `ngnet_qmux::io::Config` is independent of the others.** The read-ahead
  allowance must not exceed the connection window, and a caller that sets one without the other
  discovers the constraint from the layer below rather than from this crate's signature.
- **A `Config` cannot be read back off a live connection.** That is the observability gap
  recorded in the next section, and it is now slightly sharper: a caller can set values it
  cannot subsequently confirm the connection is actually running with.

**What would settle the remainder:** accessors, which the observability entry covers, and a
decision about whether the two configurations should be validated jointly at construction
rather than separately by the layers that consume them.

*The write-path work learned nothing about post-construction adjustment*, and it is worth
saying why not, because it added two constants that look like configuration and are not.
`OUTBOUND_CARRY` and `OUTBOUND_CEILING` are compile-time constants rather than `Config` fields,
deliberately: they describe a buffer the layer manages on the caller's behalf and never hands
out, so a caller has nothing to do with the numbers except read them to size its own
expectations. Making them configurable would add two more values that cannot be adjusted once
the connection is up, which is the very complaint this entry records.

## The stream allowance is never extended, and a connection stops at its initial budget

`max_streams_bidi` in QMux is a **cumulative budget, not a concurrency limit**: it counts every
stream ever opened on the connection, not the number open at one time, and the peer is expected
to raise it as streams complete. Nothing in this crate ever does. A connection therefore opens
exactly `max_streams_bidi` streams over its whole life and then stops.

The failure mode is the bad one. On the `max_streams_bidi + 1`-th request the connection does
not return an error, does not close, and does not report anything: it **hangs**. The request
future never completes, the pump keeps running, and the caller has no signal to distinguish it
from a slow peer. A budget exhausted at request 101 on a default connection looks exactly like
a network that stopped.

The mechanism is not a missing capability. `extend_stream_limit` exists on
`crates/ngnet-qmux/src/io/conn.rs` and does what its name says. This crate never calls it —
neither on stream close, nor on a low-water mark, nor on demand — so the initial transport
parameter is the whole allowance for the connection's life.

Raising the initial value is a workaround and not a fix, and it has a ceiling. dwnx caps a
transport parameter at `DWNX_MAX_STREAMS`, `1 << 60`
(`deps/dwnx/lib/dwnx_transport_params.h`). Values at or above `1 << 61` pass
`TransportParams::validate`, which only checks that the number fits a QUIC varint, and then
fail the connection at setup with `ErrorKind::Closed` — a validation gap on the QMux side worth
noting on its own. So the largest allowance a connection can actually be given is `1 << 60`,
which postpones the hang rather than removing it, and a long-lived connection is precisely the
case where a cumulative budget runs out.

The benchmark harness works around this by asking for `1 << 40` streams up front, which no
benchmark run will exhaust. That is a harness choice made because the benchmarks must not
measure a workaround's cost, and it should not be read as a recommendation: production code
cannot pick a number large enough for a connection with no known lifetime.

**What would settle it:** calling `extend_stream_limit` from the pump when streams close —
with a decision about the policy, since extending on every close is a frame per stream and
extending on a low-water mark risks a stall if the peer is exactly at the boundary. A test that
opens `max_streams_bidi + 1` streams and asserts the last one either succeeds or fails with an
error is the thing that is missing either way; today it would hang, which is why it has to be
written with a timeout.

*The write-path work learned nothing about this.* It ran the suite repeatedly against the
`1 << 40` harness workaround and never approached the budget, which is what the workaround is
for, so no run it took bears on what happens at the boundary. It did not touch
`extend_stream_limit`, and its one change to credit — batching window extensions for the length
of a run — is deliberately about the *flow-control* windows and not about the stream allowance:
those are separate budgets with separate frames, and the batching code path never calls the
stream-limit extension at all. The defect was screened against FR-033 before any optimisation
was attempted and was found not to block the measurements this work needed, which is why it was
left exactly as it is.

## The join hangs at high concurrency on a multi-worker runtime

With sixty-four requests issued together on one connection and a tokio runtime with more than
one worker thread, the join wedges: **roughly three attempts in four never complete**, at both
two and four workers, typically after about fifty-five of the sixty-four requests have
finished. The remaining futures never resolve, no error is produced, and nothing closes.

What narrowed it when it was first recorded:

- Concurrency 1 and 8 complete on every runtime tried.
- A `current_thread` runtime completes at every concurrency tried, including 64.
- It reproduces over an in-memory byte-stream pair, so it needs no socket and no kernel.
- Loopback TCP is clean throughout, so it is not transport-specific.
- It persists with the flow-control windows and the stream allowance raised far out of the
  way, so it is neither credit exhaustion nor the budget exhaustion recorded above.

That combination points at the pump's wakeup handling rather than at protocol state: something
that is a lost wakeup when two threads race and is not reachable when the same work is
serialised on one. Nothing here has been changed to address it.

### What the write-path work found, without fixing it

Before any optimisation was attempted, the defect was screened under FR-033 — the rule that a
known defect is only worked on if it blocks a measurement — and screening it meant driving it
deliberately. The reproduction was a throwaway test under `tests/ngnet-bench/tests/`, every wait
bounded by a five-second timeout, deleted after the runs; the numbers below are recorded in
`.paw/work/qmux-h3-perf/Phase2Screen.md` and a reader who wants them again has to write the
harness again. They are counts from a bounded fixture, not timings from the benchmark suite, and
they carry none of `docs/benchmarks/controls.md`'s guarantees.

Four things changed about what is known, and one of them corrects a claim above.

- **The benchmark fixtures do not hang, at all.** `NgnetQmuxH3` and `NgnetQmuxH3Socket`, exactly
  as the arms use them, completed **1,520 out of 1,520 attempts** — 760 in a debug build and the
  same table again in release — across duplex and loopback TCP, `current_thread` and two and
  four workers, at concurrencies 1, 8 and 64. Against a recorded rate of three failures in four
  that is not a weaker sample of the same phenomenon; it is a different answer, and the reason
  is the next point.
- **A single response header decides it.** With the fixture unrolled so the response is built
  without its `content-type` header, the same workload wedges at **100%**, not 75%. Adding the
  header takes it back to 0% at every point tried up to 128 streams. `response_for` in
  `tests/ngnet-bench/src/lib.rs` sets that header, which is why the arms are clean: they are not
  avoiding the defect by design, they are one header away from it, and anyone editing that
  fixture should know that.
- **The threshold is exact, and the shortfall is linear.** Sixty-three concurrent streams is
  clean; sixty-four hangs. Between 64 and 96 streams the number of response heads that arrive is
  `126 − N`, so the shortfall grows at two exchanges per additional stream. The server side is
  not where it stops: every one of the 65 handlers ran to completion and returned a response, and
  the missing exchanges are clients waiting for a head whose response already exists. At N = 128
  it stopped one short, which does not fit that line and is not explained.
- **"Loopback TCP is clean throughout" is wrong, and is corrected here rather than deleted
  above** so that the change of state is visible. In a *release* build, four workers at N = 64
  without the header hung once in four attempts, reaching 49 of 64. Debug loopback runs were
  clean. The substrate changes the rate, not the outcome.
- **It is not the sixty-four-offer yield**, which is the coincidence the in-flight-streams entry
  below flags as worth not ignoring. `MAX_OFFERS` was temporarily set to 32 and the threshold
  stayed at 64 concurrent streams, with 30, 31, 32, 33, 34, 40 and 48 all clean; the constant was
  restored and the tree left unmodified. That suspect can be struck off.

Requiring more than one worker thread is confirmed: `current_thread` and a multi-thread runtime
with a single worker are both clean at every point tried. Nothing was narrowed beyond this and
nothing was changed, as the screen required.

**This is why one benchmark group has no QMux arm** — and the reason has now shifted. The
suite's `concurrent_throughput_multi_thread` group runs the same sweep as its single-threaded
sibling on a four-worker runtime; the QMux arm was written, and it was left out because it hung.
On the evidence above the fixture it would use does *not* hang, so the group is now technically
addable, and it should still not be added — for a reason that has nothing to do with this
defect. It is a **duplex** group, so it cannot show a syscall saving; the arm's value would be
the userspace bookkeeping its single-threaded sibling already reports, plus the scheduling noise
the group exists to display. Adding an arm that sits one response header away from a
deterministic wedge, to measure something another arm measures more cleanly, is a bad trade.
`docs/benchmarks/cases/concurrent-throughput.md` records the omission and points here. Every
other group in the suite carries a QMux arm except the two shared-body groups, whose absence has
an unrelated cause and must not be filed with this one.

**What would settle it:** the reduced reproduction now exists in outline — sixty-four streams,
two workers, no `content-type` on the response — so what is left is the wakeup path under it,
and an explanation for why one response header moves the outcome from certain to impossible.
That header changes the size and the count of the records a response occupies, which is the
first place to look. A timeout-guarded test at concurrency 64 on a multi-worker runtime with the
header omitted is the regression test, and it would fail today, which is why it is described
here rather than committed.

## The connection is not observable

There is no way to ask a live connection how much read-ahead it is holding, what its peer's
transport parameters said, or how many streams are open. The layer below exposes some of that
on its own `Connection`, and this crate takes ownership of that value at construction, so a
caller holding a `QmuxConnection` cannot reach it.

**What would settle it:** deciding whether accessors belong on this crate's type. The QUIC join
has the same gap for the same reason, and both should be answered together.

*The write-path work learned nothing about the design question, and made the gap slightly
wider.* It added counters — `Connection::copied_record_bytes`, `RecordFramer::copied_bytes`, and
a group of write-log and credit accessors in `ngnet_qmux::io::testing` — so there is now more
state a caller might reasonably want to see, and the ones under `testing` are `#[doc(hidden)]`
and exist to let a test assert a count, not to be an observability interface. Reading them as
one would be a mistake: they are shaped for assertions and their names, arity and existence are
not covered by any stability claim.

## Body bytes are copied once before this crate sees them, and that one was measured

The heading used to say twice. Inbound, the QMux layer copied every record's payload into the
framer's retention *and* copied each delivery into an owned `Vec` for its event. The first is
gone for a record that arrives whole, which is nearly all of them; the second is still paid.
This crate turns the delivered `Vec` into `Bytes` by taking the allocation over, so it adds
nothing to either. Outbound, `StreamSource::write_next` lends buffers that are invalid once the
closure returns, so the bytes are copied into the record being built — one copy, and the reason
`RETAINS_BUFFERS` is `false`.

Both are consequences of gaps in dwnx rather than of anything decided here; see
`docs/qmux/pending-work.md`, which records what would remove each.

**It is no longer unmeasured, and the answer was not the expected one.** The entry used to ask
for a build with the inbound copy removed. That build exists: deliveries became reference-counted
views into a pooled read buffer, per-delivery allocation fell from 8,216 bytes to 24 — and it was
slower at every payload size but one, by two to five percent against controls that moved under
one. It was reverted.
[`05-qmux-delivery-aliasing`](../benchmarks/data/xeon-8370c-azure/05-qmux-delivery-aliasing.md)
has the figures and the mechanism. So the remaining question is not "what does this copy cost" but "why does removing it
not pay", and the leading answer is that a pooled buffer's bookkeeping and the copy a short
delivery still needs are together larger than one allocation of the size being avoided.

## A multi-slice offer is one write, and one run of records — settled

`StreamSource::write_next` may hand over several `IoSlice`s at once, and this crate now submits
the whole list in one call: `Connection::try_write_stream_vectored` in the layer below, over
`RecordWriter::push_vectored` and `dwnx_conn_writev_stream`. The fragments share records, so a
slice boundary is no longer a record boundary and a request's headers ride inside the body's
first record instead of occupying an undersized one of their own.

**What it used to be.** One `try_write_stream` per slice, stopping at the first not fully
accepted, and a call begins a fresh record however few bytes the slice holds — so at least one
record per slice. Before write coalescing it was worse still: the layer below refused a second
production while a record was outstanding, so the second slice answered `Blocked`, the loop
broke, and the offer reported only what the first slice had produced — one record *and one write
and one pass through the pump* per slice. Coalescing removed the write and the pass; this
removes the record.

**What it was worth, and why that is a count rather than a time.** Measured on a 64 KiB POST
against an otherwise identical body-less request, end to end through this stack
(`tests/ngnet-qmux-h3-tests/tests/fragmented_offers.rs`):

| | before | after |
| --- | --- | --- |
| records in the write carrying the request | 6 | 5 |
| bytes the body cost over a body-less request | 65 587 | 65 579 |
| writes the client's byte stream saw | 3 | 3 |

One record and its eight bytes of framing — a two-byte record length prefix and dwnx's STREAM
frame header — per request with a body. The write count does not move, because coalescing had
already merged those records into one write; that is the honest shape of the result and not a
disappointment, since the record is what a fragment boundary cost once the write had stopped
costing anything.

**The establishment is a count, and the prediction that a timing would say nothing was wrong.**
FR-021 accepts a mechanism established by a count, and the record and byte counts are that. The
argument originally made here — that a timed comparison would report a number inside its own
noise — quoted the Phase 2 screen's saving as one write in two at 1 KiB and one in sixty-six at
1 MiB, and omitted the screen's middle point, **one write out of six at 64 KiB**, which is around
seventeen percent and does not sit inside a 0.5%-to-5% band. A timing does exist:
[`07-qmux-per-commit-attribution`](../benchmarks/data/xeon-8370c-azure/07-qmux-per-commit-attribution.md)
reports **−7.7% at 64 KiB against a control worst of 5.18%** — outside the band, in the noisiest
step of seven, and marginal rather than settled. It deserves a run of its own before it is quoted
as a figure. The counts stay the establishment; the correction is kept because an argument built
by leaving out the inconvenient point is the kind that should be visible afterwards.

**The part that is delicate, recorded because it is silent when it is wrong.** A vectored push
reports `*pdatalen` as **one total across every vector**, not a count per vector, so resuming
after a short take means walking the array against a byte count — and a short take routinely
stops part-way through a fragment rather than between two. A walk that assumed whole fragments
were taken would send some bytes twice and others never, and would report a count that agreed
with itself; nothing above this layer could notice. The walk lives in one place, `Fragments` in
`crates/ngnet-qmux/src/io/conn.rs`, and the single-slice write is expressed as its degenerate
case rather than kept as a second loop. Its guard is
`a_take_that_stops_inside_a_fragment_resumes_inside_it` in
`crates/ngnet-qmux/tests/io_vectored.rs`.

## Window extensions are batched here, because the HTTP/3 layer does not batch them — settled

The question Spec FR-037 asks is whether flow-control extensions and the read-ahead wakeups they
cause are *already* coalesced within one transmit pass. `CodeResearch.md` left it open, having
not read the HTTP/3 driver's credit path. It has now been read, and the answer is **no**.

**The evidence.** `Driver::extend` in `crates/ngnet-h3/src/http/driver.rs` makes two
`extend_credit` calls for the same bytes — the stream's window and the connection's, which are
separate and neither implies the other — and it is reached from three places inside a single
pass: once per `QuicEvent::Data` the driver applied, via `Driver::read`; once per stream whose
QPACK-deferred credit was released; and once per credit entry the caller returned by reading.
Nothing between those sites accumulates, and every one of them used to become an
`extend_stream_credit` or `extend_connection_credit` call on the connection below, each of which
marks the connection as having something to produce and each connection-level one of which also
wakes the read-ahead pump. Read rather than assumed: the reading is pinned by a count, in
`tests/ngnet-qmux-h3-tests/tests/credit_batching.rs`, so a driver that starts coalescing fails a
test that names this finding rather than silently invalidating it.

**What was done.** Batched at this seam. `Inner::defer_credit` accumulates a run — one sum per
stream, one for the connection — and `Inner::flush_credit` applies it at the first interaction
with the layer below that follows: `poll_event`, `poll_open_uni`, `poll_open_bi`,
`poll_transmit`, `reset`, `stop_sending`, `close`, and the tail's `poll_finish`.

**Why here and not in `ngnet-h3`.** `ngnet-h3` is shared with the QUIC stack, which cannot be
fully built on this host — so a change there could not have been verified against the other
consumer of the trait it changes. This seam is QMux-only, and the bias of that choice is stated
plainly: it fixes the cost for this transport and leaves it in place for the other one.

**The rejected alternative.** Flushing only in `poll_transmit`, which is the largest batch
available and the one place a pass demonstrably ends. Rejected because the driver's loop can
park between reporting credit and transmitting — `poll_open_bi` waits for stream capacity the
peer has not granted — and credit stranded behind that park is a window the peer is never told
about while both ends wait for the other. The rule adopted instead is that a run ends at the
*next interaction of any kind*, which needs no list of which interactions can park.

**What it is worth, by count.** Eight concurrent 64 KiB downloads, measured end to end
(`credit_batching.rs`):

| | before | after |
| --- | --- | --- |
| `extend_credit` calls the driver made | 42 | 42 |
| extensions applied to the QMux connection | 42 | 31 |
| connection-window extensions, and so read-ahead wakeups | 21 | 10 |

The "before" column needs no stashed build: every call was forwarded straight through, so the
driver's call count *is* what the connection below used to see. The stream half of the total
does not move here — each stream that delivers in a pass needs its own stream-window extension
either way, and these bodies deliver at most once per stream per run — so the whole of the
saving is the shared connection window, which is also the one that fires the pump's waker.

**No timing was taken, and that is a decision**, on the same grounds as the vectored-write
entry above: FR-021 accepts a mechanism established by a count where no benchmark identifier can
resolve it, and eleven fewer calls into a state machine per eight-stream exchange is not
something an end-to-end arm can separate from its own drift.

**Bias in the measurement, stated.** The harness stops at the completed exchange rather than at
a closed connection, so credit still held when the run ends is never applied. That flatters the
"after" figure by at most one flush — one connection extension — which is inside the margin the
test asserts.

**What is bounded, and what happens at the bound.** The per-stream run is a `Vec` scanned
linearly, which is right for the single-digit stream counts a pass delivers on and wrong for
hundreds. `MAX_PENDING_STREAMS` (sixty-four, the driver's own per-pass offer bound) applies the
run early rather than letting the scan grow, so the worst case is exactly the behaviour this
replaced rather than something worse.

## Something scales with in-flight streams on a real socket — it is the write count

**Explained, fixed, and closed.** The answer is at the head of this entry; everything below it
is the history of getting there, kept because a lead's provenance is part of it.

### The answer

[`09-qmux-h2-mechanisms`](../benchmarks/data/xeon-8370c-azure/09-qmux-h2-mechanisms.md) counted
writes per exchange over a socket at one, eight and sixty-four streams. QMux fits **`2n + 2`**;
HTTP/2 is **constant at 2**, one stream or sixty-four. Reads do not scale on either side — QMux
takes three at every concurrency, so the sixty-four responses do arrive coalesced. The bytes are
collectable and only the writer will not collect them.

The cause was the interaction between the batch boundary and forced per-interaction flushing.
The boundary at the join is required for correctness. `ngnet-h3`
applies control-plane events before data events within a batch, so a stream ending sharing a batch
with that stream's last bytes would release the stream before the bytes were read. `poll_event`
therefore returns `Pending` at every stream ending to start a fresh batch. Earlier text called
that the end of an HTTP/3 driver turn; code research corrected the conflation. This `Pending`
ends one event batch, but the connection future may execute further productive passes in the
same executor poll. The old adapter nevertheless forced the QMux buffer at every such
interaction, turning each boundary into a write. Removing the boundary still breaks the
connection exactly as `emitted_since_pending` predicts: the warm-up dies with *the exchange
ended before a response arrived*.

Over a duplex each of those writes is a memcpy and the penalty is mild; over a socket each is a
syscall, which is why this is the one workload whose ratio worsens when a kernel is added.

**This corrects a claim made in this entry's own text below**, and in
[`../benchmarks/data/xeon-8370c-azure/README.md`](../benchmarks/data/xeon-8370c-azure/README.md).
The write-path work established that the write count *per driver turn* no longer grew with the
streams in flight. That was true but incomplete: the number of driver passes grew instead, so
writes per exchange still did at the old revision. Run 11 removes that second coupling.

**What settled it.** Productive event/open/transmit operations use bounded buffering, and
`QuicConnection::poll_flush` is called at every real suspension site: binding, bidirectional
stream opening, transmit backpressure, and the idle event poll. Capacity pressure still writes
immediately, and close, finish, orderly EOF, and explicit error handling retain their own
finalization paths. No timer or unrelated future wakeup is involved. The ending boundary and
its self-wake are unchanged.

[`11-qmux-flush-decoupling`](../benchmarks/data/xeon-8370c-azure/11-qmux-flush-decoupling.md)
measured exact socket writes at 1, 8, and 64 streams as **3/3.009/3.052**, replacing
4/18.009/132.052. HTTP/2 stayed exactly two. The socket timing arms improved 8.9% at n=1,
21.9% at n=8, and 24.6% at n=64; the normalised serial gate passed. The deterministic
both-endpoint regression independently reports 5/5/5, so a return of the linear term fails
without depending on scheduler timing.

### The history

**Now measured, its leading suspect eliminated — and then explained, above.** This began as a
lead from unpinned exploratory runs, and that provenance is kept below because the sequence
matters. It is no longer the evidence:
[`08-qmux-against-h2`](../benchmarks/data/xeon-8370c-azure/08-qmux-against-h2.md) measured it
properly, over five pinned passes with the ratio formed inside each pass.

What it found. Every workload in the suite has a *smaller* QMux-to-HTTP/2 ratio with a kernel in
the path than without one — a body at 1 MiB goes from 1.34× on a duplex to 0.89× on a socket —
**except concurrency, which goes the other way**: 2.33× on a duplex against **3.12×** on a socket
at sixty-four streams, and 2.48× against **3.14×** at eight. Five passes out of five, at both
concurrencies, with per-pass ranges under 0.06×. So the shape the exploratory runs suggested is
real, and it is the only place in the suite where adding a kernel makes QMux's position worse.

What it eliminates. The leading suspect was one write per offered `IoSlice`. That mechanism is
gone — a fragmented offer is now one vectored push into as few records as the size allows, and
the write count per driver turn no longer grows with the streams in flight — and **the inversion
survived its removal**. The candidate that produced this entry is therefore not the cause, or not
the only one.

What is left. The two remaining candidates named when this entry was written are untouched: QMux
produces more records for the same payload than HTTP/2 produces frames, since a record caps at
16382 bytes against a 16384-byte frame payload; and the pump's fixed sixty-four-offer yield may
interact with sixty-four concurrent streams in a way worth not dismissing. A third was named
here as most promising — that QMux's favourable 61% kernel-path cost per megabyte might be
unfavourable on the concurrency axis — and it was right about where to look but not about the
direction: the counts came back showing QMux writing *more* often at concurrency, not fewer and
larger. **What settled it** was the count this entry asked for, taken in
[`09`](../benchmarks/data/xeon-8370c-azure/09-qmux-h2-mechanisms.md).

The original provenance note, kept because a lead's history is part of it: the numbers that
produced this entry came from unpinned, short-sample exploratory runs taken while the benchmark
arms were being built, with no drift controls and no replication. They were never filed under
`docs/benchmarks/data/` and must still not be quoted as results.

Across the suite the QMux arm's cost relative to its HTTP/2 counterpart behaved like a fixed
per-exchange overhead: largest with an empty body, smallest at 1 MiB, and smaller with a
kernel in the way than without one — which is what an overhead amortised over a growing
payload, or diluted by a growing constant, looks like. **One point did not fit.** Concurrency
64 over a real socket was the only place where the socket ratio *exceeded* the duplex ratio for
the same parameter, and it was worse than the same arm's own empty-body ratio. Everything else
got relatively better as more work was added; that point got worse.

A fixed cost per exchange cannot produce that. Something that scales with the number of streams
in flight can, and the entry above was the obvious candidate: one write per `IoSlice`, stopping
at the first not fully accepted, cost nothing without a kernel and cost a syscall each with
one. **That mechanism is gone** — the entry above is settled — which makes this lead weaker
rather than stronger: its obvious explanation has been removed, and a shape that survives a
pinned run now needs another one. That is the same mechanism the HTTP/2 write-path finding turned on
(`docs/benchmarks/findings/write-path-and-gathering.md`), which is a reason to suspect it and
not evidence that it is the cause here. It is not the same *fix*: the HTTP/2 finding was won by
gathering a driver pass into one `writev`, and the layer below has since established that its
output is a single region with nothing to gather and no copy for gathering to avoid
(`docs/qmux/pending-work.md`). What would reduce the write count here is fewer records for the
same payload — the vectored push above, now built — not a gathering byte stream.

Two other candidates have not been ruled out: the record layer produces more records for the
same payload than HTTP/2 produces frames, since QMux's maximum record is 16382 bytes against
HTTP/2's 16384-byte payload; and the pump's fixed sixty-four-offer yield may interact with
sixty-four concurrent streams in a way that is not a coincidence worth ignoring. The hang entry
above reports that setting `MAX_OFFERS` to 32 did not move the hang's threshold, and that
strikes the constant off as a cause of *the hang* only — it was a wedge-or-not experiment on an
unrolled fixture, not a timed run, so it says nothing about whether the constant costs anything
at concurrency 64, and it must not be borrowed to close this candidate.

**What run 06 established, and what it did not.** The suspected mechanism was removed and the
result was measured under controls
([`06-qmux-write-path`](../benchmarks/data/xeon-8370c-azure/06-qmux-write-path.md)). At the
anomalous point — `transport_concurrent_throughput/ngnet-qmux-h3-tokio/64`, the socket arm — the
build got **8.5% faster**, and its concurrency-8 sibling 7.1% faster. At the *same* parameters
the duplex arms went the other way, 1.8% and 2.1% *slower*. That sign flip is the load-bearing
observation: it is one session, one machine, one pair of builds, and the two families differ in
exactly one thing, whether a write reaches a kernel. Drift cannot produce it — the two socket
identifiers drift −0.22% and −0.50% under
[`04-qmux-drift-baseline`](../benchmarks/data/xeon-8370c-azure/04-qmux-drift-baseline.md), and
the session's 46 unchanged controls moved 1.06% on average and 4.47% at worst. So a cost that
scaled with streams in flight and was paid only with a socket in the way did exist, it was the
write count, and it is now smaller: seventy writes per driver turn at concurrency 64 became
sixty-six with an empty body and, with a body, 390 became 66 at 64 KiB.

**The lead is narrowed, not closed, and the reason is precise.** What run 06 compares is one
build of the QMux arm against another. What this entry is about is a *ratio* — the QMux arm
against its HTTP/2 counterpart — and no run under `docs/benchmarks/data/` computes that ratio
under controls for either family, because the sessions that produced these figures were paired
build comparisons and cross-protocol arms in a paired session carry the drift of two protocols
rather than one. So it remains unknown whether the shape that prompted this entry survives at
all. What is known is that its obvious cause has been removed and that removing it moved the
anomalous point substantially and in the predicted direction, which is the strongest thing a
lead can have short of being measured.

**What would settle it:** a pinned, replicated run of `transport_concurrent_throughput` and
`concurrent_throughput` across the full 1/8/64 sweep with drift controls, recorded under
`docs/benchmarks/data/` as a run, comparing the QMux and HTTP/2 arms in the same session — that
is the ratio, and it has still never been taken. If the shape is gone, this entry closes and the
cause was the write count. If it holds, the next step is a syscall count per pass for the QMux
arm at each concurrency: a count that still grows with `N` after coalescing would place the
cause in the record count or the offer bound rather than in the write path, and the two
candidates above are then the places to look.

## The transmit pass yields on a fixed count

A pass takes at most sixty-four offers and then returns, so a layer with an endless supply
cannot keep it from returning to the driver. Sixty-four accepted offers used to be on the order
of a megabyte, which was a guess rather than a measurement: too low costs wakeups on a large
body, too high delays the events the driver has to attend to. Multi-record production loosened
that reading — an offer is now worth as many records as the outbound buffer will hold, so the
cap bounds offers rather than bytes and what bounds the bytes is the buffer's ceiling. The
constant is unchanged, and the trade it encodes is now less about bytes than about how many
different streams a pass will visit.

**The write-path work did not answer this, and did not try.** No run it took varies the
constant, so nothing on file says what sixty-four costs or saves. Two things about it did
change, and neither is an answer. The reading changed, as described above: the same number now
bounds a different quantity, so a figure taken against the old build would not transfer even if
one existed. And a second constant now sits beside it — `MAX_PENDING_STREAMS` in the credit
batching, deliberately given the same value of sixty-four *because* it is the driver's per-pass
offer bound, which means a future experiment that changes `MAX_OFFERS` has to decide whether to
move that one with it or hold it fixed and measure the two separately. The one experiment that
did move `MAX_OFFERS`, recorded in the hang entry above, set it to 32 to see whether a wedge
still occurred; it was not timed, and it establishes nothing about cost.

**What would settle it:** a benchmark showing which end of that trade actually costs anything.
The suite added in `docs/benchmarks/` does not, because it holds the count fixed at sixty-four
in every arm; what it would take is the same sweep run against two builds differing only in
that constant.

## Each request is submitted in a driver pass of its own — the write consequence is settled

Coalescing removed nearly every write from a payload-carrying workload and left the empty-body
concurrency arms almost untouched: at concurrency 64 the count went from seventy writes per
driver turn to sixty-six, which is a four-write saving where the same concurrency carrying
64 KiB bodies fell from 390 writes to 66, a saving of 83%. The cause is not in the write path,
and the four writes that did disappear say where it is — they are the connection preamble, which
genuinely did have several records to merge, and not the requests.

The requests did not merge because each is submitted in a driver pass of its own and the old
adapter flushed at the end of each interaction. It was correct that a caller may stop polling
at a public boundary and bytes cannot be left waiting there; it was incorrect to infer that
every productive internal pass was such a boundary. The screen in
`.paw/work/qmux-h3-perf/Phase2Screen.md` predicted a collapse here and was wrong for an
instructive reason: its model merged every write within a harness *turn*, and a turn is one poll
of the connection future and contains many passes. That made it an upper bound rather than a
prediction, and the distance between the bound and the outcome is exactly the per-pass forced
write.

This residual was worth naming rather than filing under noise, because "coalescing saved most
of the writes" was true of a body and false of a request burst, and the two were easy to
conflate. It is the historical baseline replaced by the regression in
`tests/ngnet-qmux-h3-tests/tests/concurrent_driver_writes.rs`.

**What settled it:** the driver, unlike the transport in isolation, does know whether it is
about to stop. It now invokes an explicit flush operation immediately before each actual task
suspension. Productive passes can therefore share the bounded buffer without guessing whether
another call will arrive, while every real stop-polling boundary either drains or registers the
transport's write wake. Exact socket counts are approximately three at 1, 8, and 64 streams,
and the deterministic both-endpoint test is exactly five at all three points; see
[`run 11`](../benchmarks/data/xeon-8370c-azure/11-qmux-flush-decoupling.md).

## No datagrams, no WebTransport, no priority

Neither `ngnet-h3` nor `ngnet-qmux` exposes unreliable datagrams, so this crate cannot.
WebTransport is the QMux draft's other stated motivation and is not implemented anywhere in
this workspace. The transport trait has no priority concept, so nghttp3's support for the
HTTP/3 priority scheme is unreachable from here.

See both families' pending-work documents.

*The write-path work learned nothing about any of the three.* Datagrams and priority are absent
from the traits below this crate, so nothing it changed about how records are written or how
credit is timed could bear on them. It is worth noting one thing it did *not* foreclose:
`ngnet_qmux::VectoredWriteRequest` and `push_vectored` let one record carry several caller
fragments, which is a mechanism a datagram or WebTransport path would plausibly want, and it is
public rather than internal. That is availability, not evidence.

## Nothing serves an axum router over this

`ngnet-axum` serves an axum `Router` without hyper, and doing the same over a QMux-carried
connection is the obvious next thing to want. It is recorded in `docs/qmux/pending-work.md`
rather than here, because the accept side belongs there — this crate takes a byte stream that
is already established and has no opinion about where it came from — and because the shape is
not settled: `ngnet-axum`'s `Listener` seam produces transports it drives with `ngnet-h2`, so
implementing one for QMux would serve HTTP/2, not this.

*The write-path work learned nothing about this.* It is a question about an accept seam and a
router, and nothing about record production, buffer arrangement or credit timing touches either.
