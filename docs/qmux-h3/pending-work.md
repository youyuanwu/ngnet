# HTTP/3 over QMux: pending work

What is missing, and what would settle each.

## Interoperability is proven against nothing

Everything here runs against this workspace's own stack: `ngnet-h3` over `ngnet-qmux` over an
in-memory byte-stream pair or a loopback TCP socket. Both ends of every test are this code.

That is a weaker position than the QUIC join is in, which at least runs against quinn. QMux is
an unratified draft and no other implementation is known to exist, so there is currently
nothing to interoperate *with* — the gap is real and it is not closable by effort here.

**What would settle it:** a second QMux implementation appearing, or dwnx's own example client
and server being driven against this stack. The latter is possible today and has not been done.

## There is no structural test suite

`ngnet-quic-h3` ships `tests/invariants.rs`, which reads its own source and asserts that
nothing here names a socket or a thread, that module files are flat, that nothing is
`include!`d, and that the manifest declares exactly what it should. This crate ships no
equivalent, so the claims on `invariants.md` that would belong to such a suite are marked there
as resting on the compiler or on review instead.

**What would settle it:** the same suite, with the forbidden names adjusted — `TcpStream` is a
plausible thing for a QMux-adjacent crate to acquire by accident in a way it is not for a QUIC
one, so the list is not a copy.

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

## The join hangs at high concurrency on a multi-worker runtime

With sixty-four requests issued together on one connection and a tokio runtime with more than
one worker thread, the join wedges: **roughly three attempts in four never complete**, at both
two and four workers, typically after about fifty-five of the sixty-four requests have
finished. The remaining futures never resolve, no error is produced, and nothing closes.

What narrows it:

- Concurrency 1 and 8 complete on every runtime tried.
- A `current_thread` runtime completes at every concurrency tried, including 64.
- It reproduces over an in-memory byte-stream pair, so it needs no socket and no kernel.
- Loopback TCP is clean throughout, so it is not transport-specific.
- It persists with the flow-control windows and the stream allowance raised far out of the
  way, so it is neither credit exhaustion nor the budget exhaustion recorded above.

That combination points at the pump's wakeup handling rather than at protocol state: something
that is a lost wakeup when two threads race and is not reachable when the same work is
serialised on one. It has not been narrowed further, and nothing here has been changed to
address it.

**This is why one benchmark group has no QMux arm.** The suite's
`concurrent_throughput_multi_thread` group runs the same sweep as its single-threaded sibling
on a four-worker runtime; the QMux arm was written, and it was left out because it hangs. Its
intermittence is what makes that necessary rather than merely tidy — an arm that failed every
time would be obvious, whereas one that hangs three times in four is a CI job that
occasionally never returns, and `cargo bench -- --test` has no timeout that would turn that
into a failure. `docs/benchmarks/cases/concurrent-throughput.md` records the omission and
points here. Every other group in the suite carries a QMux arm except the two shared-body
groups, whose absence has an unrelated cause and must not be filed with this one.

**What would settle it:** a reduced reproduction — the smallest number of concurrent streams
and worker threads that still hangs — and then the wakeup path under it. A timeout-guarded
test at concurrency 64 on a multi-worker runtime is the regression test, and it would fail
today, which is why it is described here rather than committed.

## The connection is not observable

There is no way to ask a live connection how much read-ahead it is holding, what its peer's
transport parameters said, or how many streams are open. The layer below exposes some of that
on its own `Connection`, and this crate takes ownership of that value at construction, so a
caller holding a `QmuxConnection` cannot reach it.

**What would settle it:** deciding whether accessors belong on this crate's type. The QUIC join
has the same gap for the same reason, and both should be answered together.

## Body bytes are copied twice before this crate sees them

The heading records what this cost when it was written. Neither copy is paid on the ordinary
path any more, and the heading stays because the gaps that caused them have not moved.

**The framer's copy.** The QMux layer used to copy every record's payload into the framer's
retention so that a completed record could be scanned for a connection close. A record that
arrives whole is now scanned where it lies; what is still copied is a record spread over several
reads, which has nothing contiguous to scan, and the one close a connection latches.

**The delivery copy.** Each delivery used to be copied into an owned `Vec` for its event, which
this crate turned into `Bytes` by taking the allocation over — so it added nothing then and adds
nothing now. A delivery is now a refcounted view of the QMux read buffer, and this crate carries
it whole into `Bytes::from_owner` rather than taking an allocation over. Both forms are a move
or a refcount bump; neither is a memcpy, which is the property this crate has to keep and the
reason the conversion is a named function with the reasoning beside it rather than a
`Bytes::copy_from_slice` that would compile and be correct and be wrong.

Outbound, `StreamSource::write_next` lends buffers that are invalid once the closure returns, so
the bytes are copied into the record being built — one copy, and the reason `RETAINS_BUFFERS` is
`false`.

Both inbound copies were consequences of gaps in dwnx rather than of anything decided here; see
`docs/qmux/pending-work.md`, which records what would remove each and what each still costs.

**What would settle it, for the time rather than the count:** the counts are now measured —
`RecordFramer::copied_bytes` for the first and the allocation harness in
`tests/ngnet-qmux-h3-tests/tests/allocations.rs` for the second — and both report what their
removal claims. What is still missing is what that is worth in time. The suite runs this stack
end to end — `docs/benchmarks/` — but an end-to-end figure cannot separate two memcpys from
framing, QPACK, the record layer and the pump. A profile of the 1 MiB body point is what would
turn this into a number.

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

**No timing was taken, and that is a decision.** Spec FR-038 requires the benchmark identifiers
an effect could appear on to be named before it is measured, and FR-021 accepts a mechanism
established by a count where no identifier can resolve it. The Phase 2 screen put this saving at
about one record per request with a body — one write in two at 1 KiB and one in sixty-six at
1 MiB — against a run-to-run drift of 0.5% to 5% on the arms that could show it. A timed
comparison would therefore report a number well inside its own noise and would support no
conclusion in either direction, so none was taken. `docs/benchmarks/allocation-counts.md` is the
same treatment applied to the HTTP/2 stack: a mechanism established by a count, with the absence
of a time stated rather than left to be noticed.

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

## Something scales with in-flight streams on a real socket

This is a lead, not a finding, and the numbers behind it are **not measurements**: they come
from unpinned, short-sample exploratory runs taken while the benchmark arms were being built,
on a shared virtual machine, with no drift controls and no replication. They are not filed
under `docs/benchmarks/data/` and must not be quoted as results. What they are good for is
saying which point is worth measuring properly.

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
sixty-four concurrent streams in a way that is not a coincidence worth ignoring.

**What would settle it:** a pinned, replicated run of
`transport_concurrent_throughput` and `concurrent_throughput` across the full 1/8/64 sweep with
drift controls, recorded under `docs/benchmarks/data/` as a run — followed, if the shape holds,
by a syscall count per pass for the QMux arm at each concurrency. If the count grows with `N`
where the HTTP/2 arm's does not, the vectored-write entry above was the obvious fix and has
already been applied, so a count that still grows would place the cause elsewhere.

## The transmit pass yields on a fixed count

A pass takes at most sixty-four offers and then returns, so a layer with an endless supply
cannot keep it from returning to the driver. Sixty-four accepted offers used to be on the order
of a megabyte, which was a guess rather than a measurement: too low costs wakeups on a large
body, too high delays the events the driver has to attend to. Multi-record production loosened
that reading — an offer is now worth as many records as the outbound buffer will hold, so the
cap bounds offers rather than bytes and what bounds the bytes is the buffer's ceiling. The
constant is unchanged, and the trade it encodes is now less about bytes than about how many
different streams a pass will visit.

**What would settle it:** a benchmark showing which end of that trade actually costs anything.
The suite added in `docs/benchmarks/` does not, because it holds the count fixed at sixty-four
in every arm; what it would take is the same sweep run against two builds differing only in
that constant.

## No datagrams, no WebTransport, no priority

Neither `ngnet-h3` nor `ngnet-qmux` exposes unreliable datagrams, so this crate cannot.
WebTransport is the QMux draft's other stated motivation and is not implemented anywhere in
this workspace. The transport trait has no priority concept, so nghttp3's support for the
HTTP/3 priority scheme is unreachable from here.

See both families' pending-work documents.

## Nothing serves an axum router over this

`ngnet-axum` serves an axum `Router` without hyper, and doing the same over a QMux-carried
connection is the obvious next thing to want. It is recorded in `docs/qmux/pending-work.md`
rather than here, because the accept side belongs there — this crate takes a byte stream that
is already established and has no opinion about where it came from — and because the shape is
not settled: `ngnet-axum`'s `Listener` seam produces transports it drives with `ngnet-h2`, so
implementing one for QMux would serve HTTP/2, not this.
