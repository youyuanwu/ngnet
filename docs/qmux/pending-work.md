# QMux pending work

Known gaps and deferred decisions, each with the evidence that produced it and what would
settle it.

Every QMux crate here is unpublished, and this page is the reason why. QMux is an unratified
IETF draft and dwnx has never been tagged; publishing would fix a public API that is expected
to move with both. This page covers `ngnet-qmux` and `ngnet-qmux-sys`; the HTTP/3 join keeps
its own at `docs/qmux-h3/pending-work.md`.

## Gaps in the vendored library

Found while writing the binding and, for four of them, while writing the asynchronous layer
over it. None is a defect in this crate, and each is something a future dwnx could close — at
which point the compensation here becomes removable, which is why they are recorded rather than
merely worked around.

| Gap | What it costs, and what would settle it |
| --- | --- |
| **No way to serialise a connection close** | dwnx parses an incoming CONNECTION_CLOSE and reports it as `DWNX_ERR_DRAINING`, and it exposes `dwnx_ccerr` and its constructors for *describing* a close. There is no `dwnx_conn_write_connection_close` or equivalent, so the state machine can observe a peer's shutdown but cannot initiate one. The asynchronous layer therefore ships its own encoder in `src/io/close.rs`, and `tests/events.rs` builds a CONNECTION_CLOSE record by hand to test the receiving side. An upstream writer would make both removable: `Conn::close` would forward to it, and the layer would call that instead. |
| **No accessor for a close the peer sent** | The parse happens — dwnx reads the kind, the error code, the frame type and the reason into a frame struct — and then the struct is private and the caller is handed `DWNX_ERR_DRAINING` with nothing attached. So this crate cannot say *why* the peer closed without decoding the record itself, which is half of why the layer retains inbound records at all. What would settle it: a getter returning the parsed `dwnx_ccerr`, at which point `src/io/close.rs`'s decoder and the framer's retention both go. |
| **No way to ask where a record ends** | `dwnx_conn_read` returns `0` for "that was fine, feed me more" whether it stopped between records or halfway through a length prefix, the record reader's state has no accessor, and the reader is private. A byte stream cut off mid-record is therefore indistinguishable from one that ended tidily, and reporting the second where the first happened is an incomplete transfer with no symptom. `src/io/framing.rs` reads the length prefixes itself so the layer always knows which just happened. What would settle it: any accessor reporting whether the reader stands at a boundary — one predicate would do. |
| **No callback for the connection-level window** | dwnx invokes `extend_max_stream_data` when a peer raises a *stream's* window, and invokes nothing when a MAX_DATA frame raises the connection's: it updates `tx.max_offset` and tells nobody (`deps/dwnx/lib/dwnx_conn.c:1045-1056`). A write parked on an exhausted connection window therefore has no event to wait for. The layer samples `Conn::max_data_left` either side of every `Conn::read` and wakes the parked writer when the figure moves — correct, and a poll where a callback would do. Waking on any inbound bytes instead would have spun a blocked writer once per arriving record for as long as the peer kept talking. What would settle it: an `extend_max_data` callback beside the one that already exists per stream. |
| **No getter for the peer's transport parameters** | `dwnx_conn_get_local_transport_params` returns the *local* set. The peer's arrive only through the `recv_transport_params` callback, so the bridge copies them into connection-owned storage and `Conn::peer_transport_params` reads that cache. A real getter upstream would let the accessor forward instead of caching. |
| **Two error codes are defined but never produced** | `DWNX_ERR_CLOSING` and `DWNX_ERR_IDLE_CLOSE` appear in the header and in `dwnx_strerror`'s table, but no operation returns either. `DWNX_ERR_DRAINING` is returned and is surfaced as `ReadOutcome::PeerClosed`. The other two are classified by the error mapping — the exhaustiveness check requires it — but have no behavioural test, because there is no way to reach them. |
| **`dwnx_conn_read`'s error contract is documented as "TBD"** | Literally. The conditions it can return were derived from `dwnx_conn.c` rather than from the header. That is why the error mapping's exhaustiveness check scans the header for constants instead of trusting prose, and why the fatality classification is this crate's own rather than a restatement of upstream's. |
| **Constructor preconditions are assertions, not error returns** | A transport parameter out of range aborts the process instead of failing. `TransportParams::validate` mirrors the same checks in Rust so the abort is unreachable; if dwnx converted them to error returns, that validation could shrink to a pass-through. It also checks `initial_max_stream_data_uni`, which the constructor does not assert although its siblings are — an upstream oversight, caught later during frame encoding. |
| **The constructor returns `NOBUF` where it documents `NOMEM`** | On allocation failure `dwnx_conn_server_new` returns `DWNX_ERR_NOBUF`, while the header's error list says `DWNX_ERR_NOMEM`. The wrapper maps what the code does. Harmless, and worth fixing upstream. |
| **No timer, and an idle timeout that is advertised but not kept** | dwnx validates `max_idle_timeout`, puts it in the transport parameters, and then never acts on it: there is no expiry API, no "when should I next be called" accessor, and nothing that fires. So neither this crate nor the layer above it enforces one, and the layer's `Clock` reports the time and offers no way to wait for one — a `sleep_until` would imply an enforcement nobody performs. `Config`'s default is zero, which is the honest advertisement: no deadline, rather than a deadline nobody is keeping. What would settle it: an expiry accessor upstream, of the shape ngtcp2 has, at which point the clock grows a wait and the layer arms it. |
| **`max_record_size` is configurable but not honoured** | dwnx overwrites it with `DWNX_DEFAULT_MAX_RECORD_SIZE` at construction, with the comment "We do not let application increase max record size". The parameter is validated because the assertion runs before the overwrite, and readback reports the library's value. If upstream ever honours it, nothing here needs to change except the documentation saying it does not. |

## What those gaps cost, in copies

Recorded here rather than in `design.md` because it is a consequence of the table above rather
than a decision: close the gaps and the cost goes with them.

Inbound there were two copies. One is gone and one is still paid, and the gaps that caused both
are still open.

**The framer's retention is now paid only where it buys something.** It used to copy every
record's payload, because a record has to be complete before it can be scanned for a close and
dwnx offers no way to ask afterwards. A record whose bytes all arrive at once is now scanned
where it lies and copied only if it turns out to carry a close; a record split across reads
still accumulates, because there is nothing contiguous to scan. That is a narrowing rather than
a closing: give dwnx an accessor for a parsed close and the retention goes entirely, split
records included. `RecordFramer::copied_bytes` reports zero for a run of whole records, which is
an assertion in a test rather than a claim here.

**Each delivery is still copied** out of the record dwnx is parsing into an owned `Vec` for
`Event::StreamData` (`src/io/event.rs`), because the handler's borrow points into that record
buffer and is valid only for the duration of the callback. That copy is not a choice in the same
sense as the first: handlers receive event values and cannot reach the connection, which is the
deliberate design recorded in `design.md`, so the alternative is invoking a caller-supplied
callback from inside the handler — which is what the sans-I/O API already offers and what this
layer exists to be an alternative to.

**Removing it was tried, measured, and reverted.** A build in which deliveries were
reference-counted views into a pooled read buffer cut per-delivery allocation from 8,216 bytes
to 24, and was *slower* on every payload size but one. The run is
[`05-qmux-delivery-aliasing`](../benchmarks/data/xeon-8370c-azure/05-qmux-delivery-aliasing.md).
The short version is that the pool's bookkeeping,
the reference count and the copy-out that a small delivery still needs together cost more than
the allocation they removed, which is a result worth having rather than a failure: it says this
copy is not where the time goes, and the next person to look at it can start somewhere else.

The HTTP/3 join adds none inbound. `ngnet-qmux-h3` turns that owned `Vec` into `Bytes` by
taking the allocation over rather than copying it, and its outbound direction copies into the
record being built exactly once — which is what `RETAINS_BUFFERS = false` means there.

Outbound within this crate there is now none. A record used to be serialised into a scratch
buffer and then copied into the outbound queue — one memcpy of up to 16382 bytes per record,
about a megabyte of copying per megabyte sent — and it is not any more: `Conn::record` is handed
a slice of the outbound buffer's own tail, so the bytes dwnx writes are already where the byte
stream will be offered them. `RecordWriter` still borrows the connection and its destination for
the whole record, which is the borrow that makes the write path sound at all (see `design.md`);
what changed is which buffer the destination is a piece of. A test asserts the count rather than
the prose: `Connection::copied_record_bytes` reports zero for a transfer of any size, and the one
thing it still counts is the encoded connection close, which is a few dozen bytes once per
connection and exists only because dwnx has no writer for a close — the first gap in the table
above.

The removal was measured, not assumed: on this machine, a client sending a megabyte over a byte
stream that accepts everything went from 1,049,226 bytes copied to 0, and construction went from
15 allocations and 33,228 bytes to 14 and 16,846 — the scratch buffer was the difference. What
it is *worth* in time is a separate question, and nothing here answers it: these are counts,
identical on every machine, and any statement about timing has to come from a recorded run under
`docs/benchmarks/`.

The two inbound copies have still not been costed. The benchmark suite in `docs/benchmarks/` now
runs `ngnet-qmux-h3` end to end over this layer, so the sentence that used to stand here — that
there were no benchmarks — is no longer true; but an end-to-end figure attributes nothing to
these copies in particular, since it also contains framing, QPACK, the record layer, the pump
and the byte stream. They therefore remain descriptions rather than numbers, and will stay so
until something profiles them or measures a build with one of them removed. The outbound one
is the exception, and only because it is gone: its count is asserted rather than estimated.

## Things this increment does not do

Deliberate omissions, each excluded to keep the work reviewable rather than because it is
unwanted.

| Gap | What it would need |
| --- | --- |
| **Establishing a byte stream** | The layer takes one already connected. There is no listener, no accept loop, no dialling and no TLS seam, and a test asserts the crate offers no way to make one. That is the scope decision the absence of an endpoint rests on — the operating system hands out one stream per peer, so a QMux "endpoint" is a TCP or unix listener the caller already has. What would settle it: nothing here; a caller who wants one uses `tokio::net` or its equivalent and hands the result over. |
| **Serving an axum `Router` over QMux** | `ngnet-axum` serves a `Router` without hyper and is generic over a `Listener`, so an axum application over a QMux-carried connection is the obvious thing to want next. The fit is not immediate and that is the open question rather than the wiring: `Listener::Io` is bounded by `ServableTransport`, which `ngnet-axum` drives with `ngnet-h2` — so a QMux listener that produced accepted byte streams would get HTTP/2 over them, not HTTP/3 over QMux. What would settle it: deciding whether `ngnet-axum` grows an engine seam alongside its transport seam, or whether the join belongs in a crate of its own. |
| **Vectored writes** | *No longer a gap; the row is kept because the answer is worth more than its absence.* Two different things go by that name here, and they were settled differently. **Settled by building it: `RecordWriter::push_vectored`.** `dwnx_conn_writev_stream` takes a `dwnx_vec` array, and the wrapper now uses it: `RecordWriter::push_vectored` submits a caller's fragments as one array, and `Connection::try_write_stream_vectored` packs them into as few records as the maximum record size permits. The HTTP/3 join no longer issues a write per slice, so a record boundary no longer falls at every slice boundary. What it was worth is a count, not a time — one record and eight bytes of framing per request with a body — and why it is a count is recorded in `tests/ngnet-qmux-h3-tests/tests/fragmented_offers.rs`. The resumption rule is the part to be careful with and it is documented where it lives (`Fragments` in `src/io/conn.rs`): `*pdatalen` is one total across every vector, not a count per vector, so resuming means walking the array against a byte count and a short take routinely stops part-way through a fragment. **Settled by asking: gathering this connection's output to its byte stream.** The question was whether the output can ever be presented to the byte stream as more than one region, so that a stream able to write several buffers in one operation could be handed them. It cannot, so no gathering capability exists on `AsyncByteStream` — it has one `poll_write` taking one `&[u8]`, that is deliberate, and no test claims otherwise. See *Gathered output was asked about, and the answer is no* below for the two reasons and what would change them. |
| **Interoperability testing** | Everything is tested against dwnx itself, in memory and over a loopback socket. Nothing has been run against another QMux implementation, and no other one is known to exist yet. |
| **Benchmarks** | None. The interesting comparison is QMux-over-TLS-over-TCP against QUIC for the same workload, which is the draft's own motivating claim about computational cost. The layer and the HTTP/3 join make it measurable now; nothing has measured it. |
| **`dwnx_settings.log_write`** | Left as a null function pointer. Bridging it to a Rust logging closure is straightforward and would be this crate's only callback that is not a protocol event. Not needed to speak the protocol. |

## Gathered output was asked about, and the answer is no

This is the recorded answer to a question that will otherwise be asked again: **can this
connection's output ever be presented to its byte stream as more than one region?** It cannot,
and there are two independent reasons. Either one alone would settle it; both hold, which is why
no gathering capability was added rather than added and left unused. Going in, the expectation
was that the answer would be negative — the bias is stated so that what follows can be read as
the case *against* one's own conclusion having been looked for, which it was: both halves were
checked against the code and against dwnx, and the checks are named below so that a later reader
can redo them rather than trust them.

**First: there is only ever one region.** Write coalescing chose to *stop early* rather than to
compact or to wrap. Production appends at `filled` and stops when the tail cannot take another
whole record (`Connection::room_for_record`); a partial accept advances `written` and leaves the
space in front of it unreclaimed; `flush` offers the byte stream exactly `outbound[written..filled]`
and resets both cursors when it drains. Nothing wraps, nothing compacts, and nothing else writes
into the buffer. The output is therefore the single region `[written..filled]` in every reachable
state, and a second region could only be introduced by adopting a ring buffer — that is, by
building the two-region state in order to have something to gather. `design.md` rejects the ring
on its own grounds, and this is the second reason not to want it.

**Second: gathering would not save a copy here, which is where the HTTP/2 answer does not
transfer.** Gathering pays for `ngnet-h2` because the regions it gathers include *caller-owned
payload that the session never copies* — that is what `NGHTTP2_DATA_FLAG_NO_COPY` buys, and
`docs/benchmarks/findings/write-path-and-gathering.md` measures what it is worth: a block at or
above the threshold goes out as the second region of a two-region `writev`, never copied. QMux
has no equivalent to buy. dwnx frames a payload *inside* the record: `dwnx_conn_write_stream`
reaches `dwnx_conn_write_stream_frame`, which hands the caller's vectors to the frame encoder,
and the encoder copies them into the record buffer (`deps/dwnx/lib/dwnx_frame.c:157`). The
payload is in the record before the record is anything the byte stream could be shown, so the
only copy gathering could avoid is one the record layer has already made and one that coalescing
cannot avoid either. What coalescing *did* avoid — the staging copy between a scratch buffer and
the outbound queue — is gone already, and its removal is counted above; gathering has nothing
left to remove.

**What would change this.** A ring buffer for the outbound queue, adopted for its own reasons
rather than for gathering's, would produce a genuine two-region state and this answer would have
to be retaken from the first half only, since the second half would still stand and would still
say the gathering saves no copy. A dwnx that could write a record header describing payload it
does not own — the shape `NGHTTP2_DATA_FLAG_NO_COPY` has, and one this crate cannot add from
outside `deps/dwnx` — would retake the second half. Short of one of those two, the answer is
settled, and the absence of `poll_write_vectored` on `AsyncByteStream` is a decision with a
reason rather than an omission: a declaration that nothing could act on would be a surface
without a behaviour.


## Deferred design decisions

**A limited callback context.** Handlers currently receive event values and no way to reach
the connection, which is what makes dwnx's callback-time restriction unrepresentable rather
than merely documented — see `design.md`. The cost is that a handler cannot act in place: it
cannot extend a window on observing data, or open a stream in response to one closing, so the
caller records what happened and acts after the entry point returns. If that proves awkward in
practice, the way out is a context object exposing only the operations dwnx permits during a
callback, with the write excluded. It is not built speculatively, because the simple form has
not yet been shown to be insufficient.

**Panic behaviour.** A panic in a handler aborts the process, matching `ngnet-quic`'s
documented convention. Containment is genuinely possible here in a way it is not there —
every dwnx callback returns an `int`, so a caught panic could become
`DWNX_ERR_CALLBACK_FAILURE` and be resumed once control returns to Rust. It is not done,
because having the two crates in this workspace disagree about what a handler panic means
would be worse than either choice on its own. If it changes, both should change together.

**Tracking upstream.** The submodule is pinned at the commit vendored with these crates.
There is no policy yet for following dwnx, and the exhaustiveness checks in
`invariants.md` are what will make a bump's consequences visible when there is one.
