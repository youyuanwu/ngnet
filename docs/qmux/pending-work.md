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

Every inbound byte is copied twice inside this crate before a caller sees it. The framer
retains the payload of the record currently arriving (`src/io/framing.rs`), because it cannot
know whether that record carries a close until the record is complete and there is no way to
ask dwnx afterwards. And each delivery is copied out of the record dwnx is parsing into an
owned `Vec` for `Event::StreamData` (`src/io/event.rs`), because the handler's borrow points
into that record buffer and is valid only for the duration of the callback.

Neither copy is a choice. The first exists because dwnx exposes no accessor for a parsed close;
give it one and the retention goes entirely. The second exists because handlers receive event
values and cannot reach the connection — which is the deliberate design recorded in
`design.md` — so the alternative is invoking a caller-supplied callback from inside the
handler, which is what the sans-I/O API already offers and what the layer exists to be an
alternative to.

The HTTP/3 join adds none inbound. `ngnet-qmux-h3` turns that owned `Vec` into `Bytes` by
taking the allocation over rather than copying it, and its outbound direction copies into the
record being built exactly once — which is what `RETAINS_BUFFERS = false` means there.

Outbound within this crate there is one more: a record is serialised into a scratch buffer and
then copied into the outbound queue. That one is an artefact of `RecordWriter` borrowing both
the connection and its destination buffer for the whole record, which is the borrow that makes
the write path sound at all (see `design.md`), and it is bounded by one record.

These copies have still not been costed. The benchmark suite in `docs/benchmarks/` now runs
`ngnet-qmux-h3` end to end over this layer, so the sentence that used to stand here — that
there were no benchmarks — is no longer true; but an end-to-end figure attributes nothing to
these copies in particular, since it also contains framing, QPACK, the record layer, the pump
and the byte stream. All of the above therefore remains a description rather than a number,
and it will stay one until something profiles the copies or measures a build with one of them
removed.

## Things this increment does not do

Deliberate omissions, each excluded to keep the work reviewable rather than because it is
unwanted.

| Gap | What it would need |
| --- | --- |
| **Establishing a byte stream** | The layer takes one already connected. There is no listener, no accept loop, no dialling and no TLS seam, and a test asserts the crate offers no way to make one. That is the scope decision the absence of an endpoint rests on — the operating system hands out one stream per peer, so a QMux "endpoint" is a TCP or unix listener the caller already has. What would settle it: nothing here; a caller who wants one uses `tokio::net` or its equivalent and hands the result over. |
| **Serving an axum `Router` over QMux** | `ngnet-axum` serves a `Router` without hyper and is generic over a `Listener`, so an axum application over a QMux-carried connection is the obvious thing to want next. The fit is not immediate and that is the open question rather than the wiring: `Listener::Io` is bounded by `ServableTransport`, which `ngnet-axum` drives with `ngnet-h2` — so a QMux listener that produced accepted byte streams would get HTTP/2 over them, not HTTP/3 over QMux. What would settle it: deciding whether `ngnet-axum` grows an engine seam alongside its transport seam, or whether the join belongs in a crate of its own. |
| **Vectored writes** | `dwnx_conn_writev_stream` takes a `dwnx_vec` array and the wrapper only uses the single-slice `dwnx_conn_write_stream` form. A `RecordWriter::push_vectored` taking `&[IoSlice]` would avoid a copy for callers whose payload is already fragmented, which the HTTP/3 join often is: `StreamSource::write_next` may offer several slices at once, and `ngnet-qmux-h3` issues one write per slice and stops at the first that is not fully taken. |
| **Interoperability testing** | Everything is tested against dwnx itself, in memory and over a loopback socket. Nothing has been run against another QMux implementation, and no other one is known to exist yet. |
| **Benchmarks** | None. The interesting comparison is QMux-over-TLS-over-TCP against QUIC for the same workload, which is the draft's own motivating claim about computational cost. The layer and the HTTP/3 join make it measurable now; nothing has measured it. |
| **`dwnx_settings.log_write`** | Left as a null function pointer. Bridging it to a Rust logging closure is straightforward and would be this crate's only callback that is not a protocol event. Not needed to speak the protocol. |

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
