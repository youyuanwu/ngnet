# QMux pending work

Known gaps and deferred decisions, each with the evidence that produced it and what would
settle it.

Both crates are unpublished, and this page is the reason why. QMux is an unratified IETF draft
and dwnx has never been tagged; publishing would fix a public API that is expected to move
with both.

## Gaps in the vendored library

Found while writing the binding. None is a defect in this crate, and each is something a
future dwnx could close — at which point the compensation here becomes removable, which is why
they are recorded rather than merely worked around.

| Gap | What it costs, and what would settle it |
| --- | --- |
| **No way to serialise a connection close** | dwnx parses an incoming CONNECTION_CLOSE and reports it as `DWNX_ERR_DRAINING`, and it exposes `dwnx_ccerr` and its constructors for *describing* a close. There is no `dwnx_conn_write_connection_close` or equivalent, so this crate can observe a peer's shutdown but cannot initiate one. `CloseReason` exists and is tested, so an upstream writer would be additive: a `Conn::close` forwarding to it. Until then `tests/events.rs` builds a CONNECTION_CLOSE record by hand to test the receiving side. |
| **No getter for the peer's transport parameters** | `dwnx_conn_get_local_transport_params` returns the *local* set. The peer's arrive only through the `recv_transport_params` callback, so the bridge copies them into connection-owned storage and `Conn::peer_transport_params` reads that cache. A real getter upstream would let the accessor forward instead of caching. |
| **Two error codes are defined but never produced** | `DWNX_ERR_CLOSING` and `DWNX_ERR_IDLE_CLOSE` appear in the header and in `dwnx_strerror`'s table, but no operation returns either. `DWNX_ERR_DRAINING` is returned and is surfaced as `ReadOutcome::PeerClosed`. The other two are classified by the error mapping — the exhaustiveness check requires it — but have no behavioural test, because there is no way to reach them. |
| **`dwnx_conn_read`'s error contract is documented as "TBD"** | Literally. The conditions it can return were derived from `dwnx_conn.c` rather than from the header. That is why the error mapping's exhaustiveness check scans the header for constants instead of trusting prose, and why the fatality classification is this crate's own rather than a restatement of upstream's. |
| **Constructor preconditions are assertions, not error returns** | A transport parameter out of range aborts the process instead of failing. `TransportParams::validate` mirrors the same checks in Rust so the abort is unreachable; if dwnx converted them to error returns, that validation could shrink to a pass-through. It also checks `initial_max_stream_data_uni`, which the constructor does not assert although its siblings are — an upstream oversight, caught later during frame encoding. |
| **The constructor returns `NOBUF` where it documents `NOMEM`** | On allocation failure `dwnx_conn_server_new` returns `DWNX_ERR_NOBUF`, while the header's error list says `DWNX_ERR_NOMEM`. The wrapper maps what the code does. Harmless, and worth fixing upstream. |
| **`max_record_size` is configurable but not honoured** | dwnx overwrites it with `DWNX_DEFAULT_MAX_RECORD_SIZE` at construction, with the comment "We do not let application increase max record size". The parameter is validated because the assertion runs before the overwrite, and readback reports the library's value. If upstream ever honours it, nothing here needs to change except the documentation saying it does not. |

## Things this increment does not do

Deliberate omissions, each excluded to keep the work reviewable rather than because it is
unwanted.

| Gap | What it would need |
| --- | --- |
| **An endpoint layer** | `ngnet-quic` grew one: a driver owning a socket, an accept loop, and a `Connection` handle over it. QMux would need less, because there is no path validation, no retry, no stateless reset and no connection-id routing — the "endpoint" for a QMux connection is a TCP listener, and the driver's job is a read loop and a write loop rather than a demultiplexer. What would settle it: the same shape as `ngnet-quic`'s `endpoint` module with the QUIC-specific machinery removed. |
| **A runtime integration** | Nothing here touches tokio, compio or any executor. Because QMux runs over a byte stream, the seam is `AsyncRead + AsyncWrite` rather than the `AsyncUdpSocket` trait the QUIC side had to define — a much smaller abstraction, and one the ecosystem already has several spellings of. Choosing between them is the open question, not the wiring. |
| **An application-protocol mapping** | There is no analogue of `ngnet-quic-h3`. The draft's stated motivation is running HTTP/3 and WebTransport over TCP, so an `ngnet-h3`-over-QMux adapter is the obvious next crate: `ngnet-h3` already defines a `QuicConnection` trait with three implementations, and QMux's stream semantics are QUIC's. What it needs is the endpoint layer above, since that trait is asynchronous. |
| **Interoperability testing** | Everything is tested against dwnx itself, in memory. Nothing has been run against another QMux implementation, and no other one is known to exist yet. |
| **Benchmarks** | None. The interesting comparison is QMux-over-TLS-over-TCP against QUIC for the same workload, which is the draft's own motivating claim about computational cost — and it needs the endpoint layer before it can be measured. |
| **`dwnx_settings.log_write`** | Left as a null function pointer. Bridging it to a Rust logging closure is straightforward and would be this crate's only callback that is not a protocol event. Not needed to speak the protocol. |
| **Vectored writes** | `dwnx_conn_writev_stream` takes a `dwnx_vec` array and the wrapper only uses the single-slice `dwnx_conn_write_stream` form. A `RecordWriter::push_vectored` taking `&[IoSlice]` would avoid a copy for callers whose payload is already fragmented. |

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
