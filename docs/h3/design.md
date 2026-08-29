# HTTP/3 design

Why the HTTP/3 crates are shaped the way they are. Behaviour is documented with the code;
this records the decisions, and in particular the places where nghttp3 differs from nghttp2
in ways that are easy to miss.

## Layering

```
ngnet-h3-sys      raw FFI; builds packaged vendor/nghttp3 source
   ↑
ngnet-h3          sans-I/O connection state machine
   ↑              + src/http/, an async API behind the default-on `http` feature
   ↑
ngnet-h3-quinn    reusable async transport adapter for an established quinn connection
   ↑
ngnet-h3-tests    unpublished; drives the core and adapter over real quinn connections
```

`ngnet-h3` performs no I/O. It opens no socket, blocks nowhere, creates no threads and reads
no clock: the caller owns the QUIC connection, opens the streams, declares which of them
carry control and QPACK data, and moves bytes in and out. `tests/invariants.rs` enforces
that structurally — the crate may not so much as *name* `std::net`, `std::fs`, `std::thread`,
`std::time`, `std::process` or `std::env`, and the only `std::io` item it may name is
`IoSlice`, which describes borrowed bytes rather than offering a way to move them.

That boundary is inherited rather than invented: nghttp3 depends on no QUIC transport and on
no TLS library, so neither does this.

The async layer at `src/http/` sits above that boundary and inherits none of it: it is
allowed to be asynchronous, which is the point of it, and the structural tests are scoped
accordingly — the core's scan runs over everything *outside* `src/http/`, and the subtree
makes its own narrower promises instead. Disabling the `http` feature returns the crate to
exactly what the paragraph above describes, with one dependency and no asynchrony at all.

Server push is absent because nghttp3 does not implement it.

## Shared work is transferred once per driver pass

Handles, bodies, and nghttp3 callbacks queue five kinds of driver work: ready streams, caller
resets, returned credit, transport actions, and graceful shutdown. They already live under one
mutex because they are consumed at the same pass boundary. The driver transfers all five into
reused driver-owned scratch with one acquisition, releases the lock, then processes them in the
established credit, action, reset, ready, shutdown order. No shared lock spans a call into
nghttp3, a transport, or a body, and no waker is invoked while the lock is held.

The transfer is a scheduling and ownership boundary. Work queued after it belongs to the next
pass, so processing an early category cannot pull a later category across the boundary. If a
fatal operation fails, the transferred batch dies with the driver and is not replayed: replaying
the unprocessed tail could repeat a side effect already applied before the failure.
Progress without an external wake is guaranteed at the idle and normal-completion decisions.
If the pass is already suspended on stream capacity or transmit backpressure, deferred work waits
for that transport suspension to end, as it did before this change.

Idle and normal-completion decisions inspect all five categories coherently. At the actual park,
the driver installs its waker and repeats the readiness check while holding the same lock. Work
queued before registration is seen by the recheck; work queued afterwards wakes the installed
waker. The lock is dropped before that waker is invoked. A writability wake remains sufficient
to retry blocked transport streams even when it carries no event.

The four driver-owned scratch vectors reuse capacity across passes. Scratch capacity above 2,048
is shrunk toward 1,024 after processing; ordinary capacity at or below the high-water mark does
not churn its allocation.

## Five differences from nghttp2 that are load-bearing

Each of these compiles fine if you assume the nghttp2 shape, and each is silently wrong.

**1. There is no `nghttp3_conn_set_user_data`.** `ngnet-h2` builds a bridge on the stack for
each call and installs a pointer to it with `nghttp2_session_set_user_data`, which is what
lets callbacks reach borrows that exist only for that call. nghttp3 accepts connection user
data only at construction, and the five `nghttp3_conn_set_*` functions it does export set
concurrency limits, stream user data and priority — nothing else.

So the pointer handed over at construction is not a bridge but a `BridgeSlot`: a stable,
separately heap-allocated cell that a bridge pointer is written into for the duration of each
call and cleared from on the way out, including while unwinding. It must be its own
allocation rather than a field of `Conn`, because `Conn` is `Send` and will be moved — a
pointer into a field would dangle at the first move, and nothing would report it.

**2. `nghttp3_mem` is stored by pointer, not copied.** nghttp2 copies the allocator struct
into the session; nghttp3 keeps the pointer for the connection's whole life, including inside
`nghttp3_conn_del`. A stack local would dangle. `Allocator` therefore owns the struct and its
state in one box. Settings and callbacks *are* copied by value — the asymmetry is the trap.

**3. `read_stream2` returns flow-control credit, not bytes consumed.** Every supplied byte is
always consumed; there is never a remainder to re-present, and re-presenting one duplicates
body data. What comes back is how much QUIC flow control may now be extended. It is modelled
as `FlowCredit` rather than a bare integer precisely so it cannot be mistaken for a count.

**4. Received fields arrive as reference-counted buffers.** `recv_header` hands over an
`nghttp3_rcbuf*` plus a QPACK static-table token, where nghttp2's equivalent gives raw
slices. The reference count is deliberately *not* incremented: nghttp3 decrements it only
after the callback returns, so borrowing for the call is both sufficient and
allocation-free. `recv_data` really is a pointer and a length, so the two cannot share a
conversion.

**5. Sending is a two-phase transaction.** `writev_stream` fills an array of vectors that
borrow both nghttp3's serialisation buffers and the application's body buffers; the caller
must then report how many bytes the transport accepted, and the next call invalidates the
vectors. That is a borrow spanning two FFI calls with library mutation in between.
`SendGuard` holds the connection for the whole transaction and `commit` consumes it, so
using the bytes afterwards is a borrow-check error rather than a documented rule — pinned by
two `compile_fail` doctests.

## The retain contract

nghttp3 has no copying data source. A `BodySource` hands over `RetainedBytes`, nghttp3 queues
the raw pointers, and it reads through them on every later write until the peer acknowledges
the bytes. Three facts determine the accounting:

- **`acked_stream_data` reports a byte delta, not a cumulative offset**, and only for
  application-owned buffers. So the registry keeps one FIFO element per *non-empty vector
  actually handed over*, and drains it by subtraction. Keying by source buffer instead would
  release a buffer as soon as its first vector was acknowledged, or never — depending on
  which length was compared.
- **Acknowledgement is the only release trigger.** `nghttp3_stream_update_ack_offset` is
  reachable from `add_ack_offset` and nowhere else; `add_write_offset` does not touch it. A
  caller that reports bytes written but never bytes acknowledged retains every body buffer it
  ever sent, which is why `Conn::add_ack_offset` is documented as required rather than
  optional.
- **`delete_outq` frees only library-owned buffers** and deliberately leaves
  application-owned ones alone, so releasing on stream close and on `Conn` drop is mandatory,
  not defensive.

**Bounds-checking acknowledgement is a memory-safety requirement, not ergonomics.**
`nghttp3_stream_update_ack_offset` fires the acknowledgement callback for the front buffer
*before* it checks whether that buffer has been written yet, so reporting more than was
committed would release a buffer nghttp3 has not sent and still points at. `Conn` therefore
tracks committed and acknowledged offsets per stream and refuses anything past the former.
Those counters are pruned when a stream closes, along with its body — so acknowledgement
reported *after* a close is refused too. nghttp3 would accept it silently; accepting it here
would mean an over-report became silent the moment a stream closed.

**Rolling back a failed submission must undo only what that call attached.** A stream may
already carry an in-flight body, and the failures that reach that path — a stream already in
use, a connection that is closing — are recoverable, so nothing poisons. Dropping the
existing entry would free buffers nghttp3 has queued and reads through on every later write,
and the connection would carry on handing the caller freed memory. This was a real
use-after-free, found in final review with a working reproduction in safe Rust; the
regression tests are in `crates/ngnet-h3/tests/body.rs`.

## Three sources of flow-control credit

`read_stream` returns framing credit only. Body bytes are excluded — the caller credits those
once it has handled the chunks — and credit for a QPACK-blocked stream arrives later, through
`on_deferred_consume`. That third source is reported exactly once, so a connection that
neither registers the handler nor drains `take_deferred_credit` under-credits the peer
permanently and stalls by degrees, with no symptom until it stops. Holding it rather than
dropping it is what makes forgetting survivable.

## Two distinct backpressure mechanisms

Conflating them livelocks a send loop.

- **The body has nothing to give.** `BodyOutcome::Defer` becomes `NGHTTP3_ERR_WOULDBLOCK`;
  the stream is deferred until `resume_stream`.
- **The transport will not take more.** `block_stream` / `unblock_stream`. Without it, a
  stream whose window is exhausted stays at the top of the priority queue and is offered
  ahead of every other stream forever.

A deferral is also how a stream is abandoned, which is not backpressure at all and is the
one case where nothing later resumes it — see *A caller's body failure is not the state
machine's* below.

## Assertions are not error reports

nghttp3 states a great many preconditions with C `assert`, which aborts where it is compiled
in and checks nothing where it is not. The vendored build keeps assertions on in every
profile — nghttp3's own `CMakeLists.txt` strips `-DNDEBUG` back out of the release
configurations — so with the default build the failure mode is an abort; a caller building
against a stock nghttp3 gets the silent-corruption version instead. Neither is something a
safe API may hand to its caller, so every reachable one is checked first and surfaced as a
typed error. The ones worth knowing:

- Reading a stream the peer cannot have written to — which, without the assertion, parses the
  peer's bytes into this endpoint's own sending state, letting an endpoint accept its own
  SETTINGS as the peer's.
- Submitting trailers on a connection-level stream. nghttp3 registers control and QPACK
  streams in the same map, so its own "stream not found" net does not catch them.
- Both shutdown calls, which queue a frame onto the control stream and write through the
  pointer without checking it.
- Raising the GOAWAY identifier, which may only ever fall.
- `is_drained` and `set_max_client_streams_bidi`, both server-only.

## Poisoning

Two conditions, and only two, make a connection unusable: any negative return from
`read_stream` or `writev_stream` — whose documentation says continuing is undefined behaviour
— and any code nghttp3's own `is_fatal` predicate accepts. Everything else stays recoverable,
which is what keeps a second bind, or a submission onto a closing connection, from killing a
connection that is otherwise fine. Poisoning also drains the body registry, because a write
that failed partway can queue a prefix of a body's vectors and abandon the rest, leaving
acknowledgements that can never arrive.

Queries are refused too. nghttp3 draws no distinction between asking and doing after the read
or write path has failed, so `is_drained` and `is_stream_writable` are fallible for that
reason alone. `Error::is_fatal` reports whether the connection survived rather than whether
nghttp3 calls the code fatal — the same protocol error is recoverable off a submission and
unrecoverable off the read path, so the code alone cannot say.

## Decisions that cost a wrong attempt first

- **`close_stream` originally took an error code.** It mapped to nghttp3's single-code entry
  point, which marks both directions with that code — so a stream that merely finished was
  indistinguishable from one reset with `H3_NO_ERROR`, which is not even a code a completed
  stream carries. A caller could retry a request over an error that never happened. The clean
  case now takes no code, and `close_stream_with` carries the per-direction shape a QUIC
  layer actually has.
- **Deferred flow credit was dropped when no handler was registered.** The failure that
  produces is the worst kind: works in testing, under-credits a little on every
  QPACK-blocked stream, stalls in production with nothing to point at. Documentation was not
  enough; it is now held and drained through `take_deferred_credit`.
- **`FieldAction::Stop` did nothing.** Both variants returned zero and behaved identically.
  An action that performs no action is worse than none, because it reads like one. It cannot
  cancel the section — QPACK is stateful, so the remaining fields must still be parsed — but
  it can and now does stop them being handed over.
- **Two `compile_fail` doctests passed for the wrong reason for four commits.** A signature
  change made them fail on arity before reaching the borrow check they existed to prove. A
  test that cannot fail is worse than no test, because it also stops anyone looking.

## Constraints that shape contributions

- The crate declares **exactly one** non-optional dependency and no dev-dependencies. Test-only
  needs belong in `ngnet-h3-tests`. Both are pinned by tests.
- `unsafe` lives only in the modules `lib.rs` grants `#[allow(unsafe_code)]`, and the list is
  exactly the FFI boundary. Adding an allow is how the compiler check is silenced, so the
  list itself is asserted.
- Edition 2024, built with the toolchain in `rust-toolchain.toml`. No declared MSRV.
- A panic inside a caller-supplied handler unwinds into a C frame and aborts. This is the
  accepted contract, matching `ngnet-h2`: `catch_unwind` would have to invent a return value
  for a callback whose contract has no "the handler is broken" case.


## The asynchronous layer

Behind the default-on `http` feature, `src/http/` turns the state machine into an API a
caller reaches through `http::Request`, `http::Response` and `http_body::Body`. It exists
because the core, correct as it is, cannot be used without first writing a QUIC integration:
three unidirectional streams to open and bind, a two-phase write to drive, acknowledgement to
report, credit to extend, per-stream bookkeeping throughout. The crate documentation lists
thirteen such obligations and the layer discharges all of them.

It takes no executor, spawner or timer. Handlers are futures the driver polls, not tasks it
spawns, and a structural test scans the subtree for the facilities that would betray a
runtime having crept in.

### The QUIC boundary

`QuicConnection` abstracts an **established** connection. No endpoint, TLS configuration,
certificate or ALPN identifier appears anywhere in it, so none of those concerns reaches this
crate — which is the same boundary nghttp3 itself draws, one level up.

Its shape was checked against the published APIs of **quiche**, **s2n-quic**, **msquic** and
**ngtcp2** before it was written, and that survey changed it in four places. The design is
worth stating with its reasons, because every part of it looks arbitrary without them.

**Reads are one connection-level event stream.** `poll_event` yields the next thing that
happened on any stream. That is msquic's and ngtcp2's native shape — both are already
callback-demultiplexed — and it lets the driver hold no per-stream futures and spawn nothing.
A per-stream poll would have been quinn's shape and only quinn's.

**Writes are pulled by the transport, not pushed at it.** When a transport has room it calls
`StreamSource::write_next`, and the layer answers with the next stream nghttp3 wants to write.
The obvious alternative — `write(stream, bytes) -> accepted` — is incompatible with ngtcp2,
which is the QUIC library nghttp3 was co-designed with: it fills a *packet* and asks the
application for stream data as it goes, so a push-shaped adapter would have to queue and copy
every outgoing byte. That copy would defeat the retain contract, which is the entire reason a
release signal exists. Pulling costs the other three nothing; each becomes a loop.

It also makes the two-phase write contract structurally keepable. `Conn::writev_stream`
hands back a `SendGuard` that must be committed or abandoned, and under the pull shape the
guard is acquired, offered and disposed of inside a single function it cannot escape — so
there is no path, including `?` and early return, on which one leaks.

**Receive credit is explicit.** `extend_credit` was the clearest quinn fingerprint in the
first draft *by its absence*: quinn returns credit implicitly when a chunk is read, so
nothing looked missing, but ngtcp2 requires `ngtcp2_conn_extend_max_stream_offset` and msquic
requires `StreamReceiveComplete`, and omitting it deadlocks both at the initial window. The
layer calls it twice for the same bytes, once naming the stream and once for the connection,
because stream credit does not imply connection credit.

It is also the bound on the event stream, and that obligation is separate from the QUIC-level
meaning. A transport that reads ahead of the layer must limit that read-ahead by the credit
extended to it *even when its QUIC library manages windows itself* — otherwise the memory
bound moves out of flow control and into the process, where a fast peer can exhaust it. For
quinn the two genuinely come apart, and its adapter says so where it implements the method.

**The retain policy is declared, not assumed.** `RETAINS_BUFFERS` says whether an
implementation reads through the buffers it is given. A transport that copies may report
release as soon as a write returns; one that borrows must wait for the peer. Declaring
`false` while borrowing is a use-after-free.

The driver does not branch on it — every implementation reports release the same way, and
nothing is freed until it does. What the constant buys is that the choice has to be made and
written down, where a comment can be copied along with an adapter's shape and left unread.
The two implementations in this repository declare it differently and reach the same answer
by different routes, which is the useful thing to be able to see at a glance.

**Suspension is an explicit transport boundary.** `poll_flush` is called immediately before
the driver can return `Pending` for transport work: while binding, while opening a request
stream, on transmit backpressure, and in the idle event poll. This is not an instruction to
flush after every internal driver pass. A transport whose output is already handed to its
endpoint returns ready immediately; a byte-stream transport may retain bounded records across
productive passes and use this call to drain them or register its write wake. Errors are
explicit, and a ready result may not leave progress dependent on an unrelated future wakeup.

**The clock is the backend's.** nghttp3 wants a timestamp on every read and the core will not
invent one — that is what keeps `std::time` out of it. A transport necessarily has a runtime
and therefore a clock; ngtcp2 exposes `ngtcp2_conn_get_timestamp` for exactly this.

### What the survey found missing outright

Three things were absent from the first draft and are present because a library other than
quinn needed them: `extend_credit`, above; `close` with an application error code, which all
four expose and HTTP/3 requires; and a stream-closed event carrying *both* directions' error
codes, which `Conn::close_stream_with` needs and without which a per-stream handle map can
only grow. `Released` also gained a `delivered` flag, because msquic's `SEND_COMPLETE` can
mean "your buffer is back but the data was cancelled", and reporting that as acknowledgement
would be a protocol lie.

### Two asymmetries that look like inconsistencies

**Dropping an unread body.** A client's unread *response* body abandons its exchange; a
server's unread *request* body does not. A handler that ignores the body it was given still
owes an answer, so abandoning there would destroy an exchange that is very much alive.

**Waker liveness.** A body's waker goes inert when its stream is forgotten. A handler's waker
is gated on the *handler* existing instead, because a handler routinely outlives its exchange
— the peer resets, and the future answering it is still running — and it must stay pollable
or it is held forever un-woken.

### Delivery cannot assume containment

Received body bytes are handed over as refcounted views of the transport's buffer wherever
possible, but the bytes a handler receives are **not guaranteed to lie inside it**. When a
stream's QPACK decoding is blocked, nghttp3 buffers the input and replays it later from its
own memory, during a call that is feeding a different stream entirely. `Bytes::slice_ref`
panics off-allocation, and a panic reached from a handler unwinds into a C frame and aborts
the process — so containment is checked and the replay path copies.

### A caller's body failure is not the state machine's

`BodyOutcome::Fail` is connection-fatal: it poisons the connection and releases every
retained buffer. A caller's body reporting an error must not do that to every unrelated
exchange sharing the connection, so the layer abandons the one stream instead — it
withholds the end-of-stream marker entirely, resets that stream with
`H3_REQUEST_CANCELLED`, and reports `ErrorKind::Body` to a client that is waiting to be
told.

Withholding the marker is the part that had to be learnt. The layer used to end the failed
body exactly as it ended a successful one and reset the stream afterwards, which is two
statements about one stream that contradict each other. Which of them the peer believed
depended on how much happened to be queued behind the marker: with a backlog the reset
discarded it and the truncation was plain, and with none the marker had already landed, so
the peer had a complete message and ignored a reset for a stream it considered finished.
Nothing about that is transport-specific — ngtcp2 declines to send a reset at all once the
marker has been acknowledged, so on QUIC a short body was reliably rather than merely
occasionally truncated in silence. A response without a content-length gives its receiver
no way to notice.

So the failure path returns `BodyOutcome::Defer`. That is not a euphemism for a variant
that ought to exist: nghttp3's data callback has exactly two non-erroring answers, and the
other one produces the marker, so deferring is the only way to say "produce nothing
further for this stream" at all. A `BodyOutcome::Abandon` would still have to become
`NGHTTP3_ERR_WOULDBLOCK` underneath and would buy a name at the cost of every match on the
enumeration in the workspace.

Deferring brings an obligation with it, because a deferred stream that is never resumed
and never reset waits forever — a silent stall, which is worse than the truncation being
removed. Three things discharge it. The recorded ending is read at the *top* of a driver
pass, through `Role::settle`, rather than where the rest of the role's work happens: the
pass can wait for a bidirectional stream the transport has not opened, or decide the
connection is finished, before it ever reaches the role's ordinary advance, and under the
old behaviour those merely delayed a reset nobody was waiting for. Both roles report
themselves busy while an ending is unread, so the driver cannot park in the window between
the body failing during one pass's transmit and the reset being queued at the top of the
next. And the reset drain tells nghttp3 the write side is done, so it stops offering write
turns to a suspended stream that will never answer with bytes.

The error code is `H3_REQUEST_CANCELLED`, which is what it always was. RFC 9114 §4.1.1
names it for abandoning a message part-way in either direction, and pairs it with the
protection this whole arrangement exists to obtain: a response cancelled after a partial
delivery SHOULD NOT be used.

Two faults that the end-of-stream marker had been covering surfaced once it was gone, and
are fixed alongside it. A reset can arrive in the same batch of transport events as the
head it follows, and the driver sweeps the control plane first, so there was no exchange to
fail yet and the reset was dropped; with the failed body's stream no longer ending itself,
that left a server handler reading a request body that could neither end nor fail. Unheard
resets are now kept and applied once the pass has opened the exchanges it read. And a
server discarded a stream's ending when the peer reset it, which was harmless while the
body still ended itself; the ending is now pruned by ownership, as the client already
pruned its own.

### One addition to the core

`RetainedBytes::from_owner`. The handle owned an `Arc<[u8]>` and `From<&[u8]>` copies, so a
`bytes::Bytes` could not become one without a copy — which made the zero-copy body path
impossible rather than merely awkward. An erased owner needs no dependency and no `unsafe`.
Its length is fixed at construction and every read is clamped, so an ill-behaved `AsRef`
cannot panic inside a C frame.
