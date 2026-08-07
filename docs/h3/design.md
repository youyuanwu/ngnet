# HTTP/3 design

Why the HTTP/3 crates are shaped the way they are. Behaviour is documented with the code;
this records the decisions, and in particular the places where nghttp3 differs from nghttp2
in ways that are easy to miss.

## Layering

```
ngnet-h3-sys      raw FFI; builds libnghttp3 from deps/nghttp3
   ↑
ngnet-h3          sans-I/O connection state machine  (no async layer)
   ↑
ngnet-h3-tests    unpublished; drives the wrapper over a real quinn connection
```

`ngnet-h3` performs no I/O. It opens no socket, blocks nowhere, creates no threads and reads
no clock: the caller owns the QUIC connection, opens the streams, declares which of them
carry control and QPACK data, and moves bytes in and out. `tests/invariants.rs` enforces
that structurally — the crate may not so much as *name* `std::net`, `std::fs`, `std::thread`,
`std::time`, `std::process` or `std::env`, and the only `std::io` item it may name is
`IoSlice`, which describes borrowed bytes rather than offering a way to move them.

That boundary is inherited rather than invented: nghttp3 depends on no QUIC transport and on
no TLS library, so neither does this.

Unlike `ngnet-h2`, there is **no asynchronous layer** — no `http`/`http-body` API, no
feature gating one. This crate is the core such a layer would be built on. Server push is
absent because nghttp3 does not implement it.

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
