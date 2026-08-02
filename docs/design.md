# Design

Why the code is shaped the way it is. Behaviour is documented with the code; this records
the decisions, and in particular the ones where the obvious alternative was tried and
failed.

## Layering

```
nghttp2-sys      raw FFI; builds libnghttp2 from deps/nghttp2
   ↑
nghttp2          sans-I/O state machine  ── feature "http" ──▶  async HTTP/2 API
   ↑                (src/*.rs)                                    (src/http/)
nghttp2-tests    unpublished; real runtimes, real sockets, third-party clients
```

The split is load-bearing rather than tidy. The sans-I/O layer never opens a socket, blocks,
sleeps or spawns, which is what makes it usable from blocking code, from any runtime, and
from tests that wire a client to a server entirely in memory. `tests/invariants.rs` enforces
this structurally: the core may not so much as *name* `std::net`, `std::io`, `std::thread`,
`std::time`, `std::fs` or `std::process`, so the claim is checked rather than asserted.

The async layer is confined to `src/http/`, contains no `unsafe`, and is the only place in
the crate where `async`/`await` may appear. Disabling the `http` feature returns the crate
to exactly the state machine — one dependency, no async.

## The async layer's five mechanisms

**1. Deferral is driven by wakers, never by polling.** An outgoing body that is not ready
defers instead of blocking. The promise is that a body is never consulted again *without an
intervening wake from its own waker* — so a body that is slow costs nothing, and a spurious
wake costs exactly one extra consultation rather than a spin. The driver's waker is
refreshed on every poll, because a waker captured at submission goes stale the moment the
runtime moves the driver to another thread.

**2. The transport traits are completion-shaped and split at construction.** Buffer
ownership passes into `read`/`write` and comes back out. A completion API (io_uring, IOCP)
requires that; a readiness API satisfies it without copying. `Transport::split` divides the
stream once so the two directions hold separate borrows rather than contending for one.
There is no `Send` bound anywhere — auto traits propagate, so a driver over a `Send`
transport is `Send` by inference, and one over an `Rc`-based transport is not, without the
API having to offer two shapes.

**3. Receive is zero-copy through buffer aliasing, from a pool.** The driver reads into a
pooled `BytesMut`, freezes it, and hands the frozen slice to the session. Chunks delivered
to the data callback alias that buffer; the driver records each chunk's address as an
*offset* into the buffer it handed over — a comparison, never a dereference — and converts
the offset into a slice of the same buffer once the call returns. Zero-copy with no
`unsafe`, and it degrades to a copy rather than to wrong octets if a chunk ever arrives from
somewhere else. A buffer returns to the pool only once no derived chunk still references it,
so a retained chunk safely outlives the pass that read it.

**4. The write strategy is the transport's choice, because the two goals are exclusive.**
The session invalidates each output block when the next is requested, so blocks can never be
gathered without copying. `TransportWrite::write_borrowed` returns `Option<impl Future>`:
`Some` both elects the borrowed path and *is* how it writes — the session's own blocks go
out directly, several small writes per pass but zero allocation and zero copy. `None` (the
default) leaves the coalesced path: one write per pass, bought by allocating and copying
every outgoing octet, every pass.

One method carries both the decision and the operation deliberately. An earlier form — a
boolean plus a separately overridable method — let an adapter advertise the fast path
without supplying it, or supply it without the driver ever taking it. Both compiled; both
regressions were silent. Now neither is expressible, pinned by `compile_fail` doctests.

**5. Commands reach the driver through a queue, and wakes never re-enter a held lock.** The
session lives in the driver and is `!Sync`, so handles and bodies cannot touch it directly.
Submissions, resets-on-drop and flow-control consumption are enqueued and drained at the top
of each pass. Because a waker can fire re-entrantly from *inside* a session call — a body
waking itself while being read — every structure written from a callback takes only a short
leaf-level lock that the driver never holds across a session call.

## Decisions that cost a wrong attempt first

- **`Role::signals` returns live closures, not booleans.** Reading the role's "busy"/"done"
  state into locals before parking — mirroring the `want_write` snapshot right next to it —
  deadlocked every client. A snapshot taken before the park still says "nothing to do" after
  the wake that had something to do. `want_write` is safe to snapshot *only* because every
  path that can change it is separately watched by the same predicate; role state has no
  such guarantee. This is the single most dangerous pattern in the driver.

- **A handler's waker is gated on the handler; a body's on its stream.** The asymmetry is
  the point. Resuming a body for a stream that is gone is meaningless. But a stream the peer
  reset is precisely when its *handler* most needs polling — to notice — so gating the
  handler on stream liveness made a cancelled handler unwakeable, which is indistinguishable
  from never telling it.

- **A reset stream's handler is not dropped.** Dropping a future tells it nothing. The
  handler runs on, learns its stream is gone through its request body failing or through the
  `Cancelled` extension, and its response is discarded at submission. That is how a handler
  gets to stop *early* rather than be cancelled silently. The concurrency cap counts these
  retained handlers, so cooperative cancellation still has a structural bound.

- **Direction, not role, decides what dropping a received body means.** Dropping an unread
  *response* resets the stream: returning the flow-control window without stopping the peer
  invites it to send the rest of something nobody will read. Dropping an unread *request* on
  a server does not, because a handler that ignores the body still owes a response.

- **The driver is a named, boxed `Connection<F>`.** `impl Future` cannot carry
  `#[must_use]`, and the trap is real — keep the handle, drop the driver, and you have a
  connection that compiles and never sends a byte. Naming the type lets the compiler say so.
  Boxing means the pin projection needs no `unsafe`: one allocation per connection.

- **Announcing trailers costs no wire frame.** `http_body` yields trailers *after* the last
  data frame, but HTTP/2 must decide at that frame whether the stream stays open. Rather than
  buffer a frame ahead, the trailing block is announced on the next consultation; libnghttp2
  cancels the zero-length DATA frame that would carry no end-of-stream. Measured by counting
  frames at the peer, not assumed — an earlier version of this note was simply wrong.

- **A client's `GOAWAY` names stream zero.** The last-stream-id is the last stream the
  *sender received*. A client that accepts no pushed streams has received none, so naming one
  of its own requests is a claim about the peer's streams — which libnghttp2 rejects.

- **`ready` and `credits` are hash, not tree, collections.** Their `drain` retains capacity,
  so a stream resumed or credited every pass reuses its slot instead of churning a node. One
  of several changes needed to reach zero steady-state allocation. Iteration order becomes
  arbitrary; neither ordering is load-bearing.

## Panics differ by layer, and one of them aborts

A server handler is an ordinary future polled on the driver's task. A panic in it unwinds
out of the driver and fails the connection — every stream on it goes too.

An outgoing body is different. The session pulls it *synchronously from inside an
`extern "C"` callback*, so a panic in a body's `poll_frame` crosses a C frame, and unwinding
out of `extern "C"` aborts the process. This has been true unconditionally since Rust 1.81,
below this crate's MSRV, so it needs no hedge. **A body that might fail must return an
error, not panic.**

## Performance shape

Two properties are pinned as tests rather than described (see [`invariants.md`](invariants.md)):

| Path | Writes per driver pass | Allocation per pass in steady state |
| --- | --- | --- |
| Borrowed (`Some`) | one per session block | **zero** |
| Owned (`None`, default) | **one**, coalesced | one per block, plus growth |

Steady-state zero allocation is reachable only on the borrowed path, which is why the tokio
adapter takes it. Per-stream setup is deliberately excluded from the measurement and
documented as such — the recurring cost of moving frames is the claim, not the one-off cost
of standing a stream up.

## Constraints that shape contributions

- MSRV **1.85**, edition **2024**. Let-chains are a 1.88 feature and are **forbidden**.
- `nghttp2` takes **no dev-dependencies** and exactly one non-optional dependency, both
  enforced by `tests/invariants.rs`. Test scaffolding lives in `src/http/testing.rs` as
  `#[doc(hidden)] pub`; anything needing third-party crates belongs in `nghttp2-tests`.
- No `unsafe` under `src/http/`.
- Verification must cover the **feature matrix**, not just `--all-features`. A doc link to a
  `tokio`-gated item once passed `--all-features` and broke every other configuration.
