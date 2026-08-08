# Design

Why the code is shaped the way it is. Behaviour is documented with the code; this records
the decisions, and in particular the ones where the obvious alternative was tried and
failed.

## Layering

```
ngnet-h2-sys      raw FFI; builds libnghttp2 from deps/nghttp2
   ↑
ngnet-h2          sans-I/O state machine  ── feature "http" ──▶  async HTTP/2 API
   ↑                (src/*.rs)                                    (src/http/)
ngnet-h2-tests    unpublished; real runtimes, real sockets, third-party clients
```

The split is load-bearing rather than tidy. The sans-I/O layer never opens a socket, blocks,
sleeps or spawns, which is what makes it usable from blocking code, from any runtime, and
from tests that wire a client to a server entirely in memory. `tests/invariants.rs` enforces
this structurally: the core may not so much as *name* `std::net`, `std::io`, `std::thread`,
`std::time`, `std::fs` or `std::process`, so the claim is checked rather than asserted.

The async layer is confined to `src/http/`, contains no `unsafe`, and is the only place in
the crate where `async`/`await` may appear. Disabling the `http` feature returns the crate
to exactly the state machine — one dependency, no async.

## The async layer's six mechanisms

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

**4. The write policy is the h2 layer's choice, and the transport only says which I/O model
it belongs to.**
Two separate facts bound what any drain may do with the session's output blocks. The C
library serialises from a reused buffer chain and recycles it at frame-item boundaries, so a
block is invalidated by the *next* block being requested — earlier, in fact, than the
published "until the next call of `nghttp2_session_mem_send2()`" wording promises.
Independently, `Session::send` returns `Option<&[u8]>` borrowing `&mut self`, so the safe
wrapper permits exactly **one live block borrow at a time** regardless of what the C library
would tolerate. Together these mean two blocks can never be gathered *with each other*.
They do **not** mean gathering is impossible: one live block may always be gathered with
memory the driver already owns, and that is what the gathering drains do. A connection that
hands its bodies over (mechanism 6) loosens this further still: a handed-over payload is the
caller's own `Bytes`, so it can be gathered as an owned region without being copied at all.

**The decision is split in two, and neither half is where it used to be.** A transport
declares its I/O model through `TransportWrite::Model` — `Readiness` or `Completion` — and
nothing else. How a pass drains is decided by the h2 layer, from `Config::write_policy`,
settled at handshake and held for the connection's life. An earlier design had the transport
name one of four *strategies* (`Coalesced`, `PerRegion`, `Gathering`, `OwnedRegions`); those
markers are gone, and with them the idea that a backend has a say in how many syscalls a pass
costs. It does not: it knows how its I/O works, not what the traffic looks like.

**Every transport gathers.** `BorrowedWrite::write_vectored` and `RegionWrite::write_regions`
are both *provided*, defaulting to a loop over the model's one required primitive
(`write_borrowed` and `write` respectively). A transport that cannot gather natively therefore
gathers by emulation, and one that can does better by overriding. Naming a model obliges the
writer to implement that model's trait, by compiler error — but not to implement its gathering
operation, which is where the "cannot advertise what you have not supplied" property now
lives: there is nothing left to advertise.

The four drains, as the two policies × two models:

- **`Readiness` under `WritePolicy::Gathered`** — the default, and what a real socket wants.
  The driver accumulates small blocks into a buffer it owns and reuses, and when a
  block exceeds `VECTORED_THRESHOLD` it emits `[accumulated, block]` through `write_vectored`
  — the large block is never copied. One syscall per pass in the common case, zero allocation
  in steady state. The driver keeps a retained list of lifetime-free region descriptors capped
  at `MAX_REGIONS` (currently 64) and materialises it into slices only at write time, so a
  write carries at most `MAX_REGIONS + 1` regions — the cap, plus one live session block riding
  as the trailing region — which is well under `IOV_MAX`. Under a default 64 KiB window the cap
  never binds (a pass carries about nine regions); a peer may advertise a window large enough
  to reach it, and then the list is flushed mid-pass and restarted rather than overrun. (An
  earlier design capped this at two regions; handing bodies over made a longer list worth
  retaining.)

  This does buffer, and the buffering is the point: small blocks are copied so that they cost
  no syscall, while anything large enough to be worth its own syscall is never copied. It is
  the same design `h2` uses, arrived at independently and landing on the same threshold —
  `h2`'s `FramedWrite` keeps a `BytesMut`, copies frames below `CHAIN_THRESHOLD` (256 when the
  transport reports `is_write_vectored`, 1024 when it does not), chains larger DATA payloads
  uncopied, and hands the pair to `writev`. Two differences are worth knowing: `h2` preallocates
  16 KiB per connection where this driver starts empty and grows to its steady-state high-water
  mark, and `h2` tops the buffer up from the head of a chained payload when the accumulation is
  smaller than the threshold, so its first region is never a runt. Neither difference has been
  measured to matter here, and the second is a refinement this driver does not make.
- **`Completion` under `WritePolicy::Gathered`** — the same accumulation, expressed in owned
  buffers. A completion transport cannot lend the kernel a borrowed `IoSlice`: the kernel
  writes from the buffers after submission, so they must be owned. The driver coalesces the
  session's own blocks into a driver buffer, every one of them, and hands the pass out as a
  list of owned `Bytes` through `write_regions`, reaching a single `writev`. A block borrowed
  from the session cannot be owned without a copy, so all of them are copied; a *handed-over*
  payload is already the caller's own `Bytes` and rides uncopied as its own region.
- **Either model under `WritePolicy::Coalesced`** — gathering off. One write per pass, bought
  by copying every outgoing octet into a driver buffer, every pass. That buffer is reused
  across passes, so it costs no allocation in steady state. This is the successor to the old
  `Coalesced` *strategy*, and the difference is who says so: it is now a caller's decision,
  reachable wherever a connection is configured, rather than a fact baked into a transport
  type.

The old `PerRegion` strategy — the session's own blocks written one per call, without
accumulation — has no successor and was removed. It is dominated on every measured workload:
emulated gathering does exactly what it did except that the driver accumulates *first*, so the
emulating loop typically runs once instead of once per block.

**A backend implements exactly one I/O model, and the type system enforces it.** `Readiness`
is a `ReadinessModel`, `Completion` a `CompletionModel`, and neither is both. `BorrowedWrite`
is bounded on `Self::Model: ReadinessModel` and `RegionWrite` on `Self::Model: CompletionModel`,
so a writer *cannot* implement operations from both models — it is a compile error, not a
convention. The two marker traits are load-bearing rather than decorative: without them the
plain shape lets a type implement both `RegionWrite` and `BorrowedWrite` and it compiles,
which was verified by building exactly that. `WriteModel` is sealed, so the set of two is
closed and the driver's handling of it is exhaustive by construction.

There is no precedence rule, because there is nothing to arbitrate: a writer belongs to one
model and gets one fast path with it. Declining a path mid-pass is likewise not expressible:
the operations are not `Option`-shaped, and a writer that cannot complete a write reports so
through its result, a short count the driver re-offers or an error. Both are pinned by
`compile_fail` doctests with error codes, each mutation-verified to fail when the guarded
construct is made legal.

**No capability is read on any path the driver or the trait surface can see.** The previous
design read exactly one:
`VectoredWrite::gathers()`, consulted once per connection through `Elects::prepare`, because
whether a stream *really* scatter-gathers is a property of the stream rather than the backend —
a tokio `AsyncWrite` whose `poll_write_vectored` is the default writes only the first region.
Both the predicate and the once-per-connection machinery are gone.

What remains is strictly private and strictly local. `TokioWriter` calls tokio's own
`is_write_vectored()` once, at construction, and keeps the answer in a private field to choose
between forwarding to `poll_write_vectored` and running the emulating default. That is not the
old capability under another name: nothing outside the adapter can read it, no trait exposes
it, the driver never branches on it, and both answers produce the same octets — it selects
only which of two implementations of the *same* contract runs. The property that matters is
that a transport can no longer change what the h2 layer does, and that holds.

That removal is safe, and it is worth being precise about why, because the obvious reason is
wrong. Such a stream was never a *correctness* hazard: it writes the first region and returns
the count it actually wrote, which is an ordinary short write, and the driver's gathering loop
re-offers the remainder from the octets still outstanding. No octet was ever at risk. What
`gathers()` avoided was the *cost* — one syscall per region with none of the gathering
benefit. Removing it is affordable because accumulation happens in the driver, before any
write: 513 small blocks from eight multiplexed streams collapse into a single region, so the
emulating loop runs once. Emulation's cost is set by the regions the driver offers, never by
the blocks the session produced.

Its removal also closed a footgun. `gathers()` defaulted to `true` while tokio's
`is_write_vectored()` defaults to `false` — opposite conservatism — so a third-party wrapper
that forgot to forward the question silently inherited the optimistic answer. There is now no
question to forget: a wrapper that forwards nothing inherits the emulating default, which is
correct and bounded.

**Where mandatory gathering is genuinely worse.** At high region counts gathering loses to
coalescing outright — the benchmarks measure roughly 68 Kelem/s against 152 at N=64 — because
`writev` with many descriptors costs more than one `write` of the same octets copied. That
ordering is why `WritePolicy::Coalesced` exists and is public rather than being a private
fallback: a caller whose traffic looks like that has a real reason to turn gathering off, and
must be able to.

Two per-call costs an earlier design replaced are worth recording, because both were invisible.
The driver used to discover the vectored capability by calling `write_vectored(&[])` and
dropping the resulting future unpolled, once per flush pass — which forced the trait contract
to be widened to require implementations tolerate that. And `TokioWriter::write_vectored` used
to call `AsyncWrite::is_write_vectored`, a virtual call whose answer never changes for a given
stream, on every write. The first has no successor; the second is asked once, in `split`, and
cached in a field — which is now a purely private optimisation choosing between the native
`writev` and the emulating default, invisible in the trait surface.

**5. Commands reach the driver through a queue, and wakes never re-enter a held lock.** The
session lives in the driver and is `!Sync`, so handles and bodies cannot touch it directly.
Submissions, resets-on-drop and flow-control consumption are enqueued and drained at the top
of each pass. Because a waker can fire re-entrantly from *inside* a session call — a body
waking itself while being read — every structure written from a callback takes only a short
leaf-level lock that the driver never holds across a session call.

**6. A connection may hand its bodies over rather than copy them, opt-in and per
connection.** libnghttp2 hands the read callback an uninitialised frame buffer that the push
path zeroes and the body source fills — every body octet is touched twice, and the memset
covers the whole 16 KiB frame budget rather than the payload, so even a 1 KiB body pays a
16 KiB memset. A connection built through the additive `handshake_shared`/`serve_shared`
entry points (and their `_with` forms) instead supplies bodies as `SharedBodySource`, handing
libnghttp2 `Bytes` the caller already owns. Those frames serialise with
`NGHTTP2_DATA_FLAG_NO_COPY`: libnghttp2 mints only the nine-octet header and never touches the
payload, which travels to the transport as the caller's own `Bytes`. The choice is per
connection, not per body, because selecting the adapter needs `B::Data = Bytes` known
statically at a monomorphic construction point; a crate-private `BodyPlan` trait carries it
down to submission without changing any public signature. The push-model API is entirely
unchanged, and the two paths are pinned octet-for-octet against each other. The measured
payoff — large on the readiness transport, small and honestly *not* meeting the stated bar on
the completion transport — is in [`benchmarks.md`](benchmarks.md).

## The completion transport, and why it compiles no fallback

A second transport ships behind the off-by-default `completion` feature: compio, over
io_uring. It exists because a completion runtime is where these traits' shape earns itself —
`read`/`write` pass buffer ownership in and hand it back, which is what a completion API
requires and what a readiness API satisfies for free.

The workspace asks compio for its `io-uring` backend and **no readiness one**. That is
deliberate and it is the whole design. With both backends compiled, compio builds a *fusion*
driver that probes the kernel and silently degrades to epoll when io_uring cannot be obtained.
A transport that quietly became readiness-based while still calling itself completion-based
would make every measurement taken through it a lie. Pinning the driver at our own call sites
would not fix that, because a transport ships to users who construct their own runtimes — so
the guarantee has to live somewhere they inherit it, which is the manifest.

The guarantee is not absolute, and the documentation says so rather than pretending otherwise:
cargo unifies features across a whole dependency graph, so a crate elsewhere in a build
enabling compio's `polling` would restore the fusion driver, and nothing here could prevent it.
A runtime assertion catches the case where a fallback *actually happened*; `cargo tree -e
features` is what shows whether `polling` reached the build at all. Neither check subsumes the
other, and conflating them was a mistake caught in review.

**What it costs, measured — and a correction.** io_uring is about twice as fast as this
crate's tokio transport once several streams are multiplexed, and slower on empty-body round
trips. The obvious reading of that pair — completion I/O multiplexes better than readiness
I/O — is **wrong**, and benchmarking hyper over a real socket is what showed it: hyper
reaches the same throughput on epoll. The gap is not the I/O model but the number of write
syscalls per pass. The tokio transport elected the borrowed path and issued a write per
session block; a completion transport cannot borrow a session block at all, so it coalesces,
and hyper coalesces by buffering. The two fast arms were the two coalescing ones. Flipping
only `TokioWriter::write_borrowed` to `None` erased the gap.

That framing presented a trade — few syscalls or zero allocation, pick one — and the trade
turned out to be false. The gathering path (mechanism 4 above) reaches both: it collapses a
multiplexed pass from one write per block to a single `writev` while copying nothing that
the driver did not already own. Measured on the tokio transport it moved concurrent
throughput **+109% at N=8 and +143% at N=64**, to parity with compio and slightly ahead of
hyper, and 1 MiB body throughput up rather than down. See `benchmarks.md` for the
numbers and the three confounds that bound them.

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
available on the pinned toolchain, so it needs no hedge. **A body that might fail must return an
error, not panic.**

## Performance shape

Two properties are pinned as tests rather than described (see [`invariants.md`](invariants.md)):

| Path | Writes per driver pass | Allocation per pass in steady state |
| --- | --- | --- |
| Readiness, `Gathered`, native `write_vectored` | **one**, or one per large block | **zero** |
| Readiness, `Gathered`, emulated | **one**, or one per region offered | **zero** |
| Completion, `Gathered` | **one**, or one per region-cap flush | **zero**, copies each session block but no handed-over payload |
| Either model, `Coalesced` | **one**, coalesced | **zero**, but copies every octet |

All four reach zero steady-state allocation; both driver buffers are reused across passes
rather than rebuilt. What separates them is the write count — a syscall count — and the
copy. Among the three readiness shapes the gathering path dominates: it reaches the borrowed
path's zero allocation and zero copy of large blocks while matching or beating the coalesced
path's write count, which is why the tokio adapter now takes it. The owned-region path is its
completion-transport counterpart — one write per pass, copying each borrowed session block but
never a handed-over payload — and looks identical to the owned path on a push-model workload,
which is why `http_zero_alloc.rs` pins it on an upload rather than a multiplexed pass. Counted
by `tests/http_zero_alloc.rs` on eight multiplexed streams, the three readiness shapes come
out at 0 allocations and 1 write (owned, having copied every octet), 0 allocations and 513
writes (borrowed), and 0 allocations and 1 write (gathering). Per-stream setup is deliberately
excluded from the measurement and documented as such — the recurring cost of moving frames is
the claim, not the one-off cost of standing a stream up.

## Constraints that shape contributions

- Edition **2024**, built with the toolchain in `rust-toolchain.toml`. No declared MSRV.
- `ngnet-h2` takes **no dev-dependencies** and exactly one non-optional dependency, both
  enforced by `tests/invariants.rs`. Test scaffolding lives in `src/http/testing.rs` as
  `#[doc(hidden)] pub`; anything needing third-party crates belongs in `ngnet-h2-tests`.
- No `unsafe` under `src/http/`.
- Verification must cover the **feature matrix**, not just `--all-features`. A doc link to a
  `tokio`-gated item once passed `--all-features` and broke every other configuration.
