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

**2. The read trait is completion-shaped; the write primitive belongs to the I/O model; both
are split at construction.** Buffer ownership passes into `read` and comes back out — a
completion API (io_uring, IOCP) requires that, and a readiness API satisfies it without
copying. Writing is not universal: `TransportWrite` carries no write at all, and the
primitive comes from `BorrowedWrite` (lends) or `RegionWrite` (owns), because a readiness
transport can never use ownership it is handed. `Transport::split` divides the
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

**4. The transport declares whether its gathering is real; the h2 layer decides what to do
about the answer.**
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

**The decision is split in two, and the split has moved twice.** A transport declares two
things and only two: its I/O model through `TransportWrite::Model` — `Readiness` or
`Completion` — and whether its gathering is native, through `TransportWrite::is_write_vectored`.
It names no drain. The h2 layer reads the second declaration once, immediately after
`Transport::split`, and derives the drain from it: `true` takes the gathered drain, `false` the
coalesced one.

The first design had the transport name one of four *strategies* (`Coalesced`, `PerRegion`,
`Gathering`, `OwnedRegions`). Those markers are gone and are not coming back. The design after
that moved the choice to the *caller*, through `Config::write_policy`, on the rule that "the
transport does not vote". That rule is now withdrawn, and it is worth being exact about why,
because it was not wrong so much as aimed at the wrong target.

What it correctly rejected was a transport *naming a drain* — declaring a decision about the h2
layer's syscall budget that a backend has no standing to make. What it incorrectly swept up
with that was the transport *describing itself*. Whether a given stream has a real `writev`
behind it is a fact about the stream, knowable only by the stream: a caller configuring a
`Config` cannot know whether the `AsyncWrite` it is about to hand over inherits tokio's
first-region-only default. Asking the caller to answer it was asking the wrong party. The
capability is therefore a *description*, one bit wide, with the h2 layer retaining the entire
decision about what that description is worth — which is the shape hyper uses, and the shape
tokio's own `AsyncWrite::is_write_vectored` has.

The transport-supplied *default policy* that was specified, implemented, and dropped before
shipping (see `pending-work.md`) is a third thing again, and it stays dropped: it let the
transport supply an initial *decision* that the caller could override, which is the shape this
one deliberately does not have. There is no override, because there is nothing to override —
the caller no longer expresses an opinion at all.

**The write primitive belongs to the model, not to the transport trait.** `TransportWrite`
carries `Model` and `commit`, and no write at all. Readiness
transports supply `BorrowedWrite::write_borrowed`; completion transports supply
`RegionWrite::write_owned`. This is what "who owns the buffer" means, and putting the
primitive anywhere else forced the wrong answer on somebody: when the owned write sat on the
shared trait, every readiness transport had to accept a buffer it could only borrow from —
`TokioWriter`'s implementation took ownership and immediately took a reference — and the
driver *manufactured* that ownership out of its own reused coalescing buffer to feed it.

**Every transport *can* gather; not every transport gathers *well*.**
`BorrowedWrite::write_vectored` and `RegionWrite::write_regions` are both *provided*,
defaulting to a loop over the model's one required primitive (`write_borrowed` and
`write_owned` respectively). So the gathering operations are always callable, and always
correct — a transport that cannot gather natively still gathers, by emulation, and one that can
does better by overriding. Naming a model obliges the writer to implement that model's trait,
by compiler error; it does not oblige the writer to implement that trait's gathering operation.

What `is_write_vectored` adds is the *quality* of that gathering, which the type system cannot
see. Overriding `write_vectored` is not detectable from outside, so the driver cannot infer the
answer and does not try: it believes the declaration. A transport that overrides the operation
and forgets the declaration is coalesced and its override is dead code; a transport that
declares `true` without overriding gets the emulating loop, which is correct but is the
transport's own choice to pay for. Neither is a bug the crate can catch, and both are the
transport's to get right — which is the price of a one-bit description over an inferred one.

**The default is `false`, and this inverts the crate's previous stance.** `gathers()` defaulted
to `true`: a wrapper that forgot to forward the question inherited "yes, I gather" and then
quietly wrote one region per pass through a stream that ignored all but the first. The new
default matches tokio's conservatism instead. A wrapper that forgets inherits "no", and the
driver coalesces: one write, one copy, bounded by the pass. Forgetting now costs a bounded copy
rather than an unbounded number of syscalls, and the failure mode of the conservative default
is a measurable slowdown rather than a silent one.

The four drains, as the two declarations × two models:

- **`Readiness` declaring `true`** — what a real socket with a real `writev` wants.
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
- **`Completion` declaring `true`** — the same accumulation, expressed in owned
  buffers. A completion transport cannot lend the kernel a borrowed `IoSlice`: the kernel
  writes from the buffers after submission, so they must be owned. The driver coalesces the
  session's own blocks into a driver buffer, every one of them, and hands the pass out as a
  list of owned `Bytes` through `write_regions`, reaching a single `writev`. A block borrowed
  from the session cannot be owned without a copy, so all of them are copied; a *handed-over*
  payload is already the caller's own `Bytes` and rides uncopied as its own region.
- **Either model declaring `false`** — gathering off. One write per pass, bought by copying
  every outgoing octet into a driver buffer, every pass. That buffer is reused across passes,
  so it costs no allocation in steady state. The two models reach the single write differently,
  and only one of them transfers anything: the readiness drain *lends* the buffer through
  `write_borrowed` and clears it, while the completion drain must hand over an owned `Bytes`,
  because a completion transport keeps the buffer until the operation finishes.

  This is the successor to the old `Coalesced` *strategy*, and it has now been said by three
  different parties: baked into a transport type, then set by the caller on a `Config`, and now
  *described* by the transport again. The difference between the first and the third is that
  `Coalesced` named a drain while `is_write_vectored() == false` states a fact and lets the h2
  layer draw the conclusion; the difference from the second is that the transport is the only
  party who knows the fact.

  **This drain is also where the change's one real cost lands, and the cost is not hypothetical.**
  A readiness transport that cannot gather used to take the *gathered* drain — the caller's
  default was `Gathered` and the transport had no say — and there reach `write_vectored`'s
  emulating loop. Because the driver accumulates sub-`VECTORED_THRESHOLD` blocks into a single
  region *before* any write, a multiplexed pass of hundreds of small blocks collapsed to one
  region, so that loop typically ran **once**: one write, and a copy of only the small blocks,
  with any handed-over payload riding uncopied as its own region. The same transport now
  declares `false`, takes this drain, and gets one write and a copy of **every** outgoing octet,
  handed-over payloads included. For the common low-region-count pass that is a regression —
  same syscall count, strictly more copying — and it was accepted with the tradeoff visible. The
  reasons are that it is hyper's shape, that the coalesced cost is bounded and predictable where
  the emulating loop's is not, and that the transport paying it is the one that declined to
  claim it could gather. A transport that *can* gather and says so is unaffected; a transport
  that can gather and forgets to say so pays this and should fix its declaration.

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

**Exactly one capability is read, exactly once, and the driver acts on it.** It is
`TransportWrite::is_write_vectored`, read in `driver::run` immediately after
`transport.split()` and held in a local for the connection's life. That placement is not
incidental: the query has to happen where only the `TransportWrite` half is in scope, before
either drain has been chosen, which is why the method lives on `TransportWrite` rather than on
`BorrowedWrite` or `RegionWrite` — the driver holds neither of those at that point and cannot
name them without first knowing the answer. It is a plain `&self -> bool` for the same reason:
it is asked before any write, so it must be answerable without I/O, and it is asked once, so it
must not change its mind.

This section previously read "**No capability is read on any path the driver or the trait
surface can see**", and described a design in which `TokioWriter` cached tokio's
`is_write_vectored()` in a private field that "never leaves this adapter". That is now false in
both halves, and the reasoning that produced it does not survive.

The reasoning was: `gathers()` was never a correctness mechanism, because a stream with the
default `poll_write_vectored` writes the first region and returns the count it wrote, which the
driver's gathering loop handles as an ordinary short write and re-offers from. No octet was
ever at risk. That part is still true, and is why the emulating defaults are still correct and
are kept. What `gathers()` avoided was *cost*, and the argument was that the cost had already
been removed elsewhere: the driver accumulates before writing, so 513 small blocks from eight
multiplexed streams collapse into a single region and the emulating loop runs once.

That argument holds for the common case and fails for the general one. Emulation's cost is set
by the regions the driver offers — which is exactly the point, because the driver offers more
than one region whenever a payload is handed over uncopied, and the region count then scales
with the number of concurrent bodies rather than being pinned at one. At high region counts the
emulating loop degenerates toward one syscall per region against a stream that will not gather,
which is the pattern the crate spent a design revision removing. The old argument mistook "the
loop usually runs once" for "the loop is bounded". It is not bounded, and the transport is the
only party that knows whether the loop will do anything useful at all.

The second half — that the tokio adapter's cached answer is private and invisible — was true
when written and is now precisely the thing that changed. The same cached field feeds
`TransportWrite::is_write_vectored`, so it leaves the adapter, reaches the driver, and selects
a drain. Nothing about how it is obtained changed; what changed is that somebody now listens.

The footgun that removing `gathers()` closed is closed differently rather than reopened.
`gathers()` defaulted to `true` while tokio's `is_write_vectored()` defaults to `false`, so a
third-party wrapper that forgot to forward the question inherited the *optimistic* answer and
wrote one region per pass. `is_write_vectored` defaults to `false`, so a wrapper that forgets
inherits the *pessimistic* answer and is coalesced: one write and one bounded copy. The
question is back, but the cost of failing to answer it went from unbounded to bounded.

**Where mandatory gathering is genuinely worse — and what is actually measured.** An earlier
version of this passage read "gathering loses to coalescing outright — the benchmarks measure
roughly 68 Kelem/s against 152 at N=64". **That was a misreading and is corrected here.** The
68.3 Kelem/s figure is the *removed `PerRegion` drain* — one write per session block, with no
accumulation at all. It is not gathering, and it is not emulated gathering. Gathering on a
natively-gathering `TcpStream` at N=64 measured **166.0** Kelem/s, against roughly 152 for the
coalescing arms: gathering *won*. On that workload a natively-gathering readiness transport
therefore does not prefer coalescing, and this document previously implied the opposite. The
scope is one measured workload, not a universal claim — which is why a transport that gathers
natively is *asked to say so* rather than being assumed to, and why the answer for a transport
that says nothing is the conservative one.

The case where coalescing genuinely wins is narrower and, importantly, **is not measured
anywhere**: *emulated* gathering at high region counts, where `write_vectored`'s provided
default loops one borrowed write per region and so degenerates toward the per-region syscall
pattern that produced the 68.3. Coalescing collapses that to one write for the cost of one
copy. The reasoning is structural — the loop's call count is visible in the code and pinned by
`http_zero_alloc.rs` — but no benchmark has swept it, and no number should be quoted for it
until one has.

That case is why the coalesced drain exists rather than gathering being unconditional, and it
is now reached by the party that can actually recognise it: a transport whose gathering is a
loop reports `false` and is coalesced. Under the previous design this required the *caller* to
diagnose a property of a socket it did not own; under this one the socket answers for itself.
Note that the transport still cannot ask for coalescing on region-count grounds — it answers
"is my gathering real", not "would coalescing suit my traffic" — so a natively-gathering
transport whose traffic happens to fit the crossover has no way to say so. That is a
deliberate narrowing: the question a transport is asked is one it can always answer correctly,
which a traffic-shape question would not be.

Two per-call costs an earlier design replaced are worth recording, because both were invisible.
The driver used to discover the vectored capability by calling `write_vectored(&[])` and
dropping the resulting future unpolled, once per flush pass — which forced the trait contract
to be widened to require implementations tolerate that. And `TokioWriter::write_vectored` used
to call `AsyncWrite::is_write_vectored`, a virtual call whose answer never changes for a given
stream, on every write. The first has no successor; the second is asked once, in `split`, and
cached in a field. That field now does double duty: it still chooses between the native
`writev` and the emulating default *inside* the adapter, and it is also what the adapter
returns from `TransportWrite::is_write_vectored`. The internal branch is kept even though the
driver will not call `write_vectored` on a writer reporting `false` — `BorrowedWrite` is a
public trait and a direct caller may invoke the operation regardless of the declaration, and
the branch is what keeps that call correct rather than first-region-only.

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
the completion transport — is in
[`benchmarks/findings/handing-bodies-over.md`](benchmarks/findings/handing-bodies-over.md).

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
hyper, and 1 MiB body throughput up rather than down. See
`benchmarks/findings/write-path-and-gathering.md` for the numbers and
`benchmarks/controls.md` for the three confounds that bound them.

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
| Readiness, declares `true`, native `write_vectored` | **one**, or one per large block | **zero** |
| Readiness, declares `true`, emulated gathering | **one**, or one per region offered | **zero** |
| Completion, declares `true` | **one**, or one per region-cap flush | **zero**, copies each session block but no handed-over payload |
| Either model, declares `false` | **one**, coalesced | **zero**, but copies every octet |

All four reach zero steady-state allocation; both driver buffers are reused across passes
rather than rebuilt. What separates them is the write count — a syscall count — and the
copy. Among the readiness shapes the native gathering path dominates: it reaches the borrowed
path's zero allocation and zero copy of large blocks while matching or beating the coalesced
path's write count, which is why the tokio adapter declares `true` whenever the stream beneath
it does. The owned-region path is its completion-transport counterpart — one write per pass,
copying each borrowed session block but never a handed-over payload — and looks identical to
the owned path on a push-model workload, which is why `http_zero_alloc.rs` pins it on an upload
rather than a multiplexed pass.

Counted by `tests/http_zero_alloc.rs` on eight multiplexed streams, every shape now comes out
at 0 allocations and 1 write, because the driver accumulates sub-`VECTORED_THRESHOLD` blocks
into a single region before any write, and a single region collapses the difference between
the drains. The write counts separate only on an upload: 4 for a gathering declaration against
1 for a coalescing one. A **513**-write figure appears in this repository's history for
multiplexed traffic; it belongs to the removed per-block borrowed drain, which wrote each
block separately and no longer exists in any form. It is not the count of any current shape,
and quoting it as one is a mistake made before.

Per-stream setup is deliberately excluded from the measurement and documented as such — the
recurring cost of moving frames is the claim, not the one-off cost of standing a stream up.

## Constraints that shape contributions

- Edition **2024**, built with the toolchain in `rust-toolchain.toml`. No declared MSRV.
- `ngnet-h2` takes **no dev-dependencies** and exactly one non-optional dependency, both
  enforced by `tests/invariants.rs`. Test scaffolding lives in `src/http/testing.rs` as
  `#[doc(hidden)] pub`; anything needing third-party crates belongs in `ngnet-h2-tests`.
- No `unsafe` under `src/http/`.
- Verification must cover the **feature matrix**, not just `--all-features`. A doc link to a
  `tokio`-gated item once passed `--all-features` and broke every other configuration.
