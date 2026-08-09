# Pending work

Known gaps, deferred decisions and things worth doing next. Each entry records the evidence
that produced it and what would settle it, so a later reader can judge whether it still
applies rather than re-deriving the argument.

Nothing here is a known defect. Items were deferred on their merits, not left half-finished.

## Deferred from the final review

### Head encoding allocates per exchange, and validates twice

`OwnedHeaders::views()` collects a fresh `Vec<Header>` on every call, and it is called twice
per exchange — once to validate early, once at submission — before the native header vector
is built. For roughly eight-field heads at high request rates that is a few short-lived
vectors per exchange, ahead of the allocation the FFI needs anyway.

Deliberately **not** optimised, because it is a projection rather than a measurement. The
Phase 9 allocation harness excludes per-stream setup by design, so it will not catch this;
what is missing is a benchmark that counts allocations across many heads at several field
counts. **Settle it by measuring first.** If duplicate view vectors turn out to be
allocator-visible at the target rate, either reuse one vector across both calls or validate
without collecting. If they do not, close this and record the number.

### Malformed request heads discard a specific diagnosis

The server boundary computes a precise reason a request head was rejected — unsupported
`:protocol`, duplicate pseudo-header, missing `:method`/`:path`/`:authority`, non-h2c scheme,
forbidden `CONNECT`, userinfo in the authority, and several more — and then throws it away,
sending a generic `RST_STREAM(PROTOCOL_ERROR)`.

Malformed heads are exactly what turns into an interop ticket, and the code pays to name the
cause at the one place an operator would want it. Deferred because the fix is an
observability hook, and adding public surface for one call site is the wrong shape:
**this belongs with a general tracing story for the crate**, decided once, rather than bolted
on here.

### Defence in depth for inbound connection-specific fields

The outbound encoders reject `connection`, `transfer-encoding`, `keep-alive`,
`proxy-connection` and `upgrade`; the inbound decoders do not check.

Three independent reviewers raised this and all three **withdrew it** after reading the
vendored C: libnghttp2 rejects those fields before the callback ever runs, so there is no
live gap. Retained here only as a possible defence-in-depth *test* — one that would fail
loudly if a future libnghttp2 upgrade relaxed that validation. Worth roughly the cost of one
test, and no more.

## Left by the completion-transport work

- **Only one completion runtime.** compio ships; tokio-uring, monoio and glommio do not.
  compio was chosen because it implements its buffer traits for `bytes` types, so the adapter
  needs no `unsafe` — tokio-uring's are `unsafe` and `Vec<u8>`-only, which would have collided
  with the no-`unsafe`-under-`src/http` invariant. The transport is generic over compio's
  `Splittable`, so `TcpStream` and `UnixStream` both work; extending to another runtime means
  another adapter, not a change to the traits.
- **No fallback where io_uring is absent.** The feature fails loudly instead, by decision.
  Revisiting that means compiling a readiness backend and accepting the silent-degradation
  hazard that comes with it.
- **RESOLVED — the tokio transport's per-block writes cost a factor of two, and gathering
  fixed it without a trade.** Recorded here because the reasoning is worth keeping, not
  because work remains. `TokioWriter` used to elect the borrowed path, so the driver handed
  each session block to `write` separately: zero-copy and zero-allocation, but one `write(2)`
  per block, with the block count growing with the number of multiplexed streams. Flipping
  that method to `None` measured **+95% at N=8 and +128% at N=64**, which framed the question
  as zero-allocation *or* syscall count, with both defaults defensible.

  **That framing was wrong, and so was the reason given for it.** This file previously
  asserted that gathering blocks into one vectored write "is closed off by the session
  invalidating each block when the next is requested, so any gather implies the copy". Two
  facts were being conflated. The C library recycles its serialisation buffer at frame-item
  boundaries, and `Session::send` returns a slice borrowing `&mut self`, so at most one block
  is live at a time — which forecloses gathering blocks **with each other**, and nothing
  more. One live block gathers perfectly well with memory the driver already owns.

  So `BorrowedWrite::write_vectored` — on `TransportWrite` at the time, moved to the readiness
  trait when the write primitive split by I/O model — accumulates small blocks into a driver-owned reused
  buffer and emits `[accumulated, large_block]` as a two-region `writev`, copying nothing
  large. It reaches zero steady-state allocation *and* one write per pass: on eight
  multiplexed streams, 0 allocations and 1 write, against the borrowed path's 0 and 513.
  Measured, `ngnet-h2-tokio` improved **-52% at N=8 and -59% at N=64**, reaching parity with
  io_uring. Body uploads improved where a `HEADERS` block could be folded into the first DATA
  frame's `writev` (-15% at 1 KiB, -14% at 64 KiB); at 1 MiB the effect is neutral, which is
  what was wanted there — the goal at large bodies was to avoid the copy a coalescing path
  would have imposed, not to gain. See `benchmarks.md`.
- **Neither driver write buffer is preallocated.** `gathered` and `out` both start as
  `BytesMut::new()` and grow to their steady-state high-water marks, reallocating a few times
  during warm-up before settling — which is why `http_zero_alloc.rs` measures only the steady
  state. `h2` instead preallocates 16 KiB per connection (`DEFAULT_BUFFER_CAPACITY`), sized so
  a maximal `HEADERS` frame always fits without growth. Preallocating would trade a fixed
  per-connection footprint for the removal of the warm-up reallocations, which matters most
  for short-lived connections that may never reach steady state — precisely the case the
  current shape serves worst.

  Sizing is the open question, and the two buffers want different answers, so they should not
  share a constant. `gathered` only ever holds blocks *below* `VECTORED_THRESHOLD` and is
  drained whenever a large block arrives, so on an upload it stays near zero; its high-water
  mark is set by how many small blocks a multiplexed pass produces — thousands of 73-byte
  blocks in the benchmark. `out` is the looser of the two: it holds a whole coalesced pass,
  16 KiB DATA frames included, so its bound is the flow-control window the *peer* advertises,
  which a peer may raise. Measure before picking either; 16 KiB is unlikely to be right for
  both. Removing `out`'s per-pass allocation was worth about 4-7% to the completion transport
  (see `benchmarks.md`), and the residual warm-up cost is what this entry is about.

  Two second-order effects are worth knowing before anyone revisits this, neither a defect.
  `BytesMut::reserve` folds the split offset into the requested capacity before doubling, so a
  buffer whose passes land just past the remaining tail can settle at roughly *twice* the
  high-water pass size rather than once. And the first `split()` on a fresh `BytesMut`
  promotes it from `KIND_VEC` to `KIND_ARC`, which costs one small `Box<Shared>` per
  connection — once, during warm-up, on every path including those where `out` stays empty.
  A shrink policy for `out` is defensible on footprint grounds; it is not needed for
  correctness, and retention is the same tradeoff the driver's other reused collections
  already accept.
- **Zero-copy DATA payloads are done, and what remains is the shape of the opt-in.**
  `NGHTTP2_DATA_FLAG_NO_COPY` with `nghttp2_send_data_callback` is implemented. A connection
  opened with `handshake_shared`/`serve_shared` (or their `_with` forms) hands its bodies over
  instead of copying them: libnghttp2 never touches the frame payload buffer, so both the
  memset and the source-side copy are gone, and the payload reaches the transport as the
  caller's own `Bytes`. The push-model API is unchanged and remains the default.

  The six recorded obstacles resolved as follows. (1) The callback is indeed synchronous and
  all-or-nothing, and is designed around rather than defeated: it *records* header and payload
  and returns, and the driver writes after `mem_send2` returns. (2) `WOULDBLOCK` is indeed
  unusable, and is never returned. (3) "Reports sent-before-wire" **dissolved** — the existing
  copying path already accounts a frame as sent before the application writes its block, so
  this was never a new concession. (4) `IOV_MAX` did return, and is handled by a retained
  descriptor list capped at `MAX_REGIONS = 64` with a generalised partial-write retry. (5) The
  no-`unsafe` boundary held: the callback records into a sink that the driver drains, which is
  the new plumbing seam. (6) The API did change, additively — the opt-in is a parallel set of
  entry points, and the no-copy source trait stayed crate-private.

  **The completion transport now gathers.** `CompioIo` declares the completion model,
  whose regions are owned `Bytes`, which satisfies compio's `IoVectoredBuf: 'static` bound and
  reaches a real `IORING_OP_SENDMSG`. The structural reason it could not gather — that borrowed
  `IoSlice`s can never be `'static` — was correctly diagnosed here, and handing bodies over is
  what removed it.

  **Two claims in the original entry were wrong, and measurement is what caught them.**

  1. *"Order 10% of a 1 MiB exchange, and nothing at all on small-body workloads."* The size
     dependence is backwards. A 1 KiB body gains the **most** on the readiness transport
     (−35.3%), because the memset zeroes `datamax` — up to 16 KiB — rather than the payload
     length, so a 1 KiB body pays a full 16 KiB memset per frame.
  2. *"On the completion transport the prize is larger."* It is much smaller: −4.07% at 1 MiB
     against −30.6% for readiness. The reasoning assumed the completion side would also collect
     the syscall collapse gathering gave the readiness side. It could not, because its
     coalescing path already emitted one write per pass — there was no syscall prize left to
     win, only the copy.

  **The dominant mechanism was not the one that was costed.** The estimate priced copy removal,
  but the readiness gain is mostly *write-count collapse*. Measured writes for one upload —
  0 B `1→1`, 1 KiB `2→1`, 64 KiB `5→2`, 1 MiB `65→17` — track the measured gain and vanish
  exactly where the ratio is 1. On the push path libnghttp2 returns one serialised block per
  `mem_send2` call, so a large upload is one write per frame; handing the body over lets a
  whole flow-control window's worth of frames ride in one gathering write. The batch is bounded
  by the 64 KiB initial window, not by `MAX_REGIONS`, which is a guard rail rather than the
  binding constraint. See `benchmarks.md` for the numbers, the drift controls and the
  method.

  **What is actually still open:**

  - **The completion result does not clear the drift bar.** −4.07% at 1 MiB is real in sign
    across every clean replicate, but the untouched `compio-push` control arm moved 34.94% in
    the same sessions, so by the stated criterion it is **not** a demonstrated win. It needs a
    quieter machine and a pre-registered replicate count, not more argument.
  - **The opt-in is per connection, not per body.** A caller who wants to hand over some bodies
    and generate others on one connection cannot; they get the shared path for all of them, and
    a source that genuinely generates bytes gains nothing from it. Making this per-body would
    mean the two source models coexisting on one stream, which the current `BodyPlan` routing
    deliberately does not do.
  - **The opt-in is a separate entry point rather than a config flag**, which is a wider public
    surface than a `Config` setting would have been. It is that way because the shared path
    needs `B::Data = Bytes`, a bound the plain entry points do not carry and could not gain
    without breaking callers.
  - **Two follow-ups were considered and deliberately deferred**, both because they are
    conditional on experience the crate does not have yet:
    - *A crate-provided enum body*, so one connection could carry both body kinds without the
      caller hand-rolling the choice. Worth doing only if the whole-connection constraint above
      proves awkward in practice, which needs callers to have used the shipped shape first.
    - *Exposing the no-copy source trait to sans-I/O callers.* The trait is `pub(crate)` today.
      Worth doing only if the driver-side design generalises cleanly, which is not yet known;
      exporting it early would freeze a shape chosen for one consumer.
- **The write-path asymmetry is unmeasured on a real NIC.** Benchmarks show tokio's borrowed
  zero-copy write cancelling io_uring's syscall advantage at 1 MiB bodies, over loopback. Whether
  that holds where real device interrupts exist is unknown, and loopback biases against
  io_uring, so the crossover point is probably not where these numbers put it.
- **SC-006's error path is evidenced by construction only.** No test exercises what a caller
  sees when io_uring is unavailable, because this machine cannot make it unavailable to order
  and a mocked one would test the mock.

## A server cannot initiate shutdown

**Found while building `ngnet-axum`.** The async server has no way to tell a connected peer
that it is winding up, so "stop accepting, let outstanding exchanges finish, then close" —
the shutdown every HTTP server library offers, and what `axum::serve`'s
`with_graceful_shutdown` does — cannot be built on top of this crate. `ngnet-axum` therefore
offers quiescence instead: it stops accepting and waits for peers to leave of their own
accord, and its method is called `with_stop_signal` rather than `with_graceful_shutdown` so
that the name does not promise a drain. A peer that holds an idle connection open holds the
server open with it.

The machinery is nearly all present, which is what makes this worth recording rather than
merely noting. `GOAWAY` handling in the driver is role-agnostic. Two things are missing, and
they have to arrive together:

- **A server-side shutdown handle.** `shutdown()` exists on the client handle
  (`src/http/client.rs`) and has no server counterpart. Sending `GOAWAY` is the easy half.
- **A completion signal that can fire.** The server's is currently hard-wired to never
  complete (`src/http/server.rs`, `|| false`), with the comment "A server does not decide
  when it is finished; the peer does." That is right for a server that has not announced a
  shutdown, and wrong for one that has. It would need to become: finished once a `GOAWAY`
  has been sent and no stream remains open.

Adding only the first gives a server that says goodbye and then waits forever, which is
worse than the present behaviour because it looks like it works.

**Settle it** by adding both, additively — no existing caller's behaviour changes, since a
server that never calls the new handle keeps the never-done signal it has today. The
acceptance test to write with it is the one `ngnet-axum` cannot write now: a client with an
exchange in flight sees that exchange complete, sees its next request refused, and the
server future resolves without the client having closed anything.

## Toolchain upgrades cost lint fixes

Raising `rust-toolchain.toml` is routine but rarely free: each release adds lints, and this
repository lints with `-D warnings`. The 1.95 → 1.97.1 move needed two — `manual_noop_waker`
(a test waker that predated `Waker::noop`) and sixteen `collapsible_if` sites, all of them
nested `if`s written that way because let-chains were once forbidden.

Neither was a defect, and `cargo clippy --fix` handled the second. Recorded so the next
person raising the pin expects a lint pass rather than a surprise, and knows to check the
diff rather than trusting `--fix` blindly.

## Deliberate scope boundaries

These are not gaps. They are decisions, recorded so they are not mistaken for oversights.

- **Cleartext (h2c) only.** TLS and ALPN are the caller's concern.
- **No server push or stream priorities in `ngnet-h2`.** HTTP/3 is no longer on this list:
  `ngnet-h3` wraps nghttp3 as a sans-I/O core. What that core deliberately lacks — an
  asynchronous layer, a bundled QUIC or TLS implementation, and server push, which nghttp3
  does not implement — is recorded in `docs/README.md`.
- **No server-initiated shutdown**, which is a gap rather than a boundary — see "A server
  cannot initiate shutdown" above. Listed here too because it is the kind of absence a
  reader is most likely to mistake for a deliberate omission.
- **One connection, no policy layer.** No pooling, retries, redirects, or `Service`
  abstraction — those belong in a layer above this crate.
- **No boxed transports.** The transport traits return `impl Future`, so they are
  generic-only and not object-safe, by design.
- **Outgoing bodies must be `Send + 'static`**, because the session holds them. Received
  bodies carry no such bound.

## Judgement calls worth revisiting with real traffic

- **A transport-supplied default write policy: specified, implemented, dropped — and then
  superseded by something narrower.** Recorded here so the sequence is legible, because the
  outcome resembles the dropped design closely enough to be mistaken for it.

  The dropped design was a `TransportWrite::DEFAULT_WRITE_POLICY` associated constant with a
  default of `WritePolicy::Gathered`, supplying the initial value when the caller set none,
  with `Config::write_policy` overriding it outright. It reached a complete, green,
  mutation-verified implementation and was removed before shipping, because all four
  declarations in the workspace were `Gathered` and so the constant had zero behavioural
  effect, at the price of permanent public API surface.

  **What shipped instead is not that design.** `TransportWrite::is_write_vectored` is a
  *description*, not a *default decision*: it says whether the transport's gathering is real,
  not which drain the h2 layer ought to use, and there is nothing for a caller to override
  because the caller no longer has a say at all. The distinction is the whole point. A
  transport can answer "is my `write_vectored` a real syscall" correctly and always; it cannot
  answer "would coalescing suit this connection's traffic", which is what a default *policy*
  asked it to guess at. `Config` lost `write_policy` rather than gaining a fallback behind it.

  **What is still open, and is not answered by this.** The drain choice remains keyed on
  capability alone. A transport whose gathering is genuinely native but whose traffic sits past
  the region-count crossover — where coalescing is expected to win — has no way to say so, and
  neither does its caller. That is deliberate: the question would be one no party can answer
  reliably in advance. If it ever needs answering it should be answered by the driver, from
  observed region counts, not by a declaration; and the prerequisite is a measurement that does
  not exist yet (see the drain-selection entry below).

- **`Config` defaults: 128 concurrent streams, 64 KiB header list.** Chosen because
  libnghttp2's own local defaults are effectively unlimited, and something conservative had
  to be picked; 128 matches nginx's default and sits below hyper's 200. These are policy,
  not physics. h2c on a trusted network may well want them looser, and a public-facing server
  may want them tighter.

- **Which drain a connection should use.** Keyed on one bit the transport declares about
  itself: `TransportWrite::is_write_vectored`. `true` takes the gathered drain, `false` the
  coalesced one, read once per connection. Gathering was measured to dominate on realistic
  traffic over a natively-gathering `TcpStream` — zero steady-state allocation, one write per
  pass, 166.0 against ~152 Kelem/s for the coalescing arms at N=64 — which is why a transport
  that gathers natively should say so, and why the two shipped adapters do.

  The case where coalescing is expected to win is **emulated** gathering at high region counts,
  where the emulating loop degenerates toward one syscall per region. That case is **identified
  structurally and has never been measured on this machine.** No number in this repository
  belongs to it. In particular the 68.3 Kelem/s that appears in `benchmarks.md` is the removed
  *per-block* drain, not emulated gathering, and citing it here would be the same conflation
  this document corrected once already. Emulated gathering accumulates in the driver first, so
  small blocks collapse into one region before the emulating loop sees them.

  **What the capability does not settle.** It routes a transport whose gathering is a loop onto
  the coalesced drain, which is the right answer at high region counts and, for the same
  transport at *low* region counts, arguably the wrong one: a pass that was already a single
  region previously cost one write and copied only the accumulated small blocks, and now costs
  one write and copies everything. That regression is real, accepted, and pinned by
  `an_honest_emulating_transport_now_costs_one_write_per_upload_pass`. Whether the crossover
  deserves a driver-side region-count predictor rather than a single bit is genuinely open and
  deliberately not attempted; measuring the emulated crossover is the cheaper open item and the
  prerequisite for any of it. A transport that wants the old behaviour today can have it by
  declaring `true` — it then reaches the emulating default, which is kept precisely so that
  answer stays meaningful.

  What else remains open is narrower: `VECTORED_THRESHOLD`
  is 256 bytes, untuned, and deliberately so. Dumping real block sizes shows the distribution
  is sharply bimodal with nothing in between — control and `HEADERS` blocks at 9–73 bytes,
  DATA blocks at 16392–16393 (a 16 KiB payload with its 9-byte header already joined) — so any
  threshold between roughly 128 and 16384 partitions real traffic identically. `h2` picks the
  same 256 for the same reason. A peer advertising a small `MAX_FRAME_SIZE`, or a workload of
  many medium-sized frames, would be the reason to revisit it.

- **`CompioWriter` leaves `commit` at its no-op default, but accepts buffering writers.**
  The bound is `W: AsyncWrite`, and compio's `BufWriter` satisfies it — as does any
  `(R, W)` pair through `Splittable` — so a caller can construct a `CompioIo` whose writes
  sit in a user-space buffer while the driver parks awaiting a response that never comes.
  The module doc's claim that "a completion write is committed when it completes" is true of
  a raw socket and false of a buffered wrapper. The tokio adapter already flushes in `commit`
  for exactly this reason. Pre-existing — it predates both the strategy split and the write
  policy, and is unchanged by either — and deliberately not fixed there, because adding a flush
  per pass to the completion
  path is a behaviour change that would perturb the owned-region measurements that refactor
  had to hold constant. Unchanged again by the capability change, which does not touch
  `commit` on either side. Fix wants its own change and its own before/after numbers, plus a
  bounded-budget regression test built on `testing::within_budget`, which `http_flush.rs` and
  `http_shared_body.rs` both already use for exactly this class of "the thing under test never
  happened, so the connection parked forever" failure.

- **Whether `ngnet-h3` and `ngnet-quic` should adopt the transport-trait shape.** Transport
  traits exist only in `ngnet-h2`; neither of the other crates has an equivalent, so the shape
  was deliberately scoped to h2. **The answer to what is worth copying has changed twice**, and
  the second change partly reverses the first, so the advice is worth stating carefully:

  Copy the *model* split, not a strategy split. An associated `type Model` naming one of a
  sealed pair — readiness or completion — with the operations on separate traits bounded by
  that model, so a backend implements exactly one and the compiler enforces it.

  Then copy one capability bit, and copy it in tokio's shape: a plain `&self -> bool`
  describing whether the backend's gathering is native, defaulting to `false`, read once at
  start-up. The previous advice here was "do **not** give the backend a say in how the layer
  drains a pass ... a backend that is asked will answer about itself rather than about the
  traffic." The observation is correct and the conclusion drawn from it was wrong. A backend
  answering about itself is exactly what is wanted, because "do I have a real `writev`" *is* a
  fact about the backend and about nothing else, and no other party can know it. What must not
  be copied is a backend saying which drain to use, or supplying a default the caller
  overrides — that is asking the backend about the traffic, and it will guess.

  Three things were not obvious and would have to be rediscovered otherwise:

  1. The `ReadinessModel`/`CompletionModel` marker traits are load-bearing — without them one
     type can implement both models and it compiles, which was verified by building exactly
     that.
  2. Make the gathering operation a *provided* default that loops over the model's required
     primitive. Every backend then gathers by construction, and one that can do better
     overrides. Note the deliberate asymmetry with the capability bit above: the *operation*
     needs no opt-in and no `prepare` step, because a backend that says nothing still gathers
     correctly; the *capability* is a separate, optional declaration about efficiency, read
     once per connection, whose default is the conservative `false`. An earlier revision of
     this list claimed there was "no capability to consult and no once-per-connection
     machinery" — that was true of that revision and is no longer true of this one. What
     survives from it is the narrower and more useful point: correctness must never depend on
     the declaration, only the write count should.
  3. A defaulted operation cannot be detected by the compiler when it is deleted, so a test
     that means to pin a native override has to find a workload where native and emulated
     genuinely differ. Most workloads do not, because accumulation collapses the region list
     before the write; the handed-over no-copy path is the one that does.

- **`CompioWriter`'s native `write_regions` override is not pinned by any test.** Measured,
  not assumed: replacing its body with a per-region loop over `write_owned` — which is what
  the provided default does — leaves the whole suite green. This is the concrete instance of
  the general problem recorded just above. It is not a defect introduced by the capability
  change, and the change did not make it worse: the override predates it, and no test at any
  point has distinguished one `IORING_OP_SENDMSG` from N. It is hard for a specific reason
  worth recording rather than rediscovering — the loop is *behaviourally identical*. Every
  octet arrives, in order, with the same return value; only the number of submissions differs.
  So no correctness oracle can catch it, and the only discriminator is a count that lives
  inside compio, below the seam this workspace can observe. The tokio side of the same
  question *is* now pinned, by
  `a_direct_vectored_call_on_a_non_gathering_tokio_writer_still_writes_every_region`, and only
  because there the emulated and forwarded paths differ observably — tokio's default drops
  regions after the first, so the difference is octets rather than syscalls. Closing the
  compio gap needs a fake `AsyncWrite` at the compio seam that counts submissions, which is a
  fixture, not a one-line test.

  Not worth doing speculatively; recorded so it is not re-derived.

## Testing gaps worth closing eventually

- **The `curl` interop test skips silently** when `curl` is not installed. A skipped test
  that says nothing is close to no test; it should at least be visible in CI that it ran.
- **The exact `RST_STREAM` code for a failed server body is not pinned.** The test asserts
  the code is not `NO_ERROR`, because the value originates inside libnghttp2 and hard-coding
  it would pin their choice rather than our behaviour. Defensible, but it means a change in
  that code would pass unnoticed.
