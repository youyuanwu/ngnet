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

  So `TransportWrite::write_vectored` accumulates small blocks into a driver-owned reused
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

  **The completion transport now gathers.** `CompioIo` declares the owned-region strategy,
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
- **One connection, no policy layer.** No pooling, retries, redirects, or `Service`
  abstraction — those belong in a layer above this crate.
- **No boxed transports.** The transport traits return `impl Future`, so they are
  generic-only and not object-safe, by design.
- **Outgoing bodies must be `Send + 'static`**, because the session holds them. Received
  bodies carry no such bound.

## Judgement calls worth revisiting with real traffic

- **`Config` defaults: 128 concurrent streams, 64 KiB header list.** Chosen because
  libnghttp2's own local defaults are effectively unlimited, and something conservative had
  to be picked; 128 matches nginx's default and sits below hyper's 200. These are policy,
  not physics. h2c on a trusted network may well want them looser, and a public-facing server
  may want them tighter.

- **Which write strategy a transport should elect.** Settled for the two that ship: the tokio
  adapter declares `Gathering`, and the completion adapter declares `OwnedRegions` because a
  completion API needs the kernel to own the buffer. Since the strategy split this is no longer
  even a choice a shipped adapter re-litigates per call — it is one line of type declaration,
  checked by the compiler. Gathering was measured to dominate rather than trade — zero
  steady-state allocation *and* one write per pass — so there is no longer a knob-shaped
  question here for a readiness transport. What remains open is narrower: `VECTORED_THRESHOLD`
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
  for exactly this reason. Pre-existing — it predates the strategy split and is unchanged by
  it — and deliberately not fixed there, because adding a flush per pass to the completion
  path is a behaviour change that would perturb the owned-region measurements that refactor
  had to hold constant. Fix wants its own change and its own before/after numbers, plus a
  bounded-budget regression test of the kind `http_flush.rs` already uses for tokio.

- **Whether `ngnet-h3` and `ngnet-quic` should adopt the strategy split.** Transport traits
  exist only in `ngnet-h2`; neither of the other crates has an equivalent, so the split was
  deliberately scoped to h2. If either grows an I/O abstraction, the shape is worth copying
  rather than reinventing: an associated `type Strategy` naming one of a sealed set, with the
  operations on separate traits bounded by the strategy's I/O model, so a backend implements
  exactly one model and the compiler enforces it. The two things that were not obvious and
  would have to be rediscovered otherwise are that the `ReadinessStrategy`/`CompletionStrategy`
  marker traits are load-bearing — without them one type can implement both models and it
  compiles — and that resolving a capability once per connection needs an explicit `prepare`
  step, because a driver generic over the base trait has no capability method in scope and
  will otherwise re-read it per pass. Not worth doing speculatively; recorded so it is not
  re-derived.

## Testing gaps worth closing eventually

- **The `curl` interop test skips silently** when `curl` is not installed. A skipped test
  that says nothing is close to no test; it should at least be visible in CI that it ran.
- **The exact `RST_STREAM` code for a failed server body is not pinned.** The test asserts
  the code is not `NO_ERROR`, because the value originates inside libnghttp2 and hard-coding
  it would pin their choice rather than our behaviour. Defensible, but it means a change in
  that code would pass unnoticed.
