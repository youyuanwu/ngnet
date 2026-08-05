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
  Measured, `ngrs-tokio` improved **-52% at N=8 and -59% at N=64**, reaching parity with
  io_uring. Body uploads improved where a `HEADERS` block could be folded into the first DATA
  frame's `writev` (-15% at 1 KiB, -14% at 64 KiB); at 1 MiB the effect is neutral, which is
  what was wanted there — the goal at large bodies was to avoid the copy a coalescing path
  would have imposed, not to gain. See `docs/benchmarks.md`.
- **True zero-copy DATA payloads are still open, and are now the remaining copy.** Gathering
  removed the driver's copy, but libnghttp2 still copies every body into its own serialisation
  buffer before we ever see it — measurement confirms DATA reaches us as 16393-byte blocks,
  i.e. the 9-byte frame header already joined to a copied 16384-byte payload.
  `NGHTTP2_DATA_FLAG_NO_COPY` with `nghttp2_send_data_callback` would eliminate it, handing the
  frame header and the payload over separately so the payload can go straight into a `writev`
  region. It was scoped out of the vectored work deliberately: it requires a send-data callback
  whose reentrancy interacts with the existing bridge, and gathering alone already brought this
  crate level with hyper at 1 MiB. This is the obvious next lever if large-body throughput ever
  needs to improve.
- **The write-path asymmetry is unmeasured on a real NIC.** Benchmarks show tokio's borrowed
  zero-copy write cancelling io_uring's syscall advantage at 1 MiB bodies, over loopback. Whether
  that holds where real device interrupts exist is unknown, and loopback biases against
  io_uring, so the crossover point is probably not where these numbers put it.
- **SC-006's error path is evidenced by construction only.** No test exercises what a caller
  sees when io_uring is unavailable, because this machine cannot make it unavailable to order
  and a mocked one would test the mock.

## Deliberate scope boundaries

These are not gaps. They are decisions, recorded so they are not mistaken for oversights.

- **Cleartext (h2c) only.** TLS and ALPN are the caller's concern.
- **No server push, stream priorities, or HTTP/3.**
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
  adapter gathers, and the completion adapter coalesces because a completion API needs the
  kernel to own the buffer. Gathering was measured to dominate rather than trade — zero
  steady-state allocation *and* one write per pass — so there is no longer a knob-shaped
  question here for a readiness transport. What remains open is narrower: `VECTORED_THRESHOLD`
  is 256 bytes, untuned, and deliberately so. Dumping real block sizes shows the distribution
  is sharply bimodal with nothing in between — control and `HEADERS` blocks at 9–73 bytes,
  DATA blocks at 16392–16393 (a 16 KiB payload with its 9-byte header already joined) — so any
  threshold between roughly 128 and 16384 partitions real traffic identically. `h2` picks the
  same 256 for the same reason. A peer advertising a small `MAX_FRAME_SIZE`, or a workload of
  many medium-sized frames, would be the reason to revisit it.

## Testing gaps worth closing eventually

- **The `curl` interop test skips silently** when `curl` is not installed. A skipped test
  that says nothing is close to no test; it should at least be visible in CI that it ran.
- **The exact `RST_STREAM` code for a failed server body is not pinned.** The test asserts
  the code is not `NO_ERROR`, because the value originates inside libnghttp2 and hard-coding
  it would pin their choice rather than our behaviour. Defensible, but it means a change in
  that code would pass unnoticed.
