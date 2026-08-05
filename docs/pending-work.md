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
- **The gathering accumulator is not preallocated.** `gathered` starts as `BytesMut::new()`
  and grows to its steady-state high-water mark, reallocating a few times during warm-up
  before it settles — which is why `http_zero_alloc.rs` measures only the steady state. `h2`
  instead preallocates 16 KiB per connection (`DEFAULT_BUFFER_CAPACITY`), sized so a maximal
  `HEADERS` frame always fits without growth. Preallocating would trade a fixed per-connection
  footprint for the removal of the warm-up reallocations, which matters most for short-lived
  connections that may never reach steady state — precisely the case the current shape serves
  worst. Sizing it is the open question, and it should be measured rather than copied from
  `h2`: the accumulator only ever holds blocks *below* `VECTORED_THRESHOLD`, so its high-water
  mark is set by how many small blocks a multiplexed pass produces, not by the maximum frame
  size. The multiplexed benchmark pass accumulates thousands of 73-byte blocks, so 16 KiB may
  well be the wrong number in either direction.
- **True zero-copy DATA payloads are still open, and are now the remaining copy — in fact two.**
  Gathering removed the driver's copy, but every body byte is still touched twice before it
  reaches a socket, both inside the read-body callback (`crates/nghttp2/src/callbacks.rs`):
  libnghttp2 hands over an uninitialised frame buffer, which is `write_bytes(.., 0, length)`
  zeroed in full — necessary today, both because forming a `&mut [u8]` over uninitialised
  memory is undefined behaviour and because a body source must never observe another stream's
  plaintext left in a reused buffer — and the source then copies the payload into it. So a
  16 KiB DATA frame costs a 16 KiB memset plus a 16 KiB copy before the write even starts.

  `NGHTTP2_DATA_FLAG_NO_COPY` with `nghttp2_send_data_callback` would remove both, handing the
  9-byte frame header and the payload over separately so the payload can go straight into a
  `writev` region from the caller's own `Bytes`. **The costs are real and were the reason it
  was scoped out:**

  1. **The callback is synchronous and all-or-nothing.** It is invoked from inside
     `nghttp2_session_mem_send2` (`nghttp2_session.c:3043`, `session_call_send_data`), so
     nothing inside it can `.await`. It must send the *complete* frame; the header is explicit
     that a partial send is unrecoverable and leaves teardown as the only option.
  2. **`WOULDBLOCK` is not usable as backpressure as things stand.** Returning it makes
     `mem_send_internal` `return 0`, which `Session::send` maps to `Ok(None)` — indistinguishable
     from "nothing left to send", so the driver would treat the pass as finished and park.
  3. **So the only viable shape is "record, don't write":** copy the 9-byte header (it points
     into libnghttp2's own buffer and *is* invalidated), clone the payload handle, append both
     to a pending region list, return 0, and let the driver's `writev` do the writing after
     `mem_send2` returns. That means reporting a frame as sent slightly before it is on the
     wire, which is tolerable only because a transport error tears the connection down anyway.
  4. **It reintroduces `IOV_MAX`.** The present design is capped at two regions, which is why
     the limit is currently a non-concern; a pass full of no-copy DATA frames would produce
     many, needing a cap and a generalised partial-`writev` retry.
  5. **It crosses the no-`unsafe` boundary.** The callback is `extern "C"` and must live below
     `src/http/`, while the region list it appends to belongs to the driver inside it — so this
     needs a new plumbing seam rather than a local change.
  6. **It changes public API.** `fill(&mut [u8])` is a push model; no-copy needs a source that
     *hands out* bytes it already owns. Sources that genuinely generate bytes gain nothing, so
     both paths would have to coexist.

  **Measure before building.** The prize is one memset plus one copy per body byte, which
  should matter only on large bodies — order 10% of a 1 MiB exchange on a back-of-the-envelope
  memory-bandwidth estimate, and nothing at all on the small-body and multiplexed workloads
  where this crate already sits at parity. A targeted measurement of what fraction of a 1 MiB
  exchange is spent in the memset and the copy is the cheap next step, and would decide whether
  any of the above is worth paying.
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
