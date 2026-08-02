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

- **Whether the borrowed write path is right for TCP.** Measured, and it is: a few small
  writes per pass buys zero allocation and zero copy, and steady-state zero allocation is
  reachable no other way. But the measurement is against an in-memory transport, so it counts
  syscalls rather than pricing them. If a workload emerges where write syscalls dominate, the
  owned path is one method away and the trade should be re-measured against real sockets.

## Testing gaps worth closing eventually

- **The `curl` interop test skips silently** when `curl` is not installed. A skipped test
  that says nothing is close to no test; it should at least be visible in CI that it ran.
- **The exact `RST_STREAM` code for a failed server body is not pinned.** The test asserts
  the code is not `NO_ERROR`, because the value originates inside libnghttp2 and hard-coding
  it would pin their choice rather than our behaviour. Defensible, but it means a change in
  that code would pass unnoticed.
