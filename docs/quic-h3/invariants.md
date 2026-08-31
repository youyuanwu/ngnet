# HTTP/3 over ngtcp2: invariants

Claims `crates/ngnet-quic-h3/tests/invariants.rs` reads from this crate's own source. Each is
cheap to keep true and expensive to rediscover.

## Nothing here is `unsafe`

The crate joins two safe APIs and has no foreign boundary of its own, which is why — unlike
both crates it depends on — it carries no module allowance list. An `unsafe` block appearing
here would mean the thing needing it belongs in `ngnet-quic` or `ngnet-h3`, behind their
allowance lists, where the reasoning about the C library's contract already lives.

## Nothing here owns a socket, a thread or a process

Everything this crate does happens on the caller's task, driven by the HTTP/3 layer polling
it. The endpoint owns the socket and the clock. Spawning anything here would put work
somewhere the caller can neither see nor cancel, and would make the crate's behaviour depend
on a runtime it does not name.

Checked by name: `thread`, `process`, `UdpSocket`, `TcpStream`.

## The suspension flush is explicit and immediate

The required `QuicConnection::poll_flush` implementation returns ready because this adapter
hands datagrams to the endpoint while pumping and retains no byte-stream output. There is no
default fallback in the trait, so omitting the implementation is a compile error. The adapter
is compiled by the targeted release suites, both workspace test modes, full all-target clippy,
and its warning-denying Rust documentation build.

## Borrowed stream writes stay packet-bounded

The transport copies because ngtcp2 retains stream pointers, but a drain must never allocate
the caller's complete large offer. The allocation suite offers 1 MiB, requires actual accepted
progress, and rejects every allocation larger than 64 KiB. The fixed-count fixture separately
keeps supervised 125 × 16 KiB and 125 × 1 MiB exact protocols. Their live-loopback tests are
ignored while run 30's intermittent outer-driver liveness failure remains unresolved.

Compiling diagnostics does not arm them. The feature-enabled unarmed allocation proof records
representative packet, release, timer, wake, and park hooks and requires zero allocations plus
the default snapshot. A separate integrated drain proof leaves a one-byte diagnostic-only
staging control set while unarmed, requires actual multi-byte progress, and still requires the
default snapshot; this catches range/retention traversal or behavior leaking into timing.

## Module files are flat

`src/foo.rs`, never `src/foo/mod.rs` — the same rule the sibling crates keep. A nested module
file produces a tree where the interesting file and the file that merely declares it share a
name in different directories.

## Nothing is included from outside

`include_str!` and `include!` are refused. A claim about what the source contains is worth
nothing if arbitrary text can be pulled in at compile time.

## The manifest declares what it should

Exactly `ngnet-h3`, `ngnet-quic` and `bytes`, and no `publish = false` — here or in either
crate it binds, since a published crate cannot depend on an unpublished one. The point of the
crate is that it is the only place the two families meet; a fourth dependency is not
necessarily wrong but should be a decision rather than a drift.

## The scanner works

Two self-tests come before the claims above: one proving the scanner sees a real violation,
one proving it ignores the same words in comments and string literals — which this crate's
documentation is full of — and does not mistake a longer identifier for the word it contains.
A structural suite that cannot fail is decoration.
