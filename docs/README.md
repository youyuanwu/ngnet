# Documentation

Notes that outlive any one change. API documentation lives with the code (`cargo doc`);
what is here is the reasoning a reader cannot recover from the source.

There are three crate families — HTTP/2, HTTP/3 and QUIC — built the same way and largely
independent of each other, so each keeps its own notes. Alongside them sit two crates that
are not families but *layers above* one: `ngnet-axum`, which wires the HTTP/2 family to a
third-party framework on the server side, and `ngnet-util`, which is the client-side policy
layer over `ngnet-h2`'s single-connection client. Both keep their notes here for the same
reason. Anything genuinely shared sits at this level.

## HTTP/2 — [`h2/`](h2/)

| Document | What it covers |
| --- | --- |
| [`h2/design.md`](h2/design.md) | How the crates fit together, the mechanisms the async layer rests on, and why each was chosen over the alternative that looks simpler. |
| [`h2/pending-work.md`](h2/pending-work.md) | Known gaps and deferred decisions, each with the evidence that produced it and what would settle it. |
| [`h2/invariants.md`](h2/invariants.md) | The properties the `ngnet-h2` suite pins rather than merely exercises, and where each is enforced. |
| [`h2/benchmarks.md`](h2/benchmarks.md) | How the `ngnet-h2` vs hyper benchmarks are run, what their numbers do and do not mean, and which protocol settings are matched between the two stacks. |

## HTTP/3 — [`h3/`](h3/)

| Document | What it covers |
| --- | --- |
| [`h3/design.md`](h3/design.md) | Why the HTTP/3 crates are shaped the way they are, the five places nghttp3 differs from nghttp2 in ways that are easy to miss, and what a survey of four QUIC libraries changed about the transport abstraction. |
| [`h3/pending-work.md`](h3/pending-work.md) | Known gaps and deferred decisions, with what would settle each. |
| [`h3/invariants.md`](h3/invariants.md) | The properties the `ngnet-h3` suite pins, and where each is enforced. |

There are no HTTP/3 benchmarks yet.

## QUIC — [`quic/`](quic/)

| Document | What it covers |
| --- | --- |
| [`quic/design.md`](quic/design.md) | Why the QUIC crates are shaped the way they are: the API ngtcp2 documents but does not export, why validation is duplicated in Rust, the three-object TLS teardown order, why entropy cannot travel through the callback bridge, why one driver owns a whole socket, and the two lengths and two flow-control windows that make a working connection go silent when either is got wrong. |
| [`quic/pending-work.md`](quic/pending-work.md) | Known gaps and deferred decisions, with what would settle each. |
| [`quic/invariants.md`](quic/invariants.md) | The properties the `ngnet-quic` suite pins, and where each is enforced. |

There are no QUIC benchmarks, and the crate has not been tested against another QUIC
implementation.

## axum integration — [`axum/`](axum/)

| Document | What it covers |
| --- | --- |
| [`axum/design.md`](axum/design.md) | Why an axum `Router` can run without hyper at all, the two designs that were assumed and turned out wrong — body adapters, and a graceful shutdown that cannot be built — and what the acceptance suite found that nobody predicted. |

There are no axum invariants or benchmarks: the crate pins its claims in its own acceptance
suite and in a CI dependency-graph check, rather than in a document of its own.

## Client policy layer — [`util/`](util/)

| Document | What it covers |
| --- | --- |
| [`util/design.md`](util/design.md) | Why HTTP/2 multiplexing makes a pool something other than a queue of idle sockets, what the dial state machine is for and why a `OnceCell` will not do, when a connection is evicted and why replacement is lazy, and why the crate reports a retriable failure without ever retrying. |

There are no `ngnet-util` invariants or benchmarks: like the axum integration, the crate
pins its claims in its own acceptance suite and in a CI dependency-graph check rather than
in a document of its own.

## Shared

| Document | What it covers |
| --- | --- |
| [`ci.md`](ci.md) | Every check CI runs, across all three families and the axum integration, and the ones it deliberately does not. |

## Orientation

**HTTP/2.** `ngnet-h2-sys` builds libnghttp2 from the vendored submodule and generates raw
bindings. `ngnet-h2` wraps it twice: a sans-I/O state machine that performs no I/O at all,
and — behind the default-on `http` feature — an asynchronous HTTP/2 API over that state
machine. `ngnet-h2-tests` is unpublished and exists so the wrapper itself can stay free of
dev-dependencies while still being driven against real runtimes and real sockets.

Cleartext (h2c) only for HTTP/2. TLS, ALPN, server push and stream priorities are out of
scope there, and that is a scope decision rather than a to-do.

**HTTP/3.** `ngnet-h3-sys` builds libnghttp3 and generates raw bindings; `ngnet-h3` wraps it
twice, in the same shape as the HTTP/2 family — a sans-I/O state machine, and behind the
default-on `http` feature an asynchronous HTTP/3 API over it. `ngnet-h3-tests` is unpublished
and drives both over a real quinn connection.

No QUIC or TLS implementation is bundled *with the HTTP/3 crates*, because nghttp3 depends on
neither. The async layer's `QuicConnection` trait begins with an *established* connection, so
which QUIC library to use, how to authenticate the peer and how to negotiate `h3` stay
entirely with the caller. Server push is absent because nghttp3 does not implement it.

`ngnet-quic` is a QUIC implementation living in this same workspace, but nothing in the
HTTP/3 family depends on it and no adapter between the two exists yet — see
[`quic/pending-work.md`](quic/pending-work.md).

**QUIC.** `ngnet-quic-sys` builds ngtcp2 from the vendored submodule and generates raw
bindings; `ngnet-quic` wraps it twice, in the same shape as the other two families — a
sans-I/O state machine, and behind the default-on `endpoint` feature an asynchronous layer
that owns a UDP socket and the connections reachable through it. TLS is a backend seam with a
default-on OpenSSL implementation, which needs system OpenSSL 3.5 or newer.
`ngnet-quic-tests` is unpublished and drives real handshakes, in process and over loopback
UDP.

Where the HTTP families take a byte transport or an established connection, this layer owns
the socket: one driver serves every connection on it, because several drivers cannot each own
one UDP socket. The socket and the clock are seams a caller implements, with a ready-made
pair for tokio behind an off-by-default feature. A server can validate client addresses
before committing any connection state, which is what makes it safe to expose.

Client and server are both supported. 0-RTT, unreliable datagrams, connection migration, key
update, ECN marking and datagram batching are not implemented, and that is a scope decision
rather than a to-do.

**axum.** `ngnet-axum` serves an axum `Router` over `ngnet-h2` instead of hyper. It is an
integration rather than a family: no `-sys` crate, no state machine, no layering of its own —
it wires two existing things together. That is possible because a `Router` is a
`tower::Service` over `http` types and the HTTP engine beneath it is separable; hyper's only
contribution to axum is turning bytes into requests, and this crate gives that job to
`ngnet-h2`. Server-side, h2c and tokio only, and unpublished for now.

Within a family's own documents, "the crate" means that family's wrapper: `ngnet-h2` under
[`h2/`](h2/), `ngnet-h3` under [`h3/`](h3/), `ngnet-quic` under [`quic/`](quic/). The same
convention holds for the integration: "the crate" means `ngnet-axum` under
[`axum/`](axum/).

## Toolchain

There is no declared MSRV, and no crate sets `rust-version`. `rust-toolchain.toml` names the
one toolchain contributors and CI both build with; rustup reads it automatically. The
workspace could not honour a single minimum in any case — the benchmark crate's Criterion
dependency and compio's buffer crate both need newer compilers than the rest — so a declared
minimum would have described some crates rather than the workspace.
