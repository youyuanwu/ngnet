# Documentation

Notes that outlive any one change. API documentation lives with the code (`cargo doc`);
what is here is the reasoning a reader cannot recover from the source.

There are four crate families — HTTP/2, HTTP/3, QUIC and QMux — built the same way and largely
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
| [`h2/benchmarks/`](h2/benchmarks/) | How the `ngnet-h2` vs hyper benchmarks are run, one page per bench case, what their numbers do and do not mean, which protocol settings are matched between the two stacks — and the measurements themselves, filed under the machine that produced them. |

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
| [`quic/design.md`](quic/design.md) | Why the QUIC crates are shaped the way they are: the API ngtcp2 documents but does not export, why validation is duplicated in Rust, why the TLS seam is safe and what removing its object cycle cost, why entropy cannot travel through the callback bridge, why one driver owns a whole socket, and the two lengths and two flow-control windows that make a working connection go silent when either is got wrong. |
| [`quic/pending-work.md`](quic/pending-work.md) | Known gaps and deferred decisions, with what would settle each. |
| [`quic/invariants.md`](quic/invariants.md) | The properties the `ngnet-quic` suite pins, and where each is enforced. |

There are no QUIC benchmarks, and the crate has not been tested against another QUIC
implementation.

## QMux — [`qmux/`](qmux/)

QMux is a draft protocol that carries QUIC's stream operations over a single ordered, reliable
byte stream, so an application written against QUIC's stream API can run over TCP. It comes
from the ngtcp2 authors and reuses QUIC's frame encoding, but shares no code with the QUIC
family here — and, unlike QUIC, it mandates no transport security and provides none.

| Document | What it covers |
| --- | --- |
| [`qmux/design.md`](qmux/design.md) | What the protocol is and is not, why the native build breaks with every other `-sys` crate and compiles C directly, the two places where the obvious safe API would have admitted a use-after-free, the four upstream behaviours the wrapper compensates for rather than passes through, and the asynchronous layer: why a QMux connection needs no endpoint and no driver, why the seam is poll-shaped, and what the pump's ordering buys. |
| [`qmux/pending-work.md`](qmux/pending-work.md) | Gaps in the vendored library — now including the four the asynchronous layer had to work around — what the crates deliberately omit, what those gaps cost in copies, and the design decisions left open. |
| [`qmux/invariants.md`](qmux/invariants.md) | The properties the QMux suite pins, including four enforced by the compiler rather than at run time, and the behavioural claims the asynchronous layer's tests hold. |

Behind a default-on `io` feature, `ngnet-qmux` drives a connection over a byte stream the caller
supplies, with the byte stream and the clock as seams a caller implements and a ready-made pair
for tokio behind an off-by-default feature. `--no-default-features` returns it to the sans-I/O
state machine, with one dependency and no asynchrony. There is no endpoint layer and no driver
task, deliberately: a QMux connection owns one byte stream and shares it with nothing, so there
is nothing to demultiplex. Establishing that stream — connecting, listening, accepting, and any
TLS on it — stays with the caller.

There are no benchmarks, and nothing has been tested against another QMux implementation.

## HTTP/3 over QMux — [`qmux-h3/`](qmux-h3/)

`ngnet-qmux-h3` implements `ngnet-h3`'s transport trait over `ngnet-qmux`'s asynchronous layer,
so HTTP/3 runs over TCP, a unix socket or a TLS session. That is the QMux draft's own stated
motivation, and this crate is the only place the two families meet.

- [`qmux-h3/design.md`](qmux-h3/design.md) — why the connection is shared rather than owned
  outright, the pump that keeps the first request from deadlocking, why nothing here may park,
  and how a close the HTTP/3 driver will never poll again reaches the peer anyway.
- [`qmux-h3/pending-work.md`](qmux-h3/pending-work.md) — what is missing, starting with the fact
  that there is nothing to interoperate with.
- [`qmux-h3/invariants.md`](qmux-h3/invariants.md) — what its suites pin, and what nothing
  currently enforces.

## HTTP/3 over QUIC

`ngnet-quic-h3` joins the two families: HTTP/3 running on this workspace's own QUIC stack. It
is the only crate that depends on both, which is deliberate and enforced.

- [`quic-h3/design.md`](quic-h3/design.md) — why the connection is owned by the transport
  adapter, the pump that keeps the handshake from deadlocking, and why a stream's close needs
  a batch of its own.
- [`quic-h3/pending-work.md`](quic-h3/pending-work.md) — what is missing, including exactly
  what interoperability has and has not established.
- [`quic-h3/invariants.md`](quic-h3/invariants.md) — the structural claims its suite makes.

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
| [`ci.md`](ci.md) | Every check CI runs, across all four families and the axum integration, and the ones it deliberately does not. |

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
`ngnet-h2`. Server-side, h2c and tokio only, and unpublished for now. The transport is not
among those limits: the server is generic over a `Listener`, with TCP and Unix-domain
implementations shipped and third-party ones supported.

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
