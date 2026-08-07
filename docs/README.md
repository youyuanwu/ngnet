# Documentation

Notes that outlive any one change. API documentation lives with the code (`cargo doc`);
what is here is the reasoning a reader cannot recover from the source.

There are two crate families, built the same way and independent of each other, so each
keeps its own notes. Anything genuinely shared sits at this level.

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
| [`h3/design.md`](h3/design.md) | Why the HTTP/3 crates are shaped the way they are, and the five places nghttp3 differs from nghttp2 in ways that are easy to miss. |
| [`h3/pending-work.md`](h3/pending-work.md) | Known gaps and deferred decisions, with what would settle each. |
| [`h3/invariants.md`](h3/invariants.md) | The properties the `ngnet-h3` suite pins, and where each is enforced. |

There are no HTTP/3 benchmarks yet.

## Shared

| Document | What it covers |
| --- | --- |
| [`ci.md`](ci.md) | Every check CI runs, for both families, and the ones it deliberately does not. |

## Orientation

**HTTP/2.** `ngnet-h2-sys` builds libnghttp2 from the vendored submodule and generates raw
bindings. `ngnet-h2` wraps it twice: a sans-I/O state machine that performs no I/O at all,
and — behind the default-on `http` feature — an asynchronous HTTP/2 API over that state
machine. `ngnet-h2-tests` is unpublished and exists so the wrapper itself can stay free of
dev-dependencies while still being driven against real runtimes and real sockets.

Cleartext (h2c) only for HTTP/2. TLS, ALPN, server push and stream priorities are out of
scope there, and that is a scope decision rather than a to-do.

**HTTP/3.** `ngnet-h3-sys` builds libnghttp3 and generates raw bindings; `ngnet-h3` is a
safe sans-I/O wrapper over it, and `ngnet-h3-tests` drives that wrapper over a real quinn
connection. There is deliberately no asynchronous layer over `ngnet-h3` — it is the core
such a layer would be built on — and no QUIC or TLS implementation is bundled, because
nghttp3 depends on neither. Server push is absent because nghttp3 does not implement it.

Within a family's own documents, "the crate" means that family's wrapper: `ngnet-h2` under
[`h2/`](h2/), `ngnet-h3` under [`h3/`](h3/).

## Toolchain

There is no declared MSRV, and no crate sets `rust-version`. `rust-toolchain.toml` names the
one toolchain contributors and CI both build with; rustup reads it automatically. The
workspace could not honour a single minimum in any case — the benchmark crate's Criterion
dependency and compio's buffer crate both need newer compilers than the rest — so a declared
minimum would have described some crates rather than the workspace.
