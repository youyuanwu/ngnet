# Documentation

Notes that outlive any one change. API documentation lives with the code (`cargo doc`);
what is here is the reasoning a reader cannot recover from the source.

| Document | What it covers |
| --- | --- |
| [`design.md`](design.md) | How the crates fit together, the mechanisms the async layer rests on, and why each was chosen over the alternative that looks simpler. |
| [`pending-work.md`](pending-work.md) | Known gaps and deferred decisions, each with the evidence that produced it and what would settle it. |
| [`invariants.md`](invariants.md) | The properties the test suite pins rather than merely exercises, and where each is enforced. |
| [`benchmarks.md`](benchmarks.md) | How the `ngnet-h2` vs hyper HTTP/2 benchmarks are run, what their numbers do and do not mean, and which protocol settings are matched between the two stacks. |

## Orientation

There are two families, built the same way and independent of each other.

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

Statements below that name "the crate" without qualification predate the second family and
describe `ngnet-h2`; the invariants document says which crate each rule belongs to.
