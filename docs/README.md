# Documentation

Notes that outlive any one change. API documentation lives with the code (`cargo doc`);
what is here is the reasoning a reader cannot recover from the source.

| Document | What it covers |
| --- | --- |
| [`design.md`](design.md) | How the crates fit together, the mechanisms the async layer rests on, and why each was chosen over the alternative that looks simpler. |
| [`pending-work.md`](pending-work.md) | Known gaps and deferred decisions, each with the evidence that produced it and what would settle it. |
| [`invariants.md`](invariants.md) | The properties the test suite pins rather than merely exercises, and where each is enforced. |

## Orientation

`nghttp2-sys` builds libnghttp2 from the vendored submodule and generates raw bindings.
`nghttp2` wraps it twice: a sans-I/O state machine that performs no I/O at all, and — behind
the default-on `http` feature — an asynchronous HTTP/2 API over that state machine.
`nghttp2-tests` is unpublished and exists so the wrapper itself can stay free of
dev-dependencies while still being driven against real runtimes and real sockets.

Cleartext (h2c) only, throughout. TLS, ALPN, server push, stream priorities and HTTP/3 are
all out of scope, and that is a scope decision rather than a to-do.
