# QUIC pending work

Known gaps and deferred decisions, each with the evidence that produced it and what would
settle it.

## The `QuicConnection` adapter

`ngnet-h3` defines a `QuicConnection` trait (`crates/ngnet-h3/src/http/quic.rs`) that its
async layer drives, with one implementation today over quinn in `ngnet-h3-tests`. An
`ngnet-quic` implementation of it would make ngtcp2 a second HTTP/3 backend, and is the
obvious next piece.

It was deliberately excluded from this work. The wrapper is large enough on its own, and
building the adapter at the same time would have meant designing the wrapper's API around a
consumer that did not exist yet — which tends to produce an API that fits exactly one caller.

**What would settle it:** writing the adapter. Two things to expect. The trait is `poll`-based
and `ngnet-quic` is not, so the adapter owns the event loop and the timer; and the trait was
shaped against a survey of four QUIC libraries including ngtcp2, so the shapes should meet,
but "should" is doing real work in that sentence until someone tries.

The API was sanity-checked against the trait during this work and no blocking mismatch was
found. That is a weaker claim than it sounds — nobody has written the adapter.

## Deliberately unimplemented QUIC features

Each is a real feature of the protocol, excluded to keep this work finishable rather than
because it is unwanted.

| Feature | What it would need |
| --- | --- |
| **0-RTT / session resumption** | Ticket storage across connections, an anti-replay story, and `SSL_SESSION` plumbing. The OpenSSL symbols are already reachable. |
| **Unreliable DATAGRAM frames** | `ngtcp2_conn_writev_datagram` is already wrapped in `ffi.rs` and unused; the work is the safe API and the flow-control interaction. |
| **Connection migration and path validation** | `path.user_data` can never be reclaimed mid-connection (`ngtcp2.h:2158-2168`), so a migrating connection needs a different ownership story for path data than the static one used now. |
| **Explicit key update** | `ngtcp2_conn_initiate_key_update`, plus deciding when a safe API should trigger one. |
| **The `MORE` coalescing write path** | `NGTCP2_WRITE_STREAM_FLAG_MORE` lets several stream writes share one packet, but requires that `conn`, `path`, `pi`, `dest`, `destlen` and `ts` be **byte-identical** across every call in a run, and forbids almost the whole API in between (`ngtcp2.h:5288-5312`). That is expressible safely only behind a guard type holding all of them, in the shape of `ngnet-h3`'s `SendGuard`. Nothing needs it yet. |
| **Retry and address validation tokens** | `accept.rs` exposes the primitives; no token issuance or validation policy is built on top. |

## Inherited behaviours worth knowing about

Two things ngtcp2 does that this crate accepts rather than works around. Both were found
during research and are recorded so they are decisions rather than surprises.

**The crypto helper swallows two error codes.**
`ngtcp2_crypto_recv_crypto_data_cb` catches `-10001` (`WANT_X509_LOOKUP`) and `-10002`
(`WANT_CLIENT_HELLO_CB`) and returns 0 (`deps/ngtcp2/crypto/shared.c:1789-1798`). Since this
crate uses the helper's callbacks directly rather than trampolining through Rust, it inherits
that. It is benign while asynchronous certificate lookup and the client-hello callback are out
of scope — which they are — but it would become a silent stall the moment either is used.
Worth revisiting alongside 0-RTT.

**Client and server set local transport parameters at different times.** The client does it in
`client_initial_cb` (`crypto/shared.c:1706`); the server from `derive_and_install_tx_key` at
HANDSHAKE level (`crypto/shared.c:502-507`). Any future API that assumes a single symmetric
"configure, then start" point will be wrong for one of the two roles.

## Only one TLS backend

The seam admits others — wolfSSL, GnuTLS, BoringSSL and Picotls all have ngtcp2 crypto
helpers — and adding one should not require touching anything outside a new `tls_*.rs` module
and a feature in `ngnet-quic-sys`.

Nobody has tried, so "should" is unverified. The seam was designed against one
implementation, which is the classic way to end up with an abstraction that fits exactly that
one. The second backend is the test of it.

Note also that ngtcp2 selects a crypto backend by **symbol probing**, not by version: the
presence of `SSL_provide_quic_data` selects quictls, `SSL_set_quic_tls_cbs` selects the
OpenSSL 3.5 helper. Only one OpenSSL-family helper can be built at a time.

## `ngtcp2_crypto_ossl_free` is never called

A deliberate, bounded leak of the static `EVP_*` objects `ngtcp2_crypto_ossl_init` prefetches.
They are process-global with no reference counting (`deps/ngtcp2/crypto/ossl/ossl.c:49-60`,
`:62`, `:82`), so calling `_free` from any per-context destructor — as the ngtcp2 examples do
— would free objects another context is still using.

**What would settle it:** an upstream refcount, or a process-exit hook that could prove
nothing is live. Neither is worth building for a handful of objects.

## The `ubuntu-26.04` CI runner is a preview image

Carried over from the `ngnet-quic-sys` work and still true. ngtcp2's OpenSSL helper needs
OpenSSL ≥ 3.5, `ubuntu-latest` is still 24.04 with 3.0.13, and 26.04 is the first image with
3.5. The pin is load-bearing and a step asserts the OpenSSL version before the build, so a
runner change fails with that message rather than somewhere inside CMake's symbol probing.

If the preview image becomes unreliable, split the OpenSSL-dependent steps into their own job
rather than lowering the requirement — nothing older has 3.5.

## Things not measured

There are no QUIC benchmarks. The crate has never been run against another QUIC
implementation, only against itself; interoperability with quinn, quiche, msquic or a browser
is unverified. Neither is a gap in correctness so much as an absence of evidence, but the
distinction is worth keeping visible.
