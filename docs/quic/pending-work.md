# QUIC pending work

Known gaps and deferred decisions, each with the evidence that produced it and what would
settle it.

## Things the endpoint layer does not do

Deliberate omissions in the asynchronous layer, each excluded to keep the work finishable
rather than because it is unwanted.

| Gap | What it would need |
| --- | --- |
| **Explicit congestion notification** | `ngtcp2_pkt_info` carries an ECN byte and this layer always passes zero. Reading it back needs socket ancillary data, which the `AsyncUdpSocket` seam would have to expose — a real widening of the trait for a signal nothing here acts on yet. |
| **Datagram batching** | `sendmmsg`, `recvmmsg` and segmentation offload. Throughput work, optional even in ngtcp2's own examples, and invisible until an endpoint is carrying enough traffic for syscall overhead to matter. |
| **More than one socket per endpoint** | A scaling concern rather than a capability one: nothing in the API would change, and a caller who needs it today can run several endpoints. |
| **An ownership-taking write at the endpoint layer** | The async `Connection::write` (`src/endpoint/connection.rs`) copies, because it takes an ordinary `&[u8]` and queues a `Command::Write` holding a `to_vec` of it. The sans-I/O core now has `Conn::write_stream_owned`, which retains an `OwnedBytes` without a copy, but the async layer does not yet forward to it. What would settle it: a `write_owned(OwnedBytes)` on the async `Connection` that hands the buffer to `write_stream_owned` instead of copying it into the command queue. |
| **Backpressure on the write queue** | A caller may queue writes faster than the connection drains them, and nothing bounds that queue. The transport's own flow control bounds what is *in flight*, not what is waiting. Not reachable by a peer — the queue is driven by the local application — but a `write` that resolved only once the transport had accepted the bytes would be the honest shape. |
| **A bounded accept backlog** | Connections waiting to be accepted accumulate in an unbounded queue. Address validation bounds it in practice on any endpoint that enables it, since an unvalidated peer never reaches the point of creating one; an endpoint without validation has no such bound. |
| **NEW_TOKEN for returning clients** | The endpoint validates Retry tokens but never issues the regular tokens that would let a client skip validation on its next connection. `accept.rs` classifies them already. |

## The `QuicConnection` adapter — done

`ngnet-h3` defines a `QuicConnection` trait (`crates/ngnet-h3/src/http/quic.rs`) that its
async layer drives. It now has three implementations: an in-memory one for tests, one over
quinn in `ngnet-h3-tests`, and one over this crate in `ngnet-quic-h3`. HTTP/3 runs on ngtcp2.

The shapes did meet, as the trait's design against a survey of four QUIC libraries suggested
they would, but not without two things that had to be found by running it rather than by
reading. Both are recorded in `docs/quic-h3/design.md`: the connection has to be *pumped*
from every entry point rather than only where datagrams seem to belong, and a stream's close
has to be delivered in a different batch from that stream's last bytes.

Building the adapter also required the endpoint to give up per-connection state to a caller
who drives it. That is the *detached connection* described in `docs/quic/design.md`.

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

## Inherited behaviours worth knowing about

Two things ngtcp2 does that this crate accepts rather than works around. Both were found
during research and are recorded so they are decisions rather than surprises.

**The crypto helper swallows two error codes.**
`ngtcp2_crypto_recv_crypto_data_cb` catches `-10001` (`WANT_X509_LOOKUP`) and `-10002`
(`WANT_CLIENT_HELLO_CB`) and returns 0 (`crates/ngnet-quic-sys/vendor/ngtcp2/crypto/shared.c:1789-1798`). Since this
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
helpers, and a Rust stack needs no helper at all — and adding one should not require touching
anything outside a new module and, if it binds a C library, a feature in `ngnet-quic-sys`.

"Should" is now partly verified. There are two implementations: the OpenSSL backend, and the
dependency-free one in `crates/ngnet-quic/tests/safe_backend.rs` that carries real connections
under `forbid(unsafe_code)`. The second was written against the seam rather than alongside it,
and it found three things the first never had to notice — that handshake data arrives as a
stream rather than as messages, that a handshake must be a round trip, and that a server's
reply belongs at the Initial level.

What is still unverified is a *production* second backend. rustls is the obvious candidate and
`docs/quic/design.md` sets out how it would map, including the one real gap: rustls never
surfaces a header protection mask, only applies it, so such a backend would have to reconstruct
header protection from the negotiated secret.

Note also that ngtcp2 selects a crypto backend by **symbol probing**, not by version: the
presence of `SSL_provide_quic_data` selects quictls, `SSL_set_quic_tls_cbs` selects the
OpenSSL 3.5 helper. Only one OpenSSL-family helper can be built at a time.

## `ngtcp2_crypto_ossl_free` is never called

A deliberate, bounded leak of the static `EVP_*` objects `ngtcp2_crypto_ossl_init` prefetches.
They are process-global with no reference counting (`crates/ngnet-quic-sys/vendor/ngtcp2/crypto/ossl/ossl.c:49-60`,
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

There are no QUIC benchmarks.

Interoperability is no longer wholly unverified, but it is worth being exact about what was
established. `tests/ngnet-quic-h3-tests/tests/interop.rs` runs this crate against **quinn**,
in both roles: a bare QUIC handshake with no HTTP/3 involved, HTTP/3 requests in each
direction, and a 512 KiB payload crossing both ways byte for byte. A negative test confirms
an untrusted certificate is refused, so the positive results are not an artefact of
verification being switched off.

That is evidence against one other implementation. It is not evidence about the protocol in
general: quiche, msquic, picoquic and browsers remain untried, and so do the conditions a
loopback socket does not produce — real loss, reordering, path changes, and peers that
negotiate different transport parameters. The QUIC Interop Runner exists for exactly this and
running against it would be the next real step.

## Copies and allocations that remain, and why each is forced

The copy/allocation audit removed everything that could be argued away from the source — the
receive-path copy the safe TLS seam used to make, the per-pass connection-index vectors, the
attached receive copy, the completing send pass's allocation, the HTTP/3 slice vector and its
production scratch, and the sent-stream-data copy on the ownership-taking path — and pinned
each removal with an allocation-counting or structural test. What is left below could not be
argued away: each is forced by a lifetime the source does not let us shorten. None is a defect.
A count that dropped to zero on any of these would mean a byte was lost, not saved.

| What remains | Why it is forced, and what would settle it |
| --- | --- |
| **The detached receive path copies once** | A datagram for a connection whose owner has *detached* it is copied out of the endpoint's reusable receive buffer (the detached arm of `deliver` in `src/endpoint/driver.rs`) before being queued. The borrow of that buffer cannot outlive the pass — the next `poll_recv` overwrites it — and the owner may not collect until a later pass, so the bytes have to be owned. This is the only copy left on the receive path once the attached path stopped copying. **What would settle it:** draining a detached connection within the same pass that received the datagram, so the borrow never crosses a pass — a different ownership boundary between the endpoint and a detached owner than the queue imposes today. Pinned at exactly one allocation per datagram by `a_receive_pass_to_a_detached_connection_allocates_one_buffer_per_datagram` in `tests/zero_alloc.rs`. |
| **A retained send datagram is copied once** | The driver writes each datagram into one reusable send buffer and sends straight from it (`flush`, `next_datagram`, `write_stream` in `src/endpoint/driver.rs`), so a datagram the socket accepts is not copied. Only a datagram the socket **refuses** — one held as `tracked.pending` until a later pass — is copied into a buffer of its own, because a later write in the same pass overwrites the shared buffer. **What would settle it:** nothing removes it — a datagram retained past the buffer's reuse must be owned; a per-connection buffer would relocate the allocation, not remove it. Pinned by `a_completing_send_pass_copies_a_datagram_only_when_the_socket_refuses_it` and `a_core_produced_datagram_costs_nothing_to_send_and_one_to_retain`, which assert a datagram the socket accepts is sent without a copy and a refused one costs exactly one. |
| **The HTTP/3 queue allocates once per datagram** | `ngnet-quic-h3` now produces each datagram directly into the buffer it hands to the detached connection's queue (`transmit.rs`, `pump.rs`, `connection.rs`), so the copy out of a separate production scratch is gone. The one owned allocation that remains is forced because the queue takes ownership (`detached.send`, `handle.rs:439-442`) and may hold the datagram across passes until the socket drains it. **What would settle it:** a queue that borrows rather than owns — not possible while the datagram must outlive the pass that produced it; a shared buffer pool would relocate the allocation, not remove it. Pinned at most one per datagram by `tests/ngnet-quic-h3-tests/tests/zero_alloc.rs`. |

## The HTTP/3 layer's borrowing write still copies, and `RETAINS_BUFFERS` depends on it

`ngnet-quic-h3`'s connection writes stream data through `Conn::write_stream_vectored`, the
borrowing path, which copies every accepted byte into the transport's own retention. Because
that copy exists, the layer's buffers are the layer's own again the instant a write returns,
and `NgtcpConnection` sets `RETAINS_BUFFERS = false`
(`crates/ngnet-quic-h3/src/connection.rs`) — release is reported on write, not on
acknowledgement. That constant is correct **only because** of the copy: were the transport
holding the layer's bytes until acknowledgement, reporting release on write would free a buffer
QUIC still points at for retransmission, and reporting it on acknowledgement while the copy
exists would hold every in-flight byte twice.

The crate now has an ownership-taking write — `Conn::write_stream_owned`, taking an
`OwnedBytes` — that retains without a copy. The HTTP/3 layer does not use it.

**What would settle it:** routing the layer's body writes through `write_stream_owned`, at
which point the transport retains the layer's buffers rather than copying them, release must be
reported on acknowledgement rather than on write, and `RETAINS_BUFFERS` becomes `true`. That is
a larger change to the layer's buffer accounting than this audit took on, and nothing needs it
yet.

## An allocation count cannot see a copy into storage already allocated

Every removal in this audit is pinned by a test, but not all by the same kind of test. A
counting allocator sees an allocation appear or fail to appear; it cannot see a
`copy_from_slice` into a buffer that was already allocated, because no allocation happens. So
the copy the safe TLS seam used to make before decrypting — ciphertext moved into an
already-sized destination — could never have been caught by counting, however carefully the
region was armed.

That is why the decrypt bridge is pinned by a **structural source test** instead:
`the_decrypt_bridge_copies_nothing` in `tests/invariants.rs` reads a named span of
`tls_bridge.rs` and fails if a copy construct reappears in it. The two kinds of test are
complementary: the allocation-counting tests guard allocations, and the structural tests guard
copies into storage that is already there.

**What would settle it:** nothing changes the limitation — it is inherent to counting
allocations. Where a copy would not allocate, a structural test over a named region is the
mechanism, as it is for the decrypt bridge.

## `ngnet-quic-h3-tests::exchange` fails occasionally under load

Observed once in four `cargo test --workspace --all-features` runs, and not reproduced in nine
attempts afterwards — six of the suite alone, three of the whole workspace. The suite binds real
sockets on `127.0.0.1:0`, so it is not a port collision; the ephemeral port is chosen by the
kernel and is unique per test.

Recorded rather than fixed because it is worth being explicit that this is known. A test that
fails one run in four and passes the next nine trains everyone who sees it to press retry, and
the first genuine failure it catches will be dismissed the same way.

It predates the safe TLS seam: `tests/ngnet-quic-h3-tests/tests/exchange.rs` was last modified by
the commit that introduced it, and the seam work changed nothing under
`tests/ngnet-quic-h3-tests/` at all.

**What would settle it:** run it under contention deliberately — the whole workspace, repeatedly,
on a loaded machine — with the failure captured rather than summarised, so the failing assertion
is known. The likely candidates are a timeout that is generous on an idle machine and not on a
busy one, or a `pump`-style relay bounded by rounds rather than by progress.

## Mutual TLS is not implemented

`Verify::RequireClientCertificate` exists and returns an error. It is there so that asking
for mutual TLS fails loudly rather than silently producing a server that accepts anyone —
which is what `Verify::Peer` on a server means, since demanding a client certificate would
reject every ordinary QUIC client.

**What would settle it:** `SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT` on the server
context, a way to configure client trust anchors separately from server ones, and a way for
the application to see the peer certificate.

## Platform portability of the address conversion

`src/path.rs` hand-rolls `sockaddr_in` and `sockaddr_in6` rather than taking a `libc`
dependency, and selects `AF_INET6` per platform. It does **not** carry the `sa_len` byte that
the BSDs and macOS place first in those structures, so on those targets the family would land
in the wrong byte.

The code is self-consistent — the sizes and the reported lengths agree, so nothing is
memory-unsafe — but the per-platform `AF_INET6` table implies a portability the layout does
not deliver. CI is Linux only.

**What would settle it:** a `cfg`-selected layout with the leading length byte for the BSD
family, and a CI target that would notice.

## Zero-length connection IDs

`accept::inspect` rejects a zero-length connection ID, because `ConnectionId` requires at
least `NGTCP2_MIN_CIDLEN` bytes. QUIC permits an endpoint to use a zero-length connection ID
— it is how an endpoint says "I do not need you to route by identifier".

This only affects inspecting a peer's identifiers, not issuing our own. **What would settle
it:** allowing an empty `ConnectionId` on the decode path specifically, distinct from the
generation path where a zero length would be a mistake.
