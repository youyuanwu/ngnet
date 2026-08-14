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
| **An ownership-taking write** | `Connection::write` copies, because the transport holds what it is given until acknowledgement. A `write_owned(Bytes)` alongside it would let a caller hand over instead — see [Sent stream data is copied](#sent-stream-data-is-copied). |
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

## Sent stream data is copied

ngtcp2 does not copy the stream data it accepts. It keeps the caller's pointer so it can
retransmit, and requires the bytes stay intact "until `acked_stream_data_offset` indicates
that they are acknowledged by a remote endpoint or the stream is closed"
(`ngtcp2.h:5244-5248`).

`Conn::write_stream` takes an ordinary `&[u8]` that the caller may reuse the moment the call
returns, so the crate copies the accepted portion and holds it until the acknowledgement
arrives. `Conn::retained_bytes` reports how much is held.

That is one copy of every byte sent. `ngnet-h3` avoids the equivalent copy by making callers
hand over ownership through its `BodySource`; the same could be done here, and would be
worth doing if this crate is ever used somewhere the copy matters.

**What would settle it:** an ownership-taking overload — `write_stream_owned(Bytes)` or
similar — alongside the copying one, so the zero-copy path is available without making the
ordinary path require a paragraph of documentation to use safely.

## Received packets are copied once more than they need to be

The safe TLS seam protects payloads **in place**: `PacketKey::seal` and `PacketKey::open` take
one `&mut [u8]` and work on it. That shape is forced on the send side. ngtcp2 encrypts in place
— `cc->encrypt(payload, ..., payload, ...)` (`ngtcp2_ppe.c:142`) passes the same pointer as
source and destination — and two overlapping slices, one shared and one mutable, cannot be
formed in safe Rust at all. So the seam takes one buffer, and on the send path the bridge
detects the aliasing and does nothing extra: sending is genuinely zero-copy, and
`tests/zero_alloc.rs` pins that it allocates nothing.

Receiving is not. ngtcp2 decrypts **out of place**, into a buffer it owns —
`decrypt_pkt(conn->crypto.decrypt_buf.base, ..., payload, ...)`
(`ngtcp2_conn.c:6846`, `:9457`) — so source and destination differ. The bridge therefore copies
the ciphertext into the destination and opens it there, where the crypto helper it replaced
handed both pointers to `EVP_DecryptUpdate` and read one while writing the other in a single
pass. That is one extra pass over the payload of every packet received. No allocation, but a
real memcpy of up to a full datagram.

The in-place shape was applied uniformly because *encrypt* requires it. Nothing checked whether
*decrypt* did, and it does not: ngtcp2's core never aliases the two buffers, even though its
documented contract permits a backend to be handed aliasing pointers (`ngtcp2.h:2846`).

**What would settle it:** give `open` separate `&[u8]` and `&mut [u8]` parameters. That is
expressible in safe Rust precisely because the two never overlap here, and it removes the copy.
The cost is that the seam then has two shapes rather than one, and a backend implementing `open`
has to be told that its inputs may not alias — so the documentation has to carry what the type
system currently carries for free. Worth doing if this crate is ever used somewhere the copy
matters; worth measuring first, since a memcpy of 1200 bytes against an AEAD over the same 1200
bytes may not show up at all.

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
