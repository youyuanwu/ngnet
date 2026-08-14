# QUIC design

Why `ngnet-quic` is shaped the way it is. What follows is the reasoning a reader cannot
recover from the source: the places where the obvious design is wrong, and why.

`ngnet-quic-sys` builds ngtcp2 from the vendored submodule and generates raw bindings.
`ngnet-quic` wraps it twice, in the same shape as the other two families: a sans-I/O state
machine that performs no I/O at all, and — behind the default-on `endpoint` feature — an
asynchronous layer that owns a UDP socket and the connections reachable through it.
`ngnet-quic-tests` is unpublished and exists so the wrapper can stay free of
dev-dependencies while still being driven through real handshakes over real sockets.

Everything from [The API in ngtcp2's documentation does not exist](#the-api-in-ngtcp2s-documentation-does-not-exist)
to [Panics abort](#panics-abort) is about the state machine. The endpoint layer's own
reasoning starts at [One driver per socket](#one-driver-per-socket).

## The API in ngtcp2's documentation does not exist

Almost every name in the ngtcp2 manual — `ngtcp2_conn_read_pkt`, `ngtcp2_conn_client_new`,
`ngtcp2_settings_default`, fifteen others — is a **function-like macro**. Each injects a
struct-version constant and forwards to a `_versioned` symbol, so that a caller compiled
against an older header keeps working when a struct gains a field
(`deps/ngtcp2/lib/includes/ngtcp2/ngtcp2.h:7295-7476`).

bindgen does not emit function-like macros. The generated bindings contain
`ngtcp2_conn_read_pkt_versioned` and no `ngtcp2_conn_read_pkt` at all.

So `src/ffi.rs` reimplements all eighteen by hand, and is the only place in the crate where
a version constant appears. That containment is not tidiness. Passing the wrong constant is
neither a compile error nor a runtime error: ngtcp2 uses it to decide how to interpret the
memory behind a pointer, so a wrong value is silent misinterpretation of a struct.
`tests/versioned_ffi.rs` pins every constant against the bindings, and an invariant asserts
none has escaped into another module.

## Validation is duplicated because the C library's own vanishes

ngtcp2 checks its settings, its transport parameters, and the presence of its mandatory
callbacks with `assert()` — about forty lines of them at the top of each connection
constructor (`deps/ngtcp2/lib/ngtcp2_conn.c:1250-1291`).

`assert()` compiles to nothing when `NDEBUG` is defined, and the `cmake` crate maps the
cargo profile onto `CMAKE_BUILD_TYPE`, so a release build of this workspace produces a
`libngtcp2.a` with `-O3 -DNDEBUG` and none of those checks in it. In exactly the builds
anyone ships, an out-of-range transport parameter is undefined behaviour rather than a
crash.

A safe API cannot rest on a safety net absent from the configuration it is used in, so
`src/validate.rs` restates the checks in Rust. CI runs `cargo test -p ngnet-quic --release`
for this reason and no other; it is the run that proves the checks hold where the C
library's no longer do.

One constant there is restated from a **private** ngtcp2 header:
`NGTCP2_DCIDTR_MAX_UNUSED_DCID_SIZE` bounds `active_connection_id_limit` but lives in
`lib/ngtcp2_dcidtr.h`, which is not installed. Restating a private constant is a real risk,
so `tests/versioned_ffi.rs` reads the value back out of the vendored source rather than
trusting the line.

## The TLS seam is safe, and what that cost

This is the highest-risk area in the crate, and it was rebuilt once the rest of it worked.

The seam was originally two `unsafe` traits. A backend had to hand ngtcp2 an untyped pointer,
fill in a foreign callback table by hand, and promise lifetimes the compiler could not check.
It is now three safe traits — `Backend`, `Session` and `Handshaking` — that traffic in byte
slices, arrays and associated types. Implementing one requires no `unsafe`, names no raw
pointer, and cannot mention ngtcp2 at all. `crates/ngnet-quic/tests/safe_backend.rs` is a
complete backend in a file that `forbid`s unsafe code, carrying two real connections through a
handshake; it exists to make that claim checkable rather than aspirational.

The `unsafe` did not disappear. It moved into `src/tls_bridge.rs`, where it is written once,
generically over the session type, instead of once per backend. That is the trade: the
allowance list in `lib.rs` gained `tls_bridge` and lost `tls`.

### The seam changed shape twice, and both are breaking

The copy/allocation audit made two changes to the seam's public surface. Both were taken
deliberately, on the owner's decision that a one-commit-old seam with two in-repository
backends is the cheapest moment to change it — the cost only grows once a third party depends
on it.

- **`PacketKey::open` now takes the destination and the ciphertext as separate slices** —
  `open(&mut dest, ciphertext, nonce, aad)` rather than one `&mut [u8]` unprotected in place.
  ngtcp2's core always decrypts a received packet into a buffer distinct from the packet
  itself (`ngtcp2_conn.c:6846`, `:9457`), never aliasing the two, even though the header
  permits it (`ngtcp2.h:2846`). With the two slices separate, this crate's bridge decrypts
  straight across and no longer copies the ciphertext into the destination first — the copy
  removed on the receive path. `seal` is unchanged: ngtcp2 encrypts in place
  (`ngtcp2_ppe.c:142`), and two overlapping slices, one shared and one mutable, cannot be
  formed in safe Rust, so sealing keeps its single `&mut [u8]`. A backend whose own primitive
  decrypts in place must now copy `ciphertext` into `dest` itself; the obligation the type
  system used to carry for free is documentation the backend author has to read. A structural
  test, `the_decrypt_bridge_copies_nothing`, pins that this crate's bridge does not copy.
- **A level's initialisation vector is a fixed-capacity `Iv`, not a `Vec<u8>`.** `Iv` holds up
  to `validate::MAX_IV_LEN` (64) bytes inline, with fallible construction that refuses anything
  longer. It replaces the `Vec<u8>` in `DirectionalKeys::iv` and `RotatedKeys::{rx_iv, tx_iv}`,
  so a level's IV no longer reaches the heap and a length ngtcp2 could not handle — the bounds
  guard a fixed stack buffer whose overrun release builds do not catch — has no representation
  rather than merely being rejected. It derefs to `[u8]`, so a reader sees the bytes and not the
  padding behind them. `compat_surface.rs` names it and asserts it has no heap representation.

Both appear in the pinned-surface test (`tests/compat_surface.rs`); a caller or backend that
implemented the old shapes will not compile against the new ones.

### Three C objects became two, and the cycle went away

The old design had an `SSL`, an `ngtcp2_crypto_ossl_ctx` wrapping it, and an
`ngtcp2_crypto_conn_ref` that OpenSSL held as application data and that pointed back at the
`ngtcp2_conn`. They referred to each other in a cycle and had to be destroyed in exactly one
order, because `SSL_free` releases outstanding CRYPTO records by calling back into the helper,
which followed the reference to the connection and dereferenced it. Every departure from the
order was a use-after-free rather than a leak.

`SSL_set_quic_tls_cbs` takes a callback argument. The backend passes its own state through it,
so OpenSSL holds nothing belonging to ngtcp2, and the reference disappears. What remains is
ordinary ownership — the engine outlives the `SSL` that reads it, and the helper context
outlives both — and `OsslSession` still implements `Drop` by hand to say so, because relying on
field declaration order would make a correctness requirement invisible to a later refactor.

The old seam also needed a `NativeTlsHandle` newtype, whose entire purpose was to stop a
backend passing the `SSL *` where the `ngtcp2_crypto_ossl_ctx *` was wanted — a mistake that
compiled cleanly and corrupted memory at run time. There is now nothing to get wrong: the
connection stores a pointer to the session it owns, and a backend never sees or supplies it.

### Some of the seam had to be synchronous, and finding out cost a phase

The obvious design has a session *report* what happened and the crate apply it afterwards. It
means a backend cannot get the ordering wrong, which is worth a great deal. It also cannot
serve a server, for a reason that only appears against another implementation:

- A server's own transport parameters name the version it settled on, which ngtcp2 fills in
  only while decoding the peer's (`ngtcp2_conn.c:11732-11737`). Encoded earlier, the field is
  zero, which the peer rejects as malformed (`ngtcp2_transport_params.c:743`).
- They also carry the server's connection identifier, filled only inside
  `ngtcp2_conn_install_tx_handshake_key` (`:11132`).
- Both must land before the TLS stack writes the message carrying them, which it does without
  returning from the call that delivered the peer's.

So there is no moment between those steps at which the crate is in control. `Handshaking` is
the answer: a session is *lent* the connection for the length of one call, with four operations
that take effect before they return — take the peer's parameters, produce this endpoint's,
install a level's keys, submit handshake bytes. Everything with no such constraint stays on the
queue and keeps its ordering guarantee.

The fourth operation is there for a separate reason. A TLS stack must be told how much of what
it offered was taken before it returns, so queuing the bytes means answering before the answer
exists — and the only answer available in advance is a claim that all of it was accepted.

The capability is borrowed rather than owned, so a backend that keeps it does not compile. A
`compile_fail` doctest on the trait asserts exactly that.

### `ngtcp2_crypto_ossl_free` is deliberately never called

`ngtcp2_crypto_ossl_init` prefetches static `EVP_*` objects into process globals with no
reference counting (`ossl.c:49-60`, `:62`, `:82`). The ngtcp2 examples pair it with a
per-context destructor — which means that with two TLS contexts, destroying the second frees
objects the first is still using.

So `init` runs once behind a `Once` and `_free` is never called. A bounded one-off leak of a
handful of static objects is the right trade against corrupting a live connection.

### Why the seam is a trait at all

Each TLS backend compiles **its own copy** of ngtcp2's shared crypto code, with
backend-specific implementations behind identically-named symbols — `ngtcp2_crypto_ctx_tls`
exists separately in `ossl.c`, `wolfssl.c` and `gnutls.c`. And those symbols do not always
exist: `crates/ngnet-quic-sys/wrapper.h:10-18` includes the crypto headers only when a backend
feature is on, so with `--no-default-features` there is no `ngtcp2_crypto_*` symbol in the
bindings at all. The seam is what lets the crate build with no TLS stack, exposing the
interface and nothing behind it.

### What a rustls backend would have to do

The seam was shaped so one is possible; none is written. `rustls::quic` maps closely but not
exactly:

- `PacketKey` is close but no longer identical — rustls protects in place, with
  `encrypt_in_place` and `decrypt_in_place`. `seal` maps directly. `open` now takes the
  destination and the ciphertext as separate slices (see above), so a rustls backend's
  `decrypt_in_place` would have to copy `ciphertext` into `dest` before unprotecting it —
  which is what this crate's own bridge avoids by giving ngtcp2's already-separate buffers
  straight to the key. rustls still exposes `confidentiality_limit` and `integrity_limit`
  under those names.
- `HeaderKey` is **not** a match, and this is the one real gap. ngtcp2 asks for a five-byte
  mask from a sample; rustls's `HeaderProtectionKey` only ever applies protection in place and
  never surfaces the mask. A rustls backend would have to reconstruct header protection from
  the negotiated secret rather than delegating to it. That is a capability gap, not a
  representational one.
- `KeyChange` and `Keys` correspond to what `Handshaking::install_keys` takes.

## Entropy travels through `rand_ctx`, not the callback bridge

Every other ngtcp2 callback reaches Rust state through a boxed slot registered as
`user_data`, in the pattern `ngnet-h3` established. The `rand` callback cannot.

It receives neither the connection nor `user_data` — its only parameter besides the output
buffer is a `const ngtcp2_rand_ctx *` (`ngtcp2.h:3112-3113`) — and it fires **during**
`ngtcp2_conn_client_new`, before `*pconn` is assigned and before `user_data` is stored
(`ngtcp2_conn.c:1357,1360,1582`, with `user_data` set at `:1592`). At the moment it first
runs there is nothing to recover state from.

So the entropy source is boxed by the connection and reached through
`settings.rand_ctx.native_handle`. `get_new_connection_id` needed a second route, having no
`rand_ctx` parameter of its own; both now reach the same per-connection source, and a test
proves it by checking the stateless-reset token continues the same byte sequence as the
identifier rather than restarting.

There is deliberately **no built-in generator**. The crate holds itself to one non-optional
dependency, so it has no RNG to reach for, and one seeded from a clock would produce
predictable connection identifiers — a real weakness, since an observer who can guess the
identifiers an endpoint will issue can correlate or interfere with its connections.

## Sent stream data is copied unless ownership is handed over

ngtcp2 does not copy what `writev_stream` accepts — it keeps the caller's pointer so it can
retransmit, and requires the bytes stay intact "until `acked_stream_data_offset` indicates
that they are acknowledged by a remote endpoint or the stream is closed"
(`ngtcp2.h:5244-5248`).

A safe API cannot pass a caller's borrow through and return. The caller may free that buffer
the instant the call ends, and a later retransmission would read freed memory — a
use-after-free reachable from entirely safe code, with nothing in the signature to warn
anyone.

So `src/retain.rs` keeps a copy of every accepted byte and hands ngtcp2 a pointer into that,
releasing it when the acknowledgement arrives or the stream closes. Each accepted write is
retained at a fixed address — a borrowed write in its own `Box<[u8]>`, an owned one behind an
`Arc` — because a growing `Vec` would reallocate and move bytes ngtcp2 still points at.

The cost of the borrowing write is one copy of everything sent, held until acknowledged. That
copy is the price of an ordinary `&[u8]` parameter whose safety does not depend on the caller
having read a paragraph of documentation, and `Conn::write_stream` and `write_stream_vectored`
keep it. `write_stream_vectored` now takes its ranges as `&[IoSlice]` rather than `&[&[u8]]`, so
a vectored source — the HTTP/3 layer, whose body writes are already `IoSlice`s — passes them
through without first collecting them into a temporary vector; the bytes still join *into* the
single retained copy, so the byte count is unchanged.

### The owned write hands the buffer over instead of copying

`Conn::write_stream_owned` sits beside the borrowing writes for a caller that can give up its
buffer. It takes an `OwnedBytes` — a reference-counted handle this crate defines, since the
crate acquires no `bytes` dependency — and retains it by keeping the handle alive rather than
by copying: ngtcp2 is handed a pointer straight into the `Arc`, whose address is fixed for as
long as any handle survives. `OwnedBytes::from_owner` lets a caller who already holds a
reference-counted buffer (a `bytes::Bytes`, a memory map, its own type) hand it over without a
copy of its own.

ngtcp2 routinely accepts a prefix and leaves the rest, so `write_stream_owned` returns an
`OwnedWrite`: the outcome, plus `unsent`, the unaccepted suffix handed back as a second handle
into the *same* allocation via `OwnedBytes::split_to`. The accepted prefix stays retained at a
stable address and the suffix is offered again, neither side copied. Both the borrowing and the
owning path retain until acknowledged; the difference is that the owning path allocates nothing
where the borrowing one allocates a retained copy, which `tests/zero_alloc.rs` pins by counting
both. The HTTP/3 layer still uses the borrowing path; why, and what changing it would take, is
in [`pending-work.md`](pending-work.md).

## `Idle` and `Blocked` are different answers

`ngtcp2_conn_writev_stream` returning `0` means "buffer too small or congestion limited",
and the documented response is to keep reading and wait for the window to open
(`ngtcp2.h:5240-5243`). It does **not** mean the send loop is finished.

Conflating the two builds a connection that works perfectly in tests and stalls under load.
`WriteOutcome` therefore gives them different names, and is a closed enum: a fourth answer
would be a change every caller must be forced to notice.

`ExpiryOutcome::IdleClose` is separate for the same class of reason. ngtcp2 documents it as
requiring the connection be dropped *without* a CONNECTION_CLOSE (`ngtcp2.h:4709-4713`);
routing it through the generic error path would send a packet to a peer that has already
gone.

## ngtcp2 paces its sending

`ngtcp2_conn_update_pkt_tx_time` records when the next packet may leave. Call it — as the
crate does after every write, because omitting it breaks pacing and returns no error — and a
subsequent write at an *unchanged* timestamp produces nothing.

That is correct behaviour, and it is invisible until a test clock that never advances gets
exactly one datagram and then silence, which looks indistinguishable from a broken
connection. `ngnet-quic-tests` advances its clock between writes for this reason, and
`PACING_STEP_NANOS` carries the explanation.

## Time is the caller's

QUIC is timer-driven in a way HTTP/2 and HTTP/3 framing are not: loss recovery, ACK delay
and the idle timeout all depend on time passing, and a connection never told the time has
passed simply stops.

ngtcp2 wants a timestamp on almost every call, and names no epoch and no clock — the header
specifies only nanosecond resolution and reserves `UINT64_MAX` (`ngtcp2.h:1070-1077`). The
bundled examples use `CLOCK_MONOTONIC`, but that is a convention of the examples rather than
a contract of the library.

So `Timestamp` is an opaque count of nanoseconds in whatever monotonic timescale the caller
keeps. Reading a clock here would pick one on the caller's behalf and would make every test
depend on wall time. The reserved sentinel is converted to `Option::None` at the boundary, so
"no timer is armed" and "a timer armed very far away" cannot be confused.

Addresses use `core::net`, not `std::net`, for a related reason: an invariant asserts the
crate names no I/O facility, and `std::net` is on that list. The address types themselves are
pure data and live in `core`.

## A server cannot exist before its client's first packet

`ngtcp2_conn_server_new` asserts that the transport parameters carry `original_dcid`
(`ngtcp2_conn.c:1264-1265`), and that value comes from the client's Initial packet. So
`src/accept.rs` — the connection-less entry point — is a prerequisite for any server at all,
not an optional extra. Since the assertion is compiled out of release builds, skipping it
there is undefined behaviour rather than a crash, which is why `TransportParams::build`
checks it too.

`Inspection::UnsupportedVersion` is a variant rather than an error because
`ngtcp2_pkt_decode_version_cid` fills its output **on** its error return: "Unlike the other
error cases, all fields of |dest| are assigned as described above" (`ngtcp2.h:2431-2476`).
The natural Rust translation — map the error to `Err`, discard the output — would throw away
exactly the identifiers a Version Negotiation packet has to echo back.

## One driver per socket

`ngnet-h3`'s asynchronous layer hands back a driver per connection, because a caller gives it
a connection that is already established. Copying that here does not work, and the reason is
only obvious once tried: several drivers cannot each own one UDP socket, and a driver
returned by the first `connect` has no way to own connections created after it.

So the unit of ownership is the **socket**. Building an endpoint yields a cheap cloneable
handle and exactly one driver; `connect` and `accept` are requests posted to that driver.

The driver owns its connections outright rather than sharing them behind a lock. A `Conn` is
`Send` and deliberately not `Sync`, and every method that drives one takes `&mut self`, so
exactly one thing may hold it — and a lock would buy nothing, because it would be taken for
every datagram and nothing else could usefully hold it anyway. Everything a caller wants to
do has to be sequenced against the packets arriving for that connection regardless.

That has a consequence worth stating: the driver holds `Conn<'static, S>`, which is possible
only because every handler it installs captures an `Arc` and borrows nothing. A handler that
borrowed a driver field would make the driver self-referential.

## The timer is one timer, and forgetting to rearm it looks like a hang

It is natural to assume a QUIC driver needs two deadlines — one for loss recovery and the
idle timeout, another for pacing, since ngtcp2 refuses to send before its pacing time. It
does not. `ngtcp2_conn_get_expiry2` ends with `ngtcp2_min(res, conn->tx.pacing.next_ts)`
(`deps/ngtcp2/lib/ngtcp2_conn.c:11387`), so what `Conn::expiry` reports is already the
earlier of the two.

The practical consequence is the whole reason this is written down. The driver rearms from
`expiry()` after **every** pass, including a pass that only wrote. A driver that rearmed only
after reading would send one datagram and then sleep until the peer said something — which
during a bulk transfer is never. The symptom is a connection that establishes, transfers a
kilobyte and stops: a hang, not a slow link.

Correspondingly the driver never calls `ngtcp2_conn_update_pkt_tx_time` itself. The core's
write paths already do, and calling it twice per packet pushes the deadline forward twice,
halving the send rate for no visible reason.

## A short header does not say how long its connection ID is

This cost real debugging time and is the sort of thing that only bites after everything
appears to work.

A long-header packet carries an explicit length for each connection ID. A short header does
not: an endpoint is expected to know how long its own identifiers are, because it chose them.
Anything demultiplexing datagrams must therefore decode short headers with exactly that
length, and a wrong value reads the wrong bytes as the identifier.

The failure is beautifully misleading. The handshake completes perfectly, because it is all
long-header packets. Every packet afterwards fails to route, so the connection establishes
and then goes silent. `ConnectionId::DEFAULT_LEN` exists so the builder and the router read
one constant rather than two that can drift.

## Flow control has two windows and only one of them is obvious

Reading is what earns credit back, and there are two allowances: per stream and per
connection. Extending only the per-stream one works — for a while. The connection window is
shared across every stream, so a transfer stalls once enough total bytes have flowed,
proportionally late and with nothing to say why.

The driver extends both, and a test sends 120 kB through a 24 kB connection window so the
path is exercised several times over rather than incidentally.

Two related things in the same area. A write is offered to the core one packet's worth at a
time, because the core copies what it accepts and holds the copy until acknowledgement —
offering a whole large payload on every attempt would recopy the remainder for every datagram
produced. And the end-of-stream flag goes on the last chunk, never the first: setting it
early closes the write side and the next attempt is refused.

Running out of stream credit is likewise not a failure. ngtcp2 reports it as an error, and
treating every error as fatal meant a caller who opened one stream too many lost the whole
connection. It is an ordinary condition — the peer advertised a limit and this endpoint
reached it — so the request waits for the peer to raise it.

## Address validation is why a server is safe to expose

A server that answers a first packet with a handshake is an amplifier. The handshake is
several times larger than the packet that provoked it, so a spoofed source address turns the
server into a weapon aimed at whoever the attacker names, and the attacker pays a fraction of
what the victim receives.

Retry closes that: an unvalidated first packet draws a small packet carrying an opaque token
and **no per-connection state at all**. Only a client that genuinely holds the address it
claimed receives the Retry and can come back with the token.

The token is derived rather than remembered, and that is the point. Remembering would
reintroduce exactly the state Retry exists to avoid — an attacker would fill the table
instead — so tokens are authenticated with a secret only the server knows and carry the
address and issue time inside them. `Validation::decide` takes `&self` for that reason: there
is nowhere for per-client state to go.

Two things about it are not obvious:

**It needs the TLS backend, and not because of TLS.** Writing a Retry packet is not
assembling bytes. The packet carries an AEAD integrity tag, and `ngtcp2_pkt_write_retry`
takes an encryption callback and an initialised AEAD context to produce it. Those and the
token helpers come from ngtcp2's crypto helper library, which `wrapper.h` includes only when
a backend is enabled. So `validate_addresses` exists only under `tls-ossl` rather than
existing everywhere and failing at run time.

**The server must echo the identifier it used.** After verifying a token, the server has to
set the `retry_scid` transport parameter, because the client checks that the identifier it
was told to address really came from a Retry this server sent (`ngtcp2.h:832`). Omitting it
produces a handshake that never completes and reports nothing — indistinguishable from an
unreachable server, which is how it was found.

Stateless reset comes with the same secret, and carries two constraints that are the
difference between informing a peer and attacking a third party: every reset is **strictly
smaller** than the datagram that provoked it or is not sent at all, and resets are rate
limited by a refilling budget. Answering unmatched traffic is useful; answering a flood of it
without limit is the attack again.

## The seams are poll-shaped, and impose no `Send`

`AsyncUdpSocket` and `Clock` are poll-shaped rather than `async fn` in trait, for two reasons
that are about the driver rather than about taste. The driver does several things per wakeup
— drain the socket, service timers, write — and must ask "is there a datagram *right now*"
without parking, which is what `Poll` expresses and what awaiting does not. And it holds the
socket behind a pointer, where `async fn` in trait would cost a box per datagram.

Neither trait requires `Send`. Thread-per-core runtimes build their I/O on `Rc`, and
requiring it would exclude them for nobody's benefit; auto traits propagate instead, so an
endpoint over a `Send` socket is `Send` without anything saying so. The in-crate test sockets
are deliberately built on `Rc` so that this is tested rather than asserted — if the bound
crept in, they would stop compiling.

The one place `Send` *is* required is the entropy factory, and it follows from the core
rather than from this layer: a `Conn` is `Send` and owns its entropy source, so the source is
already `Send`, and a factory that was not would have to capture thread-bound state to
produce values that are not.

The entropy source itself is supplied by the caller for the same reason the clock is. QUIC
needs unpredictable connection identifiers and stateless reset tokens; choosing a generator
here would choose it on the caller's behalf, and choosing a weak one would be a security
defect nothing in the API would reveal. So `EndpointBuilder::build` refuses to produce an
endpoint without one.

## Panics abort

A panic inside a handler unwinds into a C stack frame while ngtcp2 holds connection state,
which is unsound. There is no `catch_unwind` anywhere in the crate, matching `ngnet-h3`. The
posture is deliberate rather than inherited: handlers should return errors, not panic. What
*is* guarded is the bridge slot, which is cleared by a guard's `Drop` even while unwinding,
so a later stray callback cannot follow a pointer into a dead frame.

## What is not here

No runtime and no timer thread: the endpoint layer names no executor, spawns nothing, and
every future it produces is polled by the caller. No 0-RTT or session resumption, no
unreliable datagrams, no connection migration, no explicit key update. No ECN marking and no
datagram batching. No adapter to `ngnet-h3`'s `QuicConnection` trait — that is deliberate
future work, and the reasons are in [`pending-work.md`](pending-work.md).

Only one TLS backend is written. The seam admits others; wolfSSL, GnuTLS and BoringSSL all
have ngtcp2 crypto helpers, and adding one should not require touching anything outside a new
`tls_*.rs` module.

- Edition 2024, built with the toolchain in `rust-toolchain.toml`. No declared MSRV.
- System OpenSSL **3.5 or newer** is required for the default backend: ngtcp2's `ossl` helper
  uses the QUIC TLS API that first appeared there. CI pins `ubuntu-26.04` for this.

## Managed and detached connections

An endpoint carries two kinds of connection at once, on one socket.

A **managed** connection is what the endpoint has always had: the driver owns the protocol
state and an application reaches it through `Connection`, a handle that exchanges commands
and observations with the driver through a mailbox.

A **detached** connection is handed to a caller, who owns the protocol state and drives it:
reading the datagrams the endpoint routes to it, producing the ones it wants sent, and firing
its own timer. `Endpoint::connect_detached` and `Endpoint::accept_detached` produce them.

### Why the second kind exists

Because a mailbox cannot serve every consumer. `ngnet-h3`'s transport trait fills a packet by
calling into its transport and expecting an answer before the call returns — the byte slices
it offers are invalid once the closure ends. Bytes cannot reach another task in time. A
consumer of that shape must *own* the connection, so the endpoint has to be able to give one
up. The alternative was for `ngnet-quic` to know about HTTP/3, which is precisely what the
crate split exists to avoid.

### What the endpoint keeps

Everything shared between connections: the socket, the connection-identifier routing table,
address validation, stateless reset, version negotiation, and the handling of datagrams that
match no connection. What moves is only the per-connection protocol state, which admits
exactly one owner.

### Hand-over happens after the handshake

The endpoint completes the handshake and only then gives the connection up. Handing one over
earlier would give the caller something that cannot yet carry anything — the HTTP/3 trait
begins with an established connection and says so — and would duplicate the handshake, which
is the part most worth having written once.

### Identifier changes travel separately

Minted and retired connection identifiers used to arrive as observations, which the driver
drained. A detached connection's owner drains observations now, so an identifier change sent
that way would never reach the endpoint. The connection would answer on the identifier it
started with and on nothing else, and since a peer switches identifiers at a time of its
choosing, that appears as a connection that works and then stops.

They have their own queue, and the endpoint applies them *before* sending anything produced
in the same pass, so an identifier is routable before the packet announcing it leaves.

### The two datagram queues have opposite overflow rules

Not symmetric, for reasons that are not symmetric.

**Inbound**, past its bound, the endpoint drops. It reads one socket on behalf of every
connection, so waiting for a slow consumer would starve the rest. A dropped datagram is a
lost packet, which QUIC recovers from and which a full socket buffer would have produced a
layer lower anyway. The count is exposed rather than hidden.

**Outbound**, dropping is not available. A datagram that has been produced cannot be
withdrawn: the connection has already accounted for the stream bytes in it, so offering them
again would send them twice and discarding it loses them until a retransmission timer
notices. So the producer asks for room *before* writing, and the bound is what it asks
against.

### Eviction needs the owner to say when it is done

The endpoint decides a managed connection is finished by asking whether it is draining. It
cannot ask a detached one, because it does not hold it. So the owner marks it, and until then
the routing entry stays. Guessing either way is a leak or a connection cut off mid-close.

### The clock travels with the connection

A detached connection reads the *endpoint's* clock, not one of its owner's.

This was found rather than reasoned out. A first attempt gave the consumer its own clock and
ngtcp2's own assertion caught it immediately: `log.last_ts <= ts`. Two clocks have two
origins, so timestamps from the second are not comparable with those the endpoint recorded
while driving the handshake. In a release build that assertion is compiled out and the
connection silently mis-times its loss detection instead.

Capturing the clock means it must be cloneable and shareable, which `Clock` deliberately does
not require: the seams in this module impose no `Send` bound so a thread-per-core runtime can
build them on non-shared types, and the test clock is non-`Send` on purpose to keep that
property honest. Rather than weaken it, the bound sits on a second constructor,
`EndpointBuilder::build_detachable`, so it is asked only of callers who need what it buys.
The test clock broke the first attempt at putting it on `build`, which is the property
defending itself.

## Both directions a stream closes in

QUIC shuts a stream's two directions independently and ngtcp2 has reported a code for each
since 1.25, through `stream_close2`. This crate bound the older `stream_close`, which reports
one code and does not say which direction it belonged to.

ngtcp2's own documentation gives the case that separates them: an endpoint receives
STOP_SENDING and answers with RESET_STREAM carrying the same code, which belongs to the
*sending* side, while the response body arrived intact so the *receiving* side has no code at
all. The single-code form cannot express that.

`StreamCloseReason` is therefore a struct with an optional code per direction rather than an
enum of `Finished` and `Reset`. A struct because the two are genuinely independent — every
combination of present and absent is meaningful — and because absent is not the same as zero:
a direction that ended cleanly reports nothing, where code zero is a reset that carries zero.

## The stream-limit notification

`extend_max_local_streams_bidi` and its unidirectional counterpart are wrapped because they
are the only signal that a refused stream open may now succeed. Opening past the peer's limit
is reported as blocked rather than as an error, deliberately, since the condition is
temporary — but nothing else announces that it has lifted. A caller that waits and is never
told does not fail; it waits. `ngnet-h3` waits indefinitely by design and has no timeout
underneath, so the absence of this callback is a hang rather than a delay.

The figure they carry is the cumulative total the endpoint may now open, not an increment.

## Retention holds an address, not a length

`retain.rs` exists because ngtcp2 does not copy the stream data it accepts: it keeps the
caller's pointer so it can retransmit, and requires the bytes stay intact until acknowledged
or the stream closes. Each accepted write therefore gets an allocation whose address is fixed
for as long as it lives — a `Box<[u8]>` for a borrowed write, copied in; or, for an owned write
handed over through `write_stream_owned`, the caller's `OwnedBytes` handle kept alive so no copy
is made. Either way the address does not move.

ngtcp2 routinely accepts *less* than it is offered — a packet fills, and the remainder comes
back as a separate write. Shrinking the allocation to the accepted prefix is the obvious
tidy-up and is a use-after-free: the address ngtcp2 was given must stay valid, and
reallocating changes it while ngtcp2 still holds the old one for retransmission. Nothing
detects that on a lossless link, because nothing retransmits, and a test comparing bytes
would pass either way since freed memory usually still reads back correctly.

So the accepted *length* is recorded separately from the allocation, and the tail beyond it
is left allocated until the chunk is released. That wastes at most one packet's worth per
outstanding chunk. The test that guards it asserts the address.
