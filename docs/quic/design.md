# QUIC design

Why `ngnet-quic` is shaped the way it is. What follows is the reasoning a reader cannot
recover from the source: the places where the obvious design is wrong, and why.

`ngnet-quic-sys` builds ngtcp2 from the vendored submodule and generates raw bindings.
`ngnet-quic` wraps it once — a sans-I/O state machine, with no async layer, unlike the
HTTP/2 and HTTP/3 families. `ngnet-quic-tests` is unpublished and exists so the wrapper can
stay free of dev-dependencies while still being driven through real handshakes over real
sockets.

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

## One type owns all three TLS objects

This is the highest-risk area in the crate, and the reason the TLS backend landed before
anything that depends on it.

A connection's TLS involves three C objects: an `SSL`, an `ngtcp2_crypto_ossl_ctx` wrapping
it, and an `ngtcp2_crypto_conn_ref` that OpenSSL holds as the `SSL`'s application data.
They refer to each other in a cycle, and must be destroyed in exactly this order
(`deps/ngtcp2/examples/tls_session_base_ossl.cc:39-48`):

```text
SSL_set_app_data(ssl, NULL)  →  SSL_free(ssl)  →  ngtcp2_crypto_ossl_ctx_del(ctx)
```

Every step guards a use-after-free. `SSL_free` releases outstanding CRYPTO records, which
calls into `ossl_crypto_release_rcd` (`deps/ngtcp2/crypto/ossl/ossl.c:1191`); that reads the
app data, calls `conn_ref->get_conn(conn_ref)`, dereferences the `ngtcp2_conn`, and writes
through the ossl context. Clearing the app data first makes it return early — the helper's
own comment at `ossl.c:1196-1200` says that is precisely why the escape hatch exists. And
`ngtcp2_crypto_ossl_ctx_del` frees a `remote_params` buffer OpenSSL borrows until then.

Rust drops struct fields in declaration order. Relying on that would make a correctness
requirement invisible — a reordering during an unrelated refactor would be a memory-safety
bug with nothing to catch it. So `OsslSession` owns all three and implements `Drop` by hand,
and the parts are never exposed as independently droppable values.

The connection's own `Drop` completes the picture: `ngtcp2_conn_del` runs **first**, while
the TLS session is still alive, because the helper's callbacks can reach the connection
during teardown.

### `NativeTlsHandle` exists to prevent one specific mistake

`ngtcp2_conn_set_tls_native_handle` takes a `void *`, and the value it wants for this
backend is the `ngtcp2_crypto_ossl_ctx *` — **not** the `SSL *`
(`deps/ngtcp2/examples/tls_session_base_ossl.cc:50-52`). An experienced OpenSSL user would
reach for the `SSL`. It compiles cleanly and corrupts memory at run time.

Wrapping the pointer in a newtype only a backend can construct makes the mistake
unrepresentable outside the module that knows which is which.

### `ngtcp2_crypto_ossl_free` is deliberately never called

`ngtcp2_crypto_ossl_init` prefetches static `EVP_*` objects into process globals with no
reference counting (`ossl.c:49-60`, `:62`, `:82`). The ngtcp2 examples pair it with a
per-context destructor — which means that with two TLS contexts, destroying the second frees
objects the first is still using.

So `init` runs once behind a `Once` and `_free` is never called. A bounded one-off leak of a
handful of static objects is the right trade against corrupting a live connection.

## Why the TLS trait is `unsafe`, and why it carries the callback table

It would be tidier for the seam to hand back an opaque handle and let the connection install
a fixed callback set. That does not work, for two reasons.

Each TLS backend compiles **its own copy** of ngtcp2's shared crypto code, with
backend-specific implementations behind identically-named symbols — `ngtcp2_crypto_ctx_tls`
exists separately in `ossl.c`, `wolfssl.c` and `gnutls.c`. The callback set is part of the
backend, not something a generic connection can name.

And those symbols do not always exist. `crates/ngnet-quic-sys/wrapper.h:10-18` includes the
crypto headers only when a backend feature is on, so with `--no-default-features` there is no
`ngtcp2_crypto_*` symbol in the bindings at all. Code naming them directly would not compile.
Routing them through the trait is what lets the crate build with no TLS stack, exposing the
seam and nothing behind it.

The trait is `unsafe` because implementing it means promising things the compiler cannot
check: that the native handle stays valid, and that the objects behind it are destroyed in
the order the C library requires. Callers of a `Conn` remain entirely safe; only writing a
*new backend* requires care.

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

## Sent stream data has to be copied

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
its own `Box<[u8]>`: a growing `Vec` would reallocate and move bytes ngtcp2 still points at.

The cost is one copy of everything sent, held until acknowledged. `ngnet-h3` avoids the
equivalent copy by making its callers hand over ownership; this crate takes the copy instead,
because the safety of an ordinary `&[u8]` parameter should not depend on the caller having
read a paragraph of documentation. An ownership-taking alternative is in
[`pending-work.md`](pending-work.md).

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

## Panics abort

A panic inside a handler unwinds into a C stack frame while ngtcp2 holds connection state,
which is unsound. There is no `catch_unwind` anywhere in the crate, matching `ngnet-h3`. The
posture is deliberate rather than inherited: handlers should return errors, not panic. What
*is* guarded is the bridge slot, which is cleared by a guard's `Drop` even while unwinding,
so a later stray callback cannot follow a pointer into a dead frame.

## What is not here

No async layer, no socket, no runtime, no timer thread. No 0-RTT or session resumption, no
unreliable datagrams, no connection migration, no explicit key update. No adapter to
`ngnet-h3`'s `QuicConnection` trait — that is deliberate future work, and the reasons are in
[`pending-work.md`](pending-work.md).

Only one TLS backend is written. The seam admits others; wolfSSL, GnuTLS and BoringSSL all
have ngtcp2 crypto helpers, and adding one should not require touching anything outside a new
`tls_*.rs` module.

- Edition 2024, built with the toolchain in `rust-toolchain.toml`. No declared MSRV.
- System OpenSSL **3.5 or newer** is required for the default backend: ngtcp2's `ossl` helper
  uses the QUIC TLS API that first appeared there. CI pins `ubuntu-26.04` for this.
