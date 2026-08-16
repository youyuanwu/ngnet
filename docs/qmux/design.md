# QMux design

Why the QMux crates are shaped the way they are: what the protocol is and is not, why the
native build breaks with every other `-sys` crate here, the two places where the obvious safe
API would have been unsound, and the handful of upstream behaviours that a wrapper has to
compensate for rather than pass through.

## What QMux is

QMux is a polyfill. It carries QUIC's stream operations over a single ordered, reliable byte
stream, so that an application written against QUIC's stream API can run over TCP — or over a
unix socket, or over anything else that delivers bytes in order — without being rewritten. The
draft's own framing is that HTTP has one binding for QUIC and another for TCP, WebTransport
has two extensions for the same reason, and the duplication is expensive.

What it keeps from QUIC is the multiplexing: many independent, flow-controlled streams over
one connection, with QUIC version 1's frame encoding and stream-id conventions reused
unchanged. What it drops is everything the underlying transport already provides. There are no
packets and no packet headers, no connection IDs, no path validation or migration, no loss
recovery, no congestion control, and no ACK frames — the transport is already reliable, so
`STREAM` frames are simply written in order and never retransmitted. The prohibited frame list
in the draft is the concise statement of this: `PING`, `ACK`, `CRYPTO`, `NEW_TOKEN`,
`NEW_CONNECTION_ID`, `RETIRE_CONNECTION_ID`, `PATH_CHALLENGE`, `PATH_RESPONSE` and
`HANDSHAKE_DONE` may not be sent.

Instead of packets there are **records**: a variable-length integer giving a length, followed
by that many bytes of QUIC frames. Records are self-delimiting, a frame may not span two of
them, and a record whose final frame is truncated is a connection error.

### QMux requires no TLS

This is the point most likely to be got wrong, so it is worth stating plainly. **Nothing in
QMux is encrypted, and the protocol mandates no transport security.** The draft delegates
confidentiality, integrity, peer authentication and application-protocol negotiation to
whatever carries the byte stream, and lists them as properties the transport *ought* to have
"unless used upon endpoints between which tampering or monitoring is a non-concern". It then
names a substrate that provides none of them: unix sockets, where the operating system is
trusted and different listening sockets stand in for ALPN.

TLS over TCP is the recommended carrier because it supplies every required property at once.
It is not a requirement, and `libdwnx` reflects that — its library half contains no reference
to TLS, encryption or sockets of any kind. Neither `ngnet-qmux-sys` nor `ngnet-qmux` links a
TLS library or has a TLS feature. `ngnet-qmux-sys` has no features at all; `ngnet-qmux`'s only
features gate its asynchronous layer and the ready-made tokio implementations of that
layer's seams, and neither brings cryptography with it.

A practical consequence shows up in the tests: two connections relaying bytes to each other in
memory is a *legitimate deployment* of the protocol rather than a test fixture standing in for
a real one. Every behavioural test in `ngnet-qmux` is written that way, and exercises the same
code a TCP carrier would.

## Why the native build uses `cc` and not CMake

Every other `-sys` crate in this workspace drives CMake, because nghttp2, nghttp3 and ngtcp2
all ship a CMakeLists.txt. dwnx does not. It ships autotools, and nothing else.

That left three options. Running `autoreconf` and `configure` from the build script would be
closest to upstream, and would make autoconf, automake, libtool and a shell prerequisites for
every contributor and every CI job on every platform. Contributing a CMakeLists.txt upstream
would fit the existing pattern, but means maintaining a build file the library does not have
and keeping it in step with a `Makefile.am` that is still moving. Compiling the sources
directly with `cc` needs only a C compiler.

The third is viable here in a way it would not be for the other three libraries, because
`libdwnx` has no external dependencies at all: 25 C files, no TLS, no event loop, no
structured-field parser, nothing to find and nothing to configure against. The whole of what
`configure` does that matters is generate a version header and probe a handful of platform
features.

### What the build script took over from `configure`

Two jobs.

The version header, `dwnx/version.h`, is generated from `version.h.in` with two substitutions
— the package version from `AC_INIT` and the same version packed into a hex integer. Both are
constants in `build.rs`.

The feature probes are the part worth reading carefully, because `configure.ac` checks around
thirty things and the sources consult only a handful of them, in two different ways that need
opposite treatment:

- **Header probes** — `HAVE_ARPA_INET_H`, `HAVE_NETINET_IN_H`, `HAVE_ENDIAN_H`,
  `HAVE_SYS_ENDIAN_H`, `HAVE_BYTESWAP_H`, `HAVE_UNISTD_H` — are tested with `#ifdef`.
  Defining one to `0` therefore *still includes the header*. They must be defined when the
  header exists and left entirely undefined otherwise.
- **Declaration probes** — `HAVE_DECL_BE64TOH`, `HAVE_DECL_BSWAP_64` — are tested with `#if`.
  Leaving one undefined evaluates to zero, which happens to be a valid answer but only by
  accident, and `-Wundef` would flag it. They are always defined, to `1` or `0`.

Getting the first kind wrong does not produce a warning. With `HAVE_ARPA_INET_H` unset on a
unix target, `dwnx_net.h` never includes `<arpa/inet.h>`, and its byte-swap fallback expands
to calls to `ntohl` that were never declared.

There is also `_GNU_SOURCE`, which is what `AC_USE_SYSTEM_EXTENSIONS` does, and which is
load-bearing rather than tidy. glibc's `endian.h` only *declares* `be64toh` and `htobe64` when
a feature-test macro asks for them. Without it the header is included, the declarations are
absent, C treats the calls as implicit, and the build reaches a link error naming two symbols
that were never compiled — a confusing way to learn that a configure step was skipped. This
was found by a test, not by inspection: the sys crate's smoke tests never reached the varint
paths that call them, and the first in-memory transfer did.

## The two places the obvious API would have been unsound

### The record buffer is retained across calls

dwnx builds a record incrementally. `dwnx_conn_writev_stream` returns `DWNX_ERR_WRITE_MORE` to
say "there is room left in this record, call me again", and the caller is expected to loop
until it gets a length back.

The trap is that the first call in such a sequence reaches `dwnx_qre_start`, which stores the
caller's `dest` pointer *inside the connection*. The follow-up calls do not re-read `dest`;
they append through the pointer retained from the first.

An API taking `&mut [u8]` on every call cannot express that. Safe code could pass a temporary,
see `WriteMore`, and pass a different buffer on the next call — leaving dwnx writing through
the first pointer, which may since have been freed. That is a use-after-free reachable without
writing `unsafe`, which is precisely what a safe wrapper exists to prevent.

So the buffer is borrowed once, by `RecordWriter`, for as long as the record is being built.
The borrow checker then enforces what the C documentation only implies, and a `compile_fail`
doctest pins it. `Conn::write` drives the whole loop internally for the common single-payload
case, so most callers never see `RecordWriter` at all.

The borrow alone turned out to be insufficient, which a review caught. `dwnx_qre_start` sets a
"started" flag that only `dwnx_qre_final` clears, and a later call skips `qre_start` while it
is set — so a `RecordWriter` merely *dropped* mid-record leaves the connection pointing into a
buffer whose borrow has ended, and the next write appends through the stale pointer while
reporting a length measured against the old record. Reaching that needs no `unsafe`: pushing
once, seeing `Accepted`, and returning early is enough, and `StreamBlocked` and `StreamClosed`
positively invite a caller to stop. `RecordWriter` therefore has a `Drop` that finalises an
unfinished record with a control-only write, which always reaches `dwnx_qre_final`.

That makes abandonment safe but not free, and the difference is worth stating. dwnx advances a
stream's send offset the moment it packs data, so bytes taken by an abandoned record are lost
and the peer rejects the next record on that stream as a gap it can never fill. The rule is
"always call `finish`", which `Conn::write` does on the caller's behalf; both halves are
pinned by tests.

This is the one place the design departs from `ngnet-quic`, which collapses ngtcp2's
equivalent `WRITE_MORE` away entirely. It can afford to: ngtcp2's version requires the caller
to repeat identical arguments, so exposing it is a footgun with no upside. QMux's does not
work that way — each call may nominate a different stream — so the loop is genuinely useful
for packing several streams into one record, and is worth exposing.

### Callbacks must not re-enter the connection

dwnx documents that `dwnx_conn_writev_stream` must not be called from inside a callback.
Independently, the callback bridge — a single boxed slot holding the live handler borrows,
inherited in shape from `ngnet-quic` — cannot survive a nested entry point at all: a second
one would overwrite the first's borrows.

`ngnet-quic` handles this by relying on ngtcp2's documented rule that its main entry points
may not be called from callbacks. dwnx's rule is narrower, covering only the write, so the
same reasoning does not carry over. The bridge slot is a `Cell` rather than something reached
through `&mut`, so that no `&mut` to it ever exists: a guard holding one across the C call, and
a callback forming a second from `user_data`, would be aliasing Rust forbids even where the two
never overlap observably.

Rather than police the difference at run time, `ngnet-qmux` removes the capability. The C
callbacks are handed a `dwnx_conn *`, but the shims do not forward it: handlers receive event
values and no means of naming the connection. Because handlers are owned by the connection and
every entry point takes `&mut self`, the borrow checker also refuses to let a handler capture
one from outside. There is nothing to call, so there is nothing to check. Four `compile_fail`
doctests pin this, and the debug assertion that remains in the bridge is a tripwire against
future changes rather than the mechanism.

Handlers are also required to be `Send`. `Conn` carries a hand-written `unsafe impl Send`, and
it owns its handlers — so without the bound a caller could capture an `Rc` in a handler, move
the connection to another thread and drop it there, racing a non-atomic refcount from entirely
safe code. `ngnet-quic` takes the same position for the same reason.

The cost is real and worth naming: a handler cannot extend a flow-control window at the moment
it observes data, or open a stream in response to one closing. It records what it saw, and the
caller acts once the entry point returns. If that proves too restrictive, the way out is a
limited callback context exposing only the operations dwnx permits during a callback — noted
in `pending-work.md` rather than built speculatively.

## Where the wrapper compensates for upstream

Four behaviours are surprising enough that passing them through unchanged would make the safe
API misleading.

**Preconditions are assertions, not error returns.** `dwnx_conn_client_new` and
`dwnx_conn_server_new` guard their transport parameters with C `assert` — an out-of-range
limit, a `max_idle_timeout` of `UINT64_MAX`, a `max_record_size` below the protocol minimum.
Tripping one aborts the process in a debug build, which is not something a caller can handle.
`TransportParams::validate` checks the same conditions in Rust and reports them as ordinary
errors, so the abort is unreachable. It also checks `initial_max_stream_data_uni`, which the
constructor does *not* assert even though its siblings are — an upstream oversight, caught
later during frame encoding.

**One configured value is silently discarded.** dwnx overwrites `max_record_size` with
`DWNX_DEFAULT_MAX_RECORD_SIZE` immediately after copying the parameters in, with the comment
"We do not let application increase max record size". Configuring it has no effect. The
wrapper documents this and reports the library's value on readback rather than the caller's.

**The fatality predicate does not mean what it appears to.** `dwnx_err_is_fatal` is
`liberr < DWNX_ERR_FATAL`, where `DWNX_ERR_FATAL` is `-500`. It therefore answers "was this
code allocated in the `-5xx` block", which is true of exactly `NOMEM` and `CALLBACK_FAILURE`;
`PROTO`, `FRAME_ENCODING` and `FLOW_CONTROL` all report as non-fatal despite the header saying
the connection must be closed. `NativeCode::is_fatal` forwards to C unchanged, so a reader of
dwnx's documentation finds the same answer, and `Error::leaves_connection_usable` is the
crate's own judgement derived per entry point from what the header actually says. The stream
codes are why it matters: `STREAM_ID_BLOCKED` means "no capacity right now" and is
recoverable, `STREAM_LIMIT` means the peer violated a limit we advertised and is terminal, and
`STREAM_NOT_FOUND` comes from the write path where the instruction is to close. Three similar
names, three different dispositions.

**There is no getter for the peer's transport parameters.**
`dwnx_conn_get_local_transport_params` returns the *local* set; the peer's arrive only through
the `recv_transport_params` callback. The bridge therefore copies them into connection-owned
storage as they arrive, and `Conn::peer_transport_params` reads that cache. If dwnx grows a
real getter, the accessor can forward to it instead.

## Two return values that cannot be taken at face value

`dwnx_conn_write_vmsg` returns `0` both when there is genuinely nothing to send *and* when
`destlen <= 2`. Those are entirely different situations, so the wrapper checks the buffer
before calling and reports `Record::BufferTooSmall` itself. The guard is that three-byte floor
and deliberately not the record size: the C documentation explicitly permits a smaller buffer
when the transport cannot accept a full record, so rejecting everything below 16382 would
refuse supported usage.

`DWNX_ERR_NOBUF` never reaches a caller from the write path. It is dwnx's internal "this
record is full" signal, swallowed at each of the sites that can raise it, after which the
record is finalised and its length returned. It remains a mapped `ErrorKind` because the
*constructor* returns it on allocation failure — where, incidentally, the header documents
`DWNX_ERR_NOMEM` and the code returns `NOBUF`.

## Module layout

`ngnet-quic` has thirty-five modules; `ngnet-qmux` has eleven, and the difference is almost
entirely protocol rather than ambition. There is no `cid`, `path`, `packet`, `rand`, `token`,
`tls`, `tls_bridge` or `tls_ossl`, because QMux has no connection IDs, paths, packets,
entropy, tokens or cryptography. There is no `retain`, because dwnx copies stream data into
the record and never retransmits, so there is nothing to keep alive on the caller's behalf —
the single largest simplification relative to the QUIC wrapper. There is no `endpoint` subtree,
because this increment is sans-I/O only.

What remains maps one-to-one onto the C API: `conn` for lifecycle and the read path,
`callbacks` and `handlers` for the event bridge, `write` and `stream_io` for the outbound and
stream operations, and `error`, `params`, `settings`, `stream`, `time` and `ccerr` for the
value types.
