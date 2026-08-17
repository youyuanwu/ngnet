# QMux design

Why the QMux crates are shaped the way they are: what the protocol is and is not, why the
native build breaks with every other `-sys` crate here, the two places where the obvious safe
API would have been unsound, the handful of upstream behaviours that a wrapper has to
compensate for rather than pass through, and — behind a default-on feature — an asynchronous
layer that drives the state machine over a byte stream the caller supplies.

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

## The asynchronous layer

Behind the default-on `io` feature, `src/io.rs` and the subtree beneath it drive the state
machine over one byte stream the caller hands over. `--no-default-features` removes the whole
of it and leaves the crate as it was: one dependency, no asynchrony, the same public API.

### There is no endpoint, and no driver task

`ngnet-quic` has both, and the reason is its unit of ownership. One UDP socket carries every
connection an endpoint serves, so *something* has to read that socket and route each datagram
to the connection it belongs to by connection id — and that something is a task the
application does not own, sitting between the caller and the connection state.

A QMux connection has none of that problem. It owns one byte stream and shares it with
nothing: the operating system already did the demultiplexing when it handed out a separate
accepted socket per peer. There is no routing table to consult, no datagram that might belong
to a connection nobody has heard of, and therefore no reason for a task to stand in the
middle. `Connection<S, C>` owns the byte stream outright, and every operation runs on the
caller's own task when the caller polls it.

That is also why this layer resembles `ngnet-h2` more than `ngnet-quic` in what it *is*, while
resembling `ngnet-quic` in the shape of its seams. Establishing the byte stream stays with the
caller — connecting, listening, accepting, and any TLS over it. The crate offers no third
constructor, and a test asserts it exposes no way to make one.

### Poll-shaped, though the closer analogue is future-shaped

`ngnet-h2` also runs a protocol over a byte stream, and its transport abstraction returns
futures and splits into a reader half and a writer half. By subject matter it is the nearer
precedent, and the layer follows `ngnet-quic`'s poll-shaped socket seam instead. Two reasons,
both about what a connection must do rather than about taste.

**Composition.** The HTTP/3 transport abstraction this work exists to satisfy,
`ngnet_h3::http::QuicConnection`, is itself poll-shaped: it hands the transport a `Context` and
expects an answer before the call returns. A future-shaped byte stream underneath it needs an
adapter holding an in-flight future between calls, and that adapter has to be
cancellation-correct — dropping a partially completed read loses bytes off a stream that cannot
resend them.

**One wakeup has to do both jobs.** A connection must drain reads *and* produce writes in a
single pass. It has to ask "are there bytes right now?", carry on when the answer is no, and go
on to flush what the state machine produced. `Poll` says exactly that; awaiting a read does
not. An awaited read parks the whole connection until bytes arrive, with the records already
queued for the peer sitting unwritten behind it — and for a peer that is waiting for precisely
those records before it says anything, that is a deadlock rather than a latency cost.

The layering settles it in any case: one protocol family in this workspace may not depend on
another, so `ngnet-h2`'s abstraction is unreachable from here whatever its merits.

Neither seam carries a `Send` bound. Thread-per-core runtimes build their I/O on `Rc`, and
requiring `Send` would exclude them to nobody's benefit; auto traits propagate instead, so a
connection over a `Send` byte stream is `Send` without anything saying so. The one bound is on
`AsyncByteStream::Error`, which must convert into a sendable, shareable boxed error — a
constraint on the *failure type* only, and there because the HTTP/3 abstraction demands it of
any transport. Discovering that mismatch in the join crate, after callers had implemented the
trait, is what stating it up front avoids.

There is no `sleep_until` beside `Clock::now`, because there is nothing to arm one from: dwnx
validates and advertises `max_idle_timeout` and then never acts on it, and has no timer or
expiry API of any kind. A clock that could wait would imply an enforcement this stack does not
perform.

### Produce up to the ceiling, write once, then read

The pump's order is the whole design. It used to be *flush, produce one record, flush*, so that
a record was produced only into an empty outbound buffer and at most one record was ever
outstanding. That rule was overturned deliberately, because it cost one write — one syscall, on
a real transport — per 16382-byte record, and a driver turn that produces sixty records paid
sixty of them where one would have carried the same bytes. What replaced it is: produce while
the buffer has room for another whole record, write what has accumulated, then read.

The old rule was defended by three arguments, and the replacement has to answer all three.

*Bounded memory.* One record outstanding bounded what a slow peer could make this side hold to
16382 bytes. The bound is now stated rather than implied: `OUTBOUND_CEILING` in
`crates/ngnet-qmux/src/io/conn.rs` is the memory the outbound buffer may occupy, and a record is
begun only while the buffer still has a whole record's room beneath it. The bound is still a
constant and still independent of what the caller has queued — it is simply a larger constant,
chosen so that the guaranteed carry beneath it is 64 KiB. Producing everything the state machine
owes and *then* writing, which is the alternative the old passage rejected, remains rejected for
exactly the reason it gave: that bounds outbound memory by the backlog.

The reserve has one relaxation, and it is enforced the same way the reserve itself is. A record
whose contents are already known to be small — the last few bytes of an offer a call has been
filling records for — is given a *shortened* destination rather than a full record's worth, so
it cannot exceed the space that is actually free. Without it, an offer that came to a few dozen
bytes more than the reserve allowed answered short, and those bytes then travelled alone in a
write of their own at the end of the pass: one extra write per stream, which at concurrency 64
cost more than multi-record production had gained. The rejected alternative was to predict the
record's size from the payload plus a framing allowance, which is an assertion about dwnx's
varint encoding rather than about the buffer, and wrong in the direction that overruns the
ceiling.

*Correct interleaving.* A record interleaved with the tail of its predecessor is not a record the
peer can parse. Nothing interleaves: each record is appended to the buffer whole, in the order it
was produced, and no record begins before its predecessor is complete. That is a weaker property
than "one record outstanding", and it is implied by it — which is why the old rule was sufficient
and is not necessary. The buffer is a byte queue, not a set of records, and the write side never
reorders it.

*Exactly one place to resume from.* A partial accept resumes from `written`, which is a single
byte cursor into the buffer and was one before this change too. What is new is that `written` may
now come to rest *inside* a record rather than only at a record boundary, because a write that
carried three records and a half stops mid-record. Nothing above the cursor cares: the buffer is
bytes, the resume point is the cursor, and the next write offers the same buffer from it.

The free space a partial accept leaves at the *front* of the buffer is not reclaimed. Production
stops when the tail cannot take another whole record, even though compacting the unwritten
remainder to the front would make room. Compaction was rejected because it is a memcpy of the
unwritten remainder on every partial accept, paid on exactly the path that is already struggling;
a ring buffer was rejected because two regions would leave the output in two pieces and every
consumer of it would have to handle both. That argument no longer has a gathering write to weigh
against it: whether the output could be presented as more than one region was asked and answered
no, and the reasons are recorded in `pending-work.md` — the ring is the only thing that would
produce a second region, and gathering would save no copy even if it did. A buffer that
stays full means the peer is not keeping up, and the right answer to that is to stop producing,
which is what stopping early does.

### A record is serialised where it will be sent from

There is no staging buffer on the write path. `Conn::record` is handed a slice of the outbound
buffer itself, so the bytes dwnx writes are already in the place the byte stream will be offered
them. What that removes is one memcpy of up to 16382 bytes per record — about a megabyte of
copying per megabyte sent — and `Connection::copied_record_bytes` is what says it is gone, rather
than this paragraph.

The arrangement it needs is a buffer held at *full length* with a fill cursor beside it, and the
reason is a rule rather than a preference: `crates/ngnet-qmux/tests/invariants.rs` forbids
`unsafe` anywhere under `src/io/`, and a `Vec`'s spare capacity cannot be handed out as a
`&mut [u8]` without it. Zeroing on growth is the safe form of the same thing; it is paid once per
connection per step of growth, where the copy it replaces was paid once per record. The buffer's
length therefore stops being how much it holds — `filled` is — and every emptiness test, bound
and slice in the layer is stated over the cursor. Growth is to exactly what a record needs rather
than by doubling, because a doubling growth would put the capacity above `OUTBOUND_CEILING` while
the queue obeyed it, and the ceiling is a promise about memory rather than about a cursor.

**The slice handed to the record writer is exactly one record wide, and never the whole tail.**
That is the part that would corrupt the wire in silence rather than fail. dwnx does not cap a
record on the write path: it initialises the record with whatever destination it is given, bounds
a payload only by what is left of that destination, and then writes the record's length as a
fixed two-byte varint whose encoder asserts the value is below 16384 and, where that assertion is
compiled out, truncates it to sixteen bits. As this workspace builds dwnx the assertion survives
in both profiles — checked by making the mistake deliberately, in debug and in release, and
finding an abort both times — but a dwnx built with assertions off would produce a record whose
declared length is nothing like its real one and a peer that has lost framing from that byte
onward. `Conn::record`'s contract refuses an over-long buffer nowhere, so the layer refuses it,
and `tests/io_writes.rs` asserts the property on the wire where neither build's behaviour is
assumed.

The rule is "never more than one maximum record" and deliberately not "always exactly one": the
relaxation above still hands a *shorter* slice to a record continuing an offer, which is safe in
the only direction that matters, since a record can only be smaller than its length prefix can
describe.

What decides that the tail is too short is arithmetic on the cursors, done before a record is
begun — not `Record::BufferTooSmall`, which fires only below three bytes and arrives as a record
of zero bytes with a "packed" verdict, which the write side reads as "the state machine has
nothing queued". A connection with output to send would stop producing it and nothing would say
so.

Reading comes last, and the order *within* the read matters as much. The bytes go to the framer
first and to `Conn::read` second, and only then is the outcome acted on. dwnx reports
`PeerClosed` after consuming the close record, possibly with more bytes still to come in the
same chunk, so feeding the framer first is what leaves the close record already latched when
that report arrives. Feeding the state machine first would work too — but by accident, and only
until someone reorders two lines that look independent.

Construction schedules the local transport parameters unprompted, which is the write-side half
of the first-flight problem and the easier one to miss. Stream capacity arrives *only* in the
peer's announcement, so a connection where both ends read diligently and neither speaks hangs
with no error at all. For the same reason `Config` supplies working limits rather than
inheriting the state machine's: `TransportParams::new` is all zeros by faithful reproduction of
dwnx, and a connection built from those could open nothing.

A push error ends the connection and is never retried. `RecordWriter::push` failing drops the
writer mid-record; `Drop` finalises so dwnx is not left writing through a retained pointer, but
the produced bytes are discarded — and if that record had already packed stream data, dwnx has
*already advanced the stream's send offset*. Retrying would send the next chunk at an offset
the peer can never reconcile, which presents as a stream that stalls rather than as an error.
The one exemption is narrow and conditional: a stream the state machine no longer has is
refused before the record is begun, so nothing was packed and nothing was lost, and that is
reported as a closed stream rather than as a failure.

### The layer frames records itself

dwnx already parses records, and doing it again looks like duplicated work. The alternative —
ask the state machine where it stands — was tried and rejected, because there is nothing to
ask. `dwnx_conn_read` answers `0` for "that was fine, feed me more" whether it stopped between
records or halfway through a length prefix, the record reader's state has no accessor, and the
reader itself is private.

Two questions the layer must answer therefore have no answer from below. **Did the byte stream
end cleanly?** A peer that stops between records has said everything it meant to; a peer that
stops partway through one has lost bytes it does not know about. Reporting the second as the
first is the failure mode with no symptom. **What did the peer's close say?** dwnx parses
CONNECTION_CLOSE into a private struct with no accessor and returns `DWNX_ERR_DRAINING` with
nothing attached, so the kind, code, frame type and reason are unreachable. Recovering them
means holding the record's own bytes and decoding them here — and encoding them here too, since
dwnx serialises no close at all.

Retention is a permanent **latch**, not a sliding window, and the difference is the point. A
window holding the most recent complete record loses closes: `Conn::read` reports the close only
after consuming its record, and a single read may carry more bytes after it, so the window would
begin the next record and evict the close in exactly the case where the peer said something
worth hearing. A close is terminal, so latching costs one record and loses nothing. The bound is
one record in progress plus one latched close — under 32 KiB, whatever the peer does, since dwnx
overwrites any configured maximum with 16382.

What is retained, though, is now narrower than that bound suggests, and the ordinary case
retains nothing. A record whose declared length is entirely present in the slice `consume` was
handed is **scanned where it lies**: the bytes are already contiguous in the connection's read
buffer, the decoder wants nothing but a contiguous payload, and copying them into a buffer of
the framer's own to look at them is a second copy of every record bought for the one record in a
connection's life that carries a close. The copy is paid only where it buys something — a record
spread over several reads, which has nothing contiguous to scan and must be reassembled before
it can be looked at at all — and once more for a close found in place, which has to be copied
because latching it means holding it after `consume` has returned. The rejected alternative was
scanning each fragment as it arrives and keeping no buffer at all: it loses a close cut across
two reads, silently, which is the same failure mode as the window.

Three conditions gate the scan-in-place path, and `src/io/framing.rs` states them where they are
applied. The retention buffer must be empty *and* the slice must hold the whole declared
remainder — either alone admits the tail of a half-arrived record being scanned as though it
were a whole one. What is scanned is exactly the declared length's worth and never the rest of
the slice, because the decoder takes a payload with its length prefix already stripped and would
otherwise walk into the next record and assemble a close out of its fields. The third, that no
close has been latched, is defensive: it keeps "the retention buffer is empty" meaning "this
record has not started" rather than depending on a check made elsewhere.
`io_framing.rs` fails on each of the first two if it is removed, and
`RecordFramer::copied_bytes` reports zero for a run of records that arrive whole.

The decoder scans frames rather than assuming the close is first, because
`dwnx_record_reader_reset` returns to the frame-type state while bytes remain in the record: a
close may legally follow other frames. And the test that matters feeds an encoded close to a
real connection and expects `PeerClosed`, since round-tripping through our own decoder would
prove only that we agree with ourselves.

### Two write forms, because there are two callers

`poll_write_stream` parks when flow-control credit is exhausted and resumes when the peer
extends the window. That is what a direct user of this layer wants, and it is what makes a
blocked write a wait rather than a failure or a truncation.

`try_write_stream` never parks. It reports `StreamWrite::Accepted(n)`, `Blocked` or `Closed` and
returns. It exists because the HTTP/3 transport abstraction offers its outbound bytes through a
*synchronous* closure — handed a stream, some slices, and a verdict to return — with no
`Context` anywhere in reach. A layer that could only park would have nothing legal to do inside
it, and discovering that at the join would have meant changing this API after the fact.

One call fills **as many records as the buffer will hold**, and that is a property the caller
above depends on rather than an optimization it cannot see. A short answer from this form means
a bound was reached — the peer's window is shut, or the buffer is at its ceiling — and never
that a record filled. The distinction is invisible from above: the HTTP/3 layer is told a count
and nothing else, and it reads a short count as congestion and stands the stream down for the
rest of its pass. While a call took one record, every large offer answered short, so a stream
with a megabyte to send moved sixteen kilobytes of it per pass however much room the buffer had;
a 2 MiB upload cost 130 writes where the carry accounts for 32. The alternative — leaving the
decision above, by re-offering after a short accept — was rejected because the layer above
cannot tell a filled record from a shut window, and re-offering into a shut window spins.

`try_write_stream_vectored` is the same call taking the payload in fragments, and it is the one
the HTTP/3 join uses: `StreamSource::write_next` lends a stream's pending output as a list, and
the fragments go into records together rather than one apiece. `try_write_stream` is written as
its one-fragment case rather than kept as a loop of its own, and so are `pack` and
`RecordWriter::push`. That is deliberate. The part that is easy to get wrong is the resumption
— dwnx reports `*pdatalen` as one total across every vector, not a count per vector, and a
short take routinely stops part-way through a fragment — and a walk that resumed at the wrong
place would send some bytes twice and others never while reporting a count that agreed with
itself. There is one walk (`Fragments`) rather than two, because a second one is a second thing
to get wrong silently. The rejected alternative was a pair of parallel loops, single-slice and
vectored, which reads more simply and duplicates exactly the part that has no safety net.

The end-of-stream marker follows from that rather than being placed: dwnx applies it when the
data one call handed it fits entirely, so a push that had to leave fragments behind — a list
longer than the sixteen-entry array one push submits — must not carry it, and the loop
suppresses it in that case only. Empty fragments are never submitted, so a trailing empty
fragment cannot take the marker away from the payload before it, and no index has to be
computed to avoid it.

Both split a payload across records and across available credit rather than truncating, and both
report what they took even when they then refuse: a count dropped because the verdict was a
refusal has the caller offer those bytes a second time, and the peer receives them twice.
`StreamWrite` is the layer's own type. The sans-I/O `Push` describes the state of a record being
*built* and invites another push, which is a conversation only the code inside the pump is in a
position to have; exposing it would put dwnx's record-building protocol into the signature of
every layer above.

### Waiting, and reading no further ahead than the caller

Three things here cannot proceed on demand: an open the peer's stream limit forbids, a write
with no credit, and a read the caller has made no room for. Each parks against the event that
ends it. The alternative — waking one's own waker and returning `Pending` — compiles, passes a
functional suite, and burns a core; it is a busy loop wearing waiting's clothes, and the tests
that keep it out count wakeups rather than checking eventual answers.

The connection-level window is the awkward one, because dwnx has no MAX_DATA callback: it
applies the frame to the send window and tells nobody. So the connection samples
`max_data_left` across each `Conn::read` and wakes a parked writer when it moves. Waking on any
inbound bytes would have been simpler and would have spun a blocked writer once per arriving
record for as long as the peer kept talking.

Read-ahead is bounded by bytes **delivered to the caller and not yet credited back**, not by
queue depth. Depth is the natural meter and the wrong one: a caller that drains events into a
`Vec` of its own without crediting them empties the queue while holding exactly as much memory,
and the bound would read zero. Only connection-level credit moves the figure. The HTTP/3 layer
above reports every consumed byte twice — once naming a stream, once naming the connection —
so counting both would credit two bytes for every one delivered, the bound would never bind,
and read-ahead would be limited by nothing at all.

An idle connection therefore arms nothing: no outbound bytes means no write offered, one read
registers the byte stream's own waker, and there is no timer here to fire in the meantime.

### A delivery is a view of the read buffer, not a copy of it

`Event::StreamData` carries a `StreamBytes`: a reference-counted handle on the connection's read
buffer plus a range within it (`src/io/delivery.rs`). It used to carry a `Vec<u8>` filled by
copying, one memcpy per delivery, because the handler receives a borrow valid only for the
duration of dwnx's callback. What made the copy removable is not a change to that borrow but a
fact about what it points at: dwnx delivers stream payload straight out of the buffer it was
handed (`deps/dwnx/lib/dwnx_conn.c:1631-1636`), which is memory this crate owns, and a reference
count can outlive a call where a borrow cannot.

The handler cannot ask which buffer, because a handler cannot reach the connection — the
property the section above records, with compile-fail cases enforcing it, and the original
reason the delivery was copied. It holds the answer instead: alongside the event queue it
already held, a cell containing the reference-counted handle for the buffer being parsed, which
the read side sets immediately before `Conn::read` and clears immediately after. Nothing in that
cell is a connection handle and no operation on it reaches a connection, so the compile-fail
cases are untouched and still fail for the reasons they name. Whether the borrow really lies
inside that buffer is *checked* by comparing addresses — never by dereferencing one — and a
slice that falls outside is copied out, so an upstream change to where dwnx delivers from costs
a copy rather than producing wrong bytes.

Reclamation is the strong count reaching one. A connection reads into a buffer again only when
every view of it has been dropped; while a caller holds one, the connection takes another rather
than waiting. Waiting was the rejected alternative and it is a stall: a caller is entitled to
hold delivered data indefinitely, and read-ahead — which is what actually bounds memory here —
is accounted in bytes delivered against bytes credited and says nothing about whether the caller
still holds them. Retired buffers are watched for reuse, at most `READ_POOL_LIMIT` of them, and
one that falls off that list is not leaked: it is simply not reused, and is freed with its last
view.

What bounds the memory a single held delivery can pin is not the pool but a threshold. A delivery
shorter than `ALIAS_THRESHOLD` is copied into an allocation of its own, so the largest ratio of
region pinned to bytes carried is one read buffer over that threshold — sixteen, with the
constants as they stand, and `delivery.rs` asserts the arithmetic rather than asking to be
believed. Without it the bound would be nominal: one retained byte would pin 16382, an
amplification of thousands, and "bounded per connection" is not a bound when a caller may hold
any number of deliveries. The threshold is set low deliberately, biased toward aliasing more
rather than less, because the failure it guards against is a caller that keeps one small
delivery for a long time and that is the rarer shape than a caller streaming a body.

### One runtime, named only when asked

`src/io/tokio.rs`, behind the off-by-default `tokio` feature, is the only place this crate names
a runtime. `TokioStream` wraps anything implementing tokio's `AsyncRead + AsyncWrite` rather
than a socket type, which keeps TCP, unix sockets and TLS sessions over either in reach without
this crate acquiring a TLS seam or naming a transport it has no business naming. The stream is
held pinned in a box: an `S: Unpin` bound would have propagated to every signature mentioning a
connection, and hand-rolled pin projection would have meant `unsafe` in a subtree that has none.

The clock reads tokio's own `Instant` rather than the standard library's, so that a test which
pauses time sees timestamps agreeing with it — and because the structural suite forbids
`std::time` in this subtree.

The loopback tests run the same exchange bodies and the same assertions as the in-memory ones;
only the socket setup differs. Two implementations sharing one test body is the evidence that
the seam is not shaped around either. Two similar bodies would not be.

## Module layout

`ngnet-quic` has thirty-five modules; `ngnet-qmux` has eleven core ones plus the layer, and the
difference is almost entirely protocol rather than ambition. There is no `cid`, `path`,
`packet`, `rand`, `token`, `tls`, `tls_bridge` or `tls_ossl`, because QMux has no connection
IDs, paths, packets, entropy, tokens or cryptography. There is no `retain`, because dwnx copies
stream data into the record and never retransmits, so there is nothing to keep alive on the
caller's behalf — the single largest simplification relative to the QUIC wrapper. Where the
QUIC wrapper has an `endpoint` subtree this one has `io`, and it is smaller for the reason
above: there is no socket to share, so there is no endpoint, no accept loop and no driver.

What remains maps one-to-one onto the C API: `conn` for lifecycle and the read path,
`callbacks` and `handlers` for the event bridge, `write` and `stream_io` for the outbound and
stream operations, and `error`, `params`, `settings`, `stream`, `time` and `ccerr` for the
value types. The layer adds `io/conn` for ownership and the pump, `io/stream` and `io/clock`
for the seams, `io/framing` and `io/close` for the two jobs dwnx cannot do, `io/delivery` for
the bytes an event carries, `io/event`, `io/scheduling`, `io/error`, `io/testing` and — behind
its feature — `io/tokio`.

Module files are flat here as everywhere in this crate: `io.rs` with submodules in `io/`, never
`io/mod.rs`. The rule is not cosmetic. The structural test that reads `lib.rs`'s `unsafe`
allowance list derives a module's name from its file stem, and a `mod.rs` would break it.
