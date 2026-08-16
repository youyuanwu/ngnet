# HTTP/3 over QMux: pending work

What is missing, and what would settle each.

## Interoperability is proven against nothing

Everything here runs against this workspace's own stack: `ngnet-h3` over `ngnet-qmux` over an
in-memory byte-stream pair or a loopback TCP socket. Both ends of every test are this code.

That is a weaker position than the QUIC join is in, which at least runs against quinn. QMux is
an unratified draft and no other implementation is known to exist, so there is currently
nothing to interoperate *with* — the gap is real and it is not closable by effort here.

**What would settle it:** a second QMux implementation appearing, or dwnx's own example client
and server being driven against this stack. The latter is possible today and has not been done.

## There is no structural test suite

`ngnet-quic-h3` ships `tests/invariants.rs`, which reads its own source and asserts that
nothing here names a socket or a thread, that module files are flat, that nothing is
`include!`d, and that the manifest declares exactly what it should. This crate ships no
equivalent, so the claims on `invariants.md` that would belong to such a suite are marked there
as resting on the compiler or on review instead.

**What would settle it:** the same suite, with the forbidden names adjusted — `TcpStream` is a
plausible thing for a QMux-adjacent crate to acquire by accident in a way it is not for a QUIC
one, so the list is not a copy.

## The connection is not configurable

A connection gets `ngnet_qmux::io::Config::new()` and nothing else. Flow-control windows, the
stream limits and the layer's read-ahead allowance are all fixed at the QMux defaults, and a
caller with a reason to change any of them has no way to. That is deliberate for a first
increment — a knob that only half works is worse than an argument that does not exist yet — but
it is a limitation rather than a position.

**What would settle it:** `connect` and `serve` taking a `Config`, or constructors on
`QmuxConnection` that do. Nothing in the design resists it; it was left out to keep the surface
small until there was a caller with a number.

## The connection is not observable

There is no way to ask a live connection how much read-ahead it is holding, what its peer's
transport parameters said, or how many streams are open. The layer below exposes some of that
on its own `Connection`, and this crate takes ownership of that value at construction, so a
caller holding a `QmuxConnection` cannot reach it.

**What would settle it:** deciding whether accessors belong on this crate's type. The QUIC join
has the same gap for the same reason, and both should be answered together.

## Body bytes are copied twice before this crate sees them

Inbound, the QMux layer copies every record's payload into the framer's retention and copies
each delivery into an owned `Vec` for its event. This crate then turns that `Vec` into `Bytes`
by taking the allocation over, so it adds nothing. Outbound, `StreamSource::write_next` lends
buffers that are invalid once the closure returns, so the bytes are copied into the record
being built — one copy, and the reason `RETAINS_BUFFERS` is `false`.

Both inbound copies are consequences of gaps in dwnx rather than of anything decided here; see
`docs/qmux/pending-work.md`, which records what would remove each.

**What would settle it:** measurement. There are no benchmarks, so the cost is a description
rather than a number.

## A multi-slice offer is written one slice at a time

`StreamSource::write_next` may hand over several `IoSlice`s at once. The layer below has no
vectored write — `RecordWriter::push_vectored` is listed in `docs/qmux/pending-work.md` — so
this crate issues one write per slice and stops at the first that is not fully accepted.

In practice that means one slice per offer, not one record holding all of them. The layer below
refuses a second production while a record is still outstanding, so the write of the second
slice answers `Blocked`, the loop breaks, and the offer reports the count the first slice
produced. A fragmented offer therefore takes one record and one pass through the pump per
slice, where a vectored push would have packed them into a single record. Correct, and more
records than the payload requires.

## The transmit pass yields on a fixed count

A pass takes at most sixty-four offers and then returns, so a layer with an endless supply
cannot keep it from returning to the driver. Sixty-four accepted offers is on the order of a
megabyte, which is a guess rather than a measurement: too low costs wakeups on a large body,
too high delays the events the driver has to attend to.

**What would settle it:** a benchmark showing which end of that trade actually costs anything.

## A response body that fails after delivering everything ends cleanly

Not this crate's behaviour, and not fixable here, but found through it and worth recording
where the next person to write a reset test will look.

When a handler's response body returns an error, `ngnet-h3` ends it with
`BodyOutcome::Eof` and separately records `Ending::Failed`
(`crates/ngnet-h3/src/http/body/outgoing.rs:112-116`). The `Eof` finishes the stream, and the
driver then resets it (`crates/ngnet-h3/src/http/server.rs:361-365`). Those are two signals
about one stream, and which the peer acts on depends on whether anything was still queued
behind the FIN.

With a backlog, the reset discards it and the caller sees a failed body, which is right. With
no backlog -- a body small enough to fit the windows, or a peer that has already drained it --
the FIN has already been delivered, the caller sees a complete body, and the reset arrives for
a stream that is finished. The response was truncated and the caller is not told.

`tests/ngnet-qmux-h3-tests/tests/reset.rs` sidesteps this by keeping a backlog deliberately,
and says so. It is recorded here rather than fixed because the fix belongs in `ngnet-h3`,
where it would change the QUIC path identically and deserves its own reasoning: the choice is
between not finishing a failed body at all and accepting that a truncated response can look
complete.

## No datagrams, no WebTransport, no priority

Neither `ngnet-h3` nor `ngnet-qmux` exposes unreliable datagrams, so this crate cannot.
WebTransport is the QMux draft's other stated motivation and is not implemented anywhere in
this workspace. The transport trait has no priority concept, so nghttp3's support for the
HTTP/3 priority scheme is unreachable from here.

See both families' pending-work documents.

## Nothing serves an axum router over this

`ngnet-axum` serves an axum `Router` without hyper, and doing the same over a QMux-carried
connection is the obvious next thing to want. It is recorded in `docs/qmux/pending-work.md`
rather than here, because the accept side belongs there — this crate takes a byte stream that
is already established and has no opinion about where it came from — and because the shape is
not settled: `ngnet-axum`'s `Listener` seam produces transports it drives with `ngnet-h2`, so
implementing one for QMux would serve HTTP/2, not this.
