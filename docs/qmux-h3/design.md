# HTTP/3 over QMux: design

`ngnet-qmux-h3` implements `ngnet-h3`'s `QuicConnection` trait over `ngnet-qmux`'s asynchronous
layer, so HTTP/3 runs over TCP, a unix socket, a TLS session, or anything else that delivers
bytes in order. It is the point the QMux work was building towards: the draft's own motivation
is running HTTP/3 and WebTransport over a byte stream, and `ngnet-h3` already speaks HTTP/3
over an abstract transport whose operations *are* QUIC's stream operations. QMux's are the same
operations. The two fit, and this crate is the join.

It is a separate crate so that neither family has to depend on the other: a caller who wants
HTTP/3 over QUIC does not compile dwnx, and a caller who wants raw QMux streams does not
compile nghttp3. Dependency-graph tests hold that line in both directions, and a third asserts
the join is still a join rather than a crate that quietly lost one of its halves.

A caller brings an established byte stream and a clock, and nothing else. There is no dialling,
no listening, no TLS and no timer here, because there is none in the layer below either.

## Why the connection is shared rather than owned outright

`ngnet_h3::http::handshake` and `serve` take their transport **by value** and hold it for the
connection's life, and the HTTP/3 layer fills a record by calling into that transport and
expecting an answer before the call returns: `StreamSource::write_next` hands out `IoSlice`s
documented invalid the moment the closure ends. There is no arrangement in which those bytes
reach another task in time to be written and reported on. So the QMux connection has to be
reachable synchronously from the value the HTTP/3 layer holds.

`ngnet-quic-h3` stops there and owns its connection outright. This crate cannot, and the reason
is `close`. The HTTP/3 driver's *last* act is to call it and return, and it never polls the
transport again — so whatever `close` queued sits in a buffer with nobody left to write it.
Something outside the driver must therefore be able to reach the connection after the driver
has finished with it, which means two handles onto one connection.

The lock is a `std::sync::Mutex`, and not for threading: the two holders are never polled at
once and this crate spawns nothing. It is because `Mutex<T>` can be built for a `T` that is
neither `Send` nor `Sync` while still being `Send` when `T` is. A connection over a `Send` byte
stream can therefore go to a work-stealing runtime, and one over an `Rc`-based byte stream is
still served. A `RefCell` would rule out the first; demanding `Arc<Mutex<..>>` of the caller's
byte stream would rule out the second. A test builds both surfaces over a non-`Send` byte
stream and completes an exchange over one, so the claim is not merely a bound that compiles.

## The pump, and the deadlock it prevents

Every entry point begins by pumping: flushing what is queued, producing what the state machine
now owes, and reading what has arrived. That is the difference between working and deadlocking,
not tidiness.

The HTTP/3 driver's first action is to open three unidirectional streams, and it reaches
nothing else until it has them. A QMux endpoint cannot open a stream before the peer's
transport parameters arrive — every limit is zero until they do — and they arrive in a record
that is only read if something reads it. An implementation that moved bytes only inside
`poll_transmit` would never read that record, because `poll_transmit` is not reached until the
streams open, which is waiting on the record. The same shape strands the window updates that
would release a peer whose flow control is exhausted.

The pump is also what a transmit pass does *between* offers rather than only after them, and
there the pump is deliberately a different one. It used to be the flushing pump, because QMux
permitted one record outstanding at a time and refused the next offer until the last record had
reached the byte stream; skipping it would have turned a large body into one record per wakeup.
QMux now holds several records — see "Produce up to the ceiling, write once, then read" in
`docs/qmux/design.md` — so the intermediate pump is `pump_buffered`: it still reads, and it still
writes when the buffer has no room for another record, but it leaves what the pass has produced
to accumulate. A flushing pump here would now buy nothing and cost a write per record, which is
the whole of what the coalescing was for.

What flushes instead is the single pump *after* the loop, and that one is not optional. A pass
returns to its driver, and no other call is obliged to come along and move what it left behind;
a driver may not poll again until the peer says something, and the peer may be waiting for
precisely those bytes. Every entry point of the layer below that a caller can stop polling after
flushes for the same reason — the one exception is the buffered pump itself, whose caller is
mid-pass and owes the flush at the end of it.

And it is why the pump's answer is read rather than discarded. `try_write_stream` refuses an
offer once the outbound buffer has no room for another record, so a transmit pass that kept
offering after the pump reported "no room" would collect a run of spurious `Blocked` verdicts and
teach the HTTP/3 layer that its streams are stalled when only the socket is — taking them out of
the running until something else happened to wake them.

## Nothing here may park

`poll_transmit` is handed a `Context`, but the closure it passes to `StreamSource::write_next`
is *synchronous*: it receives a stream, some slices and a flag, and must return a
`WriteOutcome`. There is no context inside it and nothing to park on. `close` is worse — it is
not a poll method at all.

That constraint is why the layer below grew `try_write_stream` alongside `poll_write_stream`.
The parking form has no answer to give a synchronous closure: it would have to block, which it
cannot, or truncate, which loses bytes. The non-parking form reports `Accepted(n)`, `Blocked` or
`Closed` and returns, and those map onto the trait's own three outcomes directly.

An offer may carry several `IoSlice`s, and the whole list goes down in one call:
`Connection::try_write_stream_vectored` in the layer below submits the fragments as one vector
array, so they share records and a slice boundary costs nothing. That is the vectored *push*,
which the layer below built; it is not a gathered write to the byte stream, which the layer
below has settled against ever having (`docs/qmux/pending-work.md`). The two are easy to
confuse and only one of them exists.

Before that, this crate issued one write per slice and stopped at the first not fully accepted,
which cost a record per slice. The stopping rule survives the change and now applies to the
offer as a whole: a short accept means the peer's window is exhausted or the outbound buffer has
reached its ceiling, and offering more of the same stream anyway would put its bytes out of
order. It once meant a third
thing, that the record had filled, and that reading is what made this break wrong for a while:
the layer below took one record per call, so a large offer answered short with the buffer
three-quarters empty and the stream was stood down over a record boundary. The distinction was
settled where it is visible rather than compensated for here — `try_write_stream` fills records
until a bound stops it, so a short accept means a bound. The end-of-stream marker is dwnx's
to place: it applies the marker when the data one call handed it fits entirely, so the marker
rides the record that takes the last byte of the whole offer and a partial accept cannot end a
stream early. Nothing here computes which slice is the last one — an empty fragment is not
submitted at all, so a trailing empty slice cannot take the marker away from the payload in
front of it. An offer carrying no bytes *and* the marker is the one empty offer that must still
reach the layer below, because it is the only way a stream that has finished writing is ended;
the short-circuit for empty offers is conditioned on the marker's absence for that reason.

A pass takes a bounded number of offers and then returns. A layer with an endless supply — a
large body — could otherwise keep the pass from returning to the driver, which has a peer to
attend to. The bound is on offers rather than on bytes, and each offer is now worth as many
records as the outbound buffer will hold rather than one, so the cap is looser in bytes than it
reads; the buffer's own ceiling is what bounds the bytes.

## The close nobody would otherwise write

`QuicConnection::close` is synchronous, so this crate records the reason and writes nothing.
Encoding a close means being prepared to wait for a byte stream that may not be taking bytes,
and a method with no context can only drop it when the stream is full. A dropped close is worse
than none: the peer waits out an idle timeout instead of learning why the connection ended.

The queued close is written by *this crate's own* connection future — the one `connect` and
`serve` return. After the HTTP/3 driver resolves, that future runs a tail: append the encoded
close behind whatever is still queued, flush it, and shut the write side of the byte stream
down. Only then does it hand back the driver's outcome. Holding the outcome back is deliberate;
a caller who saw `Ok(())` and dropped the connection would drop the close with it.

Where the driver failed or was dropped without closing, the tail flushes what it wrote and
shuts down anyway. `QmuxConnection::poll_finish` is public for the same reason: a caller who
drove `ngnet_h3::http::handshake` themselves has the identical obligation and no other way to
discharge it.

The test that matters watches a real peer decode the close frame and read its code. A close
that was encoded and never written would pass every test that only asked what this side did.

## Peer-opened unidirectional streams are not announced

The trait is explicit that `QuicEvent::Accepted` is for peer-opened *bidirectional* streams. A
peer's unidirectional streams — its control stream and its two QPACK streams — need no event,
because nghttp3 reads the HTTP/3 stream-type prefix itself to work out what it is looking at.
Announcing one would tell the layer to answer on a stream that exists to be read, and the
answer would be a protocol violation on a stream the peer will never read.

The translation checks the initiator as well as the directionality. QMux raises its open event
for peer opens only, so the initiator test is redundant today — and dropping it would announce
this endpoint's own streams the day the layer below started reporting them, with the failure
looking like the HTTP/3 layer answering its own requests.

## A stream's close needs a batch of its own

The HTTP/3 driver drains events in batches and applies the control-plane ones before the data
ones *within* a batch. Deliver a stream's close in the same batch as that stream's last bytes
and the close is applied first: the state machine releases the stream, and the data event that
follows is read against a stream it has already forgotten. That is the ordinary path where a
response ends, not an edge case.

So an event that ends a stream — and the connection's own ending, which ends every stream at
once — waits until nothing else has been handed over since the last `Poll::Pending`, which puts
it at the head of a batch. This is the same rule `docs/quic-h3/design.md` records, found the
same way, and it is here because it is a property of the HTTP/3 driver rather than of either
transport.

Events are translated one at a time, with a single lookahead, rather than drained wholesale
into a buffer here. The layer below measures its read-ahead in bytes it has *delivered*, so
emptying its queue into this crate would tell it the reader had caught up when the bytes had
merely moved.

## Release is reported on acceptance, from one place

`RETAINS_BUFFERS` is `false`: a write is packed into the record the connection is building, so
the HTTP/3 layer's memory is its own again the moment the write returns.

Retaining instead was considered and rejected, and the reason is particular to this transport.
QMux has **no acknowledgement signal** — the substrate underneath it is already reliable and
ordered, which is the whole point of the protocol — so a retaining implementation would have to
invent a moment to release at, and the only honest one is the moment the copy is made. Where
`ngnet-quic-h3` reports on acceptance despite having a genuine acknowledgement available, this
crate reports on acceptance because there is no other candidate.

The accepted count and the release come from **one running total**, computed in one place. A
second source for either number is a second chance for the two to disagree, and disagreement is
expensive in both directions: reporting too few holds the application's buffers for the
connection's life, and reporting too many tells nghttp3 that bytes it is still reading through
have been dealt with. The test asserts an equality against what the writes actually accepted,
so it fails either way; a one-sided "at least everything was released" would pass while the
second bug was live.

Bytes accepted but then refused are still counted. A refusal is the offer's answer only while
nothing has been taken; once bytes are in the record the layer must hear the count, or it
offers them a second time and the stream carries them twice.

## Refusals that are absorbed, and refusals that are not

The HTTP/3 layer resets streams it has stopped tracking and credits streams that have since
gone, as a matter of course: a cancelled request whose peer had already finished is the
ordinary case, not a corner. It has no way to distinguish a stream that is gone from one that
never existed, and neither refusal tells the peer anything it does not already know.

So a refusal from the state machine that reports "this endpoint has no such stream" is
absorbed. `extend_credit` discards it outright; `shutdown_stream` absorbs it only when the
layer below classifies it as an internal refusal, and any other failure ends the connection.
Failing the connection over either would kill a healthy connection carrying every other
exchange, every time one exchange was cancelled.

## Orderly endings are events; failures are errors

The layer below distinguishes a peer that closed, a byte stream that ended between records, a
byte stream truncated mid-record, a protocol violation and a transport failure. This crate
collapses that into one question — was it orderly? — because that is the only distinction the
HTTP/3 layer can act on. An orderly ending becomes `QuicEvent::Closed`, which the driver treats
as "the peer is gone" and winds down on, reporting success to a caller whose exchanges had
already finished. Anything else is returned as an error and fails the connection.

Getting that backwards is expensive in the direction that is easy to miss: a client that hangs
up politely without closing produces a byte stream that ends between records, and reporting
that as a failure turns every well-behaved disconnection into a server-side protocol error. A
test pins it over a real loopback socket.

The ending is latched the first time it is observed and reproduced on every later call, because
every operation on a dead connection has to fail somehow. It is rendered to a string when it is
latched rather than kept as a source, since a boxed error cannot be cloned and handing the real
one out only to the first caller makes the diagnostic depend on which call won a race.

## What this crate does not own

No socket, no runtime, no threads, no timer and no clock of its own — the clock is the
connection's, so that a timestamp the HTTP/3 layer records is comparable with the one the layer
below stamps against.

It holds no configuration *state* either, in the sense that matters here: the two `Config`
values `connect_with` and `serve_with` accept are consumed at construction and handed to the
layers that own them — the transport half to `ngnet-qmux`, the HTTP half to `ngnet-h3`. This
crate keeps neither, and there is nothing to ask a `QmuxConnection` about its settings because
it does not know them. `connect` and `serve` remain as the no-argument forms, forwarding the
defaults, so a caller with no opinion is not made to have one.

The two are given distinct names at this crate's boundary — `TransportConfig` and `HttpConfig`,
re-exports of the underlying types rather than wrappers — because a signature taking two
configurations that are both called `Config` cannot be read at the call site. Nothing is added
and nothing is hidden by the renaming: a value built through `ngnet-qmux` or `ngnet-h3`
directly is the same value.

What a caller still cannot do is change any of it afterwards. That is a real gap rather than a
narrowing, and for the stream allowance in particular it has a failure mode; see
[`pending-work.md`](pending-work.md).
