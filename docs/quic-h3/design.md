# HTTP/3 over ngtcp2: design

`ngnet-quic-h3` implements `ngnet-h3`'s `QuicConnection` trait over `ngnet-quic`. It is the
only crate in the workspace that depends on both families, and it exists as a separate crate
so that neither has to depend on the other: a caller who wants HTTP/3 over some other
transport does not compile ngtcp2 and OpenSSL, and a caller who wants raw QUIC streams does
not compile nghttp3. Three dependency-graph tests hold that line.

## Why the connection is owned here

`ngnet_h3::http::handshake` and `serve` take their transport **by value** and hold it for the
connection's life. That alone would not settle where the ngtcp2 connection lives — a handle
to something shared would satisfy it. What settles it is how the HTTP/3 layer writes.

It fills a packet by calling into its transport and expecting an answer before the call
returns. `StreamSource::write_next` hands out `IoSlice`s pointing into nghttp3's own memory,
documented invalid the moment the closure ends, and the transport must report how many bytes
it took before it can be called again. There is no arrangement in which those bytes reach
another task in time to be written and reported on.

So the connection state has to be reachable synchronously from the value the HTTP/3 layer
owns, and since `Conn` is `Send` but not `Sync` with `&mut self` methods, "reachable
synchronously" means "owned".

Two alternatives were considered and rejected on merit rather than impossibility. Sharing the
connection behind a mutex with the endpoint's driver would force `Sync` onto a type that
deliberately is not, defeat the transport trait's deliberate absence of a `Send` bound, and
split timer ownership between two lockers. Inverting control, so the endpoint polls the
HTTP/3 driver, would couple `ngnet-quic` to `ngnet-h3` — which is the one thing this
arrangement exists to avoid — and would make it impossible for one endpoint to carry
consumers of different kinds.

That is what *detached connections* are for; see `docs/quic/design.md`.

## The pump, and the deadlock it prevents

Every entry point — `poll_event`, `poll_transmit`, `poll_open_uni`, `poll_open_bi` — begins
by draining what arrived, firing the expiry timer if due, and producing whatever the
connection now owes. This is not tidiness.

The HTTP/3 driver's *first* action is `poll_bind`, which calls only `poll_open_uni`, three
times, and reaches nothing else until it succeeds. A client cannot open a stream before the
peer's transport parameters arrive, and those arrive in a packet that is only read if
something reads it. An implementation that moved datagrams only inside `poll_transmit` would
never send its first flight, because `poll_transmit` is not reached until the stream opens,
which is waiting on the flight. The connection deadlocks at step zero, before a single
datagram is sent.

The same shape has two further consequences. While the driver is parked on `poll_event`
waiting for something to happen, acknowledgements and loss probes still fall due — and
`poll_event` must return an *event*, so there is no way to say "I owe the peer a datagram".
And the stream-limit notification that would release an exhausted peer limit arrives in a
packet, which again only gets read if something reads it.

The endpoint's own driver avoids all of this by producing datagrams every pass regardless of
whether the application wrote anything. This crate has to do the same, from wherever it is
called.

That order remains transport-first. The diagnostics can count packets produced by the
standalone transport pass and packets produced while accepting stream data, but cannot say
whether a standalone packet could have carried a simultaneously pending stream prefix.
Inspecting `StreamSource` first would itself change the order because the trait has no
non-consuming pending-data query. The stream-first candidate was therefore deferred without
a source change; the evidence and missing attribution are in
[`run 28`](../benchmarks/data/xeon-8370c-azure/28-ngtcp2-stream-first-gate.md).

`QuicConnection::poll_flush` is therefore immediate here. The ngtcp2 connection does not keep a
QMux-style byte-stream output buffer: pumping has already handed its datagrams to the endpoint,
so there is no deferred output for a suspension hook to discharge. The explicit implementation
is still required so the HTTP/3 driver can make the same progress guarantee for every
transport, without a silent default. The adapter is compiled by the targeted release suites,
both workspace test modes, full all-target clippy, and its warning-denying Rust documentation
build.

## Imminent transport deadlines keep a bounded fallback wake

ngtcp2 folds pacing into the same expiry used for loss recovery and idle timeout. A stream
write can therefore return blocked while stream, connection, and congestion credit all remain,
with its next useful action only nanoseconds away. The detached adapter owns the sleep for that
expiry; the endpoint driver cannot wake a detached connection's HTTP/3 task for it.

Runtime timers may coalesce such sub-tick deadlines with the task currently being polled. The
adapter keeps the ordinary expiry sleep and also schedules a bounded fallback wake when the
deadline is within 100 microseconds. The fallback is capped at 64 wakes for one unchanged
deadline and resets when ngtcp2 changes the deadline, the sleep becomes ready, or the
connection has no expiry. It is therefore deadline-backed rather than an unconditional retry
loop.

This rule was added for S9, the intermittent native large-body stall. Same-occurrence traces
showed blocked writes with positive stream, connection, and congestion credit, an armed expiry
15 ns to 11.8 µs away, no subsequent timer-ready or driver wake, and eventual idle timeout.
The full reliability record is
[`03-native-h3-s9-timer-wake.md`](../benchmarks/data/epyc-7763-azure/03-native-h3-s9-timer-wake.md).

## A stream's close needs a batch of its own

Found by running it. The whole exchange worked — request sent, response body received intact
— and then the connection failed with `H3_STREAM_CREATION_ERROR`.

The HTTP/3 driver drains events in batches, and *within* a batch it handles control-plane
events before data events. Deliver a stream's close in the same batch as that stream's last
bytes and the close is processed first: the state machine releases the stream, and then the
data event is read against a stream it has already forgotten. That is the ordinary path where
a response ends, not an edge case, and it fails every time.

So a close waits until nothing else has been handed over since the last `Poll::Pending`,
which puts it at the head of a batch. Removing that rule makes the basic request-and-response
test fail again, which is how the rule is kept honest.

## Release is reported on acceptance

`QuicEvent::Released` feeds nghttp3's acknowledgement accounting, so reporting it on genuine
peer acknowledgement would be more truthful — and this is the only transport in the workspace
that *has* a genuine acknowledgement signal, since ngtcp2's `acked_stream_data_offset`
delivers exactly the delta the event wants.

It is reported on acceptance anyway, and the reason is that `ngnet-quic` already copies.
Every accepted write is staged into an allocation the crate owns, because ngtcp2 keeps the
pointer it was handed and a borrowed slice cannot outlive the call. The HTTP/3 layer's
buffers are therefore its own again the moment a write returns. Deferring release until
acknowledgement would hold every in-flight byte *twice* — once in nghttp3's buffer and once
in the retained copy — for no benefit, because nghttp3 does not retransmit. QUIC does, out of
the copy.

The borrowed copy is packet-bounded. One call stages at most ngtcp2's current maximum transmit
UDP payload; if that prefix omits any caller suffix, FIN is withheld until the true final
prefix. A zero-byte final write remains valid. ngtcp2 may accept less than the staged prefix,
so the complete packet-sized backing remains stable until the accepted bytes are acknowledged
or the stream closes. The stream's cumulative retention offset survives a temporarily empty
chunk queue and is forgotten only on stream close.

`RETAINS_BUFFERS` is `false`, which matches: this transport does not read through the
HTTP/3 layer's memory. `QuicEvent::Released` therefore still reports acceptance, while
ngtcp2's acknowledgement releases the transport-owned backing.

## Backpressure retries require new information

The detached outbound queue is bounded at 64 datagrams in total. Normal production may occupy
63 slots; the final slot is reserved because synchronous `QuicConnection::close` cannot
return `Pending`. A close waits behind every already-produced datagram, so it neither exceeds
the bound nor discards/reorders earlier output. Before normal production, the adapter
atomically observes capacity or registers its waker while those 63 slots remain full. The
first full-to-available transition consumes that registration; later removals do not create
duplicate retries.

A stream-write call that produces a transport-only datagram while accepting zero stream bytes
ends the current drain, avoiding an immediate inner-loop reoffer. Diagnostics distinguish
that local packet production and a generic driver wake from true inbound-datagram,
timer-fire, or outbound-capacity events. The outer HTTP/3 driver can still poll the same
prefix again without a proven external/sendability generation; diagnostics report that
honestly as `zero_accept_retries_without_enable`. Changing production scheduling to enforce a
generation gate is deferred until a reproduced defect justifies that broader redesign.

## Peer-opened unidirectional streams are not announced

The trait is explicit that `QuicEvent::Accepted` is for peer-opened *bidirectional* streams.
A peer's unidirectional streams — its control stream and its two QPACK streams — need no
event, because nghttp3 reads the HTTP/3 stream-type prefix itself to work out what it is
looking at. Announcing them would tell the layer to answer on a stream that exists to be
read. ngtcp2's `stream_open` callback fires for all peer-opened streams, so the filter is
this crate's to apply.

## Both close directions come from ngtcp2

`QuicEvent::StreamClosed` carries a code per direction. An earlier plan was to synthesise
them by remembering resets and stop-sendings seen earlier, which would have been lossy.
ngtcp2 has supplied both directly since 1.25 through `stream_close2`, and `ngnet-quic` now
binds that rather than the older single-code callback. See `docs/quic/design.md`.

## What this crate does not own

No socket, no runtime, no threads, and no clock of its own. The endpoint owns the socket and
the routing table; the clock travels with the connection, for reasons described in
`docs/quic/design.md`. A structural test asserts that none of those names appear in this
crate's source.
