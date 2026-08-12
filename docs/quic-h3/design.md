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

`RETAINS_BUFFERS` is `false`, which matches: this transport does not read through the
caller's memory.

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
