# Hyperium H3 over ngtcp2: design

`h3-ngnet-quic` adapts an established `ngnet_quic::endpoint::DetachedConnection` to hyperium
H3's connection, opener, send, receive, bidi, split and unframed traits. Establishing the
connection — the endpoint, its socket, its TLS policy, its driver — remains the caller's, as it
does for the QMux adapter and for `ngnet-quic-h3`.

Two decisions shape everything else, and both were choices between real alternatives.

## Why the detached connection, not the endpoint-driven one

`ngnet-quic` offers two ways to hold a connection. The endpoint-driven `Connection` is the
friendlier API: `write` takes a slice and returns immediately, and the endpoint's own driver
does the packet work. The detached `DetachedConnection` hands the caller the protocol state and
expects it to read datagrams, produce them, and fire the connection's timer itself.

The friendlier one is the wrong one here, for three reasons that compound.

`Connection::write` copies the entire offer into a `Vec` and enqueues it
(`crates/ngnet-quic/src/endpoint/connection.rs`), and the driver re-copies whatever the packet
did not accept. Offered a large body as a single slice, that is quadratic.

It applies no backpressure to the caller at all: `write` always returns `Ok`, appending to an
unbounded command queue. Hyperium's `poll_ready` exists precisely to report whether the
transport can take more, and over that API it would have nothing to report — it would always
say yes, and the queue would grow.

And it would have made the benchmark dishonest. The comparison this crate exists to enable
holds the transport fixed and changes the HTTP/3 implementation. Driving the connection from a
different place, with a different number of copies and no backpressure, changes the transport
too — three confounders in the one measurement that is supposed to isolate one thing.

The detached path calls `write_stream_vectored`, which is the same call `ngnet-quic-h3` makes,
with the same packet-bounded staging, and it exposes `poll_outbound_capacity` as genuine
backpressure. The cost is that this crate owns a pump. That cost is paid in `pump.rs`, and it is
about two hundred lines rather than the twelve hundred that `h3-ngnet-qmux`'s `state.rs` needs —
because `ngnet-quic`'s detached connection already provides inbound queueing, outbound capacity
signalling, waker registration and a sleeper, none of which QMux does.

## Why there is no driver future

`h3-ngnet-qmux` returns a `Driver` the caller must poll. This crate returns only a `Connection`.

Partly that is surface: one constructor and no lifecycle obligation is a smaller thing to learn,
and the crate has no backward-compatibility debt to justify more. Mostly it is symmetry. The
native stack drives its transport from inside the HTTP/3 layer's polls, and it spawns one
HTTP/3 driver task per endpoint. An adapter that additionally required its own task would have
put a task on one side of the benchmark that the other side does not have — a difference with
nothing to do with HTTP/3. So the transport is driven from inside the trait methods hyperium
already calls, exactly as `ngnet-quic-h3` drives it from inside its own.

That leaves one thing a driver task would have provided for free, and it is the subtlest part
of the crate.

## The three wake sources

Liveness rests on three mechanisms that are easy to conflate and must not be.

**Inbound datagrams are `ngnet-quic`'s problem, already solved.** `ConnectionShared` keeps a
*list* of wakers, `DetachedConnection::register` appends to it with a `will_wake` check, and
every routed datagram wakes all of them. So each task that pumps may register without displacing
the others, and this crate must not reimplement it.

**Stream-level fan-out is this crate's problem.** Whichever task happens to pump routes data for
streams that other tasks are parked on, so the pump wakes them. `Core` keeps a waker registry —
the connection-level waker and one per stream — and fans out at the end of a pass. Two details
matter. The registry lives behind its own lock rather than inside `Core`'s, so a fan-out never
re-enters a mutex a pump may be holding. And the fan-out fires only when a pass actually observed
a change; waking unconditionally would have the timer waker re-wake the task that just pumped,
every pass, forever.

**The expiry timer needs a stable wake target, and this is the part a driver task would have
given us.** The expiry is a single `Sleep`, and the endpoint's own timer deliberately does not
cover detached connections. Armed under whichever transient request task pumped last, it would
be left bound to a dead waker the moment that task finished — and during a quiet period, with no
inbound datagram to rescue it, loss recovery and the idle timeout would simply never fire. So
`Core` owns a waker built from `std::task::Wake` and the sleep is polled only under that, never
under a caller's. `std::task::Wake` rather than `futures`' `ArcWake` for a reason a
dependency-graph test enforces: it keeps this a three-crate join.

`tests/liveness.rs` is the regression test, and it earned its place immediately — it caught
`ExpiryOutcome::IdleClose` being reported through `close_error()`, which turned an idle timeout
into ngtcp2's default transport `NO_ERROR` and lost the dedicated `Timeout` variant hyperium
has for exactly this.

## Buffers

ngtcp2 keeps a pointer to accepted stream data until it is acknowledged, so that data must not
move. This crate does not have to arrange that, and saying so plainly is worth more than
re-deriving it at each call site: `write_stream_vectored` stages its own bounded copy and
retains *that*, which is the `RETAINS_BUFFERS = false` contract the native stack already relies
on. Bytes offered here are free the moment the call returns.

What is left is the ordinary obligation partial acceptance creates. Hyperium hands over a whole
logical send in one synchronous `send_data` and expects `poll_ready` to report when the transport
has taken all of it; the transport takes what fits. So the buffer is taken *out* of the stream
state for the duration of one offer and put back with only the accepted prefix consumed. Zero
acceptance puts it back untouched. A partial acceptance is ordinary — a packet filled — so the
remainder is offered again immediately rather than parking, which is what `ngnet-quic-h3`'s
`transmit::drain` does and what keeps the two arms comparable. A packet that carried none of the
stream is re-offered for the same reason; see the next section for why it is a distinct outcome
rather than a zero acceptance.

## The four things a write can do, and why three is not enough

A stream write reports one of four outcomes, and the fourth exists because three of them were
once collapsed into an ambiguity that stalled connections.

`ngtcp2_conn_writev_stream` fills a packet from whatever the connection owes, and the caller's
stream is only one candidate. It may take a prefix, all of it, or nothing; and "nothing" splits
in two. A *zero-length STREAM frame* is something ngtcp2 serialises deliberately, and only for
an offer carrying nothing but `fin` — that frame **is** the end of the stream. A packet with no
STREAM frame at all is the opposite: the caller's stream was skipped because something else was
queued ahead of it, and nothing of the offer left. ngtcp2 separates them by the sign of
`*pdatalen`, `0` against `-1` (`ngtcp2.h:5233-5243`).

`ngnet-quic` used to clamp that sign, so both arrived as `Datagram { accepted: 0 }`. On a body
write the difference is invisible — zero bytes taken either way, offer it again — which is why
it went unnoticed. On a `fin`-only write it is the whole meaning of the call, and this crate
read "declined" as "sent": `poll_finish` recorded the stream finished, the FIN was never
serialised, nothing was in flight for loss recovery to retransmit, and the peer read until its
idle timeout. That was the intermittent stall.

So the transport reports `DatagramWithoutStream` separately, and this crate answers it with
`Offered::Displaced`: send the datagram, keep the offer, try again. Trying again rather than
parking is the right response because a produced packet means the connection had something to
say and has now said it, so the next attempt has room — and the retry loop is bounded, ending
in a self-wake rather than a park on an event that may never arrive.

## The two stream directions

RESET_STREAM and STOP_SENDING are not two spellings of "the stream ended". A peer's
RESET_STREAM abandons the peer's sending side, which is our receiving side; a peer's
STOP_SENDING asks us to abandon ours. Hyperium draws the same line. `StreamState` therefore keeps
two separate terminals, and `poll_data` prefers a clean FIN over an abnormal terminal so a body
that arrived intact is never reported as a failure.

Sharing one field between them is not an exotic bug: it breaks the ordinary case where a server
sends a complete response and then stop-sends the request-body stream.

## Stream state lifetime

A bidirectional stream splits into halves that may be dropped independently and in either order,
over one stream id and one `StreamState`. So the state is reference counted and discarded with
the last handle, not the first. Dropping it early truncates whatever the survivor still had
retained, and then resets a stream that had already finished cleanly.

The other end of that lifetime is the transport's. `state()` creates an entry on demand, so a
routed event naming a stream whose handles have all gone would leave one behind for the life of
the connection; an entry with no handles is therefore discarded when the transport reports the
stream closed.

A locally reset sending half is recorded as a send-side terminal rather than left as a flag,
because ngtcp2 shuts the write side at that moment and refuses anything offered to it
afterwards. Without the record, an ordinary "abandon this request, then let the handle run its
normal finish path" sequence reaches the transport, comes back `ERR_STREAM_SHUT_WR`, and — if
that is treated as a transport failure — takes the whole connection down over one stream. Both
guards are in place: the terminal stops the offer, and a refusal that arrives anyway is reported
at stream level.

## Bounds

Every loop is bounded, so no single poll can monopolise the executor: at most four
read/expire/produce turns per pump pass (`TIMER_TURNS`), at most 64 datagrams produced per turn,
at most 64 write attempts per send.
