# HTTP/3 pending work

Known gaps, deferred decisions and things worth doing next for the `ngnet-h3` family. Each
entry records the evidence that produced it and what would settle it, so a later reader can
judge whether it still applies rather than re-deriving the argument.

Everything still pending here was deferred on its merits rather than left half-finished.

The HTTP/2 family keeps its own list in [`../h2/pending-work.md`](../h2/pending-work.md).

## Resolved

### A linear scan on every stream close

**Resolved.** `Driver::close_stream` used to open with `self.closed.contains(&stream)` over a
`Vec<StreamId>` held at `CLOSED_TOMBSTONES = 1024`.
The tombstones exist to discard a late release for a stream already gone, and the bound is there
so the list cannot grow without end. Both are right. What was not considered is that `contains`
is linear, so on any connection that has closed more than 1024 streams — which is every
long-lived one — **every subsequent close scans 8 KiB of stream ids before doing anything**.

What the original mechanism cost, from
[`../benchmarks/data/xeon-8370c-azure/09-qmux-h2-mechanisms.md`](../benchmarks/data/xeon-8370c-azure/09-qmux-h2-mechanisms.md),
measured by changing only that constant from 1024 to 16 and re-timing:

| Workload | 1024 | 16 | change |
| --- | --- | --- | --- |
| empty exchange, duplex | 28.93 µs | 25.56 µs | **−11.7%** |
| empty exchange, socket | 54.98 µs | 51.16 µs | −6.9% |
| 64 concurrent streams, duplex | 1218.26 µs | 975.73 µs | **−19.9%** |

No range overlapped its pair over five runs each. It is 6.9% of the profiled cost of an empty
exchange, and about a quarter of everything this driver spends on one.

**It is the scan and not the `drain` beside it.** Leaving the constant at 1024 but trimming only
at 2048 amortises the drain while lengthening the average scan by half; both arms got *worse*, in
proportion — 30.73 µs and 1318.26 µs. A cost that tracks the length of the scanned list is the
scan.

**What settled it:** the driver now keeps membership in a `HashSet<StreamId>` and insertion order
in a `VecDeque<StreamId>`, behind one component that inserts and evicts from both together.
The bound remains 1024, duplicate closes remain no-ops, and the oldest distinct close is evicted
first. The driver also discards the state machine's callback observation for a close it has
already applied, so a batch larger than the tombstone window cannot replay close side effects
after eviction. Unit tests cross the bound and compare both representations;
`http_closed_streams` distinguishes retained from evicted late releases; and
`closed_stream_membership_never_scans_eviction_order` pins both driver call sites to the hash
index. The valid implementation was then measured directly in
[`10-h3-closed-stream-lookup`](../benchmarks/data/xeon-8370c-azure/10-h3-closed-stream-lookup.md):
**13–18% faster on the tested duplex arms and 7–13% faster on the socket arms**, with every
effect larger than its matching HTTP/2 control movement. The earlier A/B remains diagnostic
evidence of the original mechanism, and **16 is not a correct setting** — it would expire
tombstones that still matter.

The measurement was taken through the QMux join because that is the transport this host can
build. Nothing in the entry is QMux-specific: `close_stream` is in `ngnet-h3`, so
`ngnet-quic-h3` should pay the same cost. That is an inference from shared source, not a
measurement — see the same run's *What it does not*.

### The asynchronous layer

Shipped. `src/http/` behind the default-on `http` feature: a client, a server, `http`-crate
types, `http_body` bodies, and `QuicConnection` abstracting the transport. The question this
entry used to pose — whether the h2 layer's shape transfers — was answered *no* for the part
that mattered. The transport abstraction is not a variation on h2's `Transport`; it is
event-sourced for reads and transport-pulled for writes, for reasons `design.md` records.

What did transfer is everything above the transport: the driver-and-handle split, the
role trait, handler futures the driver polls rather than spawns, and the shape of the error
surface.

### Shared driver work snapshot — settled

The async driver formerly acquired the same shared mutex once per category and then once per
category again around idle and park decisions. It now transfers all five work categories into
driver-owned scratch under one lock, processes them with the lock released, and uses coherent
idle, completion, and under-waker readiness predicates. Matched count builds reduce these method
entries from 155 to 29 per empty exchange; controlled timing improves phase-one duplex/socket
serial by 7.59% / 5.68%. The generic contract and race tests live in this crate; the timed QMux/H3
evidence is [`run 15`](../benchmarks/data/xeon-8370c-azure/15-qmux-h3-shared-snapshot.md).

### QPACK is used but not exposed

The encoder and decoder are driven internally and no public API reaches them. nghttp3 exposes
`nghttp3_qpack_*` as a self-contained, independently useful pair — a caller wanting to encode
a field section outside a connection cannot do it through the safe API today, though
`ngnet_h3::raw` reaches it unsafely.

Deferred because nothing in the specification needed it, and **an API nobody has asked for
gets designed against imagined requirements.** Worth doing when a caller appears with a real
shape in mind.

## Not reachable with the current test transport

### Release accounting under genuine acknowledgement delay

The retain contract's central claim is that a buffer stays alive until the peer acknowledges
it. The in-memory backend proves this by withholding acknowledgement deliberately — it
declares `RETAINS_BUFFERS = true` and must therefore report release explicitly — which is the
sharpest available test of *when* release happens.

What it cannot prove is the same behaviour under a real transport's timing. quinn copies on
write, so it declares `RETAINS_BUFFERS = false` and reports release as soon as a write
returns. That is sound and it exercises the other arm of the constant, but a buffer staying
retained across many writes is never seen over a real connection.

**Settle it with a QUIC implementation that surfaces per-byte acknowledgement** — msquic with
send buffering disabled, or ngtcp2, whose `acked_stream_data_offset` callback is exactly this
signal — or by driving quinn with an artificial delay between accepting bytes and reporting
them released. The second is cheaper and would catch a regression in the accounting even if
it does not reproduce real network timing.

## Deferred in the asynchronous layer

### No adapter for any QUIC library but quinn

The backend trait was designed against the published APIs of quiche, s2n-quic, msquic and
ngtcp2, and `design.md` records what each of them changed about it. But only two
implementations exist — quinn and the in-memory one — so the fit with the other four is
*argued* rather than demonstrated.

This is the residual form of the risk that the trait is accidentally shaped around one
library. Two independent implementations is meaningfully better than one, and it is not the
same as four. **Settle it by writing one**, and ngtcp2 is the most informative: it is the
library nghttp3 was co-designed with, it is the one that forced the pull-shaped write side,
and it is the only one of the four whose native model the trait has never been run against.

### An undelivered release holds its buffer until the stream closes

A transport may report bytes back with `delivered: false` — msquic does, when a send is
cancelled. Those bytes must not reach the state machine as acknowledgement, because claiming
more arrived than ever did is how its offset accounting frees a buffer early. So they are
discarded, and nothing else releases the buffer: it is held until the stream closes.

That errs in the safe direction, but it is still holding. A transport that cancels many sends
on a long-lived connection accumulates them. **Settle it with a way to drop a retained buffer
without reporting acknowledgement**, which the sans-I/O core does not currently offer — it
would be a small addition beside `add_ack_offset`, and it is deliberately not being invented
speculatively before a transport that needs it exists.

### A stream this end resets keeps its body registered until the connection is dropped

`Conn::close_stream` is the only thing that releases a stream's body entry, and the driver
reaches it from one place: a `StreamClosed` event. A transport reports that for a stream the
*peer* finished with, and none of them reports it for a reset this end issued — the layer
asked for the reset, so there is nothing to tell it. So the entry, and whatever body buffers
it still holds, survive until the connection itself is dropped, which drains the registry.

This is true of every locally initiated reset — a caller dropping a response future, an
unread incoming body being dropped, a server abandoning an exchange — and predates the change
that made a failed body reset rather than end its stream. It is bounded by the connection's
lifetime rather than unbounded, and the reset does tell nghttp3 the write side is finished, so
no further bytes are ever offered for the stream; what remains is memory held longer than it
is useful on a long-lived connection that resets many streams.

**Settle it by closing the stream where the reset is issued**, which needs care: the driver
would be declaring a stream closed on its own authority rather than on the transport's report,
and a `StreamClosed` arriving afterwards must not then be a second close. The honest form is
probably for the reset drain to record the stream as closed-by-us and for the event handler to
recognise its own work.

### The trait has no timer, datagram, priority or stream-limit surface

quiche and ngtcp2 both require their caller to arm and fire a timer; an implementation over
either owns one behind `poll_event`, which means "established connection" quietly also means
"and an I/O driver". All four expose datagrams, two expose stream priority, and all four
expose stream-limit signalling that would answer "can this connection even start HTTP/3"
before three unidirectional streams are attempted.

None is needed for request/response over quinn or over ngtcp2, so none is present. Datagrams
are the one with a concrete future use — WebTransport and MASQUE both need them.

The stream-limit point has since been exercised for real. `ngnet-quic-h3` returns pending from
`poll_open_uni` when the peer's limit is exhausted, and that is safe only because ngtcp2's
`extend_max_local_streams_*` callbacks are wrapped to wake it. This layer waits indefinitely
by design and has no timeout underneath, so an implementation without such a signal does not
stall — it hangs.

### Blocked streams are unblocked by retrying, not by a writability signal

When a transport refuses bytes the layer blocks the stream, and it unblocks everything and
retries whenever anything wakes the driver. All four surveyed libraries expose a more precise
signal — `extend_max_stream_data`, `stream_writable`, `poll_send_ready`,
`IDEAL_SEND_BUFFER_SIZE` — that would say *which* stream became writable.

The cheaper mechanism was chosen deliberately and is correct, but it does more work than it
needs to on a connection with many congested streams. **Revisit if a profile shows the
retries mattering.**

### No first-party quinn adapter ships

`QuinnBackend` lives in `ngnet-h3-tests`, which is unpublished. A user wanting HTTP/3 over
quinn has a working implementation to read and copy, and no crate to depend on.

Deliberate: quinn brings rustls and a cryptographic backend, and putting that behind a
feature of a crate whose selling point is having one dependency would undo the thing being
sold. **Revisit if a separate `ngnet-h3-quinn` crate is wanted**, which is where it would go
rather than into the wrapper.

### Self-interop only

Both ends of every exchange are this crate. Two implementations of the same misreading agree
with each other, so wire-format and QPACK mistakes that a third party would catch are not
caught here. Head validation is written against RFC 9114's text rather than against observed
behaviour, which is the mitigation, not a substitute.

**Settle it by running against an independent HTTP/3 implementation** — the `h3` crate, curl,
or nghttp3's own client — as an ignored test or a documented manual procedure.

### Two questions the API survey could not settle

Recorded because they affect adapters not yet written, and because "we checked and could not
tell" is more useful than silence.

- **Does msquic guarantee per-stream FIFO ordering of `SEND_COMPLETE`?** Neither `msquic.h`
  nor its `Streams.md` says. An adapter should correlate by `ClientSendContext` rather than
  assume an ordered byte delta, which sidesteps the question entirely.
- **Does s2n-quic impose `Send` on a driven connection?** Nothing in its public poll APIs
  does, but whether a connection can be obtained and driven without a `Send` endpoint is
  decided in `s2n-quic-transport`, which was not read.

## Judgement calls worth revisiting

### `add_ack_offset` keeps nghttp3's vocabulary

Final review argued the name promises peer acknowledgement while the only real consumer —
quinn — reports on acceptance instead, and proposed `release_sent_bytes` or
`transport_released`.

Kept, because the name matches `nghttp3_conn_add_ack_offset` and renaming would put this
crate's vocabulary out of step with the library it wraps, which is its own kind of trap for
anyone reading both. The contract is stated instead: a copying transport may report on
acceptance, a borrowing one must wait. **Revisit if a second transport integration finds the
name genuinely misleading in practice** rather than in review.

### `FlowCredit` is framing credit only

It excludes body bytes, which the caller credits itself, and excludes credit that arrives
late through `on_deferred_consume`. Review proposed renaming it `FramingCredit`, or folding
all three sources into one lossless accessor.

Kept as three sources, because they genuinely arrive at three different times and merging
them would mean either buffering body bytes the caller has already seen or delaying the
return of credit that is available now. The loss risk — the third source being dropped — was
the real problem, and that is fixed: it is held and drained rather than discarded. **Revisit
the name if users conflate the three in practice.**

### `BodyOutcome::Fail` carries no cause

A body source that fails reports only that it failed; the underlying error is lost, and the
connection-level error that surfaces is a generic callback failure. `ngnet-h2` preserves the
source error with `Fail(BodyError)`.

Deferred rather than rejected. The h2 shape would transfer, and the argument for it is
production diagnosis. **Worth doing before the API stabilises**, since adding a field to the
variant is a breaking change afterwards — though `BodyOutcome` is `#[non_exhaustive]`, which
buys some room.

The motivating example used to be a file read failing mid-body, and that example has moved
rather than gone away. A caller's body failing in the asynchronous layer no longer travels
through `Fail` at all — it abandons its own stream by deferring and being reset, which is
what keeps one exchange's misfortune from poisoning the connection — but the cause is
discarded at that seam too: `Outgoing::next` drops the `http_body` error, and the client is
told `ErrorKind::Body` with a fixed message rather than what its body actually said. So
there are now two places to carry a cause and they would be settled by different changes:
a field on the variant here, and a source error threaded through `Ending::Failed` in
`src/http/`. The second is the cheaper of the two and reaches the caller who is actually
waiting to hear.

### Test hooks are `#[doc(hidden)]` rather than private

`Conn::live_allocations` and `Conn::retained_body_buffers` are public-but-hidden so tests can
assert on native allocation counts and retained buffers. They are semver-visible despite
being absent from rustdoc.

Kept because the alternative — a test-only feature, or moving the assertions into the crate —
costs more than it buys for two accessors that return integers. **Revisit if either grows a
richer return type**, at which point it should either become properly public with stable
semantics or move behind test-only plumbing.

## Deliberate scope boundaries

These are decided, not pending. They are here so a reader does not mistake them for gaps.

- **No QUIC or TLS implementation.** nghttp3 depends on neither and neither does this crate.
  The integration tests happen to use quinn, and that choice reaches no crate but
  `ngnet-h3-tests`. `ngnet-quic-h3` implements this crate's transport trait over the
  workspace's own QUIC stack, and it is a *separate crate* for exactly this reason: a
  dependency-graph test in `ngnet-workspace-tests` fails if a QUIC implementation ever
  appears in this crate's graph.
- **No server push.** nghttp3 does not implement it.
- **`FieldAction::Stop` does not cancel a field section.** QPACK is stateful, so the
  remaining fields must still be parsed; `Stop` stops them being *delivered*. A caller that
  wants the exchange itself to stop resets the stream through its QUIC layer, which is the
  only place a reset can come from.
- **No caller-supplied randomness.** nghttp3 uses a random seed for its internal stream map's
  hash and falls back to zero when the callback is absent. Supplying real entropy would
  harden that map against a peer choosing stream identifiers to force collisions, but this
  crate has no entropy source of its own and inventing one means an I/O or dependency cost.
  Exposing a caller-supplied source is the honest way to do it, and is deferred rather than
  faked.
