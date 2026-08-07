# HTTP/3 pending work

Known gaps, deferred decisions and things worth doing next for the `ngnet-h3` family. Each
entry records the evidence that produced it and what would settle it, so a later reader can
judge whether it still applies rather than re-deriving the argument.

Nothing here is a known defect. Items were deferred on their merits, not left half-finished.

The HTTP/2 family keeps its own list in [`../h2/pending-work.md`](../h2/pending-work.md).

## Deferred at specification time

### No asynchronous layer

`ngnet-h2` has an `http`/`http-body` API behind a default-on feature; `ngnet-h3` has nothing
above the sans-I/O core, and that was decided before implementation rather than discovered
during it. The core is the deliverable, and it is what such a layer would be built on.

Building one is a substantial piece of work with its own design questions — how request and
response bodies map onto a transport the caller owns, how deferral and stream blocking
surface as `Poll`, whether it takes a runtime dependency or a trait. **Settle it by deciding
whether the h2 async layer's shape transfers**, which is not obvious: that layer assumes a
single byte stream, whereas here the caller owns N QUIC streams and must bind three of them
before anything works.

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
it. The in-memory suite proves this by withholding acknowledgement deliberately and walking
it forward one byte at a time, which is the sharpest available test of *when* release
happens.

What it cannot prove is the same behaviour under a real transport's timing. quinn reports no
per-byte acknowledgement; it does take ownership — once `write` returns, the bytes are in
quinn's buffers — so the integration harness reports acknowledgement immediately after a
successful write. That is sound but immediate, so a buffer staying retained across many
writes is never exercised over a real connection.

**Settle it with a QUIC implementation that surfaces per-byte acknowledgement**, or by
driving quinn with an artificial delay between accepting bytes and reporting them released.
The second is cheaper and would catch a regression in the accounting even if it does not
reproduce real network timing.

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
production diagnosis: a file read failing mid-body becomes indistinguishable from any other
callback failure. **Worth doing before the API stabilises**, since adding a field to the
variant is a breaking change afterwards — though `BodyOutcome` is `#[non_exhaustive]`, which
buys some room.

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
  `ngnet-h3-tests`.
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
