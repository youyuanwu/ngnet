# h3-ngnet-qmux

`h3-ngnet-qmux` implements hyperium H3's per-stream QUIC traits over an
already-established [`ngnet-qmux`](https://docs.rs/ngnet-qmux) asynchronous
connection. It is unpublished while QMux remains an evolving draft.

## Ownership and progress

Construction takes a QMux client or server connection and returns an H3-facing
connection plus one `#[must_use]` `Driver`. The caller must poll that driver
concurrently for the lifetime of every connection and stream handle. The crate
spawns nothing and owns no endpoint, listener, socket, TLS policy, runtime,
executor, or timer.

All handles share one `Arc<Mutex<Core<S, C>>>`. A stable proxy waker is the only
waker passed to QMux; accept, open, receive, send, finish, and driver operations
keep independent current waiters above it. Each poll performs bounded
opportunistic work: at most one lower read batch and 64 routed events.

Hyperium's synchronous `close` records the first application reason and wakes
the driver. Delivery is complete only when the driver finishes. Dropping all
capable handles and the driver before another poll can lose buffered output,
including that close.

## Streams and flow control

- Framed sends retain at most one generic `WriteBuf<B>` and walk every
  `Buf::chunk()` without copying the body into another body-sized buffer.
- Unframed sends never retain the borrowed buffer and advance only the exact
  accepted prefix.
- Receive data returns stream and connection credit when it is handed to H3,
  not when QMux routes it.
- Stopped or abandoned receives discard queued and later data, return only
  connection credit, and do not reopen the stopped stream.
- Finish, reset, stop, split-half drop, and repeated terminal polls are
  idempotent. A split half cannot invalidate its sibling.

Pending peer streams are bounded by `AdapterConfig::pending_accept_limit`
(default 128). Exceeding it is connection-fatal with `H3_EXCESSIVE_LOAD`;
hyperium exposes no per-stream rejection result. QMux stream allowances are
cumulative lifetime budgets and are not recycled when streams close.

Connection errors preserve application-close codes. Peer resets and
`STOP_SENDING` preserve stream codes. Byte-stream, protocol, truncation, and
other QMux failures map to hyperium's undefined underlying failure; the adapter
does not invent timeouts.

## Runtime neutrality and diagnostics

The stream, clock, and body buffer have no imposed `Send` bound. Sendability
follows those supplied types, so `Rc`-based thread-per-core streams and
sendable Tokio streams are both supported.

The off-by-default `diagnostics` feature adds an `ObservedStream<S>`, explicitly
armed counters, snapshots, and interval drains. Default builds contain none of
that path. A feature-enabled but unarmed build still pays arming checks and
receive-gauge calculation sites; use it for diagnostics, not timing.

See [`docs/h3-ngnet-qmux/`](../../docs/h3-ngnet-qmux/) for design, invariants,
and known limits.
