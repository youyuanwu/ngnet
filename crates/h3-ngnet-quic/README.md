# h3-ngnet-quic

Hyperium [`h3`](https://crates.io/crates/h3) transport traits over an established
[`ngnet-quic`](../ngnet-quic) connection.

> **Not ready for use.** This adapter intermittently stalls under a repeated small-body
> workload — roughly two runs in five at 200 x 1 KiB — leaving a connection to sit until its
> idle timeout. It is this crate's own defect, not the separately known `ngnet-quic-h3`
> large-body stall; the native stack passes the identical workload. See
> [`docs/h3-ngnet-quic/pending-work.md`](../../docs/h3-ngnet-quic/pending-work.md).

This is the join that lets the community HTTP/3 state machine run on this workspace's
ngtcp2-backed QUIC transport. It is the counterpart to
[`ngnet-quic-h3`](../ngnet-quic-h3), which joins the same transport to this workspace's own
HTTP/3 implementation, and to [`h3-ngnet-qmux`](../h3-ngnet-qmux), which joins hyperium's
HTTP/3 to the QMux transport instead.

## Using it

The caller builds an endpoint, spawns its driver, establishes a **detached** connection, and
hands it over. Everything after that is ordinary hyperium `h3`.

```rust,ignore
let (endpoint, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
    .build_detachable()?;
tokio::spawn(driver);

let detached = endpoint.connect_detached(remote, Some("example.com")).await?;
let (mut h3, mut send_request) = h3::client::builder()
    .build(h3_ngnet_quic::from_detached(detached))
    .await?;
```

The server side is the same with `accept_detached` and `h3::server::builder()`.

## What it owns, and what it does not

It owns the HTTP/3-facing view of one connection: stream opening and acceptance, the
retained-write state machine behind hyperium's `send_data`/`poll_ready` pair, stream and
connection termination, and flow-control credit.

It owns no endpoint, socket, TLS configuration, runtime, task or timer, and it never spawns.
The endpoint's own driver must stay polled, because it owns the socket.

There is deliberately **no driver future** to poll. The transport is driven from inside the
trait methods hyperium already calls, exactly as `ngnet-quic-h3` drives it from inside its
own — which keeps the public surface to one constructor and keeps the spawned-task count equal
to that stack's, so the two can be benchmarked against each other without a task-count
difference confounding the result.

Because a detached connection is driven by nothing but its owner, a caller that establishes a
client connection and then waits for the peer to accept without polling anything will wait
forever. Handing the connection to hyperium immediately is what avoids this; see the crate
documentation.

## Documentation

- [`docs/h3-ngnet-quic/design.md`](../../docs/h3-ngnet-quic/design.md)
- [`docs/h3-ngnet-quic/invariants.md`](../../docs/h3-ngnet-quic/invariants.md)
- [`docs/h3-ngnet-quic/pending-work.md`](../../docs/h3-ngnet-quic/pending-work.md)
