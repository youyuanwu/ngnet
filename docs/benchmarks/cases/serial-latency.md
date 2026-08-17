# `serial_latency`

**Family:** duplex — `tests/ngnet-bench/benches/serial_latency.rs`

One request in flight at a time on a persistent connection, empty body.

```sh
cargo bench -p ngnet-bench --bench serial_latency
```

## What it measures

The per-request headers round trip and the wrapper work around it. With an empty body there
is no payload movement to time, and with a duplex there is no kernel, so what remains is
protocol and wrapper CPU for one exchange.

This is Criterion's home ground: one iteration is one small, repeatable unit of work, so the
mean/median, confidence intervals and outlier detection all mean what they say.

## Arms

| Arm | Stack | I/O |
| --- | --- | --- |
| `ngnet-h2` | this crate | `tokio::io::duplex` |
| `hyper` | hyper | `tokio::io::duplex` |

Both on one `current_thread` runtime. The connection is stood up once outside the timed
closure; each iteration issues one request and drains the response.

## Parameters

None. One group, two arms.

## Reading it

- **This is not tail latency under load.** It is the mean cost of one exchange on an
  otherwise idle connection.
- At N=1 there is nothing to gather and nothing to multiplex, so this case is largely blind
  to the write-path questions the concurrency cases turn on — which makes it useful as a
  near-tie control rather than as a discriminator.
- The socket counterpart is [`transport_serial_latency`](transport-serial-latency.md), and
  the two are **not** two measurements of the same thing.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
