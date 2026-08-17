# `serial_latency`

**Family:** duplex — `tests/ngnet-bench/benches/serial_latency.rs`

One request in flight at a time on a persistent connection, empty body. Three arms: two HTTP/2
implementations and HTTP/3 over QMux.

```sh
cargo bench -p ngnet-bench --bench serial_latency
```

## What it measures

The per-request headers round trip and the wrapper work around it. With an empty body there
is no payload movement to time, and with a duplex there is no kernel, so what remains is
protocol and wrapper CPU for one exchange — on the cross-protocol pair, that includes the
transport framing the QMux arm carries beneath its HTTP framing and the HTTP/2 arms do not
carry at all.

This is Criterion's home ground: one iteration is one small, repeatable unit of work, so the
mean/median, confidence intervals and outlier detection all mean what they say.

## Arms

| Arm | Stack | Protocol | I/O |
| --- | --- | --- | --- |
| `ngnet-h2` | this crate | HTTP/2 | `tokio::io::duplex` |
| `ngnet-qmux-h3` | this crate | HTTP/3 over QMux | `tokio::io::duplex` |
| `hyper` | hyper | HTTP/2 | `tokio::io::duplex` |

All three on one `current_thread` runtime. Every connection is stood up once outside the
timed closure; each iteration issues one request and drains the response.

Read pairwise, because the three arms answer two different questions and one non-question:

- **`ngnet-h2` against `ngnet-qmux-h3`** — same crate family, same substrate, same runtime,
  same request and same drain, differing in protocol. This is the cross-protocol comparison.
- **`ngnet-h2` against `hyper`** — same protocol, differing implementation. The comparison that
  predates the HTTP/3 arm, carried unchanged so that runs recorded before it stay comparable.
- **`ngnet-qmux-h3` against `hyper`** — differs in both, and is attributable to neither.

The arms are registered in that order, so the two halves of the cross-protocol pair are emitted
back to back rather than with `hyper` timed between them; [`../controls.md`](../controls.md)
treats that adjacency as a methodological device rather than a presentational one.

The QMux arm completes one exchange inside `establish`, before anything is timed, and the
HTTP/2 arms do not. That asymmetry is deliberate and is what keeps handshake cost out of the
QMux numbers — see [`../controls.md`](../controls.md).

## Parameters

None. One group, three arms.

## Reading it

- **This is not tail latency under load.** It is the mean cost of one exchange on an
  otherwise idle connection.
- At N=1 there is nothing to gather and nothing to multiplex, so this case is largely blind
  to the write-path questions the concurrency cases turn on — which makes it useful as a
  near-tie control rather than as a discriminator for the HTTP/2 pair.
- **For the cross-protocol pair it is the opposite: this is the case that shows the most.**
  With an empty body there is nothing to amortise a fixed per-exchange cost over, and the QMux
  arm carries a whole stream-multiplexing transport under its HTTP framing that the HTTP/2 arms
  do not carry at all. A large ratio here and a smaller one at 1 MiB in
  [`body-throughput`](body-throughput.md) is the signature of a per-exchange overhead rather
  than a per-byte one; the reverse would be the surprising reading and would need a different
  mechanism.
- What that gap does **not** license is a statement about HTTP/2 against HTTP/3, or about QMux
  against TCP — see [`../README.md`](../README.md) on what a cross-protocol comparison licenses,
  and [`../configuration.md`](../configuration.md) for what was held equal between the two.
- The socket counterpart is [`transport_serial_latency`](transport-serial-latency.md), and
  the two are **not** two measurements of the same thing.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
