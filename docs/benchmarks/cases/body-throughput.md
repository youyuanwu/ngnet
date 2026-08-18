# `body_throughput`

**Family:** duplex — `tests/ngnet-bench/benches/body_throughput.rs`

A request/response body sweep on a persistent connection, with `Throughput::Bytes` so
Criterion reports MB/s.

```sh
cargo bench -p ngnet-bench --bench body_throughput
```

## What it measures

Payload movement through both stacks with the kernel absent: the copies, the frame
serialisation, the read-buffer pool, and flow control. The server echoes the body, so each
iteration moves `size` bytes up and `size` back; throughput is normalised to one body's
worth, which is the number reported.

At 1 MiB the 64 KiB initial window — matched between all three arms, see
[`../configuration.md`](../configuration.md) — forces repeated `WINDOW_UPDATE` round trips, so
the large end of this sweep is as much a flow-control benchmark as a copy benchmark. That
sentence needs reading twice on the QMux arm: it is given 65535 bytes of credit per stream and
65535 across the connection, matched to libnghttp2's fixed window, so it pays repeated credit
extensions exactly as the HTTP/2 arms do — but the two are not extending quite the same
quantity, since QMux's unidirectional streams spend connection credit where HTTP/2's control
frames do not. [`../configuration.md`](../configuration.md) accounts for that and for the one
setting neither stack exposes.

## Arms and parameters

| Arm | Stack | Protocol |
| --- | --- | --- |
| `ngnet-h2` | this crate | HTTP/2 |
| `ngnet-qmux-h3` | this crate | HTTP/3 over QMux |
| `hyper` | hyper | HTTP/2 |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**. 0 B exercises the headers-only path and is
reported as `Throughput::Elements(1)`, since a `Throughput::Bytes(0)` MB/s figure would be
meaningless; every non-empty size is reported as bytes/sec.

Read pairwise, as in [`serial-latency`](serial-latency.md): `ngnet-h2` against `ngnet-qmux-h3`
varies the protocol, `ngnet-h2` against `hyper` varies the HTTP/2 implementation, and the third
pairing varies both. The arms are registered in that order inside the size loop, so size is the
outer loop and the arms are the inner one.

## Reading it

- The block distribution libnghttp2 produces is sharply bimodal — control and `HEADERS`
  blocks are ≤ ~73 bytes, DATA blocks are 16392–16393, the 9-byte frame header already joined
  to its 16 KiB payload. That fact governs how much any write-side change can move this sweep,
  and it falsified an earlier explanation of these arms; see
  [`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).
- **This is where a cross-protocol per-exchange overhead should be at its smallest.** A fixed
  cost per exchange is amortised over a growing payload, so the QMux arm's ratio against
  `ngnet-h2` should fall as the sweep climbs, and the 0 B point should look like
  [`serial-latency`](serial-latency.md). A ratio that *grows* with body size would be a per-byte
  effect and would need a mechanism. There were three candidates: the record-size difference,
  the join's one-write-per-`IoSlice` offer, and the copies below it. Two have since been
  removed and one measured — coalescing and direct serialisation took the write count and the
  outbound copy out. The six changes together are worth −30.3% at 1 MiB on this group, of which
  coalescing alone accounts for −21.7% and direct serialisation is not resolved separately
  ([`../findings/qmux-write-path.md`](../findings/qmux-write-path.md)), and the remaining
  inbound copy was measured by removing it and found to cost nothing worth having
  ([`../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md`](../data/xeon-8370c-azure/05-qmux-delivery-aliasing.md)).
  The record-size difference is what is left, and it is the one to reach for first now.
  [`../controls.md`](../controls.md) sets out each with its direction.
- The 1 MiB point is historically **the noisiest in the suite**. Treat a single-digit
  percentage move there as neutral unless the drift controls in the same session were quiet.
- For the same sweep with a socket in the way, see
  [`transport_body_throughput`](transport-body-throughput.md); for the same sweep varying the
  body *strategy* rather than the stack, see [`shared_body`](shared-body.md), which carries no
  QMux arm and says why.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
