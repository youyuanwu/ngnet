# `body_throughput`

**Family:** duplex — `tests/ngnet-h2-bench/benches/body_throughput.rs`

A request/response body sweep on a persistent connection, with `Throughput::Bytes` so
Criterion reports MB/s.

```sh
cargo bench -p ngnet-h2-bench --bench body_throughput
```

## What it measures

Payload movement through both stacks with the kernel absent: the copies, the frame
serialisation, the read-buffer pool, and flow control. The server echoes the body, so each
iteration moves `size` bytes up and `size` back; throughput is normalised to one body's
worth, which is the number reported.

At 1 MiB the 64 KiB initial window — matched between the two stacks, see
[`../configuration.md`](../configuration.md) — forces repeated `WINDOW_UPDATE` round trips, so
the large end of this sweep is as much a flow-control benchmark as a copy benchmark.

## Arms and parameters

| Arm | Stack |
| --- | --- |
| `ngnet-h2` | this crate |
| `hyper` | hyper |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**. 0 B exercises the headers-only path and is
reported as `Throughput::Elements(1)`, since a `Throughput::Bytes(0)` MB/s figure would be
meaningless; every non-empty size is reported as bytes/sec.

## Reading it

- The block distribution libnghttp2 produces is sharply bimodal — control and `HEADERS`
  blocks are ≤ ~73 bytes, DATA blocks are 16392–16393, the 9-byte frame header already joined
  to its 16 KiB payload. That fact governs how much any write-side change can move this sweep,
  and it falsified an earlier explanation of these arms; see
  [`../findings/write-path-and-gathering.md`](../findings/write-path-and-gathering.md).
- The 1 MiB point is historically **the noisiest in the suite**. Treat a single-digit
  percentage move there as neutral unless the drift controls in the same session were quiet.
- For the same sweep with a socket in the way, see
  [`transport_body_throughput`](transport-body-throughput.md); for the same sweep varying the
  body *strategy* rather than the stack, see [`shared_body`](shared-body.md).

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md).
