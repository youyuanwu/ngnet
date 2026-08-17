# Benchmark cases

One page per bench target. Each says what the case measures, what it varies, what Criterion
reports, and how the result is to be read — but holds no measurements: those are in
[`../data/`](../data/), filed under the machine that produced them.

## The duplex family

This stack against hyper, and this stack against itself over a different protocol, all on
tokio over a `tokio::io::duplex`. No sockets, no syscalls, no kernel.

| Case | Varies | Reports |
| --- | --- | --- |
| [`serial_latency`](serial-latency.md) | stack × protocol | latency of one empty-body exchange |
| [`concurrent_throughput`](concurrent-throughput.md) | stack × protocol × N ∈ {1, 8, 64} | requests/sec |
| [`body_throughput`](body-throughput.md) | stack × protocol × body ∈ {0, 1 KiB, 64 KiB, 1 MiB} | MB/s |
| [`shared_body`](shared-body.md) | body strategy × body size | MB/s |

Two axes, never varied together in a pair worth reading: `ngnet-h2` against `hyper` varies the
HTTP/2 implementation, `ngnet-h2` against `ngnet-qmux-h3` varies the protocol.
[`shared_body`](shared-body.md) varies neither and carries no QMux arm; its page says why.

## The real-socket family

Up to four arms over a real loopback TCP connection, varying the HTTP/2 implementation, the
I/O model *and* the protocol — one axis per pair. Requires the `completion` feature and a host
with io_uring. QMux runs over the same loopback TCP socket as everything else: it is a
stream-multiplexing layer over a reliable byte stream, not a UDP transport.

| Case | Varies | Reports |
| --- | --- | --- |
| [`transport_serial_latency`](transport-serial-latency.md) | stack × I/O model × protocol | latency of one empty-body exchange |
| [`transport_concurrent_throughput`](transport-concurrent-throughput.md) | stack × I/O model × protocol × N | requests/sec |
| [`transport_body_throughput`](transport-body-throughput.md) | stack × I/O model × protocol × body | MB/s |
| [`transport_shared_body`](transport-shared-body.md) | body strategy × I/O model × body | MB/s |

## What every case shares

- The connection is established **once, outside the timed closure**, and each iteration
  issues requests on it and drains the responses. Handshake cost is not in any number here.
  The QMux fixtures go one step further and complete a whole exchange during establishment,
  because their first exchange is not like the rest; the asymmetry with the HTTP/2 fixtures
  and the direction it biases are recorded in [`../controls.md`](../controls.md).
- The server echoes the request body, so a body sweep moves the payload up and back;
  throughput is normalised to one body's worth, which is the figure reported.
- Sizes and concurrency points are deliberately identical across families, so the two are
  comparable *in shape*. They are not comparable in magnitude — see the reading rule in
  [`../README.md`](../README.md).
- The 0 B point in every body sweep reports `Throughput::Elements(1)` rather than
  `Throughput::Bytes(0)`, which would be a meaningless MB/s.
- Protocol settings are matched between the stacks — both between the two HTTP/2
  implementations and, separately and to a different standard, between the HTTP/2 and HTTP/3
  arms. See [`../configuration.md`](../configuration.md), which accounts for every value in
  the comparison set individually.
- Arms are registered so that the two halves of a comparison are timed adjacently: each QMux
  arm sits immediately after its HTTP/2 counterpart, and no pre-existing arm moved relative to
  another. Emission order is itself a control ([`../controls.md`](../controls.md)).

## Three groups carry no QMux arm, for two unrelated reasons

Do not read these as one fact:

| Group | Reason |
| --- | --- |
| [`shared_body`](shared-body.md) | No counterpart mechanism. The arms differ in a libnghttp2 no-copy body path that `ngnet-qmux-h3` has no equivalent of, so there is nothing to put in the arm. |
| [`transport_shared_body`](transport-shared-body.md) | The same reason as its duplex counterpart. |
| `concurrent_throughput_multi_thread` ([page](concurrent-throughput.md)) | A recorded defect. The arm exists and hangs at high concurrency on a multi-worker runtime, intermittently — see [`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md). |

The first two would end if `ngnet-qmux-h3` grew a no-copy body path; the third would end if the
join were fixed. Nothing about either would resolve the other.
