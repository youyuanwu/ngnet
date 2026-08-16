# Benchmark cases

One page per bench target. Each says what the case measures, what it varies, what Criterion
reports, and how the result is to be read — but holds no measurements: those are in
[`../data/`](../data/), filed under the machine that produced them.

## The duplex family

This stack against hyper, both on tokio over a `tokio::io::duplex`. No sockets, no syscalls,
no kernel. Varies the HTTP/2 implementation with I/O held constant.

| Case | Varies | Reports |
| --- | --- | --- |
| [`serial_latency`](serial-latency.md) | stack | latency of one empty-body exchange |
| [`concurrent_throughput`](concurrent-throughput.md) | stack × N ∈ {1, 8, 64} | requests/sec |
| [`body_throughput`](body-throughput.md) | stack × body ∈ {0, 1 KiB, 64 KiB, 1 MiB} | MB/s |
| [`shared_body`](shared-body.md) | body strategy × body size | MB/s |

## The real-socket family

Three arms over a real loopback TCP connection, varying the HTTP/2 implementation *and* the
I/O model. Requires the `completion` feature and a host with io_uring.

| Case | Varies | Reports |
| --- | --- | --- |
| [`transport_serial_latency`](transport-serial-latency.md) | stack × I/O model | latency of one empty-body exchange |
| [`transport_concurrent_throughput`](transport-concurrent-throughput.md) | stack × I/O model × N | requests/sec |
| [`transport_body_throughput`](transport-body-throughput.md) | stack × I/O model × body | MB/s |
| [`transport_shared_body`](transport-shared-body.md) | body strategy × I/O model × body | MB/s |

## What every case shares

- The connection is established **once, outside the timed closure**, and each iteration
  issues requests on it and drains the responses. Handshake cost is not in any number here.
- The server echoes the request body, so a body sweep moves the payload up and back;
  throughput is normalised to one body's worth, which is the figure reported.
- Sizes and concurrency points are deliberately identical across families, so the two are
  comparable *in shape*. They are not comparable in magnitude — see the reading rule in
  [`../README.md`](../README.md).
- The 0 B point in every body sweep reports `Throughput::Elements(1)` rather than
  `Throughput::Bytes(0)`, which would be a meaningless MB/s.
- Protocol settings are matched between the stacks; see
  [`../configuration.md`](../configuration.md).
