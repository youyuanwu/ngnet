# QUIC-stack HTTP/3 body throughput

`quic_stack_body_throughput` sends a 1 KiB body over each persistent connection. Each server
fully collects and echoes it, and each client drains the response. Criterion declares 2 KiB
per iteration because the payload crosses the connection in both directions.

The arms are the three complete stacks listed in
[`quic-stack-serial-latency.md`](quic-stack-serial-latency.md). Runtime shape, loopback UDP,
ALPN, certificate trust, request, response, and drain are equivalent; QUIC/TLS defaults remain
those of each production stack.

The Criterion target remains at 1 KiB while larger persistent behavior is qualified. The
fixed-count probe supports 16 KiB and 1 MiB specifically to classify the previously observed
stall/close and native-termination failures before either size can become a performance
guard. A successful single body is not qualification: the required stress point is 125
exact sequential exchanges, and 1 MiB additionally uses fresh 125/250/500-process RSS
sampling. See [Running the benchmarks](../running.md#fixed-count-ngtcp2-probe-modes) for the
separate unarmed timing and armed diagnostic procedures.
