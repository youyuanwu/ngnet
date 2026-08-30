# QUIC-stack HTTP/3 body throughput

`quic_stack_body_throughput` sends a 1 KiB body over each persistent connection. Each server
fully collects and echoes it, and each client drains the response. Criterion declares 2 KiB
per iteration because the payload crosses the connection in both directions.

The arms are the three complete stacks listed in
[`quic-stack-serial-latency.md`](quic-stack-serial-latency.md). Runtime shape, loopback UDP,
ALPN, certificate trust, request, response, and drain are equivalent; QUIC/TLS defaults remain
those of each production stack.

Larger points are deliberately absent. Repeated 16 KiB exchanges through `ngnet-quic-h3`
showed high variance and could stall or close the connection. Repeated 1 MiB exchanges could
terminate the optimized process with `SIGSEGV`. The Quinn-only body target remains the
appropriate runnable comparison at those sizes until the ngtcp2 path is fixed.
