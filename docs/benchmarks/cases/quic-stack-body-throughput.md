# QUIC-stack HTTP/3 body throughput

`quic_stack_body_throughput` sends a 1 KiB body over each persistent connection. Each server
fully collects and echoes it, and each client drains the response. Criterion declares 2 KiB
per iteration because the payload crosses the connection in both directions.

The arms are the three complete stacks listed in
[`quic-stack-serial-latency.md`](quic-stack-serial-latency.md). Runtime shape, loopback UDP,
ALPN, certificate trust, request, response, and drain are equivalent; QUIC/TLS defaults remain
those of each production stack.

The Criterion target remains at 1 KiB while larger persistent behavior is investigated. The
fixed-count probe supports 16 KiB and 1 MiB specifically to classify the previously observed
stall/close and native-termination failures before either size can become a performance
guard. A successful single body is not qualification: the required stress point is 125
exact sequential exchanges, and 1 MiB additionally uses fresh 125/250/500-process RSS
sampling. See [Running the benchmarks](../running.md#fixed-count-ngtcp2-probe-modes) for the
separate unarmed timing and armed diagnostic procedures.

The packet-bounded retention repair and its historical qualification are recorded in
[run 27](../data/xeon-8370c-azure/27-ngtcp2-packet-bounded-staging.md). Final-review
[run 30](../data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md) preserves later
diagnostic timeout evidence and leaves the RSS/stability criterion unmet. The failed
predecessor remains a correctness reference rather than a large-body throughput baseline;
neither record makes a before/after 1 MiB performance claim.

[Run 28](../data/xeon-8370c-azure/28-ngtcp2-stream-first-gate.md) leaves packet production
order unchanged because frame/coalescing eligibility is not observable, and
[run 29](../data/xeon-8370c-azure/29-ngtcp2-residual-eligibility.md) defers all six residual
candidates because recurring counts do not clear both drift and layer-attribution gates.
