# Hyperium H3 against ngnet H3 over QMux

Four Criterion targets compare two complete H3 stacks over one shared QMux configuration:

| Target | Substrate | Workload |
| --- | --- | --- |
| `qmux_h3_serial_latency` | Tokio duplex, 1 MiB capacity | one empty request/response |
| `qmux_h3_body_throughput` | Tokio duplex | 0, 1 KiB, 64 KiB, 1 MiB, 8 MiB echo |
| `qmux_h3_socket_serial_latency` | loopback TCP, `TCP_NODELAY` | one empty request/response |
| `qmux_h3_socket_body_throughput` | loopback TCP, `TCP_NODELAY` | the same body sweep |

Each target places `ngnet-h3 + ngnet-qmux-h3` immediately beside
`hyperium h3 + h3-ngnet-qmux`. Each arm gets a separate current-thread Tokio runtime, a
persistent connection, exactly one spawned task per endpoint, and one explicit empty warm-up
outside Criterion's measured closure. Body throughput declares request plus echoed response
bytes. Diagnostic probes use the same per-instance lower-I/O and endpoint-poll counters around
both arms; production adapters expose no benchmark instrumentation.

QMux windows, read-ahead, cumulative stream allowances, request headers, response, body
generator, echo, and complete drain are identical. Hyperium GREASE is disabled and its field
section bound is 64 KiB. Hyperium 0.0.8 exposes no QPACK dynamic-table control, so the matched
ngnet fixture sets its capacity to zero. Hyperium's request handle is cloned per exchange while
the ngnet handle API does not require it; that small measured difference is disclosed.

Read duplex and socket results separately. The pair changes the H3 implementation and adapter,
not QMux. It supports a whole-stack statement on this workload and no attribution to one layer.
Run [`31`](../data/xeon-8370c-azure/31-h3-ngnet-qmux.md) is historical evidence from unequal
topology and removed diagnostics. Run
[`33`](../data/xeon-8370c-azure/33-h3-qmux-post-revision.md) records the current symmetric,
still-inconclusive comparison.
