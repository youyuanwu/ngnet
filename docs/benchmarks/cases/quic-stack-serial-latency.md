# QUIC-stack HTTP/3 serial latency

`quic_stack_serial_latency` times one empty request and response at a time on a persistent
loopback UDP connection.

| Arm | Stack |
| --- | --- |
| `ngnet-h3-quinn` | `ngnet-h3` + `ngnet-h3-quinn` + Quinn + rustls |
| `ngnet-quic-h3` | `ngnet-h3` + `ngnet-quic-h3` + ngtcp2 + OpenSSL |
| `h3-quinn` | upstream `h3` + `h3-quinn` + Quinn + rustls |

Setup, TLS handshake, ALPN negotiation, and a warm-up exchange happen before timing. Every
iteration includes headers, stream completion, and draining the empty response. The first two
arms answer which complete transport integration is faster under `ngnet-h3`; they do not
isolate QUIC, TLS, endpoint, or adapter costs from one another.
