# Quinn HTTP/3 serial latency

`quinn_serial_latency` times one empty-body request and response at a time on a persistent QUIC
connection over loopback UDP.

| Arm | Stack |
| --- | --- |
| `ngnet-h3-quinn` | `ngnet-h3` + `ngnet-h3-quinn` + Quinn |
| `h3-quinn` | upstream `h3` + `h3-quinn` + Quinn |

Connection setup, TLS handshake, ALPN negotiation, and one warm-up exchange happen before the
timed closure. Each iteration includes request headers, response headers, stream completion, and
draining the empty response body. Read the result as steady-state local round-trip overhead, not
handshake cost or network latency.
