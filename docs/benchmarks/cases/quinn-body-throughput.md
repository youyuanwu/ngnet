# Quinn HTTP/3 body throughput

`quinn_body_throughput` sends 16 KiB and 1 MiB bodies over persistent Quinn connections. The
server fully collects each request and echoes it; the client drains the complete response before
the iteration ends. Criterion declares twice the payload size as throughput because every byte
crosses the connection in both directions.

At each size the `ngnet-h3-quinn` arm runs immediately before the upstream `h3-quinn` arm. Body
allocation, cloning outside the timed closure, Quinn version, TLS, ALPN, runtime shape, endpoint
configuration, request headers, and response headers are held equal. HTTP/3 framing and QPACK
implementation differ and are the subject of the comparison.
