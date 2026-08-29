# ngnet-h3-quinn

A [Quinn](https://github.com/quinn-rs/quinn) transport adapter for
[`ngnet-h3`](https://crates.io/crates/ngnet-h3).

`ngnet-h3` deliberately owns no QUIC implementation. This crate implements its
`QuicConnection` trait for an established `quinn::Connection`:

```rust,no_run
use ngnet_h3::http::handshake;
use ngnet_h3_quinn::QuinnBackend;

# async fn connect(
#     connection: quinn::Connection,
# ) -> Result<(), Box<dyn std::error::Error>> {
let (requests, driver) = handshake::<_, http_body_util::Empty<bytes::Bytes>>(
    QuinnBackend::new(connection),
)?;
tokio::task::spawn_local(driver);
# let _ = requests;
# Ok(())
# }
```

Endpoint creation, TLS configuration, certificate verification, ALPN negotiation, and socket
ownership remain the caller's responsibility.
