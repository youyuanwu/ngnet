# ngnet-quic-h3

HTTP/3 over ngtcp2, joining [`ngnet-h3`](https://crates.io/crates/ngnet-h3) to
[`ngnet-quic`](https://crates.io/crates/ngnet-quic).

```toml
[dependencies]
ngnet-quic-h3 = "0.0.1"
```

`ngnet-h3` intentionally knows nothing about a concrete QUIC implementation, while
`ngnet-quic` intentionally knows nothing about HTTP/3. This adapter is the only crate that
depends on both. It pumps an owned ngtcp2 connection while implementing the asynchronous
HTTP/3 transport expected by `ngnet-h3`.

The crate has no features: it requires both dependencies' default asynchronous layers
because joining those layers is its entire purpose. It owns no socket, runtime, timer or TLS
configuration; those remain with the `ngnet-quic` endpoint.

The underlying native crates require a C compiler, CMake 3.20 or newer, libclang and, with
the default TLS backend, OpenSSL 3.5 or newer.

The API is released at `0.0.x` and may change between every release. See the
[API documentation](https://docs.rs/ngnet-quic-h3) and
[design notes](https://github.com/youyuanwu/ngnet/tree/main/docs/quic-h3).

License: MIT
