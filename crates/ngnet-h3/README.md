# ngnet-h3

Safe Rust bindings to [libnghttp3](https://nghttp2.org/nghttp3/) for HTTP/3 message framing
and QPACK.

The core is sans-I/O: callers supply bytes from QUIC streams and receive bytes and stream
identifiers to write back. The crate deliberately contains no QUIC implementation, TLS
implementation, socket or runtime.

```toml
[dependencies]
ngnet-h3 = "0.0.1"
```

The default `http` feature adds asynchronous client and server APIs using `http` and
`http-body`. Applications provide an established transport by implementing
`ngnet_h3::http::QuicConnection`. Use
[`ngnet-quic-h3`](https://crates.io/crates/ngnet-quic-h3) for the ready-made integration
with this workspace's ngtcp2-based QUIC stack.

| Feature | Default | Purpose |
| --- | --- | --- |
| `http` | Yes | Asynchronous HTTP/3 client/server APIs and the transport abstraction. |

The bundled native dependency requires a C compiler, CMake 3.20 or newer, and libclang when
this crate is built from source.

The API is released at `0.0.x` and may change between every release. See the
[API documentation](https://docs.rs/ngnet-h3) and
[design notes](https://github.com/youyuanwu/ngnet/tree/main/docs/h3).

License: MIT
