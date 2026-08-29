# ngnet-h2

Safe Rust bindings to [libnghttp2](https://nghttp2.org) for cleartext HTTP/2 (**h2c**).

The core is a sans-I/O client/server state machine: it opens no sockets, blocks nowhere and
creates no threads. The default `http` feature adds asynchronous APIs using the standard
`http` and `http-body` types.

```toml
[dependencies]
ngnet-h2 = "0.0.1"
```

This crate intentionally supports h2c only. TLS and ALPN belong outside the protocol state
machine, and HTTP/1 upgrade, server push and stream priorities are out of scope.

## Features

| Feature | Default | Purpose |
| --- | --- | --- |
| `http` | Yes | Asynchronous client/server APIs using `http` and `http-body`. |
| `tokio` | No | Ready-made transport adapter for tokio. Implies `http`. |
| `completion` | No | Ready-made compio transport using io_uring. Implies `http`. |

Run the included tokio server with:

```sh
cargo run -p ngnet-h2 --features tokio --example h2c_async_server
curl --http2-prior-knowledge http://127.0.0.1:8080/hello
```

The bundled native dependency requires a C compiler, CMake and libclang when this crate is
built from source.

The API is released at `0.0.x` and may change between every release. See the
[API documentation](https://docs.rs/ngnet-h2) and
[design notes](https://github.com/youyuanwu/ngnet/tree/main/docs/h2).

License: MIT
