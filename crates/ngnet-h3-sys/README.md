# ngnet-h3-sys

Raw Rust FFI bindings to [libnghttp3](https://nghttp2.org/nghttp3/), built from the nghttp3
source bundled with this crate.

The build script compiles a static, library-only copy of nghttp3 and generates bindings from
the matching headers with `bindgen`. The package includes the required sfparse sources but
not upstream examples or tests.

nghttp3 implements HTTP/3 message framing and QPACK. It contains no QUIC transport, TLS
implementation, socket or runtime. This crate exposes that C API directly and is inherently
unsafe. Most applications should use [`ngnet-h3`](https://crates.io/crates/ngnet-h3).

## Build requirements

- A C compiler
- CMake 3.20 or newer
- libclang, used by `bindgen`

The bundled source is the default. Set `NGHTTP3_SOURCE_DIR` to build against another
nghttp3 checkout; it must contain `lib/sfparse/sfparse.c`.

## Version

This crate is released at `0.0.x` while its API settles and is versioned alongside
`ngnet-h3`.

License: MIT
