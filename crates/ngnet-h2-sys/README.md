# ngnet-h2-sys

Raw Rust FFI bindings to [libnghttp2](https://nghttp2.org), built from the nghttp2 source
bundled with this crate.

The build script compiles a static, library-only copy of nghttp2 and generates bindings from
the matching headers with `bindgen`. Applications, examples, upstream tests and optional
system integrations are not built.

This crate exposes the C API directly and is inherently unsafe. Most applications should
depend on [`ngnet-h2`](https://crates.io/crates/ngnet-h2), which provides the safe sans-I/O
and asynchronous HTTP/2 APIs.

## Build requirements

- A C compiler
- CMake 3.14 or newer
- libclang, used by `bindgen`

The bundled source is the default and makes crates.io builds self-contained. Set
`NGHTTP2_SOURCE_DIR` to build against another nghttp2 checkout.

## Version

This crate is released at `0.0.x` while its API settles and is versioned alongside
`ngnet-h2`.

License: MIT
