# ngrs

Rust bindings for [nghttp2](https://nghttp2.org), targeting cleartext HTTP/2
(**h2c**).

## Crates

| Crate | Description |
| --- | --- |
| [`nghttp2`](crates/nghttp2) | Safe, sans-I/O API. Drives a client or server connection; the caller owns the transport. |
| [`nghttp2-sys`](crates/nghttp2-sys) | Raw FFI bindings. Builds libnghttp2 from source and generates bindings with `bindgen`. |

## Usage

`nghttp2` performs no I/O. It never opens a socket, never blocks and creates no
threads: you hand it the bytes you read from wherever your data came from, and
it hands back the bytes to write. That makes it usable from blocking code, from
any async runtime, and from tests that wire a client to a server entirely in
memory.

```rust
let mut client = SessionBuilder::<Response>::client()
    .on_header(|res, _frame, name, value| { /* borrowed, not copied */ HeaderAction::Continue })
    .on_data_chunk(|res, _stream, chunk| res.body.extend_from_slice(chunk))
    .build()?;

let stream = client.submit_request(&[
    Header::new(":method", "GET"),
    Header::new(":scheme", "http"),
    Header::new(":path", "/hello"),
    Header::new(":authority", "example.test"),
])?;

// Write what the session wants to send, then feed it what you read back.
while let Some(block) = client.send(&mut response)? {
    socket.write_all(block)?;
}
```

Incoming headers and body chunks reach your handlers as borrowed slices, valid
for the duration of the call, so receiving allocates nothing in the wrapper. Your application state
is passed in at call time rather than captured, so handlers can mutate it
directly.

See the [crate documentation](crates/nghttp2/src/lib.rs) for a complete worked
example and for the guarantees the type system enforces.

Cleartext only: TLS and ALPN are the caller's concern, and server push, HTTP/3
and stream priorities are out of scope.

## Dependencies

This repo vendors [nghttp2](https://github.com/nghttp2/nghttp2) at tag
`v1.70.0` as a git submodule under `deps/nghttp2`.

## Minimum submodule checkout

`nghttp2` declares its own nested submodules (mruby, neverbleed, munit,
urlparse) that are **not** required here. They are only used by `nghttpx`,
`nghttp`, `h2load`, the examples and the upstream test suite — none of which we
build. To fetch only the top-level `deps/nghttp2` submodule and skip the nested
ones, do a **non-recursive** checkout.

### Fresh clone

```sh
# Clone without any submodules...
git clone https://github.com/youyuanwu/ngrs.git
cd ngrs

# ...then init/update only the top-level submodule (non-recursive).
git submodule update --init deps/nghttp2
```

### Existing clone

```sh
git submodule update --init deps/nghttp2
```

> Do **not** pass `--recursive`. That would pull in nghttp2's nested
> submodules, which this repo does not need.

### Optional: save bandwidth with a shallow checkout

```sh
git submodule update --init --depth 1 deps/nghttp2
```

## Building

Requires a C compiler, [CMake](https://cmake.org) 3.14+, and `libclang` (for
`bindgen`).

```sh
cargo build
cargo test
```

`crates/nghttp2-sys/build.rs` drives the native build:

1. Locates the `deps/nghttp2` submodule and fails with actionable instructions
   if it has not been checked out.
2. Configures CMake with `ENABLE_LIB_ONLY=ON`, which disables the applications,
   examples and HPACK tools. This is what keeps the nested submodules and the
   heavyweight system dependencies (libev, libevent, OpenSSL, zlib, jansson)
   out of the build.
3. Also sets `BUILD_TESTING=OFF`, because it otherwise defaults to `ON`
   whenever `BUILD_STATIC_LIBS` is on and would require the `tests/munit`
   submodule.
4. Builds `libnghttp2.a` statically and installs it into `OUT_DIR`.
5. Runs `bindgen` over `wrapper.h` against the freshly installed headers, so
   the bindings can never drift from the vendored library version.

Nothing is installed system-wide, and no prebuilt nghttp2 is required.

### Using a different nghttp2 checkout

Set `NGHTTP2_SOURCE_DIR` to build against sources elsewhere on disk:

```sh
NGHTTP2_SOURCE_DIR=/path/to/nghttp2 cargo build
```

### Downstream crates

`nghttp2-sys` sets the `links = "nghttp2"` key, so dependent build scripts can
read the native paths from the environment:

- `DEP_NGHTTP2_ROOT` — install prefix
- `DEP_NGHTTP2_INCLUDE` — header directory
- `DEP_NGHTTP2_LIB` — directory containing `libnghttp2.a`
