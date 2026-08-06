# ngnet

Rust bindings for [nghttp2](https://nghttp2.org), targeting cleartext HTTP/2
(**h2c**).

Design notes, the invariants the test suite pins, and the tracked backlog live in
[`docs/`](docs/).

## Crates

| Crate | Description |
| --- | --- |
| [`ngnet-h2`](crates/ngnet-h2) | Safe, sans-I/O API driving a client or server connection, the caller owning the transport — plus an optional asynchronous `http`/`http-body` client and server built on it (default `http` feature). |
| [`ngnet-h2-sys`](crates/ngnet-h2-sys) | Raw FFI bindings. Builds libnghttp2 from source and generates bindings with `bindgen`. |
| [`ngnet-h2-tests`](crates/ngnet-h2-tests) | Not published. Drives `ngnet-h2` over a real async transport, so the wrapper needs no runtime dependency of its own. |

## Usage

`ngnet-h2` performs no I/O. It never opens a socket, never blocks and creates no
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

See the [crate documentation](crates/ngnet-h2/src/lib.rs) for a complete worked
example and for the guarantees the type system enforces.

Cleartext only: TLS and ALPN are the caller's concern, and server push, HTTP/3
and stream priorities are out of scope.

### Running it over a real socket

Because the crate owns no transport, attaching one is the caller's job. Three
worked answers ship with the repo:

- [`examples/h2c_server.rs`](crates/ngnet-h2/examples/h2c_server.rs) — a blocking
  h2c server on `std::net`, one thread per connection.
- [`tests/std_net.rs`](crates/ngnet-h2/tests/std_net.rs) — a client and a server
  exchanging requests over loopback TCP, covering multiplexed streams and bodies
  large enough to exercise flow control.
- [`ngnet-h2-tests`](crates/ngnet-h2-tests) — the same exchanges over `tokio`,
  plus many connections in flight at once. The adapter between session and
  socket is the same three functions in both cases; only the `.await` points
  differ.

The example answers any HTTP/2 client that speaks cleartext with prior
knowledge, so it can be driven with `curl`:

```sh
cargo run -p ngnet-h2 --example h2c_server
curl --http2-prior-knowledge -i http://127.0.0.1:8080/hello
curl --http2-prior-knowledge -i --data 'ping' http://127.0.0.1:8080/echo
```

## Asynchronous HTTP/2

The default `http` feature adds an asynchronous client and server over the same core,
speaking in [`http`](https://docs.rs/http), [`http-body`](https://docs.rs/http-body) and
[`bytes`](https://docs.rs/bytes) types. It owns no runtime either: `handshake` and `serve`
hand back a *driver* future, and where that future runs — spawned, joined, or polled
alongside other work — is the caller's choice. Nothing is sent until it is polled, so the
driver is `#[must_use]`; dropping it fails every exchange it was carrying.

```rust
use bytes::Bytes;
use http_body_util::Empty; // any `http_body::Body` will do
use ngnet_h2::http::{handshake, transport::TokioIo};

let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
let (requests, connection) = handshake::<_, Empty<Bytes>>(TokioIo::new(stream))?;

// The driver runs wherever the caller's runtime puts work; the handle only enqueues.
tokio::spawn(connection);

let response = requests
    .send_request(http::Request::get("http://127.0.0.1:8080/hello").body(Empty::new())?)
    .await?;
assert_eq!(response.status(), 200);
// `response.into_body()` is an `http_body::Body`, delivering data then trailers as they
// arrive — the head is readable here, before the body has finished.
```

The `TokioIo` transport comes from the optional, off-by-default `tokio` feature. A second
ready-made transport, `CompioIo`, comes from the off-by-default `completion` feature and runs
on [compio](https://github.com/compio-rs/compio) over **io_uring** — the completion-based
shape these traits were designed around. This crate asks compio for no readiness backend, so
a build that takes its dependency as declared uses io_uring or fails to start rather than
quietly falling back to epoll; enable it only where io_uring is available. That is not an
absolute guarantee — cargo unifies features across a whole dependency graph, so another crate
enabling compio's `polling` would restore the fallback, and the module documentation explains
how to check. On any other runtime, implementing the three transport traits is a twenty-line
job.
A runnable server is in
[`examples/h2c_async_server.rs`](crates/ngnet-h2/examples/h2c_async_server.rs), answering
the same `curl --http2-prior-knowledge` as the blocking one.

Which to pick is measured rather than asserted, and the answer changed once the measurement
was understood. `CompioIo` was roughly twice as fast as `TokioIo` under multiplexing — but
benchmarking hyper over the same socket showed it reaching the same throughput on *epoll*, so
the gap was never the I/O model. It was the number of write syscalls per pass. `TokioIo` now
gathers its writes (`writev`, no copy of large blocks), which collapsed a multiplexed pass
from 513 writes to 1 and left the two **within noise of each other** — so pick on ergonomics
and runtime fit rather than on throughput. See
[`docs/benchmarks.md`](docs/benchmarks.md), which gives the numbers, the mechanism and the
confounds that bound what they license.

For bodies you already hold as [`bytes::Bytes`](https://docs.rs/bytes), an opt-in set of
entry points — `handshake_shared`, `serve_shared`, and their `_with` forms — hands the
payload to libnghttp2 without copying it (`NGHTTP2_DATA_FLAG_NO_COPY`). The choice is per
connection, the push-model API is unchanged, and the payoff is honest rather than uniform: on
the readiness transport a 1 MiB upload runs about 30% faster, mostly by collapsing the write
count; on the completion transport the gain is small and does not clear the benchmark's own
drift bar. [`docs/benchmarks.md`](docs/benchmarks.md) reports both.

### When to disable the feature

`http` is additive but not free: it pulls in `http`, `http-body` and `bytes`. Turn it off
with `default-features = false` when you already have your own HTTP types, or want the
crate at its smallest — one dependency, no async, and the sans-I/O API above unchanged.

```toml
ngnet-h2 = { version = "*", default-features = false }
```

## Dependencies

This repo vendors two upstream C libraries as git submodules:

| Submodule | Tag | Purpose |
| --- | --- | --- |
| [`deps/nghttp2`](https://github.com/nghttp2/nghttp2) | `v1.70.0` | HTTP/2, behind `ngnet-h2-sys`. |
| [`deps/nghttp3`](https://github.com/ngtcp2/nghttp3) | `v1.18.0` | HTTP/3 (RFC 9114) framing and QPACK (RFC 9204). Vendored ahead of the bindings that will use it; nothing in the workspace builds it yet. |

`nghttp3` depends on no QUIC transport and on no TLS library — it is a state
machine over stream bytes, and choosing a QUIC implementation (ngtcp2 or
otherwise) is a decision this repo has not taken.

## Minimum submodule checkout

Both libraries declare nested submodules that only their own tests, tooling and
example applications need, so a `--recursive` checkout fetches a great deal this
repo never compiles:

| Nested submodule | Needed here? |
| --- | --- |
| `nghttp2/third-party/{mruby,neverbleed,urlparse}`, `nghttp2/tests/munit` | No — `nghttpx`, `nghttp`, `h2load` and the upstream test suite only. |
| `nghttp3/tests/munit` | No — upstream test suite only. |
| `nghttp3/lib/sfparse` | **Yes** — the structured-field parser is part of the library, not its tests. `nghttp3` does not compile without it. |

That last row is the trap: "clone non-recursively" is correct for `nghttp2` and
quietly wrong for `nghttp3`. The [`justfile`](justfile) encodes the right set, so
it does not have to be remembered:

```sh
just submodules
```

### By hand

```sh
# Clone without any submodules...
git clone https://github.com/youyuanwu/ngnet.git
cd ngnet

# ...then init the two top-level submodules (non-recursive)...
git submodule update --init deps/nghttp2 deps/nghttp3

# ...plus the one nested submodule that nghttp3's own sources require.
git -C deps/nghttp3 submodule update --init lib/sfparse
```

The same two commands update an existing clone.

> Do **not** pass `--recursive` at the top level. That would pull in every
> nested submodule in the table above, rather than the single one this repo
> needs.

### Optional: save bandwidth with a shallow checkout

```sh
just submodules depth=1
```

which is `git submodule update --init --depth 1 …` for each of the above.

### Checking what you have

```sh
just submodules-status
```

A leading `-` means "not checked out" and `+` means "at a different commit than
this repo records" — the two states that turn into confusing build failures
rather than obvious ones.

## Building

Requires a C compiler, [CMake](https://cmake.org) 3.14+, and `libclang` (for
`bindgen`).

```sh
cargo build
cargo test
```

`crates/ngnet-h2-sys/build.rs` drives the native build:

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

`ngnet-h2-sys` sets the `links = "nghttp2"` key, so dependent build scripts can
read the native paths from the environment:

- `DEP_NGHTTP2_ROOT` — install prefix
- `DEP_NGHTTP2_INCLUDE` — header directory
- `DEP_NGHTTP2_LIB` — directory containing `libnghttp2.a`
