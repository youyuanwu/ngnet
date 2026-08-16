# ngnet

Safe Rust bindings for the nghttp2 family: [nghttp2](https://nghttp2.org) for
cleartext HTTP/2 (**h2c**), and [nghttp3](https://nghttp2.org/nghttp3/) for
HTTP/3 framing and QPACK.

Design notes, the invariants the test suite pins, and the tracked backlog live in
[`docs/`](docs/).

## Crates

| Crate | Description |
| --- | --- |
| [`ngnet-h2`](crates/ngnet-h2) | Safe, sans-I/O API driving a client or server connection, the caller owning the transport — plus an optional asynchronous `http`/`http-body` client and server built on it (default `http` feature). |
| [`ngnet-h2-sys`](crates/ngnet-h2-sys) | Raw FFI bindings. Builds libnghttp2 from source and generates bindings with `bindgen`. |
| [`ngnet-h2-tests`](tests/ngnet-h2-tests) | Not published. Drives `ngnet-h2` over a real async transport, so the wrapper needs no runtime dependency of its own. |
| [`ngnet-h3`](crates/ngnet-h3) | Not published yet — the API is still expected to change; see [`docs/h3/pending-work.md`](docs/h3/pending-work.md). Safe, sans-I/O API driving an HTTP/3 client or server connection over QUIC streams the caller owns — plus an optional asynchronous `http`/`http-body` client and server built on it (default `http` feature). No QUIC or TLS of its own. |
| [`ngnet-h3-sys`](crates/ngnet-h3-sys) | Not published yet, alongside `ngnet-h3`. Raw FFI bindings. Builds libnghttp3 from source and generates bindings with `bindgen`. |
| [`ngnet-h3-tests`](tests/ngnet-h3-tests) | Not published. Drives `ngnet-h3` over a real QUIC connection using [quinn](https://github.com/quinn-rs/quinn), so the wrapper needs no transport dependency of its own. Contains the reference `QuicConnection` implementation. |
| [`ngnet-quic`](crates/ngnet-quic) | Not published yet — the API is still expected to change; see [`docs/quic/pending-work.md`](docs/quic/pending-work.md). Safe API driving a QUIC client or server. A sans-I/O state machine, and behind the default-on `endpoint` feature an asynchronous layer that owns a UDP socket and every connection on it, with the socket and clock as seams a caller implements and a ready-made pair for tokio behind an optional feature. Servers can validate client addresses before committing state. TLS is a backend seam with a default-on OpenSSL implementation. |
| [`ngnet-quic-sys`](crates/ngnet-quic-sys) | Not published yet, alongside `ngnet-quic`. Raw FFI bindings to [ngtcp2](https://github.com/ngtcp2/ngtcp2), the QUIC transport. Builds libngtcp2 from source with `bindgen`, plus its OpenSSL crypto helper behind a default-on `crypto-ossl` feature. |
| [`ngnet-quic-tests`](tests/ngnet-quic-tests) | Not published. Drives `ngnet-quic` through real TLS handshakes and real stream data, in process and over loopback UDP, so the wrapper needs no certificate or runtime dependency of its own. |
| [`ngnet-quic-h3`](crates/ngnet-quic-h3) | Not published yet, alongside the two crates it binds. HTTP/3 over ngtcp2: implements `ngnet-h3`'s transport trait on an `ngnet-quic` connection, so the two families in this workspace form one stack. The only crate that depends on both — deliberately, and a dependency-graph test enforces it, so a caller wanting either alone pays for neither. |
| [`ngnet-quic-h3-tests`](tests/ngnet-quic-h3-tests) | Not published. Drives HTTP/3 over ngtcp2 across real loopback UDP, and interoperates against [quinn](https://github.com/quinn-rs/quinn) in both roles. |
| [`ngnet-axum`](crates/ngnet-axum) | Not published yet — the API is new and expected to change; see [`docs/axum/design.md`](docs/axum/design.md). Serves an [axum](https://github.com/tokio-rs/axum) `Router` over `ngnet-h2` instead of hyper. Server-side, h2c and tokio only; transports are pluggable, with TCP and Unix-domain listeners shipped. |
| [`ngnet-util`](crates/ngnet-util) | Not published yet — the API is new and expected to change; see [`docs/util/design.md`](docs/util/design.md). A pooling HTTP/2 client over `ngnet-h2`: send a request at a URI and the connection is opened, reused, retired and replaced for you. Client-side, h2c and tokio only. |
| [`ngnet-workspace-tests`](tests/ngnet-workspace-tests) | Not published. Checks that belong to the workspace rather than to any crate in it: what the resolved dependency graph contains, and what the linked binaries pull in. Takes no dependencies of its own — it drives `cargo` and `readelf` and reads the output. |

### HTTP/3, and what it deliberately is not

`ngnet-h3` owns no socket, no runtime and no QUIC implementation. That boundary
is where nghttp3 itself draws the line — it depends on no QUIC transport and on
no TLS library — and neither does this crate.

The sans-I/O core is the whole of it with the `http` feature off: you open the
QUIC streams, tell the connection which carry control and QPACK data, and move
bytes in and out. With the feature on, the asynchronous layer does all of that
for you and you write `http::Request` and `http::Response` instead. What it still
does not do is supply QUIC: you bring an *established* connection behind the
`QuicConnection` trait, so choosing a QUIC library, authenticating the peer and
negotiating `h3` remain yours. `ngnet-h3-tests` has a working implementation of
that trait over quinn to copy.

Server push is absent because nghttp3 does not implement it.

### A client you do not have to hold open

`ngnet-util` is to `ngnet-h2`'s client what `ngnet-axum` is to its server: the
policy layer above a single connection. `ngnet-h2` hands back a `SendRequest`
and a driver future and leaves the rest to you — resolving the address, opening
the socket, spawning the driver, holding the handle, and noticing when the
connection goes closed or starts refusing. `ngnet-util` does all of that, keeps
one connection per origin because HTTP/2 multiplexes, retires it when the peer
says goodbye, and dials a replacement when the next request needs one. It also
implements `tower_service::Service`, which is the same seam `ngnet-axum` uses on
the server side.

### axum without hyper

`ngnet-axum` runs an unmodified axum `Router` — its routing, extractors,
middleware and state — with this workspace's HTTP/2 stack underneath instead of
hyper. CI checks that no hyper crate appears in its *normal* dependency graph —
dev-dependencies are excluded on purpose, because the acceptance tests drive the
server with hyper's client to prove it interoperates with something other than
itself.

That is possible because axum barely touches hyper. A `Router` is a
`tower::Service` from `http::Request` to `http::Response`; hyper's job is only to
turn socket bytes into the one and the response back into the other, and that job
is separable. There are no body adapters in the crate, because none is needed:
the body types on both sides already agree.

It differs from `axum::serve` in ways worth knowing before deploying it —
handlers must not block, a panicking handler costs its whole connection, and
graceful shutdown drains without a deadline, so a handler that never returns
holds the server open. All of them are on the crate's front page, with the
reasons.

```rust,no_run
let router = axum::Router::new().route("/hello", axum::routing::get(|| async { "world" }));
let tcp = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
ngnet_axum::serve(ngnet_axum::TcpListener::new(tcp), router).await;
```

## Building

Both vendored libraries are git submodules, and they do **not** want the same
checkout: nghttp2 needs no nested submodules, while nghttp3 compiles
`lib/sfparse` — a submodule of its own — directly into the library. A plain
`--recursive` clone is wrong for one and a plain shallow clone is wrong for the
other, so the correct set is defined once, in the `justfile`:

```sh
just submodules         # check out exactly what the build needs
just submodules-status  # report what is actually checked out
```

Building also needs `cmake` and libclang, which `bindgen` uses.

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

Cleartext only for HTTP/2: TLS and ALPN are the caller's concern, and server
push and stream priorities are out of scope. HTTP/3 is a separate family of
crates, described above.

### Running it over a real socket

Because the crate owns no transport, attaching one is the caller's job. Three
worked answers ship with the repo:

- [`examples/h2c_server.rs`](crates/ngnet-h2/examples/h2c_server.rs) — a blocking
  h2c server on `std::net`, one thread per connection.
- [`tests/std_net.rs`](crates/ngnet-h2/tests/std_net.rs) — a client and a server
  exchanging requests over loopback TCP, covering multiplexed streams and bodies
  large enough to exercise flow control.
- [`ngnet-h2-tests`](tests/ngnet-h2-tests) — the same exchanges over `tokio`,
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
[`docs/h2/benchmarks/`](docs/h2/benchmarks/), which gives the numbers, the mechanism and the
confounds that bound what they license.

For bodies you already hold as [`bytes::Bytes`](https://docs.rs/bytes), an opt-in set of
entry points — `handshake_shared`, `serve_shared`, and their `_with` forms — hands the
payload to libnghttp2 without copying it (`NGHTTP2_DATA_FLAG_NO_COPY`). The choice is per
connection, the push-model API is unchanged, and the payoff is honest rather than uniform: on
the readiness transport a 1 MiB upload runs 24–31% faster depending on the machine, mostly by
collapsing the write count; on the completion transport the gain is a few percent at 1 MiB and
reverses into a small loss below 64 KiB, where the frame headers the shared path has to mint
are not yet paid for.
[`docs/h2/benchmarks/findings/handing-bodies-over.md`](docs/h2/benchmarks/findings/handing-bodies-over.md)
reports both, on two machines.

### When to disable the feature

`http` is additive but not free: it pulls in `http`, `http-body` and `bytes`. Turn it off
with `default-features = false` when you already have your own HTTP types, or want the
crate at its smallest — one dependency, no async, and the sans-I/O API above unchanged.

```toml
ngnet-h2 = { version = "*", default-features = false }
```

## Dependencies

This repo vendors three upstream C libraries as git submodules:

| Submodule | Tag | Purpose |
| --- | --- | --- |
| [`deps/nghttp2`](https://github.com/nghttp2/nghttp2) | `v1.70.0` | HTTP/2, behind `ngnet-h2-sys`. |
| [`deps/nghttp3`](https://github.com/ngtcp2/nghttp3) | `v1.18.0` | HTTP/3 (RFC 9114) framing and QPACK (RFC 9204), behind `ngnet-h3-sys`. |
| [`deps/ngtcp2`](https://github.com/ngtcp2/ngtcp2) | `v1.25.0` | QUIC transport (RFC 9000), behind `ngnet-quic-sys`. |

`nghttp3` depends on no QUIC transport and on no TLS library — it is a state
machine over stream bytes — and neither does `ngnet-h3`. Choosing a QUIC
implementation is left to the caller; the integration tests happen to use quinn,
and that choice reaches no crate but `ngnet-h3-tests`.

`ngtcp2` is vendored as a step towards a second QUIC backend, and is not wired
into `ngnet-h3` yet. It draws the same line one layer down: libngtcp2 itself
links no TLS, and OpenSSL reaches it only through a safe backend seam that a
third-party stack could implement without writing `unsafe`. ngtcp2's crypto
helper archive is still linked, behind `ngnet-quic-sys`'s default-on
`crypto-ossl` feature — but for its cryptographic primitives, Retry tokens and
stateless reset, not to drive the handshake. The handshake goes through
OpenSSL's own QUIC TLS API, which needs **OpenSSL 3.5 or newer**, and which is
why CI pins its runner image rather than using `ubuntu-latest`.

## Minimum submodule checkout

All three libraries declare nested submodules that only their own tests, tooling
and example applications need, so a `--recursive` checkout fetches a great deal
this repo never compiles:

| Nested submodule | Needed here? |
| --- | --- |
| `nghttp2/third-party/{mruby,neverbleed,urlparse}`, `nghttp2/tests/munit` | No — `nghttpx`, `nghttp`, `h2load` and the upstream test suite only. |
| `nghttp3/tests/munit` | No — upstream test suite only. |
| `ngtcp2/tests/munit`, `ngtcp2/third-party/urlparse` | No — upstream test suite, and the example client/server. `urlparse` reads like a library dependency, but its CMake target is guarded on libev and nghttp3 being found, so a lib-only build never reaches it. |
| `nghttp3/lib/sfparse` | **Yes** — the structured-field parser is part of the library, not its tests. `nghttp3` does not compile without it. |

That last row is the trap: "clone non-recursively" is correct for `nghttp2` and
`ngtcp2`, and quietly wrong for `nghttp3`. The [`justfile`](justfile) encodes the right set, so
it does not have to be remembered:

```sh
just submodules
```

### By hand

```sh
# Clone without any submodules...
git clone https://github.com/youyuanwu/ngnet.git
cd ngnet

# ...then init the three top-level submodules (non-recursive)...
git submodule update --init deps/nghttp2 deps/nghttp3 deps/ngtcp2

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
