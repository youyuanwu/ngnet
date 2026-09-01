# What CI runs

One workflow covers both crate families, so this is one file rather than two. **If you add a
check to `.github/workflows/ci.yml`, add it here too** — this list and that workflow are
meant to say the same thing, and the workflow says so at the top.

The invariants each family pins in its own test suite are listed separately, in
[`h2/invariants.md`](h2/invariants.md) and [`h3/invariants.md`](h3/invariants.md).

## The commands

The feature matrix matters: a doc link to a `tokio`-gated item once passed `--all-features`
and broke every other configuration.

Everything below runs on every pull request. CI reads the compiler from
`rust-toolchain.toml`, so a local run uses the same one.

CI pins `runs-on: ubuntu-26.04` rather than using `ubuntu-latest`, and the pin is
load-bearing. ngtcp2's OpenSSL crypto helper needs OpenSSL >= 3.5, and `ubuntu-latest` is
still 24.04, which ships 3.0.13; 26.04 is the first image with 3.5. It is currently a
preview image, so when it becomes the `ubuntu-latest` default the pin can go away. A step
before the build asserts the OpenSSL version, so a runner image change fails with that
message rather than somewhere inside CMake's symbol probing.

```sh
cargo test --workspace --all-features
cargo test --workspace
cargo test -p ngnet-h2 --no-default-features
cargo test -p ngnet-h3 --no-default-features
cargo test -p ngnet-quic-sys --no-default-features
cargo test -p ngnet-quic --no-default-features
cargo test -p ngnet-quic --no-default-features --features endpoint
cargo test -p ngnet-quic --release
cargo test -p ngnet-quic-h3 -p ngnet-quic-h3-tests --release
cargo test -p ngnet-qmux-h3 -p ngnet-qmux-h3-tests --release
cargo test -p h3-ngnet-qmux -p h3-ngnet-qmux-tests --release

# Runs each benchmark once without timing it. Benchmarks are not part of `cargo test`, so
# without this they rot silently as the API moves.
cargo bench --workspace -- --test
```

Run `touch crates/ngnet-h2/src/lib.rs` before a final run so no stale incremental artefact
flatters the result.

Clippy and rustdoc are each **one step iterating a list**, rather than one step per
configuration. The lists live in the workflow, each entry with the reason it exists written
above it; what follows is the set they expand to, which is the thing worth checking against
this document.

A `strategy.matrix` would have been the other way to write them, and is the wrong shape
here: every leg is a fresh runner, and this build compiles libnghttp2, nghttp3 and ngtcp2
from source. Twelve clippy legs would mean twelve cold C builds.

`--all-targets` and `-- -D warnings` are hoisted out of the clippy list, since every entry
has them. `--all-targets` reaches benchmarks and examples as well as tests, which is half of
what keeps the benchmark crate from rotting.

```sh
cargo clippy --workspace --all-features       --all-targets -- -D warnings
cargo clippy -p ngnet-h2                      --all-targets -- -D warnings
cargo clippy -p ngnet-h2 --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-h3                      --all-targets -- -D warnings
cargo clippy -p ngnet-h3 --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-quic-sys --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-quic                    --all-targets -- -D warnings
cargo clippy -p ngnet-quic --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-quic --features tokio   --all-targets -- -D warnings
cargo clippy -p ngnet-quic --no-default-features --features endpoint --all-targets -- -D warnings
cargo clippy -p ngnet-quic-h3                 --all-targets -- -D warnings
cargo clippy -p ngnet-qmux                    --all-targets -- -D warnings
cargo clippy -p ngnet-qmux --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-qmux --all-features     --all-targets -- -D warnings
cargo clippy -p ngnet-qmux-h3 -p ngnet-qmux-h3-tests --all-targets -- -D warnings
cargo clippy -p h3-ngnet-qmux -p h3-ngnet-qmux-tests --all-targets -- -D warnings

# One entry each, not a matrix: `ngnet-axum` and `ngnet-util` have no features. axum, tokio,
# the h2 transport and the tower `Service` impl are all unconditional, so there is no second
# configuration to get wrong.
cargo clippy -p ngnet-axum                    --all-targets -- -D warnings
cargo clippy -p ngnet-util                    --all-targets -- -D warnings
```

The rustdoc list runs under a single `RUSTDOCFLAGS: -D warnings` covering every entry. The
feature matrix matters here for a reason the repository learned the hard way: a doc link to
an item behind the `tokio` feature once passed `--all-features` and broke every
configuration without it. `--all-features` alone is not a documentation check. `ngnet-h3`
and `ngnet-quic` have matrices of their own for the same reason -- an async layer and a TLS
backend behind default-on features. `ngnet-quic` needs two entries beyond the obvious three:
its runtime integration is off by default, and the configuration with the endpoint but no TLS
backend is the only one where address validation is absent, because writing a Retry packet
needs packet protection the backend supplies.

```sh
RUSTDOCFLAGS="-D warnings"

cargo doc --no-deps -p ngnet-h2
cargo doc --no-deps -p ngnet-h2 --no-default-features
cargo doc --no-deps -p ngnet-h2 --all-features
cargo doc --no-deps -p ngnet-h2 --features tokio
cargo doc --no-deps -p ngnet-h2 --features completion

cargo doc --no-deps -p ngnet-h3
cargo doc --no-deps -p ngnet-h3 --no-default-features
cargo doc --no-deps -p ngnet-h3 --all-features
cargo doc --no-deps -p ngnet-h3-sys -p ngnet-h3-tests -p ngnet-quic-sys

cargo doc --no-deps -p ngnet-quic
cargo doc --no-deps -p ngnet-quic --no-default-features
cargo doc --no-deps -p ngnet-quic --features tokio
cargo doc --no-deps -p ngnet-quic --no-default-features --features endpoint
cargo doc --no-deps -p ngnet-quic --all-features
cargo doc --no-deps -p ngnet-quic-tests
cargo doc --no-deps -p ngnet-quic-h3
cargo doc --no-deps -p ngnet-quic-h3-tests
cargo doc --no-deps -p ngnet-qmux
cargo doc --no-deps -p ngnet-qmux --no-default-features
cargo doc --no-deps -p ngnet-qmux --all-features
cargo doc --no-deps -p ngnet-qmux-h3
cargo doc --no-deps -p ngnet-qmux-h3-tests
cargo doc --no-deps -p h3-ngnet-qmux
cargo doc --no-deps -p h3-ngnet-qmux-tests

cargo doc --no-deps -p ngnet-axum
cargo doc --no-deps -p ngnet-util
```

## The checks that used to be shell scripts

Seven checks used to be inline shell in the workflow, which meant the only way to ask them
was to open a pull request. They are Rust tests now, in
[`tests/ngnet-workspace-tests`](../tests/ngnet-workspace-tests), and the workspace test runs
above pick them up with no wiring of their own. All of them:

```sh
cargo test -p ngnet-workspace-tests
```

They assert one of two things, and each test says which:

- **what the resolved dependency graph contains** -- what a downstream user's build actually
  pulls in, after cargo has resolved versions, unified features across the workspace and
  applied defaults;
- **what a linked binary pulls in** -- for the cases cargo cannot answer, because a C library
  arriving through build-script link flags is not a cargo dependency and appears in no graph.

Neither is the claim made by the `invariants.rs` suite in each crate, listed in
[`h2/invariants.md`](h2/invariants.md) and [`h3/invariants.md`](h3/invariants.md). Those
assert what a crate **declares** in its own `Cargo.toml`. These assert what the resolution of
that declaration **produces**. The two agree today, which is exactly why they are worth
keeping apart: a transitive dependency, a feature enabled by an unrelated workspace member,
or an axum feature that quietly wants `hyper-util` moves the second without touching the
first.

### The dependency-graph checks

```sh
cargo test -p ngnet-workspace-tests --test dependency_graph
```

| Test | Asks |
| --- | --- |
| `http3_core_depends_only_on_its_bindings` | `cargo tree -p ngnet-h3 --no-default-features -e normal` |
| `no_transport_or_tls_reaches_http3` | `cargo tree -p ngnet-h3 -e normal` |
| `completion_transport_compiles_no_readiness_backend` | `cargo tree -p ngnet-h2 --features completion -e features` |
| `no_hyper_reaches_the_axum_integration` | `cargo tree -p ngnet-axum -e normal` |
| `no_hyper_reaches_the_client_policy_layer` | `cargo tree -p ngnet-util -e normal` |

That the sans-I/O HTTP/3 core still stands alone is asked with `--no-default-features`,
because `cargo tree` resolves default features and the async layer's feature is default-on --
without the flag it asks about the async layer instead. The test checks both that there is
exactly one dependency and that it is `ngnet-h3-sys`: counting alone would pass for any
single dependency at all.

That no transport or TLS crate reaches the graph is asked with default features **on**, and
the difference is deliberate. Cargo unifies features across a workspace, so a crate added
later could pull a transport in with nothing in `ngnet-h3` changing.

That the completion transport compiles **no readiness backend** is a property of the resolved
graph and of no source file. `ngnet-h2` takes compio's `io-uring` and deliberately not
`polling`; with both, compio compiles a fusion driver that probes the kernel and silently
falls back to epoll, and a transport that quietly became readiness-based while still calling
itself completion-based would make every measurement taken through it a lie. The runtime
assertion in the compio test only fires where io_uring is genuinely absent, which is not true
of CI or of most developer machines -- this is the check that catches it where io_uring
exists.

That no hyper crate reaches `ngnet-axum` is the claim that crate exists to make, and
`-e normal` is what makes the check honest rather than merely strict: hyper **is** a
dev-dependency there, deliberately, because the acceptance tests drive the server with an
independent HTTP/2 client -- a client from this workspace could only show `ngnet-h2` agreeing
with itself. The claim is about what a downstream user links, not about what the test
binaries link. The usual way to lose it is an axum feature that depends on `hyper-util` --
`ConnectInfo` is one -- arriving transitively where no manifest shows it.

The same claim is made separately for `ngnet-util` rather than assumed to follow. It is in
the same position for the same reason: hyper is a dev-dependency there because the acceptance
suite drives the pool against hyper's HTTP/2 **server**, a client-side crate needing a server
this workspace did not write. That is precisely the arrangement in which a hyper crate
reaches the normal graph unnoticed -- everything builds, every test passes, and only the
graph shows it.

Two of these are about `ngnet-quic` rather than about hyper, and they are asked of the
resolved graph for a reason worth stating. `ngnet-h3` proves it owns no transport by having
its source never name one, but `ngnet-quic` ships a ready-made socket and clock for tokio
behind an off-by-default feature — so its source legitimately contains the word, and a
textual scan would either fail or have to be weakened into uselessness. The graph is the
honest place to ask. `no_async_runtime_reaches_the_quic_wrapper_by_default` asks with default
features, which is what a caller gets by depending on the crate;
`the_quic_endpoint_layer_alone_brings_no_runtime` asks with the asynchronous layer on and the
runtime integration off, which is the configuration that would show the socket and clock
seams not being seams at all.

Note the division of labour with `crates/ngnet-quic/tests/invariants.rs`: that suite asserts
what the crate's manifest *declares*, and these assert what a downstream caller actually
builds. Neither implies the other, which is why both exist.

When one of these fails it prints the offending tree and the command that finds the culprit,
for instance `cargo tree -p ngnet-axum -e normal -i hyper`.

### The linkage checks

```sh
cargo test -p ngnet-workspace-tests --test linkage
```

| Test | Asks |
| --- | --- |
| `quic_bindings_link_no_tls_without_the_crypto_backend` | `readelf -d` over `cargo test -p ngnet-quic-sys --no-default-features --no-run` |
| `quic_wrapper_links_tls_only_when_its_backend_is_enabled` | the same for `ngnet-quic`, with the backend off **and** on |

`cargo tree` cannot answer these. ngtcp2 links no TLS of its own; only its crypto helper
does, and OpenSSL is not a cargo dependency at all -- it arrives through link flags
`ngnet-quic-sys`'s build script emits. So the question is asked of the linked binary instead.
By hand, one executable is enough to see the difference:

```sh
cargo test -p ngnet-quic-sys --no-default-features --no-run
readelf -d target/debug/deps/smoke-* | grep -i 'NEEDED.*libssl'   # expect nothing
```

The wrapper's check asserts **both** halves, because a guard that passes in either
configuration proves nothing -- which was true of the bindings check above until the wrapper
gave it something to see. With the TLS backend on, `ngnet-quic` genuinely does link OpenSSL;
with it off it must not. The positive half asserts that *at least one* binary links `libssl`,
not that all do: of the five test executables `ngnet-quic` produces, only those that exercise
the TLS seam pull it in.

These two are **Linux-only**, and the two halves of that are handled differently on purpose.
On a platform whose executables are not ELF they report themselves skipped, so the suite stays
runnable on macOS. On Linux they always run, and a missing `readelf` is a failure rather than
a skip -- that half is what stops them evaporating quietly on the platform where they mean
something.

They are deliberately **not** `#[ignore]`d, although they cost a nested build. The three
configurations they build are ones CI builds anyway, so the nested builds reorder work rather
than add it, and warm the whole crate runs in well under a second. More to the point, the
only thing that carries these checks into CI is `cargo test --workspace` picking up a
workspace member; `#[ignore]` would remove them from exactly that and need new workflow
wiring to restore -- wiring whose absence would be invisible.

The completion transport has no `cargo test` line of its own here, and does not need one:
`cargo test --workspace --all-features` builds and runs it. (This document listed a
standalone `cargo test -p ngnet-h2-tests --features completion` for some time, matching no
step in the workflow; it is gone.) The graph-level claim that this build contains no
readiness backend is `completion_transport_compiles_no_readiness_backend`, above.

CI deliberately does not run a repository-wide `cargo fmt --check`: this repo is not globally
rustfmt-clean, and the convention is to format only touched files.

The QUIC wrapper is additionally tested in **release**, which the other crates are not.
ngtcp2 validates its settings and transport parameters with `assert()`, and `NDEBUG` strips
every one of them from a release build -- so `ngnet-quic` performs those checks itself, and
that run is what proves they hold where the C library's own no longer do.

There is no MSRV check, because there is no declared MSRV: no crate sets `rust-version`.
The workspace could not honour a single minimum anyway — the benchmark crate's Criterion
dependency and compio's buffer crate both need newer compilers than the rest — so a declared
minimum would have been a claim about some crates rather than about the workspace.
`rust-toolchain.toml` names the one toolchain everything is built with, and CI reads it.
