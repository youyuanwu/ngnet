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
cargo test -p ngnet-quic --release
cargo test -p ngnet-h2-tests --features completion

cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy -p ngnet-h2 --all-targets -- -D warnings
cargo clippy -p ngnet-h2 --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-h3 --all-targets -- -D warnings
cargo clippy -p ngnet-h3 --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-quic-sys --no-default-features --all-targets -- -D warnings
cargo clippy -p ngnet-quic --all-targets -- -D warnings
cargo clippy -p ngnet-quic --no-default-features --all-targets -- -D warnings

for f in "" "--no-default-features" "--all-features" "--features tokio" "--features completion"; do
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-h2 $f
done

# `ngnet-h3` has a matrix of its own now that its async layer sits behind a default-on
# `http` feature. A doc link to a gated item passes under one configuration and breaks the
# others, which is the failure the h2 matrix above exists for.
for f in "" "--no-default-features" "--all-features"; do
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-h3 $f
done
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-h3-sys -p ngnet-h3-tests -p ngnet-quic-sys

# `ngnet-quic` has a matrix of its own for the same reason: its TLS backend is default-on,
# so a doc link into it breaks the configuration without it.
for f in "" "--no-default-features" "--all-features"; do
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-quic $f
done
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-quic-tests

# Two claims, two commands, and the flags are the whole point.
#
# That the sans-I/O core still stands alone -- asked with `--no-default-features`, because
# `cargo tree` resolves default features and the async layer's feature is default-on, so
# without the flag this asks about the async layer instead. A different claim from the
# manifest test in h3/invariants.md: that one asserts what `ngnet-h3` *declares*, this
# asserts what the resolved graph contains, which is what a downstream user gets.
cargo tree -p ngnet-h3 --no-default-features -e normal   # only ngnet-h3-sys

# That no transport or TLS crate reaches the graph in *any* configuration -- asked with
# default features on. Cargo unifies features across a workspace, so a crate added later
# could pull a transport in with nothing in `ngnet-h3` changing.
cargo tree -p ngnet-h3 -e normal | grep -qiE 'quinn|rustls|tokio|ring' && exit 1

# That the QUIC bindings link no TLS with `crypto-ossl` off. `cargo tree` cannot answer
# this one: OpenSSL is not a cargo dependency at all, it arrives through link flags the
# build script emits. So it is asked of the linked binary instead. CI inspects every test
# executable; by hand, one is enough to see the difference:
cargo test -p ngnet-quic-sys --no-default-features --no-run
readelf -d target/debug/deps/smoke-*  | grep -i 'NEEDED.*libssl'   # expect nothing

# And the same claim one layer up, which is the one that actually discriminates. With the
# TLS backend on, `ngnet-quic` genuinely does link OpenSSL; with it off it must not. CI
# asserts *both* halves, because a guard that passes in either configuration proves nothing
# -- which was true of the bindings check above until the wrapper gave it something to see.
cargo test -p ngnet-quic --no-run                      # expect libssl
cargo test -p ngnet-quic --no-default-features --no-run # expect none

# Runs each benchmark once without timing it. Benchmarks are not part of `cargo test`, so
# without this they rot silently as the API moves.
cargo bench --workspace -- --test
```

Run `touch crates/ngnet-h2/src/lib.rs` before a final run so no stale incremental artefact
flatters the result.

CI additionally checks a property no source file carries: that the completion transport's
build contains **no readiness backend**. That is a fact about the resolved dependency graph,
and cargo unifies features across the workspace, so a crate added later could enable compio's
`polling` and restore the silent epoll fallback without a line of code changing. The runtime
assertion in the compio test only fires where io_uring is genuinely absent, which is not true
of CI or of most developer machines — this is the check that catches it where io_uring exists.

```sh
cargo tree -p ngnet-h2 --features completion -e features | grep 'compio-driver feature "polling"'
```

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
