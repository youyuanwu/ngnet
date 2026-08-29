# Packaging and publishing

This workspace publishes seven crates as one release set:

| Layer | Crates |
| --- | --- |
| Native bindings | `ngnet-h2-sys`, `ngnet-h3-sys`, `ngnet-quic-sys` |
| Safe wrappers | `ngnet-h2`, `ngnet-h3`, `ngnet-quic` |
| HTTP/3 adapter | `ngnet-quic-h3` |

The QMux, axum, util, test and benchmark crates have `publish = false` and are not part of
this release. The root manifest gives every publishable workspace dependency both a `path`
and a `version`: local builds use the path, while Cargo removes it from the published
manifest and keeps the registry version.

## Before packaging

Use the pinned toolchain from `rust-toolchain.toml`, initialize the native sources, and run
the checks in [`ci.md`](ci.md):

```sh
just submodules
just submodules-status
```

The default `ngnet-quic-sys` feature builds ngtcp2's OpenSSL backend and therefore requires
OpenSSL 3.5 or newer. Its core-only configuration does not:

```sh
cargo package -p ngnet-quic-sys --locked --no-default-features
```

The final package and publish commands should run from a clean commit. Do not add
`--allow-dirty`: refusing a dirty tree proves that the archive can be reproduced from the
commit recorded in its `.cargo_vcs_info.json`. `--allow-dirty` is useful only while
iterating.

Authenticate once before the real publish:

```sh
cargo login
```

## Inspecting package contents

`cargo package --list` prints the exact archive contents without creating the archive. Check
the three native packages explicitly: their `vendor/` entries should contain the library C
sources and headers, required build files and upstream licenses, but no upstream test,
example, fuzzing or autotools source.

```sh
cargo package -p ngnet-h2-sys --list | grep '^vendor/'
cargo package -p ngnet-h3-sys --list | grep '^vendor/'
cargo package -p ngnet-quic-sys --list | grep '^vendor/'
```

The allowlists live in each `-sys` manifest. Some apparently unrelated `CMakeLists.txt` and
`.in` files are intentional: the upstream top-level projects enter or configure them even
when the corresponding application, documentation or example target is disabled.

## Packaging the release set

Cargo 1.98 accepts multiple `--package` selections and understands their workspace
dependencies. One command creates and verifies every release archive:

```sh
cargo package --locked \
  -p ngnet-h2-sys \
  -p ngnet-h2 \
  -p ngnet-h3-sys \
  -p ngnet-h3 \
  -p ngnet-quic-sys \
  -p ngnet-quic \
  -p ngnet-quic-h3
```

The resulting `.crate` files are under `target/package/`. Verification extracts each archive
and builds from that isolated copy, which is what proves that a package does not depend on
an unlisted workspace file. It does not replace the test suite.

Do not use `--no-verify`. It suppresses the isolated build that catches missing vendored
sources, incomplete include patterns and registry-only dependency failures.

`cargo package --workspace` is shorter and skips members with `publish = false`, but the
explicit list is deliberate: making another workspace member publishable later cannot add
it to this release accidentally.

## Dry run

Use the same selection with `cargo publish --dry-run` for the closest available simulation
of crates.io publishing:

```sh
cargo publish --dry-run --locked \
  -p ngnet-h2-sys \
  -p ngnet-h2 \
  -p ngnet-h3-sys \
  -p ngnet-h3 \
  -p ngnet-quic-sys \
  -p ngnet-quic \
  -p ngnet-quic-h3
```

This performs package construction, isolated verification and registry checks without
uploading. It does not reserve crate names or versions, and it cannot prove that the
credentials used for the eventual upload own an existing crate.

## Publishing

Publishing is permanent at the crate-version level: an uploaded version cannot be replaced.
Run the same command without `--dry-run` only after inspecting the archives and completing
the dry run:

```sh
cargo publish --locked \
  -p ngnet-h2-sys \
  -p ngnet-h2 \
  -p ngnet-h3-sys \
  -p ngnet-h3 \
  -p ngnet-quic-sys \
  -p ngnet-quic \
  -p ngnet-quic-h3
```

Cargo orders the selected crates by dependency: native bindings first, then safe wrappers,
then `ngnet-quic-h3`. After each upload it polls the registry index before attempting a
dependent crate.

The operation is not atomic. If a later upload fails, every earlier successful version
remains published. Do not use `--keep-going` for a release. Before retrying, use `cargo info`
to identify which versions reached the registry, then rerun `cargo publish` with only the
remaining `-p` selections; crates.io rejects an attempt to upload the same version twice.

## Confirming the release

Once Cargo reports success, confirm that every version has reached the registry index:

```sh
VERSION=0.0.1
for crate in \
  ngnet-h2-sys ngnet-h2 \
  ngnet-h3-sys ngnet-h3 \
  ngnet-quic-sys ngnet-quic \
  ngnet-quic-h3
do
  cargo info "$crate@$VERSION"
done
```

An upload can succeed while Cargo times out waiting for the index. Check `cargo info` before
retrying: crates.io rejects a second upload of the same crate version.
