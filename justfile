# Task runner for the checkout steps that are easy to get subtly wrong.
#
# Nothing here is required to build: `cargo build` and `cargo test` remain the entry points,
# and every recipe below is a git command a contributor could type by hand. What the file
# buys is that the *exact* set of submodules this repository needs lives in one place rather
# than in prose, so it cannot drift from what the build scripts expect.
#
# Install with `cargo install just`, or from your package manager.

# `just` with no arguments lists what is available rather than doing something.
[doc("List the available recipes")]
default:
    @just --list

# Check out exactly the submodules a build needs, and none of the ones it does not.
#
# All three vendored libraries declare nested submodules that only their own tests, tooling
# and example applications use, so a `--recursive` checkout fetches several hundred megabytes
# this repository never compiles:
#
#   nghttp2  mruby, neverbleed, urlparse, munit  — nghttpx, nghttp, h2load, upstream tests
#   nghttp3  tests/munit                         — upstream tests
#   ngtcp2   tests/munit, third-party/urlparse   — upstream tests, example client/server
#
# ngtcp2's `third-party/urlparse` looks like a library dependency from its path, but the
# target is wrapped in `if(LIBEV_FOUND AND LIBNGHTTP3_FOUND)` and so is only ever compiled
# for the example applications, which `ENABLE_LIB_ONLY=ON` does not build.
#
# The one nested submodule that *is* required is `nghttp3/lib/sfparse`: the structured-field
# parser is part of the library itself, not of its test suite, and nghttp3 does not compile
# without it. That asymmetry is the whole reason this recipe exists — "clone non-recursively"
# is correct for nghttp2 and ngtcp2, and quietly wrong for nghttp3.
#
# Pass `depth=1` for a shallow checkout when the history is not wanted:
#
#     just submodules depth=1
[doc("Check out the submodules the build requires (never --recursive)")]
submodules depth="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=(--init)
    if [ -n "{{ depth }}" ]; then
        args+=(--depth "{{ depth }}")
    fi
    git submodule update "${args[@]}" deps/nghttp2 deps/nghttp3 deps/ngtcp2
    git -C deps/nghttp3 submodule update "${args[@]}" lib/sfparse

# Report which submodules are present, which are missing, and which have drifted from the
# commit this repository pins. A leading `-` means "not checked out", `+` means "checked out
# at a different commit than recorded" — the two states that turn into confusing build
# failures rather than obvious ones.
[doc("Show which submodules are checked out, missing or at an unexpected commit")]
submodules-status:
    @git submodule status --recursive
