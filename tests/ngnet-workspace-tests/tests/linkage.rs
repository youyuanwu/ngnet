//! What the linked binaries actually pull in.
//!
//! These two checks exist because `cargo tree` cannot answer the question they ask. ngtcp2
//! links no TLS of its own; only its crypto helper does, and OpenSSL is not a cargo
//! dependency at all -- it arrives through link flags `ngnet-quic-sys`'s build script emits.
//! There is no graph to inspect, so the claim is checked where it is actually observable: in
//! the dynamic section of a linked binary.
//!
//! That makes these the more expensive of the migrated checks, since answering them requires
//! a build rather than a resolution. See the note on ignoring below for why they are not
//! ignored anyway.
//!
//! They were steps in `.github/workflows/ci.yml` until they were moved here. A contributor
//! runs them with:
//!
//! ```sh
//! cargo test -p ngnet-workspace-tests --test linkage
//! ```
//!
//! # Why these are not `#[ignore]`d
//!
//! They cost a nested build, so making them opt-in is the obvious thought, and it is wrong
//! here for two reasons.
//!
//! The first is that they cost much less than they look like they should. The three
//! configurations they build -- `ngnet-quic-sys --no-default-features`, `ngnet-quic
//! --no-default-features`, and `ngnet-quic` with default features -- are all built by CI
//! anyway, by the `cargo test` steps that exercise exactly those feature combinations. The
//! nested builds reorder that work rather than add it, and against a warm target directory
//! they take about a second.
//!
//! The second is decisive. The only thing that carries these checks into CI is that
//! `cargo test --workspace` picks up a new workspace member. `#[ignore]` removes them from
//! precisely that, and would need new workflow wiring to put them back -- wiring whose
//! absence would be invisible, because the suite would still report success. Trading a
//! second of build time for a check that can silently disappear is the trade this migration
//! exists to stop making.
//!
//! # Platform
//!
//! ELF is not universal, and the two halves of that are handled differently on purpose. On a
//! platform whose executables are Mach-O or PE there is no dynamic section to read: the
//! question is malformed rather than failing, so the tests report themselves skipped and a
//! contributor on macOS can still run the suite. On Linux they always run, and a missing
//! `readelf` is a failure rather than a skip -- that half is what stops the checks from
//! evaporating quietly on the one platform where they mean something.

use ngnet_workspace_tests::{
    is_libssl, is_openssl, needed_libraries, platform_uses_elf, test_executables,
};

/// The QUIC bindings link no TLS without the crypto backend.
///
/// ngtcp2 itself links no TLS; only the OpenSSL crypto helper does, and that helper sits
/// behind a default-on `crypto-ossl` feature. The crate promises that `--no-default-features`
/// reflects that, and the workspace test runs only ever exercise it with the backend on, so
/// without this the promise is untested.
///
/// This is a claim about the *linked binary*, not about the manifest and not about the
/// dependency graph. `crates/ngnet-quic-sys` declares no OpenSSL dependency in either
/// configuration, because there is none to declare -- the library arrives through link flags
/// the build script emits. A manifest test and a `cargo tree` check would both pass here with
/// OpenSSL linked in, which is why the question is asked of `readelf`.
#[test]
fn quic_bindings_link_no_tls_without_the_crypto_backend() {
    if !platform_uses_elf() {
        eprintln!(
            "SKIPPED: quic_bindings_link_no_tls_without_the_crypto_backend -- this check reads \
             the ELF dynamic section, and this platform does not produce ELF executables. It \
             runs on Linux, which is what CI is."
        );
        return;
    }

    for binary in test_executables(&["-p", "ngnet-quic-sys", "--no-default-features"]) {
        let linked: Vec<String> = needed_libraries(&binary)
            .into_iter()
            .filter(|library| is_openssl(library))
            .collect();

        assert!(
            linked.is_empty(),
            "{} links OpenSSL ({}) with the crypto-ossl feature off.\n\
             ngtcp2 itself links no TLS, and the bindings promise that \
             --no-default-features reflects that.\n\
             Inspect it with:\n  readelf -d {}",
            binary.display(),
            linked.join(", "),
            binary.display(),
        );
    }
}

/// The QUIC wrapper links TLS only when its backend is enabled.
///
/// The same claim one layer up, and the one that actually discriminates. `ngnet-quic` with
/// its TLS backend on genuinely does link OpenSSL; with it off it must not. Both halves are
/// asserted, because a guard that passes in both configurations is proving nothing -- which
/// was true of the bindings-level check above until the wrapper gave it something to see.
///
/// The two halves are one test rather than two on purpose. Split apart, the positive half
/// could be deleted or allowed to rot while the negative half stayed green, and the suite
/// would report success for a check that had stopped discriminating. That is the exact
/// failure mode the paragraph above describes, so the arrangement that makes it impossible
/// is worth the slightly larger test.
///
/// The positive half asserts that **at least one** binary links `libssl`, not that all of
/// them do. Of the five test executables `ngnet-quic` produces, only those that actually
/// exercise the TLS seam pull it in; `invariants`, `compat_surface` and `versioned_ffi`
/// legitimately link neither. Requiring all five would fail for a correct build.
#[test]
fn quic_wrapper_links_tls_only_when_its_backend_is_enabled() {
    if !platform_uses_elf() {
        eprintln!(
            "SKIPPED: quic_wrapper_links_tls_only_when_its_backend_is_enabled -- this check \
             reads the ELF dynamic section, and this platform does not produce ELF \
             executables. It runs on Linux, which is what CI is."
        );
        return;
    }

    // The backend off: nothing may link OpenSSL.
    for binary in test_executables(&["-p", "ngnet-quic", "--no-default-features"]) {
        let linked: Vec<String> = needed_libraries(&binary)
            .into_iter()
            .filter(|library| is_openssl(library))
            .collect();

        assert!(
            linked.is_empty(),
            "{} links OpenSSL ({}) with the TLS backend off.\n\
             The seam is still compiled and still type-checked with the backend off; there \
             is simply meant to be nothing behind it.\n\
             Inspect it with:\n  readelf -d {}",
            binary.display(),
            linked.join(", "),
            binary.display(),
        );
    }

    // The backend on: something must, or the half above is vacuous.
    let with_backend = test_executables(&["-p", "ngnet-quic"]);
    let linking: Vec<String> = with_backend
        .iter()
        .filter(|binary| needed_libraries(binary).iter().any(|l| is_libssl(l)))
        .map(|binary| binary.display().to_string())
        .collect();

    assert!(
        !linking.is_empty(),
        "no binary links libssl with the TLS backend on, so the negative half of this check \
         proves nothing -- it would pass in both configurations.\n\
         Either the backend stopped linking OpenSSL, or the build layout changed and these \
         are no longer the binaries to inspect.\n\
         Inspected {} executable(s):\n{}",
        with_backend.len(),
        with_backend
            .iter()
            .map(|b| format!("  {}", b.display()))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The hyperium/ngtcp2 adapter really does link OpenSSL.
///
/// The positive half of a claim `dependency_graph.rs` makes negatively. That check permits
/// OpenSSL in `h3-ngnet-quic`'s graph, unlike the QMux adapter's, on the grounds that
/// `ngnet-quic` links it. A permission is not evidence, though: if the transport ever stopped
/// linking OpenSSL, the graph check would keep passing while quietly asserting nothing, and the
/// difference between the two adapters -- the reason they need separate checks at all -- would
/// have evaporated unnoticed. This is what makes that difference observable.
///
/// As with the QUIC wrapper's own check, **at least one** binary must link `libssl`: the
/// adapter's test executables exercise different seams and not all of them reach TLS.
#[test]
fn the_hyperium_quic_adapter_links_the_tls_its_transport_brings() {
    if !platform_uses_elf() {
        eprintln!(
            "SKIPPED: the_hyperium_quic_adapter_links_the_tls_its_transport_brings -- this \
             check reads the ELF dynamic section, and this platform does not produce ELF \
             executables. It runs on Linux, which is what CI is."
        );
        return;
    }

    let binaries = test_executables(&["-p", "h3-ngnet-quic"]);
    let linking: Vec<String> = binaries
        .iter()
        .filter(|binary| needed_libraries(binary).iter().any(|l| is_libssl(l)))
        .map(|binary| binary.display().to_string())
        .collect();

    assert!(
        !linking.is_empty(),
        "no h3-ngnet-quic test binary links libssl.\n\
         This adapter is allowed OpenSSL in its dependency graph precisely because \
         `ngnet-quic` links it; if nothing links it any more, that allowance has stopped \
         meaning anything and `dependency_graph.rs` should be tightened to match.\n\
         Inspected {} executable(s):\n{}",
        binaries.len(),
        binaries
            .iter()
            .map(|b| format!("  {}", b.display()))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The QMux crates link no TLS, in any configuration.
///
/// Simpler than the QUIC checks above, and simpler for a reason worth stating. `ngnet-quic`
/// needs a feature matrix because it genuinely links OpenSSL when its backend is on; QMux has
/// no such feature and no such configuration. The protocol delegates confidentiality and
/// integrity to whatever carries its byte stream and provides none itself, and libdwnx depends
/// on nothing at all -- so the absence here is unconditional.
///
/// Asked of `readelf` rather than of the manifest for the same reason as the QUIC checks: a
/// native library arrives through link flags a build script emits, which no manifest or
/// `cargo tree` inspection can see.
///
/// `ngnet-qmux-h3` is inspected alongside the two QMux crates because it is the one place the
/// property could plausibly break. It links nghttp3 as well as libdwnx, and a caller running
/// HTTP/3 over QMux is the caller most likely to assume TLS is in the picture somewhere --
/// it is HTTP/3, after all. It is not: the join adds an HTTP layer and no cryptography, and
/// if OpenSSL ever appeared under it, that would be a transport arriving by accident rather
/// than a decision anyone made.
#[test]
fn qmux_links_no_tls_in_any_configuration() {
    if !platform_uses_elf() {
        eprintln!(
            "SKIPPED: qmux_links_no_tls_in_any_configuration -- this check reads the ELF \
             dynamic section, and this platform does not produce ELF executables. It runs on \
             Linux, which is what CI is."
        );
        return;
    }

    for package in [
        "ngnet-qmux-sys",
        "ngnet-qmux",
        "ngnet-qmux-h3",
        "h3-ngnet-qmux",
    ] {
        for binary in test_executables(&["-p", package]) {
            let linked: Vec<String> = needed_libraries(&binary)
                .into_iter()
                .filter(|library| is_openssl(library))
                .collect();

            assert!(
                linked.is_empty(),
                "{} links OpenSSL ({}).\n\
                 QMux has no TLS backend and libdwnx has no external dependencies, so nothing \
                 here should reach a TLS library in any configuration.\n\
                 Inspect it with:\n  readelf -d {}",
                binary.display(),
                linked.join(", "),
                binary.display(),
            );
        }
    }
}
