//! What the resolved dependency graph actually contains.
//!
//! Every check in this file asks `cargo tree`, which is to say it asks what a build resolves
//! to after versions are picked, defaults applied and features unified across the workspace.
//! That is the graph a downstream user gets. It is deliberately **not** the same question the
//! `invariants.rs` suite in each crate asks: those assert what a crate *declares* in its own
//! `Cargo.toml`, and a declaration can stay untouched while the resolution around it moves.
//!
//! These were steps in `.github/workflows/ci.yml` until they were moved here, so that a
//! contributor can ask them without opening a pull request:
//!
//! ```sh
//! cargo test -p ngnet-workspace-tests --test dependency_graph
//! ```

use ngnet_workspace_tests::{cargo_tree, contains_at_word_boundary, dependency_name};

/// The HTTP/3 sans-I/O core depends on nothing but its bindings.
///
/// `cargo tree` resolves default features, and the async layer's `http` feature is
/// default-on, so this must be asked with `--no-default-features` or it is asking about the
/// async layer instead -- a different crate, and one that legitimately has more in its graph.
///
/// This is the claim about the *resolved graph*. `crates/ngnet-h3/tests/invariants.rs` makes
/// the neighbouring claim about the *manifest*: that `ngnet-h3` declares one dependency. Both
/// are worth having, because a dependency arriving transitively would move this one while
/// leaving that one green.
///
/// The shape of the assertion is not incidental. `cargo tree` prints the crate itself on the
/// first line and one line per dependency after it, each prefixed with a box-drawing
/// character, so "exactly one dependency" is "exactly one line after the first". Counting
/// alone would pass for any single dependency at all, so the name on that line is checked
/// too -- a graph containing exactly one wrong crate is not the property being defended.
#[test]
fn http3_core_depends_only_on_its_bindings() {
    let tree = cargo_tree(&["-p", "ngnet-h3", "--no-default-features", "-e", "normal"]);
    let dependencies: Vec<&str> = tree
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(
        dependencies.len(),
        1,
        "the HTTP/3 core's dependency graph is no longer just its bindings: expected exactly \
         one dependency, found {}.\n\
         Inspect it with:\n  \
         cargo tree -p ngnet-h3 --no-default-features -e normal\n\n{tree}",
        dependencies.len(),
    );

    assert!(
        dependency_name(dependencies[0]) == "ngnet-h3-sys",
        "the HTTP/3 core has exactly one dependency, but it is not its bindings: it is `{}`.\n\
         Counting alone would have passed here, which is why the name is checked.\n\
         Inspect it with:\n  \
         cargo tree -p ngnet-h3 --no-default-features -e normal\n\n{tree}",
        dependency_name(dependencies[0]),
    );
}

/// No transport or TLS crate reaches the HTTP/3 wrapper, in *any* configuration.
///
/// Asked with default features on, unlike the check above, and for a different reason: cargo
/// unifies features across a workspace, so a crate added later could pull a transport into
/// this graph with nothing in `ngnet-h3` itself changing. Asking with the async layer enabled
/// is asking the question in the configuration where it can actually go wrong.
///
/// The list is matched as plain case-insensitive substrings, with no word boundary, because
/// that is what `grep -qiE 'quinn|rustls|tokio|ring'` did. It is deliberately blunt: the
/// property being defended is that nothing in this neighbourhood appears at all, and a blunt
/// matcher fails safe by occasionally catching a name that merely contains one of these.
#[test]
fn no_transport_or_tls_reaches_http3() {
    const FORBIDDEN: &[&str] = &["quinn", "rustls", "tokio", "ring"];

    let tree = cargo_tree(&["-p", "ngnet-h3", "-e", "normal"]);
    let lowercase = tree.to_ascii_lowercase();

    for forbidden in FORBIDDEN {
        assert!(
            !lowercase.contains(forbidden),
            "`{forbidden}` reached ngnet-h3's normal dependency graph, and the crate's whole \
             claim is that it owns no transport and no TLS.\n\
             Find it with:\n  cargo tree -p ngnet-h3 -e normal -i {forbidden}\n\
             Test-only dependencies belong in ngnet-h3-tests.\n\n{tree}",
        );
    }
}

/// No async runtime reaches the QUIC wrapper with its endpoint layer off.
///
/// `ngnet-quic` ships a ready-made socket and clock for tokio behind an off-by-default
/// feature, so the crate's source legitimately contains the word — which is why this claim
/// is made against the **resolved dependency graph** rather than by scanning the source, as
/// `ngnet-h3`'s equivalent does. The two ask different questions: `ngnet-quic`'s own
/// invariants test asserts what the manifest *declares*, and this asserts what a downstream
/// caller actually builds.
///
/// Asked with default features on, because that is the configuration a caller gets by
/// asking for `ngnet-quic` and is where feature unification could go wrong.
#[test]
fn no_async_runtime_reaches_the_quic_wrapper_by_default() {
    const FORBIDDEN: &[&str] = &["tokio", "quinn", "rustls"];

    let tree = cargo_tree(&["-p", "ngnet-quic", "-e", "normal"]);
    let lowercase = tree.to_ascii_lowercase();

    for forbidden in FORBIDDEN {
        assert!(
            !lowercase.contains(forbidden),
            "`{forbidden}` reached ngnet-quic's normal dependency graph with default \
             features, and the crate's claim is that its runtime integration is something a \
             caller opts into rather than something they are given.\n\
             Find it with:\n  cargo tree -p ngnet-quic -e normal -i {forbidden}\n\
             The tokio socket and clock belong behind the `tokio` feature.\n\n{tree}",
        );
    }
}

/// And the endpoint layer alone brings no runtime either.
///
/// The layer between the two extremes: asynchronous, but with the caller supplying the
/// socket and the clock. If a runtime appeared here, the seams would not be seams.
#[test]
fn the_quic_endpoint_layer_alone_brings_no_runtime() {
    let tree = cargo_tree(&[
        "-p",
        "ngnet-quic",
        "--no-default-features",
        "--features",
        "endpoint",
        "-e",
        "normal",
    ]);
    let lowercase = tree.to_ascii_lowercase();

    assert!(
        !lowercase.contains("tokio"),
        "tokio reached ngnet-quic's graph with only the `endpoint` feature enabled, which \
         would mean the socket and clock seams are not seams at all.\n\n{tree}",
    );
}

/// The completion transport compiles no readiness backend.
///
/// `ngnet-h2`'s compio dependency takes `io-uring` and deliberately not `polling`: with both,
/// compio compiles a fusion driver that probes the kernel and silently falls back to epoll
/// when io_uring cannot be obtained. A transport that quietly became readiness-based while
/// still calling itself completion-based would make every measurement taken through it a lie.
///
/// That is a property of the resolved dependency graph and of no source file, which is why it
/// is checked here rather than by a compile-time assertion. Cargo unifies features across the
/// whole workspace, so a crate added later could enable compio's `polling` and undo this with
/// nothing in the code changing.
///
/// The runtime assertion in the compio test only fires where io_uring is genuinely absent,
/// which is not true of CI or of most developer machines -- so this is the check that catches
/// it where io_uring exists, which is to say nearly everywhere it would otherwise go unnoticed.
#[test]
fn completion_transport_compiles_no_readiness_backend() {
    let tree = cargo_tree(&[
        "-p",
        "ngnet-h2",
        "--features",
        "completion",
        "-e",
        "features",
    ]);

    // Anchor the negative before asserting it. This check is unusual among the five in that
    // the string it looks for only exists while `compio-driver` is in the graph at all: if the
    // completion transport were rewritten onto a different driver, or compio's crate split
    // renamed this one, the search target would simply vanish and the assertion below would
    // pass while asking nothing. The shell this replaces had the same hole. It is worth
    // closing here rather than merely reproducing, because a check that has quietly stopped
    // asking its question is the exact failure this repository has been bitten by before --
    // and unlike the hyper and transport checks, whose targets are crates that *should* be
    // absent, this one names a feature of a crate that must be present for the question to
    // mean anything.
    assert!(
        tree.contains("compio-driver"),
        "`compio-driver` is not in the completion transport's feature graph at all, so the \
         `polling` check below is asking nothing.\n\
         Either the transport no longer uses compio's driver, in which case this test needs \
         rewriting against whatever replaced it, or the graph broke.\n\
         Inspect it with:\n  \
         cargo tree -p ngnet-h2 --features completion -e features\n\n{tree}",
    );

    assert!(
        !tree.contains(r#"compio-driver feature "polling""#),
        "compio's `polling` feature reached the build, which restores the fusion driver and \
         its silent fallback to epoll.\n\
         Find who enabled it with:\n  \
         cargo tree -p ngnet-h2 --features completion -e features\n\n{tree}",
    );
}

/// No hyper crate reaches the axum integration.
///
/// This is the claim `ngnet-axum` exists to make -- that an axum `Router` runs without hyper
/// underneath it -- and it is easy to state and easy to lose. axum has optional features that
/// pull `hyper-util` in, `ConnectInfo` among them, and enabling one in a moment of convenience
/// would quietly reinstate the dependency the crate was written to remove. A reader cannot see
/// that in a manifest, because it would arrive transitively.
///
/// `-e normal` is the whole point of the check, and it is what makes it honest rather than
/// merely strict. hyper *is* a dev-dependency of `ngnet-axum`, on purpose: the acceptance
/// tests drive the server with an independent HTTP/2 client, because a client from this
/// workspace could only show `ngnet-h2` agreeing with itself. So the claim is about what a
/// downstream user links, not about what the test binaries link, and only the normal graph
/// expresses that.
#[test]
fn no_hyper_reaches_the_axum_integration() {
    let tree = cargo_tree(&["-p", "ngnet-axum", "-e", "normal"]);

    assert!(
        !contains_at_word_boundary(&tree, "hyper"),
        "a hyper crate reached ngnet-axum's normal dependency graph, which is the one thing \
         this crate exists to avoid.\n\
         Find it with:\n  cargo tree -p ngnet-axum -e normal -i hyper\n\
         The usual cause is an axum feature that depends on hyper-util.\n\
         Test-only clients belong in [dev-dependencies], which this check deliberately \
         ignores.\n\n{tree}",
    );
}

/// No hyper crate reaches the client policy layer.
///
/// The same claim one crate over, and it is worth making separately rather than trusting the
/// one above to cover it. `ngnet-util` reaches hyper in its *dev*-dependencies on purpose: the
/// acceptance suite drives the pool against hyper's HTTP/2 server, because a server from this
/// workspace could only show `ngnet-h2` agreeing with itself.
///
/// That is exactly the arrangement in which a hyper crate slips into the normal graph
/// unnoticed, since everything still builds and every test still passes. Only the graph shows
/// it.
#[test]
fn no_hyper_reaches_the_client_policy_layer() {
    let tree = cargo_tree(&["-p", "ngnet-util", "-e", "normal"]);

    assert!(
        !contains_at_word_boundary(&tree, "hyper"),
        "a hyper crate reached ngnet-util's normal dependency graph.\n\
         Find it with:\n  cargo tree -p ngnet-util -e normal -i hyper\n\
         The acceptance suite's hyper server belongs in [dev-dependencies], which this check \
         deliberately ignores.\n\n{tree}",
    );
}

/// The HTTP/3 wrapper does not reach a transport implementation.
///
/// `ngnet-h3` is a state machine over nghttp3 and nothing else. It defines a transport
/// abstraction and takes whatever implements it, which is what lets a caller run HTTP/3 over
/// quinn, over this workspace's own QUIC stack, over QMux, or over something not written yet.
///
/// The workspace now contains three crates implementing that abstraction: one over Quinn, one
/// over `ngnet-quic`, and one over `ngnet-qmux`. That is precisely when this check starts to
/// matter. With a
/// single adapter a stray dependency would be a mild waste; with two, `ngnet-h3` depending on
/// either one would force every caller of the other to compile a transport they will never
/// instantiate — ngtcp2 and OpenSSL for the QMux user, ngtcp2-less QMux bindings for the QUIC
/// user — and the abstraction would have quietly stopped being one.
#[test]
fn the_http3_wrapper_reaches_no_quic_implementation() {
    let tree = cargo_tree(&["-p", "ngnet-h3", "-e", "normal"]);

    for forbidden in [
        "ngnet-quic",
        "ngnet-quic-sys",
        "ngnet-quic-h3",
        "ngnet-qmux",
        "ngnet-qmux-sys",
        "ngnet-qmux-h3",
        "h3-ngnet-qmux",
        "h3-ngnet-quic",
        "quinn",
    ] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached ngnet-h3's normal dependency graph.\n\
             Find it with:\n  cargo tree -p ngnet-h3 -e normal -i {forbidden}\n\
             ngnet-h3 takes a transport through a trait; it must not depend on one.\n\n{tree}",
        );
    }
}

/// The QUIC wrapper does not reach an HTTP/3 implementation.
///
/// The same claim the other way round, and it needs making separately. `ngnet-quic` is
/// useful on its own — raw QUIC streams with no HTTP anywhere — and a caller who wants that
/// should not be compiling nghttp3 to get it.
#[test]
fn the_quic_wrapper_reaches_no_http3_implementation() {
    let tree = cargo_tree(&["-p", "ngnet-quic", "-e", "normal"]);

    for forbidden in ["ngnet-h3", "ngnet-h3-sys", "ngnet-quic-h3"] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached ngnet-quic's normal dependency graph.\n\
             Find it with:\n  cargo tree -p ngnet-quic -e normal -i {forbidden}\n\
             ngnet-quic is usable without HTTP/3 and must stay that way.\n\n{tree}",
        );
    }
}

/// The adapter is the one place the two families meet.
///
/// The negative checks above are only meaningful if something positive holds: that the
/// integration genuinely depends on both. A version of this workspace where `ngnet-quic-h3`
/// had quietly lost one of them would satisfy every check above and integrate nothing.
#[test]
fn the_adapter_depends_on_both_families() {
    let tree = cargo_tree(&["-p", "ngnet-quic-h3", "-e", "normal"]);

    for required in ["ngnet-h3", "ngnet-quic"] {
        assert!(
            contains_at_word_boundary(&tree, required),
            "{required} is missing from ngnet-quic-h3's normal dependency graph.\n\
             Check with:\n  cargo tree -p ngnet-quic-h3 -e normal\n\
             This crate exists to join the two families; without both there is nothing to \
             join.\n\n{tree}",
        );
    }
}

/// The Quinn adapter is the one place `ngnet-h3` and Quinn meet.
///
/// It is separate from both sides for the same reason as `ngnet-quic-h3`: an HTTP/3 caller
/// choosing another QUIC implementation must not compile Quinn, while a Quinn caller not using
/// this HTTP/3 stack must not compile nghttp3. The upstream `h3-quinn` comparison belongs only
/// to the unpublished benchmark crate and must not leak into this adapter.
#[test]
fn the_quinn_adapter_depends_on_http3_and_quinn_only() {
    let tree = cargo_tree(&["-p", "ngnet-h3-quinn", "-e", "normal"]);

    for required in ["ngnet-h3", "quinn", "tokio"] {
        assert!(
            tree.lines()
                .skip(1)
                .any(|line| dependency_name(line) == required),
            "{required} is missing from ngnet-h3-quinn's normal dependency graph.\n\
             Check with:\n  cargo tree -p ngnet-h3-quinn -e normal\n\
             This crate exists to adapt ngnet-h3 to Quinn's Tokio API.\n\n{tree}",
        );
    }

    for forbidden in [
        "ngnet-quic",
        "ngnet-quic-sys",
        "ngnet-quic-h3",
        "ngnet-qmux",
        "ngnet-qmux-sys",
        "ngnet-qmux-h3",
    ] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached ngnet-h3-quinn's normal dependency graph.\n\
             Find it with:\n  cargo tree -p ngnet-h3-quinn -e normal -i {forbidden}\n\
             The adapter joins ngnet-h3 directly to Quinn and no other transport or HTTP/3 \
             implementation belongs in that path.\n\n{tree}",
        );
    }

    assert!(
        !tree
            .lines()
            .skip(1)
            .any(|line| dependency_name(line) == "h3-quinn"),
        "the upstream h3-quinn comparison reached ngnet-h3-quinn's normal dependency graph.\n\
         It belongs only in ngnet-bench.\n\n{tree}",
    );
}

/// The QMux join is the second, and only other, place the two families meet.
///
/// `ngnet-qmux-h3` is the permitted exception to every negative check in this file that names
/// a QMux crate and an HTTP/3 crate together: it exists to implement `ngnet-h3`'s transport
/// trait over `ngnet-qmux`'s connection, so it must reach both, and a check that forbade the
/// combination outright would forbid the crate.
///
/// Stating the positive separately is what keeps the exception honest. The alternative — just
/// leaving `ngnet-qmux-h3` off the forbidden lists — would be satisfied equally well by a
/// crate that had lost one of its two halves, or by no crate at all. This asserts the join is
/// still a join.
#[test]
fn the_qmux_adapter_depends_on_both_families() {
    let tree = cargo_tree(&["-p", "ngnet-qmux-h3", "-e", "normal"]);

    for required in ["ngnet-h3", "ngnet-qmux"] {
        assert!(
            contains_at_word_boundary(&tree, required),
            "{required} is missing from ngnet-qmux-h3's normal dependency graph.\n\
             Check with:\n  cargo tree -p ngnet-qmux-h3 -e normal\n\
             This crate exists to join HTTP/3 to QMux; without both there is nothing to \
             join.\n\n{tree}",
        );
    }

    for forbidden in ["ngnet-quic", "ngnet-quic-sys", "quinn", "openssl-sys"] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached ngnet-qmux-h3's normal dependency graph.\n\
             Find it with:\n  cargo tree -p ngnet-qmux-h3 -e normal -i {forbidden}\n\
             Joining HTTP/3 to QMux needs no QUIC implementation and no TLS: QMux runs over a \
             byte stream the caller supplies.\n\n{tree}",
        );
    }
}

/// The hyperium adapter joins only hyperium H3 and QMux, without choosing a runtime or TLS.
#[test]
fn the_hyperium_qmux_adapter_has_the_exact_direct_dependencies() {
    let tree = cargo_tree(&["-p", "h3-ngnet-qmux", "-e", "normal"]);
    let mut direct: Vec<String> = tree
        .lines()
        .skip(1)
        .filter(|line| line.starts_with('\u{251c}') || line.starts_with('\u{2514}'))
        .map(dependency_name)
        .collect();
    direct.sort();
    assert_eq!(
        direct,
        ["bytes", "h3", "ngnet-qmux"],
        "h3-ngnet-qmux must directly join bytes, hyperium h3, and ngnet-qmux only.\n{tree}"
    );

    for forbidden in [
        "ngnet-h3",
        "ngnet-quic",
        "ngnet-quic-sys",
        "quinn",
        "rustls",
        "openssl",
        "openssl-sys",
        "compio",
    ] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached h3-ngnet-qmux.\n{tree}"
        );
    }
    assert!(
        !tree
            .lines()
            .skip(1)
            .any(
                |line| (line.starts_with('\u{251c}') || line.starts_with('\u{2514}'))
                    && dependency_name(line) == "tokio"
            ),
        "tokio may occur transitively through hyperium h3 synchronization, but must not be a \
         direct adapter dependency.\n{tree}"
    );
}

/// The hyperium/ngtcp2 adapter joins exactly three crates, and reaches no second stack.
///
/// The counterpart of the check above, and it has to say something different in one place.
/// `h3-ngnet-qmux` may not reach OpenSSL because QMux provides no confidentiality at all;
/// `h3-ngnet-quic` legitimately does, because `ngnet-quic` links it and that is the whole
/// point of the transport. So OpenSSL is deliberately absent from the forbidden list here,
/// and `linkage.rs` asserts the positive form of the same fact -- that the OpenSSL really is
/// there -- so its absence from this list cannot quietly become an unnoticed loss.
///
/// What must stay out is a *second* protocol stack: `ngnet-h3` (the workspace's own HTTP/3,
/// which is what this adapter exists as an alternative to), quinn and rustls (a different
/// QUIC implementation entirely), and the whole QMux family.
#[test]
fn the_hyperium_quic_adapter_has_the_exact_direct_dependencies() {
    let tree = cargo_tree(&["-p", "h3-ngnet-quic", "-e", "normal"]);
    let mut direct: Vec<String> = tree
        .lines()
        .skip(1)
        .filter(|line| line.starts_with('\u{251c}') || line.starts_with('\u{2514}'))
        .map(dependency_name)
        .collect();
    direct.sort();
    assert_eq!(
        direct,
        ["bytes", "h3", "ngnet-quic"],
        "h3-ngnet-quic must directly join bytes, hyperium h3, and ngnet-quic only.\n\
         In particular no futures crate may appear here. The stable waker its expiry timer is \
         polled under is built from `std::task::Wake` for exactly that reason -- an `ArcWake` \
         would have made this a four-crate join. (`futures-util` does appear further down the \
         tree; hyperium h3 brings it, which is h3's business and not this crate's.)\n{tree}"
    );

    for forbidden in [
        "ngnet-h3",
        "ngnet-h3-sys",
        "ngnet-qmux",
        "ngnet-qmux-sys",
        "ngnet-qmux-h3",
        "h3-ngnet-qmux",
        "quinn",
        "rustls",
        "compio",
    ] {
        assert!(
            !contains_at_word_boundary(&tree, forbidden),
            "{forbidden} reached h3-ngnet-quic.\n\
             Find it with:\n  cargo tree -p h3-ngnet-quic -e normal -i {forbidden}\n{tree}"
        );
    }

    // The one difference from the QMux adapter, asserted rather than assumed.
    assert!(
        contains_at_word_boundary(&tree, "ngnet-quic-sys"),
        "h3-ngnet-quic must reach ngnet-quic-sys: it is an ngtcp2 adapter, and if the \
         transport stopped pulling in its bindings then this check and the OpenSSL linkage \
         check are both asking about something that is no longer there.\n{tree}"
    );
}

/// The QMux core depends only on its bindings.
///
/// The same claim `http3_core_depends_only_on_its_bindings` makes, for the newest pair, and
/// asked the same way: with `--no-default-features`, because `ngnet-qmux` now has an
/// asynchronous layer behind a default-on `io` feature. This is the sans-I/O crate as it
/// existed before that layer, and the claim is that turning the layer off leaves it
/// unchanged -- which is what Spec SC-005 asks for and what a caller who wants a state machine
/// and nothing else is buying.
///
/// The shape of the assertion is not incidental. `cargo tree` prints the crate itself on the
/// first line and one line per dependency after it, so "exactly one dependency" is "exactly
/// one line after the first". Counting alone would pass for any single dependency at all, so
/// the name on that line is checked too.
#[test]
fn qmux_core_depends_only_on_its_bindings() {
    let tree = cargo_tree(&["-p", "ngnet-qmux", "--no-default-features", "-e", "normal"]);
    let dependencies: Vec<&str> = tree
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(
        dependencies.len(),
        1,
        "the QMux core's dependency graph is no longer just its bindings: expected exactly \
         one dependency, found {}.\n\
         Inspect it with:\n  \
         cargo tree -p ngnet-qmux --no-default-features -e normal\n\n{tree}",
        dependencies.len(),
    );

    assert!(
        dependency_name(dependencies[0]) == "ngnet-qmux-sys",
        "the QMux core has exactly one dependency, but it is not its bindings: it is `{}`.\n\
         Inspect it with:\n  \
         cargo tree -p ngnet-qmux --no-default-features -e normal\n\n{tree}",
        dependency_name(dependencies[0]),
    );
}

/// The default build -- the asynchronous layer included -- still reaches no runtime.
///
/// The claim that makes the seam a seam. `ngnet-qmux`'s default features compile a layer that
/// owns a byte stream and drives a connection over it, and the caller supplies both the byte
/// stream and the clock; if a runtime appeared in this graph, that would have stopped being
/// true and every caller would be paying for tokio whether they use it or not.
///
/// Asked with default features on, which is the configuration a caller gets by writing
/// `ngnet-qmux = "..."` and the one where feature unification across the workspace could go
/// wrong. The neighbouring claim about the *manifest* -- that tokio is declared optional and
/// gated behind `dep:` -- lives in `crates/ngnet-qmux/tests/invariants.rs`; a dependency
/// arriving transitively would move this one while leaving that one green.
#[test]
fn the_qmux_async_layer_brings_no_runtime() {
    let tree = cargo_tree(&["-p", "ngnet-qmux", "-e", "normal"]);
    let dependencies: Vec<&str> = tree
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(
        dependencies.len(),
        1,
        "the default build of ngnet-qmux resolved to more than its bindings: expected exactly \
         one dependency, found {}. The asynchronous layer was added on the promise that it \
         adds nothing to the dependency graph.\n\
         Inspect it with:\n  cargo tree -p ngnet-qmux -e normal\n\n{tree}",
        dependencies.len(),
    );

    assert!(
        dependency_name(dependencies[0]) == "ngnet-qmux-sys",
        "the default build has exactly one dependency and it is not its bindings: it is \
         `{}`.\n\
         Inspect it with:\n  cargo tree -p ngnet-qmux -e normal\n\n{tree}",
        dependency_name(dependencies[0]),
    );
}

/// And the `tokio` feature is what actually reaches tokio.
///
/// The positive half, and it needs making separately. Every check around it is a negative --
/// no runtime here, none there -- and a version of this crate whose `tokio` feature had been
/// misspelled, or whose optional dependency had been dropped, would satisfy all of them while
/// shipping a feature that enables nothing. A feature that quietly stopped doing anything is
/// the failure mode a list of absences cannot catch.
///
/// Only the *direct* dependencies are checked. tokio brings its own transitive graph, which is
/// tokio's business and not a property of this crate; what matters here is that nothing new
/// arrives alongside it at the top level.
#[test]
fn the_qmux_tokio_feature_is_what_reaches_tokio() {
    let tree = cargo_tree(&["-p", "ngnet-qmux", "--features", "tokio", "-e", "normal"]);

    assert!(
        contains_at_word_boundary(&tree, "tokio"),
        "the `tokio` feature did not bring tokio into ngnet-qmux's graph, so it is a feature \
         that enables nothing.\n\
         Inspect it with:\n  cargo tree -p ngnet-qmux --features tokio -e normal\n\n{tree}",
    );

    // `cargo tree`'s direct dependencies are the lines whose prefix has no continuation
    // character before the branch: nesting indents by four columns per level, so a direct
    // dependency's box-drawing prefix starts at column zero.
    let direct: Vec<String> = tree
        .lines()
        .skip(1)
        .filter(|line| line.starts_with('\u{251c}') || line.starts_with('\u{2514}'))
        .map(dependency_name)
        .collect();

    assert_eq!(
        direct,
        vec!["ngnet-qmux-sys".to_string(), "tokio".to_string()],
        "the `tokio` feature brought something other than tokio into ngnet-qmux's direct \
         dependencies. It is meant to add a ready-made byte stream and clock, and nothing \
         else.\n\
         Inspect it with:\n  cargo tree -p ngnet-qmux --features tokio -e normal\n\n{tree}",
    );
}

/// QMux reaches neither the QUIC family nor the HTTP families, nor any TLS or runtime crate.
///
/// The names invite confusion -- QMux comes from the ngtcp2 authors and reuses QUIC's frame
/// encoding and stream semantics -- but the two share no code here, and should not. QMux runs
/// over a byte stream the caller supplies; it has no UDP, no packets, and no cryptography of
/// its own, and the draft explicitly permits carrying it over an unsecured substrate such as a
/// unix socket. A caller who wants QMux should not be compiling ngtcp2 or OpenSSL to get it.
///
/// `ngnet-qmux-h3` is on the forbidden list rather than exempt from it. The join depends on
/// QMux, not the other way round, and an edge in the reverse direction would drag nghttp3 into
/// every plain QMux build — the exact cost this check exists to prevent, arriving through the
/// one crate that has a plausible-looking reason to be there.
#[test]
fn no_other_protocol_stack_or_tls_reaches_qmux() {
    for crate_name in ["ngnet-qmux", "ngnet-qmux-sys"] {
        let tree = cargo_tree(&["-p", crate_name, "-e", "normal"]);

        for forbidden in [
            "ngnet-quic",
            "ngnet-quic-sys",
            "ngnet-quic-h3",
            "ngnet-qmux-h3",
            "h3-ngnet-qmux",
            "h3-ngnet-quic",
            "ngnet-h2",
            "ngnet-h2-sys",
            "ngnet-h3",
            "ngnet-h3-sys",
            "quinn",
            "rustls",
            "openssl",
            "openssl-sys",
            "tokio",
            "compio",
        ] {
            assert!(
                !contains_at_word_boundary(&tree, forbidden),
                "{forbidden} reached {crate_name}'s normal dependency graph.\n\
                 Find it with:\n  cargo tree -p {crate_name} -e normal -i {forbidden}\n\
                 QMux is a sans-I/O state machine over a caller-supplied byte stream: it needs \
                 no other protocol stack, no TLS, and no runtime.\n\n{tree}",
            );
        }
    }
}

/// Nothing that existed before QMux has picked it up.
///
/// The check above keeps QMux from growing into the rest of the workspace; this one keeps the
/// rest of the workspace from growing into QMux. Both crates are unpublished and expected to
/// churn with the draft, so anything depending on them inherits that churn.
///
/// The two QMux/H3 adapters are deliberately absent from the list, and that absence is the whole point of
/// the list being spelled out crate by crate rather than derived from workspace membership.
/// The join is new, unpublished and expected to churn alongside QMux itself, so it is allowed
/// to take the dependency; every crate that predates QMux is not. Adding a member here is the
/// deliberate act that decides which side of that line a new crate falls on, and
/// the two positive adapter checks above assert each exception is a real join rather than a
/// hole in the check.
#[test]
fn no_existing_crate_reaches_qmux() {
    for crate_name in [
        "ngnet-h2",
        "ngnet-h3",
        "ngnet-quic",
        "ngnet-quic-h3",
        // The hyperium/ngtcp2 adapter belongs on this side of the line, not among the
        // exceptions: it joins hyperium H3 to the *QUIC* transport, so QMux reaching it would
        // mean the two transport families had met.
        "h3-ngnet-quic",
        "ngnet-axum",
        "ngnet-util",
    ] {
        let tree = cargo_tree(&["-p", crate_name, "-e", "normal"]);

        for forbidden in [
            "ngnet-qmux",
            "ngnet-qmux-sys",
            "ngnet-qmux-h3",
            "h3-ngnet-qmux",
        ] {
            assert!(
                !contains_at_word_boundary(&tree, forbidden),
                "{forbidden} reached {crate_name}'s normal dependency graph.\n\
                 Find it with:\n  cargo tree -p {crate_name} -e normal -i {forbidden}\n\
                 The QMux crates are unpublished and track an unratified draft; nothing \
                 established should depend on them yet.\n\n{tree}",
            );
        }
    }
}
