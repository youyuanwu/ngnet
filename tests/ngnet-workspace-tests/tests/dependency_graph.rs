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
