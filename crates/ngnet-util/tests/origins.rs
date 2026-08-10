//! Origin derivation: the normalisation that decides what shares a connection.
//!
//! These are unit-level and deliberately so. Every case here has an end-to-end counterpart in
//! `reuse.rs` that counts accepts at a real server, because "these two URIs produce equal
//! `Origin`s" is only interesting if it also means "these two requests share a socket". What
//! these add is the ability to say *which* rule broke when the end-to-end test fails.

use ngnet_util::{ErrorKind, Origin};

fn origin(uri: &str) -> Origin {
    Origin::from_uri(&uri.parse().expect("test URI parses")).expect("origin derives")
}

fn failure(uri: &str) -> ErrorKind {
    Origin::from_uri(&uri.parse().expect("test URI parses"))
        .expect_err("origin should not derive")
        .kind()
}

#[test]
fn host_case_is_ignored() {
    // `Uri` preserves the case it was given, so without normalisation these are two origins
    // and two connections to one server.
    assert_eq!(origin("http://example.com/"), origin("http://EXAMPLE.com/"));
    assert_eq!(origin("http://Example.COM/"), origin("http://example.com/"));
}

#[test]
fn an_omitted_port_is_the_default_port() {
    // `Uri::port` returns `None` for the first and `Some(80)` for the second.
    assert_eq!(origin("http://example.com/"), origin("http://example.com:80/"));
    assert_eq!(origin("http://example.com/").port(), 80);
}

#[test]
fn a_non_default_port_is_a_different_origin() {
    assert_ne!(
        origin("http://example.com/"),
        origin("http://example.com:8080/")
    );
}

#[test]
fn different_hosts_are_different_origins() {
    assert_ne!(origin("http://a.example/"), origin("http://b.example/"));
}

#[test]
fn a_path_does_not_affect_the_origin() {
    assert_eq!(
        origin("http://example.com/one?a=1"),
        origin("http://example.com/two#b")
    );
}

#[test]
fn an_ipv6_host_loses_its_brackets() {
    // `Uri::host` hands back `[::1]`. No resolver accepts that, so leaving the brackets on
    // makes every IPv6 origin fail as though the host were unreachable — a bug that looks
    // like a network problem, which is why it survives a long time.
    assert_eq!(origin("http://[::1]:8080/").host(), "::1");
}

#[test]
fn one_ipv6_address_written_three_ways_is_one_origin() {
    // The case lower-casing alone does not cover: all three are already lower-case and all
    // three are the same address. Comparing their text gives three origins and three
    // connections to one server.
    let compressed = origin("http://[::1]:8080/");
    let medium = origin("http://[0:0:0:0:0:0:0:1]:8080/");
    let expanded = origin("http://[0000:0000:0000:0000:0000:0000:0000:0001]:8080/");

    assert_eq!(compressed, medium);
    assert_eq!(compressed, expanded);
    assert_eq!(compressed.host(), "::1");
}

#[test]
fn ipv4_literals_are_canonicalised_too() {
    assert_eq!(origin("http://127.0.0.1:8080/").host(), "127.0.0.1");
    assert_eq!(
        origin("http://127.0.0.1:8080/"),
        origin("http://127.0.0.1:8080/")
    );
}

#[test]
fn a_trailing_dot_is_preserved() {
    // Not an oversight. `example.com.` is fully qualified; `example.com` is subject to the
    // resolver's search list. They can name different servers, so collapsing them would be
    // wrong exactly when it mattered.
    assert_ne!(origin("http://example.com./"), origin("http://example.com/"));
    assert_eq!(origin("http://example.com./").host(), "example.com.");
}

#[test]
fn a_missing_scheme_is_a_uri_error() {
    assert_eq!(failure("//example.com/"), ErrorKind::Uri);
    assert_eq!(failure("/just/a/path"), ErrorKind::Uri);
}

#[test]
fn an_empty_host_is_a_uri_error() {
    // `http://:80/` parses, and `Uri::host` returns `Some("")` for it rather than `None` —
    // which is why the crate checks emptiness rather than trusting the `Option`. Accepting it
    // would produce an origin with no host, which resolves to nothing and would then be
    // reported as a *connect* failure: a malformed URI presented to the caller as a network
    // problem.
    assert_eq!(failure("http://:80/"), ErrorKind::Uri);
}

#[test]
fn https_is_refused_rather_than_downgraded() {
    // The stack is cleartext-only. Serving this in cleartext would be a silent downgrade,
    // which is worse than a clear refusal.
    assert_eq!(failure("https://example.com/"), ErrorKind::Uri);
}

#[test]
fn other_schemes_are_refused() {
    assert_eq!(failure("ftp://example.com/"), ErrorKind::Uri);
    assert_eq!(failure("ws://example.com/"), ErrorKind::Uri);
}

#[test]
fn uri_errors_are_never_retriable() {
    let error =
        Origin::from_uri(&"https://example.com/".parse().expect("parses")).expect_err("refused");
    assert!(
        !error.is_retriable(),
        "a URI this client cannot serve would fail identically every time"
    );
}

#[test]
fn display_restores_brackets_for_ipv6() {
    // For humans and error messages, where `[::1]:8080` is unambiguous and `::1:8080` is not.
    assert_eq!(origin("http://[::1]:8080/").to_string(), "[::1]:8080");
    assert_eq!(
        origin("http://example.com/").to_string(),
        "example.com:80"
    );
}
