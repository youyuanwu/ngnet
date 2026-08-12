//! Structural claims about this crate, read from its own source.
//!
//! Every other wrapper in this workspace carries a suite like this, and each pins the things
//! a reviewer would otherwise have to remember. These are cheap to keep true and expensive to
//! rediscover.

use std::path::{Path, PathBuf};

fn source_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Every `.rs` file under `src`, with its path and contents.
fn sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, into: &mut Vec<(PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).expect("reading the source directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                walk(&path, into);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("reading a source file");
                into.push((path, text));
            }
        }
    }
    let mut found = Vec::new();
    walk(&source_dir(), &mut found);
    assert!(
        found.len() >= 5,
        "the scan found {} files, which is too few to be scanning anything real",
        found.len()
    );
    found
}

/// Strips comments and string literals, so a scan sees code rather than prose.
///
/// Without this every claim below is defeated by mentioning the forbidden thing in a comment
/// explaining why it is forbidden — which this crate's documentation does repeatedly.
fn code_only(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while let Some(c) = chars.next() {
        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
                out.push('\n');
            }
            continue;
        }
        if in_block_comment > 0 {
            if c == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment -= 1;
            } else if c == '/' && chars.peek() == Some(&'*') {
                chars.next();
                in_block_comment += 1;
            }
            continue;
        }
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '/' if chars.peek() == Some(&'/') => {
                chars.next();
                in_line_comment = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                in_block_comment = 1;
            }
            '"' => in_string = true,
            other => out.push(other),
        }
    }
    out
}

/// Whether the code names a word, rather than merely containing it as a substring.
fn names(code: &str, word: &str) -> bool {
    code.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|token| token == word)
}

/// This crate contains no `unsafe`.
///
/// It joins two safe APIs and has no foreign boundary of its own, which is why — unlike both
/// crates it depends on — it needs no module allowance list. An `unsafe` block appearing here
/// would mean something belongs in one of those crates instead, and this is the check that
/// says so rather than a reviewer noticing.
#[test]
fn nothing_here_is_unsafe() {
    for (path, text) in sources() {
        let code = code_only(&text);
        assert!(
            !names(&code, "unsafe"),
            "{} contains `unsafe`. This crate has no foreign boundary; whatever needs it \
             belongs in ngnet-quic or ngnet-h3, behind their allowance lists.",
            path.display()
        );
    }
}

/// This crate owns no socket, no runtime and no threads.
///
/// Everything it does happens on the caller's task, driven by the HTTP/3 layer polling it.
/// The endpoint owns the socket and the clock; spawning anything here would put work
/// somewhere the caller cannot see or cancel.
#[test]
fn nothing_here_owns_a_socket_or_a_thread() {
    for (path, text) in sources() {
        let code = code_only(&text);
        for forbidden in ["thread", "process", "UdpSocket", "TcpStream"] {
            assert!(
                !names(&code, forbidden),
                "{} names `{forbidden}`. This crate runs on the caller's task and reaches \
                 the network only through the connection it was handed.",
                path.display()
            );
        }
    }
}

/// Module files are flat.
///
/// `src/foo.rs`, never `src/foo/mod.rs`. The same rule the sibling crates keep: a nested
/// module file makes a source tree where the interesting file and the file that merely
/// declares it have the same name in different directories.
#[test]
fn module_files_are_flat() {
    for (path, _) in sources() {
        assert_ne!(
            path.file_name().and_then(|n| n.to_str()),
            Some("mod.rs"),
            "{} is a nested module file; this crate keeps module files flat",
            path.display()
        );
    }
}

/// Nothing is smuggled in through `include_str!`.
///
/// A claim about what the source contains is worth nothing if arbitrary text can be pulled
/// in from elsewhere at compile time.
#[test]
fn nothing_is_included_from_outside() {
    for (path, text) in sources() {
        let code = code_only(&text);
        assert!(
            !names(&code, "include_str") && !names(&code, "include"),
            "{} includes external content, which would defeat every claim in this file",
            path.display()
        );
    }
}

/// The scanner catches a real violation.
///
/// A structural suite that cannot fail is decoration. These prove the machinery works before
/// the claims above are trusted.
#[test]
fn the_scanner_sees_code() {
    assert!(names(&code_only("let x = unsafe { 1 };"), "unsafe"));
    assert!(names(&code_only("std::thread::spawn(f);"), "thread"));
}

/// The scanner ignores comments and string literals.
#[test]
fn the_scanner_ignores_prose() {
    assert!(!names(&code_only("// this crate contains no unsafe code"), "unsafe"));
    assert!(!names(&code_only("/* unsafe */ let x = 1;"), "unsafe"));
    assert!(!names(&code_only("let s = \"unsafe\";"), "unsafe"));
    // A word appearing inside a longer identifier is not that word.
    assert!(!names(&code_only("let unsafely = 1;"), "unsafe"));
}

/// The manifest declares exactly the dependencies this crate is supposed to have.
///
/// The point of the crate is that it is the *only* place the two families meet. A fourth
/// dependency appearing here is not necessarily wrong, but it should be a decision rather
/// than a drift.
#[test]
fn the_manifest_declares_what_it_should() {
    let manifest = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .expect("reading the manifest");

    let deps = manifest
        .split_once("[dependencies]")
        .map(|(_, rest)| rest)
        .expect("a dependencies section");

    let declared: Vec<&str> = deps
        .lines()
        .filter_map(|line| line.split_once('.').or_else(|| line.split_once(" =")))
        .map(|(name, _)| name.trim())
        .filter(|name| !name.is_empty() && !name.starts_with('#') && !name.starts_with('['))
        .collect();

    assert_eq!(
        declared,
        vec!["ngnet-h3", "ngnet-quic", "bytes"],
        "this crate's dependencies changed. It exists to join two families and should need \
         little else; if this is deliberate, update the expectation and say why."
    );

    assert!(
        manifest.contains("publish = false"),
        "this crate cannot be published while either crate it binds is unpublished"
    );
}
