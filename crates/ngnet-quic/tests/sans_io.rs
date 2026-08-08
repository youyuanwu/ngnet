//! An early guard on the sans-I/O property.
//!
//! The full structural suite arrives later, but one of its claims is worth asserting from
//! the moment there is source to assert it about: **the crate names no I/O facility**.
//!
//! The reason to bring this one forward is specific. Addresses are unavoidable in a
//! transport library, and the obvious way to spell them is `std::net::SocketAddr` — which
//! would fail the scanner the moment it was written, after the type had spread through
//! every signature in the crate. `core::net` provides the same types with no I/O attached.
//! Catching the difference now costs one test; catching it later costs a refactor.

use std::path::{Path, PathBuf};

/// Facilities a sans-I/O crate must not name.
///
/// `std::net` is the interesting one here — see the module comment. The rest are included
/// because the same argument applies to them and the scanner may as well cover them from
/// the start.
const FORBIDDEN: &[&str] = &[
    "std::net",
    "std::fs",
    "std::thread",
    "std::time",
    "std::process",
    "std::env",
];

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every `.rs` file under a directory, recursively.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(rust_files(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            found.push(path);
        }
    }
    found
}

/// Removes comments and string literals, so prose about a forbidden name is not mistaken
/// for a use of it.
///
/// This crate's own documentation discusses `std::net` at length, so without this the scan
/// would fail on the comment explaining why the scan exists.
fn strip_comments_and_literals(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
                out.push(c);
            }
            continue;
        }
        if in_block_comment {
            if c == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_string {
            if c == '\\' {
                chars.next();
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
                in_block_comment = true;
            }
            '"' => in_string = true,
            _ => out.push(c),
        }
    }
    out
}

#[test]
fn the_crate_reaches_for_no_io_threading_or_time_facility() {
    let src = crate_root().join("src");
    let mut offenders = Vec::new();

    for path in rust_files(&src) {
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        let code = strip_comments_and_literals(&source);
        for forbidden in FORBIDDEN {
            if code.contains(forbidden) {
                offenders.push(format!("{} names {forbidden}", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "the sans-I/O core reached for something it must not: {offenders:#?}"
    );
}

#[test]
fn the_scanner_would_catch_a_real_violation() {
    // A scanner that silently stopped matching would turn the test above into a claim about
    // an empty set. This proves it still bites.
    let violating = "fn f() { let _ = std::net::SocketAddr::V4; }";
    let code = strip_comments_and_literals(violating);
    assert!(code.contains("std::net"));
}

#[test]
fn the_scanner_sees_through_comments_and_literals() {
    // And this proves it does not bite on prose, which is what lets the crate document the
    // very thing it forbids.
    let prose = r#"
        //! This module explains why std::net is not used.
        /* std::thread would also be wrong. */
        fn f() { let _ = "std::fs"; }
    "#;
    let code = strip_comments_and_literals(prose);
    assert!(!code.contains("std::net"));
    assert!(!code.contains("std::thread"));
    assert!(!code.contains("std::fs"));
}

#[test]
fn the_scan_actually_sees_files() {
    // A path filter that stopped matching would make every claim above vacuous.
    let files = rust_files(&crate_root().join("src"));
    assert!(
        files.len() >= 5,
        "the scan found only {} files, which suggests it stopped matching",
        files.len()
    );
}
