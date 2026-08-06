//! Structural invariants of the crate itself (Spec SC-019, SC-020, SC-021).
//!
//! These assert properties of the source rather than of runtime behaviour: that `unsafe`
//! stays confined to the modules that declare they need it, that a caller never needs any,
//! that the crate names no facility that would contradict its sans-I/O claim, and that it
//! takes on exactly one dependency.
//!
//! The `unsafe` boundary is enforced twice over, deliberately. `lib.rs` carries
//! `#![deny(unsafe_code)]` with a per-module `#[allow(unsafe_code)]`, so the compiler
//! already rejects a stray `unsafe` elsewhere. What this file adds is the rule about
//! *which* modules may carry the allow, and a check that the list has not quietly grown —
//! which the compiler cannot express, because adding an allow is exactly how it is
//! silenced.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The tests permitted to contain `unsafe`, each with the reason it must.
///
/// None of them is a caller using the HTTP/3 API — that is the property this protects.
/// Naming them keeps the rule mechanical rather than a judgement call, and the companion
/// test below fails if one stops needing its exemption.
const UNSAFE_TEST_EXEMPTIONS: &[(&str, &str)] = &[
    // Implementing `GlobalAlloc` is unsafe by language rule. This is measurement
    // scaffolding for the allocation-free receive claim, not use of this crate's API.
    (
        "zero_alloc.rs",
        "implements GlobalAlloc to count allocations",
    ),
];

/// Facilities the crate's own source must not reach for.
///
/// Their absence is what makes the sans-I/O claim structural rather than aspirational: the
/// crate cannot open a socket, block, sleep or spawn if it never names the means to. The
/// clock is the interesting one — nghttp3 wants a timestamp on every read, and the only
/// way to supply one without reading a clock is to make the caller pass it, which is why
/// [`ngnet_h3::Timestamp`] exists at all.
const FORBIDDEN: &[&str] = &[
    "std::net",
    "std::fs",
    "std::thread",
    "std::time",
    "std::process",
    "std::env",
];

/// The only `std::io` item the crate may name.
///
/// `IoSlice` is a description of borrowed bytes, not a way to move them: it performs no
/// I/O and exists here because a vectored write is what a caller will hand these buffers
/// to. Anything else from `std::io` would be the real thing.
const PERMITTED_STD_IO: &str = "std::io::IoSlice";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {dir:?}: {e}"));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            found.extend(rust_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
    found.sort();
    found
}

/// Removes comments and the contents of string and character literals.
///
/// Needed because the crate's documentation legitimately *discusses* the facilities it
/// avoids and the `unsafe` it confines, so scanning raw text would flag prose. Not a Rust
/// lexer; it handles the constructs this crate actually uses, and
/// [`the_scanner_sees_through_comments_and_literals`] pins that it handles them correctly.
fn strip_comments_and_literals(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;

    let starts = |pat: &str, at: usize| chars[at..].iter().copied().take(pat.len()).eq(pat.chars());

    while i < chars.len() {
        if starts("//", i) {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if starts("/*", i) {
            // Rust's block comments nest, so a depth counter rather than a search for the
            // first `*/`.
            let mut depth = 1;
            i += 2;
            while i < chars.len() && depth > 0 {
                if starts("/*", i) {
                    depth += 1;
                    i += 2;
                } else if starts("*/", i) {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if let Some(len) = raw_string_len(&chars, i, out.chars().next_back()) {
            // Raw strings carry their own quotes and hashes, so the ordinary string scan
            // would stop inside one and let the remainder leak out as if it were code.
            i += len;
            out.push_str("\"\"");
        } else if chars[i] == '"' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' {
                    i += 2;
                    continue;
                }
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str("\"\"");
        } else if let Some(len) = char_literal_len(&chars, i) {
            // A `'` opens either a character literal or a lifetime, and the two must be
            // told apart: blanking `'static` would corrupt ordinary code.
            i += len;
            out.push_str("''");
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// The length, in `char`s, of the raw string literal starting at `start`, if there is one.
///
/// Handles `r"..."` and `r#"..."#` with any number of hashes, plus the `br` byte-string
/// forms. `prev` is the previous character of the *output*, used to reject an `r` that is
/// merely the tail of an identifier — `for`, or a variable named `r` — rather than a prefix.
fn raw_string_len(chars: &[char], start: usize, prev: Option<char>) -> Option<usize> {
    if prev.is_some_and(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let mut at = start;
    if chars.get(at) == Some(&'b') {
        at += 1;
    }
    if chars.get(at) != Some(&'r') {
        return None;
    }
    at += 1;
    let hashes = chars[at..].iter().take_while(|c| **c == '#').count();
    at += hashes;
    if chars.get(at) != Some(&'"') {
        return None;
    }
    at += 1;
    // The terminator is a quote followed by exactly as many hashes as the opener had.
    while at < chars.len() {
        if chars[at] == '"'
            && chars[at + 1..]
                .iter()
                .take(hashes)
                .filter(|c| **c == '#')
                .count()
                == hashes
        {
            return Some(at + 1 + hashes - start);
        }
        at += 1;
    }
    None
}

/// The length, in `char`s, of the character literal starting at `start`, if there is one.
fn char_literal_len(chars: &[char], start: usize) -> Option<usize> {
    if chars.get(start) != Some(&'\'') {
        return None;
    }
    match chars.get(start + 1)? {
        // An escape may be any width -- `'\n'` is four chars, `'\u{7b}'` is eight -- so
        // scan to the closing quote. The scan starts past the escaped character, because
        // that character may itself be a quote.
        '\\' => {
            let mut at = start + 3;
            while at < chars.len() && chars[at] != '\'' {
                at += 1;
            }
            (at < chars.len()).then_some(at + 1 - start)
        }
        // A plain literal is exactly three chars. Requiring the closing quote keeps `'a`
        // and `'static` out.
        _ => (chars.get(start + 2) == Some(&'\'')).then_some(3),
    }
}

/// Whether the stripped source contains `unsafe` as a keyword rather than inside a word.
fn mentions_unsafe(stripped: &str) -> bool {
    stripped
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|word| word == "unsafe")
}

/// The modules `lib.rs` grants an `unsafe` allowance to, read from `lib.rs` itself.
///
/// Derived rather than listed here, so the two cannot disagree. What is asserted about it
/// is the set's *contents*, below.
fn modules_allowed_unsafe() -> BTreeSet<String> {
    let lib = std::fs::read_to_string(crate_root().join("src").join("lib.rs")).expect("lib.rs");
    let mut allowed = BTreeSet::new();
    let mut granted = false;
    for line in lib.lines() {
        let line = line.trim();
        if line.starts_with("#[allow(unsafe_code)]") {
            granted = true;
            continue;
        }
        if granted {
            if let Some(rest) = line.strip_prefix("mod ") {
                allowed.insert(rest.trim_end_matches(';').trim().to_string());
            }
            granted = false;
        }
    }
    allowed
}

#[test]
fn unsafe_lives_only_in_the_modules_that_declare_they_need_it() {
    let allowed = modules_allowed_unsafe();
    assert!(
        !allowed.is_empty(),
        "no module carries #[allow(unsafe_code)]; the boundary has been read wrongly"
    );

    let src = crate_root().join("src");
    let mut offenders = Vec::new();
    let mut carriers = BTreeSet::new();
    for path in rust_files(&src) {
        let module = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("a file stem")
            .to_string();
        if module == "lib" {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        if !mentions_unsafe(&strip_comments_and_literals(&source)) {
            continue;
        }
        carriers.insert(module.clone());
        if !allowed.contains(&module) {
            offenders.push(path);
        }
    }

    assert!(
        offenders.is_empty(),
        "these modules use `unsafe` without being granted it in lib.rs: {offenders:#?}"
    );

    // The other direction: an allowance that nothing uses is a boundary that has drifted
    // wider than the code needs, and the point of the boundary is that it is exactly as
    // wide as it has to be.
    let stale: Vec<&String> = allowed.difference(&carriers).collect();
    assert!(
        stale.is_empty(),
        "these modules are granted `unsafe` but do not use it: {stale:?}"
    );
}

#[test]
fn the_allowance_list_is_the_ffi_boundary_and_nothing_else() {
    // Named explicitly, because the test above only proves the list matches the code —
    // both could grow together. Every module here touches the raw bindings; anything else
    // appearing would mean `unsafe` had leaked into protocol logic.
    let expected: BTreeSet<String> = ["alloc", "callbacks", "conn", "error", "send", "settings"]
        .into_iter()
        .map(String::from)
        .collect();
    assert_eq!(
        modules_allowed_unsafe(),
        expected,
        "the FFI boundary has moved; if that is deliberate, say so here"
    );
}

#[test]
fn a_caller_never_needs_unsafe() {
    // The tests are the crate's own callers, so if none of them needs `unsafe` to drive a
    // whole exchange, neither does anyone else. The exemptions are measurement
    // scaffolding, named individually.
    let exempt: BTreeSet<&str> = UNSAFE_TEST_EXEMPTIONS
        .iter()
        .map(|(name, _)| *name)
        .collect();
    let tests = crate_root().join("tests");
    let mut offenders = Vec::new();
    let mut used_exemptions = BTreeSet::new();

    for path in rust_files(&tests) {
        let name = path.file_name().and_then(|s| s.to_str()).expect("a name");
        let source = std::fs::read_to_string(&path).expect("reading a test file");
        if !mentions_unsafe(&strip_comments_and_literals(&source)) {
            continue;
        }
        if exempt.contains(name) {
            used_exemptions.insert(name.to_string());
        } else {
            offenders.push(name.to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "these tests use `unsafe` to drive the crate, which callers should never have to: \
         {offenders:?}"
    );
    let stale: Vec<&(&str, &str)> = UNSAFE_TEST_EXEMPTIONS
        .iter()
        .filter(|(name, _)| !used_exemptions.contains(*name))
        .collect();
    assert!(
        stale.is_empty(),
        "these exemptions are no longer needed and should be removed: {stale:?}"
    );
}

#[test]
fn the_crate_reaches_for_no_io_threading_or_time_facility() {
    let src = crate_root().join("src");
    let files = rust_files(&src);
    assert!(!files.is_empty(), "no source files found under {src:?}");

    let mut findings = Vec::new();
    for path in &files {
        let source = std::fs::read_to_string(path).expect("reading a source file");
        let stripped = strip_comments_and_literals(&source);
        for facility in FORBIDDEN {
            if stripped.contains(facility) {
                findings.push(format!("{}: {facility}", path.display()));
            }
        }
        // `std::io` is not forbidden outright, because one item in it is a description of
        // borrowed bytes rather than a way to move them.
        let mut rest = stripped.as_str();
        while let Some(at) = rest.find("std::io") {
            let tail = &rest[at..];
            if !tail.starts_with(PERMITTED_STD_IO) {
                findings.push(format!("{}: std::io beyond IoSlice", path.display()));
            }
            rest = &tail[7..];
        }
    }

    assert!(
        findings.is_empty(),
        "the sans-I/O core names facilities it must not: {findings:#?}"
    );
}

#[test]
fn the_crate_has_no_asynchrony_of_its_own() {
    // There is deliberately no async layer here. Its absence is what lets the crate be
    // driven from blocking code, from any runtime, and from a test with no runtime at all.
    let src = crate_root().join("src");
    let mut offenders = Vec::new();
    for path in rust_files(&src) {
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        let stripped = strip_comments_and_literals(&source);
        if stripped.contains("async fn") || stripped.contains("async move") {
            offenders.push(path);
        }
    }
    assert!(
        offenders.is_empty(),
        "these modules introduce asynchrony the crate promises not to have: {offenders:#?}"
    );
}

#[test]
fn the_crate_declares_exactly_one_non_optional_dependency() {
    // Read textually rather than by inspecting the built graph, because the claim is about
    // what this crate asks for, not about what happens to be in the workspace lock file.
    let manifest =
        std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("reading Cargo.toml");

    let mut in_dependencies = false;
    let mut declared = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_dependencies = line == "[dependencies]";
            continue;
        }
        if !in_dependencies || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, rest)) = line.split_once('=') else {
            continue;
        };
        if rest.contains("optional = true") {
            continue;
        }
        // `ngnet-h3-sys.workspace = true` names the crate before the dot; the shorthand is
        // what the workspace uses, so it has to be understood rather than tripped over.
        let name = name.trim().split('.').next().expect("a dependency name");
        declared.push(name.to_string());
    }

    assert_eq!(
        declared,
        vec!["ngnet-h3-sys".to_string()],
        "the crate's dependency list has changed; its whole shape depends on staying at one"
    );

    // No dev-dependencies either: a test-only dependency is still something a contributor
    // has to build, and every test here runs on the standard library alone.
    assert!(
        !manifest.contains("[dev-dependencies]"),
        "the crate has acquired dev-dependencies; test-only needs belong in ngnet-h3-tests"
    );
}

#[test]
fn the_scanner_sees_through_comments_and_literals() {
    // Without this, every scan above could be passing because the stripper eats everything
    // it is given. These are the exact shapes the crate's own sources contain.
    let source = r#"
        // unsafe std::net in a line comment
        /* unsafe std::thread in a block /* nested */ comment */
        /// unsafe std::fs in a doc comment
        let message = "unsafe std::process in a string";
        let quote = "a \" escaped quote then std::time";
        let brace = '{';
        let escaped = '\'';
        fn generic<'a>(value: &'a str) -> &'a str { value }
        let real = 1;
    "#;
    let stripped = strip_comments_and_literals(source);

    assert!(!mentions_unsafe(&stripped), "prose must not read as code");
    // A raw string holding a quote, which is the shape that defeats a naive scan: without
    // raw-string handling the scan would end early and spill the rest out as code. This
    // very file contains one, so the check is not hypothetical.
    let raw = "let held = r#\"unsafe \"quoted\" std::net\"#; let after = 2;";
    let raw_stripped = strip_comments_and_literals(raw);
    assert!(!mentions_unsafe(&raw_stripped), "got: {raw_stripped}");
    assert!(!raw_stripped.contains("std::net"), "got: {raw_stripped}");
    assert!(
        raw_stripped.contains("let after = 2;"),
        "got: {raw_stripped}"
    );
    for facility in FORBIDDEN {
        assert!(
            !stripped.contains(facility),
            "{facility} leaked out of a comment or literal"
        );
    }
    // Real code either side of the literals survives, so the stripper is not simply
    // blanking the file.
    assert!(stripped.contains("let message"));
    assert!(stripped.contains("let real = 1;"));
    assert!(
        stripped.contains("<'a>") && stripped.contains("&'a str"),
        "lifetimes are not character literals and must survive: {stripped}"
    );
}

#[test]
fn the_scanner_would_catch_a_real_violation() {
    // The companion to the test above: proof that the scans fail on genuine code, not only
    // that they pass on prose. A scanner that never fires is indistinguishable from one
    // that is not run.
    let planted = "use std::net::TcpStream;\nfn f() { unsafe { g() } }\nasync fn h() {}\n";
    let stripped = strip_comments_and_literals(planted);

    assert!(mentions_unsafe(&stripped));
    assert!(stripped.contains("std::net"));
    assert!(stripped.contains("async fn"));

    // And that `unsafe` is matched as a word, so an identifier containing it is not a
    // false positive.
    assert!(!mentions_unsafe(&strip_comments_and_literals(
        "fn unsafely() {}"
    )));
    assert!(!mentions_unsafe(&strip_comments_and_literals(
        "let unsafe_count = 1;"
    )));
}
