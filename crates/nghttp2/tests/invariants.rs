//! Structural invariants of the crate itself (Spec SC-002, SC-020, SC-021).
//!
//! These assert properties of the source rather than of runtime behaviour: that `unsafe`
//! stays confined, that tests do not need it, and that the crate takes on no dependency
//! or facility that would contradict its sans-I/O claim.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The tests permitted to contain `unsafe`, each with the reason it must.
///
/// Neither is a caller using the HTTP/2 API — that is the property SC-002 protects.
/// Naming them here keeps the rule mechanical rather than a judgement call, and the
/// companion test below fails if one of these stops needing its exemption.
const UNSAFE_TEST_EXEMPTIONS: &[(&str, &str)] = &[
    // SC-007 requires demonstrating that unwrapped capabilities remain reachable, which
    // necessarily means calling a raw binding.
    ("raw_escape_hatch.rs", "calls a raw binding by design"),
    // Implementing `GlobalAlloc` is unsafe by language rule. This is measurement
    // scaffolding for SC-005, not use of this crate's API.
    ("zero_alloc.rs", "implements GlobalAlloc to count allocations"),
];

/// Facilities the crate's own source must not reach for.
///
/// Their absence is what makes the sans-I/O claim structural rather than aspirational:
/// the crate cannot perform I/O, block, sleep or spawn if it never names the means to.
const FORBIDDEN: &[&str] = &[
    "std::net",
    "std::fs",
    "std::thread",
    "std::time",
    "std::process",
    "std::io",
];

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

/// Removes line and block comments, and the contents of string literals.
///
/// Needed because the crate's documentation legitimately *discusses* the facilities it
/// avoids and the `unsafe` it confines; scanning raw text would flag prose. Not a full
/// Rust lexer, but it handles the constructs this crate actually uses.
fn strip_comments_and_strings(source: &str) -> String {
    let bytes: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;

    while i < bytes.len() {
        let rest_is = |pat: &str, i: usize| {
            bytes[i..].iter().copied().take(pat.len()).eq(pat.chars())
        };

        if rest_is("//", i) {
            while i < bytes.len() && bytes[i] != '\n' {
                i += 1;
            }
        } else if rest_is("/*", i) {
            let mut depth = 1;
            i += 2;
            while i < bytes.len() && depth > 0 {
                if rest_is("/*", i) {
                    depth += 1;
                    i += 2;
                } else if rest_is("*/", i) {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if bytes[i] == '"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == '\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str("\"\"");
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

#[test]
fn the_crate_reaches_for_no_io_threading_or_time_facility() {
    // SC-021. Together with the interop tests, which complete a whole exchange without a
    // socket, this is what establishes FR-007.
    let src = crate_root().join("src");
    let files = rust_files(&src);
    assert!(!files.is_empty(), "no source files found under {src:?}");

    let mut offences = Vec::new();

    for file in &files {
        let source = std::fs::read_to_string(file).expect("reading source");
        let code = strip_comments_and_strings(&source);

        for facility in FORBIDDEN {
            if code.contains(facility) {
                offences.push(format!("{}: {facility}", file.display()));
            }
        }
        if code.contains(".await") {
            offences.push(format!("{}: .await", file.display()));
        }
    }

    assert!(
        offences.is_empty(),
        "the crate must not reach for I/O, threading or time facilities:\n{}",
        offences.join("\n")
    );
}

#[test]
fn the_comment_stripper_actually_strips() {
    // Guards the scan above against passing because stripping removed everything, or
    // because it removed nothing and the crate happens to mention nothing.
    let stripped = strip_comments_and_strings(
        "// std::net in a line comment\n/* std::fs in a block */\nlet s = \"std::thread\";\nlet real = std::process::id();",
    );

    assert!(!stripped.contains("std::net"), "line comments must be stripped");
    assert!(!stripped.contains("std::fs"), "block comments must be stripped");
    assert!(!stripped.contains("std::thread"), "string literals must be stripped");
    assert!(
        stripped.contains("std::process"),
        "real code must survive stripping, got: {stripped}"
    );
}

#[test]
fn the_crate_declares_exactly_one_runtime_dependency() {
    // SC-021. A second runtime dependency would need justifying against the crate's
    // promise to be a thin, self-contained layer over the raw bindings.
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("manifest");

    let dependencies: Vec<&str> = manifest
        .split("[dependencies]")
        .nth(1)
        .expect("no [dependencies] section")
        .split('[')
        .next()
        .expect("dependencies section")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();

    assert_eq!(
        dependencies.len(),
        1,
        "expected exactly one runtime dependency, found: {dependencies:?}"
    );
    assert!(
        dependencies[0].starts_with("nghttp2-sys"),
        "the single dependency should be the raw bindings, found: {}",
        dependencies[0]
    );
}

#[test]
fn unsafe_is_confined_to_the_modules_that_wrap_the_bindings() {
    // SC-020. The crate root denies `unsafe_code`, so any module using it must carry an
    // explicit allow. This asserts the allow list has not quietly grown to cover the
    // whole crate.
    let lib = std::fs::read_to_string(crate_root().join("src/lib.rs")).expect("lib.rs");

    assert!(
        lib.contains("#![deny(unsafe_code)]"),
        "the crate root must deny unsafe_code; without it the allows below confine nothing"
    );

    let allowed: BTreeSet<String> = lib
        .lines()
        .map(str::trim)
        .scan(false, |pending, line| {
            let was_pending = *pending;
            if line == "#[allow(unsafe_code)]" {
                *pending = true;
                return Some(None);
            }
            *pending = false;
            if was_pending && line.starts_with("mod ") {
                let name = line.trim_start_matches("mod ").trim_end_matches(';');
                return Some(Some(name.to_string()));
            }
            Some(None)
        })
        .flatten()
        .collect();

    // Modules that legitimately touch the raw bindings. Adding to this list should be a
    // deliberate act, which is exactly why the test names them.
    let expected: BTreeSet<String> = ["callbacks", "error", "options", "session", "state"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();

    assert_eq!(
        allowed, expected,
        "the set of modules permitted to use `unsafe` changed; if that is intended, update \
         this test deliberately"
    );
}

#[test]
fn no_test_needs_unsafe_to_use_the_api() {
    // SC-002. Callers should never have to write `unsafe`; the tests are the proof, so
    // one of them containing it would undercut the claim.
    let tests = crate_root().join("tests");
    let mut offenders = Vec::new();

    for file in rust_files(&tests) {
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        if UNSAFE_TEST_EXEMPTIONS
            .iter()
            .any(|(exempt, _)| *exempt == name)
        {
            continue;
        }

        let code = strip_comments_and_strings(&std::fs::read_to_string(&file).expect("reading"));
        if code.contains("unsafe ") || code.contains("unsafe{") || code.contains("unsafe {") {
            offenders.push(file.display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "these tests use `unsafe`, which the safe API should make unnecessary:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn every_unsafe_exemption_is_still_earned() {
    // The counterpart to the list above: an exemption that is no longer needed is dead
    // weight that quietly widens the rule, so each must still be using `unsafe`.
    for (name, reason) in UNSAFE_TEST_EXEMPTIONS {
        let path = crate_root().join("tests").join(name);
        let code = strip_comments_and_strings(&std::fs::read_to_string(&path).expect("reading"));

        assert!(
            code.contains("unsafe"),
            "{name} no longer uses unsafe ({reason}); drop its exemption"
        );
    }
}
