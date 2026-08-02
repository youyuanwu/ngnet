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
    (
        "zero_alloc.rs",
        "implements GlobalAlloc to count allocations",
    ),
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
        let rest_is =
            |pat: &str, i: usize| bytes[i..].iter().copied().take(pat.len()).eq(pat.chars());

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

/// Whether a source file belongs to the feature-gated async subtree.
///
/// That subtree is the one part of the crate permitted I/O-adjacent facilities and the
/// `async` keyword; everything else is the sans-I/O core and is held to the original rule.
///
/// The match is on the exact location, `src/http/`, not on any path component that
/// happens to be named `http`. A component-wise match would silently exempt a future core
/// module at, say, `src/protocol/http/`, turning these scans into a partial no-op for
/// exactly the code they exist to police.
fn is_async_subtree(path: &Path) -> bool {
    path.starts_with(crate_root().join("src").join("http"))
}

#[test]
fn the_sans_io_core_reaches_for_no_io_threading_or_time_facility() {
    // SC-021 and SC-027. Together with the interop tests, which complete a whole exchange
    // without a socket, this is what establishes FR-007.
    //
    // The async subtree is excluded, because an async transport layer must name the very
    // facilities the core forbids. What is *not* relaxed is the core itself: the scan
    // still runs over every other file, and the companion test below proves it would
    // still fail for them.
    let src = crate_root().join("src");
    let files: Vec<PathBuf> = rust_files(&src)
        .into_iter()
        .filter(|path| !is_async_subtree(path))
        .collect();
    assert!(!files.is_empty(), "no source files found under {src:?}");
    assert!(
        files.iter().any(|f| f.ends_with("session.rs")),
        "the scan must still cover the core; it no longer does"
    );

    let mut offences = Vec::new();

    for file in &files {
        let source = std::fs::read_to_string(file).expect("reading source");
        let code = strip_comments_and_strings(&source);

        for facility in FORBIDDEN {
            if code.contains(facility) {
                offences.push(format!("{}: {facility}", file.display()));
            }
        }
        // Bare `async` and `await` as keywords, not only the `.await` postfix form.
        for keyword in ["async", "await"] {
            if uses_keyword(&code, keyword) {
                offences.push(format!("{}: {keyword}", file.display()));
            }
        }
    }

    assert!(
        offences.is_empty(),
        "the sans-I/O core must not reach for I/O, threading or time facilities:\n{}",
        offences.join("\n")
    );
}

#[test]
fn the_facility_scan_would_still_catch_the_core() {
    // The re-scoping above is only honest if the scan still fails for the code it was
    // written to protect. Excluding a subtree by path is exactly the kind of change that
    // can quietly turn a test into a no-op, so the detector is exercised directly on
    // samples standing in for core modules.
    let core_sample = "use std::net::TcpStream;";
    assert!(
        FORBIDDEN
            .iter()
            .any(|facility| strip_comments_and_strings(core_sample).contains(facility)),
        "the scan must still detect a socket in a core module"
    );
    assert!(
        uses_keyword(&strip_comments_and_strings("async fn drive() {}"), "async"),
        "the scan must still detect async in a core module"
    );

    // And the exclusion must be narrow: the async subtree at its exact location, and
    // nothing else. In particular a core module nested under a directory that happens to
    // be named `http` must still be scanned — a component-wise match would exempt it, and
    // the resulting hole would be invisible.
    let src = crate_root().join("src");
    assert!(is_async_subtree(&src.join("http/transport.rs")));
    assert!(is_async_subtree(&src.join("http/mod.rs")));
    assert!(!is_async_subtree(&src.join("session.rs")));
    assert!(!is_async_subtree(&src.join("header.rs")));
    assert!(!is_async_subtree(&src.join("lib.rs")));
    assert!(
        !is_async_subtree(&src.join("protocol/http/probe.rs")),
        "a core module merely nested under a directory named `http` must still be scanned"
    );
    assert!(
        !is_async_subtree(&src.join("codec/http/mod.rs")),
        "the exemption is a location, not a name"
    );
    assert!(
        !is_async_subtree(&src.join("http_helpers.rs")),
        "a sibling sharing a prefix is not part of the subtree"
    );
}

#[test]
fn no_async_facility_escapes_the_subtree() {
    // SC-027. The counterpart to the exclusion: async code is permitted in `src/http/`
    // and nowhere else, so containment is structural rather than a matter of habit.
    let mut offences = Vec::new();

    for file in rust_files(&crate_root().join("src")) {
        if is_async_subtree(&file) {
            continue;
        }
        let code = strip_comments_and_strings(&std::fs::read_to_string(&file).expect("reading"));
        for keyword in ["async", "await"] {
            if uses_keyword(&code, keyword) {
                offences.push(format!("{}: {keyword}", file.display()));
            }
        }
    }

    assert!(
        offences.is_empty(),
        "async facilities must stay inside the http subtree:\n{}",
        offences.join("\n")
    );
}

/// Whether `code` uses `unsafe` as a keyword rather than merely containing the letters.
///
/// A substring search is not enough: an identifier such as `uses_unsafe` contains the
/// text but is not a use of the keyword, and mistaking one for the other would make this
/// file fail its own check.
fn uses_unsafe_keyword(code: &str) -> bool {
    uses_keyword(code, "unsafe")
}

/// Whether `code` uses `keyword` as a token rather than merely containing the letters.
fn uses_keyword(code: &str, keyword: &str) -> bool {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';

    code.match_indices(keyword).any(|(index, _)| {
        let before_ok = index == 0 || !code[..index].chars().next_back().is_some_and(is_ident);
        let after = &code[index + keyword.len()..];
        let after_ok = !after.chars().next().is_some_and(is_ident);
        before_ok && after_ok
    })
}

#[test]
fn the_unsafe_keyword_detector_distinguishes_identifiers() {
    assert!(uses_unsafe_keyword("let x = unsafe { 1 };"));
    assert!(uses_unsafe_keyword("unsafe impl Send for T {}"));
    assert!(uses_unsafe_keyword("unsafe extern \"C\" {}"));
    assert!(!uses_unsafe_keyword("let uses_unsafe = true;"));
    assert!(!uses_unsafe_keyword("fn check_unsafely() {}"));
    assert!(!uses_unsafe_keyword("UNSAFE_TEST_EXEMPTIONS"));

    // The same rule is what makes the async/await scan meaningful.
    assert!(uses_keyword("async fn f() {}", "async"));
    assert!(uses_keyword("x.await;", "await"));
    assert!(!uses_keyword("let asynchronous = 1;", "async"));
    assert!(!uses_keyword("fn awaited() {}", "await"));
}

#[test]
fn the_comment_stripper_actually_strips() {
    // Guards the scan above against passing because stripping removed everything, or
    // because it removed nothing and the crate happens to mention nothing.
    let stripped = strip_comments_and_strings(
        "// std::net in a line comment\n/* std::fs in a block */\nlet s = \"std::thread\";\nlet real = std::process::id();",
    );

    assert!(
        !stripped.contains("std::net"),
        "line comments must be stripped"
    );
    assert!(
        !stripped.contains("std::fs"),
        "block comments must be stripped"
    );
    assert!(
        !stripped.contains("std::thread"),
        "string literals must be stripped"
    );
    assert!(
        stripped.contains("std::process"),
        "real code must survive stripping, got: {stripped}"
    );
}

#[test]
fn the_crate_declares_exactly_one_non_optional_dependency() {
    // SC-020. The property worth protecting is that a default sans-I/O build pulls in
    // nothing but the raw bindings. Optional dependencies do not compromise that — they
    // are absent unless a feature asks for them — so they are permitted, but only if a
    // feature actually gates them. An optional dependency named by no feature would be
    // dead weight nobody can enable, and an ungated one would not be optional at all.
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("manifest");

    let dependencies: Vec<&str> = manifest
        .split("[dependencies]")
        .nth(1)
        .expect("no [dependencies] section")
        .split("\n[")
        .next()
        .expect("dependencies section")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();

    let (optional, required): (Vec<&str>, Vec<&str>) = dependencies
        .iter()
        .partition(|line| line.contains("optional = true"));

    assert_eq!(
        required.len(),
        1,
        "expected exactly one non-optional dependency, found: {required:?}"
    );
    assert!(
        required[0].starts_with("nghttp2-sys"),
        "the single required dependency should be the raw bindings, found: {}",
        required[0]
    );

    // Every optional dependency must be reachable through a declared feature.
    let features = manifest
        .split("[features]")
        .nth(1)
        .expect("no [features] section")
        .split("\n[")
        .next()
        .expect("features section");

    for line in optional {
        let name = line
            .split(['=', ' '])
            .next()
            .expect("dependency name")
            .trim();
        assert!(
            features.contains(&format!("dep:{name}")),
            "optional dependency `{name}` is not enabled by any feature; either gate it \
             or drop it"
        );
    }
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

    // Other attributes may sit between the allow and the module it applies to — the
    // `#[path = "..."]` on the allocator module does exactly that — so intervening
    // attribute lines are skipped rather than resetting the search.
    let mut allowed: BTreeSet<String> = BTreeSet::new();
    let mut pending = false;
    for line in lib.lines().map(str::trim) {
        if line == "#[allow(unsafe_code)]" {
            pending = true;
            continue;
        }
        if !pending {
            continue;
        }
        if line.starts_with("#[") || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("mod ") {
            allowed.insert(rest.trim_end_matches(';').to_string());
        }
        pending = false;
    }

    // Modules that legitimately touch the raw bindings. Adding to this list should be a
    // deliberate act, which is exactly why the test names them.
    let expected: BTreeSet<String> = [
        "alloc_state",
        "callbacks",
        "error",
        "options",
        "session",
        "state",
    ]
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
fn every_module_using_unsafe_is_in_the_pinned_set() {
    // The counterpart to the pinned list: derive the set of modules that actually contain
    // `unsafe` from the source itself, so a module cannot use it while escaping the list
    // through a parsing quirk.
    let lib = std::fs::read_to_string(crate_root().join("src/lib.rs")).expect("lib.rs");
    let mut declared_allowed = BTreeSet::new();
    let mut pending = false;
    for line in lib.lines().map(str::trim) {
        if line == "#[allow(unsafe_code)]" {
            pending = true;
            continue;
        }
        if !pending || line.starts_with("#[") || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("mod ") {
            declared_allowed.insert(rest.trim_end_matches(';').to_string());
        }
        pending = false;
    }

    // `alloc.rs` is mounted as `alloc_state`, so map file stems onto module names.
    let module_name = |stem: &str| match stem {
        "alloc" => "alloc_state".to_string(),
        other => other.to_string(),
    };

    for file in rust_files(&crate_root().join("src")) {
        let stem = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if stem == "lib" {
            continue;
        }
        let code = strip_comments_and_strings(&std::fs::read_to_string(&file).expect("reading"));
        if uses_unsafe_keyword(&code) {
            assert!(
                declared_allowed.contains(&module_name(stem)),
                "{} uses unsafe but is not in the pinned allow list ({declared_allowed:?})",
                file.display()
            );
        }
    }
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
        if uses_unsafe_keyword(&code) {
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
            uses_unsafe_keyword(&code),
            "{name} no longer uses unsafe ({reason}); drop its exemption"
        );
    }
}

#[test]
fn the_async_subtree_contains_no_unsafe_at_all() {
    // SC-021. The sans-I/O core confines `unsafe` to the modules that wrap the raw
    // bindings; the async layer is held to a stricter rule, because it wraps the safe API
    // rather than the bindings and has no reason to reach past it. Needing `unsafe` here
    // would mean the safe layer is missing something — which is a signal worth failing a
    // build for, rather than a licence to write it.
    let mut offenders = Vec::new();

    for file in rust_files(&crate_root().join("src")) {
        if !is_async_subtree(&file) {
            continue;
        }
        let code = strip_comments_and_strings(&std::fs::read_to_string(&file).expect("reading"));
        if uses_unsafe_keyword(&code) {
            offenders.push(file.display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "the async layer must need no `unsafe`; if one of these genuinely does, the safe \
         layer is missing a capability and that is what should change:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_async_subtree_exists_and_is_scanned() {
    // Guards every assertion above that filters on the subtree: if the directory moved or
    // were renamed, those tests would pass by scanning nothing at all.
    let files: Vec<_> = rust_files(&crate_root().join("src"))
        .into_iter()
        .filter(|path| is_async_subtree(path))
        .collect();

    assert!(
        !files.is_empty(),
        "no files found in the async subtree; the path filter has gone stale"
    );
    // Named by module rather than by file, since a module grows into a directory the
    // moment it gains a submodule — as `transport` did when it acquired the tokio one.
    assert!(
        files
            .iter()
            .any(|f| f.ends_with("transport.rs") || f.ends_with("transport/mod.rs")),
        "the transport module should be part of the async subtree, found: {files:?}"
    );
    assert!(
        files.iter().any(|f| f.ends_with("driver.rs")),
        "the driver should be part of the async subtree, found: {files:?}"
    );
}

#[test]
fn the_send_path_has_nowhere_to_put_a_second_chunk() {
    // SC-018, the half that no runtime assertion can cover. The hook a test reads reports
    // what the bridge *did* hold; this reports what it *could* hold. A container added to
    // the send path would be free to fill up under exactly the conditions a test is least
    // likely to reproduce — a fast producer against a blocked window — so the absence of
    // one is checked here rather than left to review.
    //
    // The single retained chunk lives in an `Option`, which cannot hold two by
    // construction. Anything that can is named below.
    let path = crate_root()
        .join("src")
        .join("http")
        .join("body")
        .join("outgoing.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {path:?}: {e}; the send path bridge has moved"));
    let code = strip_comments_and_strings(&source);

    assert!(
        code.contains("leftover: Option<"),
        "the send path no longer holds its one chunk in an Option; this scan is stale",
    );

    for container in [
        "Vec<", "VecDeque", "BTreeMap", "HashMap", "BTreeSet", "HashSet", "BytesMut", "Box<[",
        "; 2]",
    ] {
        assert!(
            !code.contains(container),
            "the send path gained a `{container}`, which can hold more than one chunk",
        );
    }
}
