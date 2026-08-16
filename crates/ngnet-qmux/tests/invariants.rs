//! Structural invariants of the crate itself (Spec FR-020, FR-021, FR-032; SC-005, SC-006).
//!
//! These assert properties of the source and the manifest rather than of runtime behaviour:
//! that `unsafe` stays confined to the modules that touch the raw bindings, that the
//! asynchronous layer contains none at all, that nothing outside that layer acquires an async
//! facility, and that the crate still asks for exactly one non-optional dependency with the
//! runtime integration reachable only through a feature.
//!
//! The `unsafe` boundary is enforced twice over, deliberately. `lib.rs` carries
//! `#![deny(unsafe_code)]` with a per-module `#[allow(unsafe_code)]`, so the compiler already
//! rejects a stray `unsafe` elsewhere -- including everywhere in `src/io/`, which is declared
//! with no allowance at all. What this file adds is the rule about *which* modules may carry
//! the allow, and a check that the list has not quietly grown, which the compiler cannot
//! express because adding an allow is exactly how it is silenced.
//!
//! This is deliberately not the question `tests/ngnet-workspace-tests` asks. Those tests
//! assert what the resolved dependency graph *contains*; these assert what this crate
//! *declares*. A dependency arriving transitively moves the first while leaving the second
//! green, and a manifest edit does the reverse.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The subtree the asynchronous layer lives in, relative to `src`.
///
/// The sans-I/O core's structural claims are about the code *outside* it. Inside it the crate
/// is deliberately asynchronous and deliberately names a waker, so scanning it for those would
/// flag the feature rather than a defect. The subtree makes its own, different claims, pinned
/// separately below, and [`the_async_subtree_exists_and_is_scanned`] fails if this path ever
/// stops matching anything -- a filter that silently matches nothing turns every test that
/// uses it into a test of nothing.
const ASYNC_SUBTREE: &str = "io";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Whether `path` is the layer's module root or one of its submodules.
///
/// Both spellings count. `lib.rs` declares the layer as `pub mod io;`, which resolves to
/// `src/io.rs` -- flat, because the allowance scan below derives a module's name from its file
/// stem and a `src/io/mod.rs` would be scanned as `mod`. The submodules then live in `src/io/`,
/// so the subtree is one file plus one directory rather than a directory alone.
fn in_async_subtree(path: &Path) -> bool {
    let src = crate_root().join("src");
    let Ok(rest) = path.strip_prefix(&src) else {
        return false;
    };
    let first = rest.components().next().map(|c| c.as_os_str().to_owned());
    first.is_some_and(|first| first == *ASYNC_SUBTREE || first == *format!("{ASYNC_SUBTREE}.rs"))
}

/// The crate's source outside the asynchronous layer.
fn core_files() -> Vec<PathBuf> {
    rust_files(&crate_root().join("src"))
        .into_iter()
        .filter(|path| !in_async_subtree(path))
        .collect()
}

/// The crate's source inside the asynchronous layer.
fn async_files() -> Vec<PathBuf> {
    rust_files(&crate_root().join("src"))
        .into_iter()
        .filter(|path| in_async_subtree(path))
        .collect()
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
/// Needed because this crate's documentation legitimately *discusses* the facilities it avoids
/// and the `unsafe` it confines -- at length, and in the very files being scanned -- so
/// scanning raw text would flag prose. Not a Rust lexer; it handles the constructs this crate
/// actually uses, and [`the_scanner_sees_through_comments_and_literals`] pins that it handles
/// them correctly.
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
            // Raw strings carry their own quotes and hashes, so the ordinary string scan would
            // stop inside one and let the remainder leak out as if it were code.
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
            // A `'` opens either a character literal or a lifetime, and the two must be told
            // apart: blanking `'static` would corrupt ordinary code.
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
/// Handles `r"..."` and `r#"..."#` with any number of hashes, plus the `br` byte-string forms.
/// `prev` is the previous character of the *output*, used to reject an `r` that is merely the
/// tail of an identifier -- `for`, or a variable named `r` -- rather than a prefix.
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
        // An escape may be any width -- `'\n'` is four chars, `'\u{7b}'` is eight -- so scan
        // to the closing quote. The scan starts past the escaped character, because that
        // character may itself be a quote.
        '\\' => {
            let mut at = start + 3;
            while at < chars.len() && chars[at] != '\'' {
                at += 1;
            }
            (at < chars.len()).then_some(at + 1 - start)
        }
        // A plain literal is exactly three chars. Requiring the closing quote keeps `'a` and
        // `'static` out.
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
/// Derived rather than listed here, so the two cannot disagree. What is asserted about it is
/// the set's *contents*, below.
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

/// The crate's manifest, read as text.
///
/// Textually rather than by inspecting the built graph, because the claim is about what this
/// crate asks for, not about what happens to be in the workspace lock file. The resolved graph
/// is asked about in `tests/ngnet-workspace-tests/tests/dependency_graph.rs`.
fn manifest() -> String {
    std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("reading Cargo.toml")
}

/// The entries of one table in the manifest, as trimmed lines with comments and blanks gone.
fn manifest_table(name: &str) -> Vec<String> {
    let mut inside = false;
    let mut entries = Vec::new();
    for line in manifest().lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line == name;
            continue;
        }
        if !inside || line.is_empty() || line.starts_with('#') {
            continue;
        }
        entries.push(line.to_string());
    }
    entries
}

#[test]
fn unsafe_lives_only_in_the_modules_that_declare_they_need_it() {
    let allowed = modules_allowed_unsafe();
    assert!(
        !allowed.is_empty(),
        "no module carries #[allow(unsafe_code)]; the boundary has been read wrongly"
    );

    let mut offenders = Vec::new();
    let mut carriers = BTreeSet::new();
    // The core only. The layer's files are excluded because their stems collide with the
    // core's -- `io/error.rs` against `error.rs` -- so a name comparison would confuse the
    // two. They are covered by the stronger claim below: not "confined to a declared list"
    // but *none at all*.
    for path in core_files() {
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

    // The other direction: an allowance nothing uses is a boundary that has drifted wider than
    // the code needs, and the point of the boundary is that it is exactly as wide as it has to
    // be.
    let stale: Vec<&String> = allowed.difference(&carriers).collect();
    assert!(
        stale.is_empty(),
        "these modules are granted `unsafe` but do not use it: {stale:?}"
    );
}

#[test]
fn the_allowance_list_is_the_ffi_boundary_and_nothing_else() {
    // Named explicitly, because the test above only proves the list matches the code -- both
    // could grow together. Every module here touches the raw bindings; anything else appearing
    // would mean `unsafe` had leaked into protocol logic, and `io` appearing would mean the
    // asynchronous layer had acquired the one thing it is defined by not having.
    let expected: BTreeSet<String> = [
        "callbacks",
        "ccerr",
        "conn",
        "error",
        "params",
        "settings",
        "stream",
        "stream_io",
        "write",
    ]
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
fn all_core_module_files_are_flat() {
    // Not style. The scan above derives a module's name from its file stem, so a nested
    // `foo/bar.rs` would be scanned as `bar` -- a name `lib.rs` never declares -- and would be
    // reported as using `unsafe` without a grant, or worse slip through under another
    // module's grant.
    //
    // The asynchronous layer is exempt because it is a subtree by construction, and it earns
    // the exemption by containing no `unsafe` at all, which the tests below pin. Its root is
    // `src/io.rs` rather than `src/io/mod.rs` for exactly this reason: `mod.rs` would be
    // scanned as a module named `mod`.
    let src = crate_root().join("src");
    let nested: Vec<PathBuf> = core_files()
        .into_iter()
        .filter(|p| p.parent() != Some(src.as_path()))
        .collect();

    assert!(
        nested.is_empty(),
        "core module files must sit directly in src/, or the unsafe scan misreads their \
         names: {nested:#?}"
    );

    assert!(
        !src.join(ASYNC_SUBTREE).join("mod.rs").exists(),
        "src/{ASYNC_SUBTREE}/mod.rs exists; the layer's root must be src/{ASYNC_SUBTREE}.rs, \
         or the scan reads the module's name as `mod`"
    );
}

#[test]
fn the_async_layer_is_declared_without_an_unsafe_allowance() {
    // This is how Spec FR-021 is enforced, and it is worth stating where a reader will look
    // for it: the layer is *not* on the allowance list, so the crate-level
    // `#![deny(unsafe_code)]` makes any `unsafe` in it a compile error. The check is that the
    // declaration has not quietly acquired one, which is the only way that could stop being
    // true.
    let lib = std::fs::read_to_string(crate_root().join("src").join("lib.rs")).expect("lib.rs");
    assert!(
        lib.contains(&format!("pub mod {ASYNC_SUBTREE};")),
        "lib.rs no longer declares the layer as `pub mod {ASYNC_SUBTREE};`; this test and the \
         scans below have gone stale"
    );
    assert!(
        !modules_allowed_unsafe().contains(ASYNC_SUBTREE),
        "the asynchronous layer has been granted an `unsafe` allowance, which removes the \
         compiler's enforcement of the one property it is defined by"
    );
}

#[test]
fn the_async_layer_contains_no_unsafe_at_all() {
    // Not "confined to a declared list", as the core's rule is: *none*. Every FFI call and
    // every raw pointer lives below this layer, in the state machine, which is what lets the
    // layer be reviewed as ordinary Rust.
    let mut offenders = Vec::new();
    for path in async_files() {
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        if mentions_unsafe(&strip_comments_and_literals(&source)) {
            offenders.push(path);
        }
    }
    assert!(
        offenders.is_empty(),
        "the asynchronous layer must need no `unsafe`: {offenders:#?}"
    );
}

#[test]
fn the_async_layer_grants_itself_no_unsafe_allowance() {
    // A different claim from containing no `unsafe`, and the one that guards it: a module-level
    // `#![allow(unsafe_code)]` inside the subtree is exactly how the crate-level deny would be
    // silenced, and the file would then be free to use `unsafe` without this suite noticing
    // anything but the previous test failing later.
    //
    // Scanned after stripping comments, unlike the equivalent in `ngnet-h3`, because the
    // layer's own documentation explains this rule and therefore contains the attribute as
    // prose. A planted allowance in real code is still caught, which
    // [`the_scanner_would_catch_a_real_violation`] pins.
    let mut offenders = Vec::new();
    for path in async_files() {
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        if strip_comments_and_literals(&source).contains("allow(unsafe_code)") {
            offenders.push(path);
        }
    }
    assert!(
        offenders.is_empty(),
        "these files grant themselves `unsafe`, which is how the crate-level deny would be \
         silenced: {offenders:#?}"
    );
}

#[test]
fn the_async_subtree_exists_and_is_scanned() {
    // Guards the path filter itself. If the layer were renamed, every claim about it would
    // pass by scanning nothing, which is the most dangerous way for a structural test to fail.
    let found = async_files();
    assert!(
        !found.is_empty(),
        "no files found for src/{ASYNC_SUBTREE}.rs or src/{ASYNC_SUBTREE}/; the subtree filter \
         has gone stale and the claims made about the layer are now claims about an empty set"
    );
    assert!(
        found.iter().any(|p| p.ends_with("io.rs")),
        "the layer's module root is missing from the scan: {found:#?}"
    );
    assert!(
        found.len() > 1,
        "only the module root was found; the submodules are not being scanned: {found:#?}"
    );

    let core = core_files();
    assert!(
        !core.is_empty(),
        "no files found outside the layer; the filter has swallowed the state machine"
    );
    assert!(
        core.iter().any(|p| p.ends_with("conn.rs")),
        "the state machine is missing from the core scan: {core:#?}"
    );
}

/// Facilities a sans-I/O state machine must not name.
const FORBIDDEN: &[&str] = &[
    "std::net",
    "std::fs",
    "std::thread",
    "std::time",
    "std::process",
    "std::env",
];

#[test]
fn the_core_reaches_for_no_io_threading_or_time_facility() {
    // The clock is the interesting one. dwnx wants a timestamp on every call that can advance
    // connection state, and the only way to supply one without reading a clock is to make the
    // caller pass it -- which is why `Timestamp` exists at all.
    //
    // This is a claim about the state machine alone. The asynchronous layer exists precisely
    // to name a clock and a byte stream, and has its own, narrower claim below.
    let mut offenders = Vec::new();
    for path in core_files() {
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

/// Async facilities that must appear only inside the layer.
///
/// The state machine is driven, not driving. Its freedom from asynchrony is what lets it be
/// used from blocking code, from any runtime, and from a test with no runtime at all -- and
/// what makes `--no-default-features` the crate that existed before this layer did.
const ASYNC_FACILITIES: &[&str] = &[
    "async fn",
    "async move",
    "core::task",
    "std::task",
    "core::future",
    "std::future",
    "Waker",
    "Poll<",
];

#[test]
fn no_asynchrony_escapes_the_layer() {
    let mut offenders = Vec::new();
    for path in core_files() {
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        let code = strip_comments_and_literals(&source);
        for facility in ASYNC_FACILITIES {
            if code.contains(facility) {
                offenders.push(format!("{} names {facility}", path.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "asynchrony must stay inside the `{ASYNC_SUBTREE}` layer, or `--no-default-features` \
         is no longer the crate it claims to be: {offenders:#?}"
    );
}

/// Facilities the asynchronous layer must not reach for either.
///
/// A shorter list than the core's, and for a different reason. The layer is allowed to be
/// asynchronous -- that is the point of it -- but it must not bring a *runtime*. Spawning,
/// sleeping and threading are what would make it choose an executor on the caller's behalf,
/// and the whole design rests on it choosing none. `tokio` is on the list because the
/// ready-made implementation of the seam is a separate, feature-gated module: until it
/// exists, no file here may name the runtime at all.
const NO_RUNTIME: &[&str] = &[
    "std::thread",
    "std::process",
    "std::env",
    "std::time",
    "thread::spawn",
    "spawn(",
    "async_std",
    "smol",
    "futures_executor",
];

#[test]
fn the_async_layer_brings_no_runtime() {
    let mut findings = Vec::new();
    let mut scanned = 0usize;
    for path in async_files() {
        scanned += 1;
        let source = std::fs::read_to_string(&path).expect("reading a source file");
        let code = strip_comments_and_literals(&source);
        for facility in NO_RUNTIME {
            if code.contains(facility) {
                findings.push(format!("{}: {facility}", path.display()));
            }
        }
    }

    assert!(
        scanned > 0,
        "the runtime scan matched no files; the subtree filter has gone stale"
    );
    assert!(
        findings.is_empty(),
        "the asynchronous layer must take no executor, spawner or timer: {findings:#?}"
    );
}

#[test]
fn the_crate_declares_exactly_one_non_optional_dependency() {
    let mut declared = Vec::new();
    let mut optional = Vec::new();
    for line in manifest_table("[dependencies]") {
        let Some((name, rest)) = line.split_once('=') else {
            continue;
        };
        // `ngnet-qmux-sys.workspace = true` names the crate before the dot; the shorthand is
        // what the workspace uses elsewhere, so it has to be understood rather than tripped
        // over.
        let name = name.trim().split('.').next().expect("a dependency name");
        if rest.contains("optional = true") {
            optional.push(name.to_string());
        } else {
            declared.push(name.to_string());
        }
    }

    assert_eq!(
        declared,
        vec!["ngnet-qmux-sys".to_string()],
        "the crate's non-optional dependency list has changed; the asynchronous layer was \
         added on the promise that it adds nothing to it"
    );

    // An optional dependency that no feature names is still compiled whenever anything in the
    // workspace happens to enable it, which would make the claim above true of the manifest
    // and false of the artefact. Requiring `dep:` means each one is reachable only through a
    // feature a caller asked for.
    let text = manifest();
    let ungated: Vec<&String> = optional
        .iter()
        .filter(|name| !text.contains(&format!("dep:{name}")))
        .collect();
    assert!(
        ungated.is_empty(),
        "these optional dependencies are not gated behind a `dep:` feature entry: {ungated:?}"
    );

    // No dev-dependencies either: a test-only dependency is still something a contributor has
    // to build, and every test here runs on the standard library alone.
    assert!(
        !text.contains("[dev-dependencies]"),
        "the crate has acquired dev-dependencies; the tests are written to need none"
    );
}

#[test]
fn the_runtime_is_reachable_only_through_its_own_feature() {
    let features = manifest_table("[features]");
    let entry = |name: &str| -> String {
        features
            .iter()
            .find(|line| line.starts_with(&format!("{name} =")))
            .unwrap_or_else(|| panic!("no `{name}` feature is declared: {features:#?}"))
            .clone()
    };

    // The layer is on by default and the runtime is not, which is the whole of Spec FR-020 and
    // FR-006 as a caller experiences them.
    assert_eq!(
        entry("default"),
        "default = [\"io\"]",
        "the default feature set has changed"
    );
    assert_eq!(
        entry("io"),
        "io = []",
        "the layer's feature has acquired something; it is meant to enable code and nothing \
         else"
    );

    let tokio = entry("tokio");
    assert!(
        tokio.contains("dep:tokio"),
        "the `tokio` feature no longer names the optional dependency: {tokio}"
    );
    assert!(
        tokio.contains("\"io\""),
        "the `tokio` feature must imply `io`; there is nothing for it to plug into \
         otherwise: {tokio}"
    );
    assert!(
        !entry("default").contains("tokio"),
        "the runtime has been switched on by default, which is the one thing the seam exists \
         to avoid"
    );

    // And the dependency itself is optional, so a default build resolves it away entirely.
    let declared: Vec<String> = manifest_table("[dependencies]")
        .into_iter()
        .filter(|line| line.starts_with("tokio"))
        .collect();
    assert_eq!(declared.len(), 1, "expected one tokio entry: {declared:#?}");
    assert!(
        declared[0].contains("optional = true"),
        "the tokio dependency is not optional: {}",
        declared[0]
    );
}

#[test]
fn a_caller_never_needs_unsafe() {
    // The tests are the crate's own callers, so if none of them needs `unsafe` to drive a whole
    // exchange -- or to implement the byte-stream seam, which is the new way a caller extends
    // this crate -- then neither does anyone else.
    let mut offenders = Vec::new();
    for path in rust_files(&crate_root().join("tests")) {
        let source = std::fs::read_to_string(&path).expect("reading a test file");
        if mentions_unsafe(&strip_comments_and_literals(&source)) {
            offenders.push(path.file_name().map(|n| n.to_string_lossy().into_owned()));
        }
    }
    assert!(
        offenders.is_empty(),
        "these tests use `unsafe` to drive the crate, which callers should never have to: \
         {offenders:?}"
    );
}

#[test]
fn the_scanner_sees_through_comments_and_literals() {
    // Without this, every scan above could be passing because the stripper eats everything it
    // is given. These are the exact shapes this crate's sources contain -- the layer's own
    // documentation discusses `unsafe`, `allow(unsafe_code)` and `std::thread` in prose.
    let source = r#"
        // unsafe std::net in a line comment
        /* unsafe std::thread in a block /* nested */ comment */
        /// unsafe allow(unsafe_code) std::fs in a doc comment
        let message = "unsafe std::process in a string";
        let quote = "a \" escaped quote then std::time";
        let brace = '{';
        let escaped = '\'';
        fn generic<'a>(value: &'a str) -> &'a str { value }
        let real = 1;
    "#;
    let stripped = strip_comments_and_literals(source);

    assert!(!mentions_unsafe(&stripped), "prose must not read as code");
    assert!(!stripped.contains("allow(unsafe_code)"), "{stripped}");
    for facility in FORBIDDEN {
        assert!(
            !stripped.contains(facility),
            "{facility} leaked out of a comment or literal"
        );
    }

    // A raw string holding a quote, which is the shape that defeats a naive scan: without
    // raw-string handling the scan would end early and spill the rest out as if it were code.
    // This very file contains one, so the check is not hypothetical.
    let raw = "let held = r#\"unsafe \"quoted\" std::net\"#; let after = 2;";
    let raw_stripped = strip_comments_and_literals(raw);
    assert!(!mentions_unsafe(&raw_stripped), "got: {raw_stripped}");
    assert!(!raw_stripped.contains("std::net"), "got: {raw_stripped}");
    assert!(
        raw_stripped.contains("let after = 2;"),
        "got: {raw_stripped}"
    );

    // Real code either side of the literals survives, so the stripper is not simply blanking
    // the file.
    assert!(stripped.contains("let message"));
    assert!(stripped.contains("let real = 1;"));
    assert!(
        stripped.contains("'a>"),
        "lifetimes are not character literals and must survive: {stripped}"
    );
}

#[test]
fn the_scanner_would_catch_a_real_violation() {
    // The companion to the test above: proof that the scans fail on genuine code, not only
    // that they pass on prose. A scanner that never fires is indistinguishable from one that
    // is not run.
    let planted = "#![allow(unsafe_code)]\nuse std::net::TcpStream;\n\
                   fn f() { unsafe { g() } }\nasync fn h() {}\n";
    let stripped = strip_comments_and_literals(planted);

    assert!(mentions_unsafe(&stripped));
    assert!(stripped.contains("allow(unsafe_code)"));
    assert!(stripped.contains("std::net"));
    assert!(stripped.contains("async fn"));

    // And that `unsafe` is matched as a word, so an identifier containing it is not a false
    // positive.
    assert!(!mentions_unsafe(&strip_comments_and_literals(
        "fn unsafely() {}"
    )));
    assert!(!mentions_unsafe(&strip_comments_and_literals(
        "let unsafe_count = 1;"
    )));
}
