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
    // Counting a future's allocations needs the counter to follow the poll across
    // suspension points, so this harness installs its own GlobalAlloc — unsafe by language
    // rule — just as `zero_alloc.rs` does for the sans-I/O path. It exercises SC-017 and
    // SC-019 through the safe async API; the unsafe is measurement scaffolding, not use of
    // the API.
    (
        "http_zero_alloc.rs",
        "implements GlobalAlloc to count a future's allocations",
    ),
    // The no-copy send tests measure SC-007's "no bodies changes no allocation count" the
    // same way, installing their own GlobalAlloc — unsafe by language rule. Every use of
    // this crate's API in the file is safe; the unsafe is measurement scaffolding.
    (
        "http_shared_body.rs",
        "implements GlobalAlloc to count a bodyless exchange's allocations",
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

/// Removes line and block comments, and the contents of string, raw string and character
/// literals.
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
        } else if let Some(len) = raw_string_len(&bytes, i, out.chars().next_back()) {
            // Raw strings hold their own quotes and hashes, so the ordinary string scan
            // would stop inside one and let the remainder — braces included — leak out as
            // if it were code.
            i += len;
            out.push_str("\"\"");
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
        } else if bytes[i] == '\'' && char_literal_len(&bytes, i).is_some() {
            // Blanked for the same reason as strings, and it matters more than it looks:
            // `'{'` and `'}'` appear in this very file, and a scanner that reads them as
            // real braces would mis-locate the end of an `if` header. A lifetime or a loop
            // label is *not* a literal and falls through to be copied verbatim.
            i += char_literal_len(&bytes, i).expect("just checked");
            out.push_str("''");
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

/// The length, in `char`s, of the raw string literal starting at `start`, if there is one.
///
/// Handles `r"..."`, `r#"..."#` with any number of hashes, and the `br` byte-string forms.
/// `prev` is the previous character of the *output*, used to reject an `r` that is merely
/// the tail of an identifier — `for`, or a variable named `r` — rather than a prefix.
fn raw_string_len(bytes: &[char], start: usize, prev: Option<char>) -> Option<usize> {
    if prev.is_some_and(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let mut at = start;
    if bytes.get(at) == Some(&'b') {
        at += 1;
    }
    if bytes.get(at) != Some(&'r') {
        return None;
    }
    at += 1;
    let hashes = bytes[at..].iter().take_while(|c| **c == '#').count();
    at += hashes;
    if bytes.get(at) != Some(&'"') {
        return None;
    }
    at += 1;
    // The terminator is a quote followed by exactly as many hashes as the opener had.
    while at < bytes.len() {
        if bytes[at] == '"'
            && bytes[at + 1..]
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
///
/// A `'` in Rust source opens either a character literal or a lifetime/label, and the two
/// must be told apart: blanking `'static` would corrupt ordinary code, while leaving `'{'`
/// intact would feed a spurious brace to the scanners. A literal is a quote, one `char` —
/// or a backslash escape — and a closing quote; anything else is a lifetime.
fn char_literal_len(bytes: &[char], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&'\'') {
        return None;
    }
    match bytes.get(start + 1)? {
        // An escape: `'\n'`, `'\''`, `'\\'` and friends are four `char`s, but `'\u{7b}'`
        // is longer, so scan to the closing quote rather than assuming a width. The scan
        // starts *past* the escaped character, because that character may itself be a
        // quote: in `'\''` the third `char` is the escapee, not the terminator, and
        // stopping there would leave a stray quote to open a phantom literal.
        '\\' => {
            let mut at = start + 3;
            while at < bytes.len() && bytes[at] != '\'' {
                at += 1;
            }
            (at < bytes.len()).then_some(at + 1 - start)
        }
        // A plain literal is exactly three `char`s. Requiring the closing quote is what
        // keeps `'a` and `'static` out.
        _ => (bytes.get(start + 2) == Some(&'\'')).then_some(3),
    }
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

#[test]
fn an_included_doc_cannot_smuggle_async_past_the_scan() {
    // SC-027, continued. `no_async_facility_escapes_the_subtree` reads `.rs` files, so a
    // doc body pulled in with `include_str!` is code the scan never sees — and a doctest
    // in one is compiled and run like any other. The async example the crate root shows is
    // exactly such a body, which is why it lives at `src/http/doc_async_example.md`.
    //
    // Requiring every included doc to sit inside the subtree keeps that a rule rather than
    // a happy accident: async may appear in an included doc precisely because the file
    // holding it is part of the subtree that is allowed async in the first place.
    let mut offences = Vec::new();
    let subtree = crate_root().join("src").join("http");

    for file in rust_files(&crate_root().join("src")) {
        let source = std::fs::read_to_string(&file).expect("reading");
        for target in included_docs(&source) {
            let resolved = file
                .parent()
                .expect("a source file has a parent")
                .join(&target);
            if !resolved.starts_with(&subtree) {
                offences.push(format!("{}: includes {target}", file.display()));
            }
        }
    }

    assert!(
        offences.is_empty(),
        "an included doc body must live under src/http/, where async is permitted and \
         where the async scan's exemption is deliberate:\n{}",
        offences.join("\n")
    );
}

/// The paths every `include_str!` in `source` names, in order.
///
/// Deliberately naive — it matches the literal spelling rather than parsing Rust — because
/// the invariant it serves is about a form the crate actually uses. A cleverer spelling
/// that evaded it would evade the scanner it protects too, and the test below pins the
/// extraction so a change in that form is noticed rather than silently tolerated.
fn included_docs(source: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = source;

    while let Some(at) = rest.find("include_str!") {
        rest = &rest[at + "include_str!".len()..];
        let opened = rest.find('"');
        let Some(opened) = opened else { break };
        let after = &rest[opened + 1..];
        match after.find('"') {
            Some(closed) => {
                found.push(after[..closed].to_string());
                rest = &after[closed + 1..];
            }
            None => break,
        }
    }

    found
}

#[test]
fn included_docs_are_found_wherever_they_are_spelled() {
    assert_eq!(
        included_docs(r#"#![doc = include_str!("http/doc_async_example.md")]"#),
        vec!["http/doc_async_example.md".to_string()],
    );
    assert_eq!(
        included_docs("include_str!( \"a.md\" ) and include_str!(\"b.md\")"),
        vec!["a.md".to_string(), "b.md".to_string()],
    );
    assert!(included_docs("no includes here").is_empty());
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
fn the_crate_declares_no_dev_dependencies() {
    // The default and no-default builds must stay a zero-dependency (bar the raw bindings)
    // sans-I/O core, and the test build must not quietly acquire more. A dev-dependency is
    // compiled into the test target, where it could mask a missing feature gate or smuggle
    // an I/O facility past the scans in this file. The tests draw on nothing but the crate
    // itself and std, so the manifest must carry no dev-dependency table at all — including
    // a target-specific one such as `[target.'cfg(...)'.dev-dependencies]`.
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("manifest");
    assert!(
        !manifest.contains("dev-dependencies"),
        "crates/nghttp2/Cargo.toml must declare no dev-dependencies; tests use only the \
         crate and std"
    );
}

/// The byte index of the first use of `keyword` as a token in `code`, if any.
///
/// The token rule is [`uses_keyword`]'s: an identifier such as `lettuce` contains the
/// letters of `let` but is not the keyword, and matching it would make the let-chain scan
/// below fire on innocent code.
fn find_keyword(code: &str, keyword: &str) -> Option<usize> {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';

    code.match_indices(keyword)
        .find(|(index, _)| {
            let before_ok =
                *index == 0 || !code[..*index].chars().next_back().is_some_and(is_ident);
            let after = &code[index + keyword.len()..];
            let after_ok = !after.chars().next().is_some_and(is_ident);
            before_ok && after_ok
        })
        .map(|(index, _)| index)
}

/// The text of an `if`/`while` header: everything from just after the keyword up to the
/// block, statement, or match-arm boundary that ends the condition.
///
/// Deliberately naive, like the other scanners here: it tracks `()`/`[]` nesting so a
/// closure body inside a call — `foo(|x| { .. })` — does not look like the block, and
/// stops at the `;` of a `let` statement (never a header) or the `=>` of a match guard,
/// both at nesting depth zero.
///
/// A `{` at depth zero is the interesting case, because it may open either the body — which
/// ends the header — or a block expression *within* the condition, which does not. Both are
/// legal: `if let Some(x) = { y } && x > 0 { .. }` and `if let Some(x) = match y { v => v }
/// && x > 0 { .. }` both compile, and both are let-chains. They are told apart by what
/// follows the matching `}`: an operator means the block was part of the condition and the
/// scan continues past it, anything else means the body has been reached. Getting this
/// wrong in the lenient direction would silently miss a chain, which is the one outcome
/// this scan exists to prevent.
fn header_up_to_block(code: &str) -> &str {
    let chars: Vec<(usize, char)> = code.char_indices().collect();
    let mut depth = 0i32;
    let mut prev = ' ';
    let mut i = 0;
    while i < chars.len() {
        let (index, ch) = chars[i];
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            ';' if depth == 0 => return &code[..index],
            // The `>` of a `=>` match-guard arrow, distinguished from `>=` and generics by
            // its preceding `=`.
            '>' if depth == 0 && prev == '=' => return &code[..index],
            '{' if depth == 0 => {
                let Some(close) = matching_brace(&chars, i) else {
                    // Unbalanced: treat the brace as the body, the conservative reading for
                    // a scanner that must never run past the construct it is looking at.
                    return &code[..index];
                };
                if !continues_condition(&chars, close) {
                    return &code[..index];
                }
                // A block expression inside the condition. Step over it; its interior is
                // nested text, which `header_at_top_level` blanks out.
                i = close;
            }
            _ => {}
        }
        prev = ch;
        i += 1;
    }
    code
}

/// The index of the `}` closing the `{` at `open`, if the braces balance.
fn matching_brace(chars: &[(usize, char)], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (offset, (_, ch)) in chars.iter().enumerate().skip(open) {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether the token after the `}` at `close` continues an expression.
///
/// `&&`, `||`, `.`, `?` and the comparison operators can only follow a block that was
/// *part of* the condition; a body block is followed by the end of the construct, an
/// `else`, or the next statement.
fn continues_condition(chars: &[(usize, char)], close: usize) -> bool {
    let next = chars
        .iter()
        .skip(close + 1)
        .find(|(_, ch)| !ch.is_whitespace());
    matches!(
        next.map(|(_, ch)| *ch),
        Some('&' | '|' | '.' | '?' | '=' | '!' | '<' | '>' | '+' | '-' | '*' | '/' | '%')
    )
}

/// The parts of `header` that sit at `()`/`[]` nesting depth zero, joined by spaces.
///
/// A `let` or `&&` inside parentheses, brackets or braces belongs to something nested in
/// the condition — most often a closure passed as an argument, as in
/// `if v.iter().any(|x| { let y = *x; y > 0 }) && v.len() > 1` — and not to the condition
/// itself. Counting those would call that a let-chain, which it is not. Splitting the
/// nested text out first is what keeps the detector from crying wolf on ordinary code.
fn header_at_top_level(header: &str) -> String {
    let mut depth = 0i32;
    let mut top = String::with_capacity(header.len());
    for ch in header.chars() {
        match ch {
            '(' | '[' | '{' => {
                depth += 1;
                top.push(' ');
            }
            ')' | ']' | '}' => {
                depth -= 1;
                top.push(' ');
            }
            _ if depth == 0 => top.push(ch),
            // Nested text is replaced rather than dropped so tokens on either side of it
            // cannot be spliced into a word that was never written.
            _ => top.push(' '),
        }
    }
    top
}

/// Whether `code` contains a let-chain: an `if`/`while` header that binds with `let` and
/// joins another condition with `&&`, such as `if let Some(x) = a && cond`.
///
/// Let-chains are stable only from Rust 1.88, so on the crate's 1.85 MSRV they do not
/// compile. This catches them as text, on every toolchain, so a let-chain written on a
/// newer toolchain is rejected here rather than only when CI reaches the MSRV build.
fn contains_let_chain(code: &str) -> bool {
    for keyword in ["if", "while"] {
        let mut rest = code;
        while let Some(at) = find_keyword(rest, keyword) {
            let after_keyword = &rest[at + keyword.len()..];
            let header = header_at_top_level(header_up_to_block(after_keyword));
            if header.contains("&&") && find_keyword(&header, "let").is_some() {
                return true;
            }
            rest = after_keyword;
        }
    }
    false
}

#[test]
fn the_let_chain_detector_distinguishes_chains_from_plain_conditions() {
    // A let-chain in every position it can take.
    assert!(contains_let_chain("if let Some(x) = a && cond { }"));
    assert!(contains_let_chain(
        "while let Some(x) = it.next() && x > 0 { }"
    ));
    assert!(contains_let_chain("if flag && let Some(x) = a { }"));
    assert!(contains_let_chain("if let Some(x) = a\n    && cond\n{ }"));

    // Not let-chains: a plain `&&` condition, a lone `if let`, a `let` statement, and a
    // match guard that merely uses `&&`.
    assert!(!contains_let_chain("if a && b { }"));
    assert!(!contains_let_chain("if let Some(x) = a { }"));
    assert!(!contains_let_chain("let x = a && b;"));
    assert!(!contains_let_chain("match v { x if a && b => 1, _ => 0 }"));
    // A closure body inside the condition must not be mistaken for the header's block.
    assert!(!contains_let_chain(
        "if v.iter().any(|x| { *x }) && v.len() > 1 { }"
    ));
    // Nor may a `let` *inside* such a closure body be read as the chain's binding: it
    // belongs to the closure, not to the condition. This is the false positive the
    // top-level split exists to prevent.
    assert!(!contains_let_chain(
        "if v.iter().any(|x| { let y = *x; y > 0 }) && v.len() > 1 { }"
    ));
    assert!(!contains_let_chain(
        "while q.retain(|x| { let k = key(x); k > 0 }) && q.len() > 1 { }"
    ));
    // A genuine chain must still be caught when a closure sits beside it.
    assert!(contains_let_chain(
        "if let Some(x) = v.iter().find(|y| { let z = **y; z > 0 }) && x > 1 { }"
    ));

    // A block expression *in the condition* does not end the header. All three of these
    // compile on edition 2024 and all three are let-chains, so missing them would be a
    // silent hole in the only guard the `completion`-gated code has.
    assert!(contains_let_chain("if let Some(x) = { y } && x > 0 { }"));
    assert!(contains_let_chain(
        "if let Some(x) = match y { v => v } && x > 0 { }"
    ));
    assert!(contains_let_chain(
        "if let Some(x) = { let z = y; z } && x > 0 { }"
    ));
    // But a body block still ends the header: what follows its `}` is not an operator, so
    // the `&&` of a *later* statement cannot be dragged into this header.
    assert!(!contains_let_chain("if let Some(x) = y { }\nif a && b { }"));
    // And a `let` confined to a condition block is the block's, not the chain's.
    assert!(!contains_let_chain("if { let z = y; z } && a > 0 { }"));

    // A brace inside a character literal is not a brace. `'{'` and `'}'` appear in this
    // file, so a scanner that took them literally would lose track of where headers end
    // and could be used — deliberately or by accident — to hide a chain from the gate.
    // These go through `strip_comments_and_strings` first, exactly as the crate scan does;
    // the two together are the gate, and neither is sound alone.
    let stripped = |code: &str| contains_let_chain(&strip_comments_and_strings(code));
    assert!(stripped("if let Some(c) = Some('{') && c == '{' { }"));
    assert!(stripped("if let Some(c) = Some('}') && cond { }"));
    assert!(stripped("if let Some(c) = Some('\\'') && cond { }"));
    // The brace-bearing literal must not swallow the body either.
    assert!(!stripped("if let Some(c) = Some('{') { }"));
    // An escaped quote must not leave a stray quote behind that opens a phantom literal
    // and swallows the brace after it.
    assert!(stripped("if let Some(cs) = Some(['\\'', '{']) && cond { }"));
    // Raw strings carry their own quotes and hashes, so the ordinary string scan would
    // stop inside one and let the rest leak out as if it were code.
    assert!(stripped("if let Some(s) = Some(r#\"\"{\"#) && cond { }"));
    assert!(stripped("if let Some(s) = Some(r\"{\") && cond { }"));
    assert!(stripped("if let Some(s) = Some(br#\"{\"#) && cond { }"));
    // But a chain's punctuation appearing *inside* a raw string is just text.
    assert!(!stripped("if let Some(s) = Some(r#\"&& let x = y\"#) { }"));
    // Lifetimes and labels are not literals and must survive the same pass untouched,
    // or ordinary generic code would start reading as something else.
    assert!(!contains_let_chain(
        "if let Some(x) = foo::<&'static str>(a) { }"
    ));
    assert!(contains_let_chain(
        "if let Some(x) = foo::<&'static str>(a) && x > 0 { }"
    ));
}

#[test]
fn stripping_removes_literals_but_keeps_lifetimes() {
    // The distinction the scanners depend on: a character literal's contents must vanish,
    // a lifetime must not, because one can smuggle punctuation and the other is punctuation
    // the code genuinely contains.
    assert_eq!(strip_comments_and_strings("let c = '{';"), "let c = '';");
    assert_eq!(strip_comments_and_strings("let c = '\\n';"), "let c = '';");
    assert_eq!(
        strip_comments_and_strings("fn f<'a>(x: &'a str) {}"),
        "fn f<'a>(x: &'a str) {}"
    );
    assert_eq!(
        strip_comments_and_strings("'outer: loop { break 'outer; }"),
        "'outer: loop { break 'outer; }"
    );
    assert_eq!(
        strip_comments_and_strings("let s = \"a{b\";"),
        "let s = \"\";"
    );
    assert_eq!(strip_comments_and_strings("let c = '\\'';"), "let c = '';");
    assert_eq!(
        strip_comments_and_strings("let s = r#\"a\"{b\"#;"),
        "let s = \"\";"
    );
    assert_eq!(
        strip_comments_and_strings("let s = r\"a{b\";"),
        "let s = \"\";"
    );
    // An `r` that is not a prefix must be left alone, or ordinary identifiers would start
    // eating the code that follows them.
    assert_eq!(
        strip_comments_and_strings("for x in r { }"),
        "for x in r { }"
    );
}

#[test]
fn no_let_chain_appears_anywhere_in_the_crate() {
    // MSRV 1.85 forbids let-chains. The scan covers `src`, `tests` and `examples` alike: a
    // let-chain in a test or an example breaks the MSRV build just as surely as one in the
    // library, and `cargo +1.85 check` on the default and `tokio` configurations sees none
    // of the three in full — it cannot build the `completion`-gated code at all, and does
    // not compile tests or examples unless asked. This scan is what closes that gap, so it
    // must not be narrower than the crate.
    let mut offences = Vec::new();

    let root = crate_root();
    for dir in ["src", "tests", "examples"] {
        for file in rust_files(&root.join(dir)) {
            let code =
                strip_comments_and_strings(&std::fs::read_to_string(&file).expect("reading"));
            if contains_let_chain(&code) {
                offences.push(file.display().to_string());
            }
        }
    }

    assert!(
        offences.is_empty(),
        "let-chains do not compile on the 1.85 MSRV; use nested `if let`/`match` instead:\n{}",
        offences.join("\n")
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

#[test]
fn the_frame_buffer_is_zeroed_only_on_the_copying_read_path() {
    // Design decision D8, instrument 1. The push read callback zeroes the frame buffer
    // libnghttp2 hands it, because a `BodySource` receives a readable slice and must never
    // see another stream's plaintext left there. The no-copy read callback hands nothing
    // to the source and writes no payload into that buffer at all — libnghttp2 serialises
    // only the header and the payload travels as the caller's own `Bytes` — so it has
    // neither the hazard nor the memset. This pins that asymmetry: the zeroing must appear
    // exactly once, and only inside `read_push_body`. A second `write_bytes`, or one that
    // migrated into the shared path, would be either a needless cost or a sign the no-copy
    // path had started touching the buffer it is supposed to leave alone.
    let path = crate_root().join("src").join("callbacks.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {path:?}: {e}; the callbacks module has moved"));
    let code = strip_comments_and_strings(&source);

    let zeroings = code.matches("write_bytes").count();
    assert_eq!(
        zeroings, 1,
        "expected exactly one `write_bytes`, the push path's memset, found {zeroings}",
    );

    // Textual position is not containment. The earlier form only asserted the memset lay
    // *between* the declarations of `read_push_body` and `read_shared_body`, which any
    // function declared in that gap would satisfy just as well — it never proved the
    // memset was inside `read_push_body`'s own body. This finds the brace that opens that
    // body, its matching close, and requires the memset to fall strictly inside the span
    // they bound. A memset that migrated into a helper sitting between the two functions,
    // or into the no-copy path, now fails here rather than passing unnoticed.
    //
    // `matching_brace` indexes the char vector by position, while `str::find` yields byte
    // offsets; the two are reconciled by reading the brace positions' byte offsets back
    // out of the vector (`chars[..].0`) so every comparison below is in byte space, which
    // is where `find` reports the memset.
    let chars: Vec<(usize, char)> = code.char_indices().collect();

    let shared = code
        .find("fn read_shared_body")
        .expect("read_shared_body has moved; this scan is stale");
    let push = code
        .find("fn read_push_body")
        .expect("read_push_body has moved; this scan is stale");
    // The first `{` after the signature opens the body: a fn signature carries only `<>`,
    // `()` and a brace-free return type, so nothing before the body can be mistaken for it.
    let open = chars
        .iter()
        .position(|(byte, ch)| *byte > push && *ch == '{')
        .expect("read_push_body has no body brace; this scan is stale");
    let close = matching_brace(&chars, open)
        .expect("read_push_body's body brace does not close; this scan is stale");
    let (body_start, body_end) = (chars[open].0, chars[close].0);

    let memset = code
        .find("write_bytes")
        .expect("the single write_bytes just counted");

    assert!(
        body_start < memset && memset < body_end,
        "the frame-buffer memset is no longer inside read_push_body's body: its write_bytes \
         sits at byte {memset}, while that body spans bytes {body_start}..{body_end}",
    );
    // The whole point of the split is that the no-copy path has no such memset; if it ever
    // moved there, the span check above would already have failed, but a body that starts
    // before the shared path keeps the two genuinely disjoint.
    assert!(
        body_end < shared,
        "read_push_body's body now overlaps read_shared_body (spans {body_start}..{body_end}, \
         read_shared_body begins at {shared}); the two paths are no longer distinct",
    );
}
