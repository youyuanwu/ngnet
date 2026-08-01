//! Verifies the native error-code mapping is total and stays in step with the vendored
//! nghttp2 headers (Spec SC-004, SC-019).
//!
//! This test lives in `tests/` rather than `src/` for two reasons: it reads from the
//! filesystem, which the SC-021 source scan forbids in the crate's own source, and the
//! crate-level `deny(unsafe_code)` does not need relaxing here.
//!
//! The source of truth is the vendored header rather than the generated bindings,
//! because bindgen emits these codes as plain integer constants under
//! `EnumVariation::Consts`; there is no enumerable set to iterate at runtime. Scanning
//! the header means that upgrading nghttp2 to a release that adds or removes a code
//! fails this test instead of silently falling through to `ErrorKind::Internal`.

use std::collections::BTreeSet;
use std::path::PathBuf;

use nghttp2::{ALL_NATIVE_CODES, Error, ErrorKind};

/// Resolves the vendored header from this crate's manifest directory.
///
/// The crate has no build script, so `DEP_NGHTTP2_INCLUDE` is not available here, and
/// this deliberately does not honour the `NGHTTP2_SOURCE_DIR` override that the sys
/// crate's build script accepts.
fn header_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deps/nghttp2/lib/includes/nghttp2/nghttp2.h")
}

/// Extracts `NGHTTP2_ERR_<NAME> = -<N>,` members from the vendored header.
fn native_codes_declared_in_header() -> BTreeSet<i32> {
    let header = std::fs::read_to_string(header_path()).expect(
        "vendored nghttp2 header not found; run `git submodule update --init deps/nghttp2`",
    );

    // Every line that begins a declaration must parse. Silently skipping an
    // unparsable one would turn this guard into a false negative precisely when the
    // header format changes, which is the case it exists to catch.
    header
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("NGHTTP2_ERR_"))
        .map(|line| {
            let (_name, value) = line
                .split_once('=')
                .unwrap_or_else(|| panic!("declaration line has no `=`: {line:?}"));
            value
                .trim()
                .trim_end_matches(',')
                .parse::<i32>()
                .unwrap_or_else(|e| panic!("could not parse value from {line:?}: {e}"))
        })
        .collect()
}

#[test]
fn crate_code_list_matches_the_vendored_header() {
    let declared = native_codes_declared_in_header();
    assert!(
        !declared.is_empty(),
        "parsed no error codes from the header; the parser or the header format changed"
    );

    let known: BTreeSet<i32> = ALL_NATIVE_CODES.iter().map(|code| code.get()).collect();

    let missing: Vec<_> = declared.difference(&known).copied().collect();
    let extra: Vec<_> = known.difference(&declared).copied().collect();

    assert!(
        missing.is_empty(),
        "the vendored header declares error codes this crate does not translate: {missing:?}. \
         Add them to ALL_NATIVE_CODES and to the classify() mapping."
    );
    assert!(
        extra.is_empty(),
        "this crate lists error codes the vendored header no longer declares: {extra:?}"
    );
}

#[test]
fn every_native_code_maps_to_exactly_one_category() {
    // `ErrorKind` is `#[non_exhaustive]`, so an exhaustive match is not possible from
    // outside the crate and would prove nothing here. What is worth asserting is that
    // translation is total, deterministic, lossless, and actually discriminates rather
    // than funnelling everything into one bucket.
    let mut seen: BTreeSet<&'static str> = BTreeSet::new();

    for code in ALL_NATIVE_CODES {
        let error = Error::from_native("probe", code.get());

        assert_eq!(
            error.native_code(),
            Some(*code),
            "translation lost the originating code for {code:?}"
        );
        assert_eq!(
            error.kind(),
            Error::from_native("probe", code.get()).kind(),
            "translation of {code:?} is not deterministic"
        );

        seen.insert(error.kind().description());
    }

    assert_eq!(
        seen.len(),
        4,
        "all four categories should be exercised across the native codes, saw: {seen:?}"
    );
}

#[test]
fn the_three_specified_categories_are_distinguishable() {
    use nghttp2_sys_codes::*;

    assert_eq!(
        Error::from_native("recv", NOMEM).kind(),
        ErrorKind::Exhausted,
        "memory exhaustion must be its own category"
    );
    assert_eq!(
        Error::from_native("recv", BAD_CLIENT_MAGIC).kind(),
        ErrorKind::Protocol,
        "a peer sending a bad preface is a protocol failure, not caller error"
    );
    assert_eq!(
        Error::from_native("submit_response", INVALID_ARGUMENT).kind(),
        ErrorKind::InvalidInput,
        "an argument the caller controls is caller error"
    );

    // The five conditions Spec FR-030 names as fatal to a receive call.
    for code in [
        NOMEM,
        CALLBACK_FAILURE,
        BAD_CLIENT_MAGIC,
        FLOODED,
        TOO_MANY_CONTINUATIONS,
    ] {
        let error = Error::from_native("recv", code);
        assert_ne!(
            error.kind(),
            ErrorKind::InvalidInput,
            "a connection-fatal condition must not be reported as caller error: {error}"
        );
    }
}

#[test]
fn display_names_the_failing_operation_for_every_category() {
    // SC-019 requires the operation to be named whatever the category, so drive one
    // representative code from each rather than trusting a single sample.
    let mut categories_covered: BTreeSet<&'static str> = BTreeSet::new();

    for code in ALL_NATIVE_CODES {
        let error = Error::from_native("submit_request", code.get());
        let rendered = error.to_string();

        assert!(
            rendered.contains("submit_request"),
            "the message must name the operation that failed, got: {rendered}"
        );
        assert!(
            rendered.contains(error.kind().description()),
            "the message must describe the category, got: {rendered}"
        );
        assert!(
            rendered.contains(&code.get().to_string()),
            "the message must carry the underlying condition, got: {rendered}"
        );

        categories_covered.insert(error.kind().description());
    }

    assert_eq!(
        categories_covered.len(),
        4,
        "every category should have been rendered at least once, saw: {categories_covered:?}"
    );
}

#[test]
fn unknown_codes_do_not_panic() {
    let error = Error::from_native("probe", -12_345);
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert!(!error.to_string().is_empty());
}

/// The handful of raw constants this test needs, reached through the crate's own escape
/// hatch so the test does not declare `nghttp2-sys` as a dependency (Spec SC-007).
mod nghttp2_sys_codes {
    pub const NOMEM: i32 = nghttp2::raw::NGHTTP2_ERR_NOMEM;
    pub const CALLBACK_FAILURE: i32 = nghttp2::raw::NGHTTP2_ERR_CALLBACK_FAILURE;
    pub const BAD_CLIENT_MAGIC: i32 = nghttp2::raw::NGHTTP2_ERR_BAD_CLIENT_MAGIC;
    pub const FLOODED: i32 = nghttp2::raw::NGHTTP2_ERR_FLOODED;
    pub const TOO_MANY_CONTINUATIONS: i32 = nghttp2::raw::NGHTTP2_ERR_TOO_MANY_CONTINUATIONS;
    pub const INVALID_ARGUMENT: i32 = nghttp2::raw::NGHTTP2_ERR_INVALID_ARGUMENT;
}
