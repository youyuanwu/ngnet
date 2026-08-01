//! The sanctioned `unsafe` test (Spec SC-007, and the sole exception named by SC-002).
//!
//! Spec FR-029 requires that capabilities the safe API does not wrap remain reachable
//! without taking a second dependency. Demonstrating that necessarily means calling a
//! raw binding, which necessarily means `unsafe` — hence this file is the one place in
//! the crate's tests where `unsafe` is permitted, and it is named explicitly in the
//! SC-002 invariant check so the rule stays mechanical.
//!
//! Note this crate's `Cargo.toml` declares only `nghttp2-sys` as a runtime dependency
//! and this test does not name it at all; everything below goes through `nghttp2::raw`.

use std::ffi::CStr;

#[test]
fn unwrapped_capability_is_reachable_through_the_escape_hatch() {
    // `nghttp2_version` is deliberately not wrapped by the safe API, standing in for any
    // capability a caller might need before this crate grows a wrapper for it.
    //
    // SAFETY: `nghttp2_version` takes a minimum-version argument, returns a pointer to a
    // static struct owned by the library, and has no preconditions.
    let info = unsafe { nghttp2::raw::nghttp2_version(0) };
    assert!(!info.is_null());

    // SAFETY: the pointer is non-null and points at a static struct whose `version_str`
    // member is a NUL-terminated string with the library's lifetime.
    let version = unsafe { CStr::from_ptr((*info).version_str) };

    assert_eq!(
        version.to_str().unwrap(),
        "1.70.0",
        "the escape hatch should reach the same library the safe API is built on"
    );
}

#[test]
fn raw_error_codes_translate_through_the_safe_error_type() {
    // The escape hatch is only useful if a raw failure can be folded back into the safe
    // API's error vocabulary rather than leaving the caller with a bare integer.
    let error = nghttp2::Error::from_native("nghttp2_session_mem_recv2", nghttp2::raw::NGHTTP2_ERR_NOMEM);

    assert_eq!(error.kind(), nghttp2::ErrorKind::Exhausted);
    assert!(error.to_string().contains("nghttp2_session_mem_recv2"));
}
