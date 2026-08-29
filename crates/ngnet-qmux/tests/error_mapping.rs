//! Proves the error mapping covers every condition dwnx defines.
//!
//! The obvious way to write this is to list today's error constants and assert each maps
//! somewhere. That version passes forever: adding a twenty-fifth `#define` upstream does not
//! make a hand-written Rust list fail, which is exactly the regression worth catching when the
//! submodule moves.
//!
//! So the list is not hand-written. This scans the vendored header for `DWNX_ERR_*`
//! definitions and checks each one against the mapping, which means a new upstream condition
//! fails here until somebody classifies it deliberately.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use ngnet_qmux::{ErrorKind, NativeCode};
use ngnet_qmux_sys as sys;

/// Every `DWNX_ERR_*` constant the vendored header defines, with its value.
///
/// `DWNX_ERR_FATAL` is excluded: it is not a condition but the threshold `dwnx_err_is_fatal`
/// compares against, and no operation returns it.
fn conditions_from_header() -> BTreeSet<(String, i32)> {
    let header = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ngnet-qmux-sys/vendor/dwnx/lib/includes/dwnx/dwnx.h");
    let source = fs::read_to_string(&header)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", header.display()));

    let mut found = BTreeSet::new();
    for line in source.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("#define DWNX_ERR_") else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let (Some(name), Some(value)) = (parts.next(), parts.next()) else {
            continue;
        };
        if name == "FATAL" {
            continue;
        }
        let Ok(value) = value.parse::<i32>() else {
            continue;
        };
        found.insert((format!("DWNX_ERR_{name}"), value));
    }

    assert!(
        found.len() >= 20,
        "only found {} error constants; the header format probably changed and this test is \
         no longer scanning anything",
        found.len()
    );
    found
}

/// Every condition in the header is classified deliberately.
///
/// The mapping's fallback arm sends unrecognised codes to `Internal`, which is right for
/// forward compatibility at run time but would hide a new condition from this test. So rather
/// than trusting the classification alone, each header constant is checked against an explicit
/// table below. A code in the header but not in the table fails.
#[test]
fn every_header_condition_is_deliberately_classified() {
    let expected: &[(i32, ErrorKind)] = &[
        (sys::DWNX_ERR_INVALID_ARGUMENT, ErrorKind::InvalidArgument),
        (sys::DWNX_ERR_NOBUF, ErrorKind::Memory),
        (sys::DWNX_ERR_PROTO, ErrorKind::Protocol),
        (sys::DWNX_ERR_INVALID_STATE, ErrorKind::InvalidState),
        (sys::DWNX_ERR_STREAM_ID_BLOCKED, ErrorKind::Stream),
        (sys::DWNX_ERR_STREAM_IN_USE, ErrorKind::Stream),
        (sys::DWNX_ERR_STREAM_DATA_BLOCKED, ErrorKind::Stream),
        (sys::DWNX_ERR_FLOW_CONTROL, ErrorKind::LimitExceeded),
        (sys::DWNX_ERR_STREAM_LIMIT, ErrorKind::LimitExceeded),
        (sys::DWNX_ERR_FINAL_SIZE, ErrorKind::Protocol),
        (
            sys::DWNX_ERR_REQUIRED_TRANSPORT_PARAM,
            ErrorKind::TransportParameter,
        ),
        (
            sys::DWNX_ERR_MALFORMED_TRANSPORT_PARAM,
            ErrorKind::TransportParameter,
        ),
        (sys::DWNX_ERR_FRAME_ENCODING, ErrorKind::Protocol),
        (sys::DWNX_ERR_STREAM_SHUT_WR, ErrorKind::Stream),
        (sys::DWNX_ERR_STREAM_NOT_FOUND, ErrorKind::Stream),
        (sys::DWNX_ERR_STREAM_STATE, ErrorKind::Stream),
        (sys::DWNX_ERR_CLOSING, ErrorKind::Closed),
        (sys::DWNX_ERR_DRAINING, ErrorKind::Closed),
        (sys::DWNX_ERR_TRANSPORT_PARAM, ErrorKind::TransportParameter),
        (sys::DWNX_ERR_INTERNAL, ErrorKind::Internal),
        (sys::DWNX_ERR_WRITE_MORE, ErrorKind::InvalidState),
        (sys::DWNX_ERR_IDLE_CLOSE, ErrorKind::Closed),
        (sys::DWNX_ERR_NOMEM, ErrorKind::Memory),
        (sys::DWNX_ERR_CALLBACK_FAILURE, ErrorKind::Handler),
    ];

    let classified: BTreeSet<i32> = expected.iter().map(|(code, _)| *code).collect();

    for (name, value) in conditions_from_header() {
        assert!(
            classified.contains(&value),
            "{name} ({value}) is defined by dwnx but not classified by ngnet-qmux. \
             Add it to the mapping in src/error.rs and to the table in this test."
        );
    }

    // The table's second column has to be asserted, not merely written down. Checking only
    // that each constant *appears* would prove the mapping is total while saying nothing
    // about whether it is right, and eighteen of these are not covered by any other test.
    for (code, expected_kind) in expected {
        let actual = ngnet_qmux::Error::from_native(*code, "test").kind();
        assert_eq!(
            actual,
            *expected_kind,
            "dwnx error {code} is classified as {actual:?}, but this test expects \
             {expected_kind:?}. One of the two is wrong."
        );
    }

    // And the reverse: nothing in the table has been removed upstream.
    let in_header: BTreeSet<i32> = conditions_from_header()
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    for (code, _) in expected {
        assert!(
            in_header.contains(code),
            "{code} is classified by ngnet-qmux but no longer defined by dwnx"
        );
    }
}

/// The classification actually produced matches the table.
#[test]
fn classification_matches_the_table() {
    use ngnet_qmux::Error;

    for (code, kind) in [
        (sys::DWNX_ERR_PROTO, ErrorKind::Protocol),
        (sys::DWNX_ERR_NOMEM, ErrorKind::Memory),
        (sys::DWNX_ERR_CALLBACK_FAILURE, ErrorKind::Handler),
        (sys::DWNX_ERR_DRAINING, ErrorKind::Closed),
        (sys::DWNX_ERR_STREAM_NOT_FOUND, ErrorKind::Stream),
        (sys::DWNX_ERR_FLOW_CONTROL, ErrorKind::LimitExceeded),
    ] {
        assert_eq!(Error::from_native(code, "test").kind(), kind);
    }
}

/// An unknown code classifies rather than panicking, so a newer dwnx does not break a caller
/// at run time -- the test above is what makes the gap visible at build time instead.
#[test]
fn unknown_codes_fall_back_to_internal() {
    use ngnet_qmux::Error;

    let error = Error::from_native(-9999, "test");
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.native().map(NativeCode::get), Some(-9999));
}
