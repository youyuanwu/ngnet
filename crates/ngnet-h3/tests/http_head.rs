#![cfg(feature = "http")]
//! Head conversion, exercised from outside the crate.
//!
//! The unit tests beside `head.rs` cover the rules one at a time. This suite covers the two
//! things they cannot: that the conversions are reachable and coherent from a caller's side
//! of the crate boundary, and that a head survives a full round trip — encoded as a field
//! section and decoded back — without losing or inventing anything.
//!
//! Round-tripping matters more than it looks. Encoding and decoding are written separately
//! and could each be self-consistently wrong; a request that comes back changed is the only
//! cheap way to catch that, and self-interop is all this crate has (no third-party HTTP/3
//! implementation is in the test matrix, which `docs/h3/pending-work.md` records).

use ngnet_h3::http::ErrorKind;
use ngnet_h3::http::testing::head;

#[test]
fn a_request_survives_a_round_trip_unchanged() {
    let request = http::Request::builder()
        .method("POST")
        .uri("https://example.test/things?q=1&r=2")
        .header("accept", "text/plain")
        .header("x-trace", "abc123")
        .body(())
        .expect("a request");
    let (parts, ()) = request.into_parts();

    let encoded = head::request_fields(&parts).expect("encoding");
    let decoded = head::request_head(&encoded).expect("decoding");

    assert_eq!(decoded.method(), parts.method);
    assert_eq!(decoded.uri().scheme_str(), Some("https"));
    assert_eq!(
        decoded.uri().authority().map(|a| a.as_str()),
        Some("example.test")
    );
    assert_eq!(decoded.uri().path(), "/things");
    assert_eq!(decoded.uri().query(), Some("q=1&r=2"));
    assert_eq!(decoded.headers().get("accept").unwrap(), "text/plain");
    assert_eq!(decoded.headers().get("x-trace").unwrap(), "abc123");
}

#[test]
fn a_response_survives_a_round_trip_unchanged() {
    let response = http::Response::builder()
        .status(201)
        .header("location", "/things/1")
        .body(())
        .expect("a response");
    let (parts, ()) = response.into_parts();

    let encoded = head::response_fields(&parts).expect("encoding");
    let decoded = head::response_head(&encoded).expect("decoding");

    assert_eq!(decoded.status(), http::StatusCode::CREATED);
    assert_eq!(decoded.headers().get("location").unwrap(), "/things/1");
}

#[test]
fn trailers_survive_a_round_trip_unchanged() {
    let mut sent = http::HeaderMap::new();
    sent.insert("x-checksum", "deadbeef".parse().expect("a value"));
    sent.insert("x-duration", "12ms".parse().expect("a value"));

    let encoded = head::trailer_fields(&sent).expect("encoding");
    let decoded = head::trailers(&encoded).expect("decoding");

    assert_eq!(decoded.get("x-checksum").unwrap(), "deadbeef");
    assert_eq!(decoded.get("x-duration").unwrap(), "12ms");
}

#[test]
fn a_repeated_field_keeps_all_of_its_values() {
    // `HeaderMap` is a multimap and HTTP/3 field sections carry repeats, so neither side may
    // silently collapse them — a lost `set-cookie` is a real bug and an easy one to make.
    let response = http::Response::builder()
        .header("set-cookie", "a=1")
        .header("set-cookie", "b=2")
        .body(())
        .expect("a response");
    let (parts, ()) = response.into_parts();

    let encoded = head::response_fields(&parts).expect("encoding");
    let decoded = head::response_head(&encoded).expect("decoding");

    let cookies: Vec<&str> = decoded
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().expect("ascii"))
        .collect();
    assert_eq!(cookies, vec!["a=1", "b=2"]);
}

#[test]
fn both_schemes_survive_a_round_trip() {
    for scheme in ["http", "https"] {
        let request = http::Request::builder()
            .uri(format!("{scheme}://example.test/"))
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let encoded = head::request_fields(&parts).expect("encoding");
        let decoded = head::request_head(&encoded).expect("decoding");
        assert_eq!(decoded.uri().scheme_str(), Some(scheme));
    }
}

#[test]
fn a_rejected_head_reports_a_protocol_error() {
    // The category matters: a caller distinguishing "I sent something invalid" from "the
    // transport died" has to be able to, and a single opaque error would force string
    // matching.
    let request = http::Request::builder()
        .uri("/no-authority-anywhere")
        .body(())
        .expect("a request");
    let (parts, ()) = request.into_parts();

    let error = head::request_fields(&parts).expect_err("a head with no authority");
    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert!(!error.is_retriable());
}

#[test]
fn an_informational_status_is_reported_as_not_final() {
    assert!(head::is_informational(http::StatusCode::CONTINUE));
    assert!(head::is_informational(
        http::StatusCode::from_u16(103).expect("early hints")
    ));
    assert!(!head::is_informational(http::StatusCode::OK));
    assert!(!head::is_informational(
        http::StatusCode::INTERNAL_SERVER_ERROR
    ));
}

#[test]
fn a_peer_sending_a_forbidden_field_is_refused_on_decode_too() {
    // Encoding refuses these, but the peer is not running this code, and that is the half
    // that matters. `transfer-encoding` has no meaning once each request owns a QUIC
    // stream; a decoder that passed it through would hand a handler a framing instruction
    // from an untrusted source, which is the shape of a request-smuggling bug. RFC 9114
    // §4.2 makes such a message malformed, so it is refused rather than sanitised.
    let response = vec![
        (b":status".to_vec(), b"200".to_vec()),
        (b"transfer-encoding".to_vec(), b"chunked".to_vec()),
    ];
    assert_eq!(
        head::response_head(&response)
            .expect_err("a forbidden field")
            .kind(),
        ErrorKind::Protocol
    );

    let request = vec![
        (b":method".to_vec(), b"GET".to_vec()),
        (b":scheme".to_vec(), b"https".to_vec()),
        (b":authority".to_vec(), b"example.test".to_vec()),
        (b":path".to_vec(), b"/".to_vec()),
        (b"connection".to_vec(), b"keep-alive".to_vec()),
    ];
    assert_eq!(
        head::request_head(&request)
            .expect_err("a forbidden field")
            .kind(),
        ErrorKind::Protocol
    );

    let trailers = vec![(b"upgrade".to_vec(), b"h2c".to_vec())];
    assert_eq!(
        head::trailers(&trailers)
            .expect_err("a forbidden field")
            .kind(),
        ErrorKind::Protocol
    );
}
