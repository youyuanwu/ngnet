//! Translating between `http` message heads and the wrapper's header sets.
//!
//! HTTP/2 does not carry a request line or a status line; both are encoded as
//! pseudo-header fields that must precede every regular field. The `http` crate models
//! them as structured parts instead, so something has to convert — and the conversion is
//! where the protocol's rules about field names live.

use crate::{Header, header};

use super::error::{Error, ErrorKind, Result};

/// Field names HTTP/2 forbids outright.
///
/// These are connection-specific in HTTP/1.1 and have no meaning once each stream has its
/// own framing. RFC 9113 §8.2.2 makes a message carrying one malformed, so they are
/// rejected here rather than being quietly dropped — silently discarding a header a
/// caller deliberately set would be worse than saying no.
const FORBIDDEN: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
];

/// A header set that owns its octets.
///
/// [`Header`] borrows, and libnghttp2 copies at submission, so the octets only have to
/// live across the submitting call — but they have to live *somewhere*, and the `http`
/// types they come from are consumed on the way. This is that somewhere.
#[derive(Debug, Default)]
pub(crate) struct OwnedHeaders {
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

impl OwnedHeaders {
    fn push(&mut self, name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.fields.push((name.into(), value.into()));
    }

    /// Borrowed views suitable for submission.
    pub(crate) fn views(&self) -> Vec<Header<'_>> {
        self.fields
            .iter()
            .map(|(name, value)| Header::from_bytes(name, value))
            .collect()
    }
}

fn protocol(detail: &'static str) -> Error {
    Error::new(ErrorKind::Protocol, detail)
}

/// Encodes a request head as an HTTP/2 header set.
///
/// The four request pseudo-headers come first, in the order RFC 9113 lists them. Their
/// order among themselves is not required by the protocol, but keeping it fixed makes the
/// output of this function reproducible, which the tests rely on.
pub(crate) fn request_headers(parts: &http::request::Parts) -> Result<OwnedHeaders> {
    if parts.method == http::Method::CONNECT {
        return Err(protocol(
            "CONNECT is not supported: this crate speaks h2c request/response only",
        ));
    }

    let mut headers = OwnedHeaders::default();
    headers.push(":method", parts.method.as_str());
    headers.push(":scheme", parts.uri.scheme_str().unwrap_or("http"));

    // A request must name its authority. The URI is the primary source; a `host` field is
    // accepted as a fallback so a caller porting HTTP/1.1 code is not stuck.
    let authority = parts
        .uri
        .authority()
        .map(|authority| authority.as_str().as_bytes().to_vec())
        .or_else(|| {
            parts
                .headers
                .get(http::header::HOST)
                .map(|host| host.as_bytes().to_vec())
        })
        .ok_or_else(|| {
            protocol("a request needs an authority: set one on the URI, or send a host field")
        })?;
    headers.push(":authority", authority);

    let path = parts
        .uri
        .path_and_query()
        .map_or("/", http::uri::PathAndQuery::as_str);
    headers.push(":path", path);

    for (name, value) in &parts.headers {
        // `http::HeaderName` is lowercase by construction, so the case rule HTTP/2 imposes
        // is already satisfied and does not need re-checking here.
        let name = name.as_str();
        if name == "host" {
            // Already carried as `:authority`; sending both invites disagreement.
            continue;
        }
        if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/2 forbids it",
            ));
        }
        headers.push(name, value.as_bytes());
    }

    // Validate before anything is submitted, so a rejected head never half-touches the
    // session. This is the same check submission would perform, run early enough that the
    // error names the caller's mistake rather than a native return code.
    header::validate(&headers.views()).map_err(|error| {
        Error::with_source(
            ErrorKind::Protocol,
            "the request head is not a valid HTTP/2 header set",
            error,
        )
    })?;
    Ok(headers)
}

/// Decodes a received response header block.
///
/// The block arrives as a flat list because that is how it is delivered field by field;
/// `:status` is extracted and everything else becomes an ordinary field.
pub(crate) fn response_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Response<()>> {
    let mut builder = http::Response::builder();
    let mut status = None;

    for (name, value) in fields {
        if name.first() == Some(&b':') {
            if name.as_slice() != b":status" {
                return Err(protocol("a response carries no pseudo-header but :status"));
            }
            if status.is_some() {
                return Err(protocol("a response carries exactly one :status"));
            }
            status = Some(
                http::StatusCode::from_bytes(value)
                    .map_err(|_| protocol("the peer sent a malformed :status"))?,
            );
            continue;
        }

        if status.is_none() {
            return Err(protocol(
                "a response must send :status before any other field",
            ));
        }

        let name = http::HeaderName::from_bytes(name)
            .map_err(|_| protocol("the peer sent a malformed field name"))?;
        let value = http::HeaderValue::from_bytes(value)
            .map_err(|_| protocol("the peer sent a malformed field value"))?;
        builder = builder.header(name, value);
    }

    let status = status.ok_or_else(|| protocol("a response must carry :status"))?;
    builder
        .status(status)
        .body(())
        .map_err(|_| protocol("the peer sent a response head that could not be assembled"))
}

/// Encodes a response head as an HTTP/2 header set.
///
/// A response carries exactly one pseudo-header, `:status`, and it must come first.
pub(crate) fn response_headers(parts: &http::response::Parts) -> Result<OwnedHeaders> {
    let mut headers = OwnedHeaders::default();
    headers.push(":status", parts.status.as_str());

    for (name, value) in &parts.headers {
        let name = name.as_str();
        if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/2 forbids it",
            ));
        }
        headers.push(name, value.as_bytes());
    }

    header::validate(&headers.views()).map_err(|error| {
        Error::with_source(
            ErrorKind::Protocol,
            "the response head is not a valid HTTP/2 header set",
            error,
        )
    })?;
    Ok(headers)
}

/// Decodes a received request header block.
///
/// HTTP/2 splits the request line across four pseudo-headers; `http::Request` has a method
/// and a URI. Reassembling the URI from `:scheme`, `:authority` and `:path` is what this
/// does, and rejecting a head that cannot make one is the other half of it — a request
/// missing a pseudo-header is malformed, not merely inconvenient.
pub(crate) fn request_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Request<()>> {
    let mut builder = http::Request::builder();
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut seen_field = false;

    for (name, value) in fields {
        if name.first() == Some(&b':') {
            if seen_field {
                return Err(protocol(
                    "a request must send its pseudo-headers before any other field",
                ));
            }
            let slot = match name.as_slice() {
                b":method" => &mut method,
                b":scheme" => &mut scheme,
                b":authority" => &mut authority,
                b":path" => &mut path,
                b":protocol" => {
                    return Err(protocol(
                        "extended CONNECT is not supported: this crate speaks h2c \
                         request/response only",
                    ));
                }
                _ => {
                    return Err(protocol(
                        "the peer sent a pseudo-header HTTP/2 does not define",
                    ));
                }
            };
            if slot.is_some() {
                return Err(protocol(
                    "a request carries each pseudo-header at most once",
                ));
            }
            *slot = Some(value.clone());
            continue;
        }

        seen_field = true;
        let name = http::HeaderName::from_bytes(name)
            .map_err(|_| protocol("the peer sent a malformed field name"))?;
        let value = http::HeaderValue::from_bytes(value)
            .map_err(|_| protocol("the peer sent a malformed field value"))?;
        builder = builder.header(name, value);
    }

    let method = method.ok_or_else(|| protocol("a request must carry :method"))?;
    let path = path.ok_or_else(|| protocol("a request must carry :path"))?;
    let authority = authority.ok_or_else(|| protocol("a request must carry :authority"))?;
    // Cleartext only, so a missing scheme has exactly one sensible reading — but a scheme
    // that says otherwise is the peer telling us something this crate cannot honour.
    let scheme = scheme.unwrap_or_else(|| b"http".to_vec());
    if scheme != b"http" {
        return Err(protocol("this crate speaks cleartext HTTP/2 only"));
    }

    let method = http::Method::from_bytes(&method)
        .map_err(|_| protocol("the peer sent a method that is not a token"))?;
    if method == http::Method::CONNECT {
        return Err(protocol(
            "CONNECT is not supported: this crate speaks h2c request/response only",
        ));
    }

    // Assembled from validated components rather than by joining text. Concatenating
    // `scheme://authority` and `path` would let an authority containing a slash claim part
    // of the path — `evil.test/admin` with a path of `/x` reads back as authority
    // `evil.test` and path `/admin/x` — which is a routing decision made by the peer
    // rather than by the server. RFC 9113 §8.3.1 forbids that shape; parsing each piece on
    // its own is what enforces it.
    let authority = http::uri::Authority::try_from(authority.as_slice())
        .map_err(|_| protocol("the peer sent an :authority that is not a valid authority"))?;
    if authority.as_str().contains('@') {
        return Err(protocol("RFC 9113 §8.3.1 forbids userinfo in :authority"));
    }

    let path_and_query = http::uri::PathAndQuery::try_from(path.as_slice())
        .map_err(|_| protocol("the peer sent a :path that is not a valid request target"))?;
    // The asterisk form is legal, but only for OPTIONS, and this crate does not serve it.
    // Accepting it elsewhere would hand a handler a path it cannot route on.
    if path_and_query.path() == "*" {
        return Err(protocol(
            "the asterisk request target is not supported: this crate speaks h2c \
             request/response only",
        ));
    }

    let uri = http::Uri::builder()
        .scheme(scheme.as_slice())
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .map_err(|_| protocol("the peer sent a request target that is not a URI"))?;

    builder
        .method(method)
        .uri(uri)
        .body(())
        .map_err(|_| protocol("the peer sent a request head that could not be assembled"))
}

/// Encodes a trailing header block for submission.
///
/// The same rule as decoding, from the other side: trailers carry ordinary fields only,
/// and the connection-specific names HTTP/2 forbids are forbidden here too. A caller who
/// set one is told rather than having it quietly dropped.
pub(crate) fn trailer_fields(trailers: &http::HeaderMap) -> Result<OwnedHeaders> {
    let mut headers = OwnedHeaders::default();

    for (name, value) in trailers {
        let name = name.as_str();
        if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/2 forbids it in trailers",
            ));
        }
        headers.push(name, value.as_bytes());
    }

    Ok(headers)
}

/// Decodes a received trailing header block.
///
/// Trailers are ordinary fields only: a pseudo-header after a message has begun is
/// malformed, and RFC 9113 §8.1 says so explicitly.
pub(crate) fn trailers(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::HeaderMap> {
    let mut map = http::HeaderMap::with_capacity(fields.len());

    for (name, value) in fields {
        if name.first() == Some(&b':') {
            return Err(protocol(
                "a trailing header block carries no pseudo-headers",
            ));
        }
        let name = http::HeaderName::from_bytes(name)
            .map_err(|_| protocol("the peer sent a malformed trailer name"))?;
        let value = http::HeaderValue::from_bytes(value)
            .map_err(|_| protocol("the peer sent a malformed trailer value"))?;
        map.append(name, value);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A received header block, as `response_head` takes it.
    type Block = Vec<(Vec<u8>, Vec<u8>)>;

    fn fields(pairs: &[(&str, &str)]) -> Block {
        pairs
            .iter()
            .map(|(name, value)| (name.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect()
    }

    /// Peer input reaches this function unvalidated by anything of ours, so every
    /// malformed shape has to produce an error rather than a panic.
    #[test]
    fn malformed_response_heads_are_rejected_rather_than_panicking() {
        let cases: &[(&str, Block)] = &[
            (
                "no status at all",
                fields(&[("content-type", "text/plain")]),
            ),
            (
                "a second status",
                fields(&[(":status", "200"), (":status", "204")]),
            ),
            (
                "a pseudo-header that is not :status",
                fields(&[(":status", "200"), (":method", "GET")]),
            ),
            (
                "a field before :status",
                fields(&[("content-type", "text/plain"), (":status", "200")]),
            ),
            (
                "a status that is not a number",
                fields(&[(":status", "two hundred")]),
            ),
            ("a status out of range", fields(&[(":status", "9999")])),
            (
                "a field name with a space in it",
                fields(&[(":status", "200"), ("bad name", "value")]),
            ),
            (
                "a field value with a newline in it",
                fields(&[(":status", "200"), ("x-note", "one\ntwo")]),
            ),
            (
                "an empty field name",
                fields(&[(":status", "200"), ("", "value")]),
            ),
        ];

        for (description, block) in cases {
            let outcome = response_head(block);
            assert!(
                outcome.is_err(),
                "a response head with {description} was accepted",
            );
            assert_eq!(
                outcome.unwrap_err().kind(),
                ErrorKind::Protocol,
                "a response head with {description} reported the wrong kind",
            );
        }
    }

    #[test]
    fn a_well_formed_response_head_is_assembled() {
        let head = response_head(&fields(&[
            (":status", "204"),
            ("x-first", "one"),
            ("x-second", "two"),
        ]))
        .expect("a well-formed head");

        assert_eq!(head.status(), http::StatusCode::NO_CONTENT);
        assert_eq!(head.headers().get("x-first").unwrap(), "one");
        assert_eq!(head.headers().get("x-second").unwrap(), "two");
    }

    /// Trailers reach this function straight off the wire, so the same rule applies as to
    /// response heads: every malformed shape must produce an error rather than a panic.
    #[test]
    fn malformed_trailers_are_rejected_rather_than_panicking() {
        let cases: &[(&str, Block)] = &[
            (
                "a pseudo-header, which no trailing block may carry",
                fields(&[(":status", "200")]),
            ),
            ("a name with a space in it", fields(&[("bad name", "one")])),
            ("an empty name", fields(&[("", "one")])),
            (
                "a value with a newline in it",
                fields(&[("x-note", "one\ntwo")]),
            ),
        ];

        for (description, block) in cases {
            let outcome = trailers(block);
            assert!(
                outcome.is_err(),
                "a trailing block with {description} was accepted",
            );
            assert_eq!(
                outcome.unwrap_err().kind(),
                ErrorKind::Protocol,
                "a trailing block with {description} reported the wrong kind",
            );
        }
    }

    /// A request head is peer input reaching a routing decision, so every malformed shape
    /// must produce an error rather than a panic — and, more importantly, rather than a
    /// *plausible* request that says something the peer did not.
    #[test]
    fn malformed_request_heads_are_rejected_rather_than_panicking() {
        let base = [
            (":method", "GET"),
            (":scheme", "http"),
            (":authority", "example.test"),
            (":path", "/"),
        ];
        let with = |name: &str, value: &str| -> Block {
            let mut pairs: Vec<(&str, &str)> = base.to_vec();
            for pair in &mut pairs {
                if pair.0 == name {
                    pair.1 = value;
                }
            }
            fields(&pairs)
        };
        let without = |name: &str| -> Block {
            fields(
                &base
                    .iter()
                    .copied()
                    .filter(|(field, _)| *field != name)
                    .collect::<Vec<_>>(),
            )
        };

        let cases: &[(&str, Block)] = &[
            ("no method", without(":method")),
            ("no path", without(":path")),
            ("no authority", without(":authority")),
            // The attack this shape carries: an authority that swallows part of the path
            // makes the peer, not the server, decide what was requested.
            (
                "an authority containing a path",
                with(":authority", "example.test/admin"),
            ),
            (
                "userinfo in the authority",
                with(":authority", "user@example.test"),
            ),
            ("an empty authority", with(":authority", "")),
            ("an empty path", with(":path", "")),
            (
                "a path that is not a request target",
                with(":path", "no-slash"),
            ),
            ("the asterisk target", with(":path", "*")),
            (
                "a scheme this crate does not speak",
                with(":scheme", "https"),
            ),
            ("a method that is not a token", with(":method", "GE T")),
            ("CONNECT", with(":method", "CONNECT")),
            (
                "a duplicate pseudo-header",
                fields(&[
                    (":method", "GET"),
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":authority", "example.test"),
                    (":path", "/"),
                ]),
            ),
            (
                "a pseudo-header after a regular field",
                fields(&[
                    (":method", "GET"),
                    (":scheme", "http"),
                    (":authority", "example.test"),
                    ("x-note", "one"),
                    (":path", "/"),
                ]),
            ),
            (
                "a pseudo-header HTTP/2 does not define",
                fields(&[
                    (":method", "GET"),
                    (":scheme", "http"),
                    (":authority", "example.test"),
                    (":path", "/"),
                    (":invented", "value"),
                ]),
            ),
            (
                "extended CONNECT",
                fields(&[
                    (":method", "GET"),
                    (":scheme", "http"),
                    (":authority", "example.test"),
                    (":path", "/"),
                    (":protocol", "websocket"),
                ]),
            ),
            (
                "a field name with a space in it",
                fields(&[
                    (":method", "GET"),
                    (":scheme", "http"),
                    (":authority", "example.test"),
                    (":path", "/"),
                    ("bad name", "value"),
                ]),
            ),
        ];

        for (description, block) in cases {
            let outcome = request_head(block);
            assert!(
                outcome.is_err(),
                "a request head with {description} was accepted",
            );
            assert_eq!(
                outcome.unwrap_err().kind(),
                ErrorKind::Protocol,
                "a request head with {description} reported the wrong kind",
            );
        }
    }

    #[test]
    fn a_well_formed_request_head_is_reassembled() {
        let head = request_head(&fields(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", "example.test:8080"),
            (":path", "/things/7?q=1"),
            ("x-note", "one"),
        ]))
        .expect("a well-formed head");

        assert_eq!(head.method(), http::Method::POST);
        assert_eq!(head.uri().scheme_str(), Some("http"));
        assert_eq!(
            head.uri().authority().map(http::uri::Authority::as_str),
            Some("example.test:8080"),
        );
        assert_eq!(head.uri().path(), "/things/7");
        assert_eq!(head.uri().query(), Some("q=1"));
        assert_eq!(head.headers().get("x-note").unwrap(), "one");
        // The pseudo-headers are the request line, not fields, and must not appear twice.
        assert!(head.headers().get(":method").is_none());
    }

    #[test]
    fn a_request_head_may_omit_its_scheme() {
        // Cleartext only, so a missing scheme has exactly one reading.
        let head = request_head(&fields(&[
            (":method", "GET"),
            (":authority", "example.test"),
            (":path", "/"),
        ]))
        .expect("a well-formed head");
        assert_eq!(head.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn a_response_head_leads_with_its_status() {
        let response = http::Response::builder()
            .status(http::StatusCode::CREATED)
            .header("x-note", "one")
            .body(())
            .expect("a response");
        let (parts, ()) = response.into_parts();

        let headers = response_headers(&parts).expect("a valid head");
        let names: Vec<String> = headers
            .fields
            .iter()
            .map(|(name, _)| String::from_utf8_lossy(name).into_owned())
            .collect();

        assert_eq!(names, [":status", "x-note"]);
        assert_eq!(headers.fields[0].1, b"201");
    }

    #[test]
    fn a_connection_specific_response_field_is_rejected() {
        for name in ["connection", "transfer-encoding", "keep-alive", "upgrade"] {
            let response = http::Response::builder()
                .header(name, "whatever")
                .body(())
                .expect("a response");
            let (parts, ()) = response.into_parts();

            let outcome = response_headers(&parts);
            assert!(outcome.is_err(), "a `{name}` response field was accepted");
            assert_eq!(outcome.unwrap_err().kind(), ErrorKind::Protocol);
        }
    }

    #[test]
    fn a_trailing_block_is_encoded_as_ordinary_fields() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-checksum", http::HeaderValue::from_static("deadbeef"));
        trailers.append("x-note", http::HeaderValue::from_static("one"));
        trailers.append("x-note", http::HeaderValue::from_static("two"));

        let encoded = trailer_fields(&trailers).expect("a valid trailing block");
        let mut seen: Vec<(String, String)> = encoded
            .fields
            .iter()
            .map(|(name, value)| {
                (
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                )
            })
            .collect();
        seen.sort();

        assert_eq!(
            seen,
            [
                ("x-checksum".to_owned(), "deadbeef".to_owned()),
                ("x-note".to_owned(), "one".to_owned()),
                ("x-note".to_owned(), "two".to_owned()),
            ],
        );
    }

    #[test]
    fn a_connection_specific_trailer_is_rejected_rather_than_dropped() {
        // The same rule as request heads, from the trailing side. Silently discarding a
        // field a caller deliberately set would be worse than saying no.
        for name in ["connection", "transfer-encoding", "keep-alive", "upgrade"] {
            let mut trailers = http::HeaderMap::new();
            trailers.insert(
                http::HeaderName::from_bytes(name.as_bytes()).expect("a field name"),
                http::HeaderValue::from_static("whatever"),
            );

            let outcome = trailer_fields(&trailers);
            assert!(outcome.is_err(), "a `{name}` trailer was accepted");
            assert_eq!(outcome.unwrap_err().kind(), ErrorKind::Protocol);
        }
    }

    #[test]
    fn an_empty_trailing_block_encodes_to_nothing() {
        let encoded = trailer_fields(&http::HeaderMap::new()).expect("an empty block");
        assert!(encoded.views().is_empty());
    }

    #[test]
    fn a_repeated_trailer_keeps_every_value() {
        let map = trailers(&fields(&[
            ("x-note", "one"),
            ("x-checksum", "deadbeef"),
            ("x-note", "two"),
        ]))
        .expect("a well-formed trailing block");

        let notes: Vec<&[u8]> = map.get_all("x-note").iter().map(|v| v.as_bytes()).collect();
        assert_eq!(notes, [b"one".as_slice(), b"two".as_slice()]);
        assert_eq!(map.get("x-checksum").unwrap(), "deadbeef");
    }

    #[test]
    fn a_request_head_leads_with_its_pseudo_headers() {
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("http://example.test/path?query=1")
            .header("x-note", "value")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let headers = request_headers(&parts).expect("a valid head");
        let names: Vec<String> = headers
            .fields
            .iter()
            .map(|(name, _)| String::from_utf8_lossy(name).into_owned())
            .collect();

        assert_eq!(
            names,
            [":method", ":scheme", ":authority", ":path", "x-note"]
        );
        assert_eq!(headers.fields[3].1, b"/path?query=1");
    }

    #[test]
    fn a_host_field_stands_in_for_a_missing_authority() {
        let request = http::Request::builder()
            .uri("/relative")
            .header("host", "example.test")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let headers = request_headers(&parts).expect("a valid head");
        assert_eq!(headers.fields[2].1, b"example.test");
        // Carried once, as `:authority`, so the two cannot disagree on the wire.
        assert!(
            headers.fields.iter().all(|(name, _)| name != b"host"),
            "the host field was carried twice",
        );
    }
}
