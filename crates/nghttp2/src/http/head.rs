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
