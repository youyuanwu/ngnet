//! Translating between `http` message heads and HTTP/3 field sections.
//!
//! HTTP/3 carries no request line and no status line. Both are encoded as pseudo-header
//! fields that must precede every regular field in a field section, and the `http` crate
//! models them as structured parts instead — so something has to convert, and the
//! conversion is where the protocol's rules about field names live.
//!
//! # Where the rules come from
//!
//! RFC 9114 §4.3 defines HTTP/3's field-section semantics, and it very deliberately mirrors
//! RFC 9113 §8 for HTTP/2: the pseudo-headers are the same, they must come first, each at
//! most once, and the connection-specific field names are forbidden in both. That similarity
//! is a trap as much as a convenience. Two rules genuinely differ from this workspace's
//! HTTP/2 implementation, and both are called out where they are written:
//!
//! - **`https` is accepted.** `ngnet-h2` speaks cleartext h2c and rejects any scheme but
//!   `http`. HTTP/3 runs over QUIC, which is always secured, so `https` is the normal case
//!   and rejecting it would make this layer useless.
//! - **Informational responses are recognised.** They are legal in HTTP/3 and a client must
//!   not mistake one for the final response.
//!
//! Everything else — the smuggling check between `host` and `:authority`, the refusal of
//! userinfo, assembling the URI from validated components rather than by joining text — is
//! protocol-agnostic and carries over unchanged, because the attacks it prevents do too.
//!
//! # What this module does not do
//!
//! [`crate::Header::new`] already validates field name and value *syntax*: lowercase names,
//! no control characters, no leading or trailing whitespace, a colon only in position zero.
//! That check is not repeated here. This module owns pseudo-header *semantics* and the
//! mapping to and from `http` types, and nothing else.

use super::error::{Error, ErrorKind, Result};
use crate::Header;
use ngnet_h3_sys as sys;

const INLINE_NAME: usize = 32;
const INLINE_VALUE: usize = 128;

/// One received field with bounded inline storage and a heap fallback.
#[derive(Debug)]
pub(crate) struct ReceivedField {
    name: SmallBytes<INLINE_NAME>,
    value: SmallBytes<INLINE_VALUE>,
}

impl ReceivedField {
    pub(crate) fn new(name: &[u8], value: &[u8]) -> Self {
        Self {
            name: SmallBytes::new(name),
            value: SmallBytes::new(value),
        }
    }

    fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.value.as_ref()
    }
}

#[derive(Debug)]
enum SmallBytes<const N: usize> {
    Inline { len: u8, bytes: [u8; N] },
    Heap(Box<[u8]>),
}

impl<const N: usize> SmallBytes<N> {
    fn new(value: &[u8]) -> Self {
        if value.len() <= N {
            let mut bytes = [0; N];
            bytes[..value.len()].copy_from_slice(value);
            Self::Inline {
                len: value.len() as u8,
                bytes,
            }
        } else {
            Self::Heap(value.into())
        }
    }
}

impl<const N: usize> AsRef<[u8]> for SmallBytes<N> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..usize::from(*len)],
            Self::Heap(bytes) => bytes,
        }
    }
}

trait FieldRef {
    fn parts(&self) -> (&[u8], &[u8]);
}

impl FieldRef for ReceivedField {
    fn parts(&self) -> (&[u8], &[u8]) {
        (self.name(), self.value())
    }
}

impl FieldRef for (Vec<u8>, Vec<u8>) {
    fn parts(&self) -> (&[u8], &[u8]) {
        (&self.0, &self.1)
    }
}

/// Field names HTTP/3 forbids outright.
///
/// These are connection-specific in HTTP/1.1 and have no meaning once each request has its
/// own QUIC stream. RFC 9114 §4.2 makes a message carrying one malformed, so they are
/// rejected here rather than being quietly dropped — silently discarding a header a caller
/// deliberately set would be worse than saying no.
const FORBIDDEN: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
];

/// A field section that owns its octets.
///
/// [`Header`] borrows, and nghttp3 copies field data at submission, so the octets only have
/// to live across the submitting call — but they have to live *somewhere*, and the `http`
/// types they come from are consumed on the way. This is that somewhere.
#[derive(Debug, Default)]
pub(crate) struct OwnedFields {
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

impl OwnedFields {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.fields.push((name.into(), value.into()));
    }

    /// Borrowed views suitable for submission.
    ///
    /// Fallible because [`Header::new`] validates, and a field that fails validation must be
    /// refused before anything reaches the wire rather than after.
    pub(crate) fn views(&self) -> Result<Vec<Header<'_>>> {
        self.fields
            .iter()
            .map(|(name, value)| {
                Header::new(name.as_slice(), value.as_slice()).map_err(|error| {
                    Error::with_source(
                        ErrorKind::Protocol,
                        "this head is not a valid HTTP/3 field section",
                        error,
                    )
                })
            })
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (name, value) in &self.fields {
            Header::new(name, value).map_err(|error| {
                Error::with_source(
                    ErrorKind::Protocol,
                    "this head is not a valid HTTP/3 field section",
                    error,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn nva(&self) -> Result<Vec<sys::nghttp3_nv>> {
        self.fields
            .iter()
            .map(|(name, value)| {
                Header::new(name, value)
                    .map(|field| field.as_nv())
                    .map_err(|error| {
                        Error::with_source(
                            ErrorKind::Protocol,
                            "this head is not a valid HTTP/3 field section",
                            error,
                        )
                    })
            })
            .collect()
    }

    /// How many fields there are.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.fields.len()
    }
}

fn protocol(detail: &'static str) -> Error {
    Error::new(ErrorKind::Protocol, detail)
}

/// Whether a `te` field is one HTTP/3 permits.
///
/// RFC 9114 §4.2 keeps `te` alive where the other connection-specific fields die, but only
/// in a request and only with the single value `trailers`. Anything else is a hop-by-hop
/// instruction that means nothing once each exchange owns a QUIC stream.
fn te_is_permitted(value: &[u8]) -> bool {
    value.eq_ignore_ascii_case(b"trailers")
}

/// Whether a scheme is one this layer will carry.
///
/// Both, and this is the first place HTTP/3 diverges from the HTTP/2 crate beside it. That
/// one is h2c-only and hard-codes `http`; HTTP/3 runs over QUIC, which is always secured, so
/// `https` is not merely allowed but usual.
fn scheme_is_supported(scheme: &[u8]) -> bool {
    scheme == b"http" || scheme == b"https"
}

/// Encodes a request head as an HTTP/3 field section.
///
/// The four request pseudo-headers come first, in the order RFC 9114 lists them. Their order
/// among themselves is not required by the protocol, but keeping it fixed makes the output
/// reproducible, which the tests rely on.
pub(crate) fn request_fields(parts: &http::request::Parts) -> Result<OwnedFields> {
    if parts.method == http::Method::CONNECT {
        return Err(protocol(
            "CONNECT is not supported: this crate speaks HTTP/3 request/response only",
        ));
    }

    let mut fields = OwnedFields::with_capacity(parts.headers.len() + 4);
    fields.push(":method", parts.method.as_str());

    // Unlike the HTTP/2 crate beside this one, both schemes are carried. The default when
    // the URI names none is `https`, because a connection that reached here came over QUIC.
    let scheme = parts.uri.scheme_str().unwrap_or("https");
    if !scheme_is_supported(scheme.as_bytes()) {
        return Err(protocol(
            "a request scheme must be http or https: this crate carries no others",
        ));
    }
    fields.push(":scheme", scheme);

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
    fields.push(":authority", authority);

    let path = parts
        .uri
        .path_and_query()
        .map_or("/", http::uri::PathAndQuery::as_str);
    fields.push(":path", path);

    for (name, value) in &parts.headers {
        // `http::HeaderName` is lowercase by construction, so the case rule HTTP/3 imposes
        // is already satisfied and does not need re-checking here.
        let name = name.as_str();
        if name == "host" {
            // Already carried as `:authority`; sending both invites disagreement, and the
            // decoder below treats disagreement as an attack.
            continue;
        }
        if name == "te" {
            if !te_is_permitted(value.as_bytes()) {
                return Err(protocol(
                    "HTTP/3 permits `te` only with the value `trailers`",
                ));
            }
        } else if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/3 forbids it",
            ));
        }
        fields.push(name, value.as_bytes());
    }

    // Validated before anything is submitted, so a rejected head never half-touches the
    // connection and the error names the caller's mistake rather than a native return code.
    fields.validate()?;
    Ok(fields)
}

/// Encodes a response head as an HTTP/3 field section.
///
/// A response carries exactly one pseudo-header, `:status`, and it must come first.
pub(crate) fn response_fields(parts: &http::response::Parts) -> Result<OwnedFields> {
    let mut fields = OwnedFields::with_capacity(parts.headers.len() + 1);
    fields.push(":status", parts.status.as_str());

    for (name, value) in &parts.headers {
        let name = name.as_str();
        if name == "te" {
            // Permitted in a request, never in a response.
            return Err(protocol("HTTP/3 forbids `te` in a response"));
        }
        if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/3 forbids it",
            ));
        }
        fields.push(name, value.as_bytes());
    }

    fields.validate()?;
    Ok(fields)
}

/// Decodes a received request field section.
///
/// HTTP/3 splits the request line across four pseudo-headers; `http::Request` has a method
/// and a URI. Reassembling the URI from `:scheme`, `:authority` and `:path` is what this
/// does, and rejecting a head that cannot make one is the other half of it — a request
/// missing a pseudo-header is malformed, not merely inconvenient.
pub(crate) fn request_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Request<()>> {
    request_head_from(fields)
}

pub(crate) fn received_request_head(fields: &[ReceivedField]) -> Result<http::Request<()>> {
    request_head_from(fields)
}

fn request_head_from<F: FieldRef>(fields: &[F]) -> Result<http::Request<()>> {
    let mut builder = http::Request::builder();
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut host = None;
    let mut seen_field = false;

    for field in fields {
        let (name, value) = field.parts();
        if name.first() == Some(&b':') {
            if seen_field {
                return Err(protocol(
                    "a request must send its pseudo-headers before any other field",
                ));
            }
            let slot = match name {
                b":method" => &mut method,
                b":scheme" => &mut scheme,
                b":authority" => &mut authority,
                b":path" => &mut path,
                b":protocol" => {
                    return Err(protocol(
                        "extended CONNECT is not supported: this crate speaks HTTP/3 \
                         request/response only",
                    ));
                }
                _ => {
                    return Err(protocol(
                        "the peer sent a pseudo-header HTTP/3 does not define",
                    ));
                }
            };
            if slot.is_some() {
                return Err(protocol(
                    "a request carries each pseudo-header at most once",
                ));
            }
            *slot = Some(value.to_vec());
            continue;
        }

        seen_field = true;
        // A regular `host` field says the same thing as `:authority`, and nothing below
        // keeps the two honest. Authority is a trust-boundary input — routing, tenant
        // checks, absolute-URL and cache-key generation all read it — so a `host` that
        // contradicts `:authority` is a smuggling attempt and the request is refused. A
        // `host` that agrees is dropped as redundant rather than delivered beside the
        // authority it merely repeats; the encoder above drops `host` for the same reason.
        // This rule is protocol-agnostic: RFC 9114 inherits it from HTTP semantics, not
        // from anything HTTP/2-specific.
        if name.eq_ignore_ascii_case(b"host") {
            host = Some(value.to_vec());
            continue;
        }
        let name = http::HeaderName::from_bytes(name)
            .map_err(|_| protocol("the peer sent a malformed field name"))?;
        if name.as_str() == "te" {
            if !te_is_permitted(value) {
                return Err(protocol(
                    "the peer sent a `te` field with a value HTTP/3 does not permit",
                ));
            }
        } else if FORBIDDEN.contains(&name.as_str()) {
            // Refused on the way in as well as on the way out. The peer is not running this
            // code, and RFC 9114 §4.2 makes a message carrying one of these malformed —
            // delivering it would hand a handler a framing instruction from an untrusted
            // source, which is the shape of a request-smuggling bug.
            return Err(protocol(
                "the peer sent a connection-specific field HTTP/3 forbids",
            ));
        }
        let value = http::HeaderValue::from_bytes(value)
            .map_err(|_| protocol("the peer sent a malformed field value"))?;
        builder = builder.header(name, value);
    }

    let method = method.ok_or_else(|| protocol("a request must carry :method"))?;
    let path = path.ok_or_else(|| protocol("a request must carry :path"))?;

    // RFC 9114 §4.3.1 lets a request carry its authority as `:authority` *or* as a `Host`
    // field, so requiring the pseudo-header would refuse conforming peers. What is not
    // negotiable is that the two agree when both are present: authority is a trust-boundary
    // input, and two sources that disagree is a smuggling attempt rather than a preference.
    let authority = match (&authority, &host) {
        (Some(authority), Some(host)) if authority != host => {
            return Err(protocol(
                "the peer sent a host field that disagrees with :authority",
            ));
        }
        (Some(authority), _) => authority.clone(),
        (None, Some(host)) if !host.is_empty() => host.clone(),
        _ => {
            return Err(protocol(
                "a request must name its authority, as :authority or as a host field",
            ));
        }
    };

    // A missing scheme reads as `https` here, where the HTTP/2 crate beside this one reads
    // it as `http`: this connection arrived over QUIC, which is secured by construction.
    let scheme = scheme.unwrap_or_else(|| b"https".to_vec());
    if !scheme_is_supported(&scheme) {
        return Err(protocol(
            "the peer sent a :scheme this crate does not carry",
        ));
    }

    let method = http::Method::from_bytes(&method)
        .map_err(|_| protocol("the peer sent a method that is not a token"))?;
    if method == http::Method::CONNECT {
        return Err(protocol(
            "CONNECT is not supported: this crate speaks HTTP/3 request/response only",
        ));
    }

    // Assembled from validated components rather than by joining text. Concatenating
    // `scheme://authority` and `path` would let an authority containing a slash claim part
    // of the path — `evil.test/admin` with a path of `/x` reads back as authority
    // `evil.test` and path `/admin/x` — which is a routing decision made by the peer rather
    // than by the server. Parsing each piece on its own is what enforces it.
    let authority = http::uri::Authority::try_from(authority.as_slice())
        .map_err(|_| protocol("the peer sent an :authority that is not a valid authority"))?;
    if authority.as_str().contains('@') {
        return Err(protocol("userinfo is forbidden in :authority"));
    }

    let path_and_query = http::uri::PathAndQuery::try_from(path.as_slice())
        .map_err(|_| protocol("the peer sent a :path that is not a valid request target"))?;
    // The asterisk form is legal, but only for OPTIONS, and this crate does not serve it.
    // Accepting it elsewhere would hand a handler a path it cannot route on.
    if path_and_query.path() == "*" {
        return Err(protocol(
            "the asterisk request target is not supported: this crate speaks HTTP/3 \
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

/// Decodes a received response field section.
///
/// The section arrives as a flat list because that is how it is delivered field by field;
/// `:status` is extracted and everything else becomes an ordinary field.
pub(crate) fn response_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Response<()>> {
    response_head_from(fields)
}

pub(crate) fn received_response_head(fields: &[ReceivedField]) -> Result<http::Response<()>> {
    response_head_from(fields)
}

fn response_head_from<F: FieldRef>(fields: &[F]) -> Result<http::Response<()>> {
    let mut builder = http::Response::builder();
    let mut status = None;

    for field in fields {
        let (name, value) = field.parts();
        if name.first() == Some(&b':') {
            if name != b":status" {
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
        if name.as_str() == "te" {
            return Err(protocol(
                "the peer sent `te` in a response, which HTTP/3 forbids",
            ));
        }
        if FORBIDDEN.contains(&name.as_str()) {
            return Err(protocol(
                "the peer sent a connection-specific field HTTP/3 forbids",
            ));
        }
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

/// Whether a status code is informational, and so does not settle an exchange.
///
/// A 1xx response is legal in HTTP/3 and is followed by the real one. A client that treated
/// it as final would resolve the caller's future with a response that has no body coming and
/// then be handed a second head on a stream it thought was finished.
pub(crate) fn is_informational(status: http::StatusCode) -> bool {
    status.is_informational()
}

/// Encodes a trailing field section for submission.
///
/// The same rule as decoding, from the other side: trailers carry ordinary fields only, and
/// the connection-specific names HTTP/3 forbids are forbidden here too. A caller who set one
/// is told rather than having it quietly dropped.
pub(crate) fn trailer_fields(trailers: &http::HeaderMap) -> Result<OwnedFields> {
    let mut fields = OwnedFields::with_capacity(trailers.len());

    for (name, value) in trailers {
        let name = name.as_str();
        if FORBIDDEN.contains(&name) {
            return Err(protocol(
                "this field is connection-specific and HTTP/3 forbids it in trailers",
            ));
        }
        fields.push(name, value.as_bytes());
    }

    fields.validate()?;
    Ok(fields)
}

/// Decodes a received trailing field section.
///
/// Trailers are ordinary fields only: a pseudo-header after a message has begun is
/// malformed, and RFC 9114 §4.3 says so explicitly.
pub(crate) fn trailers(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::HeaderMap> {
    trailers_from(fields)
}

pub(crate) fn received_trailers(fields: &[ReceivedField]) -> Result<http::HeaderMap> {
    trailers_from(fields)
}

fn trailers_from<F: FieldRef>(fields: &[F]) -> Result<http::HeaderMap> {
    let mut map = http::HeaderMap::with_capacity(fields.len());

    for field in fields {
        let (name, value) = field.parts();
        if name.first() == Some(&b':') {
            return Err(protocol(
                "a trailing field section carries no pseudo-headers",
            ));
        }
        let name = http::HeaderName::from_bytes(name)
            .map_err(|_| protocol("the peer sent a malformed trailer name"))?;
        if FORBIDDEN.contains(&name.as_str()) {
            return Err(protocol(
                "the peer sent a connection-specific field HTTP/3 forbids in trailers",
            ));
        }
        let value = http::HeaderValue::from_bytes(value)
            .map_err(|_| protocol("the peer sent a malformed trailer value"))?;
        map.append(name, value);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a field list the way a decoder receives one.
    pub(super) fn fields(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
        pairs
            .iter()
            .map(|(name, value)| (name.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect()
    }

    /// A minimal well-formed request, which each rejection test then spoils in one way.
    fn a_request() -> Vec<(Vec<u8>, Vec<u8>)> {
        fields(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.test"),
            (":path", "/"),
        ])
    }

    fn encoded(fields: &OwnedFields) -> Vec<(String, String)> {
        fields
            .fields
            .iter()
            .map(|(name, value)| {
                (
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                )
            })
            .collect()
    }

    // ---------------------------------------------------------------- outgoing requests

    #[test]
    fn a_request_head_is_encoded_with_its_pseudo_headers_first() {
        let request = http::Request::builder()
            .method("POST")
            .uri("https://example.test/things?q=1")
            .header("accept", "text/plain")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = request_fields(&parts).expect("encoding");
        assert_eq!(
            encoded(&out),
            vec![
                (":method".into(), "POST".into()),
                (":scheme".into(), "https".into()),
                (":authority".into(), "example.test".into()),
                (":path".into(), "/things?q=1".into()),
                ("accept".into(), "text/plain".into()),
            ]
        );
    }

    #[test]
    fn an_https_request_is_carried() {
        // The first of the two rules that differ from the HTTP/2 crate beside this one,
        // which is h2c-only and rejects anything but `http`. HTTP/3 runs over QUIC.
        let request = http::Request::builder()
            .uri("https://example.test/")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = request_fields(&parts).expect("https must be carried");
        assert!(encoded(&out).contains(&(":scheme".into(), "https".into())));
    }

    #[test]
    fn a_cleartext_request_is_also_carried() {
        let request = http::Request::builder()
            .uri("http://example.test/")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = request_fields(&parts).expect("http must be carried");
        assert!(encoded(&out).contains(&(":scheme".into(), "http".into())));
    }

    #[test]
    fn a_request_with_no_scheme_defaults_to_https() {
        // Diverges from the HTTP/2 crate deliberately: this connection arrived over QUIC.
        let request = http::Request::builder()
            .uri("/just-a-path")
            .header("host", "example.test")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = request_fields(&parts).expect("encoding");
        assert!(encoded(&out).contains(&(":scheme".into(), "https".into())));
    }

    #[test]
    fn a_request_scheme_this_crate_does_not_carry_is_refused() {
        let request = http::Request::builder()
            .uri("ftp://example.test/")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        assert!(request_fields(&parts).is_err());
    }

    #[test]
    fn a_request_authority_falls_back_to_the_host_field() {
        let request = http::Request::builder()
            .uri("/path")
            .header("host", "example.test")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = encoded(&request_fields(&parts).expect("encoding"));
        assert!(out.contains(&(":authority".into(), "example.test".into())));
        // Carried once, as the authority, not twice.
        assert!(!out.iter().any(|(name, _)| name == "host"));
    }

    #[test]
    fn a_request_with_no_authority_at_all_is_refused() {
        let request = http::Request::builder()
            .uri("/path")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        assert!(request_fields(&parts).is_err());
    }

    #[test]
    fn a_request_with_no_path_is_encoded_as_root() {
        let request = http::Request::builder()
            .uri("https://example.test")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        let out = encoded(&request_fields(&parts).expect("encoding"));
        assert!(out.contains(&(":path".into(), "/".into())));
    }

    #[test]
    fn an_outgoing_connect_request_is_refused() {
        let request = http::Request::builder()
            .method("CONNECT")
            .uri("https://example.test/")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();

        assert!(request_fields(&parts).is_err());
    }

    #[test]
    fn every_connection_specific_field_is_refused_on_a_request() {
        // Rejected rather than dropped: silently discarding a header a caller deliberately
        // set would hide the mistake rather than report it.
        for forbidden in FORBIDDEN {
            let request = http::Request::builder()
                .uri("https://example.test/")
                .header(*forbidden, "whatever")
                .body(())
                .expect("a request");
            let (parts, ()) = request.into_parts();

            assert!(
                request_fields(&parts).is_err(),
                "{forbidden} should have been refused"
            );
        }
    }

    // --------------------------------------------------------------- outgoing responses

    #[test]
    fn a_response_head_is_encoded_with_status_first() {
        let response = http::Response::builder()
            .status(204)
            .header("x-trace", "abc")
            .body(())
            .expect("a response");
        let (parts, ()) = response.into_parts();

        let out = encoded(&response_fields(&parts).expect("encoding"));
        assert_eq!(out[0], (":status".into(), "204".into()));
        assert!(out.contains(&("x-trace".into(), "abc".into())));
    }

    #[test]
    fn every_connection_specific_field_is_refused_on_a_response() {
        for forbidden in FORBIDDEN {
            let response = http::Response::builder()
                .header(*forbidden, "whatever")
                .body(())
                .expect("a response");
            let (parts, ()) = response.into_parts();

            assert!(
                response_fields(&parts).is_err(),
                "{forbidden} should have been refused"
            );
        }
    }

    // ---------------------------------------------------------------- incoming requests

    #[test]
    fn a_well_formed_request_head_is_reassembled() {
        let head = request_head(&fields(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.test"),
            (":path", "/things?q=1"),
            ("accept", "text/plain"),
        ]))
        .expect("decoding");

        assert_eq!(head.method(), http::Method::GET);
        assert_eq!(head.uri().scheme_str(), Some("https"));
        assert_eq!(
            head.uri().authority().map(|a| a.as_str()),
            Some("example.test")
        );
        assert_eq!(head.uri().path(), "/things");
        assert_eq!(head.uri().query(), Some("q=1"));
        assert_eq!(head.headers().get("accept").unwrap(), "text/plain");
    }

    #[test]
    fn received_fields_keep_small_values_inline_and_large_values_owned() {
        let small = ReceivedField::new(b"x-short", b"value");
        assert!(matches!(small.name, SmallBytes::Inline { .. }));
        assert!(matches!(small.value, SmallBytes::Inline { .. }));

        let large_value = vec![b'x'; INLINE_VALUE + 1];
        let large = ReceivedField::new(b"x-large", &large_value);
        assert!(matches!(large.value, SmallBytes::Heap(_)));
        assert_eq!(large.value(), large_value);
    }

    #[test]
    fn an_incoming_request_may_omit_its_scheme_and_reads_as_https() {
        let head = request_head(&fields(&[
            (":method", "GET"),
            (":authority", "example.test"),
            (":path", "/"),
        ]))
        .expect("decoding");
        assert_eq!(head.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn a_pseudo_header_after_a_regular_field_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.test"),
                ("accept", "text/plain"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn a_repeated_pseudo_header_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":method", "POST"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn an_unknown_pseudo_header_is_refused() {
        let mut spoiled = a_request();
        spoiled.push((b":invented".to_vec(), b"x".to_vec()));
        // Placed before any regular field, so only its name can be the objection.
        assert!(request_head(&spoiled).is_err());
    }

    #[test]
    fn the_protocol_pseudo_header_is_refused() {
        // Extended CONNECT, which this crate does not serve. Named separately from the
        // unknown-pseudo-header case because it is a real pseudo-header being declined
        // rather than a nonsense one being rejected.
        let mut spoiled = a_request();
        spoiled.push((b":protocol".to_vec(), b"websocket".to_vec()));
        assert!(request_head(&spoiled).is_err());
    }

    #[test]
    fn a_request_missing_each_required_pseudo_header_is_refused() {
        for omitted in [":method", ":authority", ":path"] {
            let kept: Vec<(Vec<u8>, Vec<u8>)> = a_request()
                .into_iter()
                .filter(|(name, _)| name != omitted.as_bytes())
                .collect();
            assert!(
                request_head(&kept).is_err(),
                "a request without {omitted} should have been refused"
            );
        }
    }

    #[test]
    fn a_host_field_disagreeing_with_authority_is_refused() {
        // The smuggling case. Authority is a trust-boundary input, so two sources that
        // disagree is a request refused rather than a preference expressed.
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "/"),
                ("host", "evil.test"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn a_host_field_agreeing_with_authority_is_dropped_not_delivered() {
        let head = request_head(&fields(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.test"),
            (":path", "/"),
            ("host", "example.test"),
        ]))
        .expect("decoding");
        assert!(
            head.headers().get("host").is_none(),
            "host repeats the authority and should not be delivered beside it"
        );
    }

    #[test]
    fn userinfo_in_the_authority_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "user@example.test"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn an_authority_that_would_claim_part_of_the_path_is_refused() {
        // `evil.test/admin` with a path of `/x` would read back as authority `evil.test`
        // and path `/admin/x` if the URI were assembled by joining text. It is not.
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "evil.test/admin"),
                (":path", "/x"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn the_asterisk_request_target_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "OPTIONS"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "*"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn an_incoming_connect_request_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "CONNECT"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn an_incoming_request_scheme_this_crate_does_not_carry_is_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "ftp"),
                (":authority", "example.test"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn malformed_request_heads_are_rejected_rather_than_panicking() {
        // Every one of these is peer-controlled, so a panic here would be a remote abort:
        // these decoders run inside handlers the state machine calls from a C frame.
        let spoiled: &[&[(&str, &str)]] = &[
            &[
                (":method", "GET\n"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", ""),
                (":path", "/"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", ""),
            ],
            &[
                (":method", ""),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
                ("bad name", "x"),
            ],
        ];
        for case in spoiled {
            assert!(
                request_head(&fields(case)).is_err(),
                "expected {case:?} to be refused"
            );
        }
    }

    // --------------------------------------------------------------- incoming responses

    #[test]
    fn a_well_formed_response_head_is_assembled() {
        let head =
            response_head(&fields(&[(":status", "200"), ("x-trace", "abc")])).expect("decoding");
        assert_eq!(head.status(), http::StatusCode::OK);
        assert_eq!(head.headers().get("x-trace").unwrap(), "abc");
    }

    #[test]
    fn a_response_with_no_status_is_refused() {
        assert!(response_head(&fields(&[("x-trace", "abc")])).is_err());
    }

    #[test]
    fn a_response_with_a_second_status_is_refused() {
        assert!(response_head(&fields(&[(":status", "200"), (":status", "204")])).is_err());
    }

    #[test]
    fn a_response_pseudo_header_other_than_status_is_refused() {
        assert!(response_head(&fields(&[(":status", "200"), (":method", "GET")])).is_err());
    }

    #[test]
    fn a_response_field_before_its_status_is_refused() {
        assert!(response_head(&fields(&[("x-trace", "abc"), (":status", "200")])).is_err());
    }

    #[test]
    fn an_informational_response_is_recognised_as_not_final() {
        // The second rule that differs from the HTTP/2 crate: a client must not treat a 1xx
        // as the response, or it resolves the caller's future and is then handed a second
        // head on a stream it thought was finished.
        let head = response_head(&fields(&[(":status", "103")])).expect("decoding");
        assert!(is_informational(head.status()));

        let final_head = response_head(&fields(&[(":status", "200")])).expect("decoding");
        assert!(!is_informational(final_head.status()));
    }

    #[test]
    fn malformed_response_heads_are_rejected_rather_than_panicking() {
        let spoiled: &[&[(&str, &str)]] = &[
            &[(":status", "")],
            &[(":status", "not-a-number")],
            &[(":status", "99")],
            &[(":status", "1000")],
            &[(":status", "200"), ("bad name", "x")],
            &[(":status", "200"), ("x-trace", "bad\nvalue")],
        ];
        for case in spoiled {
            assert!(
                response_head(&fields(case)).is_err(),
                "expected {case:?} to be refused"
            );
        }
    }

    // ------------------------------------------------------------------------- trailers

    #[test]
    fn trailers_round_trip_in_both_directions() {
        let mut map = http::HeaderMap::new();
        map.insert("x-checksum", "abc123".parse().expect("a value"));

        let encoded_out = trailer_fields(&map).expect("encoding");
        assert_eq!(encoded_out.len(), 1);

        let decoded = trailers(&fields(&[("x-checksum", "abc123")])).expect("decoding");
        assert_eq!(decoded.get("x-checksum").unwrap(), "abc123");
    }

    #[test]
    fn a_pseudo_header_in_incoming_trailers_is_refused() {
        assert!(trailers(&fields(&[(":status", "200")])).is_err());
    }

    #[test]
    fn every_connection_specific_field_is_refused_in_outgoing_trailers() {
        for forbidden in FORBIDDEN {
            let mut map = http::HeaderMap::new();
            map.insert(
                http::HeaderName::from_bytes(forbidden.as_bytes()).expect("a name"),
                "whatever".parse().expect("a value"),
            );
            assert!(
                trailer_fields(&map).is_err(),
                "{forbidden} should have been refused in trailers"
            );
        }
    }

    #[test]
    fn malformed_trailers_are_rejected_rather_than_panicking() {
        let spoiled: &[&[(&str, &str)]] = &[
            &[("bad name", "x")],
            &[("x-ok", "bad\nvalue")],
            &[("", "x")],
        ];
        for case in spoiled {
            assert!(
                trailers(&fields(case)).is_err(),
                "expected {case:?} to be refused"
            );
        }
    }
}

#[cfg(test)]
mod rfc_9114_conformance {
    use super::tests::fields;
    use super::*;

    #[test]
    fn te_trailers_is_permitted_on_a_request() {
        // The one exception RFC 9114 §4.2 makes among the connection-specific names.
        let request = http::Request::builder()
            .uri("https://example.test/")
            .header("te", "trailers")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();
        assert!(request_fields(&parts).is_ok());

        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "/"),
                ("te", "trailers"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn any_other_te_value_is_refused() {
        // `te: gzip` is a hop-by-hop instruction that means nothing once each exchange owns
        // a QUIC stream, and RFC 9114 makes a message carrying one malformed.
        let request = http::Request::builder()
            .uri("https://example.test/")
            .header("te", "gzip")
            .body(())
            .expect("a request");
        let (parts, ()) = request.into_parts();
        assert!(request_fields(&parts).is_err());

        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.test"),
                (":path", "/"),
                ("te", "gzip"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn te_is_refused_in_a_response_whatever_its_value() {
        let response = http::Response::builder()
            .header("te", "trailers")
            .body(())
            .expect("a response");
        let (parts, ()) = response.into_parts();
        assert!(response_fields(&parts).is_err());

        assert!(response_head(&fields(&[(":status", "200"), ("te", "trailers")])).is_err());
    }

    #[test]
    fn a_request_may_name_its_authority_with_host_instead() {
        // RFC 9114 §4.3.1 permits either. Requiring the pseudo-header would refuse a
        // conforming peer.
        let head = request_head(&fields(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("host", "example.test"),
        ]))
        .expect("host names the authority");
        assert_eq!(
            head.uri().authority().map(|a| a.as_str()),
            Some("example.test")
        );
        assert!(
            head.headers().get("host").is_none(),
            "host was carried as the authority, so it is not delivered beside it"
        );
    }

    #[test]
    fn a_request_naming_no_authority_at_all_is_still_refused() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn an_empty_host_does_not_count_as_an_authority() {
        assert!(
            request_head(&fields(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/"),
                ("host", ""),
            ]))
            .is_err()
        );
    }
}
