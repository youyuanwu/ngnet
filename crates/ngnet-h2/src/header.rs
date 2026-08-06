//! Header fields for outgoing messages.
//!
//! Headers borrow their name and value. libnghttp2 copies both into its own storage
//! during submission, so they need only outlive the submitting call.

use ngnet_h2_sys as sys;

use crate::error::{Error, ErrorKind, Result};

/// One header field of an outgoing message.
///
/// Names must already be lowercase: HTTP/2 carries field names in lowercase and treats
/// an uppercase one as malformed. Pseudo-header names begin with `:` and must precede
/// every regular field in the set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header<'a> {
    name: &'a [u8],
    value: &'a [u8],
    sensitive: bool,
}

impl<'a> Header<'a> {
    /// A header field with the given name and value.
    pub const fn new(name: &'a str, value: &'a str) -> Self {
        Self {
            name: name.as_bytes(),
            value: value.as_bytes(),
            sensitive: false,
        }
    }

    /// A header field whose name and value are arbitrary octets.
    pub const fn from_bytes(name: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            name,
            value,
            sensitive: false,
        }
    }

    /// Marks this field as sensitive, so it is never added to the compression table.
    ///
    /// Use for credentials and anything else whose presence should not be inferable from
    /// compressed sizes.
    #[must_use]
    pub const fn sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    /// The field name.
    pub const fn name(&self) -> &'a [u8] {
        self.name
    }

    /// The field value.
    pub const fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Whether this field is marked sensitive.
    pub const fn is_sensitive(&self) -> bool {
        self.sensitive
    }

    fn flags(&self) -> u8 {
        if self.sensitive {
            sys::NGHTTP2_NV_FLAG_NO_INDEX as u8
        } else {
            sys::NGHTTP2_NV_FLAG_NONE as u8
        }
    }

    /// Builds the C representation.
    ///
    /// The pointers borrow this header. No `NO_COPY` flag is set, so libnghttp2 copies
    /// both name and value and the borrow need not outlive the submitting call. The
    /// `cast_mut` is required by the C struct's signature; libnghttp2 does not write
    /// through these pointers.
    pub(crate) fn as_nv(&self) -> sys::nghttp2_nv {
        sys::nghttp2_nv {
            name: self.name.as_ptr().cast_mut(),
            value: self.value.as_ptr().cast_mut(),
            namelen: self.name.len(),
            valuelen: self.value.len(),
            flags: self.flags(),
        }
    }
}

/// Whether `byte` is legal in an HTTP field name, per RFC 9110's `token` rule.
const fn is_token_byte(byte: u8) -> bool {
    matches!(byte,
        b'a'..=b'z'
        | b'0'..=b'9'
        | b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*'
        | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
    )
}

fn invalid(detail: &'static str) -> Error {
    Error::new("submit headers", ErrorKind::InvalidInput, detail)
}

/// Checks a header set before any of it reaches C.
///
/// Rejecting here rather than letting libnghttp2 discover the problem keeps the session
/// usable: a rejected set is never queued, so nothing partial is left behind.
pub(crate) fn validate(headers: &[Header<'_>]) -> Result<()> {
    if headers.is_empty() {
        return Err(invalid("a header set must contain at least one field"));
    }

    let mut seen_regular = false;

    for header in headers {
        let name = header.name;

        if name.is_empty() {
            return Err(invalid("a header name must not be empty"));
        }

        let is_pseudo = name[0] == b':';
        if is_pseudo && name.len() == 1 {
            return Err(invalid("a pseudo-header name must not be just a colon"));
        }

        // Pseudo-headers are `:` followed by a token; regular fields are a bare token.
        let token = if is_pseudo { &name[1..] } else { name };
        for &byte in token {
            if !is_token_byte(byte) {
                return Err(if byte.is_ascii_uppercase() {
                    invalid("header names must be lowercase in HTTP/2")
                } else {
                    invalid("a header name contains a character that is not permitted")
                });
            }
        }

        // Every pseudo-header must precede every regular field.
        if is_pseudo {
            if seen_regular {
                return Err(invalid("pseudo-headers must precede regular header fields"));
            }
        } else {
            seen_regular = true;
        }

        // RFC 9110 field-value: HTAB, SP, VCHAR and obs-text only. Every other control
        // character and DEL is forbidden, not merely NUL, CR and LF — libnghttp2 applies
        // the same rule, so accepting more here would only defer the rejection.
        for &byte in header.value {
            let permitted = matches!(byte, b'\t' | b' '..=b'~') && byte != 0x7f;
            let obs_text = byte >= 0x80;
            if !permitted && !obs_text {
                return Err(invalid(
                    "a header value may only contain visible characters, spaces and tabs",
                ));
            }
        }

        // Nested rather than a let-chain: those stabilised in 1.88, above this crate's
        // declared minimum.
        if let (Some(&first), Some(&last)) = (header.value.first(), header.value.last()) {
            if matches!(first, b' ' | b'\t') || matches!(last, b' ' | b'\t') {
                return Err(invalid(
                    "a header value must not begin or end with whitespace",
                ));
            }
        }
    }

    Ok(())
}

/// Validates a header set and converts it for submission.
pub(crate) fn to_nv_vec(headers: &[Header<'_>]) -> Result<Vec<sys::nghttp2_nv>> {
    validate(headers)?;
    Ok(headers.iter().map(Header::as_nv).collect())
}

/// Validates a trailer set, which may not contain pseudo-headers.
pub(crate) fn to_trailer_nv_vec(headers: &[Header<'_>]) -> Result<Vec<sys::nghttp2_nv>> {
    validate(headers)?;

    if headers.iter().any(|header| header.name.starts_with(b":")) {
        return Err(invalid("trailers must not contain pseudo-header fields"));
    }

    Ok(headers.iter().map(Header::as_nv).collect())
}
