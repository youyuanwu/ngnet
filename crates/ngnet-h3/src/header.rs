//! HTTP field names and values.

use core::fmt;

use ngnet_h3_sys as sys;

use crate::error::{Error, Result};

/// One HTTP field, borrowed from the caller.
///
/// Borrowed rather than owned because submission copies: nghttp3's no-copy flags exist,
/// but they would oblige the caller to keep the buffers alive until the library was
/// finished with them, in exchange for saving a copy of a header name. That is a poor
/// trade, so this crate lets nghttp3 copy and the borrow ends when the call returns.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Header<'a> {
    name: &'a [u8],
    value: &'a [u8],
    sensitive: bool,
}

impl<'a> Header<'a> {
    /// Builds a field, validating the name.
    ///
    /// HTTP/3 field names must be lowercase and non-empty, and may not contain the
    /// characters that would let a peer smuggle a second field into one
    /// (RFC 9114 §4.1.2). Rejecting them here means a malformed field is a typed error
    /// rather than something written to the wire and then rejected by the peer.
    pub fn new(
        name: &'a (impl AsRef<[u8]> + ?Sized),
        value: &'a (impl AsRef<[u8]> + ?Sized),
    ) -> Result<Self> {
        let name = name.as_ref();
        let value = value.as_ref();
        validate_name(name)?;
        validate_value(value)?;
        Ok(Self {
            name,
            value,
            sensitive: false,
        })
    }

    /// Marks the field as one that must never be added to a QPACK index.
    ///
    /// For fields whose value would leak something if an attacker could observe its
    /// compression behaviour — an authorization token, most obviously.
    pub fn sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    /// The field name.
    pub fn name(&self) -> &'a [u8] {
        self.name
    }

    /// The field value.
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Whether the field is marked as never indexable.
    pub fn is_sensitive(&self) -> bool {
        self.sensitive
    }

    /// The raw struct to hand to nghttp3.
    ///
    /// Borrows this header; the pointers are valid only while it is alive. No no-copy flag
    /// is set, so nghttp3 copies both buffers during the submitting call.
    pub(crate) fn as_nv(&self) -> sys::nghttp3_nv {
        let flags = if self.sensitive {
            sys::NGHTTP3_NV_FLAG_NEVER_INDEX
        } else {
            sys::NGHTTP3_NV_FLAG_NONE
        };
        sys::nghttp3_nv {
            name: self.name.as_ptr(),
            value: self.value.as_ptr(),
            namelen: self.name.len(),
            valuelen: self.value.len(),
            flags: flags as u8,
        }
    }
}

fn validate_name(name: &[u8]) -> Result<()> {
    if name.is_empty() {
        return Err(Error::invalid_input("a field name cannot be empty"));
    }

    // A pseudo-header's leading colon is legal only in that position.
    let body = match name.first() {
        Some(b':') => &name[1..],
        _ => name,
    };
    if body.is_empty() {
        return Err(Error::invalid_input(
            "a pseudo-header name needs more than its colon",
        ));
    }

    for &byte in body {
        let ok = byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        if !ok {
            return Err(if byte.is_ascii_uppercase() {
                Error::invalid_input("HTTP/3 field names must be lowercase")
            } else {
                Error::invalid_input("that byte is not allowed in an HTTP/3 field name")
            });
        }
    }
    Ok(())
}

fn validate_value(value: &[u8]) -> Result<()> {
    // Matches what nghttp3 itself accepts on receipt: HTAB, SP, printable ASCII and the
    // high range. Rejecting only NUL/CR/LF would let this crate submit a value that every
    // nghttp3 peer then rejects as malformed, which is exactly the outcome validating here
    // is supposed to prevent.
    for &byte in value {
        let ok = byte == b'\t' || byte == b' ' || (0x21..=0x7e).contains(&byte) || byte >= 0x80;
        if !ok {
            return Err(match byte {
                0x00 => Error::invalid_input("a field value cannot contain NUL"),
                b'\r' | b'\n' => Error::invalid_input(
                    "a field value cannot contain CR or LF, which would let it inject a \
                     second field",
                ),
                _ => Error::invalid_input("that control byte is not allowed in a field value"),
            });
        }
    }

    // RFC 9114 4.1.2: a value may not begin or end with whitespace, because the framing
    // that would preserve it does not survive a round trip through HTTP/1.1.
    if matches!(value.first(), Some(b' ' | b'\t')) || matches!(value.last(), Some(b' ' | b'\t')) {
        return Err(Error::invalid_input(
            "a field value cannot begin or end with a space or tab",
        ));
    }
    Ok(())
}

impl fmt::Debug for Header<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = String::from_utf8_lossy(self.name);
        if self.sensitive {
            return write!(f, "Header({name}: <sensitive>)");
        }
        write!(f, "Header({name}: {})", String::from_utf8_lossy(self.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_fields_are_accepted() {
        let header = Header::new("content-type", "text/plain").unwrap();
        assert_eq!(header.name(), b"content-type");
        assert_eq!(header.value(), b"text/plain");
        assert!(!header.is_sensitive());
    }

    #[test]
    fn pseudo_headers_are_accepted() {
        for name in [":method", ":scheme", ":path", ":authority", ":status"] {
            Header::new(name, "x").unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn uppercase_names_are_rejected_with_a_specific_message() {
        let error = Header::new("Content-Type", "text/plain").unwrap_err();
        assert!(error.to_string().contains("lowercase"), "got: {error}");
    }

    #[test]
    fn empty_and_bare_colon_names_are_rejected() {
        assert!(Header::new("", "x").is_err());
        assert!(Header::new(":", "x").is_err());
    }

    #[test]
    fn values_cannot_smuggle_a_second_field() {
        assert!(Header::new("x", "a\r\nb: c").is_err());
        assert!(Header::new("x", "a\nb").is_err());
        assert!(Header::new("x", "a\0b").is_err());
        // A space is legal inside a value, unlike inside a name.
        assert!(Header::new("x", "a b").is_ok());
    }

    #[test]
    fn values_match_what_nghttp3_itself_accepts() {
        // Control bytes other than HTAB are rejected, matching nghttp3's own receiving
        // table -- otherwise this crate would happily send what every peer refuses.
        for byte in [0x01u8, 0x08, 0x0b, 0x0c, 0x1f, 0x7f] {
            let value = [b'a', byte, b'b'];
            assert!(
                Header::new("x", &value[..]).is_err(),
                "byte {byte:#04x} should be rejected"
            );
        }
        // HTAB inside a value is legal, as is the high range.
        assert!(Header::new("x", "a\tb").is_ok());
        assert!(Header::new("x", &[b'a', 0x80, b'b'][..]).is_ok());
    }

    #[test]
    fn values_cannot_be_padded_with_whitespace() {
        assert!(Header::new("x", " leading").is_err());
        assert!(Header::new("x", "trailing ").is_err());
        assert!(Header::new("x", "\ttab").is_err());
        assert!(Header::new("x", "inner space is fine").is_ok());
        // An empty value is legal.
        assert!(Header::new("x", "").is_ok());
    }

    #[test]
    fn separators_are_rejected_in_names() {
        for name in ["a b", "a:b", "a,b", "a(b", "a@b", "a/b"] {
            assert!(Header::new(name, "x").is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn the_sensitive_flag_reaches_the_wire_struct() {
        let header = Header::new("authorization", "secret").unwrap().sensitive();
        assert!(header.is_sensitive());
        assert_eq!(
            u32::from(header.as_nv().flags),
            sys::NGHTTP3_NV_FLAG_NEVER_INDEX
        );
        // And it is redacted when printed, so it cannot leak through a log line.
        assert!(format!("{header:?}").contains("<sensitive>"));
        assert!(!format!("{header:?}").contains("secret"));
    }

    #[test]
    fn the_wire_struct_borrows_rather_than_copies() {
        let name = b"content-type".to_vec();
        let header = Header::new(&name, "text/plain").unwrap();
        let nv = header.as_nv();
        assert_eq!(nv.name, name.as_ptr());
        assert_eq!(nv.namelen, name.len());
    }
}
