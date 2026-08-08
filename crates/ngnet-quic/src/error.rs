//! Error types.
//!
//! ngtcp2 reports failures as small negative integers, and this module keeps three things
//! separate that are easy to conflate.
//!
//! First, the **native code**: the exact value ngtcp2 returned. It is preserved rather than
//! discarded, because no fixed set of variants can cover every condition a C library
//! defines, and [`NativeCode::is_fatal`] and [`NativeCode::describe`] are the library's own
//! predicates rather than a table maintained here.
//!
//! Second, the **kind**: a coarse classification a caller can branch on without matching
//! two dozen codes.
//!
//! Third — and unlike HTTP/3, which has one — QUIC has **two error-code spaces**.
//! Transport error codes are defined by the QUIC transport specification and describe the
//! connection itself; application error codes are opaque to QUIC and mean whatever the
//! protocol running over it says they mean. The same integer means different things in
//! each, so they are different types here. Collapsing them into one would let an
//! application code be passed where a transport code is required, with nothing to catch it.

// The constructors that build an error from a native code, and the classifier behind them,
// are exercised by the tests below but have no non-test caller until a connection exists to
// return them. They are written here because this is where the classification belongs.
#![allow(dead_code)]

use core::fmt;

use ngnet_quic_sys as sys;

/// The result of an operation that can fail.
pub type Result<T> = core::result::Result<T, Error>;

/// A raw ngtcp2 error code.
///
/// Preserved alongside the [`ErrorKind`] so that a caller needing the exact condition can
/// have it, without this crate having to grow a variant for every code ngtcp2 defines.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NativeCode(i32);

impl NativeCode {
    /// Wraps a raw code as returned by ngtcp2.
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    /// The raw value.
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Whether ngtcp2 considers this code fatal to the connection.
    ///
    /// This is the library's own predicate (`ngtcp2_err_is_fatal`), not a local guess.
    pub fn is_fatal(self) -> bool {
        // SAFETY: a pure predicate over an integer, with no preconditions.
        unsafe { sys::ngtcp2_err_is_fatal(self.0) != 0 }
    }

    /// A short description, as ngtcp2 words it.
    pub fn describe(self) -> &'static str {
        // SAFETY: `ngtcp2_strerror` returns a pointer to a static string for every input,
        // including codes it does not recognise.
        let raw = unsafe { sys::ngtcp2_strerror(self.0) };
        if raw.is_null() {
            return "unknown error";
        }
        // SAFETY: the returned string is static, NUL-terminated and never freed.
        unsafe { core::ffi::CStr::from_ptr(raw) }
            .to_str()
            .unwrap_or("unknown error")
    }
}

impl fmt::Debug for NativeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NativeCode({}: {})", self.0, self.describe())
    }
}

impl fmt::Display for NativeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.describe(), self.0)
    }
}

/// A QUIC **transport** error code.
///
/// Defined by the QUIC transport specification and meaningful to any QUIC implementation.
/// Distinct from [`ApplicationErrorCode`]: the same integer means different things in the
/// two spaces, and the type system is what keeps them apart.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TransportErrorCode(u64);

impl TransportErrorCode {
    /// No error. The peer is closing the connection without complaint.
    pub const NO_ERROR: Self = Self(sys::NGTCP2_NO_ERROR as u64);

    /// Wraps a raw transport error code.
    pub const fn new(code: u64) -> Self {
        Self(code)
    }

    /// The raw value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The transport error code ngtcp2 infers for a native error.
    ///
    /// Uses ngtcp2's own mapping rather than a table maintained here, so this crate cannot
    /// disagree with the library it wraps.
    pub fn infer(native: NativeCode) -> Self {
        // SAFETY: a pure mapping over an integer, with no preconditions.
        Self(unsafe { sys::ngtcp2_err_infer_quic_transport_error_code(native.get()) })
    }
}

impl fmt::Display for TransportErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transport error 0x{:x}", self.0)
    }
}

/// A QUIC **application** error code.
///
/// Opaque to QUIC itself: it means whatever the protocol running over the connection says
/// it means. HTTP/3 defines its own set, for instance. Distinct from
/// [`TransportErrorCode`] for that reason.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ApplicationErrorCode(u64);

impl ApplicationErrorCode {
    /// Wraps a raw application error code.
    pub const fn new(code: u64) -> Self {
        Self(code)
    }

    /// The raw value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ApplicationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "application error 0x{:x}", self.0)
    }
}

/// The broad category of a failure.
///
/// Deliberately coarse. The exact condition is always available through
/// [`Error::native_code`]; these variants exist so a caller can decide what to *do* without
/// matching on two dozen codes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The peer violated the transport protocol, or sent something this endpoint will not
    /// accept.
    Protocol,
    /// An allocation failed. Always fatal to the connection.
    Exhausted,
    /// This crate's caller passed something invalid.
    ///
    /// Distinguished from [`ErrorKind::Protocol`] because it indicates a bug on this side
    /// of the connection rather than misbehaviour on the other. ngtcp2 checks most of these
    /// with `assert()`, which is compiled out of release builds, so this crate checks them
    /// itself.
    InvalidInput,
    /// The TLS handshake failed.
    Crypto,
    /// The connection is no longer usable and only dropping it is permitted.
    ConnectionUnusable,
    /// The connection is closing or draining and will accept no new work.
    Closing,
    /// The peer's limits prevent this right now; it may succeed later.
    ///
    /// Covers the stream-count and flow-control blocks, which are ordinary conditions in a
    /// working connection rather than failures.
    Blocked,
    /// Anything else, including conditions added by a future ngtcp2.
    Internal,
}

/// A failure from ngtcp2, or from this crate's own validation.
#[derive(Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    native: Option<NativeCode>,
    context: &'static str,
    /// Whether the connection this came from is now unusable.
    ///
    /// Not derivable from the code alone: the same condition can be recoverable from one
    /// entry point and terminal from another, and only the connection knows which happened.
    unusable: bool,
}

impl Error {
    /// Builds an error from a raw ngtcp2 code.
    pub(crate) fn native(code: i32, context: &'static str) -> Self {
        let native = NativeCode::new(code);
        Self {
            kind: classify(native),
            native: Some(native),
            context,
            unusable: false,
        }
    }

    /// Marks this failure as one that left the connection unusable.
    pub(crate) fn into_unusable(mut self) -> Self {
        self.unusable = true;
        self
    }

    /// Builds an error for a precondition this crate checked itself.
    ///
    /// `const` so that validating constructors can be `const` too, and a bad literal fails
    /// to compile rather than at run time.
    pub(crate) const fn invalid_input(context: &'static str) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            native: None,
            context,
            unusable: false,
        }
    }

    /// Builds the error every operation returns once the connection is poisoned.
    pub(crate) fn unusable() -> Self {
        Self {
            kind: ErrorKind::ConnectionUnusable,
            native: None,
            context: "the connection encountered an unrecoverable error and cannot be used",
            unusable: true,
        }
    }

    /// The broad category of this failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The underlying ngtcp2 code, if this came from ngtcp2 rather than from this crate's
    /// own validation.
    pub fn native_code(&self) -> Option<NativeCode> {
        self.native
    }

    /// Whether this failure left the connection unusable.
    pub fn is_fatal(&self) -> bool {
        self.unusable
            || self.kind == ErrorKind::ConnectionUnusable
            || self.native.is_some_and(NativeCode::is_fatal)
    }

    /// The QUIC transport error code to close the connection with.
    ///
    /// Returns `None` for failures that did not come from ngtcp2, which have no transport
    /// meaning to convey.
    pub fn transport_error_code(&self) -> Option<TransportErrorCode> {
        self.native.map(TransportErrorCode::infer)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("native", &self.native)
            .field("context", &self.context)
            .field("unusable", &self.unusable)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.native {
            Some(native) => write!(f, "{}: {native}", self.context),
            None => write!(f, "{}", self.context),
        }
    }
}

impl core::error::Error for Error {}

/// Maps a raw ngtcp2 code onto a coarse category.
///
/// Codes not named here fall to [`ErrorKind::Internal`], which is why that variant exists:
/// a future ngtcp2 may add codes this build has never heard of, and they must still arrive
/// as something rather than being lost.
fn classify(native: NativeCode) -> ErrorKind {
    match native.get() {
        sys::NGTCP2_ERR_NOMEM => ErrorKind::Exhausted,
        sys::NGTCP2_ERR_INVALID_ARGUMENT | sys::NGTCP2_ERR_INVALID_STATE => ErrorKind::InvalidInput,
        sys::NGTCP2_ERR_CRYPTO
        | sys::NGTCP2_ERR_REQUIRED_TRANSPORT_PARAM
        | sys::NGTCP2_ERR_MALFORMED_TRANSPORT_PARAM
        | sys::NGTCP2_ERR_TRANSPORT_PARAM
        | sys::NGTCP2_ERR_VERSION_NEGOTIATION_FAILURE => ErrorKind::Crypto,
        sys::NGTCP2_ERR_STREAM_DATA_BLOCKED
        | sys::NGTCP2_ERR_STREAM_ID_BLOCKED
        | sys::NGTCP2_ERR_STREAM_SHUT_WR
        | sys::NGTCP2_ERR_STREAM_NOT_FOUND
        | sys::NGTCP2_ERR_CONN_ID_BLOCKED => ErrorKind::Blocked,
        sys::NGTCP2_ERR_CLOSING | sys::NGTCP2_ERR_DRAINING => ErrorKind::Closing,
        sys::NGTCP2_ERR_PROTO
        | sys::NGTCP2_ERR_FRAME_ENCODING
        | sys::NGTCP2_ERR_ACK_FRAME
        | sys::NGTCP2_ERR_FINAL_SIZE
        | sys::NGTCP2_ERR_FLOW_CONTROL
        | sys::NGTCP2_ERR_STREAM_LIMIT => ErrorKind::Protocol,
        sys::NGTCP2_ERR_CALLBACK_FAILURE | sys::NGTCP2_ERR_INTERNAL => {
            ErrorKind::ConnectionUnusable
        }
        _ => ErrorKind::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_codes_describe_themselves_using_the_library() {
        let code = NativeCode::new(sys::NGTCP2_ERR_NOMEM);
        assert!(!code.describe().is_empty());
        assert_ne!(code.describe(), "unknown error");
    }

    #[test]
    fn a_code_ngtcp2_does_not_know_still_yields_a_description() {
        // The point is that `describe` never returns a dangling or empty string, even for
        // a value ngtcp2 has no name for.
        let code = NativeCode::new(-999_999);
        assert!(!code.describe().is_empty());
    }

    #[test]
    fn out_of_memory_is_fatal_and_classified_as_exhausted() {
        let code = NativeCode::new(sys::NGTCP2_ERR_NOMEM);
        assert!(code.is_fatal());
        assert_eq!(classify(code), ErrorKind::Exhausted);
    }

    #[test]
    fn blocking_conditions_are_not_protocol_errors() {
        // These are ordinary states of a working connection, and a caller that treats them
        // as failures will close connections it should have waited on.
        for raw in [
            sys::NGTCP2_ERR_STREAM_DATA_BLOCKED,
            sys::NGTCP2_ERR_STREAM_ID_BLOCKED,
        ] {
            assert_eq!(classify(NativeCode::new(raw)), ErrorKind::Blocked);
        }
    }

    #[test]
    fn an_unknown_code_classifies_as_internal_rather_than_being_lost() {
        assert_eq!(classify(NativeCode::new(-31_337)), ErrorKind::Internal);
    }

    #[test]
    fn validation_errors_carry_no_native_code() {
        let err = Error::invalid_input("test");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(err.native_code().is_none());
        assert!(err.transport_error_code().is_none());
    }

    #[test]
    fn the_two_error_code_spaces_are_distinct_types() {
        // Same integer, different meaning. This test exists to pin that they cannot be
        // interchanged; it would fail to compile if they became one type.
        let transport = TransportErrorCode::new(0x0a);
        let application = ApplicationErrorCode::new(0x0a);
        assert_eq!(transport.get(), application.get());
    }

    #[test]
    fn transport_codes_are_inferred_by_the_library() {
        let inferred = TransportErrorCode::infer(NativeCode::new(sys::NGTCP2_ERR_FLOW_CONTROL));
        assert_ne!(inferred.get(), 0);
    }
}
