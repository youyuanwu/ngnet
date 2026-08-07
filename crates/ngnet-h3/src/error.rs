//! Error types.
//!
//! nghttp3 reports failures as small negative integers. Two facts about them shape this
//! module. First, they mix conditions a caller can recover from with conditions that make
//! the connection unusable, and the difference is not visible in the number itself —
//! [`NativeCode::is_fatal`] is the library's own predicate for it. Second, a protocol
//! error has to be turned into an HTTP/3 application error code before the QUIC layer can
//! close the connection with it, and nghttp3 ships that mapping too
//! ([`Error::app_error_code`]). Both are used here rather than reimplemented, so this
//! crate cannot disagree with the library it wraps.

use core::fmt;

use ngnet_h3_sys as sys;

/// The result of an operation that can fail.
pub type Result<T> = core::result::Result<T, Error>;

/// A raw nghttp3 error code.
///
/// Preserved alongside the [`ErrorKind`] so that a caller needing the exact condition can
/// have it, without this crate having to grow a variant for every code nghttp3 defines.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NativeCode(i32);

impl NativeCode {
    /// Wraps a raw code as returned by nghttp3.
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    /// The raw value.
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Whether nghttp3 considers this code fatal to the connection.
    ///
    /// This is the library's own predicate (`nghttp3_err_is_fatal`), not a local guess:
    /// it covers out-of-memory and callback failure, which can surface from almost any
    /// entry point. It is one of the two conditions that poison a [`Conn`]; the other is
    /// any failure of the read or write paths, whose documentation states outright that
    /// continuing to use the connection is undefined behaviour.
    ///
    /// [`Conn`]: crate::Conn
    pub fn is_fatal(self) -> bool {
        // SAFETY: a pure predicate over an integer, with no preconditions.
        unsafe { sys::nghttp3_err_is_fatal(self.0) != 0 }
    }

    /// A short description, as nghttp3 words it.
    pub fn describe(self) -> &'static str {
        // SAFETY: `nghttp3_strerror` returns a pointer to a static string for every
        // input, including codes it does not recognise.
        let raw = unsafe { sys::nghttp3_strerror(self.0) };
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

/// An HTTP/3 application error code, as carried by QUIC's `CONNECTION_CLOSE` and
/// `RESET_STREAM`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ErrorCode(u64);

impl ErrorCode {
    /// Wraps a raw application error code.
    pub const fn new(code: u64) -> Self {
        Self(code)
    }

    /// The raw value to hand to the QUIC layer.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:04x}", self.0)
    }
}

/// The broad category of a failure.
///
/// Deliberately coarse. The exact condition is always available through
/// [`Error::native_code`]; these variants exist so a caller can decide what to *do*
/// without matching on two dozen codes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The peer violated the protocol, or sent something this endpoint will not accept.
    ///
    /// The connection or stream should be closed with [`Error::app_error_code`].
    Protocol,
    /// An allocation failed. Always fatal to the connection.
    Exhausted,
    /// This crate's caller passed something invalid.
    ///
    /// Distinguished from [`ErrorKind::Protocol`] because it indicates a bug on this side
    /// of the connection rather than misbehaviour on the other.
    InvalidInput,
    /// The connection is no longer usable and only dropping it is permitted.
    ///
    /// Returned by every operation after a fatal failure. See [`NativeCode::is_fatal`].
    ConnectionUnusable,
    /// Anything else, including conditions added by a future nghttp3.
    Internal,
    /// A stream HTTP/3 requires to stay open for the connection's whole life was closed.
    ///
    /// Distinct from [`ErrorKind::Protocol`] because there is no recovering from it at the
    /// stream level: the control or a QPACK stream is gone, so the connection itself has to
    /// be closed with the code from [`Error::app_error_code`].
    ClosedCriticalStream,
}

/// A failure from nghttp3, or from this crate's own validation.
#[derive(Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    native: Option<NativeCode>,
    context: &'static str,
    /// Whether the connection this came from is now unusable.
    ///
    /// Not derivable from the code. A protocol error is recoverable when it comes from a
    /// submission and unrecoverable when it comes from the read path, because nghttp3
    /// documents that continuing after that path fails is undefined behaviour. Only the
    /// connection knows which happened, so it says so here.
    unusable: bool,
}

impl Error {
    /// Builds an error from a raw nghttp3 code.
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
    /// These exist because nghttp3 validates many preconditions only with `assert`, which
    /// is not an error report. Checking them here is what keeps a safe API from being able
    /// to abort or reach undefined behaviour — see the note on assertions in
    /// [`crate::Conn`].
    ///
    /// `const` so that validating constructors such as [`StreamId::new`] can be `const`
    /// too, and a bad literal identifier fails to compile rather than at run time.
    ///
    /// [`StreamId::new`]: crate::StreamId::new
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

    /// The underlying nghttp3 code, if this came from nghttp3 rather than from this
    /// crate's own validation.
    pub fn native_code(&self) -> Option<NativeCode> {
        self.native
    }

    /// Whether this failure left the connection unusable.
    ///
    /// Exactly equivalent to `!conn.is_usable()` immediately after the call that returned
    /// this error, and that equivalence is what makes it worth asking. It is **not** the
    /// same as the code being one nghttp3 calls fatal: a protocol error is recoverable when
    /// it comes from a submission and unrecoverable when it comes from the read path, whose
    /// documentation says continuing is undefined behaviour. The code alone cannot tell
    /// those apart, so the connection records which happened.
    pub fn is_fatal(&self) -> bool {
        self.unusable
            || self.kind == ErrorKind::ConnectionUnusable
            || self.native.is_some_and(NativeCode::is_fatal)
    }

    /// The HTTP/3 application error code to close the connection with.
    ///
    /// Uses nghttp3's own inference rather than a table maintained here, so the mapping
    /// cannot drift from the library's. Returns `None` for failures that did not come
    /// from nghttp3, which have no protocol meaning to convey.
    pub fn app_error_code(&self) -> Option<ErrorCode> {
        let native = self.native?;
        // SAFETY: a pure mapping over an integer, with no preconditions.
        let code = unsafe { sys::nghttp3_err_infer_quic_app_error_code(native.get()) };
        Some(ErrorCode::new(code))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.native {
            Some(code) => write!(f, "{}: {}", self.context, code),
            None => write!(f, "{}", self.context),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("native", &self.native)
            .field("context", &self.context)
            .finish()
    }
}

impl core::error::Error for Error {}

/// Sorts a raw code into an [`ErrorKind`].
///
/// The fatal codes are checked first and through nghttp3's own predicate, so a future
/// version adding a fatal code is categorised correctly without this table being updated.
fn classify(code: NativeCode) -> ErrorKind {
    if code.get() == sys::NGHTTP3_ERR_NOMEM {
        return ErrorKind::Exhausted;
    }
    if code.is_fatal() {
        return ErrorKind::Internal;
    }

    // Checked before the protocol group: closing a critical stream is a protocol
    // violation, but one with no stream-level recovery, so it gets its own category.
    if code.get() == sys::NGHTTP3_ERR_H3_CLOSED_CRITICAL_STREAM {
        return ErrorKind::ClosedCriticalStream;
    }

    match code.get() {
        // Conditions this crate's caller caused.
        sys::NGHTTP3_ERR_INVALID_ARGUMENT
        | sys::NGHTTP3_ERR_INVALID_STATE
        | sys::NGHTTP3_ERR_STREAM_IN_USE
        | sys::NGHTTP3_ERR_STREAM_NOT_FOUND
        | sys::NGHTTP3_ERR_CONN_CLOSING => ErrorKind::InvalidInput,

        // Conditions the peer caused.
        sys::NGHTTP3_ERR_MALFORMED_HTTP_HEADER
        | sys::NGHTTP3_ERR_MALFORMED_HTTP_MESSAGING
        | sys::NGHTTP3_ERR_QPACK_FATAL
        | sys::NGHTTP3_ERR_QPACK_HEADER_TOO_LARGE
        | sys::NGHTTP3_ERR_QPACK_DECOMPRESSION_FAILED
        | sys::NGHTTP3_ERR_QPACK_ENCODER_STREAM_ERROR
        | sys::NGHTTP3_ERR_QPACK_DECODER_STREAM_ERROR
        | sys::NGHTTP3_ERR_STREAM_DATA_OVERFLOW
        | sys::NGHTTP3_ERR_H3_FRAME_UNEXPECTED
        | sys::NGHTTP3_ERR_H3_FRAME_ERROR
        | sys::NGHTTP3_ERR_H3_MISSING_SETTINGS
        | sys::NGHTTP3_ERR_H3_INTERNAL_ERROR
        | sys::NGHTTP3_ERR_H3_GENERAL_PROTOCOL_ERROR
        | sys::NGHTTP3_ERR_H3_ID_ERROR
        | sys::NGHTTP3_ERR_H3_SETTINGS_ERROR
        | sys::NGHTTP3_ERR_H3_STREAM_CREATION_ERROR
        | sys::NGHTTP3_ERR_H3_EXCESSIVE_LOAD => ErrorKind::Protocol,

        _ => ErrorKind::Internal,
    }
}

/// Every native code this crate classifies, for exhaustiveness testing.
///
/// Not part of the API contract: it exists so a test can prove the classification table
/// has not drifted from the library's own set of codes.
#[doc(hidden)]
pub const ALL_NATIVE_CODES: &[i32] = &[
    sys::NGHTTP3_ERR_INVALID_ARGUMENT,
    sys::NGHTTP3_ERR_INVALID_STATE,
    sys::NGHTTP3_ERR_WOULDBLOCK,
    sys::NGHTTP3_ERR_STREAM_IN_USE,
    sys::NGHTTP3_ERR_MALFORMED_HTTP_HEADER,
    sys::NGHTTP3_ERR_REMOVE_HTTP_HEADER,
    sys::NGHTTP3_ERR_MALFORMED_HTTP_MESSAGING,
    sys::NGHTTP3_ERR_QPACK_FATAL,
    sys::NGHTTP3_ERR_QPACK_HEADER_TOO_LARGE,
    sys::NGHTTP3_ERR_STREAM_NOT_FOUND,
    sys::NGHTTP3_ERR_CONN_CLOSING,
    sys::NGHTTP3_ERR_STREAM_DATA_OVERFLOW,
    sys::NGHTTP3_ERR_QPACK_DECOMPRESSION_FAILED,
    sys::NGHTTP3_ERR_QPACK_ENCODER_STREAM_ERROR,
    sys::NGHTTP3_ERR_QPACK_DECODER_STREAM_ERROR,
    sys::NGHTTP3_ERR_H3_FRAME_UNEXPECTED,
    sys::NGHTTP3_ERR_H3_FRAME_ERROR,
    sys::NGHTTP3_ERR_H3_MISSING_SETTINGS,
    sys::NGHTTP3_ERR_H3_INTERNAL_ERROR,
    sys::NGHTTP3_ERR_H3_CLOSED_CRITICAL_STREAM,
    sys::NGHTTP3_ERR_H3_GENERAL_PROTOCOL_ERROR,
    sys::NGHTTP3_ERR_H3_ID_ERROR,
    sys::NGHTTP3_ERR_H3_SETTINGS_ERROR,
    sys::NGHTTP3_ERR_H3_STREAM_CREATION_ERROR,
    sys::NGHTTP3_ERR_H3_EXCESSIVE_LOAD,
    sys::NGHTTP3_ERR_FATAL,
    sys::NGHTTP3_ERR_NOMEM,
    sys::NGHTTP3_ERR_CALLBACK_FAILURE,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_native_code_classifies_and_describes() {
        for &code in ALL_NATIVE_CODES {
            let error = Error::native(code, "test");
            // A description that is not the fallback proves nghttp3 recognises the code,
            // which is what stops this list drifting from the library's own.
            assert_ne!(
                NativeCode::new(code).describe(),
                "unknown error",
                "nghttp3 does not recognise {code}"
            );
            assert!(error.app_error_code().is_some());
        }
    }

    #[test]
    fn out_of_memory_is_exhausted_and_fatal() {
        let error = Error::native(sys::NGHTTP3_ERR_NOMEM, "test");
        assert_eq!(error.kind(), ErrorKind::Exhausted);
        assert!(error.is_fatal());
    }

    #[test]
    fn callback_failure_is_fatal() {
        assert!(NativeCode::new(sys::NGHTTP3_ERR_CALLBACK_FAILURE).is_fatal());
    }

    #[test]
    fn protocol_violations_are_not_fatal_but_carry_a_code() {
        let error = Error::native(sys::NGHTTP3_ERR_H3_FRAME_UNEXPECTED, "test");
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(!error.is_fatal());
        assert_eq!(
            error.app_error_code().map(ErrorCode::get),
            Some(u64::from(sys::NGHTTP3_H3_FRAME_UNEXPECTED))
        );
    }

    #[test]
    fn a_second_bind_is_recoverable_caller_error() {
        let error = Error::native(sys::NGHTTP3_ERR_INVALID_STATE, "test");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            !error.is_fatal(),
            "INVALID_STATE must not poison the connection"
        );
    }

    #[test]
    fn validation_errors_carry_no_native_code() {
        let error = Error::invalid_input("nope");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.native_code().is_none());
        assert!(error.app_error_code().is_none());
    }
}
