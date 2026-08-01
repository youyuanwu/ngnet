//! Typed errors over libnghttp2's numeric return codes.
//!
//! Every fallible operation in the safe API reports failure as an [`Error`] rather than
//! a bare negative integer. Each error names the operation that failed, carries a
//! [`ErrorKind`] category, and where it originated in the native library also carries
//! the [`NativeCode`] that produced it.

use core::ffi::CStr;
use core::fmt;

/// Result alias for the safe API.
pub type Result<T> = core::result::Result<T, Error>;

/// A native libnghttp2 error code.
///
/// These are the `NGHTTP2_ERR_*` values. They are negative integers; the raw bindings
/// expose them as plain constants rather than an enumeration, so this newtype exists to
/// keep them distinguishable from any other integer in the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeCode(i32);

impl NativeCode {
    /// Wraps a raw `NGHTTP2_ERR_*` value.
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    /// The underlying integer.
    pub const fn get(self) -> i32 {
        self.0
    }

    /// libnghttp2's own description of this code.
    ///
    /// Returns `None` for a code libnghttp2 does not define, and in the unexpected case
    /// that the library hands back a string that is not valid UTF-8.
    ///
    /// The guard on [`ALL_NATIVE_CODES`] is deliberate: `nghttp2_strerror` documents
    /// that its argument must be one of the `nghttp2_error` values. Its implementation
    /// happens to be defensive about unknown input, but relying on that would be relying
    /// on undocumented behaviour.
    pub fn describe(self) -> Option<&'static str> {
        if !ALL_NATIVE_CODES.contains(&self) {
            return None;
        }
        // SAFETY: `self` was just confirmed to be one of the `nghttp2_error` values, which
        // is `nghttp2_strerror`'s documented precondition. It returns a pointer to a
        // static, NUL-terminated string with the same lifetime as the library itself.
        let ptr = unsafe { nghttp2_sys::nghttp2_strerror(self.0) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: the pointer is non-null and points at a static NUL-terminated string.
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }
}

impl fmt::Display for NativeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.describe() {
            Some(text) => write!(f, "{text} ({})", self.0),
            None => write!(f, "unknown native error ({})", self.0),
        }
    }
}

/// The category an [`Error`] falls into.
///
/// These categories are the contract: any failure the safe API reports is exactly one of
/// them, so callers can branch on cause without inspecting native codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The peer violated the protocol, or sent something that cannot be processed.
    ///
    /// Note that most protocol violations never surface as an error at all: libnghttp2
    /// handles them by queueing a `GOAWAY` or `RST_STREAM` for the peer and reporting
    /// the input as processed. Only connection-fatal conditions reach the caller.
    Protocol,
    /// The caller supplied something invalid, or called at a time the operation is not
    /// permitted. This category indicates a bug in the calling program.
    InvalidInput,
    /// A resource was exhausted; in practice, memory allocation failed.
    Exhausted,
    /// A condition that is neither the caller's doing nor a plain protocol violation.
    ///
    /// This covers libnghttp2's internal control-flow signals, which should not escape the
    /// safe API and would indicate a defect in this crate if one did, together with
    /// genuine internal failures such as a callback failing.
    Internal,
}

impl ErrorKind {
    /// A short human-readable description of the category.
    pub const fn description(self) -> &'static str {
        match self {
            Self::Protocol => "protocol failure",
            Self::InvalidInput => "invalid input",
            Self::Exhausted => "resource exhausted",
            Self::Internal => "internal error",
        }
    }
}

/// A failure reported by the safe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    op: &'static str,
    kind: ErrorKind,
    native: Option<NativeCode>,
    detail: Option<&'static str>,
}

impl Error {
    /// Builds an error this crate detected itself, rather than one libnghttp2 reported.
    pub(crate) const fn new(op: &'static str, kind: ErrorKind, detail: &'static str) -> Self {
        Self {
            op,
            kind,
            native: None,
            detail: Some(detail),
        }
    }

    /// Translates a negative return value from `op` into a typed error.
    ///
    /// The mapping is total: every `NGHTTP2_ERR_*` value maps to exactly one
    /// [`ErrorKind`], and any value this crate does not recognise maps to
    /// [`ErrorKind::Internal`] rather than being silently discarded.
    ///
    /// This is public so that callers who drop down to [`crate::raw`] can translate a
    /// native return value using the same categories the safe API uses.
    pub fn from_native(op: &'static str, code: i32) -> Self {
        Self {
            op,
            kind: classify(code),
            native: Some(NativeCode::new(code)),
            detail: None,
        }
    }

    /// The operation that failed.
    pub const fn operation(&self) -> &'static str {
        self.op
    }

    /// The category of this failure.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The originating native code, when this failure came from libnghttp2.
    pub const fn native_code(&self) -> Option<NativeCode> {
        self.native
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed: {}", self.op, self.kind.description())?;
        match (self.native, self.detail) {
            (Some(code), _) => write!(f, ": {code}"),
            (None, Some(detail)) => write!(f, ": {detail}"),
            (None, None) => Ok(()),
        }
    }
}

impl core::error::Error for Error {}

/// Every `NGHTTP2_ERR_*` value this crate knows how to translate.
///
/// A test cross-checks this list against the vendored nghttp2 header, so upgrading the
/// bundled library to a version that adds or removes a code fails the build rather than
/// silently falling through to [`ErrorKind::Internal`].
pub const ALL_NATIVE_CODES: &[NativeCode] = &[
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_ARGUMENT),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_BUFFER_ERROR),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_UNSUPPORTED_VERSION),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_WOULDBLOCK),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_PROTO),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_FRAME),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_EOF),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_DEFERRED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_STREAM_CLOSED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_STREAM_CLOSING),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_STREAM_SHUT_WR),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_STREAM_ID),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_STREAM_STATE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_DEFERRED_DATA_EXIST),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_START_STREAM_NOT_ALLOWED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_GOAWAY_ALREADY_SENT),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_HEADER_BLOCK),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INVALID_STATE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_FRAME_SIZE_ERROR),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_HEADER_COMP),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_FLOW_CONTROL),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INSUFF_BUFSIZE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_PAUSE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_TOO_MANY_INFLIGHT_SETTINGS),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_PUSH_DISABLED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_DATA_EXIST),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_SESSION_CLOSING),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_HTTP_HEADER),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_HTTP_MESSAGING),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_REFUSED_STREAM),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_INTERNAL),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_CANCEL),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_SETTINGS_EXPECTED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_TOO_MANY_SETTINGS),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_FATAL),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_NOMEM),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_CALLBACK_FAILURE),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_BAD_CLIENT_MAGIC),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_FLOODED),
    NativeCode::new(nghttp2_sys::NGHTTP2_ERR_TOO_MANY_CONTINUATIONS),
];

/// Maps a native code to its category.
///
/// Kept as a free function so the mapping table stays readable and so tests can drive it
/// exhaustively over [`ALL_NATIVE_CODES`].
pub(crate) const fn classify(code: i32) -> ErrorKind {
    use nghttp2_sys as sys;
    match code {
        // Allocation failure is the only genuine resource exhaustion.
        sys::NGHTTP2_ERR_NOMEM => ErrorKind::Exhausted,

        // The peer misbehaved, or the connection cannot continue because of it.
        sys::NGHTTP2_ERR_INVALID_FRAME
        | sys::NGHTTP2_ERR_INVALID_HEADER_BLOCK
        | sys::NGHTTP2_ERR_FRAME_SIZE_ERROR
        | sys::NGHTTP2_ERR_HEADER_COMP
        | sys::NGHTTP2_ERR_FLOW_CONTROL
        | sys::NGHTTP2_ERR_HTTP_HEADER
        | sys::NGHTTP2_ERR_HTTP_MESSAGING
        | sys::NGHTTP2_ERR_REFUSED_STREAM
        | sys::NGHTTP2_ERR_SETTINGS_EXPECTED
        | sys::NGHTTP2_ERR_TOO_MANY_SETTINGS
        | sys::NGHTTP2_ERR_TOO_MANY_CONTINUATIONS
        | sys::NGHTTP2_ERR_BAD_CLIENT_MAGIC
        | sys::NGHTTP2_ERR_FLOODED => ErrorKind::Protocol,

        // The calling program asked for something it may not have.
        sys::NGHTTP2_ERR_INVALID_ARGUMENT
        | sys::NGHTTP2_ERR_UNSUPPORTED_VERSION
        | sys::NGHTTP2_ERR_PROTO
        | sys::NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE
        | sys::NGHTTP2_ERR_STREAM_CLOSED
        | sys::NGHTTP2_ERR_STREAM_CLOSING
        | sys::NGHTTP2_ERR_STREAM_SHUT_WR
        | sys::NGHTTP2_ERR_INVALID_STREAM_ID
        | sys::NGHTTP2_ERR_INVALID_STREAM_STATE
        | sys::NGHTTP2_ERR_DEFERRED_DATA_EXIST
        | sys::NGHTTP2_ERR_START_STREAM_NOT_ALLOWED
        | sys::NGHTTP2_ERR_GOAWAY_ALREADY_SENT
        | sys::NGHTTP2_ERR_INVALID_STATE
        | sys::NGHTTP2_ERR_PUSH_DISABLED
        | sys::NGHTTP2_ERR_DATA_EXIST
        | sys::NGHTTP2_ERR_SESSION_CLOSING
        | sys::NGHTTP2_ERR_INSUFF_BUFSIZE
        // Not peer misconduct: the local endpoint has too many SETTINGS frames in
        // flight and may not transmit another yet.
        | sys::NGHTTP2_ERR_TOO_MANY_INFLIGHT_SETTINGS => ErrorKind::InvalidInput,

        // Control-flow signals and internal markers. None of these should escape the
        // safe API; if one does, this crate mishandled it.
        _ => ErrorKind::Internal,
    }
}

/// An HTTP/2 error code, as carried by `RST_STREAM` and `GOAWAY` frames.
///
/// These are the wire-level codes from RFC 9113 section 7, distinct from [`NativeCode`],
/// which describes libnghttp2's own API failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ErrorCode(u32);

impl ErrorCode {
    /// The associated condition is not an error.
    pub const NO_ERROR: Self = Self(nghttp2_sys::NGHTTP2_NO_ERROR);
    /// The endpoint detected an unspecific protocol error.
    pub const PROTOCOL_ERROR: Self = Self(nghttp2_sys::NGHTTP2_PROTOCOL_ERROR);
    /// The endpoint encountered an unexpected internal error.
    pub const INTERNAL_ERROR: Self = Self(nghttp2_sys::NGHTTP2_INTERNAL_ERROR);
    /// The endpoint detected that its peer violated the flow-control protocol.
    pub const FLOW_CONTROL_ERROR: Self = Self(nghttp2_sys::NGHTTP2_FLOW_CONTROL_ERROR);
    /// The endpoint sent a `SETTINGS` frame but did not receive a response in time.
    pub const SETTINGS_TIMEOUT: Self = Self(nghttp2_sys::NGHTTP2_SETTINGS_TIMEOUT);
    /// The endpoint received a frame after a stream was half-closed.
    pub const STREAM_CLOSED: Self = Self(nghttp2_sys::NGHTTP2_STREAM_CLOSED);
    /// The endpoint received a frame with an invalid size.
    pub const FRAME_SIZE_ERROR: Self = Self(nghttp2_sys::NGHTTP2_FRAME_SIZE_ERROR);
    /// The endpoint refused the stream before any processing had been done.
    pub const REFUSED_STREAM: Self = Self(nghttp2_sys::NGHTTP2_REFUSED_STREAM);
    /// The endpoint no longer needs the stream.
    pub const CANCEL: Self = Self(nghttp2_sys::NGHTTP2_CANCEL);
    /// The endpoint is unable to maintain the header compression context.
    pub const COMPRESSION_ERROR: Self = Self(nghttp2_sys::NGHTTP2_COMPRESSION_ERROR);
    /// The connection established in response to a `CONNECT` request was reset.
    pub const CONNECT_ERROR: Self = Self(nghttp2_sys::NGHTTP2_CONNECT_ERROR);
    /// The endpoint detected that its peer is exhibiting a behavior causing load.
    pub const ENHANCE_YOUR_CALM: Self = Self(nghttp2_sys::NGHTTP2_ENHANCE_YOUR_CALM);
    /// The underlying transport has properties that do not meet minimum requirements.
    pub const INADEQUATE_SECURITY: Self = Self(nghttp2_sys::NGHTTP2_INADEQUATE_SECURITY);
    /// The endpoint requires that HTTP/1.1 be used instead of HTTP/2.
    pub const HTTP_1_1_REQUIRED: Self = Self(nghttp2_sys::NGHTTP2_HTTP_1_1_REQUIRED);

    /// Wraps a raw HTTP/2 error code.
    ///
    /// Unknown codes are permitted: RFC 9113 requires unrecognised error codes be
    /// treated as [`Self::INTERNAL_ERROR`], but they are preserved verbatim here so the
    /// caller can observe exactly what the peer sent.
    pub const fn new(code: u32) -> Self {
        Self(code)
    }

    /// The underlying integer.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match *self {
            Self::NO_ERROR => "NO_ERROR",
            Self::PROTOCOL_ERROR => "PROTOCOL_ERROR",
            Self::INTERNAL_ERROR => "INTERNAL_ERROR",
            Self::FLOW_CONTROL_ERROR => "FLOW_CONTROL_ERROR",
            Self::SETTINGS_TIMEOUT => "SETTINGS_TIMEOUT",
            Self::STREAM_CLOSED => "STREAM_CLOSED",
            Self::FRAME_SIZE_ERROR => "FRAME_SIZE_ERROR",
            Self::REFUSED_STREAM => "REFUSED_STREAM",
            Self::CANCEL => "CANCEL",
            Self::COMPRESSION_ERROR => "COMPRESSION_ERROR",
            Self::CONNECT_ERROR => "CONNECT_ERROR",
            Self::ENHANCE_YOUR_CALM => "ENHANCE_YOUR_CALM",
            Self::INADEQUATE_SECURITY => "INADEQUATE_SECURITY",
            Self::HTTP_1_1_REQUIRED => "HTTP_1_1_REQUIRED",
            _ => return write!(f, "UNKNOWN({})", self.0),
        };
        write!(f, "{name}({})", self.0)
    }
}
