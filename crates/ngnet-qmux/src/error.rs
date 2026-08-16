//! Turning dwnx's integer return codes into Rust errors.
//!
//! # Why fatality is not simply `dwnx_err_is_fatal`
//!
//! dwnx exposes a predicate for this, and it is not one a safe wrapper can lean on. The whole
//! of it is `liberr < DWNX_ERR_FATAL`, where `DWNX_ERR_FATAL` is `-500`; so it answers "was
//! this code allocated in the -5xx block", which is true of exactly `NOMEM` and
//! `CALLBACK_FAILURE`. Everything else -- `PROTO`, `FRAME_ENCODING`, `FLOW_CONTROL`, a
//! malformed transport parameter -- is reported as non-fatal despite the header saying, for
//! most of them, that the connection must be closed.
//!
//! So this module keeps two separate notions. [`NativeCode::is_fatal`] forwards to the C
//! predicate unchanged, because a caller reading dwnx's own documentation should be able to
//! find the same answer here. [`Error::leaves_connection_usable`] is this crate's own
//! judgement, derived from what the header says about each code at each entry point, and it
//! is the one the API acts on.
//!
//! The distinction matters most for the stream codes, which do not group the way their names
//! suggest. `STREAM_ID_BLOCKED` means "no stream capacity right now", is returned by the open
//! functions, and is entirely recoverable. `STREAM_LIMIT` means the *peer* exceeded a limit
//! we advertised -- a protocol violation, and terminal. `STREAM_NOT_FOUND` comes back from
//! the write path, where the header's closing instruction is to close the connection. Three
//! similar names, three different dispositions.

use ngnet_qmux_sys as sys;

use core::ffi::CStr;
use core::fmt;

/// A raw dwnx error code, preserved exactly as the library returned it.
///
/// Kept alongside the classified [`ErrorKind`] so that nothing is lost in translation: a
/// caller comparing against dwnx's own documentation, or reporting a bug upstream, needs the
/// number the library actually produced.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NativeCode(i32);

impl NativeCode {
    /// Wrap a raw code.
    #[must_use]
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    /// The raw code, as dwnx returned it. Always negative for a genuine error.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }

    /// dwnx's own fatality predicate, forwarded unchanged.
    ///
    /// See the module documentation for why this is rarely the question a caller wants
    /// answered; [`Error::leaves_connection_usable`] usually is.
    #[must_use]
    pub fn is_fatal(self) -> bool {
        // SAFETY: a pure function over an integer, with no preconditions.
        unsafe { sys::dwnx_err_is_fatal(self.0) != 0 }
    }

    /// dwnx's text for this code, e.g. `ERR_PROTO`.
    ///
    /// Returns `None` for a code dwnx does not recognise, rather than its placeholder string,
    /// so that an unknown code is distinguishable from a known one.
    #[must_use]
    pub fn text(self) -> Option<&'static str> {
        // SAFETY: `dwnx_strerror` returns a pointer to a static string literal for every
        // input, including unrecognised ones, and never null.
        let text = unsafe { CStr::from_ptr(sys::dwnx_strerror(self.0)) };
        match text.to_str() {
            Ok("(unknown)") | Err(_) => None,
            Ok(text) => Some(text),
        }
    }

    /// The QUIC transport error code dwnx infers for this condition.
    ///
    /// Useful when building a close reason: it is the value dwnx itself would put on the wire.
    #[must_use]
    pub fn inferred_transport_error(self) -> u64 {
        // SAFETY: a pure function over an integer, with no preconditions.
        unsafe { sys::dwnx_err_infer_quic_transport_error_code(self.0) }
    }
}

impl fmt::Debug for NativeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.text() {
            Some(text) => write!(f, "NativeCode({} {text})", self.0),
            None => write!(f, "NativeCode({})", self.0),
        }
    }
}

impl fmt::Display for NativeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.text() {
            Some(text) => write!(f, "{text} ({})", self.0),
            None => write!(f, "unknown dwnx error ({})", self.0),
        }
    }
}

/// What went wrong, classified.
///
/// Non-exhaustive because dwnx may add conditions; an unrecognised code classifies as
/// [`ErrorKind::Internal`] rather than failing to convert.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An argument was rejected by dwnx, or by this crate before reaching it.
    InvalidArgument,
    /// The peer violated the protocol, or sent something unparseable.
    Protocol,
    /// The operation is not valid in the connection's current state.
    InvalidState,
    /// A flow-control or stream limit was exceeded by the peer.
    LimitExceeded,
    /// A stream-level condition: unknown stream, wrong state, or already in use.
    Stream,
    /// A transport parameter was missing, malformed, or otherwise unacceptable.
    TransportParameter,
    /// The connection is closing, draining, or closed on idle timeout.
    Closed,
    /// A caller-supplied handler reported failure.
    Handler,
    /// Out of memory, or a buffer dwnx needed could not be obtained.
    Memory,
    /// An internal dwnx failure, or a condition this crate does not recognise.
    Internal,
}

/// An error from dwnx, or from this crate's own validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    native: Option<NativeCode>,
    context: &'static str,
    usable: bool,
}

impl Error {
    /// Build an error from a raw dwnx return code.
    ///
    /// `context` names the operation, so that a `STREAM_NOT_FOUND` from a write reads
    /// differently from one from a shutdown.
    #[must_use]
    pub fn from_native(code: i32, context: &'static str) -> Self {
        let native = NativeCode::new(code);
        Self {
            kind: classify(code),
            native: Some(native),
            context,
            usable: leaves_connection_usable(code),
        }
    }

    /// Build an error this crate raised itself, without ever calling into dwnx.
    ///
    /// Used for the preconditions dwnx guards with `assert` rather than an error return: a
    /// wrapper that passed those through would abort the process instead of failing.
    #[must_use]
    pub const fn validation(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            native: None,
            context,
            usable: true,
        }
    }

    /// The classified kind.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The raw dwnx code, if this error came from dwnx.
    #[must_use]
    pub const fn native(&self) -> Option<NativeCode> {
        self.native
    }

    /// The operation that produced this error.
    #[must_use]
    pub const fn context(&self) -> &'static str {
        self.context
    }

    /// Whether the connection can continue to be used.
    ///
    /// This crate's own judgement, not dwnx's `dwnx_err_is_fatal`; see the module
    /// documentation for why the two differ and which to trust.
    #[must_use]
    pub const fn leaves_connection_usable(&self) -> bool {
        self.usable
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.context)?;
        match self.native {
            Some(native) => write!(f, "{native}"),
            None => write!(f, "{:?}", self.kind),
        }
    }
}

impl core::error::Error for Error {}

/// Map a raw code to its kind.
///
/// Every condition dwnx defines is named here. The exhaustiveness test scans the C header and
/// fails if a code exists that this function sends to `Internal` by default, so a new upstream
/// condition cannot be absorbed silently.
fn classify(code: i32) -> ErrorKind {
    match code {
        sys::DWNX_ERR_INVALID_ARGUMENT => ErrorKind::InvalidArgument,
        sys::DWNX_ERR_NOBUF => ErrorKind::Memory,
        sys::DWNX_ERR_PROTO => ErrorKind::Protocol,
        sys::DWNX_ERR_INVALID_STATE => ErrorKind::InvalidState,
        sys::DWNX_ERR_STREAM_ID_BLOCKED => ErrorKind::Stream,
        sys::DWNX_ERR_STREAM_IN_USE => ErrorKind::Stream,
        sys::DWNX_ERR_STREAM_DATA_BLOCKED => ErrorKind::Stream,
        sys::DWNX_ERR_FLOW_CONTROL => ErrorKind::LimitExceeded,
        sys::DWNX_ERR_STREAM_LIMIT => ErrorKind::LimitExceeded,
        sys::DWNX_ERR_FINAL_SIZE => ErrorKind::Protocol,
        sys::DWNX_ERR_REQUIRED_TRANSPORT_PARAM
        | sys::DWNX_ERR_MALFORMED_TRANSPORT_PARAM
        | sys::DWNX_ERR_TRANSPORT_PARAM => ErrorKind::TransportParameter,
        sys::DWNX_ERR_FRAME_ENCODING => ErrorKind::Protocol,
        sys::DWNX_ERR_STREAM_SHUT_WR => ErrorKind::Stream,
        sys::DWNX_ERR_STREAM_NOT_FOUND => ErrorKind::Stream,
        sys::DWNX_ERR_STREAM_STATE => ErrorKind::Stream,
        sys::DWNX_ERR_CLOSING | sys::DWNX_ERR_DRAINING | sys::DWNX_ERR_IDLE_CLOSE => {
            ErrorKind::Closed
        }
        sys::DWNX_ERR_INTERNAL => ErrorKind::Internal,
        sys::DWNX_ERR_WRITE_MORE => ErrorKind::InvalidState,
        sys::DWNX_ERR_NOMEM => ErrorKind::Memory,
        sys::DWNX_ERR_CALLBACK_FAILURE => ErrorKind::Handler,
        _ => ErrorKind::Internal,
    }
}

/// Whether a connection survives this condition.
///
/// Derived from what the dwnx header says about each code, not from `dwnx_err_is_fatal`. The
/// recoverable set is deliberately small: the write path's flow-control signals, which are
/// instructions to the caller rather than failures, and the stream-open capacity signal.
///
/// `DRAINING` is the awkward one. The connection is finished, so it is not usable in the sense
/// of carrying more data -- but it is not a failure either, and the caller should not treat it
/// as one. It is reported as unusable here and given its own outcome by the read path, which
/// is where the distinction can actually be expressed.
fn leaves_connection_usable(code: i32) -> bool {
    matches!(
        code,
        sys::DWNX_ERR_WRITE_MORE
            | sys::DWNX_ERR_STREAM_DATA_BLOCKED
            | sys::DWNX_ERR_STREAM_SHUT_WR
            | sys::DWNX_ERR_STREAM_ID_BLOCKED
            | sys::DWNX_ERR_STREAM_IN_USE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_code_reports_dwnx_text() {
        let code = NativeCode::new(sys::DWNX_ERR_PROTO);
        assert_eq!(code.text(), Some("ERR_PROTO"));
        assert_eq!(code.get(), sys::DWNX_ERR_PROTO);
    }

    #[test]
    fn unknown_codes_have_no_text() {
        assert_eq!(NativeCode::new(-9999).text(), None);
    }

    /// The gap between dwnx's predicate and this crate's judgement, pinned so it stays visible.
    #[test]
    fn dwnx_fatality_and_usability_disagree_as_documented() {
        let proto = Error::from_native(sys::DWNX_ERR_PROTO, "read");
        assert!(!proto.native().unwrap().is_fatal());
        assert!(!proto.leaves_connection_usable());

        let blocked = Error::from_native(sys::DWNX_ERR_STREAM_DATA_BLOCKED, "write");
        assert!(!blocked.native().unwrap().is_fatal());
        assert!(blocked.leaves_connection_usable());
    }

    /// The three similarly named stream codes classify differently, on purpose.
    #[test]
    fn stream_codes_are_not_interchangeable() {
        assert!(
            Error::from_native(sys::DWNX_ERR_STREAM_ID_BLOCKED, "open")
                .leaves_connection_usable()
        );
        assert!(
            !Error::from_native(sys::DWNX_ERR_STREAM_LIMIT, "read").leaves_connection_usable()
        );
        assert!(
            !Error::from_native(sys::DWNX_ERR_STREAM_NOT_FOUND, "write")
                .leaves_connection_usable()
        );
    }

    #[test]
    fn validation_errors_carry_no_native_code() {
        let error = Error::validation(ErrorKind::InvalidArgument, "transport parameters");
        assert!(error.native().is_none());
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}
