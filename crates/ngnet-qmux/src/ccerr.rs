//! Connection close reasons.
//!
//! dwnx can *parse* a CONNECTION_CLOSE frame -- receiving one is how a peer's shutdown becomes
//! visible -- but it exposes no function to serialise one. There is no
//! `dwnx_conn_write_connection_close`, so this crate cannot offer a way to close a connection
//! on the wire; see `docs/qmux/pending-work.md`.
//!
//! What the C API does expose is the close-reason value itself, `dwnx_ccerr`, together with
//! constructors for its three kinds. Those are wrapped here, because they are how a caller
//! describes *why* a connection ended, and because the mapping from a dwnx error code to a
//! QUIC transport error code is a piece of protocol knowledge worth borrowing rather than
//! reimplementing.

use ngnet_qmux_sys as sys;

use core::mem::MaybeUninit;

use crate::error::NativeCode;

/// The kind of close a [`CloseReason`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseKind {
    /// A transport-level error, carrying a QUIC transport error code.
    Transport,
    /// An application-level error, carrying a code the application defines.
    Application,
    /// The connection ended because it was idle, rather than because of an error.
    IdleClose,
    /// A kind this crate does not recognise.
    Unknown,
}

/// Why a connection ended.
///
/// The reason phrase is bounded and owned: dwnx's `dwnx_ccerr` holds a borrowed pointer with
/// no lifetime attached, which is not something to hand a Rust caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseReason {
    kind: CloseKind,
    error_code: u64,
    frame_type: u64,
    reason: Vec<u8>,
}

impl CloseReason {
    /// The default close reason: a transport close with no error.
    #[must_use]
    pub fn no_error() -> Self {
        let mut ccerr = MaybeUninit::<sys::dwnx_ccerr>::uninit();
        // SAFETY: `dwnx_ccerr_default` fully initialises the struct it is given.
        let ccerr = unsafe {
            sys::dwnx_ccerr_default(ccerr.as_mut_ptr());
            ccerr.assume_init()
        };
        Self::from_native(&ccerr)
    }

    /// A transport-level close with an explicit QUIC transport error code.
    #[must_use]
    pub fn transport(error_code: u64, reason: &[u8]) -> Self {
        Self::build(reason, |ccerr, ptr, len| {
            // SAFETY: `ccerr` is a valid uninitialised struct, and `ptr`/`len` describe a slice
            // that outlives the call. dwnx copies neither -- it stores the pointer -- which is
            // why the bytes are copied out again immediately in `build`.
            unsafe { sys::dwnx_ccerr_set_transport_error(ccerr, error_code, ptr, len) }
        })
    }

    /// An application-level close with a code the application defines.
    #[must_use]
    pub fn application(error_code: u64, reason: &[u8]) -> Self {
        Self::build(reason, |ccerr, ptr, len| {
            // SAFETY: as above.
            unsafe { sys::dwnx_ccerr_set_application_error(ccerr, error_code, ptr, len) }
        })
    }

    /// The close reason dwnx infers for one of its own error codes.
    ///
    /// This is the interesting constructor: dwnx knows which of its conditions map to which
    /// QUIC transport error codes, and that `IDLE_CLOSE` is not an error at all but its own
    /// kind of close. Deferring to it avoids re-deriving a table that already exists.
    #[must_use]
    pub fn from_native_error(code: NativeCode, reason: &[u8]) -> Self {
        Self::build(reason, |ccerr, ptr, len| {
            // SAFETY: as above.
            unsafe { sys::dwnx_ccerr_set_liberr(ccerr, code.get(), ptr, len) }
        })
    }

    /// The kind of close.
    #[must_use]
    pub const fn kind(&self) -> CloseKind {
        self.kind
    }

    /// The error code, whose meaning depends on [`CloseReason::kind`].
    #[must_use]
    pub const fn error_code(&self) -> u64 {
        self.error_code
    }

    /// The frame type that triggered the close, where one applies.
    #[must_use]
    pub const fn frame_type(&self) -> u64 {
        self.frame_type
    }

    /// The reason phrase, which may be empty and is not required to be UTF-8.
    #[must_use]
    pub fn reason(&self) -> &[u8] {
        &self.reason
    }

    /// Assembles a close reason from fields decoded off the wire.
    ///
    /// The wire is the one source these four fields can come from that dwnx's own constructors
    /// cannot reproduce: they set `frame_type` to zero and offer no way to change it, so a
    /// transport close naming the frame that provoked it would arrive here with that field
    /// lost. Crate-private and compiled only with the `io` layer, so the sans-I/O crate's
    /// public API is what it was.
    #[cfg(feature = "io")]
    pub(crate) fn from_parts(
        kind: CloseKind,
        error_code: u64,
        frame_type: u64,
        reason: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            error_code,
            frame_type,
            reason,
        }
    }

    /// Copy a `dwnx_ccerr` into an owned Rust value.
    pub(crate) fn from_native(ccerr: &sys::dwnx_ccerr) -> Self {
        let reason = if ccerr.reason.is_null() || ccerr.reasonlen == 0 {
            Vec::new()
        } else {
            // SAFETY: dwnx guarantees `reason` points to `reasonlen` readable bytes when both
            // are non-empty. The bytes are copied here so nothing outlives the borrow.
            unsafe { core::slice::from_raw_parts(ccerr.reason, ccerr.reasonlen) }.to_vec()
        };

        Self {
            kind: match ccerr.type_ {
                sys::DWNX_CCERR_TYPE_TRANSPORT => CloseKind::Transport,
                sys::DWNX_CCERR_TYPE_APPLICATION => CloseKind::Application,
                sys::DWNX_CCERR_TYPE_IDLE_CLOSE => CloseKind::IdleClose,
                _ => CloseKind::Unknown,
            },
            error_code: ccerr.error_code,
            frame_type: ccerr.frame_type,
            reason,
        }
    }

    /// Run one of dwnx's setters and copy the result out.
    fn build(reason: &[u8], set: impl FnOnce(*mut sys::dwnx_ccerr, *const u8, usize)) -> Self {
        let mut ccerr = MaybeUninit::<sys::dwnx_ccerr>::uninit();
        set(ccerr.as_mut_ptr(), reason.as_ptr(), reason.len());
        // SAFETY: every dwnx setter fully initialises the struct.
        let ccerr = unsafe { ccerr.assume_init() };
        Self::from_native(&ccerr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_a_transport_close_with_no_error() {
        let reason = CloseReason::no_error();
        assert_eq!(reason.kind(), CloseKind::Transport);
        assert_eq!(reason.error_code(), u64::from(sys::DWNX_NO_ERROR));
        assert!(reason.reason().is_empty());
    }

    #[test]
    fn each_kind_round_trips() {
        let transport = CloseReason::transport(0x0a, b"bad frame");
        assert_eq!(transport.kind(), CloseKind::Transport);
        assert_eq!(transport.error_code(), 0x0a);
        assert_eq!(transport.reason(), b"bad frame");

        let application = CloseReason::application(42, b"done");
        assert_eq!(application.kind(), CloseKind::Application);
        assert_eq!(application.error_code(), 42);
        assert_eq!(application.reason(), b"done");
    }

    /// `IDLE_CLOSE` is the one dwnx error that becomes its own close kind rather than a
    /// transport code; everything else is inferred into the transport space.
    #[test]
    fn idle_close_is_its_own_kind() {
        let idle = CloseReason::from_native_error(NativeCode::new(sys::DWNX_ERR_IDLE_CLOSE), b"");
        assert_eq!(idle.kind(), CloseKind::IdleClose);

        let flow = CloseReason::from_native_error(NativeCode::new(sys::DWNX_ERR_FLOW_CONTROL), b"");
        assert_eq!(flow.kind(), CloseKind::Transport);
        assert_eq!(flow.error_code(), u64::from(sys::DWNX_FLOW_CONTROL_ERROR));
    }

    /// The reason phrase is copied, not borrowed: dwnx stores the pointer it is given.
    #[test]
    fn reason_phrase_is_owned() {
        let reason = {
            let scratch = b"transient".to_vec();
            CloseReason::application(1, &scratch)
        };
        assert_eq!(reason.reason(), b"transient");
    }
}
