//! Owned wrapper over `nghttp2_option`.
//!
//! Options only influence session construction; libnghttp2 does not retain the pointer
//! afterwards, so this lives just long enough to build a session.

use ngnet_h2_sys as sys;

use crate::error::{Error, Result};

pub(crate) struct Options {
    raw: *mut sys::nghttp2_option,
}

impl Options {
    pub(crate) fn new() -> Result<Self> {
        let mut raw: *mut sys::nghttp2_option = core::ptr::null_mut();
        // SAFETY: `raw` is a valid out-parameter. On success the callee stores a freshly
        // allocated option object in it, which `Drop` releases.
        let rc = unsafe { sys::nghttp2_option_new(&mut raw) };
        if rc != 0 {
            return Err(Error::from_native("nghttp2_option_new", rc));
        }
        debug_assert!(!raw.is_null());
        Ok(Self { raw })
    }

    /// Stops libnghttp2 replenishing flow-control windows on its own.
    ///
    /// Only enabled when the caller opts into manual flow control. Enabling it
    /// unconditionally would stall any connection whose owner never reports consumption.
    pub(crate) fn set_no_auto_window_update(&mut self, enabled: bool) {
        // SAFETY: `self.raw` is a live option object owned by `self`.
        unsafe { sys::nghttp2_option_set_no_auto_window_update(self.raw, i32::from(enabled)) };
    }

    pub(crate) fn as_ptr(&self) -> *const sys::nghttp2_option {
        self.raw
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        // SAFETY: `self.raw` was produced by `nghttp2_option_new` and is not aliased.
        // `nghttp2_option_del` is null-safe regardless.
        unsafe { sys::nghttp2_option_del(self.raw) };
    }
}
