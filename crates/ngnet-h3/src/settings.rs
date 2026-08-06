//! Connection settings.

use core::mem::MaybeUninit;

use ngnet_h3_sys as sys;

/// The largest value a QUIC varint can carry (RFC 9000 §16).
const MAX_VARINT: u64 = (1 << 62) - 1;

/// Clamps a size to the varint maximum, on platforms where `usize` can exceed it.
fn clamp_varint(value: usize) -> usize {
    // On a 64-bit target `MAX_VARINT` fits a `usize`; on a 32-bit one no `usize` can
    // exceed it, so the conversion below cannot fail in either direction.
    match usize::try_from(MAX_VARINT) {
        Ok(max) => value.min(max),
        Err(_) => value,
    }
}

/// The HTTP/3 SETTINGS this endpoint advertises.
///
/// Built from nghttp3's own defaults rather than from zero, so a field this crate does not
/// expose keeps whatever the library considers sensible.
#[derive(Clone)]
pub struct Settings {
    raw: sys::nghttp3_settings,
}

impl Settings {
    /// nghttp3's default settings.
    pub fn new() -> Self {
        let mut raw = MaybeUninit::<sys::nghttp3_settings>::uninit();
        // SAFETY: `nghttp3_settings_default_versioned` writes every byte of the struct
        // for the version it is given, so the value is fully initialised on return.
        unsafe {
            sys::nghttp3_settings_default_versioned(
                sys::NGHTTP3_SETTINGS_VERSION as i32,
                raw.as_mut_ptr(),
            );
        }
        // SAFETY: initialised by the call above.
        Self {
            raw: unsafe { raw.assume_init() },
        }
    }

    /// The largest field section this endpoint will accept, in bytes.
    ///
    /// Values are clamped to the QUIC varint maximum. nghttp3 checks that bound with an
    /// `assert`, which is not something a safe API may rely on — see the note on
    /// assertions in [`crate::Conn`].
    pub fn max_field_section_size(mut self, size: u64) -> Self {
        self.raw.max_field_section_size = size.min(MAX_VARINT);
        self
    }

    /// The QPACK dynamic table capacity this endpoint will accept.
    pub fn qpack_max_dtable_capacity(mut self, capacity: usize) -> Self {
        self.raw.qpack_max_dtable_capacity = clamp_varint(capacity);
        self
    }

    /// How many streams may be blocked awaiting QPACK insertions.
    pub fn qpack_blocked_streams(mut self, streams: usize) -> Self {
        self.raw.qpack_blocked_streams = clamp_varint(streams);
        self
    }

    /// Whether the extended CONNECT protocol is permitted.
    pub fn enable_connect_protocol(mut self, enabled: bool) -> Self {
        self.raw.enable_connect_protocol = u8::from(enabled);
        self
    }

    /// The raw struct to hand to a constructor.
    ///
    /// nghttp3 copies settings by value, so this need not outlive the call — unlike the
    /// allocator, which is stored by pointer.
    pub(crate) fn as_raw(&self) -> *const sys::nghttp3_settings {
        &self.raw
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for Settings {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Settings")
            .field("max_field_section_size", &self.raw.max_field_section_size)
            .field(
                "qpack_max_dtable_capacity",
                &self.raw.qpack_max_dtable_capacity,
            )
            .field("qpack_blocked_streams", &self.raw.qpack_blocked_streams)
            .field("enable_connect_protocol", &self.raw.enable_connect_protocol)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_come_from_the_library() {
        let settings = Settings::new();
        // nghttp3's documented default for the encoder's dynamic table.
        assert_eq!(settings.raw.qpack_encoder_max_dtable_capacity, 4096);
    }

    #[test]
    fn builders_apply() {
        let settings = Settings::new()
            .max_field_section_size(4096)
            .qpack_blocked_streams(7)
            .enable_connect_protocol(true);
        assert_eq!(settings.raw.max_field_section_size, 4096);
        assert_eq!(settings.raw.qpack_blocked_streams, 7);
        assert_eq!(settings.raw.enable_connect_protocol, 1);
    }
}
