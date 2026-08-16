//! Connection settings.
//!
//! dwnx's `dwnx_settings` is small: a connection id used only for logging, the timestamp the
//! connection starts from, and an optional log callback. It is copied by the constructor, so
//! nothing here has to outlive the call.

use ngnet_qmux_sys as sys;

use core::mem::MaybeUninit;

use crate::time::Timestamp;

/// Local, connection-scoped configuration.
///
/// Starts from dwnx's own defaults, which are all-zero.
#[derive(Clone, Debug)]
pub struct Settings {
    raw: sys::dwnx_settings,
}

impl Settings {
    /// dwnx's default settings.
    #[must_use]
    pub fn new() -> Self {
        let mut raw = MaybeUninit::<sys::dwnx_settings>::uninit();
        // SAFETY: `dwnx_settings_default` fully initialises the struct it is given.
        let raw = unsafe {
            sys::dwnx_settings_default(raw.as_mut_ptr());
            raw.assume_init()
        };
        Self { raw }
    }

    /// Set the connection id, which dwnx uses only to label log output.
    #[must_use]
    pub const fn with_conn_id(mut self, conn_id: u64) -> Self {
        self.raw.conn_id = conn_id;
        self
    }

    /// Set the timestamp the connection starts from.
    #[must_use]
    pub const fn with_initial_timestamp(mut self, ts: Timestamp) -> Self {
        self.raw.initial_ts = ts.as_nanos();
        self
    }

    /// The connection id.
    #[must_use]
    pub const fn conn_id(&self) -> u64 {
        self.raw.conn_id
    }

    /// The initial timestamp.
    #[must_use]
    pub const fn initial_timestamp(&self) -> Timestamp {
        Timestamp::from_nanos(self.raw.initial_ts)
    }

    /// The raw struct, for handing to a dwnx constructor.
    ///
    /// `log_write` is left null. Bridging it to a Rust logging closure is possible and is
    /// noted as future work; it is not wired up here because it would be the crate's only
    /// callback that is not a protocol event, and it is not needed to speak the protocol.
    pub(crate) const fn as_raw(&self) -> &sys::dwnx_settings {
        &self.raw
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_c_library() {
        let settings = Settings::new();
        assert_eq!(settings.conn_id(), 0);
        assert_eq!(settings.initial_timestamp(), Timestamp::from_nanos(0));
        assert!(settings.as_raw().log_write.is_none());
    }

    #[test]
    fn builders_set_what_they_say() {
        let settings = Settings::new()
            .with_conn_id(7)
            .with_initial_timestamp(Timestamp::from_nanos(1_234));
        assert_eq!(settings.conn_id(), 7);
        assert_eq!(settings.initial_timestamp().as_nanos(), 1_234);
    }
}
