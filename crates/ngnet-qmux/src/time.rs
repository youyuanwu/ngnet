//! Timestamps and durations.
//!
//! dwnx measures both in nanoseconds as a bare `uint64_t`, and takes a timestamp on every
//! entry point that can advance connection state. It never reads a clock itself -- a sans-I/O
//! library has none to read -- so the caller supplies one, and is responsible for it being
//! monotonic.
//!
//! These are newtypes rather than aliases so that a duration cannot be passed where a
//! timestamp is wanted. The units come from `wrapper.h`, which restates dwnx's cast-style
//! macros that bindgen would otherwise drop; note they arrive with the width of their literal
//! rather than of `dwnx_duration`, which is why each is widened here.

use ngnet_qmux_sys as sys;

/// A point in time, in nanoseconds, on a clock the caller chooses.
///
/// The origin is arbitrary and dwnx never compares one against wall-clock time; only the
/// differences matter, and they must not go backwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Wrap a nanosecond reading.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// The reading, in nanoseconds.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

/// A span of time, in nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(u64);

impl Duration {
    /// One nanosecond.
    pub const NANOSECOND: Self = Self(sys::NGNET_QMUX_NANOSECONDS as u64);
    /// One microsecond.
    pub const MICROSECOND: Self = Self(sys::NGNET_QMUX_MICROSECONDS as u64);
    /// One millisecond.
    pub const MILLISECOND: Self = Self(sys::NGNET_QMUX_MILLISECONDS as u64);
    /// One second.
    pub const SECOND: Self = Self(sys::NGNET_QMUX_SECONDS as u64);
    /// One minute.
    pub const MINUTE: Self = Self(sys::NGNET_QMUX_MINUTES);

    /// Wrap a nanosecond count.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Build a duration from a count of milliseconds.
    ///
    /// Saturating, because dwnx rejects a `max_idle_timeout` of `u64::MAX` outright and an
    /// overflow that wrapped to a small value would be worse than one that clamps.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(sys::NGNET_QMUX_MILLISECONDS as u64))
    }

    /// Build a duration from a count of seconds.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(sys::NGNET_QMUX_SECONDS as u64))
    }

    /// The span, in nanoseconds.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_relate_as_expected() {
        assert_eq!(Duration::MICROSECOND.as_nanos(), 1_000);
        assert_eq!(Duration::MILLISECOND.as_nanos(), 1_000_000);
        assert_eq!(Duration::SECOND.as_nanos(), 1_000_000_000);
        assert_eq!(Duration::MINUTE.as_nanos(), 60 * Duration::SECOND.as_nanos());
    }

    #[test]
    fn constructors_scale_correctly() {
        assert_eq!(Duration::from_millis(250).as_nanos(), 250_000_000);
        assert_eq!(Duration::from_secs(30), Duration::from_millis(30_000));
    }

    /// The saturating multiply, which exists so a careless caller clamps rather than wraps.
    #[test]
    fn oversized_durations_saturate() {
        assert_eq!(Duration::from_secs(u64::MAX).as_nanos(), u64::MAX);
    }
}
