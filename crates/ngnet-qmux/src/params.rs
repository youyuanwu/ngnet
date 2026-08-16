//! Transport parameters.
//!
//! # The defaults are not usable defaults
//!
//! [`TransportParams::new`] returns exactly what `dwnx_transport_params_default` produces,
//! which sets `max_record_size` and leaves every flow-control and stream limit at zero. A
//! connection built from them is legal and constructs fine -- it simply cannot carry any
//! application data, because it has advertised permission for none.
//!
//! `ngnet-quic` solves the equivalent ngtcp2 problem by overlaying its own working values on
//! top. This crate deliberately does not: "the defaults" should mean the library's defaults,
//! not a second set invented here that disagrees with the C documentation. A caller raises the
//! limits, and the connection-level and stream-level extension operations can raise them
//! further once running.
//!
//! # Why validation exists at all
//!
//! dwnx guards its constructor preconditions with `assert`, not with error returns
//! (`dwnx_conn.c`). A `NULL` callbacks pointer, a `max_idle_timeout` of `UINT64_MAX`, a
//! `max_record_size` below the minimum -- each aborts the process in a debug build rather than
//! returning a code. An abort is not something a caller can handle, so [`TransportParams`]
//! checks the same conditions first and reports them as ordinary errors.
//!
//! # One field is not honoured
//!
//! dwnx overwrites `max_record_size` with `DWNX_DEFAULT_MAX_RECORD_SIZE` immediately after
//! copying the parameters in, with the comment "We do not let application increase max record
//! size". Configuring it therefore has no effect on the connection, and readback reports the
//! library's value rather than the caller's. It is still validated, because the assertion runs
//! before the overwrite.

use ngnet_qmux_sys as sys;

use core::mem::MaybeUninit;

use crate::error::{Error, ErrorKind};
use crate::time::Duration;

/// The maximum value representable as a QUIC variable-length integer.
const MAX_VARINT: u64 = sys::NGNET_QMUX_MAX_VARINT;

/// The parameters advertised to, or received from, the peer.
#[derive(Clone, Debug)]
pub struct TransportParams {
    raw: sys::dwnx_transport_params,
}

// Compared field by field rather than derived. bindgen does not derive `PartialEq` on the C
// struct, and deriving it here by delegating would compare whatever padding the C compiler
// inserted along with the values. Only the eight documented fields carry meaning.
impl PartialEq for TransportParams {
    fn eq(&self, other: &Self) -> bool {
        self.raw.initial_max_stream_data_bidi_local
            == other.raw.initial_max_stream_data_bidi_local
            && self.raw.initial_max_stream_data_bidi_remote
                == other.raw.initial_max_stream_data_bidi_remote
            && self.raw.initial_max_stream_data_uni == other.raw.initial_max_stream_data_uni
            && self.raw.initial_max_data == other.raw.initial_max_data
            && self.raw.initial_max_streams_bidi == other.raw.initial_max_streams_bidi
            && self.raw.initial_max_streams_uni == other.raw.initial_max_streams_uni
            && self.raw.max_idle_timeout == other.raw.max_idle_timeout
            && self.raw.max_record_size == other.raw.max_record_size
    }
}

impl Eq for TransportParams {}

impl TransportParams {
    /// dwnx's own defaults.
    ///
    /// Every limit is zero; see the module documentation for why that is reproduced faithfully
    /// rather than improved upon.
    #[must_use]
    pub fn new() -> Self {
        let mut raw = MaybeUninit::<sys::dwnx_transport_params>::uninit();
        // SAFETY: `dwnx_transport_params_default` fully initialises the struct.
        let raw = unsafe {
            sys::dwnx_transport_params_default(raw.as_mut_ptr());
            raw.assume_init()
        };
        Self { raw }
    }

    /// Permitted incoming data on locally-initiated bidirectional streams.
    #[must_use]
    pub const fn with_initial_max_stream_data_bidi_local(mut self, value: u64) -> Self {
        self.raw.initial_max_stream_data_bidi_local = value;
        self
    }

    /// Permitted incoming data on remotely-initiated bidirectional streams.
    #[must_use]
    pub const fn with_initial_max_stream_data_bidi_remote(mut self, value: u64) -> Self {
        self.raw.initial_max_stream_data_bidi_remote = value;
        self
    }

    /// Permitted incoming data on unidirectional streams.
    #[must_use]
    pub const fn with_initial_max_stream_data_uni(mut self, value: u64) -> Self {
        self.raw.initial_max_stream_data_uni = value;
        self
    }

    /// Permitted incoming data across the whole connection.
    #[must_use]
    pub const fn with_initial_max_data(mut self, value: u64) -> Self {
        self.raw.initial_max_data = value;
        self
    }

    /// How many bidirectional streams the peer may open.
    #[must_use]
    pub const fn with_initial_max_streams_bidi(mut self, value: u64) -> Self {
        self.raw.initial_max_streams_bidi = value;
        self
    }

    /// How many unidirectional streams the peer may open.
    #[must_use]
    pub const fn with_initial_max_streams_uni(mut self, value: u64) -> Self {
        self.raw.initial_max_streams_uni = value;
        self
    }

    /// How long the connection may sit idle before closing.
    #[must_use]
    pub const fn with_max_idle_timeout(mut self, timeout: Duration) -> Self {
        self.raw.max_idle_timeout = timeout.as_nanos();
        self
    }

    /// Set every limit to a single value.
    ///
    /// A convenience for the common case of "permit a reasonable amount of everything", and
    /// for tests, which would otherwise repeat six builder calls to get a connection that can
    /// carry anything at all.
    #[must_use]
    pub const fn with_all_limits(self, data: u64, streams: u64) -> Self {
        self.with_initial_max_stream_data_bidi_local(data)
            .with_initial_max_stream_data_bidi_remote(data)
            .with_initial_max_stream_data_uni(data)
            .with_initial_max_data(data)
            .with_initial_max_streams_bidi(streams)
            .with_initial_max_streams_uni(streams)
    }

    /// Permitted incoming data on locally-initiated bidirectional streams.
    #[must_use]
    pub const fn initial_max_stream_data_bidi_local(&self) -> u64 {
        self.raw.initial_max_stream_data_bidi_local
    }

    /// Permitted incoming data on remotely-initiated bidirectional streams.
    #[must_use]
    pub const fn initial_max_stream_data_bidi_remote(&self) -> u64 {
        self.raw.initial_max_stream_data_bidi_remote
    }

    /// Permitted incoming data on unidirectional streams.
    #[must_use]
    pub const fn initial_max_stream_data_uni(&self) -> u64 {
        self.raw.initial_max_stream_data_uni
    }

    /// Permitted incoming data across the whole connection.
    #[must_use]
    pub const fn initial_max_data(&self) -> u64 {
        self.raw.initial_max_data
    }

    /// How many bidirectional streams the peer may open.
    #[must_use]
    pub const fn initial_max_streams_bidi(&self) -> u64 {
        self.raw.initial_max_streams_bidi
    }

    /// How many unidirectional streams the peer may open.
    #[must_use]
    pub const fn initial_max_streams_uni(&self) -> u64 {
        self.raw.initial_max_streams_uni
    }

    /// How long the connection may sit idle before closing.
    #[must_use]
    pub const fn max_idle_timeout(&self) -> Duration {
        Duration::from_nanos(self.raw.max_idle_timeout)
    }

    /// The maximum record size.
    ///
    /// Note that configuring this has no effect: dwnx overwrites it at construction. See the
    /// module documentation.
    #[must_use]
    pub const fn max_record_size(&self) -> u64 {
        self.raw.max_record_size
    }

    /// Check the preconditions dwnx asserts rather than reports.
    ///
    /// # Errors
    ///
    /// Returns an error if any value would trip an assertion in `dwnx_conn_client_new` or
    /// `dwnx_conn_server_new`, which would otherwise abort the process.
    pub fn validate(&self) -> Result<(), Error> {
        // The five limits dwnx asserts against the varint bound.
        for (value, what) in [
            (
                self.raw.initial_max_stream_data_bidi_local,
                "initial_max_stream_data_bidi_local exceeds the varint bound",
            ),
            (
                self.raw.initial_max_stream_data_bidi_remote,
                "initial_max_stream_data_bidi_remote exceeds the varint bound",
            ),
            (
                self.raw.initial_max_data,
                "initial_max_data exceeds the varint bound",
            ),
            (
                self.raw.initial_max_streams_bidi,
                "initial_max_streams_bidi exceeds the varint bound",
            ),
            (
                self.raw.initial_max_streams_uni,
                "initial_max_streams_uni exceeds the varint bound",
            ),
            // Not asserted by the constructor -- an upstream oversight, since its siblings are
            // -- but asserted later when the parameter is encoded into a frame. Checked here
            // so the abort happens nowhere.
            (
                self.raw.initial_max_stream_data_uni,
                "initial_max_stream_data_uni exceeds the varint bound",
            ),
        ] {
            if value > MAX_VARINT {
                return Err(Error::validation(ErrorKind::InvalidArgument, what));
            }
        }

        if self.raw.max_idle_timeout == u64::MAX {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "max_idle_timeout must not be u64::MAX",
            ));
        }
        if self.raw.max_idle_timeout / u64::from(sys::NGNET_QMUX_MILLISECONDS) > MAX_VARINT {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "max_idle_timeout in milliseconds exceeds the varint bound",
            ));
        }

        if self.raw.max_record_size > MAX_VARINT {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "max_record_size exceeds the varint bound",
            ));
        }
        if self.raw.max_record_size < u64::from(sys::DWNX_DEFAULT_MAX_RECORD_SIZE) {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "max_record_size is below the protocol minimum",
            ));
        }

        Ok(())
    }

    /// The raw struct, for handing to a dwnx constructor.
    pub(crate) const fn as_raw(&self) -> &sys::dwnx_transport_params {
        &self.raw
    }

    /// Copy a `dwnx_transport_params` into an owned Rust value.
    pub(crate) const fn from_native(raw: sys::dwnx_transport_params) -> Self {
        Self { raw }
    }
}

impl Default for TransportParams {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults are dwnx's, zeros and all. Pinned because it is surprising, and because
    /// the alternative -- quietly substituting workable values -- is what this crate chose not
    /// to do.
    #[test]
    fn defaults_are_the_c_defaults_including_the_zeros() {
        let params = TransportParams::new();

        assert_eq!(params.initial_max_stream_data_bidi_local(), 0);
        assert_eq!(params.initial_max_stream_data_bidi_remote(), 0);
        assert_eq!(params.initial_max_stream_data_uni(), 0);
        assert_eq!(params.initial_max_data(), 0);
        assert_eq!(params.initial_max_streams_bidi(), 0);
        assert_eq!(params.initial_max_streams_uni(), 0);
        assert_eq!(params.max_idle_timeout(), Duration::from_nanos(0));
        assert_eq!(
            params.max_record_size(),
            u64::from(sys::DWNX_DEFAULT_MAX_RECORD_SIZE)
        );
    }

    /// The unmodified defaults must pass validation: C accepts them, so this crate must too.
    #[test]
    fn defaults_validate() {
        TransportParams::new().validate().unwrap();
    }

    #[test]
    fn builders_set_what_they_say() {
        let params = TransportParams::new()
            .with_initial_max_data(1 << 20)
            .with_initial_max_streams_bidi(16)
            .with_max_idle_timeout(Duration::from_secs(30));

        assert_eq!(params.initial_max_data(), 1 << 20);
        assert_eq!(params.initial_max_streams_bidi(), 16);
        assert_eq!(params.max_idle_timeout(), Duration::from_secs(30));
        params.validate().unwrap();
    }

    /// Each of these would trip a C `assert` and abort rather than return an error.
    #[test]
    fn validation_catches_what_dwnx_would_assert() {
        let over = MAX_VARINT + 1;

        assert!(
            TransportParams::new()
                .with_initial_max_data(over)
                .validate()
                .is_err()
        );
        assert!(
            TransportParams::new()
                .with_initial_max_streams_bidi(over)
                .validate()
                .is_err()
        );
        assert!(
            TransportParams::new()
                .with_initial_max_stream_data_bidi_local(over)
                .validate()
                .is_err()
        );
        assert!(
            TransportParams::new()
                .with_max_idle_timeout(Duration::from_nanos(u64::MAX))
                .validate()
                .is_err()
        );
    }

    /// The sibling the constructor forgets to assert, checked anyway.
    #[test]
    fn validation_covers_the_unasserted_sibling() {
        assert!(
            TransportParams::new()
                .with_initial_max_stream_data_uni(MAX_VARINT + 1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn all_limits_sets_every_limit() {
        let params = TransportParams::new().with_all_limits(1 << 16, 8);

        assert_eq!(params.initial_max_data(), 1 << 16);
        assert_eq!(params.initial_max_stream_data_uni(), 1 << 16);
        assert_eq!(params.initial_max_streams_uni(), 8);
        params.validate().unwrap();
    }
}
