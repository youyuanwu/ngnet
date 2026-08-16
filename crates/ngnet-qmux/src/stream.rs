//! Stream identifiers.
//!
//! QMux reuses QUIC's stream-id encoding unchanged: the low two bits carry the initiator and
//! the directionality, and the rest is a counter. That means the interesting questions about a
//! stream -- who opened it, whether it is bidirectional -- are answerable from the number
//! itself, with no call into dwnx and no connection needed.

use ngnet_qmux_sys as sys;

use crate::error::{Error, ErrorKind};

/// Which endpoint opened a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Initiator {
    /// Opened by the client.
    Client,
    /// Opened by the server.
    Server,
}

/// Whether a stream carries data in one direction or both.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Directionality {
    /// Both endpoints can send.
    Bidirectional,
    /// Only the initiator can send.
    Unidirectional,
}

/// A QMux stream identifier.
///
/// Validated on construction: dwnx passes stream ids as `int64_t`, but the wire encoding is a
/// variable-length integer, so the usable range stops at `2^62 - 1` and negative values are
/// not identifiers at all. Rejecting them here means the rest of the crate can pass a
/// `StreamId` to C without rechecking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(i64);

impl StreamId {
    /// The largest representable stream id.
    pub const MAX: i64 = sys::NGNET_QMUX_MAX_VARINT as i64;

    /// Wrap a raw stream id.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is negative or exceeds the variable-length integer bound.
    pub fn new(id: i64) -> Result<Self, Error> {
        if id < 0 {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "stream id is negative",
            ));
        }
        if id > Self::MAX {
            return Err(Error::validation(
                ErrorKind::InvalidArgument,
                "stream id exceeds the varint bound",
            ));
        }
        Ok(Self(id))
    }

    /// The raw value, for handing to dwnx.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Which endpoint opened this stream.
    #[must_use]
    pub const fn initiator(self) -> Initiator {
        if self.0 & 0x1 == 0 {
            Initiator::Client
        } else {
            Initiator::Server
        }
    }

    /// Whether this stream is bidirectional.
    #[must_use]
    pub const fn directionality(self) -> Directionality {
        if self.0 & 0x2 == 0 {
            Directionality::Bidirectional
        } else {
            Directionality::Unidirectional
        }
    }

    /// Whether this stream is bidirectional, as a bool.
    #[must_use]
    pub const fn is_bidirectional(self) -> bool {
        matches!(self.directionality(), Directionality::Bidirectional)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_bits_decode_the_quic_encoding() {
        assert_eq!(StreamId::new(0).unwrap().initiator(), Initiator::Client);
        assert_eq!(StreamId::new(1).unwrap().initiator(), Initiator::Server);
        assert!(StreamId::new(0).unwrap().is_bidirectional());
        assert!(StreamId::new(1).unwrap().is_bidirectional());
        assert!(!StreamId::new(2).unwrap().is_bidirectional());
        assert!(!StreamId::new(3).unwrap().is_bidirectional());
    }

    /// The local decoding must agree with dwnx's own helper, or one of them is wrong.
    #[test]
    fn agrees_with_dwnx_helper() {
        for id in 0..64i64 {
            let ours = StreamId::new(id).unwrap().is_bidirectional();
            // SAFETY: a pure function over an integer.
            let theirs = unsafe { sys::dwnx_is_bidi_stream(id) } != 0;
            assert_eq!(ours, theirs, "disagreement on stream {id}");
        }
    }

    #[test]
    fn rejects_out_of_range_ids() {
        assert!(StreamId::new(-1).is_err());
        assert!(StreamId::new(StreamId::MAX + 1).is_err());
        assert!(StreamId::new(StreamId::MAX).is_ok());
    }
}
