//! Stream identity.
//!
//! nghttp3 names streams by their QUIC wire identifier (RFC 9000 §2.1), and validates the
//! ones it is given only with `assert` — which a release build compiles out. Passing a
//! bidirectional identifier where a unidirectional one is required, or one above the
//! varint range, is therefore not an error in a release build but undefined behaviour.
//!
//! [`StreamId`] exists to make that unreachable. Its inner value is private and every API
//! in this crate that names a stream takes a `StreamId`, so the range check happens once,
//! at construction, and cannot be bypassed.

use core::fmt;

use crate::error::{Error, Result};

/// The largest legal QUIC stream identifier: a 62-bit varint (RFC 9000 §16).
const MAX_VARINT: i64 = (1 << 62) - 1;

/// Which endpoint opened a stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Initiator {
    /// Opened by the client.
    Client,
    /// Opened by the server.
    Server,
}

/// Whether a stream carries data in one direction or both.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Directionality {
    /// Both endpoints may send.
    Bidirectional,
    /// Only the initiator may send.
    Unidirectional,
}

/// A validated QUIC stream identifier.
///
/// The low two bits encode the initiator and directionality, so a stream's role is
/// derivable from its number alone — which is why [`Self::initiator`] and
/// [`Self::directionality`] need no additional state.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(i64);

impl StreamId {
    /// Wraps a raw QUIC stream identifier.
    ///
    /// Rejects negative values and values above the 62-bit varint maximum. Both are
    /// conditions nghttp3 checks only with `assert`, so rejecting them here is what makes
    /// every other entry point in this crate sound rather than merely tidy.
    pub const fn new(id: i64) -> Result<Self> {
        if id < 0 {
            return Err(Error::invalid_input("stream id is negative"));
        }
        if id > MAX_VARINT {
            return Err(Error::invalid_input(
                "stream id exceeds the QUIC varint maximum of 2^62 - 1",
            ));
        }
        Ok(Self(id))
    }

    /// Builds an identifier from its parts.
    pub const fn compose(
        initiator: Initiator,
        directionality: Directionality,
        index: u64,
    ) -> Result<Self> {
        let initiator_bit = match initiator {
            Initiator::Client => 0,
            Initiator::Server => 1,
        };
        let direction_bit = match directionality {
            Directionality::Bidirectional => 0,
            Directionality::Unidirectional => 1,
        };
        if index > (MAX_VARINT as u64) >> 2 {
            return Err(Error::invalid_input("stream index is too large"));
        }
        Self::new(((index << 2) | (direction_bit << 1) | initiator_bit) as i64)
    }

    /// The raw identifier, as nghttp3 and QUIC both use it.
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Which endpoint opened this stream.
    pub const fn initiator(self) -> Initiator {
        if self.0 & 0x1 == 0 {
            Initiator::Client
        } else {
            Initiator::Server
        }
    }

    /// Whether this stream is bidirectional or unidirectional.
    pub const fn directionality(self) -> Directionality {
        if self.0 & 0x2 == 0 {
            Directionality::Bidirectional
        } else {
            Directionality::Unidirectional
        }
    }

    /// Whether this stream was opened by `role` and is unidirectional.
    ///
    /// This is exactly the predicate nghttp3 asserts when a stream is declared for a
    /// connection-level role.
    pub(crate) const fn is_local_unidirectional(self, role: Initiator) -> bool {
        matches!(self.directionality(), Directionality::Unidirectional)
            && matches!(
                (self.initiator(), role),
                (Initiator::Client, Initiator::Client) | (Initiator::Server, Initiator::Server)
            )
    }

    /// Whether this stream is a request stream opened by the client.
    ///
    /// nghttp3 asserts this when a request is submitted and when it decides whether a
    /// stream can carry peer data, and asserts are compiled out of a release build, so
    /// this crate checks it instead.
    pub(crate) const fn is_client_bidirectional(self) -> bool {
        matches!(self.directionality(), Directionality::Bidirectional)
            && matches!(self.initiator(), Initiator::Client)
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let direction = match self.directionality() {
            Directionality::Bidirectional => "bidi",
            Directionality::Unidirectional => "uni",
        };
        let initiator = match self.initiator() {
            Initiator::Client => "client",
            Initiator::Server => "server",
        };
        write!(f, "StreamId({}, {initiator} {direction})", self.0)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_low_bits_decode_the_role() {
        // RFC 9000 §2.1 table.
        let cases = [
            (0, Initiator::Client, Directionality::Bidirectional),
            (1, Initiator::Server, Directionality::Bidirectional),
            (2, Initiator::Client, Directionality::Unidirectional),
            (3, Initiator::Server, Directionality::Unidirectional),
        ];
        for (raw, initiator, directionality) in cases {
            let id = StreamId::new(raw).unwrap();
            assert_eq!(id.initiator(), initiator, "id {raw}");
            assert_eq!(id.directionality(), directionality, "id {raw}");
        }
    }

    #[test]
    fn compose_round_trips() {
        let id = StreamId::compose(Initiator::Client, Directionality::Unidirectional, 3).unwrap();
        assert_eq!(id.get(), 14);
        assert_eq!(id.initiator(), Initiator::Client);
        assert_eq!(id.directionality(), Directionality::Unidirectional);
    }

    #[test]
    fn negative_ids_are_rejected() {
        assert!(StreamId::new(-1).is_err());
    }

    #[test]
    fn ids_above_the_varint_maximum_are_rejected() {
        assert!(StreamId::new(MAX_VARINT).is_ok());
        assert!(StreamId::new(MAX_VARINT + 1).is_err());
        assert!(StreamId::new(i64::MAX).is_err());
    }

    #[test]
    fn role_predicates_match_the_c_assertions() {
        let client_uni = StreamId::new(2).unwrap();
        let server_uni = StreamId::new(3).unwrap();
        let client_bidi = StreamId::new(0).unwrap();

        assert!(client_uni.is_local_unidirectional(Initiator::Client));
        assert!(!client_uni.is_local_unidirectional(Initiator::Server));
        assert!(server_uni.is_local_unidirectional(Initiator::Server));
        assert!(!client_bidi.is_local_unidirectional(Initiator::Client));

        assert!(client_bidi.is_client_bidirectional());
        assert!(!client_uni.is_client_bidirectional());
    }
}
