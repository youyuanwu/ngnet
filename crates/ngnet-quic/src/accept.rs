//! Deciding what to do with a datagram before any connection exists.
//!
//! A server receives packets on one socket for many connections, including packets for
//! connections that do not exist yet. Something has to look at a datagram and decide: is
//! this a new connection, does it belong to an existing one, is its QUIC version supported,
//! should it be answered with a Retry?
//!
//! ngtcp2 answers those questions with free functions that take a byte slice, not methods on
//! a connection — because at that point there is no connection. This module wraps them.
//!
//! # This is not optional for a server
//!
//! `ngtcp2_conn_server_new` asserts that the transport parameters carry `original_dcid`
//! (`deps/ngtcp2/lib/ngtcp2_conn.c:1264-1265`), and that value comes from the client's first
//! packet. A server therefore *cannot be built* without decoding one first. Since the
//! assertion is compiled out of release builds, skipping this step is undefined behaviour
//! rather than a crash — which is why [`crate::TransportParams::build`] checks it too.

use ngnet_quic_sys as sys;

use crate::cid::ConnectionId;
use crate::error::{Error, Result};

/// What a datagram turned out to be.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Inspection {
    /// A long-header packet whose version this build supports.
    ///
    /// The connection IDs are usable and a connection may be built from them.
    Supported {
        /// The QUIC version the client chose.
        version: u32,
        /// The destination connection ID the client addressed.
        dcid: ConnectionId,
        /// The client's own source connection ID.
        scid: ConnectionId,
    },
    /// A long-header packet naming a QUIC version this build does not support.
    ///
    /// The identifiers are still present, and are exactly what a Version Negotiation packet
    /// must echo back — which is why this is a variant rather than an error. ngtcp2 signals
    /// it with an error return while still filling its output, and discarding that output
    /// would leave nothing to answer with.
    UnsupportedVersion {
        /// The version the client asked for.
        version: u32,
        /// The destination connection ID, to be echoed as the source.
        dcid: ConnectionId,
        /// The source connection ID, to be echoed as the destination.
        scid: ConnectionId,
    },
    /// A short-header packet, which belongs to a connection that already exists.
    ShortHeader {
        /// The destination connection ID, for routing to that connection.
        dcid: ConnectionId,
    },
}

/// Examines a datagram that arrived with no connection attached.
///
/// `short_dcidlen` is the length of connection IDs this server issues, which it needs
/// because short headers do not carry a length field.
///
/// # Errors
///
/// Returns [`ErrorKind::Protocol`] if the datagram is not a well-formed QUIC packet.
///
/// [`ErrorKind::Protocol`]: crate::ErrorKind::Protocol
pub fn inspect(datagram: &[u8], short_dcidlen: usize) -> Result<Inspection> {
    if short_dcidlen > crate::cid::MAX_LEN {
        return Err(Error::invalid_input(
            "short_dcidlen exceeds NGTCP2_MAX_CIDLEN",
        ));
    }

    let mut version_cid = sys::ngtcp2_version_cid {
        version: 0,
        dcid: core::ptr::null(),
        dcidlen: 0,
        scid: core::ptr::null(),
        scidlen: 0,
    };

    // SAFETY: `datagram` is readable for its length and `version_cid` is a valid
    // out-parameter. The identifiers it writes borrow into `datagram`, which outlives the
    // copies made below.
    let rc = unsafe {
        sys::ngtcp2_pkt_decode_version_cid(
            &mut version_cid,
            datagram.as_ptr(),
            datagram.len(),
            short_dcidlen,
        )
    };

    // Reads the borrowed identifiers out into owned ones. They point into `datagram`, so
    // this must happen before returning.
    let copy = |ptr: *const u8, len: usize| -> Result<ConnectionId> {
        if ptr.is_null() || len == 0 {
            return ConnectionId::new(&[]);
        }
        // SAFETY: ngtcp2 guarantees the pointer is readable for `len` bytes and lies within
        // `datagram`, which is still borrowed here.
        let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
        ConnectionId::new(bytes)
    };

    match rc {
        0 => {
            let dcid = copy(version_cid.dcid, version_cid.dcidlen)?;
            if version_cid.version == 0 {
                // A zero version with no source ID is the short-header case: there is no
                // version field on the wire to read.
                return Ok(Inspection::ShortHeader { dcid });
            }
            let scid = copy(version_cid.scid, version_cid.scidlen)?;
            Ok(Inspection::Supported {
                version: version_cid.version,
                dcid,
                scid,
            })
        }
        // Documented as an error return, but `dest` is filled regardless: "Unlike the other
        // error cases, all fields of |dest| are assigned as described above"
        // (`ngtcp2.h:2431-2476`). Mapping it to `Err` and discarding the output -- the
        // obvious Rust translation -- would throw away exactly what is needed to answer.
        sys::NGTCP2_ERR_VERSION_NEGOTIATION => Ok(Inspection::UnsupportedVersion {
            version: version_cid.version,
            dcid: copy(version_cid.dcid, version_cid.dcidlen)?,
            scid: copy(version_cid.scid, version_cid.scidlen)?,
        }),
        other => Err(Error::native(other, "could not decode the packet header")),
    }
}

/// Whether a datagram is acceptable as the very first packet of a new connection.
///
/// Returns `false` for anything that is not — a stray short header, a packet too small to
/// carry an Initial, a version this build does not support.
pub fn is_acceptable_initial(datagram: &[u8]) -> bool {
    // SAFETY: `datagram` is readable for its length; a null `dest` means "decide only".
    let rc =
        unsafe { sys::ngtcp2_accept(core::ptr::null_mut(), datagram.as_ptr(), datagram.len()) };
    rc == 0
}

/// Writes a Version Negotiation packet into `dest`.
///
/// Sent in response to [`Inspection::UnsupportedVersion`]. The connection IDs are
/// **swapped** relative to the packet that prompted it: the client's source becomes the
/// destination and vice versa.
///
/// `supported_versions` is what this build offers, in preference order.
///
/// # Errors
///
/// Returns a native error if `dest` is too small.
pub fn write_version_negotiation(
    dest: &mut [u8],
    unused_random: u8,
    dcid: &ConnectionId,
    scid: &ConnectionId,
    supported_versions: &[u32],
) -> Result<usize> {
    // SAFETY: `dest` is writable for its length, both identifiers are valid, and
    // `supported_versions` is readable for its length. Everything is copied.
    let written = unsafe {
        sys::ngtcp2_pkt_write_version_negotiation(
            dest.as_mut_ptr(),
            dest.len(),
            unused_random,
            dcid.as_bytes().as_ptr(),
            dcid.as_bytes().len(),
            scid.as_bytes().as_ptr(),
            scid.as_bytes().len(),
            supported_versions.as_ptr(),
            supported_versions.len(),
        )
    };
    if written < 0 {
        return Err(Error::native(
            written as i32,
            "could not write a Version Negotiation packet",
        ));
    }
    Ok(written as usize)
}

/// The QUIC version this build treats as primary.
pub const VERSION_V1: u32 = sys::NGTCP2_PROTO_VER_V1;

/// Every QUIC version this build supports, in preference order.
pub fn supported_versions() -> &'static [u32] {
    &[sys::NGTCP2_PROTO_VER_V1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    /// A minimal long-header Initial packet, enough for the header decoder.
    ///
    /// Built by hand rather than captured, so the version and identifier fields can be
    /// varied one at a time.
    fn long_header(version: u32, dcid: &[u8], scid: &[u8]) -> Vec<u8> {
        let mut pkt = Vec::new();
        // Long header form, fixed bit, Initial type.
        pkt.push(0b1100_0000);
        pkt.extend_from_slice(&version.to_be_bytes());
        pkt.push(dcid.len() as u8);
        pkt.extend_from_slice(dcid);
        pkt.push(scid.len() as u8);
        pkt.extend_from_slice(scid);
        // Token length (varint 0), then payload padding.
        pkt.push(0);
        pkt.resize(1200, 0);
        pkt
    }

    #[test]
    fn a_supported_version_yields_its_identifiers() {
        let pkt = long_header(VERSION_V1, &[1; 8], &[2; 8]);
        match inspect(&pkt, 8).unwrap() {
            Inspection::Supported {
                version,
                dcid,
                scid,
            } => {
                assert_eq!(version, VERSION_V1);
                assert_eq!(dcid.as_bytes(), &[1; 8]);
                assert_eq!(scid.as_bytes(), &[2; 8]);
            }
            other => panic!("expected a supported version, got {other:?}"),
        }
    }

    #[test]
    fn an_unsupported_version_still_yields_its_identifiers() {
        // The trap this variant exists for. ngtcp2 reports this as an error return while
        // filling its output; mapping it to `Err` and dropping the output would leave
        // nothing to build a Version Negotiation packet from.
        let pkt = long_header(0x0badf00d, &[3; 8], &[4; 8]);
        match inspect(&pkt, 8).unwrap() {
            Inspection::UnsupportedVersion {
                version,
                dcid,
                scid,
            } => {
                assert_eq!(version, 0x0badf00d);
                assert_eq!(dcid.as_bytes(), &[3; 8]);
                assert_eq!(scid.as_bytes(), &[4; 8]);
            }
            other => panic!("expected an unsupported version, got {other:?}"),
        }
    }

    #[test]
    fn the_identifiers_survive_the_datagram_being_dropped() {
        // They borrow into the input, so `inspect` must copy them out. If it did not, this
        // would read freed memory.
        let inspection = {
            let pkt = long_header(VERSION_V1, &[7; 12], &[8; 12]);
            inspect(&pkt, 8).unwrap()
        };
        match inspection {
            Inspection::Supported { dcid, scid, .. } => {
                assert_eq!(dcid.as_bytes(), &[7; 12]);
                assert_eq!(scid.as_bytes(), &[8; 12]);
            }
            other => panic!("expected a supported version, got {other:?}"),
        }
    }

    #[test]
    fn a_truncated_datagram_is_rejected() {
        let Err(err) = inspect(&[0b1100_0000, 0, 0], 8) else {
            panic!("a truncated packet must be rejected");
        };
        assert!(matches!(
            err.kind(),
            ErrorKind::Protocol | ErrorKind::InvalidInput | ErrorKind::Internal
        ));
    }

    #[test]
    fn an_oversized_short_dcidlen_is_rejected_before_reaching_c() {
        let pkt = long_header(VERSION_V1, &[1; 8], &[2; 8]);
        let Err(err) = inspect(&pkt, crate::cid::MAX_LEN + 1) else {
            panic!("an out-of-range short_dcidlen must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn a_well_formed_initial_is_acceptable() {
        let pkt = long_header(VERSION_V1, &[1; 8], &[2; 8]);
        assert!(is_acceptable_initial(&pkt));
    }

    #[test]
    fn a_tiny_datagram_is_not_an_acceptable_initial() {
        // An Initial must be at least 1200 bytes; accepting a smaller one would open an
        // amplification vector.
        let mut pkt = long_header(VERSION_V1, &[1; 8], &[2; 8]);
        pkt.truncate(100);
        assert!(!is_acceptable_initial(&pkt));
    }

    #[test]
    fn a_version_negotiation_packet_can_be_written() {
        let dcid = ConnectionId::new(&[1; 8]).unwrap();
        let scid = ConnectionId::new(&[2; 8]).unwrap();
        let mut buf = [0u8; 256];
        let written =
            write_version_negotiation(&mut buf, 0x5a, &dcid, &scid, supported_versions()).unwrap();
        assert!(written > 0);
        // The long-header bit must be set, and the version field must be zero, which is
        // what marks a packet as Version Negotiation.
        assert_eq!(buf[0] & 0b1000_0000, 0b1000_0000);
        assert_eq!(&buf[1..5], &[0, 0, 0, 0]);
    }

    #[test]
    fn a_version_negotiation_packet_into_a_tiny_buffer_fails() {
        let dcid = ConnectionId::new(&[1; 8]).unwrap();
        let scid = ConnectionId::new(&[2; 8]).unwrap();
        let mut buf = [0u8; 4];
        assert!(
            write_version_negotiation(&mut buf, 0, &dcid, &scid, supported_versions()).is_err()
        );
    }

    #[test]
    fn version_one_is_supported() {
        assert!(supported_versions().contains(&VERSION_V1));
    }
}
