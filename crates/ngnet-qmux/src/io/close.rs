//! Reading and writing a CONNECTION_CLOSE record, which dwnx does exactly half of.
//!
//! dwnx parses an incoming close and produces nothing on the way out. There is no
//! `dwnx_conn_write_connection_close`, so a connection cannot be closed on the wire by asking
//! the state machine to do it; and the close it *does* parse lands in a private frame struct
//! behind a `DWNX_ERR_DRAINING` return, so its four fields are unreachable from outside
//! (`deps/dwnx/lib/dwnx_conn.c:1982-2110`). Both halves of the codec therefore live here, and
//! both are stand-ins to be deleted if the library grows the functions (Spec C-5).
//!
//! # Encoding
//!
//! The field order is not a choice; it is what dwnx's reader expects, so it is copied from the
//! reader's own state machine (`deps/dwnx/lib/dwnx_conn.c:1982-2038`): frame type, error code,
//! then a frame-type field **only for a transport close**, then the reason length and the
//! reason bytes. An application close has no frame-type field at all, and writing one produces
//! a record whose reason length is read out of the wrong bytes -- the peer either rejects the
//! record or reports a reason phrase of garbage, both silently.
//!
//! Correctness here cannot be established by round-tripping through the decoder below, since
//! the two would agree with each other while disagreeing with dwnx. It is established by
//! feeding an encoded close to a real [`Conn`](crate::Conn) and requiring
//! [`ReadOutcome::PeerClosed`](crate::ReadOutcome::PeerClosed), which is what
//! `tests/io_close_codec.rs` does (Spec SC-008).
//!
//! # Decoding, and why it is a scan
//!
//! The obvious decoder reads the record's first frame and asks whether it is a close. That is
//! wrong, and dwnx is where the answer is: after each frame it resets its reader to the
//! frame-type state whenever the record has bytes left
//! (`deps/dwnx/lib/dwnx_record_reader.c:88-103`), so one record may carry several frames and a
//! close may sit behind any of them. A first-frame decoder loses exactly those closes, leaving
//! the layer to report that the peer closed without being able to say why.
//!
//! Scanning means knowing how long every other frame is, so [`skip_frame`] carries dwnx's frame
//! layouts. A frame type it does not recognise ends the scan: without its length there is no
//! way to find the next frame, and guessing would mean decoding a close out of the middle of
//! someone else's field. dwnx rejects such a frame anyway, so the connection is failing either
//! way; this side simply declines to invent a reason for it.
//!
//! # There is no 1024-byte cap on the reason
//!
//! dwnx's header mentions truncating a reason phrase to 1024 bytes, and that describes the
//! `dwnx_ccerr` struct, which is not on this path. The streaming reader allocates and stores
//! the full declared length (`deps/dwnx/lib/dwnx_conn.c:2061-2083`). The bound that applies is
//! the record's own declared length, itself bounded by the maximum record size -- which is why
//! [`encode_close_record`] truncates a reason that would not fit rather than emitting a record
//! the peer must reject.

use crate::ccerr::{CloseKind, CloseReason};
use crate::io::framing::{read_varint, varint_len, write_varint};

/// A transport-level close: `DWNX_FRAME_CONNECTION_CLOSE`.
const FRAME_CONNECTION_CLOSE: u64 = 0x1c;
/// An application-level close: `DWNX_FRAME_CONNECTION_CLOSE_APP`.
const FRAME_CONNECTION_CLOSE_APP: u64 = 0x1d;

const FRAME_PADDING: u64 = 0x00;
const FRAME_RESET_STREAM: u64 = 0x04;
const FRAME_STOP_SENDING: u64 = 0x05;
const FRAME_STREAM: u64 = 0x08;
const FRAME_MAX_DATA: u64 = 0x10;
const FRAME_MAX_STREAM_DATA: u64 = 0x11;
const FRAME_MAX_STREAMS_BIDI: u64 = 0x12;
const FRAME_MAX_STREAMS_UNI: u64 = 0x13;
const FRAME_DATA_BLOCKED: u64 = 0x14;
const FRAME_STREAM_DATA_BLOCKED: u64 = 0x15;
const FRAME_STREAMS_BLOCKED_BIDI: u64 = 0x16;
const FRAME_STREAMS_BLOCKED_UNI: u64 = 0x17;
const FRAME_QX_TRANSPORT_PARAMETERS: u64 = 0x3F51_5330_0D0A_0D0A;
const FRAME_QX_PING_REQUEST: u64 = 0x348C_6752_9EF8_C7BD;
const FRAME_QX_PING_RESPONSE: u64 = 0x348C_6752_9EF8_C7BE;

/// The STREAM frame's "an explicit length follows" bit.
const STREAM_LEN_BIT: u64 = 0x02;
/// The STREAM frame's "an explicit offset follows" bit.
const STREAM_OFF_BIT: u64 = 0x04;

/// The largest record any QMux peer may send, and so the largest this may produce.
const MAX_RECORD_SIZE: usize = crate::DEFAULT_MAX_RECORD_SIZE as usize;

/// Serialises `reason` as a complete record, length prefix included.
///
/// The result is ready to hand to a byte stream as it stands. It is a whole record rather than
/// a bare frame because a close is never packed alongside anything: this layer emits it when
/// the connection is ending, and the state machine that would have packed it has no way to be
/// told about it.
///
/// [`CloseKind::IdleClose`] and [`CloseKind::Unknown`] are written as transport closes. The
/// wire has two close frames and no third, an idle close is not something a peer announces --
/// it is what dwnx infers locally when a connection times out -- and refusing to encode one
/// would mean a connection that could not be closed at all.
///
/// A reason too long to fit the maximum record size is truncated. The alternative is to fail
/// the close, which would leave the connection open and the peer uninformed for the sake of a
/// diagnostic string.
#[must_use]
pub fn encode_close_record(reason: &CloseReason) -> Vec<u8> {
    let application = matches!(reason.kind(), CloseKind::Application);
    let frame_type = if application {
        FRAME_CONNECTION_CLOSE_APP
    } else {
        FRAME_CONNECTION_CLOSE
    };

    // Everything but the reason phrase, so the room left for it is known before it is written.
    let mut fixed = varint_len(frame_type) + varint_len(reason.error_code());
    if !application {
        fixed += varint_len(reason.frame_type());
    }

    let reason_bytes = reason.reason();
    // A reason bounded by the maximum record size has a length field of at most two bytes, so
    // budgeting two keeps the payload inside the maximum whatever the reason turns out to be.
    let budget = MAX_RECORD_SIZE.saturating_sub(fixed + 2);
    let reason_bytes = &reason_bytes[..reason_bytes.len().min(budget)];

    let mut frame = Vec::with_capacity(fixed + 2 + reason_bytes.len());
    write_varint(&mut frame, frame_type);
    write_varint(&mut frame, reason.error_code());
    if !application {
        write_varint(&mut frame, reason.frame_type());
    }
    write_varint(&mut frame, reason_bytes.len() as u64);
    frame.extend_from_slice(reason_bytes);

    let mut record = Vec::with_capacity(varint_len(frame.len() as u64) + frame.len());
    write_varint(&mut record, frame.len() as u64);
    record.extend_from_slice(&frame);
    record
}

/// Finds and decodes a connection close in one record's payload.
///
/// `payload` is a whole record with its length prefix already stripped -- what
/// [`RecordFramer`](super::RecordFramer) retains. The frames in it are walked in order until a
/// close is found, since a close need not be the first.
///
/// Returns [`None`] when the record carries no close, when it is malformed, and when it carries
/// a frame whose length this side cannot work out. All three mean the same thing to the caller:
/// there is no close to report from this record. Deciding that the record is *invalid* is
/// dwnx's job, and it is looking at the same bytes.
#[must_use]
pub fn decode_close_frame(payload: &[u8]) -> Option<CloseReason> {
    let mut at = 0;
    while at < payload.len() {
        let (frame_type, read) = read_varint(&payload[at..])?;
        at += read;

        if frame_type == FRAME_CONNECTION_CLOSE || frame_type == FRAME_CONNECTION_CLOSE_APP {
            return decode_fields(frame_type, &payload[at..]);
        }

        at += skip_frame(frame_type, &payload[at..])?;
    }
    None
}

/// Decodes a close frame's fields, the type varint having already been read.
fn decode_fields(frame_type: u64, fields: &[u8]) -> Option<CloseReason> {
    let mut at = 0;

    let (error_code, read) = read_varint(&fields[at..])?;
    at += read;

    let triggering_frame = if frame_type == FRAME_CONNECTION_CLOSE {
        let (value, read) = read_varint(&fields[at..])?;
        at += read;
        value
    } else {
        // An application close carries no frame type on the wire, and dwnx's reader skips
        // straight to the reason length for one.
        0
    };

    let (reason_len, read) = read_varint(&fields[at..])?;
    at += read;
    let reason_len = usize::try_from(reason_len).ok()?;
    let reason = fields.get(at..at.checked_add(reason_len)?)?;

    let kind = if frame_type == FRAME_CONNECTION_CLOSE {
        CloseKind::Transport
    } else {
        CloseKind::Application
    };
    Some(CloseReason::from_parts(
        kind,
        error_code,
        triggering_frame,
        reason.to_vec(),
    ))
}

/// How many bytes of `fields` the frame of type `frame_type` occupies, its type varint aside.
///
/// [`None`] for a frame type with no known layout, which ends the scan; see the module
/// documentation for why guessing is not an option.
fn skip_frame(frame_type: u64, fields: &[u8]) -> Option<usize> {
    let varints = |count: usize| -> Option<usize> {
        let mut at = 0;
        for _ in 0..count {
            let (_, read) = read_varint(&fields[at..])?;
            at += read;
        }
        Some(at)
    };

    match frame_type {
        // Each padding byte is a frame of its own, so there is nothing after the type.
        FRAME_PADDING => Some(0),
        FRAME_MAX_DATA
        | FRAME_MAX_STREAMS_BIDI
        | FRAME_MAX_STREAMS_UNI
        | FRAME_DATA_BLOCKED
        | FRAME_STREAMS_BLOCKED_BIDI
        | FRAME_STREAMS_BLOCKED_UNI
        | FRAME_QX_PING_REQUEST
        | FRAME_QX_PING_RESPONSE => varints(1),
        FRAME_STOP_SENDING | FRAME_MAX_STREAM_DATA | FRAME_STREAM_DATA_BLOCKED => varints(2),
        FRAME_RESET_STREAM => varints(3),
        FRAME_QX_TRANSPORT_PARAMETERS => {
            let (len, read) = read_varint(fields)?;
            let len = usize::try_from(len).ok()?;
            let end = read.checked_add(len)?;
            (end <= fields.len()).then_some(end)
        }
        _ if (frame_type & !0x07) == FRAME_STREAM => {
            let (_, read) = read_varint(fields)?;
            let mut at = read;
            if frame_type & STREAM_OFF_BIT != 0 {
                let (_, read) = read_varint(&fields[at..])?;
                at += read;
            }
            if frame_type & STREAM_LEN_BIT == 0 {
                // Without the length bit the data runs to the end of the record, so the frame
                // is the rest of it and nothing can follow.
                return Some(fields.len());
            }
            let (len, read) = read_varint(&fields[at..])?;
            at += read;
            let len = usize::try_from(len).ok()?;
            let end = at.checked_add(len)?;
            (end <= fields.len()).then_some(end)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record the sans-I/O suite already pins by hand, which the encoder must reproduce
    /// byte for byte.
    #[test]
    fn the_default_close_matches_the_record_the_existing_tests_use() {
        let encoded = encode_close_record(&CloseReason::no_error());
        assert_eq!(encoded, vec![0x04, 0x1c, 0x00, 0x00, 0x00]);
    }

    /// An application close has three fields, not four; the frame-type field is transport-only.
    #[test]
    fn an_application_close_omits_the_frame_type_field() {
        let encoded = encode_close_record(&CloseReason::application(7, b"bye"));
        assert_eq!(encoded, vec![0x06, 0x1d, 0x07, 0x03, b'b', b'y', b'e']);
    }

    #[test]
    fn a_reason_too_long_for_a_record_is_truncated_rather_than_refused() {
        let reason = CloseReason::application(1, &vec![b'x'; MAX_RECORD_SIZE * 2]);
        let encoded = encode_close_record(&reason);

        // The maximum bounds the record's *payload*; the length prefix sits outside it.
        let (payload_len, prefix) = read_varint(&encoded).expect("a length prefix");
        assert_eq!(payload_len as usize, encoded.len() - prefix);
        assert!(payload_len as usize <= MAX_RECORD_SIZE, "{payload_len}");

        let decoded = decode_close_frame(&encoded[prefix..]).expect("still a close");
        assert!(!decoded.reason().is_empty());
        assert!(decoded.reason().len() < MAX_RECORD_SIZE * 2);
    }

    #[test]
    fn a_record_with_no_close_in_it_decodes_to_nothing() {
        // A MAX_DATA frame and nothing else.
        assert_eq!(decode_close_frame(&[0x10, 0x44, 0x00]), None);
        assert_eq!(decode_close_frame(&[]), None);
    }

    #[test]
    fn an_unknown_frame_type_ends_the_scan_rather_than_guessing() {
        // 0x1e is not a frame dwnx defines; a close after it must not be reported, because
        // finding it would mean having guessed the unknown frame's length.
        assert_eq!(decode_close_frame(&[0x1e, 0x1c, 0x00, 0x00, 0x00]), None);
    }
}
