//! The CONNECTION_CLOSE codec, checked against itself and then against dwnx.
//!
//! Round-tripping through this crate's own decoder proves that the encoder and the decoder
//! agree with each other, which is exactly as much as two halves of one misunderstanding would
//! prove. The test that carries the weight is [`an_encoded_close_is_accepted_by_dwnx`]: it
//! feeds an encoded close to a real [`Conn`] and requires [`ReadOutcome::PeerClosed`], so the
//! field order is checked against the parser a peer will actually use (Spec SC-008).
//!
//! The other property worth a test of its own is that a close is found when it is *not* the
//! first frame in its record. dwnx returns its record reader to the frame-type state for as
//! long as the record has bytes left, so a close may sit behind anything; a decoder that read
//! only the leading frame would report a connection ending for no stated reason, and would do
//! so only against peers that pack their records.

#![cfg(feature = "io")]

use ngnet_qmux::io::{decode_close_frame, encode_close_record};
use ngnet_qmux::{
    CloseKind, CloseReason, Conn, DEFAULT_MAX_RECORD_SIZE, ReadOutcome, Role, Timestamp,
    TransportParams, WriteRequest,
};

const BUF: usize = 16 * 1024;
const MAX_VARINT: u64 = (1 << 62) - 1;

fn now() -> Timestamp {
    Timestamp::from_nanos(0)
}

fn params() -> TransportParams {
    TransportParams::new().with_all_limits(1 << 20, 8)
}

/// The bytes of a record, prefix stripped -- what a [`RecordFramer`] scans and what
/// [`decode_close_frame`] is given.
///
/// "Scans" rather than "retains": a record that arrives whole is scanned where it lies and is
/// never retained at all. What is stripped is the same either way, and it is the stripping that
/// matters here -- the decoder is given the payload, never the record.
///
/// [`RecordFramer`]: ngnet_qmux::io::RecordFramer
fn payload_of(record: &[u8]) -> &[u8] {
    let width = 1usize << (record[0] >> 6);
    &record[width..]
}

/// Wraps frame bytes in a record with a one-byte length prefix.
fn record(frames: &[u8]) -> Vec<u8> {
    assert!(frames.len() < 0x40, "a one-byte prefix holds six bits");
    let mut out = vec![frames.len() as u8];
    out.extend_from_slice(frames);
    out
}

/// Encodes `reason` and decodes it again, asserting every field survived.
fn round_trip(reason: &CloseReason) -> CloseReason {
    let encoded = encode_close_record(reason);
    let decoded = decode_close_frame(payload_of(&encoded)).expect("an encoded close decodes");

    assert_eq!(decoded.kind(), reason.kind(), "the kind survives");
    assert_eq!(
        decoded.error_code(),
        reason.error_code(),
        "the error code survives"
    );
    assert_eq!(
        decoded.frame_type(),
        reason.frame_type(),
        "the frame type survives"
    );
    assert_eq!(decoded.reason(), reason.reason(), "the reason survives");
    decoded
}

#[test]
fn transport_and_application_closes_round_trip() {
    let transport = round_trip(&CloseReason::transport(0x0a, b"frame encoding error"));
    assert_eq!(transport.kind(), CloseKind::Transport);

    let application = round_trip(&CloseReason::application(0x0105, b"h3 request cancelled"));
    assert_eq!(application.kind(), CloseKind::Application);
}

#[test]
fn an_empty_reason_round_trips() {
    round_trip(&CloseReason::no_error());
    round_trip(&CloseReason::transport(0, b""));
    round_trip(&CloseReason::application(0, b""));
}

#[test]
fn a_long_reason_round_trips() {
    // Long enough to need a two-byte length field, and close enough to the record maximum to
    // exercise the case where the reason dominates the record.
    for len in [63usize, 64, 16_000] {
        let reason = CloseReason::application(1, &vec![b'z'; len]);
        let decoded = round_trip(&reason);
        assert_eq!(decoded.reason().len(), len);
    }
}

#[test]
fn large_error_codes_round_trip() {
    for code in [
        0x3f,
        0x40,
        0x3fff,
        0x4000,
        0x3fff_ffff,
        0x4000_0000,
        MAX_VARINT,
    ] {
        round_trip(&CloseReason::transport(code, b"code"));
        round_trip(&CloseReason::application(code, b"code"));
    }
}

/// A transport close names the frame that provoked it; an application close has no such field
/// on the wire, and its frame type is therefore always zero.
#[test]
fn the_frame_type_field_belongs_to_transport_closes_only() {
    let encoded = encode_close_record(&CloseReason::transport(0x0a, b""));
    assert_eq!(encoded, vec![0x04, 0x1c, 0x0a, 0x00, 0x00]);

    let encoded = encode_close_record(&CloseReason::application(0x0a, b""));
    assert_eq!(encoded, vec![0x03, 0x1d, 0x0a, 0x00]);

    // A transport close whose frame type is set decodes it back, which is the field dwnx's
    // own constructors cannot express and the reason the decoder builds the reason itself.
    let decoded = decode_close_frame(&[0x1c, 0x41, 0x00, 0x08, 0x00])
        .expect("a transport close naming a STREAM frame");
    assert_eq!(decoded.error_code(), 0x100);
    assert_eq!(decoded.frame_type(), 0x08);
    assert!(decoded.reason().is_empty());
}

/// dwnx infers this kind for a connection that timed out; the wire has no third close frame,
/// so it goes out as the transport close it maps to rather than failing to go out at all.
#[test]
fn a_kind_with_no_wire_representation_is_written_as_a_transport_close() {
    let idle = CloseReason::from_native_error(
        ngnet_qmux::NativeCode::new(ngnet_qmux::raw::DWNX_ERR_IDLE_CLOSE),
        b"idle",
    );
    assert_eq!(idle.kind(), CloseKind::IdleClose);

    let decoded = decode_close_frame(payload_of(&encode_close_record(&idle))).expect("a close");
    assert_eq!(decoded.kind(), CloseKind::Transport);
    assert_eq!(decoded.error_code(), idle.error_code());
    assert_eq!(decoded.reason(), b"idle");
}

/// The record maximum is the only bound on a reason phrase, and a reason that would exceed it
/// is truncated rather than producing a record the peer must reject.
#[test]
fn a_reason_larger_than_a_record_is_truncated_to_fit() {
    let huge = vec![b'x'; DEFAULT_MAX_RECORD_SIZE as usize * 3];
    let encoded = encode_close_record(&CloseReason::application(1, &huge));

    let payload = payload_of(&encoded);
    assert!(
        payload.len() <= DEFAULT_MAX_RECORD_SIZE as usize,
        "the record payload is {} bytes",
        payload.len()
    );

    let decoded = decode_close_frame(payload).expect("still a close");
    assert!(!decoded.reason().is_empty());
    assert!(decoded.reason().len() < huge.len());
    assert!(decoded.reason().iter().all(|byte| *byte == b'x'));
}

#[test]
fn a_close_behind_another_frame_in_the_same_record_is_found() {
    let reason = CloseReason::transport(0x0b, b"behind a MAX_DATA frame");
    let close = payload_of(&encode_close_record(&reason)).to_vec();

    // MAX_DATA (0x10) carrying 0x200000 as a four-byte varint, then a PADDING byte, then the
    // close. Both leading frames are ones dwnx parses and steps over.
    let mut payload = vec![0x10, 0x80, 0x20, 0x00, 0x00, 0x00];
    payload.extend_from_slice(&close);

    let decoded = decode_close_frame(&payload).expect("a close behind two other frames");
    assert_eq!(decoded.kind(), CloseKind::Transport);
    assert_eq!(decoded.error_code(), 0x0b);
    assert_eq!(decoded.reason(), b"behind a MAX_DATA frame");
}

/// A STREAM frame with no length bit runs to the end of its record, so nothing can follow it;
/// the scan must not then decode a close out of stream data that merely looks like one.
#[test]
fn stream_data_that_resembles_a_close_is_not_decoded_as_one() {
    // STREAM (0x08, no OFF and no LEN bit) on stream 0, whose data happens to be the bytes of
    // a transport close.
    let payload = vec![0x08, 0x00, 0x1c, 0x00, 0x00, 0x00];
    assert_eq!(decode_close_frame(&payload), None);
}

/// A STREAM frame that *does* declare its length is stepped over, and a close after it found.
#[test]
fn a_close_after_a_length_bearing_stream_frame_is_found() {
    let mut payload = vec![0x0a, 0x00, 0x03, b'a', b'b', b'c'];
    payload.extend_from_slice(&[0x1d, 0x2a, 0x02, b'h', b'i']);

    let decoded = decode_close_frame(&payload).expect("a close after stream data");
    assert_eq!(decoded.kind(), CloseKind::Application);
    assert_eq!(decoded.error_code(), 0x2a);
    assert_eq!(decoded.reason(), b"hi");
}

#[test]
fn a_truncated_close_frame_decodes_to_nothing() {
    // The reason length says four bytes and two are present.
    assert_eq!(
        decode_close_frame(&[0x1c, 0x00, 0x00, 0x04, b'a', b'b']),
        None
    );
    // The frame type field is missing entirely.
    assert_eq!(decode_close_frame(&[0x1c]), None);
}

/// The proof that matters: dwnx's own parser accepts what this encoder produces.
///
/// Everything else in this file could pass while the encoder wrote its fields in an order no
/// peer understands, because the decoder here would make the same mistake in reverse.
#[test]
fn an_encoded_close_is_accepted_by_dwnx() {
    for reason in [
        CloseReason::no_error(),
        CloseReason::transport(0x0a, b"frame encoding error"),
        CloseReason::application(0x0105, b"request cancelled"),
        CloseReason::application(MAX_VARINT, b""),
        CloseReason::transport(0x03, &vec![b'w'; 4096]),
    ] {
        let mut peer = connected_peer();
        assert_eq!(
            peer.read(&encode_close_record(&reason), now())
                .expect("dwnx must accept the encoded close"),
            ReadOutcome::PeerClosed,
            "dwnx rejected a close of kind {:?}",
            reason.kind()
        );
    }
}

/// And it is still accepted when another frame precedes it in the same record.
#[test]
fn dwnx_accepts_a_close_that_is_not_the_first_frame_in_its_record() {
    let reason = CloseReason::transport(0x0b, b"after a MAX_DATA frame");
    let close = payload_of(&encode_close_record(&reason)).to_vec();

    // MAX_DATA raising the connection limit above the 1 MiB the handshake advertised, so dwnx
    // processes rather than rejects it, then the close.
    let mut payload = vec![0x10, 0x80, 0x20, 0x00, 0x00];
    payload.extend_from_slice(&close);

    let mut framed = vec![u8::try_from(payload.len()).expect("a short record")];
    framed.extend_from_slice(&payload);

    let mut peer = connected_peer();
    assert_eq!(
        peer.read(&framed, now()).expect("dwnx parses both frames"),
        ReadOutcome::PeerClosed
    );

    // The same record, decoded by this crate: dwnx knows a close arrived and cannot say what
    // it was, which is the whole reason the decoder exists.
    let decoded = decode_close_frame(&payload).expect("the close is recoverable");
    assert_eq!(decoded.reason(), b"after a MAX_DATA frame");
}

/// A padding frame is the other frame dwnx will step over, and the cheapest to construct.
#[test]
fn dwnx_accepts_a_close_behind_padding() {
    let encoded = encode_close_record(&CloseReason::application(9, b"bye"));
    let mut payload = vec![0x00, 0x00, 0x00];
    payload.extend_from_slice(payload_of(&encoded));

    let mut peer = connected_peer();
    assert_eq!(
        peer.read(&record(&payload), now())
            .expect("dwnx skips padding and parses the close"),
        ReadOutcome::PeerClosed
    );

    let decoded = decode_close_frame(&payload).expect("and so does this crate");
    assert_eq!(decoded.reason(), b"bye");
}

/// A connection that has exchanged transport parameters, and so will accept ordinary frames.
///
/// dwnx refuses any frame before `QX_TRANSPORT_PARAMETERS`, so a close cannot be fed to a
/// freshly built connection; the handshake has to happen first.
fn connected_peer() -> Conn<'static> {
    let mut client = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .expect("a client");
    let mut server = Conn::builder(Role::Server)
        .transport_params(params())
        .build()
        .expect("a server");

    let mut buf = [0u8; BUF];
    let (record, _) = client
        .write(&mut buf, WriteRequest::control_only(), now())
        .expect("the client's parameters");
    let hello = record.bytes().expect("a record").to_vec();

    let mut buf = [0u8; BUF];
    let (record, _) = server
        .write(&mut buf, WriteRequest::control_only(), now())
        .expect("the server's parameters");
    let reply = record.bytes().expect("a record").to_vec();

    server.read(&hello, now()).expect("the server reads");
    client.read(&reply, now()).expect("the client reads");
    server
}
