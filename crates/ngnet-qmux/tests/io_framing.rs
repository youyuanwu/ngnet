//! Record framing: boundaries, splits, refusals, and the latched close.
//!
//! The framer's job is to answer two questions dwnx cannot (Spec FR-008, FR-017): whether the
//! byte stream stands between records right now, and what the peer's close said. Both answers
//! are only as good as the framer's agreement with the record structure under *arbitrary*
//! chunking, because a byte stream chooses its own chunk boundaries and a test that only ever
//! feeds whole records proves nothing about the case that occurs in production.
//!
//! So the central test here is not "this stream frames correctly" but "this stream frames
//! identically however it is cut up", asserted at every split point of a multi-record stream
//! and byte by byte.
//!
//! The other half is retention. A close arriving with trailing bytes behind it in the same
//! read is the case a sliding window loses, and it is silent when lost: the connection still
//! ends, and the caller is simply told nothing about why.

#![cfg(feature = "io")]

use ngnet_qmux::io::{ErrorKind, RecordFramer, encode_close_record};
use ngnet_qmux::{CloseKind, CloseReason, DEFAULT_MAX_RECORD_SIZE};

const MAX_RECORD_SIZE: usize = DEFAULT_MAX_RECORD_SIZE as usize;

/// A record: `payload` behind the two-byte length prefix dwnx itself emits.
fn record(payload: &[u8]) -> Vec<u8> {
    let len = u16::try_from(payload.len()).expect("a record within the two-byte prefix");
    assert!(len <= 0x3fff, "a two-byte prefix holds 14 bits");
    let mut out = vec![0x40 | (len >> 8) as u8, (len & 0xff) as u8];
    out.extend_from_slice(payload);
    out
}

/// A record with the shortest prefix that holds its length, which a conforming peer may use
/// even though dwnx does not.
fn short_record(payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 0x40, "a one-byte prefix holds six bits");
    let mut out = vec![payload.len() as u8];
    out.extend_from_slice(payload);
    out
}

/// A record with a four-byte prefix: the same length, spelled at the next width up.
fn wide_record(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).expect("a record length");
    let mut out = (len | 0x8000_0000).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}

/// The close frame alone, with the record prefix `encode_close_record` puts in front of it.
fn close_frame(reason: &CloseReason) -> Vec<u8> {
    let encoded = encode_close_record(reason);
    let width = 1usize << (encoded[0] >> 6);
    encoded[width..].to_vec()
}

/// A stream of four records of differing prefix widths and payload sizes, and the offsets at
/// which a framer fed it should stand at a boundary.
fn mixed_stream() -> (Vec<u8>, Vec<usize>) {
    let records = [
        short_record(&[0x10, 0x44, 0x00]),
        record(&[0x01; 300]),
        wide_record(&[0x02; 7]),
        short_record(&[0x00]),
    ];

    let mut bytes = Vec::new();
    let mut boundaries = vec![0];
    for record in &records {
        bytes.extend_from_slice(record);
        boundaries.push(bytes.len());
    }
    (bytes, boundaries)
}

#[test]
fn a_known_stream_frames_the_same_in_one_chunk_and_byte_by_byte() {
    let (bytes, boundaries) = mixed_stream();

    let mut whole = RecordFramer::new();
    whole.consume(&bytes).expect("a well-formed stream");
    assert!(whole.at_boundary(), "the stream ends on a record boundary");

    let mut single = RecordFramer::new();
    assert!(single.at_boundary(), "offset 0 is a boundary");
    for (offset, byte) in bytes.iter().enumerate() {
        single.consume(&[*byte]).expect("a well-formed stream");
        let at = offset + 1;
        assert_eq!(
            single.at_boundary(),
            boundaries.contains(&at),
            "byte-at-a-time framing disagrees at offset {at}"
        );
    }
}

/// The property the connection actually relies on: the chunking is not observable.
#[test]
fn every_split_point_of_a_multi_record_stream_agrees() {
    let (bytes, boundaries) = mixed_stream();

    for split in 0..=bytes.len() {
        let mut framer = RecordFramer::new();
        framer.consume(&bytes[..split]).expect("the first chunk");
        assert_eq!(
            framer.at_boundary(),
            boundaries.contains(&split),
            "a stream cut at {split} misreports where it stands"
        );

        framer.consume(&bytes[split..]).expect("the second chunk");
        assert!(
            framer.at_boundary(),
            "a stream cut at {split} did not finish on a boundary"
        );
    }
}

/// Everything short of the last byte of a record is mid-record, including a half-read prefix.
#[test]
fn a_truncated_final_record_leaves_the_framer_off_a_boundary() {
    let payload = [0x11; 64];
    let stream = record(&payload);

    for truncation in 1..stream.len() {
        let mut framer = RecordFramer::new();
        framer
            .consume(&stream[..truncation])
            .expect("a valid prefix");
        assert!(
            !framer.at_boundary(),
            "a record cut after {truncation} bytes was reported as complete"
        );
    }

    // A peer that sent the first byte of a two-byte prefix and stopped began announcing a
    // record it never sent, which is a truncation and not a clean ending.
    let mut framer = RecordFramer::new();
    framer.consume(&[0x40]).expect("half a length prefix");
    assert!(!framer.at_boundary());
}

#[test]
fn a_declared_length_above_the_maximum_is_rejected() {
    let too_long = u32::try_from(MAX_RECORD_SIZE + 1).expect("a length");
    let mut framer = RecordFramer::new();
    let error = framer
        .consume(&(too_long | 0x8000_0000).to_be_bytes())
        .expect_err("a record larger than the maximum must be refused");
    assert_eq!(error.kind(), ErrorKind::Protocol);

    // The largest legal record is still accepted, so the bound is the maximum and not one
    // below it.
    let mut framer = RecordFramer::new();
    framer
        .consume(&record(&vec![0x00; MAX_RECORD_SIZE]))
        .expect("a record of exactly the maximum size");
    assert!(framer.at_boundary());
}

/// dwnx refuses a zero-length record too, so the framer agreeing keeps the two in step.
#[test]
fn a_declared_length_of_zero_is_rejected() {
    let mut framer = RecordFramer::new();
    let error = framer
        .consume(&[0x00])
        .expect_err("a record of no length carries no frame");
    assert_eq!(error.kind(), ErrorKind::Protocol);
}

#[test]
fn a_close_is_latched_with_every_field_intact() {
    let reason = CloseReason::application(0x1234, b"the peer had enough");
    let mut framer = RecordFramer::new();
    framer
        .consume(&encode_close_record(&reason))
        .expect("a close record");

    assert!(framer.latched_close().is_some(), "the close was latched");
    let decoded = framer.close_reason().expect("a decodable close");
    assert_eq!(decoded.kind(), CloseKind::Application);
    assert_eq!(decoded.error_code(), 0x1234);
    assert_eq!(decoded.reason(), b"the peer had enough");
}

/// The case a sliding window loses: a close with more bytes behind it in the same read.
///
/// `Conn::read` reports the close only after consuming its record, and a peer may write the
/// close and whatever else was queued in one go. A framer that evicted the previous record
/// when the next one began would have nothing left to decode by the time the caller asked.
#[test]
fn a_close_followed_by_trailing_bytes_in_the_same_read_survives() {
    let reason = CloseReason::transport(0x0a, b"frame encoding error");
    let mut stream = encode_close_record(&reason);
    stream.extend_from_slice(&record(&[0x10, 0x44, 0x00]));
    stream.extend_from_slice(&short_record(&[0x00, 0x00]));
    // And a record left half-arrived behind it, which is the awkward variant: the framer is
    // now mid-record with a close it must still be holding.
    stream.extend_from_slice(&record(&[0x07; 40])[..12]);

    let mut framer = RecordFramer::new();
    framer.consume(&stream).expect("a well-formed stream");

    assert!(!framer.at_boundary(), "the trailing record is incomplete");
    let decoded = framer
        .close_reason()
        .expect("the close survived the trailer");
    assert_eq!(decoded.kind(), CloseKind::Transport);
    assert_eq!(decoded.error_code(), 0x0a);
    assert_eq!(decoded.reason(), b"frame encoding error");
}

/// Latched means latched: a second close does not replace the first, because the first is the
/// one that ended the connection.
#[test]
fn the_first_close_is_the_one_kept() {
    let mut stream = encode_close_record(&CloseReason::application(1, b"first"));
    stream.extend_from_slice(&encode_close_record(&CloseReason::application(
        2, b"second",
    )));

    let mut framer = RecordFramer::new();
    framer.consume(&stream).expect("a well-formed stream");

    let decoded = framer.close_reason().expect("a close");
    assert_eq!(decoded.error_code(), 1);
    assert_eq!(decoded.reason(), b"first");
}

#[test]
fn retention_never_exceeds_one_record_in_progress_plus_one_latched_close() {
    let bound = MAX_RECORD_SIZE * 2;
    let close = encode_close_record(&CloseReason::transport(9, &vec![b'r'; 4096]));

    let mut stream = Vec::new();
    for _ in 0..3 {
        stream.extend_from_slice(&record(&vec![0x01; MAX_RECORD_SIZE]));
    }
    stream.extend_from_slice(&close);
    for _ in 0..3 {
        stream.extend_from_slice(&record(&vec![0x02; MAX_RECORD_SIZE]));
    }

    let mut framer = RecordFramer::new();
    let mut latched_at = None;
    for (offset, byte) in stream.iter().enumerate() {
        framer.consume(&[*byte]).expect("a well-formed stream");
        assert!(
            framer.retained_bytes() <= bound,
            "retention reached {} bytes at offset {offset}",
            framer.retained_bytes()
        );
        if framer.latched_close().is_some() && latched_at.is_none() {
            latched_at = Some(offset);
        }
    }

    let latched = framer.latched_close().expect("the close was latched").len();
    assert!(latched_at.is_some(), "the close is latched as it completes");
    assert_eq!(
        framer.retained_bytes(),
        latched,
        "once a close is latched nothing further is retained"
    );

    // Before any close, retention is one record at most.
    let mut plain = RecordFramer::new();
    let records = record(&vec![0x03; MAX_RECORD_SIZE]);
    for _ in 0..4 {
        plain.consume(&records).expect("a well-formed record");
        assert_eq!(
            plain.retained_bytes(),
            0,
            "a completed record carrying no close is dropped"
        );
    }
    plain
        .consume(&records[..MAX_RECORD_SIZE])
        .expect("a partial record");
    assert!(plain.retained_bytes() <= MAX_RECORD_SIZE);
}

/// A close behind another frame in the same record is still latched, because the framer scans
/// the whole record rather than looking at its first frame.
#[test]
fn a_close_behind_another_frame_in_its_record_is_latched() {
    let reason = CloseReason::transport(0x07, b"after a padding frame");
    let mut payload = vec![0x00, 0x00, 0x00];
    payload.extend_from_slice(&close_frame(&reason));

    let mut framer = RecordFramer::new();
    framer
        .consume(&record(&payload))
        .expect("a well-formed record");

    let decoded = framer.close_reason().expect("the close was found");
    assert_eq!(decoded.error_code(), 0x07);
    assert_eq!(decoded.reason(), b"after a padding frame");
}

#[test]
fn an_ordinary_record_latches_nothing() {
    let mut framer = RecordFramer::new();
    framer
        .consume(&record(&[0x10, 0x44, 0x00]))
        .expect("a MAX_DATA record");
    assert!(framer.latched_close().is_none());
    assert!(framer.close_reason().is_none());
    assert_eq!(framer.retained_bytes(), 0);
}

#[test]
fn a_fresh_framer_stands_at_a_boundary_and_holds_nothing() {
    let framer = RecordFramer::default();
    assert!(framer.at_boundary());
    assert_eq!(framer.retained_bytes(), 0);
    assert!(framer.latched_close().is_none());
}

/// A close behind a *real* frame, in a record the framer never saw whole in one call.
///
/// [`a_close_behind_another_frame_in_its_record_is_latched`] makes the same claim about a
/// record delivered in one piece behind padding. This one is the awkward variant and it exists
/// to pin a precondition rather than to repeat that claim: the scan reaches the close only
/// because the framer has the record's payload *reassembled in its own buffer* by the time
/// `finish_record` runs (`src/io/framing.rs`, the payload arm of `consume` feeding the scan in
/// `finish_record`). A change that scanned the arriving bytes where they lie, rather than
/// copying them first, has nothing to scan when the record arrives in fragments and nothing to
/// find when the close is not in the fragment it happens to be looking at.
///
/// The frame in front of the close is a MAX_DATA frame rather than padding, so the scan has to
/// know a frame's length to step over it rather than merely skip zero bytes.
#[test]
fn a_close_behind_a_real_frame_is_found_however_the_record_was_cut() {
    let reason = CloseReason::transport(0x0b, b"after a MAX_DATA frame");
    let mut payload = vec![0x10, 0x44, 0x00];
    payload.extend_from_slice(&close_frame(&reason));
    let stream = record(&payload);

    for split in 0..=stream.len() {
        let mut framer = RecordFramer::new();
        framer.consume(&stream[..split]).expect("the first chunk");
        framer.consume(&stream[split..]).expect("the second chunk");

        let decoded = framer
            .close_reason()
            .unwrap_or_else(|| panic!("the close was lost when the record was cut at {split}"));
        assert_eq!(decoded.kind(), CloseKind::Transport);
        assert_eq!(decoded.error_code(), 0x0b);
        assert_eq!(decoded.reason(), b"after a MAX_DATA frame");
    }
}

/// Every payload byte of every record is copied into the framer's retention.
///
/// The number asserted is the sum of the records' payloads and nothing else, because that is
/// what the code does today: `consume`'s payload arm copies each chunk of payload into
/// `record` while no close has been latched, and copies no length prefix -- prefixes are
/// consumed by `LengthPrefix::feed`, which never reaches the retention buffer. So a stream of
/// four records costs exactly their four payloads, whatever the byte stream's chunking was,
/// and the two framers below are fed the same stream cut two different ways to say so.
///
/// This is the figure Phase 6 (inbound scan in place) is expected to drive down for records
/// that arrive whole -- at which point this assertion is inverted deliberately, and the number
/// it is inverted to has to be justified the same way this one is.
#[cfg(debug_assertions)]
#[test]
fn a_run_of_whole_records_copies_exactly_their_payloads() {
    let payloads: [&[u8]; 4] = [&[0x10, 0x44, 0x00], &[0x01; 300], &[0x02; 7], &[0x00]];
    let total: usize = payloads.iter().map(|payload| payload.len()).sum();

    let mut stream = Vec::new();
    for payload in payloads {
        stream.extend_from_slice(&record(payload));
    }

    let mut whole = RecordFramer::new();
    whole.consume(&stream).expect("a well-formed stream");
    assert_eq!(
        whole.copied_bytes(),
        total,
        "one memcpy per record, of exactly that record's payload"
    );

    // The same stream, one byte at a time. A copy charged per call rather than per byte would
    // differ here, and a copy that took in the length prefixes would exceed `total` in both.
    let mut single = RecordFramer::new();
    for byte in &stream {
        single.consume(&[*byte]).expect("a well-formed stream");
    }
    assert_eq!(
        single.copied_bytes(),
        total,
        "the chunking is not observable in the cost, only in how it is paid"
    );

    // And a partly-arrived record is charged for what has arrived, which is what makes the
    // count a measure of the copying rather than of the framing.
    let half = &record(&[0x03; 64])[..2 + 25];
    let mut partial = RecordFramer::new();
    partial.consume(half).expect("a valid prefix");
    assert_eq!(partial.copied_bytes(), 25);
}

/// Once a close is latched the copying stops, because there is nothing further to deliver.
///
/// `consume` copies only while `close` is `None`, so the count freezes at the close record's
/// own payload however much the peer sends afterwards. Asserted because it is the reason the
/// retention bound holds, and a scan-in-place change has to keep it: a framer that resumed
/// copying after a close would grow without limit on a peer that kept writing.
#[cfg(debug_assertions)]
#[test]
fn nothing_is_copied_once_a_close_has_been_latched() {
    let reason = CloseReason::application(7, b"enough");
    let close = encode_close_record(&reason);
    let close_payload = close.len() - 1;

    let mut framer = RecordFramer::new();
    framer.consume(&close).expect("a close record");
    assert_eq!(
        framer.copied_bytes(),
        close_payload,
        "the close record's own payload is copied like any other"
    );

    for _ in 0..4 {
        framer
            .consume(&record(&[0x04; 512]))
            .expect("a well-formed record");
    }
    assert_eq!(
        framer.copied_bytes(),
        close_payload,
        "records behind a latched close are framed but not retained, so nothing is copied"
    );
}

/// The copy counter is absent from a build with debug assertions off, which is the benchmark
/// build.
///
/// Asserted against the source because that is where the decision lives and the only place it
/// can be checked from: a test cannot observe the absence of a field in a profile it is not
/// compiled into. The gate matters beyond tidiness -- a later phase compares one benchmark run
/// against another, and a counter present in one build and not the other would make the
/// difference between them partly the instrument's.
///
/// The check is deliberately narrow: it asserts the gate sits immediately above the increment
/// and above the accessor, so removing either gate fails here rather than silently putting a
/// per-record counter on the receive path of every measured run.
#[test]
fn the_copy_counter_is_gated_out_of_a_release_build() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("io")
            .join("framing.rs"),
    )
    .expect("reading the framer's source");

    for gated in [
        "#[cfg(debug_assertions)]\n                        {\n                            self.copied += take;",
        "#[cfg(debug_assertions)]\n    #[must_use]\n    pub fn copied_bytes(&self) -> usize {",
        "#[cfg(debug_assertions)]\n    copied: usize,",
    ] {
        assert!(
            source.contains(gated),
            "the copy counter has lost its `cfg(debug_assertions)` gate, which puts it into \
             the benchmark build: expected to find\n{gated}"
        );
    }
}
