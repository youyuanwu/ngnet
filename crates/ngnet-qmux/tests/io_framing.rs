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

/// A record that arrives whole costs no copy at all, and one that arrives in pieces costs its
/// payload.
///
/// This assertion is the deliberate inverse of the one it replaces (Spec SC-011). The old one
/// said that the count equals the sum of the records' payloads, because `consume`'s payload arm
/// copied every chunk of payload into the retention buffer so that `finish_record` would have
/// something contiguous to scan. It now scans the arriving slice where it lies whenever that
/// slice holds the whole record, so a run of records that each arrive whole copies nothing.
///
/// The second count is what makes the first mean something. SC-011 asks for the reverted
/// figure beside the new one, and reverting the fast path is exactly what fragmenting the
/// records does: every record cut in two takes the accumulate-then-scan path, and the count
/// comes back to `total`. So the pair below is "with the change" and "with the change taken
/// away", on the same records, rather than a number to be taken on trust.
///
/// Length prefixes are outside both figures in either arrangement: they are consumed by
/// `LengthPrefix::feed` and never reach the retention buffer, so a copy that took them in would
/// exceed `total` here.
#[cfg(debug_assertions)]
#[test]
fn a_run_of_whole_records_copies_nothing() {
    // Every payload is at least two bytes, so that "cut this record in two" is available for
    // each of them; the one-byte case is its own assertion below, because a one-byte record is
    // whole under every chunking there is.
    let payloads: [&[u8]; 4] = [&[0x10, 0x44, 0x00], &[0x01; 300], &[0x02; 7], &[0x00, 0x00]];
    let total: usize = payloads.iter().map(|payload| payload.len()).sum();

    let mut stream = Vec::new();
    for payload in payloads {
        stream.extend_from_slice(&record(payload));
    }

    let mut whole = RecordFramer::new();
    whole.consume(&stream).expect("a well-formed stream");
    assert_eq!(
        whole.copied_bytes(),
        0,
        "a record whose declared length is entirely present is scanned where it lies"
    );
    assert_eq!(whole.retained_bytes(), 0, "and so is never held either");

    // The same records, each cut inside its payload: the arrangement the fast path cannot take
    // and the count the fast path removed.
    let mut fragmented = RecordFramer::new();
    for payload in payloads {
        let bytes = record(payload);
        let cut = bytes.len() - 1;
        fragmented.consume(&bytes[..cut]).expect("the first chunk");
        fragmented.consume(&bytes[cut..]).expect("the second chunk");
    }
    assert_eq!(
        fragmented.copied_bytes(),
        total,
        "a record spread over two reads has nothing contiguous to scan, so it is reassembled \
         -- which is also the figure the whole-record framer above reported before the fast \
         path existed"
    );

    // One byte at a time is the same path throughout, and charges per byte rather than per
    // call: a copy charged per call would differ from `total` here.
    let mut single = RecordFramer::new();
    for byte in &stream {
        single.consume(&[*byte]).expect("a well-formed stream");
    }
    assert_eq!(
        single.copied_bytes(),
        total,
        "the chunking is observable in the cost now, which is the point of the change"
    );

    // A one-byte record is its own whole remainder however the stream is cut, so it is scanned
    // in place even byte by byte. Stated rather than left implicit, because it is why the
    // payloads above avoid the case.
    let mut tiny = RecordFramer::new();
    for byte in &record(&[0x00]) {
        tiny.consume(&[*byte]).expect("a one-byte record");
    }
    assert_eq!(tiny.copied_bytes(), 0);

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
/// retention bound holds: a framer that resumed copying after a close would grow without limit
/// on a peer that kept writing.
///
/// The close record's own payload is still copied even though it arrived whole, and that is the
/// third precondition of the fast path rather than an oversight: latching means holding the
/// bytes after `consume` has returned, and the bytes it was scanning belong to the caller's read
/// buffer. It is one copy per connection, against one per record before.
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
        "a close found in place is copied out, because latching outlives the call"
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
        "#[cfg(debug_assertions)]\n                    {\n                        self.copied += payload.len();",
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

/// Appends `value` as a QUIC variable-length integer.
///
/// The crate's own `write_varint` is crate-private, and these tests are a separate compilation
/// unit, so the four widths are spelled again here rather than reached for.
fn varint(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=0x3f => out.push(value as u8),
        0x40..=0x3fff => out.extend_from_slice(&((value as u16) | 0x4000).to_be_bytes()),
        0x4000..=0x3fff_ffff => {
            out.extend_from_slice(&((value as u32) | 0x8000_0000).to_be_bytes());
        }
        _ => out.extend_from_slice(&(value | 0xc000_0000_0000_0000).to_be_bytes()),
    }
}

/// A transport close frame carrying all three of its fields, built by hand.
///
/// `encode_close_record` cannot produce one: it takes a [`CloseReason`], and the public
/// constructors set the triggering frame type to zero with no way to change it — which is the
/// field Spec FR-013 names alongside the code and the reason. The field order is dwnx's
/// reader's (`deps/dwnx/lib/dwnx_conn.c:1982-2038`): type, error code, triggering frame type,
/// reason length, reason.
fn transport_close_frame(error_code: u64, frame_type: u64, reason: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x1c];
    varint(&mut frame, error_code);
    varint(&mut frame, frame_type);
    varint(&mut frame, reason.len() as u64);
    frame.extend_from_slice(reason);
    frame
}

/// Asserts that `framer` latched a transport close with exactly these three fields.
fn assert_close(framer: &RecordFramer, error_code: u64, frame_type: u64, reason: &[u8], at: &str) {
    let decoded = framer
        .close_reason()
        .unwrap_or_else(|| panic!("no close was reported {at}"));
    assert_eq!(decoded.kind(), CloseKind::Transport, "kind, {at}");
    assert_eq!(decoded.error_code(), error_code, "error code, {at}");
    assert_eq!(decoded.frame_type(), frame_type, "frame type, {at}");
    assert_eq!(decoded.reason(), reason, "reason, {at}");
}

/// All three fields of a close survive, wherever the close sits in its record (Spec SC-012).
///
/// The scan looks at the whole record rather than its first frame, and the fast path did not
/// narrow that: what changed is which buffer it looks at, not how far. So the close is put
/// behind nothing, behind a padding frame, and behind a MAX_DATA frame — the last of which the
/// scan can only step over by knowing a frame's length, which is the part a first-frame decoder
/// gets wrong silently.
///
/// The triggering frame type is asserted alongside the code and the reason because it is the
/// field a close carries that this crate's own constructors cannot set, so it is the one a
/// round trip through them would never notice losing.
#[test]
fn a_close_reports_its_code_frame_type_and_reason_wherever_it_sits_in_its_record() {
    let reason = b"the peer named the frame that provoked it";
    let close = transport_close_frame(0x0a, 0x1c, reason);

    let leadings: [&[u8]; 3] = [&[], &[0x00, 0x00, 0x00], &[0x10, 0x44, 0x00]];
    for leading in leadings {
        let mut payload = leading.to_vec();
        payload.extend_from_slice(&close);
        let stream = record(&payload);
        let at = &format!(
            "with {} bytes of frames in front of the close",
            leading.len()
        );

        // The record whole in one call, which is the arriving-contiguously path.
        let mut whole = RecordFramer::new();
        whole.consume(&stream).expect("a close record");
        assert_close(&whole, 0x0a, 0x1c, reason, at);

        // And the record behind other records in the same call, so that the payload handed to
        // the scan is a slice of a longer buffer rather than the whole of one.
        let mut trailing = stream.clone();
        trailing.extend_from_slice(&record(&[0x02; 9]));
        let mut buried = RecordFramer::new();
        buried.consume(&trailing).expect("a close and a trailer");
        assert_close(&buried, 0x0a, 0x1c, reason, at);
    }
}

/// A close record cut anywhere decodes exactly as the same record delivered whole (SC-013).
///
/// Every split point rather than one, because the two paths through `consume` are selected by
/// where the cut falls: a cut inside the payload puts the record on the reassembling path, a
/// cut in or before the length prefix leaves the payload contiguous in the second chunk and
/// puts it on the scan-in-place path. A test that picked a single split would exercise one of
/// them and report the other as covered.
///
/// One byte per read is the extreme of the first, and the case the module's retention exists
/// for. And the byte stream ending immediately after the record's last byte is asserted as
/// well: that is the moment the connection asks whether the peer stopped cleanly, and a framer
/// that had deferred anything to a following call would answer wrongly with nothing left to
/// correct it.
#[test]
fn a_close_record_cut_anywhere_decodes_as_the_whole_one_does() {
    let reason = b"cut this record wherever you like";
    let mut payload = vec![0x10, 0x44, 0x00];
    payload.extend_from_slice(&transport_close_frame(0x0b, 0x08, reason));
    let stream = record(&payload);

    let mut whole = RecordFramer::new();
    whole.consume(&stream).expect("a close record");
    assert_close(&whole, 0x0b, 0x08, reason, "delivered whole");
    assert!(
        whole.at_boundary(),
        "the stream ends immediately after the record's last byte"
    );

    for split in 0..=stream.len() {
        let mut framer = RecordFramer::new();
        framer.consume(&stream[..split]).expect("the first chunk");
        framer.consume(&stream[split..]).expect("the second chunk");
        assert_close(&framer, 0x0b, 0x08, reason, &format!("cut at {split}"));
        assert!(framer.at_boundary(), "cut at {split}");
    }

    let mut single = RecordFramer::new();
    for byte in &stream {
        single.consume(&[*byte]).expect("one byte per read");
    }
    assert_close(&single, 0x0b, 0x08, reason, "one byte per read");
    assert!(
        single.at_boundary(),
        "the last byte of the record leaves the framer between records"
    );
}

/// The scan looks at the declared length and not at whatever else the read brought.
///
/// The second precondition of the fast path, and the one whose failure is silent rather than
/// loud. `decode_close_frame` takes a payload with its length prefix already stripped; hand it
/// the rest of the inbound slice and it walks straight out of the record it was asked about,
/// reads the *next* record's length prefix as a frame type, and reports whatever it makes of
/// the bytes behind it — a close attributed to a record that did not contain one, assembled
/// from another record's fields.
///
/// The bytes are chosen so that exactly that happens rather than so that it might. The close
/// record's payload is 28 bytes, so its one-byte length prefix is `0x1c`, which is the
/// CONNECTION_CLOSE frame type; and the real close's triggering-frame field is `0x02`, which
/// lands where a reason length would be read and is short enough for the bytes behind it to
/// satisfy it. A scan that ran past the ordinary record's declared length therefore finds a
/// well-formed close with an error code of `0x1c` — every field of it wrong, and nothing about
/// it malformed enough to be rejected.
///
/// The record is fed with the close behind it in the same call, cut so that only the first
/// record completes. Nothing may be latched at that point. The close is then completed and must
/// arrive intact, which is what says the guard did not simply make the framer blind.
#[test]
fn a_record_is_scanned_to_its_declared_length_and_no_further() {
    let reason = b"twenty-four bytes of why";
    let close_frame = transport_close_frame(0x0c, 0x02, reason);
    assert_eq!(
        close_frame.len(),
        0x1c,
        "the length prefix has to be the close frame type for this test to test anything"
    );

    let ordinary = short_record(&[0x00]);
    let close = short_record(&close_frame);
    let mut stream = ordinary.clone();
    stream.extend_from_slice(&close);

    // The ordinary record entire, plus enough of the close record behind it that a scan reading
    // past the declared length would find a complete close rather than running out of bytes.
    let cut = ordinary.len() + 7;
    let mut framer = RecordFramer::new();
    framer
        .consume(&stream[..cut])
        .expect("one record and part of another");
    assert!(
        framer.latched_close().is_none(),
        "a record carrying one padding frame latched a close, which means the scan read past \
         the record's declared length and into the next one"
    );

    framer
        .consume(&stream[cut..])
        .expect("the rest of the close record");
    assert_close(&framer, 0x0c, 0x02, reason, "behind an ordinary record");
}

/// A record half of which arrived earlier is not mistaken for a whole one.
///
/// The first precondition of the fast path. The slice at hand holds the record's remaining
/// declared length, which is the same test the fast path applies — so without the second half
/// of the condition, that the retention buffer is empty, the tail of a record would be scanned
/// as though it were the record.
///
/// The payload here is chosen so that the difference is observable rather than theoretical: an
/// unrecognised frame type ends the scan, because without its length there is no way to find
/// the next frame (`src/io/close.rs`). The whole record therefore carries no reportable close.
/// Its tail, taken alone, is a close frame and nothing else. A framer that scanned the tail
/// would report a close the record does not have.
#[test]
fn the_tail_of_a_record_is_not_scanned_as_though_it_were_the_record() {
    let close = transport_close_frame(0x0d, 0x1e, b"unreachable behind an unknown frame");
    let mut payload = vec![0x1e];
    payload.extend_from_slice(&close);
    let stream = record(&payload);

    // Cut so that the second call holds exactly the rest of the declared length: the first
    // chunk is the prefix and the unknown frame type, the second is the close frame.
    let cut = 2 + 1;
    let mut framer = RecordFramer::new();
    framer.consume(&stream[..cut]).expect("the first chunk");
    framer.consume(&stream[cut..]).expect("the second chunk");
    assert!(
        framer.close_reason().is_none(),
        "the record's tail was scanned as if it were the whole record, so a close behind a \
         frame of unknown length was reported"
    );

    // The same record delivered whole agrees, which is what makes the two paths one behaviour.
    let mut whole = RecordFramer::new();
    whole.consume(&stream).expect("a well-formed record");
    assert!(whole.close_reason().is_none());
}
