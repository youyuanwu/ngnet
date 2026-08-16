//! Where the record boundaries are, counted here because dwnx will not say.
//!
//! # Why the layer frames records a second time
//!
//! dwnx already parses records: it reads each length prefix, walks the frames inside, and
//! buffers whatever arrived incomplete. Doing that again here looks like duplicated work, and
//! the obvious alternative -- ask the state machine where it stands -- is the one that was
//! tried and rejected, because there is nothing to ask. `dwnx_conn_read` returns `0` for "that
//! was fine, feed me more", whether it stopped between records or halfway through a length
//! prefix (`deps/dwnx/lib/dwnx_conn.c:1158-1228`). There is no accessor for the record reader's
//! state, and the reader itself is private.
//!
//! Two questions this layer must answer therefore have no answer from below.
//!
//! **Did the byte stream end cleanly?** A peer that stops speaking between records has said
//! everything it meant to say. A peer that stops partway through one has lost bytes, and
//! whatever that record carried is gone in a way the peer does not know about. dwnx reports
//! both as "need more input". Getting this wrong means an incomplete transfer is reported to
//! the caller as a clean ending, which is the failure mode with no symptom.
//!
//! **What did the peer's close say?** dwnx parses CONNECTION_CLOSE into a private frame struct
//! and returns `DWNX_ERR_DRAINING` (`deps/dwnx/lib/dwnx_conn.c:2044-2110`), so the kind, error
//! code, frame type and reason are unreachable from outside. Recovering them means holding on
//! to the record's own bytes and decoding them here; see [`super::close`].
//!
//! So the framer consumes the same bytes that go to [`Conn::read`](crate::Conn::read), reads
//! each length prefix, counts the payload down, and knows at every instant whether it stands at
//! a boundary. It parses nothing else: what is inside a record is dwnx's business, and this
//! module's only interest in the contents is finding a close.
//!
//! # Retention, and why a close is latched rather than windowed
//!
//! The framer keeps the payload of the record currently arriving, and nothing else -- until a
//! record completes and turns out to contain a close, at which point that payload is **latched
//! permanently** and no further record is retained.
//!
//! The rejected alternative is the natural one: a sliding window holding the most recent
//! complete record, replaced when the next record starts. It loses closes. `Conn::read` reports
//! `PeerClosed` only *after* it has consumed the close record, and a single read may hand it a
//! chunk with more bytes after that record -- a peer is entitled to write the close and
//! whatever else was already queued in one go, and a byte stream is entitled to deliver them
//! together. A window would have begun the next record, evicted the close, and left the caller
//! with "the peer closed" and no code, no reason and no frame type: Spec FR-016 unmet, in
//! exactly the case that is hardest to reproduce.
//!
//! Latching costs nothing, because a close is terminal. Nothing after it will ever be
//! delivered, so retaining nothing after it loses nothing.
//!
//! # The bound
//!
//! One record in progress plus one latched close: at most two records' worth. A record is at
//! most [`DEFAULT_MAX_RECORD_SIZE`](crate::DEFAULT_MAX_RECORD_SIZE) = 16382 bytes, and dwnx
//! overwrites any configured maximum with that value at construction, so the ceiling is fixed
//! rather than negotiated. The framer's retention is therefore under 32 KiB per connection,
//! whatever the peer does, which is why holding the bytes at all is acceptable.
//!
//! A declared length above the maximum is refused rather than trusted, so a peer cannot use
//! the length prefix to make this layer allocate. dwnx refuses the same record for the same
//! reason (`deps/dwnx/lib/dwnx_conn.c:1200-1204`); the framer refuses it first, since it is the
//! one holding the buffer.

use crate::ccerr::CloseReason;
use crate::io::close::decode_close_frame;
use crate::io::error::{Error, ErrorKind, Result};

/// The largest QUIC variable-length integer, `2^62 - 1`.
const MAX_VARINT: u64 = crate::raw::NGNET_QMUX_MAX_VARINT;

/// The largest record any QMux peer may send.
///
/// Constant rather than negotiated: dwnx overwrites a configured `max_record_size` with its
/// own default immediately after copying the transport parameters in, so both endpoints use
/// this value whatever they advertised.
const MAX_RECORD_SIZE: usize = crate::DEFAULT_MAX_RECORD_SIZE as usize;

/// The most bytes a QUIC variable-length integer occupies.
const MAX_VARINT_BYTES: usize = 8;

/// Decodes a QUIC variable-length integer from the front of `input`.
///
/// Returns the value and how many bytes it occupied, or [`None`] if `input` holds fewer bytes
/// than the encoding's first byte says it needs.
pub(super) fn read_varint(input: &[u8]) -> Option<(u64, usize)> {
    let first = *input.first()?;
    let len = 1usize << (first >> 6);
    let bytes = input.get(..len)?;
    let mut value = u64::from(first & 0x3f);
    for byte in &bytes[1..] {
        value = (value << 8) | u64::from(*byte);
    }
    Some((value, len))
}

/// Appends `value` as a QUIC variable-length integer, in the shortest encoding that holds it.
///
/// A value above [`MAX_VARINT`] has no encoding at all, and is clamped to it rather than
/// panicking or silently writing a truncated field: the only values that reach here are a
/// caller's error code and a reason length, and a close that cannot be written is worse than a
/// close carrying a saturated code.
///
/// dwnx emits every record length as a two-byte prefix (`deps/dwnx/lib/dwnx_qre.c:93-108`) and
/// accepts all four widths on read (`deps/dwnx/lib/dwnx_conn.c:1158-1228`), so the shortest
/// encoding is both legal and what a conforming peer must handle.
pub(super) fn write_varint(out: &mut Vec<u8>, value: u64) {
    let value = value.min(MAX_VARINT);
    match value {
        0..=0x3f => out.push(value as u8),
        0x40..=0x3fff => out.extend_from_slice(&(value as u16 | 0x4000).to_be_bytes()),
        0x4000..=0x3fff_ffff => out.extend_from_slice(&(value as u32 | 0x8000_0000).to_be_bytes()),
        _ => out.extend_from_slice(&(value | 0xc000_0000_0000_0000).to_be_bytes()),
    }
}

/// How many bytes [`write_varint`] would append for `value`.
pub(super) const fn varint_len(value: u64) -> usize {
    match value {
        0..=0x3f => 1,
        0x40..=0x3fff => 2,
        0x4000..=0x3fff_ffff => 4,
        _ => 8,
    }
}

/// A partially received length prefix.
///
/// The prefix is a variable-length integer, so its own width is not known until its first byte
/// arrives, and either the first byte or any of the following seven may be the last byte of a
/// read. Accumulating them is the whole reason this is a struct rather than a call to
/// [`read_varint`].
#[derive(Debug, Default)]
struct LengthPrefix {
    bytes: [u8; MAX_VARINT_BYTES],
    len: usize,
}

impl LengthPrefix {
    /// Whether no byte of a prefix has been seen.
    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How many bytes this prefix will occupy in total, once its first byte has arrived.
    fn width(&self) -> Option<usize> {
        self.bytes
            .first()
            .filter(|_| self.len > 0)
            .map(|first| 1usize << (first >> 6))
    }

    /// Feeds bytes in, returning the decoded length once the prefix is complete.
    ///
    /// Consumes only what the prefix needs, and reports how much that was.
    fn feed(&mut self, input: &[u8]) -> (Option<u64>, usize) {
        let mut taken = 0;
        while taken < input.len() {
            self.bytes[self.len] = input[taken];
            self.len += 1;
            taken += 1;
            if self.width() == Some(self.len) {
                let (value, _) = read_varint(&self.bytes[..self.len])
                    .expect("a prefix of its own declared width decodes");
                self.len = 0;
                return (Some(value), taken);
            }
        }
        (None, taken)
    }
}

/// Where the framer stands in the record structure.
#[derive(Debug)]
enum State {
    /// Reading a length prefix, which may be partly arrived.
    Length(LengthPrefix),
    /// Inside a record, with this many payload bytes still to come.
    Payload(usize),
}

/// Tracks record boundaries in a QMux byte stream, and latches the peer's close.
///
/// Fed the same bytes as [`Conn::read`](crate::Conn::read), in the same order. See the module
/// documentation for why the layer counts records itself, and for the retention rule.
///
/// # Example
///
/// ```
/// use ngnet_qmux::io::RecordFramer;
///
/// let mut framer = RecordFramer::new();
/// assert!(framer.at_boundary(), "a fresh framer stands between records");
///
/// // One record: a two-byte payload behind a one-byte length prefix.
/// framer.consume(&[0x02, 0x10]).expect("a well-formed prefix");
/// assert!(!framer.at_boundary(), "the record is half arrived");
///
/// framer.consume(&[0x40]).expect("the rest of the payload");
/// assert!(framer.at_boundary(), "and now it is whole");
/// ```
#[derive(Debug)]
pub struct RecordFramer {
    state: State,
    /// The payload of the record currently arriving, empty once a close has been latched.
    record: Vec<u8>,
    /// The payload of the record that carried the peer's close, kept for as long as the
    /// connection object lives.
    close: Option<Vec<u8>>,
}

impl Default for RecordFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordFramer {
    /// A framer standing at a record boundary, having seen nothing.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: State::Length(LengthPrefix::default()),
            record: Vec::new(),
            close: None,
        }
    }

    /// Feeds inbound bytes, in the order they arrived.
    ///
    /// Any split is acceptable: a record spread over any number of calls, several records in
    /// one call, and a length prefix cut in half all behave identically to the whole stream
    /// arriving at once. That is the property the connection depends on, since a byte stream
    /// chooses its own chunk boundaries.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Protocol`] if a record declares a length above the maximum, or a length of
    /// zero. Both are refusals dwnx would also make, made here first because this is the side
    /// holding a buffer.
    ///
    /// A framer that has returned an error must not be fed again. It stopped partway through a
    /// record whose declared length was a lie, so it no longer knows where the next record
    /// begins -- and neither does dwnx, which is why the connection ends there.
    pub fn consume(&mut self, bytes: &[u8]) -> Result<()> {
        let mut rest = bytes;
        while !rest.is_empty() {
            match &mut self.state {
                State::Length(prefix) => {
                    let (complete, taken) = prefix.feed(rest);
                    rest = &rest[taken..];
                    if let Some(length) = complete {
                        self.begin_record(length)?;
                    }
                }
                State::Payload(remaining) => {
                    let take = (*remaining).min(rest.len());
                    if self.close.is_none() {
                        self.record.extend_from_slice(&rest[..take]);
                    }
                    *remaining -= take;
                    rest = &rest[take..];
                    if *remaining == 0 {
                        self.finish_record();
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether the framer stands exactly between records.
    ///
    /// True before the first byte of a length prefix and after the last byte of a payload,
    /// false anywhere else -- including partway through a length prefix, which is a peer that
    /// began announcing a record and stopped.
    ///
    /// This is what separates a byte stream that ended cleanly from one that was truncated
    /// (Spec FR-017): at end-of-stream, `true` here means everything the peer sent was whole.
    #[must_use]
    pub const fn at_boundary(&self) -> bool {
        match &self.state {
            State::Length(prefix) => prefix.is_empty(),
            State::Payload(_) => false,
        }
    }

    /// The payload of the record that carried the peer's connection close, if one arrived.
    ///
    /// Latched: once set it never changes and is never evicted, so it survives whatever else
    /// the same read delivered.
    #[must_use]
    pub fn latched_close(&self) -> Option<&[u8]> {
        self.close.as_deref()
    }

    /// The peer's close, decoded from the latched record.
    ///
    /// Decoded on demand rather than kept alongside the bytes, so that the retention bound is
    /// exactly the two records it claims to be. A caller asks at most once per connection,
    /// when the state machine reports the close.
    #[must_use]
    pub fn close_reason(&self) -> Option<CloseReason> {
        decode_close_frame(self.close.as_deref()?)
    }

    /// How many bytes the framer is holding.
    ///
    /// Bounded by one record in progress plus one latched close; see the module documentation.
    /// Exposed so a test can assert the bound rather than trust it.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.record.len() + self.close.as_ref().map_or(0, Vec::len)
    }

    /// Starts a record of `length` bytes, having just read its prefix.
    fn begin_record(&mut self, length: u64) -> Result<()> {
        if length == 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                "the peer declared a record of zero length",
            ));
        }
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        if length > MAX_RECORD_SIZE {
            return Err(Error::new(
                ErrorKind::Protocol,
                "the peer declared a record larger than the maximum record size",
            ));
        }

        self.record.clear();
        if self.close.is_none() {
            self.record.reserve(length);
        }
        self.state = State::Payload(length);
        Ok(())
    }

    /// Completes the record in progress, latching it if it carried a close.
    fn finish_record(&mut self) {
        self.state = State::Length(LengthPrefix::default());

        if self.close.is_some() {
            return;
        }
        // The whole record is scanned, never just its first frame: a record may carry several
        // frames and dwnx returns to its frame-type state for each
        // (`deps/dwnx/lib/dwnx_record_reader.c:88-103`), so a close may follow anything.
        if decode_close_frame(&self.record).is_some() {
            self.close = Some(core::mem::take(&mut self.record));
        } else {
            self.record.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_varint_width_round_trips() {
        for value in [
            0,
            0x3f,
            0x40,
            0x3fff,
            0x4000,
            0x3fff_ffff,
            0x4000_0000,
            MAX_VARINT,
        ] {
            let mut out = Vec::new();
            write_varint(&mut out, value);
            assert_eq!(out.len(), varint_len(value), "value {value:#x}");
            assert_eq!(
                read_varint(&out),
                Some((value, out.len())),
                "value {value:#x}"
            );
        }
    }

    /// A value with no encoding saturates rather than writing a corrupt field.
    #[test]
    fn a_value_above_the_varint_bound_is_clamped() {
        let mut out = Vec::new();
        write_varint(&mut out, u64::MAX);
        assert_eq!(read_varint(&out), Some((MAX_VARINT, 8)));
    }

    #[test]
    fn a_varint_shorter_than_its_declared_width_is_incomplete() {
        // A four-byte encoding with only three bytes present.
        assert_eq!(read_varint(&[0x80, 0x00, 0x00]), None);
        assert_eq!(read_varint(&[]), None);
    }

    /// A length prefix that is not the shortest encoding of its value is still legal.
    #[test]
    fn a_wide_prefix_for_a_small_length_is_accepted() {
        let mut framer = RecordFramer::new();
        framer
            .consume(&[0x80, 0x00, 0x00, 0x01, 0x00])
            .expect("a four-byte prefix declaring one byte");
        assert!(framer.at_boundary());
    }
}
