//! Where the record boundaries are, counted here because dwnx will not say.
//!
//! # Why the layer frames records a second time
//!
//! dwnx already parses records: it reads each length prefix, walks the frames inside, and
//! buffers whatever arrived incomplete. Doing that again here looks like duplicated work, and
//! the obvious alternative -- ask the state machine where it stands -- is the one that was
//! tried and rejected, because there is nothing to ask. `dwnx_conn_read` returns `0` for "that
//! was fine, feed me more", whether it stopped between records or halfway through a length
//! prefix (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_conn.c:1158-1228`). There is no
//! accessor for the record reader's
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
//! and returns `DWNX_ERR_DRAINING`
//! (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_conn.c:2044-2110`), so the kind, error
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
//! The framer keeps the payload of the record currently arriving **only while that record is
//! arriving in pieces**, and nothing else -- until a record completes and turns out to contain
//! a close, at which point that payload is **latched permanently** and no further record is
//! retained.
//!
//! The qualification is the whole of the scan-in-place arrangement, and it is stated here
//! because it is what the retention bound now means. A record whose declared length is all
//! present in the slice `consume` was handed is scanned **where it lies**: the bytes are
//! already contiguous in the caller's read buffer, `decode_close_frame` needs nothing but a
//! contiguous payload, and copying them into a buffer of this framer's own in order to look at
//! them buys a second copy of every record for the sake of the one record in a connection's
//! life that carries a close. So the copy is paid only where it buys something -- a record
//! spread over several reads, which has nothing contiguous to scan and must be reassembled
//! before it can be looked at at all.
//!
//! The rejected alternative is scanning each fragment as it arrives and keeping no record
//! buffer whatever. It loses closes, in the same silent way a sliding window does: a close
//! frame cut across two reads is in neither fragment, and a frame *before* the close cut
//! across two reads leaves the scan unable to find where the next frame begins.
//!
//! Three conditions gate the fast path, and the first two silently mis-decode if dropped:
//!
//! 1. **Nothing of this record has been retained yet.** An earlier `consume` may have delivered
//!    the record's first half, in which case the slice at hand holds the remaining declared
//!    length and *not* the whole record. Scanning it would look at the tail of a record and
//!    call it a whole one.
//! 2. **The slice holds the whole declared remainder.** The same requirement from the other
//!    end: a record continuing into the next read is not complete here.
//! 3. **No close has been latched.** Defensive rather than load-bearing, and stated as such:
//!    after a latch nothing further is retained, so an empty retention buffer would no longer
//!    mean "this record has not started". Nothing currently mis-decodes if it is dropped,
//!    because `finish_record` returns early once a close is latched — but that makes condition
//!    1 mean two different things depending on a fact stated somewhere else, and the direction
//!    of the bias here is toward the condition that is true on its own.
//!
//! What is scanned is exactly the declared length's worth and never the rest of the slice:
//! `decode_close_frame` takes a payload with its length prefix already stripped, so handing it
//! the raw inbound bytes would let it walk out of one record and decode a close out of the
//! next record's length prefix and frames -- latching, for the record that did not contain it,
//! a close reason assembled from someone else's fields.
//!
//! A close found in place is still **copied**, because latching it means holding it after
//! `consume` has returned and the slice it was found in belongs to the caller. That copy is
//! once per connection.
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
//! One record in progress plus one latched close: at most two records' worth, and in the
//! ordinary case -- records arriving whole -- nothing at all. A record is at
//! most [`DEFAULT_MAX_RECORD_SIZE`](crate::DEFAULT_MAX_RECORD_SIZE) = 16382 bytes, and dwnx
//! overwrites any configured maximum with that value at construction, so the ceiling is fixed
//! rather than negotiated. The framer's retention is therefore under 32 KiB per connection,
//! whatever the peer does, which is why holding the bytes at all is acceptable.
//!
//! A declared length above the maximum is refused rather than trusted, so a peer cannot use
//! the length prefix to make this layer allocate. dwnx refuses the same record for the same
//! reason (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_conn.c:1200-1204`); the framer refuses
//! it first, since it is the
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
/// dwnx emits every record length as a two-byte prefix
/// (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_qre.c:93-108`) and
/// accepts all four widths on read
/// (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_conn.c:1158-1228`), so the shortest
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
    /// What has arrived so far of a record that is spanning several reads.
    ///
    /// Empty for a record that arrived whole -- that one is scanned where it lies and never
    /// enters this buffer -- and empty once a close has been latched, after which nothing
    /// further is retained. Kept allocated across records so that a connection whose peer
    /// fragments its records pays the growth once rather than per record.
    record: Vec<u8>,
    /// The payload of the record that carried the peer's close, kept for as long as the
    /// connection object lives.
    close: Option<Vec<u8>>,
    /// How many payload bytes have been copied into the framer's retention, cumulatively.
    ///
    /// Compiled only where debug assertions are, and [`RecordFramer::copied_bytes`] says why
    /// that gate rather than another.
    #[cfg(debug_assertions)]
    copied: usize,
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
            #[cfg(debug_assertions)]
            copied: 0,
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
                        self.begin_record(length, rest.len())?;
                    }
                }
                State::Payload(remaining) => {
                    let take = (*remaining).min(rest.len());
                    // The three conditions the module documentation states, in the order it
                    // states them: nothing of this record retained, the whole declared
                    // remainder present, and no close latched. All three, because each one
                    // alone admits a slice that is not this record's whole payload.
                    let in_place =
                        self.record.is_empty() && take == *remaining && self.close.is_none();
                    *remaining -= take;
                    let complete = *remaining == 0;

                    // Exactly the declared length's worth. The bytes after it in `rest` belong
                    // to the next record and must not be scanned as part of this one.
                    let (payload, tail) = rest.split_at(take);
                    rest = tail;

                    if !in_place && self.close.is_none() {
                        self.record.extend_from_slice(payload);
                        // Counted at the copy rather than at the record boundary, so that a
                        // record delivered in fragments is charged the same total as one
                        // delivered whole -- which is the equality that makes this count a
                        // measure of the copying rather than of the framing.
                        #[cfg(debug_assertions)]
                        {
                            self.copied += take;
                        }
                    }

                    if complete {
                        self.finish_record(in_place.then_some(payload));
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
    /// Zero between records, and zero *during* a record that arrived whole, since such a
    /// record is scanned where it lies. Exposed so a test can assert the bound rather than
    /// trust it.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.record.len() + self.close.as_ref().map_or(0, Vec::len)
    }

    /// How many payload bytes this framer has copied into its retention, cumulatively.
    ///
    /// Exposed for the same reason [`RecordFramer::retained_bytes`] is -- so a test can assert
    /// the cost rather than trust the prose -- and gated for a reason that one has no need of.
    ///
    /// What it counts is now a record that arrived in fragments and had to be reassembled
    /// before it could be scanned, plus the one close a connection latches. It used to count
    /// one memcpy per record for every record that arrives, which is the figure the
    /// scan-in-place arrangement removed and which `tests/io_framing.rs` asserts is zero for a
    /// run of whole records. The gate below is unchanged and its reason is unchanged: a later
    /// measurement compares a benchmark run against another run of the same benchmarks, and
    /// instrumentation compiled into one side of that comparison and not the other measures
    /// the instrument, so this counter must be absent from a benchmark build rather than
    /// merely unused in one.
    ///
    /// `cfg(test)` is the obvious gate and cannot be it. This crate's integration tests are
    /// separate compilation units linked against the ordinary library, so a `cfg(test)` item
    /// here would be invisible to `tests/io_framing.rs` -- the same constraint that put
    /// [`super::testing`] in the library rather than under `tests/`. A cargo feature is
    /// available and was rejected too: the crate's feature set is asserted by
    /// `tests/invariants.rs` and a feature nobody enables by default would leave every
    /// verification command in the plan unable to see this, while a feature enabled by default
    /// would be in the benchmark build, which is the one thing that must not happen.
    ///
    /// `cfg(debug_assertions)` is what remains, and it is a better fit than either: it holds
    /// for the dev profile that `cargo test` uses and not for the bench profile, which
    /// inherits release. The field, the increment and this accessor are all absent from a
    /// benchmark build, so the claim that this phase changes nothing on a hot path is a
    /// property of the artefact rather than an assertion about it.
    ///
    /// The cost is that `cargo test --release` cannot name this; `tests/io_framing.rs` records
    /// how it handles that and why the trade is the right way round.
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn copied_bytes(&self) -> usize {
        self.copied
    }

    /// Starts a record of `length` bytes, having just read its prefix.
    ///
    /// `available` is how much of the current slice is left behind the prefix, which is what
    /// decides whether this record will be scanned where it lies or reassembled here.
    fn begin_record(&mut self, length: u64, available: usize) -> Result<()> {
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
        // Reserved only for a record that will actually be accumulated. The reservation exists
        // to stop a record arriving in fragments regrowing the buffer once per fragment; a
        // record whose whole payload is already in the slice at hand is never put in the
        // buffer at all, so reserving for it would be an allocation on the receive hot path
        // bought for nothing. `available` is what the length prefix has already told us.
        if self.close.is_none() && available < length {
            self.record.reserve(length);
        }
        self.state = State::Payload(length);
        Ok(())
    }

    /// Completes the record in progress, latching it if it carried a close.
    ///
    /// `in_place` carries the record's whole payload when it arrived contiguously and was
    /// therefore never copied into the retention buffer; [`None`] means the payload was
    /// reassembled in `record` and is scanned from there.
    fn finish_record(&mut self, in_place: Option<&[u8]>) {
        self.state = State::Length(LengthPrefix::default());

        if self.close.is_some() {
            return;
        }
        // The whole record is scanned, never just its first frame: a record may carry several
        // frames and dwnx returns to its frame-type state for each
        // (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_record_reader.c:88-103`), so a close
        // may follow anything. Which
        // buffer the payload is in changes; that it is scanned end to end does not.
        match in_place {
            Some(payload) => {
                if decode_close_frame(payload).is_some() {
                    // Copied because latching means holding it beyond this call, and these
                    // bytes are the caller's read buffer. Once per connection.
                    self.close = Some(payload.to_vec());
                    #[cfg(debug_assertions)]
                    {
                        self.copied += payload.len();
                    }
                }
            }
            None => {
                if decode_close_frame(&self.record).is_some() {
                    self.close = Some(core::mem::take(&mut self.record));
                } else {
                    self.record.clear();
                }
            }
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
