//! Per-stream sending state: outgoing bodies, and the offsets that release their buffers.
//!
//! # Why the buffers cannot simply be dropped when the write returns
//!
//! nghttp3 has no copying data source. Its data callback fills vectors that point at the
//! *application's* memory, and those pointers are queued and read again on every later
//! `writev_stream` until the peer acknowledges the bytes. Freeing a buffer any earlier is
//! a use-after-free, and nghttp3's own teardown will not do it for us: `delete_outq` frees
//! only the buffers it allocated itself and deliberately leaves application-owned ones
//! alone. So this module owns them, and releases them on exactly three events —
//! acknowledgement, stream close, and connection drop.
//!
//! # Why the entries are ordinary values rather than raw pointers
//!
//! `ngnet-h2` keeps its equivalent registry as `NonNull` entries, because libnghttp2
//! stores the address of each entry in its data-source union and writes through it while
//! a mutable borrow of the registry is live. nghttp3 does no such thing here: the body is
//! found by stream identifier through the installed bridge, and no address of anything in
//! this module is ever handed to C. The one thing C does hold a pointer to is the *payload*
//! of a [`RetainedBytes`], which is behind an `Arc` and therefore does not move when the
//! queue holding the handle does. Ordinary ownership is sound here, so it is what is used.

use std::collections::{HashMap, VecDeque};

use crate::body::{BodyOutcome, BodySource, RetainedBytes};
use crate::error::{Error, Result};
use crate::stream::StreamId;

/// How a body finishes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BodyEnd {
    /// The sending direction of the stream closes with the last byte of the body.
    Stream,
    /// The body ends, but the stream stays open for a trailing field section.
    Trailers,
}

/// One outgoing body, and the buffers nghttp3 is still holding pointers into.
pub(crate) struct BodyEntry {
    source: Box<dyn BodySource>,
    /// Pieces the source produced that did not fit into one callback's vector array.
    ///
    /// nghttp3 offers a fixed eight vectors per call, so a source that hands over more
    /// than that would otherwise have the surplus silently dropped.
    pending: VecDeque<RetainedBytes>,
    /// Set once the source has signalled the end; acted on when `pending` drains.
    end: Option<BodyEnd>,
    /// One element per non-empty vector handed to nghttp3, in the order handed over.
    ///
    /// The granularity is deliberate. Keying this by source buffer instead would go wrong
    /// for a buffer split across several vectors: it would either be released as soon as
    /// its first vector was acknowledged, or never, depending on which length was
    /// compared. One element per vector makes the queue's lengths add up to exactly the
    /// byte count nghttp3 reports.
    retained: VecDeque<RetainedBytes>,
    /// Acknowledged bytes that do not yet cover the front of `retained`.
    carry: u64,
}

/// What a body has for one invocation of the data callback.
pub(crate) enum Handover {
    /// Nothing is available; the stream defers until it is resumed.
    Defer,
    /// The source has given up, and the connection must fail.
    Fail,
    /// Bytes and/or an end marker are available.
    Ready,
}

impl BodyEntry {
    fn new(source: Box<dyn BodySource>) -> Self {
        Self {
            source,
            pending: VecDeque::new(),
            end: None,
            retained: VecDeque::new(),
            carry: 0,
        }
    }

    /// Asks the source for more, if the last call's surplus has been handed over and the
    /// end has not already been signalled.
    ///
    /// The source is never consulted again after it signals the end, so a source whose
    /// `next` is not idempotent cannot be made to produce bytes after its last ones.
    pub(crate) fn begin_round(&mut self) -> Handover {
        if !self.pending.is_empty() || self.end.is_some() {
            return Handover::Ready;
        }
        let (pieces, end) = match self.source.next() {
            BodyOutcome::Defer => return Handover::Defer,
            BodyOutcome::Fail => return Handover::Fail,
            BodyOutcome::Wrote(pieces) => (pieces, None),
            BodyOutcome::Eof(pieces) => (pieces, Some(BodyEnd::Stream)),
            BodyOutcome::EofWithTrailers(pieces) => (pieces, Some(BodyEnd::Trailers)),
        };
        // Empty pieces are dropped here rather than queued. nghttp3 skips zero-length
        // vectors without queueing them, so an element for one would sit at the front of
        // the retain queue waiting for an acknowledgement that can never arrive, and
        // every buffer behind it would be stuck too.
        self.pending
            .extend(pieces.into_iter().filter(|piece| !piece.is_empty()));
        self.end = end;
        Handover::Ready
    }

    /// Takes the next piece to write into nghttp3's vector array.
    pub(crate) fn take_piece(&mut self) -> Option<RetainedBytes> {
        self.pending.pop_front()
    }

    /// Records a piece as handed over, keeping its bytes alive until they are acknowledged.
    pub(crate) fn retain(&mut self, piece: RetainedBytes) {
        debug_assert!(!piece.is_empty(), "a zero-length vector is never queued");
        self.retained.push_back(piece);
    }

    /// The end marker to report, once everything the source gave has been handed over.
    pub(crate) fn end_reached(&self) -> Option<BodyEnd> {
        self.pending.is_empty().then_some(self.end).flatten()
    }

    /// Releases the buffers covered by `n` more acknowledged bytes.
    ///
    /// `n` is a delta, not a cumulative offset: nghttp3 computes it per outbound buffer as
    /// `min(offset, ack_base + buflen) - ack_offset` and reports it only for
    /// application-owned buffers, so the deltas sum to exactly the bytes of this queue.
    /// A partially covered element stays retained, because nghttp3 still points into it.
    pub(crate) fn on_acked(&mut self, n: u64) {
        self.carry = self.carry.saturating_add(n);
        while let Some(front) = self.retained.front() {
            let len = front.len() as u64;
            if self.carry < len {
                break;
            }
            self.carry -= len;
            self.retained.pop_front();
        }
    }

    /// How many buffers are still held for this stream.
    pub(crate) fn retained_buffers(&self) -> usize {
        self.retained.len()
    }
}

/// The write and acknowledgement offsets of one stream, in raw stream bytes.
#[derive(Clone, Copy, Default, Debug)]
struct Offsets {
    /// Bytes reported accepted by the transport, cumulative.
    committed: u64,
    /// Bytes reported acknowledged by the peer, cumulative.
    acked: u64,
}

/// Outgoing bodies and send offsets, keyed by stream.
///
/// The offsets live here rather than on the connection because they are bounded and pruned
/// by exactly the same event as the bodies — a stream closing — and are reached from the
/// same callback.
#[derive(Default)]
pub(crate) struct BodyRegistry {
    bodies: HashMap<StreamId, BodyEntry>,
    offsets: HashMap<StreamId, Offsets>,
}

impl BodyRegistry {
    /// Takes ownership of a body for a stream.
    ///
    /// Refuses to replace an existing one. That is a memory-safety guard rather than
    /// tidiness: replacing would drop the previous entry's retained buffers while nghttp3
    /// still held pointers into them.
    pub(crate) fn attach(&mut self, stream: StreamId, source: Box<dyn BodySource>) -> Result<()> {
        if self.bodies.contains_key(&stream) {
            return Err(Error::invalid_input(
                "that stream already carries an outgoing body",
            ));
        }
        self.bodies.insert(stream, BodyEntry::new(source));
        Ok(())
    }

    /// Drops a body that was attached but whose submission then failed.
    pub(crate) fn detach(&mut self, stream: StreamId) {
        self.bodies.remove(&stream);
    }

    /// The body for a stream, if it has one.
    pub(crate) fn entry_mut(&mut self, stream: StreamId) -> Option<&mut BodyEntry> {
        self.bodies.get_mut(&stream)
    }

    /// Forgets everything about a stream, releasing whatever it still held.
    ///
    /// Called from the stream-close callback, which is the single detach point on the
    /// close path.
    pub(crate) fn forget(&mut self, stream: StreamId) {
        self.bodies.remove(&stream);
        self.offsets.remove(&stream);
    }

    /// Releases everything, for a connection that can no longer send.
    pub(crate) fn clear(&mut self) {
        self.bodies.clear();
        self.offsets.clear();
    }

    /// Buffers currently held across all streams.
    pub(crate) fn retained_buffers(&self) -> usize {
        self.bodies.values().map(BodyEntry::retained_buffers).sum()
    }

    /// Records bytes the transport accepted for a stream.
    pub(crate) fn record_committed(&mut self, stream: StreamId, n: usize) {
        if n == 0 {
            return;
        }
        let offsets = self.offsets.entry(stream).or_default();
        offsets.committed = offsets.committed.saturating_add(n as u64);
    }

    /// Checks and records bytes the peer acknowledged for a stream.
    ///
    /// Rejecting an acknowledgement that runs past what was written is a memory-safety
    /// requirement, not ergonomics. `nghttp3_stream_update_ack_offset` fires the
    /// acknowledgement callback for the front buffer *before* it checks whether that
    /// buffer has been written yet, so an over-report would release a buffer nghttp3 has
    /// not sent and still points at — handing it a dangling pointer on the next write.
    pub(crate) fn record_acked(&mut self, stream: StreamId, n: u64) -> Result<()> {
        const OVER: &str =
            "more bytes were reported acknowledged than were ever written to that stream";
        if n == 0 {
            return Ok(());
        }
        // A stream with no offsets was either never written to or has since closed. The
        // two are not distinguished because the answer is the same either way: there is
        // nothing left to release, and reporting acknowledgement for it is a mistake.
        // nghttp3 itself returns success for an unknown stream, but doing that here would
        // turn a genuine over-report into silence the moment a stream closed.
        let Some(offsets) = self.offsets.get_mut(&stream) else {
            return Err(Error::invalid_input(
                "that stream has nothing outstanding to acknowledge; it was never written \
                 to, or it has already closed",
            ));
        };
        let acked = offsets
            .acked
            .checked_add(n)
            .ok_or(Error::invalid_input(OVER))?;
        if acked > offsets.committed {
            return Err(Error::invalid_input(OVER));
        }
        offsets.acked = acked;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::FixedBody;

    fn entry(pieces: Vec<&[u8]>) -> BodyEntry {
        struct Once(Option<Vec<RetainedBytes>>);
        impl BodySource for Once {
            fn next(&mut self) -> BodyOutcome {
                match self.0.take() {
                    Some(pieces) => BodyOutcome::Eof(pieces),
                    None => BodyOutcome::Eof(Vec::new()),
                }
            }
        }
        BodyEntry::new(Box::new(Once(Some(
            pieces.into_iter().map(RetainedBytes::from).collect(),
        ))))
    }

    /// Hands everything the entry has over, as nghttp3's callback would.
    fn hand_over(entry: &mut BodyEntry) {
        while let Some(piece) = entry.take_piece() {
            entry.retain(piece);
        }
    }

    #[test]
    fn a_partly_acknowledged_buffer_stays_retained() {
        let mut entry = entry(vec![b"hello"]);
        assert!(matches!(entry.begin_round(), Handover::Ready));
        hand_over(&mut entry);
        assert_eq!(entry.retained_buffers(), 1);

        entry.on_acked(4);
        assert_eq!(
            entry.retained_buffers(),
            1,
            "nghttp3 still points into the buffer until its last byte is acknowledged"
        );
        entry.on_acked(1);
        assert_eq!(entry.retained_buffers(), 0);
    }

    #[test]
    fn a_delta_spanning_several_buffers_releases_all_of_them() {
        let mut entry = entry(vec![b"aa", b"bbb", b"c"]);
        assert!(matches!(entry.begin_round(), Handover::Ready));
        hand_over(&mut entry);
        assert_eq!(entry.retained_buffers(), 3);

        entry.on_acked(5);
        assert_eq!(entry.retained_buffers(), 1, "the boundary lands exactly");
        entry.on_acked(1);
        assert_eq!(entry.retained_buffers(), 0);
    }

    #[test]
    fn empty_pieces_are_never_queued() {
        let mut entry = entry(vec![b"", b"x", b""]);
        assert!(matches!(entry.begin_round(), Handover::Ready));
        hand_over(&mut entry);
        assert_eq!(
            entry.retained_buffers(),
            1,
            "a queued zero-length element would never be acknowledged, and would block \
             everything behind it"
        );
    }

    #[test]
    fn the_end_is_reported_only_once_everything_has_been_handed_over() {
        let mut entry = entry(vec![b"one", b"two"]);
        assert!(matches!(entry.begin_round(), Handover::Ready));
        let first = entry.take_piece().expect("a piece");
        entry.retain(first);
        assert_eq!(entry.end_reached(), None, "one piece is still pending");
        hand_over(&mut entry);
        assert_eq!(entry.end_reached(), Some(BodyEnd::Stream));
    }

    #[test]
    fn a_failing_source_reports_a_failure() {
        struct Broken;
        impl BodySource for Broken {
            fn next(&mut self) -> BodyOutcome {
                BodyOutcome::Fail
            }
        }
        let mut entry = BodyEntry::new(Box::new(Broken));
        assert!(matches!(entry.begin_round(), Handover::Fail));
    }

    #[test]
    fn a_deferring_source_reports_a_deferral() {
        struct Never;
        impl BodySource for Never {
            fn next(&mut self) -> BodyOutcome {
                BodyOutcome::Defer
            }
        }
        let mut entry = BodyEntry::new(Box::new(Never));
        assert!(matches!(entry.begin_round(), Handover::Defer));
    }

    #[test]
    fn acknowledgement_beyond_what_was_written_is_refused() {
        let mut registry = BodyRegistry::default();
        let stream = StreamId::new(0).unwrap();
        registry.record_committed(stream, 10);

        registry
            .record_acked(stream, 6)
            .expect("within what was written");
        let error = registry
            .record_acked(stream, 5)
            .expect_err("6 + 5 runs past the 10 bytes committed");
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        registry
            .record_acked(stream, 4)
            .expect("the remainder is still accepted, so nothing was consumed by the refusal");
    }

    #[test]
    fn acknowledgement_on_a_stream_that_never_wrote_is_refused() {
        let mut registry = BodyRegistry::default();
        let stream = StreamId::new(0).unwrap();
        registry
            .record_acked(stream, 0)
            .expect("nothing is always fine");
        assert!(registry.record_acked(stream, 1).is_err());
    }

    #[test]
    fn a_second_body_on_one_stream_is_refused() {
        let mut registry = BodyRegistry::default();
        let stream = StreamId::new(0).unwrap();
        registry
            .attach(stream, Box::new(FixedBody::new(b"first".to_vec())))
            .expect("the first body");
        registry
            .attach(stream, Box::new(FixedBody::new(b"second".to_vec())))
            .expect_err("replacing would free buffers nghttp3 still points at");
    }

    #[test]
    fn forgetting_a_stream_prunes_its_offsets_too() {
        let mut registry = BodyRegistry::default();
        let stream = StreamId::new(0).unwrap();
        registry.record_committed(stream, 4);
        registry.forget(stream);
        assert!(
            registry.record_acked(stream, 1).is_err(),
            "a closed stream must not keep an offset entry alive"
        );
    }
}
