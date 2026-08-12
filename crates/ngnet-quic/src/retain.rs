//! Holding sent stream data until the peer acknowledges it.
//!
//! ngtcp2 does not copy the stream data it accepts. `ngtcp2_conn_writev_stream` serialises
//! some of it into the packet being built and **keeps the caller's pointer** so it can
//! retransmit if the packet is lost:
//!
//! > The caller must keep the portion of data covered by `|*pdatalen|` bytes in tact until
//! > `ngtcp2_callbacks.acked_stream_data_offset` indicates that they are acknowledged by a
//! > remote endpoint or the stream is closed.
//! >
//! > — `deps/ngtcp2/lib/includes/ngtcp2/ngtcp2.h:5244-5248`
//!
//! A safe API therefore cannot pass the caller's borrow through and return. The caller is
//! free to drop that buffer the instant the call ends, and any later retransmission would
//! read freed memory — a use-after-free reachable from entirely safe code.
//!
//! So the connection keeps its own copy of every byte ngtcp2 accepted, and hands ngtcp2 a
//! pointer into *that*. The copy is released when the acknowledgement arrives, or when the
//! stream ends.
//!
//! # Why chunks rather than one buffer per stream
//!
//! A `Vec` would reallocate as it grew, moving bytes ngtcp2 still holds pointers to. Each
//! accepted write therefore becomes its own `Box<[u8]>`, whose address is fixed for as long
//! as it lives, and chunks are dropped whole once fully acknowledged.
//!
//! # The cost, stated plainly
//!
//! One copy of every byte sent, held until acknowledged. `ngnet-h3` avoids the equivalent
//! copy by making its callers hand over ownership; this crate takes the copy instead,
//! because a `&[u8]` that the caller may reuse immediately is the more ordinary signature
//! and the safety of the API should not depend on reading a paragraph of documentation.
//! See `docs/quic/pending-work.md`.

use std::collections::BTreeMap;
use std::collections::VecDeque;

use crate::stream::StreamId;

/// One accepted write, held at a fixed address.
struct Chunk {
    /// The bytes, boxed so the address does not move.
    ///
    /// This is the *allocation*, which is what ngtcp2 holds a pointer into. It is never
    /// reallocated, moved, or shrunk for as long as the chunk lives — see [`Chunk::len`].
    data: Box<[u8]>,
    /// Offset in the stream of this chunk's first byte.
    start: u64,
    /// How many of `data`'s bytes ngtcp2 actually accepted.
    ///
    /// Separate from `data.len()` because ngtcp2 routinely takes less than it was offered —
    /// a packet fills, and the remainder is offered again as a *new* write. The obvious
    /// response, shrinking the allocation to the accepted prefix, is a use-after-free: the
    /// address ngtcp2 was given must stay valid until the acknowledgement arrives
    /// (`ngtcp2.h:5244-5248`), and reallocating changes it while ngtcp2 still holds the old
    /// one for retransmission. Nothing detects that on a lossless link, because nothing
    /// retransmits.
    ///
    /// So the tail beyond `len` is left allocated and unused until the chunk is released.
    /// That waste is bounded by one packet's worth per outstanding chunk, and it is the
    /// price of the address staying put.
    len: usize,
}

impl Chunk {
    /// Offset one past this chunk's last accepted byte.
    fn end(&self) -> u64 {
        self.start + self.len as u64
    }
}

/// Everything still held for one stream.
#[derive(Default)]
struct Stream {
    chunks: VecDeque<Chunk>,
    /// Offset the next accepted byte will have.
    next_offset: u64,
}

/// Per-connection retention of sent stream data.
#[derive(Default)]
pub(crate) struct Retained {
    streams: BTreeMap<i64, Stream>,
}

impl Retained {
    /// Copies `data` and returns a pointer and length to hand to ngtcp2.
    ///
    /// The returned pointer stays valid until [`Retained::acknowledge`] covers it or
    /// [`Retained::forget`] is called for the stream, which is exactly the contract ngtcp2
    /// states.
    ///
    /// Returns `None` for empty input; ngtcp2 treats a zero-length write specially and
    /// there is nothing to retain.
    /// Copies one or more ranges as a single contiguous chunk, and returns a pointer to it.
    ///
    /// The ranges are concatenated rather than handed to ngtcp2 as a vector array. That
    /// sounds like it gives up something, and it does not: the copy has to happen regardless,
    /// because ngtcp2 keeps the pointer and the caller's buffers are borrowed only for the
    /// call. Copying into one chunk costs exactly what copying into several would, and
    /// leaves a single address and length to account for on acknowledgement instead of a set
    /// that ngtcp2 may accept a prefix of.
    pub(crate) fn stage_many(
        &mut self,
        stream: StreamId,
        ranges: &[&[u8]],
    ) -> Option<(*const u8, usize)> {
        let total: usize = ranges.iter().map(|r| r.len()).sum();
        if total == 0 {
            return None;
        }
        let mut buffer = Vec::with_capacity(total);
        for range in ranges {
            buffer.extend_from_slice(range);
        }
        let entry = self.streams.entry(stream.get()).or_default();
        let chunk = Chunk {
            data: buffer.into_boxed_slice(),
            start: entry.next_offset,
            // Provisional: the whole staged buffer is on offer. `commit` records how much
            // ngtcp2 took, and is always called before the write path returns.
            len: total,
        };
        let ptr = chunk.data.as_ptr();
        let len = chunk.data.len();
        entry.chunks.push_back(chunk);
        Some((ptr, len))
    }

    /// Records how much of the staged chunk ngtcp2 accepted.
    ///
    /// Anything beyond `accepted` was never handed over, so it must not count towards the
    /// stream's offset — the caller will submit those bytes again, and they would otherwise
    /// be retained twice and numbered wrongly.
    ///
    /// The allocation is deliberately **not** shrunk to fit. ngtcp2 was given its address a
    /// moment ago and holds it until the accepted prefix is acknowledged, so moving those
    /// bytes to a smaller box would leave ngtcp2 pointing at freed memory and corrupt any
    /// retransmission. Only the recorded length changes.
    pub(crate) fn commit(&mut self, stream: StreamId, accepted: usize) {
        let Some(entry) = self.streams.get_mut(&stream.get()) else {
            return;
        };
        let Some(chunk) = entry.chunks.back_mut() else {
            return;
        };

        if accepted == 0 {
            // Nothing was taken, so ngtcp2 kept no pointer into this allocation and it can
            // go. This is the one case where dropping the chunk is safe.
            entry.chunks.pop_back();
            return;
        }
        debug_assert!(
            accepted <= chunk.data.len(),
            "ngtcp2 cannot accept more than it was offered"
        );
        chunk.len = accepted.min(chunk.data.len());
        entry.next_offset = chunk.end();
    }

    /// The pointer ngtcp2 was given for the most recent staged write, and how much of it
    /// was accepted.
    ///
    /// The pointer is the allocation's, which never moves; the length is the accepted
    /// prefix. Used by the tests to assert exactly that.
    #[cfg(test)]
    pub(crate) fn last_pointer(&self, stream: StreamId) -> Option<(*const u8, usize)> {
        let entry = self.streams.get(&stream.get())?;
        let chunk = entry.chunks.back()?;
        Some((chunk.data.as_ptr(), chunk.len))
    }

    /// Releases everything acknowledged up to `offset + len` on a stream.
    pub(crate) fn acknowledge(&mut self, stream: StreamId, offset: u64, len: u64) {
        let Some(entry) = self.streams.get_mut(&stream.get()) else {
            return;
        };
        let acked_to = offset.saturating_add(len);
        while let Some(front) = entry.chunks.front() {
            if front.end() <= acked_to {
                entry.chunks.pop_front();
            } else {
                break;
            }
        }
        if entry.chunks.is_empty() && entry.next_offset <= acked_to {
            self.streams.remove(&stream.get());
        }
    }

    /// Releases everything held for a stream, which a closed stream no longer needs.
    pub(crate) fn forget(&mut self, stream: StreamId) {
        self.streams.remove(&stream.get());
    }

    /// Bytes currently held, across every stream.
    ///
    /// Exposed so a caller can see whether acknowledgements are being processed: an
    /// application that never sees them will watch this grow without bound.
    pub(crate) fn bytes_held(&self) -> usize {
        self.streams
            .values()
            .flat_map(|s| s.chunks.iter())
            .map(|c| c.len)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(id: i64) -> StreamId {
        StreamId::new(id).unwrap()
    }

    #[test]
    fn staged_data_is_copied_not_borrowed() {
        // The whole point: the caller's buffer must be free to die immediately.
        let mut retained = Retained::default();
        let (ptr, len) = {
            let caller_buffer = vec![1u8, 2, 3, 4];
            let staged = retained.stage_many(sid(0), &[&caller_buffer]).unwrap();
            retained.commit(sid(0), 4);
            staged
            // `caller_buffer` is dropped here.
        };
        assert_eq!(len, 4);
        // SAFETY: the retained copy is alive; this is the pointer ngtcp2 would hold.
        let held = unsafe { core::slice::from_raw_parts(ptr, len) };
        assert_eq!(held, &[1, 2, 3, 4]);
    }

    #[test]
    fn an_empty_write_stages_nothing() {
        let mut retained = Retained::default();
        assert!(retained.stage_many(sid(0), &[&[]]).is_none());
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn a_partial_acceptance_retains_only_what_was_accepted() {
        // Bytes ngtcp2 did not take will be offered again; retaining them here would both
        // waste memory and number the stream wrongly.
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[1, 2, 3, 4, 5, 6, 7, 8]]);
        retained.commit(sid(0), 3);
        assert_eq!(retained.bytes_held(), 3);

        let (ptr, len) = retained.last_pointer(sid(0)).unwrap();
        // SAFETY: the chunk is alive.
        let held = unsafe { core::slice::from_raw_parts(ptr, len) };
        assert_eq!(held, &[1, 2, 3]);
    }

    #[test]
    fn a_partial_acceptance_leaves_the_address_ngtcp2_was_given_alone() {
        // The one ngtcp2 actually cares about. It keeps the pointer it was handed until the
        // accepted bytes are acknowledged, so shrinking the allocation to fit the accepted
        // prefix -- which is the obvious tidy-up, and what this code used to do -- leaves
        // ngtcp2 reading freed memory the moment it retransmits.
        //
        // Nothing catches that on a lossless loopback, because nothing retransmits. It
        // needs a test that looks at the address rather than at the bytes.
        let mut retained = Retained::default();
        let (staged, offered) = retained.stage_many(sid(0), &[&[7u8; 1200]]).unwrap();
        assert_eq!(offered, 1200);

        // A packet fills before the offer is exhausted -- the ordinary case, not an edge
        // one, since the endpoint offers up to a full datagram and framing overhead takes
        // its share.
        retained.commit(sid(0), 1100);

        let (held, len) = retained.last_pointer(sid(0)).unwrap();
        assert_eq!(len, 1100, "only the accepted prefix counts as retained");
        assert_eq!(
            staged, held,
            "the allocation ngtcp2 points into must not move"
        );
        assert_eq!(retained.bytes_held(), 1100);
    }

    #[test]
    fn a_partially_accepted_chunk_still_reads_back_correctly_after_more_writes() {
        // The tail beyond the accepted prefix stays allocated. That must not leak into the
        // stream's offsets: the next chunk has to start where the accepted prefix ended,
        // not where the allocation ended.
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[1, 2, 3, 4, 5, 6, 7, 8]]);
        retained.commit(sid(0), 3);
        retained.stage_many(sid(0), &[&[4, 5, 6]]);
        retained.commit(sid(0), 3);
        assert_eq!(retained.bytes_held(), 6);

        // Acknowledging the first three must release exactly the first chunk.
        retained.acknowledge(sid(0), 0, 3);
        assert_eq!(retained.bytes_held(), 3);
        let (ptr, len) = retained.last_pointer(sid(0)).unwrap();
        // SAFETY: the chunk is alive.
        let held = unsafe { core::slice::from_raw_parts(ptr, len) };
        assert_eq!(held, &[4, 5, 6], "offsets must follow the accepted length");
    }

    #[test]
    fn a_rejected_write_retains_nothing() {
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[1, 2, 3]]);
        retained.commit(sid(0), 0);
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn offsets_advance_across_writes() {
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[0; 10]]);
        retained.commit(sid(0), 10);
        retained.stage_many(sid(0), &[&[0; 5]]);
        retained.commit(sid(0), 5);
        assert_eq!(retained.bytes_held(), 15);

        // Acknowledging the first ten releases exactly the first chunk.
        retained.acknowledge(sid(0), 0, 10);
        assert_eq!(retained.bytes_held(), 5);
    }

    #[test]
    fn a_partial_acknowledgement_releases_nothing_it_should_not() {
        // A chunk is released only when every one of its bytes is acknowledged; releasing
        // early would be the same use-after-free this module exists to prevent.
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[0; 10]]);
        retained.commit(sid(0), 10);
        retained.acknowledge(sid(0), 0, 4);
        assert_eq!(retained.bytes_held(), 10);
        retained.acknowledge(sid(0), 0, 10);
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn forgetting_a_stream_releases_everything_it_held() {
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[0; 32]]);
        retained.commit(sid(0), 32);
        retained.forget(sid(0));
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn streams_are_accounted_separately() {
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[0; 4]]);
        retained.commit(sid(0), 4);
        retained.stage_many(sid(4), &[&[0; 6]]);
        retained.commit(sid(4), 6);
        assert_eq!(retained.bytes_held(), 10);

        retained.acknowledge(sid(0), 0, 4);
        assert_eq!(retained.bytes_held(), 6);
    }

    #[test]
    fn acknowledging_an_unknown_stream_is_harmless() {
        let mut retained = Retained::default();
        retained.acknowledge(sid(99), 0, 100);
        retained.forget(sid(99));
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn chunk_addresses_do_not_move_when_another_chunk_is_added() {
        // A `Vec` would reallocate and move bytes ngtcp2 still points at. Boxed chunks are
        // what makes that impossible.
        let mut retained = Retained::default();
        retained.stage_many(sid(0), &[&[1, 2, 3]]);
        retained.commit(sid(0), 3);
        let (first, _) = retained.last_pointer(sid(0)).unwrap();

        for _ in 0..64 {
            retained.stage_many(sid(0), &[&[9; 16]]);
            retained.commit(sid(0), 16);
        }

        // The first chunk is still where it was.
        let entry = retained.streams.get(&0).unwrap();
        assert_eq!(entry.chunks.front().unwrap().data.as_ptr(), first);
    }
}
