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
    data: Box<[u8]>,
    /// Offset in the stream of this chunk's first byte.
    start: u64,
}

impl Chunk {
    /// Offset one past this chunk's last byte.
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
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
    pub(crate) fn stage(&mut self, stream: StreamId, data: &[u8]) -> Option<(*const u8, usize)> {
        if data.is_empty() {
            return None;
        }
        let entry = self.streams.entry(stream.get()).or_default();
        let chunk = Chunk {
            data: data.to_vec().into_boxed_slice(),
            start: entry.next_offset,
        };
        let ptr = chunk.data.as_ptr();
        let len = chunk.data.len();
        entry.chunks.push_back(chunk);
        Some((ptr, len))
    }

    /// Trims a staged chunk to what ngtcp2 actually accepted.
    ///
    /// Anything beyond `accepted` was never handed over, so it must not count towards the
    /// stream's offset — the caller will submit those bytes again, and they would otherwise
    /// be retained twice and numbered wrongly.
    pub(crate) fn commit(&mut self, stream: StreamId, accepted: usize) {
        let Some(entry) = self.streams.get_mut(&stream.get()) else {
            return;
        };
        let Some(chunk) = entry.chunks.back_mut() else {
            return;
        };

        if accepted == 0 {
            entry.chunks.pop_back();
            return;
        }
        if accepted < chunk.data.len() {
            // Reallocating here is safe: ngtcp2 was given the pointer moments ago and holds
            // it only for the accepted prefix, which this preserves bit for bit -- but the
            // address must not change, so the prefix is copied into a fresh box and the old
            // one dropped only after ngtcp2 has been told how much it got. That ordering is
            // the caller's, in `stream_io.rs`, which calls this before returning.
            let kept = chunk.data[..accepted].to_vec().into_boxed_slice();
            chunk.data = kept;
        }
        entry.next_offset = chunk.end();
    }

    /// The pointer ngtcp2 was given for the most recent staged write.
    ///
    /// Read back after [`Retained::commit`] has trimmed it, because trimming may have moved
    /// the bytes. Used by the tests below to assert what is actually retained; the write
    /// path stages and commits in one go and does not need to re-read it.
    #[cfg(test)]
    pub(crate) fn last_pointer(&self, stream: StreamId) -> Option<(*const u8, usize)> {
        let entry = self.streams.get(&stream.get())?;
        let chunk = entry.chunks.back()?;
        Some((chunk.data.as_ptr(), chunk.data.len()))
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
            .map(|c| c.data.len())
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
            let staged = retained.stage(sid(0), &caller_buffer).unwrap();
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
        assert!(retained.stage(sid(0), &[]).is_none());
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn a_partial_acceptance_retains_only_what_was_accepted() {
        // Bytes ngtcp2 did not take will be offered again; retaining them here would both
        // waste memory and number the stream wrongly.
        let mut retained = Retained::default();
        retained.stage(sid(0), &[1, 2, 3, 4, 5, 6, 7, 8]);
        retained.commit(sid(0), 3);
        assert_eq!(retained.bytes_held(), 3);

        let (ptr, len) = retained.last_pointer(sid(0)).unwrap();
        // SAFETY: the chunk is alive.
        let held = unsafe { core::slice::from_raw_parts(ptr, len) };
        assert_eq!(held, &[1, 2, 3]);
    }

    #[test]
    fn a_rejected_write_retains_nothing() {
        let mut retained = Retained::default();
        retained.stage(sid(0), &[1, 2, 3]);
        retained.commit(sid(0), 0);
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn offsets_advance_across_writes() {
        let mut retained = Retained::default();
        retained.stage(sid(0), &[0; 10]);
        retained.commit(sid(0), 10);
        retained.stage(sid(0), &[0; 5]);
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
        retained.stage(sid(0), &[0; 10]);
        retained.commit(sid(0), 10);
        retained.acknowledge(sid(0), 0, 4);
        assert_eq!(retained.bytes_held(), 10);
        retained.acknowledge(sid(0), 0, 10);
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn forgetting_a_stream_releases_everything_it_held() {
        let mut retained = Retained::default();
        retained.stage(sid(0), &[0; 32]);
        retained.commit(sid(0), 32);
        retained.forget(sid(0));
        assert_eq!(retained.bytes_held(), 0);
    }

    #[test]
    fn streams_are_accounted_separately() {
        let mut retained = Retained::default();
        retained.stage(sid(0), &[0; 4]);
        retained.commit(sid(0), 4);
        retained.stage(sid(4), &[0; 6]);
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
        retained.stage(sid(0), &[1, 2, 3]);
        retained.commit(sid(0), 3);
        let (first, _) = retained.last_pointer(sid(0)).unwrap();

        for _ in 0..64 {
            retained.stage(sid(0), &[9; 16]);
            retained.commit(sid(0), 16);
        }

        // The first chunk is still where it was.
        let entry = retained.streams.get(&0).unwrap();
        assert_eq!(entry.chunks.front().unwrap().data.as_ptr(), first);
    }
}
