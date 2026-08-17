//! Outgoing message bodies.
//!
//! # Why there is no copying variant
//!
//! nghttp2 offers a body source that writes into a buffer the library owns. nghttp3 has no
//! such thing: its data callback takes vectors that point at the *application's* memory,
//! and the contract is that the application keeps those bytes alive until nghttp3 reports
//! them acknowledged. Zero-copy is therefore not an optimisation to opt into here, it is
//! the only shape available, and it is why [`RetainedBytes`] exists — a source can hand
//! out the same allocation across several calls, so the buffers must be shareable rather
//! than owned by one vector each.
//!
//! # The release contract
//!
//! Buffers handed over are held until [`Conn::add_ack_offset`] says the peer acknowledged
//! them, and are released then, on stream close, or when the connection is dropped —
//! nothing else releases them. In particular, reporting bytes *written* does not: nghttp3
//! reaches its release accounting only from the acknowledgement entry point.
//!
//! [`Conn::add_ack_offset`]: crate::Conn::add_ack_offset

use std::sync::Arc;

/// A reference-counted byte buffer handed to the connection.
///
/// Shareable because one allocation can span several of the vectors nghttp3 asks for, and
/// across several calls: the buffer is released when its last byte is acknowledged, which
/// may be long after the call that offered it.
///
/// This is deliberately not [`bytes::Bytes`]. That would be the obvious choice, but this
/// crate declares exactly one non-optional dependency and a test enforces it, so the
/// handle is defined here instead. [`RetainedBytes::from_owner`] is how a caller who
/// *does* have a `Bytes` — or any other reference-counted buffer — hands it over without
/// a copy.
///
/// [`bytes::Bytes`]: https://docs.rs/bytes
#[derive(Clone)]
pub struct RetainedBytes {
    store: Store,
    start: usize,
    end: usize,
}

/// Where the bytes actually live.
///
/// Two cases rather than one because the crate has no `bytes` dependency to name in its
/// own types, but a caller who has one should not have to copy to use this crate. The
/// erased case costs a second pointer indirection on every read; the owned case is what
/// the crate's own [`FixedBody`] uses and stays flat.
#[derive(Clone)]
enum Store {
    Owned(Arc<[u8]>),
    Erased(Arc<dyn Owner>),
}

/// A buffer whose bytes stay put for as long as it is alive.
///
/// Sealed by being private: the blanket implementation below covers every type that can
/// satisfy it, so there is nothing for a caller to implement.
trait Owner: Send + Sync {
    fn bytes(&self) -> &[u8];
}

impl<T: AsRef<[u8]> + Send + Sync + 'static> Owner for T {
    fn bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl Store {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Owned(buffer) => buffer,
            Self::Erased(owner) => owner.bytes(),
        }
    }
}

impl RetainedBytes {
    /// Takes ownership of a buffer.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        let buffer: Arc<[u8]> = bytes.into();
        let end = buffer.len();
        Self {
            store: Store::Owned(buffer),
            start: 0,
            end,
        }
    }

    /// Retains a buffer this crate cannot name, without copying it.
    ///
    /// The motivating case is [`bytes::Bytes`]: it is reference-counted already, so
    /// copying it into an `Arc<[u8]>` to satisfy [`new`](Self::new) would defeat the whole
    /// point of a zero-copy body path. Anything that can lend a stable slice works —
    /// `Vec<u8>`, `Arc<Vec<u8>>`, a memory map, a caller's own buffer type.
    ///
    /// # What the owner must guarantee
    ///
    /// [`AsRef::as_ref`] should return the same bytes every time it is called. That is true
    /// of every sane implementation and of every type in the standard library, but it is
    /// not something the type system promises, and nghttp3 will hold the address across
    /// many calls.
    ///
    /// An owner that misbehaves cannot cause unsoundness, and it takes two separate
    /// measures to say so. Every read here is clamped to the length taken at construction,
    /// so no access can run past the buffer. And the release accounting stores the length
    /// nghttp3 was *told about* at the moment the pointer was handed over, rather than
    /// measuring the buffer again when acknowledgement arrives — because those two numbers
    /// disagreeing is exactly how a buffer gets freed while nghttp3 is still reading
    /// through it. The clamp alone is not enough; both are needed.
    ///
    /// `Send + Sync` is required rather than merely `Send` because the buffer is stored
    /// behind an [`Arc`], and `Arc<T>` is only `Send` when `T` is both.
    ///
    /// [`bytes::Bytes`]: https://docs.rs/bytes
    pub fn from_owner(owner: impl AsRef<[u8]> + Send + Sync + 'static) -> Self {
        let owner: Arc<dyn Owner> = Arc::new(owner);
        // Fixed once, here. Re-reading the length on every access would let a misbehaving
        // owner change the size of a buffer nghttp3 is already pointing at.
        let end = owner.bytes().len();
        Self {
            store: Store::Erased(owner),
            start: 0,
            end,
        }
    }

    /// The bytes this handle refers to.
    pub fn as_slice(&self) -> &[u8] {
        // Clamped rather than indexed directly. The bounds cannot be wrong for an owner
        // that behaves, and for one that does not this is the difference between fewer
        // bytes than expected and a panic — which, reached from a body source, would
        // unwind into a C frame and abort the process.
        let all = self.store.bytes();
        let end = self.end.min(all.len());
        let start = self.start.min(end);
        &all[start..end]
    }

    /// How many bytes this handle refers to.
    pub fn len(&self) -> usize {
        // Derived from the same place as `as_slice`, so the two cannot disagree about a
        // buffer nghttp3 has been handed.
        self.as_slice().len()
    }

    /// Whether this handle refers to no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Splits off the first `n` bytes, leaving the remainder here.
    ///
    /// Both halves share one allocation, which is the point: a source can offer a large
    /// buffer in pieces without copying it or losing track of when it may be released.
    pub fn split_to(&mut self, n: usize) -> Self {
        let n = n.min(self.len());
        let head = Self {
            store: self.store.clone(),
            start: self.start,
            end: self.start + n,
        };
        self.start += n;
        head
    }
}

impl core::fmt::Debug for RetainedBytes {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RetainedBytes")
            .field("len", &self.len())
            .finish()
    }
}

impl From<Vec<u8>> for RetainedBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&[u8]> for RetainedBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }
}

/// What a body source has for the connection.
///
/// Open to extension. nghttp3 has no per-stream failure code today — its own header carries
/// a TODO for one — so if it gains one this will gain a variant, and a caller who matched
/// exhaustively would stop compiling for a change that took nothing away from them.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BodyOutcome {
    /// Here are some bytes; there may be more later.
    Wrote(Vec<RetainedBytes>),
    /// Here are the last bytes, and the stream ends after them.
    Eof(Vec<RetainedBytes>),
    /// Here are the last bytes, but a trailing field section follows.
    EofWithTrailers(Vec<RetainedBytes>),
    /// Nothing is available yet.
    ///
    /// The stream is deferred until [`Conn::resume_stream`] is called. Returning no bytes
    /// without deferring is not expressible, because nghttp3 would take it as a
    /// zero-length frame rather than as "ask me again".
    ///
    /// It says a second thing as well, and the second is the more consequential of the
    /// two: it is also how a source abandons one stream. "Ask me again" and "never ask me
    /// again" produce the same bytes on the wire — which is to say none, and no
    /// end-of-stream marker — so the difference lies entirely in what happens next.
    /// Resuming the stream means the first; resetting it means the second. See
    /// [`Fail`](Self::Fail), where the whole mechanism is written down, and note the
    /// obligation it carries: a deferred stream that is neither resumed nor reset waits
    /// for bytes that will never come.
    ///
    /// [`Conn::resume_stream`]: crate::Conn::resume_stream
    Defer,
    /// The body cannot be produced, and the exchange must stop.
    ///
    /// **This fails the whole connection, not just the stream.** nghttp3 offers exactly
    /// one way for a data callback to report failure, and it is connection-fatal; the
    /// header carries a TODO for a per-stream variant that does not exist yet. The
    /// connection is poisoned and every retained buffer released.
    ///
    /// A source that wants to abandon one stream without taking the connection with it
    /// returns [`Defer`](Self::Defer) — never an `Eof` — and has the caller reset that
    /// stream through its QUIC layer, which is the only place a per-stream reset can come
    /// from. Deferring is what withholds the end-of-stream marker, and withholding it is
    /// the point: a marker and a reset are two statements about one stream that
    /// contradict each other, and a peer with nothing queued behind the marker has
    /// already been told the message was complete by the time the reset arrives. It then
    /// rightly ignores the reset, and a truncated message looks like a whole one. The
    /// reset must be the only thing the peer is ever told about how the message ended.
    ///
    /// This is the route the asynchronous layer in `src/http/` takes when a caller's body
    /// reports an error, and `crates/ngnet-h3/tests/http_failed_body.rs` watches the
    /// result at the transport seam: for a stream whose body failed, zero end-of-stream
    /// markers and exactly one reset, carrying `H3_REQUEST_CANCELLED`.
    Fail,
}

/// Supplies the bytes of an outgoing message body.
///
/// `Send` because a connection is, and a source is stored inside one.
///
/// A source is never asked for more once it has reported an end, so `next` does not have
/// to be idempotent past that point.
pub trait BodySource: Send {
    /// Asks for the next piece of the body.
    ///
    /// Any number of buffers may be returned. nghttp3 takes at most eight per call, so a
    /// surplus is held back and offered to it on the following call rather than dropped.
    /// Empty buffers are discarded: nghttp3 skips zero-length vectors without queueing
    /// them, so one could never be reported acknowledged.
    ///
    /// Returning [`BodyOutcome::Wrote`] with nothing in it is treated as
    /// [`BodyOutcome::Defer`], because the alternative — telling nghttp3 there are zero
    /// bytes and the body has not ended — makes it write an empty data frame.
    fn next(&mut self) -> BodyOutcome;
}

/// A body that is already entirely in memory.
#[derive(Debug)]
pub struct FixedBody {
    remaining: Option<RetainedBytes>,
}

impl FixedBody {
    /// Wraps a complete body.
    pub fn new(bytes: impl Into<RetainedBytes>) -> Self {
        Self {
            remaining: Some(bytes.into()),
        }
    }
}

impl BodySource for FixedBody {
    fn next(&mut self) -> BodyOutcome {
        match self.remaining.take() {
            Some(bytes) if !bytes.is_empty() => BodyOutcome::Eof(vec![bytes]),
            _ => BodyOutcome::Eof(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitting_shares_one_allocation() {
        let mut bytes = RetainedBytes::new(b"hello world".to_vec());
        let head = bytes.split_to(5);
        assert_eq!(head.as_slice(), b"hello");
        assert_eq!(bytes.as_slice(), b" world");
        // Both halves refer into the same buffer, which is what lets release accounting
        // hold one handle per offered vector without duplicating the payload.
        assert!(std::ptr::eq(
            head.as_slice().as_ptr(),
            bytes.as_slice().as_ptr().wrapping_sub(5)
        ));
    }

    #[test]
    fn an_erased_owner_is_not_copied() {
        // The whole point of `from_owner`: the retained handle must point *into* the
        // caller's allocation, not at a duplicate of it. Compared by address, because
        // comparing by value would pass just as well for a copy.
        // Moving the `Vec` does not move its heap allocation, so the address taken before
        // the move is the one the handle must end up pointing at.
        let owner = b"hello world".to_vec();
        let address = owner.as_ptr();

        let bytes = RetainedBytes::from_owner(owner);
        assert_eq!(bytes.as_slice(), b"hello world");
        assert!(std::ptr::eq(bytes.as_slice().as_ptr(), address));
    }

    #[test]
    fn an_erased_owner_survives_splitting() {
        let owner = b"hello world".to_vec();
        let address = owner.as_ptr();

        let mut bytes = RetainedBytes::from_owner(owner);
        let head = bytes.split_to(5);

        assert_eq!(head.as_slice(), b"hello");
        assert_eq!(bytes.as_slice(), b" world");
        // Neither half copied: both still point into the original allocation.
        assert!(std::ptr::eq(head.as_slice().as_ptr(), address));
        assert!(std::ptr::eq(
            bytes.as_slice().as_ptr(),
            address.wrapping_add(5)
        ));
    }

    #[test]
    fn an_owner_that_shrinks_cannot_panic() {
        // `AsRef` is not required by the type system to be stable, and a panic here would
        // unwind into a C frame and abort. Clamping turns the worst case into fewer bytes
        // than expected, which is a protocol problem rather than a process-ending one.
        struct Shrinking(std::sync::atomic::AtomicUsize);
        impl AsRef<[u8]> for Shrinking {
            fn as_ref(&self) -> &[u8] {
                let seen = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if seen == 0 { b"abcdef" } else { b"ab" }
            }
        }

        let bytes = RetainedBytes::from_owner(Shrinking(std::sync::atomic::AtomicUsize::new(0)));
        // The length was fixed at construction from the first call; every read after that
        // sees a shorter slice and is clamped rather than indexed out of bounds.
        assert_eq!(bytes.as_slice(), b"ab");
        assert_eq!(bytes.len(), 2);
    }

    #[test]
    fn splitting_past_the_end_is_clamped() {
        let mut bytes = RetainedBytes::new(b"abc".to_vec());
        let head = bytes.split_to(99);
        assert_eq!(head.as_slice(), b"abc");
        assert!(bytes.is_empty());
    }

    #[test]
    fn a_fixed_body_yields_everything_then_ends() {
        let mut body = FixedBody::new(b"payload".to_vec());
        match body.next() {
            BodyOutcome::Eof(pieces) => {
                assert_eq!(pieces.len(), 1);
                assert_eq!(pieces[0].as_slice(), b"payload");
            }
            other => panic!("expected Eof, got {other:?}"),
        }
        // A second call yields an empty end rather than repeating the payload.
        match body.next() {
            BodyOutcome::Eof(pieces) => assert!(pieces.is_empty()),
            other => panic!("expected empty Eof, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_fixed_body_ends_immediately() {
        let mut body = FixedBody::new(Vec::new());
        match body.next() {
            BodyOutcome::Eof(pieces) => assert!(pieces.is_empty()),
            other => panic!("expected empty Eof, got {other:?}"),
        }
    }
}
