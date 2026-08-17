//! The bytes a delivery carries, and why they are not a `Vec`.
//!
//! # What this is
//!
//! [`StreamBytes`] is what [`Event::StreamData`](super::Event::StreamData) carries. It is a
//! slice of bytes that owns its storage, and the storage is one of two things: an allocation of
//! its own, or a reference-counted share of the connection's read buffer. Which of the two it
//! is, is not observable except through [`StreamBytes::try_into_vec`], and nothing about the
//! bytes differs between them.
//!
//! # Why it exists
//!
//! Because the delivery used to be a `Vec<u8>` filled by copying, once per delivery, out of the
//! record dwnx was parsing. dwnx's data callback hands out a slice of the buffer *it was given*
//! (`deps/dwnx/lib/dwnx_conn.c:1631-1636`), which is the connection's own read buffer, so the
//! bytes were already in memory this crate owns and the copy moved them from one piece of that
//! memory to another. What made the copy necessary was lifetime rather than ownership: the
//! borrow is valid only for the duration of the callback, and a caller polled later cannot hold
//! it.
//!
//! A reference count answers that where a borrow cannot. The read buffer is held in an
//! [`Arc`], the delivery holds a clone of it and a range, and the connection reads into a
//! buffer again only once every delivery cut from it has been dropped -- which the strong count
//! reaching one is exactly what says.
//!
//! The rejected alternative is `bytes::Bytes`, which is what the HTTP/2 stack in this workspace
//! uses for the same job (`crates/ngnet-h2/src/http/driver.rs`). It is a better type than this
//! one and it is not available: `ngnet-qmux` declares exactly one non-optional dependency and no
//! dev-dependencies, and `crates/ngnet-qmux/tests/invariants.rs` fails if either changes. So the
//! shape is borrowed and the type is not. What is lost by not having `Bytes` is a cheap
//! `split_to`, an inline representation for short values, and a static constructor; none of the
//! three is on this path.
//!
//! # The copy that is kept, and the bound it buys
//!
//! A delivery shorter than [`ALIAS_THRESHOLD`] is copied into an allocation of its own rather
//! than aliased, and that is not a performance tuning knob -- it is what makes the pinning bound
//! true. A view holds its whole buffer alive, not its own range of it, so without a threshold a
//! caller holding one byte would keep a whole read buffer -- 16382 bytes, one maximum-size
//! record -- from being reused: an amplification of thousands, and a bound stated per connection
//! would not be a bound at all.
//!
//! With it, a delivery that is aliased carries at least [`ALIAS_THRESHOLD`] bytes and pins at
//! most one read buffer, so **the memory a held delivery can pin is at most sixteen times the
//! bytes it carries**. Deliveries cut from the same buffer share it rather than each pinning one
//! of their own, so that factor is a per-delivery worst case and not a sum.
//!
//! The bias in choosing 1 KiB is stated rather than left to be inferred: it is set low, toward
//! aliasing more deliveries rather than fewer, because the payload deliveries this layer exists
//! to carry are whole records of up to 16382 bytes and the small ones are control traffic that
//! was cheap to copy anyway. A higher threshold would tighten the bound and start copying real
//! payload; a lower one would loosen it for no gain.

use std::sync::Arc;

/// The shortest delivery that is handed out as a view rather than copied.
///
/// See the module documentation: this constant and the read buffer's size together are the
/// pinning bound, and neither means anything without the other. `conn::READ_BUFFER` is the
/// other half, and `the_pinning_factor_is_what_the_documentation_says_it_is` asserts the
/// arithmetic between them rather than leaving it to the prose.
pub const ALIAS_THRESHOLD: usize = 1024;

/// The bytes of one delivery.
///
/// Behaves as a `[u8]` through [`Deref`](core::ops::Deref), compares equal to slices and
/// vectors, and clones by bumping a reference count rather than by copying. See the
/// [module documentation](self) for what it is a view of and why it is not a `Vec<u8>`.
///
/// # Example
///
/// ```
/// use ngnet_qmux::io::StreamBytes;
///
/// let bytes = StreamBytes::from(b"a delivery".to_vec());
/// assert_eq!(&bytes[..], b"a delivery");
/// assert_eq!(bytes.len(), 10);
///
/// // A clone shares the storage rather than copying it.
/// let second = bytes.clone();
/// assert_eq!(second, bytes);
/// ```
#[derive(Clone, Default)]
pub struct StreamBytes {
    inner: Inner,
}

/// Where a delivery's bytes actually live.
///
/// Private, and deliberately so: a caller that could tell the two apart would be reading an
/// implementation detail, and the one operation for which the difference matters --
/// [`StreamBytes::try_into_vec`] -- is spelled as a question rather than as an inspection.
#[derive(Clone)]
enum Inner {
    /// An allocation of this delivery's own: a copied-out short delivery, or one whose bytes
    /// did not come from the read buffer at all.
    Owned(Vec<u8>),
    /// A range of a read buffer that this delivery keeps alive.
    Aliased {
        buffer: Arc<Vec<u8>>,
        start: usize,
        end: usize,
    },
}

impl Default for Inner {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl StreamBytes {
    /// An empty delivery, which allocates nothing.
    ///
    /// A peer that has already sent everything ends a stream with a zero-length STREAM frame
    /// carrying the fin bit, so this is not a degenerate case: it is how a stream ends.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: Inner::Owned(Vec::new()),
        }
    }

    /// A delivery of its own copy of `data`.
    pub(crate) fn copied(data: &[u8]) -> Self {
        Self {
            inner: Inner::Owned(data.to_vec()),
        }
    }

    /// A delivery aliasing `buffer[start..start + len]`.
    ///
    /// The range is checked rather than trusted, and a range that does not fit is refused with
    /// [`None`] rather than clamped: the only caller computes it from two addresses, and a
    /// clamp would turn an arithmetic mistake into a delivery of the wrong bytes.
    pub(crate) fn aliased(buffer: Arc<Vec<u8>>, start: usize, len: usize) -> Option<Self> {
        let end = start.checked_add(len)?;
        if end > buffer.len() {
            return None;
        }
        Some(Self {
            inner: Inner::Aliased { buffer, start, end },
        })
    }

    /// The bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            Inner::Owned(owned) => owned,
            Inner::Aliased { buffer, start, end } => &buffer[*start..*end],
        }
    }

    /// How many bytes this delivery carries.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.inner {
            Inner::Owned(owned) => owned.len(),
            Inner::Aliased { start, end, .. } => end - start,
        }
    }

    /// Whether it carries none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The allocation behind this delivery, if it has one of its own.
    ///
    /// `Ok` hands over the `Vec` by move; `Err` gives the delivery back unchanged, because it
    /// is a view of a buffer shared with others and there is no allocation to hand over.
    ///
    /// This exists for one caller: a layer above that wants an owned byte container of its own
    /// kind and must not copy to get one. `ngnet-qmux-h3` takes the allocation when there is one
    /// and wraps the view when there is not, so the delivery crosses that boundary as a move or
    /// a reference-count bump either way. Asking instead of copying is the whole point; a
    /// method that always produced a `Vec` would silently reintroduce the copy this type
    /// exists to remove.
    ///
    /// # Errors
    ///
    /// The delivery itself, when its bytes are a view of a shared read buffer.
    pub fn try_into_vec(self) -> Result<Vec<u8>, Self> {
        match self.inner {
            Inner::Owned(owned) => Ok(owned),
            inner @ Inner::Aliased { .. } => Err(Self { inner }),
        }
    }

    /// Whether these bytes are a view of a shared buffer rather than an allocation of their own.
    ///
    /// For tests and for the pinning bound's assertions. A caller has no reason to ask: the
    /// bytes are the same either way, which is the property the whole type rests on.
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn is_aliased(&self) -> bool {
        matches!(self.inner, Inner::Aliased { .. })
    }
}

impl core::ops::Deref for StreamBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for StreamBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Printed as the bytes rather than as the representation, so a failing assertion shows what
/// arrived rather than which of the two storage forms it happened to take.
impl core::fmt::Debug for StreamBytes {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl PartialEq for StreamBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for StreamBytes {}

impl PartialEq<[u8]> for StreamBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for StreamBytes {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for StreamBytes {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for StreamBytes {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<Vec<u8>> for StreamBytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<StreamBytes> for Vec<u8> {
    fn eq(&self, other: &StreamBytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

/// Taking an allocation over, which is what a caller assembling a delivery by hand has.
impl From<Vec<u8>> for StreamBytes {
    fn from(owned: Vec<u8>) -> Self {
        Self {
            inner: Inner::Owned(owned),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer() -> Arc<Vec<u8>> {
        Arc::new((0..64u8).collect())
    }

    #[test]
    fn a_view_reads_the_range_it_was_given() {
        let shared = buffer();
        let view = StreamBytes::aliased(Arc::clone(&shared), 8, 16).expect("a range that fits");
        assert_eq!(view.len(), 16);
        assert_eq!(&view[..], &shared[8..24]);
        assert_eq!(view.as_ref(), &shared[8..24]);
    }

    #[test]
    fn a_range_that_does_not_fit_is_refused_rather_than_clamped() {
        let shared = buffer();
        assert!(StreamBytes::aliased(Arc::clone(&shared), 60, 8).is_none());
        assert!(StreamBytes::aliased(Arc::clone(&shared), 0, usize::MAX).is_none());
        assert!(StreamBytes::aliased(Arc::clone(&shared), usize::MAX, 1).is_none());
        // The empty range at the very end fits, and is not the same thing as overrunning.
        assert!(StreamBytes::aliased(shared, 64, 0).is_some());
    }

    /// The reclamation gate: the buffer is reusable exactly when no view is left.
    #[test]
    fn a_view_keeps_its_buffer_alive_and_a_dropped_one_releases_it() {
        let mut shared = buffer();
        let view = StreamBytes::aliased(Arc::clone(&shared), 0, 32).expect("a range that fits");
        assert!(
            Arc::get_mut(&mut shared).is_none(),
            "a live view must make the buffer unusable for a further read"
        );

        let cloned = view.clone();
        drop(view);
        assert!(
            Arc::get_mut(&mut shared).is_none(),
            "a clone shares the buffer rather than copying it, so it holds it too"
        );

        drop(cloned);
        assert!(
            Arc::get_mut(&mut shared).is_some(),
            "the last view going away is what makes the buffer free"
        );
    }

    #[test]
    fn an_owned_delivery_hands_its_allocation_over_and_a_view_does_not() {
        let owned = StreamBytes::copied(b"a copied delivery");
        let taken = owned.try_into_vec().expect("an allocation of its own");
        assert_eq!(taken, b"a copied delivery");

        let shared = buffer();
        let view = StreamBytes::aliased(shared, 0, 8).expect("a range that fits");
        let returned = view.try_into_vec().expect_err("a view owns no allocation");
        assert_eq!(returned.len(), 8, "and it comes back unchanged");
    }

    #[test]
    fn an_empty_delivery_is_empty_whichever_form_it_takes() {
        assert!(StreamBytes::new().is_empty());
        assert!(StreamBytes::default().is_empty());
        assert!(StreamBytes::copied(&[]).is_empty());
        assert!(
            StreamBytes::aliased(buffer(), 4, 0)
                .expect("an empty range")
                .is_empty()
        );
    }

    /// The bound this type is required to satisfy, stated as arithmetic rather than as prose.
    #[test]
    fn the_pinning_factor_is_what_the_documentation_says_it_is() {
        let buffer = super::super::conn::READ_BUFFER;
        assert!(
            buffer.div_ceil(ALIAS_THRESHOLD) <= 16,
            "a held delivery may pin at most sixteen times the bytes it carries; a read buffer \
             of {buffer} against a threshold of {ALIAS_THRESHOLD} makes that factor {}",
            buffer.div_ceil(ALIAS_THRESHOLD)
        );
    }

    /// The bound the layer above needs: a delivery may be moved between threads.
    #[test]
    fn a_delivery_is_sendable_and_shareable() {
        fn require<T: Send + Sync + 'static>(_: &T) {}
        require(&StreamBytes::new());
    }
}
