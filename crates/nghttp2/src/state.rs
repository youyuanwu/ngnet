//! Per-session state the trampolines reach through the bridge.
//!
//! These types are reached from `extern "C"` callbacks by way of disjoint mutable
//! borrows of individual [`crate::session::Session`] fields, never through the session
//! itself.

use core::ptr::NonNull;
use std::collections::{HashMap, HashSet};

use crate::body::{BodyError, BodySource};
use crate::stream::StreamId;

/// One outgoing body, at an address libnghttp2 holds for the life of its stream.
pub(crate) struct BodyEntry {
    pub(crate) source: Box<dyn BodySource>,
    /// Set once the source reports that trailers will follow.
    pub(crate) trailers_ready: bool,
}

impl BodyEntry {
    pub(crate) fn new(source: Box<dyn BodySource>) -> Self {
        Self {
            source,
            trailers_ready: false,
        }
    }
}

/// Outgoing message bodies, keyed by the stream they belong to.
///
/// Entries are owned as raw pointers rather than `Box`es. That is deliberate: libnghttp2
/// keeps the address of each entry in its data-source union and writes through it while a
/// mutable borrow of this registry is live. Holding the entries in `Box`es would assert a
/// uniqueness guarantee those writes violate, so ownership here is manual and released
/// explicitly.
#[derive(Default)]
pub(crate) struct BodyRegistry {
    entries: HashMap<StreamId, NonNull<BodyEntry>>,
}

// SAFETY: the registry owns its entries exclusively, and `BodyEntry` holds only a
// `Box<dyn BodySource>` where `BodySource: Send`. Moving the registry between threads
// therefore moves only data that is itself `Send`.
unsafe impl Send for BodyRegistry {}

impl BodyRegistry {
    /// Takes ownership of `entry`, returning the address libnghttp2 should hold.
    ///
    /// The entry is not yet associated with a stream: submission assigns the identifier,
    /// and only then can it be recorded with [`Self::attach`].
    pub(crate) fn prepare(entry: BodyEntry) -> NonNull<BodyEntry> {
        let raw = Box::into_raw(Box::new(entry));
        // SAFETY: `Box::into_raw` never yields null.
        unsafe { NonNull::new_unchecked(raw) }
    }

    /// Records a prepared entry against the stream it was assigned.
    pub(crate) fn attach(&mut self, stream: StreamId, entry: NonNull<BodyEntry>) {
        if let Some(previous) = self.entries.insert(stream, entry) {
            // Reaching here would mean one stream carried two bodies. Callers prevent it:
            // a request is always given a fresh identifier, and a response requires an
            // open stream that the duplicate guard has not already claimed.
            //
            // The previous entry is deliberately NOT freed. libnghttp2 may still hold its
            // address in a queued outbound item, so freeing it would be a use-after-free
            // waiting to happen, whereas leaking it merely wastes memory. Given the choice
            // between unsound and untidy, this takes untidy.
            debug_assert!(
                false,
                "two bodies attached to stream {stream}; the previous entry was leaked"
            );
            let _ = previous;
        }
    }

    /// Drops a prepared entry that was never attached, because submission failed.
    pub(crate) fn discard(entry: NonNull<BodyEntry>) {
        Self::release_raw(entry);
    }

    /// Drops the entry belonging to `stream`, if any.
    pub(crate) fn detach(&mut self, stream: StreamId) {
        if let Some(entry) = self.entries.remove(&stream) {
            Self::release_raw(entry);
        }
    }

    /// Whether this stream's body has announced that trailers may follow.
    pub(crate) fn trailers_ready(&self, stream: StreamId) -> bool {
        self.entries.get(&stream).is_some_and(|entry| {
            // SAFETY: the pointer came from `prepare` and is owned by this registry, so it
            // is live. No trampoline can be running while this shared borrow exists: both
            // require the session, and the caller holds it.
            unsafe { entry.as_ref() }.trailers_ready
        })
    }

    fn release_raw(entry: NonNull<BodyEntry>) {
        // SAFETY: every pointer in this registry came from `prepare`, which produced it
        // with `Box::into_raw`, and each is released exactly once.
        drop(unsafe { Box::from_raw(entry.as_ptr()) });
    }
}

impl Drop for BodyRegistry {
    fn drop(&mut self) {
        for (_, entry) in self.entries.drain() {
            Self::release_raw(entry);
        }
    }
}

impl core::fmt::Debug for BodyRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BodyRegistry")
            .field("streams", &self.entries.len())
            .finish()
    }
}

/// Errors reported by a caller's body source, held until the stream closes.
///
/// A body source signals failure by returning, which libnghttp2 turns into a stream
/// reset; the error itself has nowhere to go at that moment. Parking it here lets the
/// stream-close handler hand it back to the caller.
#[derive(Debug, Default)]
pub(crate) struct PendingErrors {
    by_stream: HashMap<StreamId, BodyError>,
}

impl PendingErrors {
    pub(crate) fn park(&mut self, stream: StreamId, error: BodyError) {
        self.by_stream.insert(stream, error);
    }

    pub(crate) fn take(&mut self, stream: StreamId) -> Option<BodyError> {
        self.by_stream.remove(&stream)
    }
}

/// Streams that already carry a response.
///
/// libnghttp2 documents submitting a second response for one stream as a programming
/// error that may crash, so it has to be caught before the call is made.
#[derive(Debug, Default)]
pub(crate) struct ResponseGuard {
    responded: HashSet<StreamId>,
}

impl ResponseGuard {
    /// Records that `stream` now carries a response.
    ///
    /// Returns `false` if one was already submitted, which the caller must treat as a
    /// rejection rather than forwarding to libnghttp2.
    pub(crate) fn claim(&mut self, stream: StreamId) -> bool {
        self.responded.insert(stream)
    }

    /// Forgets a stream, so a later stream reusing the identifier starts clean.
    pub(crate) fn release(&mut self, stream: StreamId) {
        self.responded.remove(&stream);
    }
}

/// Tracks whether the session is part-way through a frame.
///
/// libnghttp2 exposes no query for this, so it is synthesised from the two callbacks that
/// bracket a frame: one fires once a frame header has been parsed, the other once the
/// whole frame has. Between them the session holds an incomplete frame, and an
/// end-of-file there means the peer truncated it rather than closing cleanly.
///
/// The nine-octet frame header itself is not covered: nothing fires until it is complete,
/// so a connection cut mid-header is indistinguishable from a clean close. That limit is
/// inherent to the callbacks available and is documented on the public accessor.
#[derive(Debug, Default)]
pub(crate) struct FrameProgress {
    in_frame: bool,
}

impl FrameProgress {
    /// A frame header has been parsed; its body is now arriving.
    pub(crate) fn begin(&mut self) {
        self.in_frame = true;
    }

    /// A frame has been fully received.
    pub(crate) fn end(&mut self) {
        self.in_frame = false;
    }

    /// Whether a frame is currently incomplete.
    pub(crate) const fn in_frame(&self) -> bool {
        self.in_frame
    }
}
