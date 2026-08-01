//! Per-session state the trampolines reach through the bridge.
//!
//! These types are reached from `extern "C"` callbacks by way of disjoint mutable
//! borrows of individual [`crate::session::Session`] fields, never through the session
//! itself. They are introduced here, in the phase that establishes the bridge, so that
//! later phases add capability without widening the aliasing surface.

use std::collections::{HashMap, HashSet};

use crate::stream::StreamId;

/// Outgoing message bodies, keyed by the stream they belong to.
///
/// Entries are boxed so that adding a stream never moves an existing one: libnghttp2
/// holds the address of each entry in its data-source union, and that pointer must stay
/// valid for the life of the stream.
#[derive(Debug, Default)]
pub(crate) struct BodyRegistry {
    #[expect(dead_code, reason = "populated in the phase that adds message bodies")]
    entries: HashMap<StreamId, Box<BodyEntry>>,
}

/// One outgoing body, at a stable address.
#[derive(Debug, Default)]
pub(crate) struct BodyEntry {}

/// Errors reported by a caller's body source, held until the stream closes.
///
/// A body source signals failure by returning, which libnghttp2 turns into a stream
/// reset; the error itself has nowhere to go at that moment. Parking it here lets the
/// stream-close handler hand it back to the caller.
#[derive(Debug, Default)]
pub(crate) struct PendingErrors {
    #[expect(dead_code, reason = "populated in the phase that adds message bodies")]
    by_stream: HashMap<StreamId, ()>,
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
