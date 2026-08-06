//! Caller-supplied handlers.
//!
//! Handlers are registered once, on the builder, but the application state they mutate is
//! supplied at call time rather than captured. That is what lets a caller keep ownership
//! of its own state and still have it mutated from inside an FFI callback, without any
//! interior mutability or cloning.
//!
//! Phase 2 wires only the handlers a connection needs to report progress; the header,
//! body and event handlers arrive with the phases that deliver those features.

/// A handler invoked with the caller's own state, a stream, and a byte count.
///
/// The `Send` bound is not decoration. [`Conn`] declares itself `Send`, and these boxes
/// are the only thing it owns that could carry a non-`Send` capture — the state type `C`
/// is never stored, only borrowed at call time. Without the bound, safe code could move an
/// `Rc` across threads by capturing it in a handler, which races a non-atomic refcount.
///
/// [`Conn`]: crate::Conn
type ByteCountHandler<C> = Box<dyn FnMut(&mut C, crate::StreamId, u64) + Send>;

/// The set of handlers a connection may call.
///
/// Generic over the caller's state type `C`, which every handler receives by mutable
/// reference.
pub(crate) struct Handlers<C> {
    /// Invoked when nghttp3 has consumed stream data that was previously blocked, and the
    /// caller may extend that much more QUIC flow-control credit.
    pub(crate) deferred_consume: Option<ByteCountHandler<C>>,
}

// Hand-written rather than derived: `#[derive(Default)]` would require `C: Default`, which
// has nothing to do with whether a handler set is empty.
impl<C> Default for Handlers<C> {
    fn default() -> Self {
        Self {
            deferred_consume: None,
        }
    }
}

impl<C> core::fmt::Debug for Handlers<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Handlers")
            .field("deferred_consume", &self.deferred_consume.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A state type that deliberately does not implement `Default`, pinning the reason the
    /// `Default` impl above is written out by hand.
    struct NotDefault(#[allow(dead_code)] u8);

    #[test]
    fn handlers_default_without_the_state_type_doing_so() {
        let handlers = Handlers::<NotDefault>::default();
        assert!(handlers.deferred_consume.is_none());
    }
}
