//! Caller-supplied handlers.
//!
//! Handlers are registered once, on the builder, but the application state they mutate is
//! supplied at call time rather than captured. That is what lets a caller keep ownership
//! of its own state and still have it mutated from inside an FFI callback, without any
//! interior mutability or cloning.

use crate::error::ErrorCode;
use crate::stream::StreamId;

/// Whether a field section carries leading fields or trailing ones.
///
/// HTTP/3 delivers both through the same shape of callback, and a receiver that could not
/// tell them apart would accept a trailer as though it were a header.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum FieldSection {
    /// The leading field section: a request's or response's headers.
    Headers,
    /// The trailing field section, which follows the body.
    Trailers,
}

/// What a field handler wants to happen next.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum FieldAction {
    /// Keep delivering fields.
    Continue,
    /// Stop caring about this message.
    ///
    /// nghttp3 has no code meaning "abandon this field section", so this does not cancel
    /// anything by itself: remaining fields are still delivered, and a caller that wants
    /// the exchange to stop resets the stream through its QUIC layer. It exists so intent
    /// is expressible without a handler having to return a value it does not mean.
    Stop,
}

/// A handler invoked with the caller's own state, a stream, and a byte count.
///
/// The `Send` bound is not decoration. [`Conn`] declares itself `Send`, and these boxes
/// are the only thing it owns that could carry a non-`Send` capture — the state type `C`
/// is never stored, only borrowed at call time. Without the bound, safe code could move an
/// `Rc` across threads by capturing it in a handler, which races a non-atomic refcount.
///
/// [`Conn`]: crate::Conn
type ByteCountHandler<C> = Box<dyn FnMut(&mut C, StreamId, u64) + Send>;

/// A handler invoked at the start or end of a field section.
type SectionHandler<C> = Box<dyn FnMut(&mut C, StreamId, FieldSection) + Send>;

/// A well-known field name, as QPACK's static table identifies it.
///
/// nghttp3 supplies this alongside every received field, which lets a handler dispatch on
/// a known name without comparing bytes. `None` when the name is not one QPACK names.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FieldToken(i32);

impl FieldToken {
    /// Wraps the raw token nghttp3 supplied, if it supplied one.
    pub(crate) fn from_raw(token: i32) -> Option<Self> {
        (token >= 0).then_some(Self(token))
    }

    /// The raw token, comparable against the `NGHTTP3_QPACK_TOKEN_*` constants in
    /// [`crate::raw`].
    pub fn get(self) -> i32 {
        self.0
    }
}

/// A handler invoked for one received field.
type FieldHandler<C> = Box<
    dyn FnMut(&mut C, StreamId, FieldSection, Option<FieldToken>, &[u8], &[u8]) -> FieldAction
        + Send,
>;

/// A handler invoked for one chunk of received body bytes.
type DataHandler<C> = Box<dyn FnMut(&mut C, StreamId, &[u8]) + Send>;

/// A handler invoked with just a stream.
type StreamHandler<C> = Box<dyn FnMut(&mut C, StreamId) + Send>;

/// Why a stream closed.
///
/// The two directions carry separate application error codes, and conflating them loses
/// the distinction between "the peer gave up on our response" and "we gave up on their
/// request". Both are [`crate::ErrorCode`] values a QUIC layer reported.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct StreamClosed {
    /// The code the peer reset its sending direction with.
    ///
    /// `None` means that direction closed cleanly. nghttp3 signals the difference with a
    /// flag rather than by the code's value, so collapsing it to a zero would lose it.
    pub receiving: Option<ErrorCode>,
    /// The code this endpoint's sending direction was stopped with, or `None` if it was
    /// not stopped.
    pub sending: Option<ErrorCode>,
}

/// A handler invoked when a stream closes.
type CloseHandler<C> = Box<dyn FnMut(&mut C, StreamId, StreamClosed) + Send>;

/// The set of handlers a connection may call.
///
/// Generic over the caller's state type `C`, which every handler receives by mutable
/// reference.
pub(crate) struct Handlers<C> {
    /// Previously blocked stream data has been consumed, and that much more QUIC
    /// flow-control credit may be extended.
    pub(crate) deferred_consume: Option<ByteCountHandler<C>>,
    /// A field section has started.
    pub(crate) section_begin: Option<SectionHandler<C>>,
    /// One field of a section has arrived.
    pub(crate) field: Option<FieldHandler<C>>,
    /// A field section has ended.
    pub(crate) section_end: Option<SectionHandler<C>>,
    /// A chunk of body bytes has arrived.
    pub(crate) data: Option<DataHandler<C>>,
    /// The peer has finished sending on a stream.
    pub(crate) end_stream: Option<StreamHandler<C>>,
    /// A stream has closed.
    pub(crate) stream_close: Option<CloseHandler<C>>,
}

// Hand-written rather than derived: `#[derive(Default)]` would require `C: Default`, which
// has nothing to do with whether a handler set is empty.
impl<C> Default for Handlers<C> {
    fn default() -> Self {
        Self {
            deferred_consume: None,
            section_begin: None,
            field: None,
            section_end: None,
            data: None,
            end_stream: None,
            stream_close: None,
        }
    }
}

impl<C> core::fmt::Debug for Handlers<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Handlers")
            .field("deferred_consume", &self.deferred_consume.is_some())
            .field("section_begin", &self.section_begin.is_some())
            .field("field", &self.field.is_some())
            .field("section_end", &self.section_end.is_some())
            .field("data", &self.data.is_some())
            .field("end_stream", &self.end_stream.is_some())
            .field("stream_close", &self.stream_close.is_some())
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
        assert!(handlers.field.is_none());
    }
}
