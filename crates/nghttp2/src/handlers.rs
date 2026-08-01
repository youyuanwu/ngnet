//! Caller-registered event handlers.
//!
//! Handlers are closures registered when a session is built. They receive the caller's
//! own application state by mutable reference — supplied at the time bytes are submitted
//! or collected — together with borrowed views into libnghttp2's buffers. Nothing is
//! copied on the way through.
//!
//! Handlers are never handed the session. Their only influence over it is their return
//! value, which is why only the header-phase handlers can ask for a stream to be
//! cancelled: libnghttp2 treats a nonzero return from the other callbacks as fatal to
//! the whole connection rather than to one stream.

use crate::error::ErrorCode;
use crate::stream::{FrameInfo, StreamId};

/// What a header-phase handler wants to happen next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeaderAction {
    /// Carry on processing the message.
    #[default]
    Continue,
    /// Cancel this stream. The peer observes a `RST_STREAM`.
    CancelStream,
}

type BeginHeaders<C> = Box<dyn FnMut(&mut C, FrameInfo) -> HeaderAction + Send>;
type Header<C> = Box<dyn FnMut(&mut C, FrameInfo, &[u8], &[u8]) -> HeaderAction + Send>;
type DataChunk<C> = Box<dyn FnMut(&mut C, StreamId, &[u8]) + Send>;
type FrameRecv<C> = Box<dyn FnMut(&mut C, FrameInfo) + Send>;
type StreamClose<C> = Box<dyn FnMut(&mut C, StreamId, ErrorCode) + Send>;

/// The set of handlers registered on a session.
///
/// Every slot is optional; an event with no handler is processed normally and discarded.
///
/// All handlers are bound `Send` because they are stored in the session, and a session
/// may be moved between threads.
pub(crate) struct Handlers<C> {
    pub(crate) begin_headers: Option<BeginHeaders<C>>,
    pub(crate) header: Option<Header<C>>,
    pub(crate) data_chunk: Option<DataChunk<C>>,
    pub(crate) frame_recv: Option<FrameRecv<C>>,
    pub(crate) stream_close: Option<StreamClose<C>>,
}

// Written by hand rather than derived: `derive(Default)` would demand `C: Default`, but
// the context type is never constructed here, only borrowed.
impl<C> Default for Handlers<C> {
    fn default() -> Self {
        Self {
            begin_headers: None,
            header: None,
            data_chunk: None,
            frame_recv: None,
            stream_close: None,
        }
    }
}

impl<C> core::fmt::Debug for Handlers<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Closures cannot be formatted, so report which slots are occupied.
        f.debug_struct("Handlers")
            .field("begin_headers", &self.begin_headers.is_some())
            .field("header", &self.header.is_some())
            .field("data_chunk", &self.data_chunk.is_some())
            .field("frame_recv", &self.frame_recv.is_some())
            .field("stream_close", &self.stream_close.is_some())
            .finish()
    }
}
