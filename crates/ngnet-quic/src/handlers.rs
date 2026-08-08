//! Events a connection reports to its owner.
//!
//! ngtcp2 delivers everything through C callbacks. Rather than exposing forty-seven of
//! them, this crate groups the ones an application actually acts on into a handler struct
//! built through [`Handlers`], following the shape `ngnet-h3` uses.
//!
//! Handlers run **inside** a call into ngtcp2, with the connection mutably borrowed. They
//! therefore cannot call back into the connection — and three ngtcp2 entry points
//! (`read_pkt`, `writev_stream`, `write_connection_close`) explicitly forbid it anyway
//! (`ngtcp2.h:4256`, `:5318`, `:6665`). Anything a handler wants to *do* should be recorded
//! and acted on after the call returns.
//!
//! A handler must not panic: unwinding into a C stack frame aborts the process.

use crate::error::ApplicationErrorCode;
use crate::stream::StreamId;

/// Why a stream ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum StreamCloseReason {
    /// Both sides finished normally.
    Finished,
    /// The peer reset it, or this endpoint did.
    Reset(ApplicationErrorCode),
}

/// A handler for stream data: the identifier, the bytes, and whether they end the stream.
///
/// Named rather than written inline because the borrowed slice makes the closure type long
/// enough to obscure the signature it appears in.
type StreamDataHandler<'a> = Box<dyn FnMut(StreamId, &[u8], bool) + 'a>;

/// A handler taking a stream and an application error code.
type StreamErrorHandler<'a> = Box<dyn FnMut(StreamId, ApplicationErrorCode) + 'a>;

/// The callbacks an application supplies.
///
/// Every field is optional; an absent handler means the event is ignored. The defaults are
/// chosen so that a connection with no handlers at all is still correct, merely silent.
#[derive(Default)]
pub struct Handlers<'a> {
    pub(crate) on_stream_data: Option<StreamDataHandler<'a>>,
    pub(crate) on_stream_open: Option<Box<dyn FnMut(StreamId) + 'a>>,
    pub(crate) on_stream_close: Option<Box<dyn FnMut(StreamId, StreamCloseReason) + 'a>>,
    pub(crate) on_stream_reset: Option<StreamErrorHandler<'a>>,
    pub(crate) on_stop_sending: Option<StreamErrorHandler<'a>>,
    pub(crate) on_acked_stream_data: Option<Box<dyn FnMut(StreamId, u64) + 'a>>,
    pub(crate) on_handshake_completed: Option<Box<dyn FnMut() + 'a>>,
}

impl<'a> Handlers<'a> {
    /// An empty handler set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Called with data received on a stream, and whether that data ends it.
    ///
    /// The slice borrows ngtcp2's own buffer and is valid only for the call.
    pub fn on_stream_data(mut self, f: impl FnMut(StreamId, &[u8], bool) + 'a) -> Self {
        self.on_stream_data = Some(Box::new(f));
        self
    }

    /// Called when the peer opens a stream.
    pub fn on_stream_open(mut self, f: impl FnMut(StreamId) + 'a) -> Self {
        self.on_stream_open = Some(Box::new(f));
        self
    }

    /// Called when a stream is fully closed.
    pub fn on_stream_close(mut self, f: impl FnMut(StreamId, StreamCloseReason) + 'a) -> Self {
        self.on_stream_close = Some(Box::new(f));
        self
    }

    /// Called when the peer resets a stream it was sending on.
    pub fn on_stream_reset(mut self, f: impl FnMut(StreamId, ApplicationErrorCode) + 'a) -> Self {
        self.on_stream_reset = Some(Box::new(f));
        self
    }

    /// Called when the peer asks this endpoint to stop sending on a stream.
    pub fn on_stop_sending(mut self, f: impl FnMut(StreamId, ApplicationErrorCode) + 'a) -> Self {
        self.on_stop_sending = Some(Box::new(f));
        self
    }

    /// Called with the number of bytes the peer has acknowledged on a stream.
    ///
    /// This is what releases buffers retained for retransmission. An application that sends
    /// large bodies and ignores this will hold every byte it ever sent.
    pub fn on_acked_stream_data(mut self, f: impl FnMut(StreamId, u64) + 'a) -> Self {
        self.on_acked_stream_data = Some(Box::new(f));
        self
    }

    /// Called once, when the TLS handshake completes.
    pub fn on_handshake_completed(mut self, f: impl FnMut() + 'a) -> Self {
        self.on_handshake_completed = Some(Box::new(f));
        self
    }
}

impl core::fmt::Debug for Handlers<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Handlers")
            .field("on_stream_data", &self.on_stream_data.is_some())
            .field("on_stream_open", &self.on_stream_open.is_some())
            .field("on_stream_close", &self.on_stream_close.is_some())
            .field("on_acked_stream_data", &self.on_acked_stream_data.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_handler_set_registers_nothing() {
        let handlers = Handlers::new();
        assert!(handlers.on_stream_data.is_none());
        assert!(handlers.on_handshake_completed.is_none());
    }

    #[test]
    fn registering_a_handler_records_it() {
        let handlers = Handlers::new()
            .on_stream_data(|_, _, _| {})
            .on_handshake_completed(|| {});
        assert!(handlers.on_stream_data.is_some());
        assert!(handlers.on_handshake_completed.is_some());
        assert!(handlers.on_stream_open.is_none());
    }

    #[test]
    fn a_handler_can_borrow_from_its_environment() {
        // The lifetime parameter exists for this: recording events into a local is the
        // ordinary way to use these, since a handler cannot call back into the connection.
        let mut seen = Vec::new();
        {
            let mut handlers = Handlers::new().on_stream_open(|id| seen.push(id));
            if let Some(f) = handlers.on_stream_open.as_mut() {
                f(StreamId::new(0).unwrap());
            }
        }
        assert_eq!(seen.len(), 1);
    }
}
