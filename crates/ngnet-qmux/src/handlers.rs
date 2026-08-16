//! Caller-supplied reactions to protocol events.
//!
//! # Handlers cannot reach the connection
//!
//! Every handler here receives event values and nothing else. dwnx's C callbacks are handed a
//! `dwnx_conn *`, but the shims in [`crate::callbacks`] do not forward it, and that omission
//! is deliberate: it is what makes the one operation dwnx forbids during a callback --
//! `dwnx_conn_writev_stream` -- impossible to express rather than merely discouraged.
//!
//! The cost is real. A handler cannot extend a flow-control window at the moment it observes
//! data, or open a stream in response to one closing; it records what it saw, and the caller
//! acts after the entry point returns. In exchange the callback bridge stays a single pointer
//! with no re-entrancy state, and the guarantee is one the compiler enforces rather than one
//! the documentation asks for.
//!
//! # Panics abort
//!
//! A handler runs on a stack that C called into. Unwinding out of it is undefined behaviour,
//! so a panic aborts the process instead. Report failure by returning [`Err`], which dwnx
//! turns into a clean error at the entry point that triggered the callback; the caller's own
//! error value is preserved on the way out.

use crate::params::TransportParams;
use crate::stream::StreamId;

/// An error a handler reports back to the connection.
///
/// Carries a caller-chosen message. dwnx collapses every nonzero callback return to a single
/// `CALLBACK_FAILURE` code, so this value is stashed on the way out and reattached to the
/// error the entry point returns; without that, the reason would be lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerError {
    message: &'static str,
}

impl HandlerError {
    /// Describe why the handler failed.
    #[must_use]
    pub const fn new(message: &'static str) -> Self {
        Self { message }
    }

    /// The message.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl core::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.message)
    }
}

impl core::error::Error for HandlerError {}

/// What a handler returns.
pub type HandlerResult = Result<(), HandlerError>;

/// A boxed, optional handler taking one event value.
///
/// Every field of [`Handlers`] is one of these; naming the shape once keeps the struct
/// readable and stops each field restating the same pieces of syntax.
///
/// The `Send` bound is load-bearing, exactly as it is in `ngnet-quic`. A [`crate::Conn`] is
/// `Send`, and it owns its handlers -- so without this a caller could capture an `Rc` in a
/// handler, move the connection to another thread and drop it there, racing a non-atomic
/// refcount from safe code. The bound is what makes that `unsafe impl Send` honest.
type Handler<'h, T> = Option<Box<dyn FnMut(T) -> HandlerResult + Send + 'h>>;

/// The same, for the events dwnx reports with two values.
type Handler2<'h, A, B> = Option<Box<dyn FnMut(A, B) -> HandlerResult + Send + 'h>>;

/// The same, for the one event reported with three.
type Handler3<'h, A, B, C> = Option<Box<dyn FnMut(A, B, C) -> HandlerResult + Send + 'h>>;

/// The two handlers taking a borrow need their own aliases, because the borrow is higher
/// ranked -- it lives only for the callback -- and cannot be threaded through a type parameter.
type RecvTransportParams<'h> =
    Option<Box<dyn FnMut(&TransportParams) -> HandlerResult + Send + 'h>>;
type RecvStreamData<'h> =
    Option<Box<dyn FnMut(StreamDataEvent<'_>) -> HandlerResult + Send + 'h>>;

/// Stream data received from the peer.
#[derive(Clone, Copy, Debug)]
pub struct StreamDataEvent<'a> {
    /// The stream the data arrived on.
    pub stream_id: StreamId,
    /// The offset within the stream at which this data begins.
    ///
    /// dwnx delivers stream data in order and without overlap, so this advances by exactly the
    /// length of the previous delivery.
    pub offset: u64,
    /// The data itself. May be empty, but only when [`StreamDataEvent::fin`] is set.
    pub data: &'a [u8],
    /// Whether this delivery carries the end of the stream.
    pub fin: bool,
}

/// A stream closing.
#[derive(Clone, Copy, Debug)]
pub struct StreamCloseEvent {
    /// The stream that closed.
    pub stream_id: StreamId,
    /// The application error code the peer sent, if it reset its sending side.
    pub rx_app_error_code: Option<u64>,
    /// The application error code sent locally, if this side reset its sending side.
    pub tx_app_error_code: Option<u64>,
}

/// Which limit was raised, for the four `extend_max_*_streams` callbacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamLimitKind {
    /// Bidirectional streams this endpoint may open.
    LocalBidi,
    /// Unidirectional streams this endpoint may open.
    LocalUni,
    /// Bidirectional streams the peer may open.
    RemoteBidi,
    /// Unidirectional streams the peer may open.
    RemoteUni,
}

/// The set of reactions to protocol events.
///
/// Every handler is optional; dwnx documents all but `recv_transport_params` as optional, and
/// omitting even that one is allowed here because the parameters are cached by the connection
/// regardless, so a caller who only wants to read them later need not supply a closure.
///
/// Handlers are `FnMut` and live for as long as the connection, so they may own state.
#[derive(Default)]
pub struct Handlers<'h> {
    pub(crate) recv_transport_params: RecvTransportParams<'h>,
    pub(crate) recv_stream_data: RecvStreamData<'h>,
    pub(crate) stream_open: Handler<'h, StreamId>,
    pub(crate) stream_close: Handler<'h, StreamCloseEvent>,
    pub(crate) stream_reset: Handler3<'h, StreamId, u64, u64>,
    pub(crate) stream_stop_sending: Handler2<'h, StreamId, u64>,
    pub(crate) recv_stop_sending: Handler2<'h, StreamId, u64>,
    pub(crate) extend_max_stream_data: Handler2<'h, StreamId, u64>,
    pub(crate) extend_max_streams: Handler2<'h, StreamLimitKind, u64>,
}

impl<'h> Handlers<'h> {
    /// No handlers at all. A connection built with these still works; events simply go
    /// unobserved.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Called when the peer's transport parameters arrive.
    #[must_use]
    pub fn on_transport_params(
        mut self,
        handler: impl FnMut(&TransportParams) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.recv_transport_params = Some(Box::new(handler));
        self
    }

    /// Called when stream data arrives.
    #[must_use]
    pub fn on_stream_data(
        mut self,
        handler: impl FnMut(StreamDataEvent<'_>) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.recv_stream_data = Some(Box::new(handler));
        self
    }

    /// Called when the peer opens a stream.
    ///
    /// dwnx invokes this only for an explicit open, not for a stream brought into existence
    /// implicitly by data arriving on a higher-numbered one.
    #[must_use]
    pub fn on_stream_open(
        mut self,
        handler: impl FnMut(StreamId) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.stream_open = Some(Box::new(handler));
        self
    }

    /// Called when a stream closes.
    #[must_use]
    pub fn on_stream_close(
        mut self,
        handler: impl FnMut(StreamCloseEvent) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.stream_close = Some(Box::new(handler));
        self
    }

    /// Called when the peer resets a stream, with its final size and application error code.
    #[must_use]
    pub fn on_stream_reset(
        mut self,
        handler: impl FnMut(StreamId, u64, u64) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.stream_reset = Some(Box::new(handler));
        self
    }

    /// Called when this endpoint stops reading a stream before receiving all of it.
    #[must_use]
    pub fn on_stream_stop_sending(
        mut self,
        handler: impl FnMut(StreamId, u64) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.stream_stop_sending = Some(Box::new(handler));
        self
    }

    /// Called when the peer sends STOP_SENDING.
    #[must_use]
    pub fn on_recv_stop_sending(
        mut self,
        handler: impl FnMut(StreamId, u64) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.recv_stop_sending = Some(Box::new(handler));
        self
    }

    /// Called when the peer raises how much this endpoint may send on a stream.
    #[must_use]
    pub fn on_extend_max_stream_data(
        mut self,
        handler: impl FnMut(StreamId, u64) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.extend_max_stream_data = Some(Box::new(handler));
        self
    }

    /// Called when any of the four stream-count limits is raised.
    ///
    /// dwnx has four separate callbacks for this; they are merged here because their
    /// signatures are identical and the distinction is one value, which is passed as
    /// [`StreamLimitKind`].
    #[must_use]
    pub fn on_extend_max_streams(
        mut self,
        handler: impl FnMut(StreamLimitKind, u64) -> HandlerResult + Send + 'h,
    ) -> Self {
        self.extend_max_streams = Some(Box::new(handler));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handlers_default_to_absent() {
        let handlers = Handlers::new();
        assert!(handlers.recv_stream_data.is_none());
        assert!(handlers.stream_open.is_none());
        assert!(handlers.extend_max_streams.is_none());
    }

    #[test]
    fn builders_install_handlers() {
        let handlers = Handlers::new()
            .on_stream_open(|_| Ok(()))
            .on_extend_max_streams(|_, _| Ok(()));
        assert!(handlers.stream_open.is_some());
        assert!(handlers.extend_max_streams.is_some());
        assert!(handlers.stream_close.is_none());
    }

    /// Handlers own state, which is the substitute for being able to act during a callback.
    #[test]
    fn handlers_may_capture_state() {
        let mut seen = 0u32;
        {
            let mut handlers = Handlers::new().on_stream_open(|_| {
                seen += 1;
                Ok(())
            });
            let handler = handlers.stream_open.as_mut().unwrap();
            handler(StreamId::new(0).unwrap()).unwrap();
            handler(StreamId::new(4).unwrap()).unwrap();
        }
        assert_eq!(seen, 2);
    }

    #[test]
    fn handler_errors_carry_their_message() {
        let error = HandlerError::new("refused");
        assert_eq!(error.message(), "refused");
        assert_eq!(error.to_string(), "refused");
    }
}
