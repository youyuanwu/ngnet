//! What the connection observed, as values a caller can act on.
//!
//! # Why events exist at all
//!
//! The state machine reports protocol activity through handlers that receive event values and
//! **no connection handle** (`crate::Handlers`). That is deliberate there: dwnx forbids
//! writing a record from inside a callback, and a handler that cannot reach the connection
//! cannot break the rule. The cost is that a handler can only record what it saw.
//!
//! This layer pays that cost once, on the caller's behalf. Its handlers push an [`Event`] onto
//! a queue and return; the pump acts on the queue after the entry point that provoked the
//! callbacks has returned, and [`poll_next_event`](super::Connection::poll_next_event) hands
//! the events to the caller in the order they happened. A caller who wants to extend a window
//! the instant data arrives therefore can, which is exactly what a handler cannot do.
//!
//! # Owned data, and what it costs
//!
//! [`Event::StreamData`] owns its bytes. The handler receives a borrow that is valid only for
//! the duration of dwnx's callback -- it points into the record buffer dwnx is parsing -- so
//! carrying it out to a caller who is polled later is not possible without copying it. The
//! alternative would be to invoke a caller-supplied callback from inside the handler, which is
//! the design the state machine already offers and which this layer exists to be an
//! alternative to.
//!
//! The copy is one memcpy per delivery, bounded by the record size, and it is recorded as an
//! acknowledged cost rather than an oversight.
//!
//! # Why the queue is a `Mutex` when nothing here is threaded
//!
//! The state machine requires its handlers to be `Send`, because a `Conn` is `Send` and owns
//! them; a handler capturing an `Rc` could be dropped on another thread and race a non-atomic
//! refcount from safe code. The layer's own handlers must satisfy that bound even though the
//! connection polls them from wherever the caller polls it, so the queue they share is an
//! `Arc<Mutex<..>>` and not an `Rc<RefCell<..>>`.
//!
//! That bound stops at the handlers. Nothing here constrains the caller's byte stream or
//! clock, which remain free to be `Rc`-based; see the [module documentation](super) for why
//! that asymmetry is not an inconsistency.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use crate::handlers::StreamLimitKind;
use crate::params::TransportParams;
use crate::stream::StreamId;

/// Something that happened on the connection.
///
/// Delivered by [`Connection::poll_next_event`](super::Connection::poll_next_event) in the
/// order the protocol produced it. Several events may arise from a single read -- a record
/// carries several frames, and several records may arrive in one chunk of bytes -- and they
/// are delivered as one sequence rather than collapsed or reordered.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    /// Data arrived on a stream.
    StreamData {
        /// The stream it arrived on.
        stream_id: StreamId,
        /// Where in the stream these bytes begin.
        ///
        /// Deliveries are in order and do not overlap, so this advances by exactly the length
        /// of the previous delivery on the same stream. It is carried anyway because a caller
        /// reassembling into a sparse buffer should not have to keep the count itself, and a
        /// caller that does keep it can assert against this.
        offset: u64,
        /// The bytes, copied out of the record dwnx was parsing.
        data: Vec<u8>,
        /// Whether these bytes end the stream.
        ///
        /// May be set on an empty `data`: a peer that finishes a stream having already sent
        /// everything sends a zero-length STREAM frame with the fin bit, and that carries the
        /// end of stream and nothing else. It is delivered rather than suppressed, because a
        /// caller waiting for the end of a stream would otherwise wait forever.
        fin: bool,
    },

    /// The peer opened a stream.
    ///
    /// dwnx raises this for an explicit open only, not for a stream brought into existence
    /// implicitly by data arriving on a higher-numbered one, so a caller must not treat the
    /// absence of this event as proof that a stream does not exist.
    StreamOpened {
        /// The stream the peer opened.
        stream_id: StreamId,
    },

    /// A stream closed, with whichever application error codes applied.
    StreamClosed {
        /// The stream that closed.
        stream_id: StreamId,
        /// The code the peer sent, if it reset its sending side.
        ///
        /// `None` and `Some(0)` are different: the first is a stream that ended without a
        /// reset, the second is a reset carrying the code zero.
        rx_app_error_code: Option<u64>,
        /// The code this endpoint sent, if it reset its sending side.
        tx_app_error_code: Option<u64>,
    },

    /// The peer reset a stream, abandoning what it had left to send.
    StreamReset {
        /// The stream the peer reset.
        stream_id: StreamId,
        /// How many bytes the stream turned out to contain in total.
        final_size: u64,
        /// Why, in the application's own numbering.
        app_error_code: u64,
    },

    /// The peer asked this endpoint to stop sending on a stream.
    StopSending {
        /// The stream the peer has stopped reading.
        stream_id: StreamId,
        /// Why, in the application's own numbering.
        app_error_code: u64,
    },

    /// The peer raised how much this endpoint may send on a stream.
    ///
    /// The event a write blocked by stream-level flow control is waiting for.
    StreamDataCredit {
        /// The stream whose window moved.
        stream_id: StreamId,
        /// The new cumulative limit, not the increment.
        max_data: u64,
    },

    /// The peer raised the connection-wide stream-data send window.
    ///
    /// Unlike [`Event::StreamDataCredit`], this is not associated with one stream. An adapter
    /// uses it to wake operations blocked specifically on the connection window.
    ConnectionDataCredit {
        /// Connection-wide send credit currently available after applying the update.
        available: u64,
    },

    /// The peer raised one of the four stream-count limits.
    ///
    /// The event a blocked open is waiting for.
    StreamLimit {
        /// Which of the four limits moved.
        kind: StreamLimitKind,
        /// The new cumulative count, not the increment.
        max_streams: u64,
    },

    /// The peer's transport parameters arrived.
    ///
    /// The first event of any connection that gets anywhere, and the one that grants this
    /// endpoint the capacity to open streams and send data at all: the limits are the peer's
    /// to advertise, and until this arrives every one of them is zero.
    PeerTransportParams(TransportParams),
}

/// The queue the layer's handlers push onto and the pump drains.
///
/// Cloneable and shared: one clone lives in each handler the layer installs, and one lives in
/// the connection. See the [module documentation](self) for why it is an `Arc<Mutex<..>>` when
/// nothing in this layer is threaded.
#[derive(Clone, Debug, Default)]
pub(crate) struct EventQueue {
    queue: Arc<Mutex<VecDeque<Event>>>,
}

impl EventQueue {
    /// An empty queue.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Appends an event, from a handler.
    ///
    /// A poisoned lock is recovered from rather than propagated. Poisoning requires a panic
    /// while the lock was held, and the only code that holds it is this file and the pump --
    /// but a panic inside a handler aborts the process anyway, since it would otherwise unwind
    /// through C. Panicking here in response would replace one connection's event with a
    /// second panic and no additional information.
    pub(crate) fn push(&self, event: Event) {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(event);
    }

    /// Takes the oldest event, if there is one.
    pub(crate) fn pop(&self) -> Option<Event> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: i64) -> StreamId {
        StreamId::new(id).expect("a valid stream id")
    }

    #[test]
    fn the_queue_preserves_the_order_events_were_pushed_in() {
        let queue = EventQueue::new();
        queue.push(Event::StreamOpened {
            stream_id: stream(0),
        });
        queue.push(Event::StreamData {
            stream_id: stream(0),
            offset: 0,
            data: b"first".to_vec(),
            fin: false,
        });
        queue.push(Event::StreamData {
            stream_id: stream(0),
            offset: 5,
            data: Vec::new(),
            fin: true,
        });

        assert!(matches!(queue.pop(), Some(Event::StreamOpened { .. })));
        assert!(matches!(
            queue.pop(),
            Some(Event::StreamData { offset: 0, .. })
        ));
        assert!(matches!(
            queue.pop(),
            Some(Event::StreamData {
                offset: 5,
                fin: true,
                ..
            })
        ));
        assert!(queue.pop().is_none());
    }

    /// A clone shares the queue rather than copying it; that is what makes the handlers and
    /// the connection see the same events.
    #[test]
    fn clones_share_one_queue() {
        let queue = EventQueue::new();
        let handler_side = queue.clone();
        handler_side.push(Event::StopSending {
            stream_id: stream(4),
            app_error_code: 7,
        });
        assert!(matches!(
            queue.pop(),
            Some(Event::StopSending {
                app_error_code: 7,
                ..
            })
        ));
    }

    /// The bound the state machine puts on handlers, which the queue has to satisfy for the
    /// layer's own handlers to be installable at all.
    #[test]
    fn the_queue_is_sendable() {
        fn require_send<T: Send>(_: &T) {}
        require_send(&EventQueue::new());
    }
}
