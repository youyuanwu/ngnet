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
//! # Delivered data, and how it stopped being a copy
//!
//! [`Event::StreamData`] carries a [`StreamBytes`], which owns its bytes without necessarily
//! having copied them. The handler receives a borrow that is valid only for the duration of
//! dwnx's callback -- it points into the buffer dwnx is parsing -- so carrying *the borrow* out
//! to a caller polled later is not possible. It used to be carried out as a copy, one memcpy
//! per delivery.
//!
//! What removed the copy is that the buffer dwnx is parsing is this connection's own read
//! buffer (`deps/dwnx/lib/dwnx_conn.c:1631-1636`), so the bytes are already in memory this
//! crate owns and a reference count can outlive a borrow where the borrow cannot. See
//! [`super::delivery`] for the view type and for the threshold below which a delivery is still
//! copied, which is what bounds the memory a held delivery pins.
//!
//! # How the handler reaches the buffer, when it cannot reach the connection
//!
//! The handler is `'static` and has no way to name the connection -- that is a deliberate
//! design property with compile-fail cases enforcing it (`crate::compile_fail`), and it is why
//! the delivery was copied in the first place. So it cannot ask the connection which buffer the
//! bytes came from.
//!
//! What it can do is hold, alongside the queue it already holds, the reference-counted handle
//! for the buffer currently being parsed. The read side puts it there immediately before
//! feeding the state machine and takes it away immediately after, and in between the handler
//! has everything it needs: the handle, and a borrow whose address says where inside it the
//! bytes lie. Neither is a connection handle -- there is no operation on either that reaches
//! the connection -- so the property the compile-fail cases pin is untouched, and it is checked
//! rather than argued: those cases still fail to compile, for the reasons they name.
//!
//! Whether the borrow really lies inside that buffer is **checked, not assumed**. dwnx delivers
//! stream payload straight out of the buffer it was handed, but an implementation detail that
//! changed underneath should cost a copy rather than produce wrong bytes, so a slice whose
//! address falls outside the buffer is copied out. The addresses are compared and never
//! dereferenced.
//!
//! The rejected alternative is the HTTP/2 stack's, which is the same idea one step later: its
//! handler records *where* a chunk lay as an offset and a length, and the driver resolves those
//! into views once the call has returned (`crates/ngnet-h2/src/http/driver.rs`). It needs no
//! shared handle at all, and it was not copied because of where the delivery type sits in each
//! crate. There, the unresolved form is an internal event the caller never sees; here it would
//! be a publicly delivered [`StreamBytes`] with a resolution step that could be missed, and a
//! missed one is an empty delivery rather than a failure.
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
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::handlers::StreamLimitKind;
use crate::io::delivery::{ALIAS_THRESHOLD, StreamBytes};
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
        /// The bytes.
        ///
        /// A view of the connection's read buffer where the delivery is large enough to be
        /// worth holding one, and an allocation of its own where it is not. Either way the
        /// bytes are the caller's for as long as it wants them, and either way they are the
        /// bytes the peer sent; see [`StreamBytes`].
        data: StreamBytes,
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
    shared: Arc<Mutex<Shared>>,
}

/// What the queue and the handlers share.
///
/// The events, and the buffer the state machine is parsing right now. The second is what lets
/// [`EventQueue::deliver`] hand out a view instead of a copy; see the
/// [module documentation](self) for why it lives here rather than being reached for through the
/// connection.
#[derive(Debug, Default)]
struct Shared {
    queue: VecDeque<Event>,
    /// The read buffer being fed to the state machine, between [`EventQueue::begin_read`] and
    /// [`EventQueue::end_read`], and [`None`] at every other instant.
    ///
    /// Held for the duration of one `Conn::read` and no longer. A handle left here would keep
    /// the buffer's strong count above one for ever, and the connection reuses a buffer exactly
    /// when that count comes back to one -- so the connection would allocate a fresh buffer on
    /// every read and the pool would never reclaim anything.
    reading: Option<Arc<Vec<u8>>>,
}

impl EventQueue {
    /// An empty queue.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The shared state.
    ///
    /// A poisoned lock is recovered from rather than propagated. Poisoning requires a panic
    /// while the lock was held, and the only code that holds it is this file and the pump --
    /// but a panic inside a handler aborts the process anyway, since it would otherwise unwind
    /// through C. Panicking here in response would replace one connection's event with a
    /// second panic and no additional information.
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Appends an event, from a handler.
    pub(crate) fn push(&self, event: Event) {
        self.lock().queue.push_back(event);
    }

    /// Appends a data delivery, from the read handler.
    ///
    /// Separate from [`EventQueue::push`] because it is the one event whose payload depends on
    /// what the connection is doing at the instant the handler runs, and because putting the
    /// address arithmetic in one place is what keeps it checkable.
    pub(crate) fn deliver(&self, stream_id: StreamId, offset: u64, data: &[u8], fin: bool) {
        let mut shared = self.lock();
        let bytes = shared.view(data);
        shared.queue.push_back(Event::StreamData {
            stream_id,
            offset,
            data: bytes,
            fin,
        });
    }

    /// Takes the oldest event, if there is one.
    pub(crate) fn pop(&self) -> Option<Event> {
        self.lock().queue.pop_front()
    }

    /// Names the buffer about to be fed to the state machine.
    pub(crate) fn begin_read(&self, buffer: &Arc<Vec<u8>>) {
        self.lock().reading = Some(Arc::clone(buffer));
    }

    /// Forgets it again, releasing the connection's claim on that buffer.
    pub(crate) fn end_read(&self) {
        self.lock().reading = None;
    }
}

impl Shared {
    /// The delivery to hand out for `data`.
    ///
    /// A view of the buffer being parsed when `data` is a range of it and is long enough to be
    /// worth holding one; a copy otherwise. The three ways it can be a copy are all ordinary
    /// rather than exceptional, and none of them is wrong: a short delivery is copied because
    /// the pinning bound requires it, a delivery arriving outside a read is copied because there
    /// is no buffer to alias, and a delivery whose bytes are not in the buffer at all is copied
    /// because they are still the right bytes and this is not the place to insist on where they
    /// came from.
    fn view(&self, data: &[u8]) -> StreamBytes {
        if data.len() < ALIAS_THRESHOLD {
            return StreamBytes::copied(data);
        }
        let Some(buffer) = &self.reading else {
            return StreamBytes::copied(data);
        };

        // Addresses, compared and never dereferenced. Two live allocations cannot overlap, so a
        // slice whose start lies inside this buffer and whose end does not run past it is a
        // range of this buffer and of nothing else.
        let base = buffer.as_ptr() as usize;
        let start = data.as_ptr() as usize;
        let Some(offset) = start.checked_sub(base) else {
            return StreamBytes::copied(data);
        };

        StreamBytes::aliased(Arc::clone(buffer), offset, data.len())
            .unwrap_or_else(|| StreamBytes::copied(data))
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
            data: StreamBytes::from(b"first".to_vec()),
            fin: false,
        });
        queue.push(Event::StreamData {
            stream_id: stream(0),
            offset: 5,
            data: StreamBytes::new(),
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

    fn read_buffer() -> Arc<Vec<u8>> {
        Arc::new((0..u8::MAX).cycle().take(4096).collect())
    }

    /// A delivery out of the buffer being parsed is a view of it, not a copy.
    #[test]
    fn a_long_delivery_from_the_buffer_being_read_is_aliased() {
        let queue = EventQueue::new();
        let buffer = read_buffer();
        queue.begin_read(&buffer);
        queue.deliver(stream(0), 0, &buffer[128..128 + ALIAS_THRESHOLD], false);
        queue.end_read();

        let Some(Event::StreamData { data, .. }) = queue.pop() else {
            panic!("a delivery");
        };
        assert!(data.is_aliased(), "the delivery should be a view");
        assert_eq!(&data[..], &buffer[128..128 + ALIAS_THRESHOLD]);
    }

    /// Three ways a delivery is copied instead, each of which must still deliver the right
    /// bytes: too short to be worth pinning a buffer for, no read in progress, and bytes that
    /// are not in the buffer at all.
    #[test]
    fn a_delivery_that_cannot_or_should_not_alias_is_copied_and_still_correct() {
        let queue = EventQueue::new();
        let buffer = read_buffer();
        let elsewhere: Vec<u8> = (0..ALIAS_THRESHOLD as u64).map(|byte| byte as u8).collect();

        queue.begin_read(&buffer);
        queue.deliver(stream(0), 0, &buffer[0..ALIAS_THRESHOLD - 1], false);
        queue.deliver(stream(0), 1, &elsewhere, false);
        queue.end_read();
        queue.deliver(stream(0), 2, &buffer[0..ALIAS_THRESHOLD], false);

        let expected: [&[u8]; 3] = [
            &buffer[0..ALIAS_THRESHOLD - 1],
            &elsewhere,
            &buffer[0..ALIAS_THRESHOLD],
        ];
        for (offset, bytes) in expected.iter().enumerate() {
            let Some(Event::StreamData { data, .. }) = queue.pop() else {
                panic!("a delivery");
            };
            assert!(
                !data.is_aliased(),
                "delivery {offset} should have been copied"
            );
            assert_eq!(&data[..], *bytes, "delivery {offset}");
        }
    }

    /// The handle is released when the read ends, which is what lets the buffer be read into
    /// again. A queue that kept it would make every read allocate.
    #[test]
    fn the_buffer_handle_is_given_back_when_the_read_ends() {
        let queue = EventQueue::new();
        let mut buffer = read_buffer();
        queue.begin_read(&buffer);
        assert!(
            Arc::get_mut(&mut buffer).is_none(),
            "the queue holds the buffer while the state machine is being fed"
        );
        queue.end_read();
        assert!(
            Arc::get_mut(&mut buffer).is_some(),
            "and lets go of it afterwards"
        );
    }
}
