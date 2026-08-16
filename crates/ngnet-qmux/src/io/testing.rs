//! Scaffolding for exercising the asynchronous layer, public but hidden.
//!
//! The crate takes no dev-dependencies by design, and its integration tests are separate
//! compilation units that cannot reach `cfg(test)` items -- so the machinery those tests need
//! lives here, marked `#[doc(hidden)]` to keep it out of the documented surface. It is not a
//! supported API and carries no compatibility promise.
//!
//! # The in-memory byte stream is a second implementation, not a mock
//!
//! [`stream_pair`] is a complete [`AsyncByteStream`] pair that moves bytes through a shared
//! buffer. It shares no code with any real transport, which is what makes it evidence: if the
//! trait had been quietly shaped around one runtime's socket, writing this would have been
//! awkward, and it was not.
//!
//! It is deliberately **not** [`Send`], built on `Rc` and `RefCell`. That is not a
//! simplification -- it is how the claim that this layer imposes no `Send` bound gets tested
//! rather than asserted.
//!
//! # Why the caps and the injections exist
//!
//! A byte stream is where a QMux implementation goes wrong, and it goes wrong in ways a
//! generous in-memory buffer never reproduces. A record split across reads, a write accepted
//! one byte at a time, a transport that takes nothing at all until it is drained, a stream
//! that ends between records or partway through one: each is ordinary behaviour for a real
//! socket and each is a distinct failure mode above. So the caps ([`TestByteStream::set_read_cap`],
//! [`TestByteStream::set_write_cap`]) and the capacity bound ([`TestByteStream::set_capacity`])
//! are the harness's whole point rather than an optional extra, and [`Fault`] covers the two
//! endings and the transient refusal.

use core::cell::{Cell, RefCell};
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::rc::Rc;

use super::clock::Clock;
use super::stream::{AsyncByteStream, Written};
use crate::time::Timestamp;

/// A condition injected into a test byte stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Every operation from now on fails.
    Broken,

    /// Reads report end of stream from now on, as if the peer had gone away.
    ///
    /// Separate from the peer actually calling [`AsyncByteStream::poll_shutdown`], so a test
    /// can end a stream at an arbitrary point -- including partway through a record, which is
    /// the ending a connection must not mistake for a clean one.
    Ended,

    /// The next write reports it can take nothing, once.
    ///
    /// What a socket does when its send buffer momentarily fills. The waker is woken
    /// immediately, so a connection that respects the contract makes progress on the next
    /// poll and one that treats [`Written::NotNow`] as success loses the bytes.
    WriteNotNow,
}

/// The error a test byte stream reports.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TestStreamError;

impl core::fmt::Display for TestStreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the test byte stream was broken deliberately")
    }
}

impl core::error::Error for TestStreamError {}

/// One direction of the shared buffer.
#[derive(Default)]
struct Pipe {
    bytes: VecDeque<u8>,
    /// Set once the writing half has shut down: the reader drains what is left and then sees
    /// end of stream.
    shutdown: bool,
    /// How many bytes may sit here at once, if bounded. `None` is unbounded, which is the
    /// default because most tests are not about backpressure.
    capacity: Option<usize>,
    /// Woken when bytes arrive or the writer shuts down.
    reader: Option<Waker>,
    /// Woken when the reader drains, which is what makes a full pipe a temporary condition
    /// rather than a stalled one.
    writer: Option<Waker>,
}

impl Pipe {
    /// How many more bytes this pipe will take.
    fn room(&self) -> usize {
        match self.capacity {
            Some(capacity) => capacity.saturating_sub(self.bytes.len()),
            None => usize::MAX,
        }
    }

    fn wake_reader(&mut self) {
        if let Some(waker) = self.reader.take() {
            waker.wake();
        }
    }

    fn wake_writer(&mut self) {
        if let Some(waker) = self.writer.take() {
            waker.wake();
        }
    }
}

/// An in-memory byte stream, one half of a [`stream_pair`].
///
/// Neither half is `Send`.
pub struct TestByteStream {
    inbox: Rc<RefCell<Pipe>>,
    outbox: Rc<RefCell<Pipe>>,
    read_cap: Cell<Option<usize>>,
    write_cap: Cell<Option<usize>>,
    fault: Cell<Option<Fault>>,
}

impl TestByteStream {
    /// Caps how many bytes a single read may return.
    ///
    /// `Some(1)` is the byte-at-a-time case: every record boundary falls between two reads,
    /// which is the arrangement that catches a connection assuming a read delivers whole
    /// records.
    pub fn set_read_cap(&self, cap: Option<usize>) {
        self.read_cap.set(cap);
    }

    /// Caps how many bytes a single write may accept.
    ///
    /// The write-side counterpart: a connection that treats a partial accept as a whole one
    /// truncates the record it was writing, and with a cap of `Some(1)` it does so on the
    /// first record it produces rather than under load six months later.
    pub fn set_write_cap(&self, cap: Option<usize>) {
        self.write_cap.set(cap);
    }

    /// Bounds how many written bytes may sit undelivered before writes report
    /// [`Written::NotNow`].
    ///
    /// Unlike [`Fault::WriteNotNow`] this is not one-shot: the condition clears only when the
    /// peer reads, which is what a genuinely backed-up transport does and the only way to test
    /// that a blocked writer is woken by the drain rather than by a courtesy wake.
    pub fn set_capacity(&self, capacity: Option<usize>) {
        self.outbox.borrow_mut().capacity = capacity;
    }

    /// Arranges for a condition on the next operation.
    pub fn inject(&self, fault: Fault) {
        self.fault.set(Some(fault));
    }

    /// Clears an injected condition.
    pub fn clear_fault(&self) {
        self.fault.set(None);
    }

    /// How many bytes this half has written that the peer has not yet read.
    pub fn queued(&self) -> usize {
        self.outbox.borrow().bytes.len()
    }

    /// Whether the peer has shut its write side down.
    pub fn peer_shutdown(&self) -> bool {
        self.inbox.borrow().shutdown
    }

    /// Delivers bytes to this half as if the peer had written them.
    ///
    /// How a test injects traffic no conforming peer would produce -- a malformed record, a
    /// length prefix promising more than follows.
    pub fn deliver(&self, bytes: &[u8]) {
        let mut inbox = self.inbox.borrow_mut();
        inbox.bytes.extend(bytes.iter().copied());
        inbox.wake_reader();
    }

    /// Removes and returns everything this half has written and the peer has not read.
    pub fn take_written(&self) -> Vec<u8> {
        let mut outbox = self.outbox.borrow_mut();
        let taken: Vec<u8> = outbox.bytes.drain(..).collect();
        outbox.wake_writer();
        taken
    }
}

impl AsyncByteStream for TestByteStream {
    type Error = TestStreamError;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        match self.fault.get() {
            Some(Fault::Broken) => return Poll::Ready(Err(TestStreamError)),
            Some(Fault::Ended) => return Poll::Ready(Ok(0)),
            _ => {}
        }

        let mut inbox = self.inbox.borrow_mut();
        let wanted = buffer.len().min(self.read_cap.get().unwrap_or(usize::MAX));
        let available = inbox.bytes.len().min(wanted);

        if available == 0 {
            // End of stream is reported only once the buffered bytes are gone: a transport
            // that dropped what it still held would end a connection whose last record had
            // already arrived.
            if inbox.shutdown {
                return Poll::Ready(Ok(0));
            }
            inbox.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }

        for slot in buffer.iter_mut().take(available) {
            *slot = inbox.bytes.pop_front().expect("counted above");
        }
        inbox.wake_writer();
        Poll::Ready(Ok(available))
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        match self.fault.get() {
            Some(Fault::Broken) => return Poll::Ready(Err(TestStreamError)),
            Some(Fault::WriteNotNow) => {
                // Once. A refusal that never cleared would be a stalled transport rather than
                // a busy one, and a connection has no way to tell those apart -- which is the
                // point of the contract on `AsyncByteStream`.
                self.fault.set(None);
                cx.waker().wake_by_ref();
                return Poll::Ready(Ok(Written::NotNow));
            }
            _ => {}
        }

        let mut outbox = self.outbox.borrow_mut();
        if outbox.shutdown {
            return Poll::Ready(Err(TestStreamError));
        }

        let room = outbox
            .room()
            .min(self.write_cap.get().unwrap_or(usize::MAX));
        let accepted = bytes.len().min(room);
        if accepted == 0 {
            outbox.writer = Some(cx.waker().clone());
            return Poll::Ready(Ok(Written::NotNow));
        }

        outbox.bytes.extend(bytes[..accepted].iter().copied());
        outbox.wake_reader();
        Poll::Ready(Ok(Written::Accepted(accepted)))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.fault.get() == Some(Fault::Broken) {
            return Poll::Ready(Err(TestStreamError));
        }
        let mut outbox = self.outbox.borrow_mut();
        outbox.shutdown = true;
        // Nothing is discarded: whatever the peer has not read is still there to be read, and
        // only then does its read report end of stream. A shutdown that dropped the queue
        // would lose the connection close it was called to deliver.
        outbox.wake_reader();
        Poll::Ready(Ok(()))
    }
}

/// Two byte streams that deliver to each other.
///
/// Neither is `Send`, which is what proves the layer requires no `Send` bound.
#[must_use]
pub fn stream_pair() -> (TestByteStream, TestByteStream) {
    let to_left = Rc::new(RefCell::new(Pipe::default()));
    let to_right = Rc::new(RefCell::new(Pipe::default()));
    (
        TestByteStream {
            inbox: Rc::clone(&to_left),
            outbox: Rc::clone(&to_right),
            read_cap: Cell::new(None),
            write_cap: Cell::new(None),
            fault: Cell::new(None),
        },
        TestByteStream {
            inbox: to_right,
            outbox: to_left,
            read_cap: Cell::new(None),
            write_cap: Cell::new(None),
            fault: Cell::new(None),
        },
    )
}

/// A clock that reports whatever a test last told it.
///
/// Cloneable and shared, so a test can hold one while a connection holds another and still
/// control both. Nothing here advances on its own: a connection's timestamps are then exactly
/// the values the test chose, which is how "one timescale, not two" is checked rather than
/// hoped for.
#[derive(Clone)]
pub struct TestClock {
    now: Rc<Cell<u64>>,
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TestClock {
    /// A clock starting at a non-zero instant.
    ///
    /// Non-zero because zero is what an uninitialised timestamp looks like, and a clock
    /// starting there makes a connection that never read the clock indistinguishable from one
    /// that did.
    #[must_use]
    pub fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(1_000_000_000)),
        }
    }

    /// Sets the value the clock reports.
    pub fn set(&self, now: Timestamp) {
        self.now.set(now.as_nanos());
    }

    /// Moves the reported value forward.
    ///
    /// Saturating, and forward only: [`Clock`] requires monotonicity, and a harness that could
    /// violate it would be testing something no caller is allowed to do.
    pub fn advance(&self, nanos: u64) {
        self.now.set(self.now.get().saturating_add(nanos));
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Polls once with a waker that does nothing, for the cases where the answer is immediate.
    fn poll_once<T>(f: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        f(&mut cx)
    }

    #[test]
    fn a_read_cap_splits_what_a_single_read_returns() {
        let (mut a, mut b) = stream_pair();
        assert_eq!(
            poll_once(|cx| a.poll_write(cx, b"hello")),
            Poll::Ready(Ok(Written::Accepted(5)))
        );
        b.set_read_cap(Some(2));

        let mut buffer = [0u8; 8];
        assert_eq!(
            poll_once(|cx| b.poll_read(cx, &mut buffer)),
            Poll::Ready(Ok(2))
        );
        assert_eq!(&buffer[..2], b"he");
    }

    #[test]
    fn a_write_cap_splits_what_a_single_write_accepts() {
        let (mut a, _b) = stream_pair();
        a.set_write_cap(Some(3));
        assert_eq!(
            poll_once(|cx| a.poll_write(cx, b"hello")),
            Poll::Ready(Ok(Written::Accepted(3)))
        );
    }

    #[test]
    fn a_broken_stream_fails_every_operation() {
        let (mut a, _b) = stream_pair();
        a.inject(Fault::Broken);
        let mut buffer = [0u8; 4];
        assert!(matches!(
            poll_once(|cx| a.poll_read(cx, &mut buffer)),
            Poll::Ready(Err(TestStreamError))
        ));
        assert!(matches!(
            poll_once(|cx| a.poll_write(cx, b"x")),
            Poll::Ready(Err(TestStreamError))
        ));
    }

    #[test]
    fn an_injected_ending_reports_zero_bytes_read() {
        let (mut a, _b) = stream_pair();
        a.inject(Fault::Ended);
        let mut buffer = [0u8; 4];
        assert_eq!(
            poll_once(|cx| a.poll_read(cx, &mut buffer)),
            Poll::Ready(Ok(0))
        );
    }

    /// The property a shutdown that discarded its queue would break.
    #[test]
    fn a_shutdown_delivers_what_was_already_written_before_ending() {
        let (mut a, mut b) = stream_pair();
        assert_eq!(
            poll_once(|cx| a.poll_write(cx, b"bye")),
            Poll::Ready(Ok(Written::Accepted(3)))
        );
        assert_eq!(poll_once(|cx| a.poll_shutdown(cx)), Poll::Ready(Ok(())));

        let mut buffer = [0u8; 8];
        assert_eq!(
            poll_once(|cx| b.poll_read(cx, &mut buffer)),
            Poll::Ready(Ok(3))
        );
        assert_eq!(&buffer[..3], b"bye");
        assert_eq!(
            poll_once(|cx| b.poll_read(cx, &mut buffer)),
            Poll::Ready(Ok(0)),
            "the ending is reported only once the bytes are gone"
        );
    }

    #[test]
    fn the_clock_only_moves_when_told_to() {
        let clock = TestClock::new();
        let start = clock.now();
        assert_eq!(clock.now(), start, "time passed on its own");
        clock.advance(5);
        assert_eq!(clock.now().as_nanos(), start.as_nanos() + 5);
        clock.set(Timestamp::from_nanos(42));
        assert_eq!(clock.now().as_nanos(), 42);
    }

    #[test]
    fn the_test_byte_stream_is_deliberately_not_send() {
        // Compile-time evidence that this layer imposes no `Send` bound: if it did, the
        // in-memory stream could not be built on `Rc` and this function would not compile.
        fn assert_not_required<T>(_: &T) {}
        let (a, _b) = stream_pair();
        assert_not_required(&a);
    }
}
