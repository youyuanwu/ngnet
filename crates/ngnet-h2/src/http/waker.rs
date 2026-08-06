//! The waker a deferred outgoing body is given.
//!
//! An asynchronous body says "not yet" by returning [`BodyOutcome::Defer`], after which
//! its stream is inert until [`Session::resume_body`] is called for it. Something has to
//! make that call, and the only thing that knows when the body is ready is the body — so
//! it is handed a [`Waker`] that, when invoked, notes the stream in the driver's ready set
//! and asks the driver to run another pass.
//!
//! [`BodyOutcome::Defer`]: crate::BodyOutcome::Defer
//! [`Session::resume_body`]: crate::Session::resume_body
//! [`Waker`]: std::task::Waker

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Weak};
use std::task::Wake;

use super::shared::Shared;

/// Wakes one stream's outgoing body.
///
/// # Why the identifier is written after construction
///
/// The waker has to exist before the body does, because the body captures it — but the
/// stream identifier only exists after submission, which needs the body. The cycle is cut
/// by creating the waker with no identifier and filling it in the moment submission
/// returns one. A wake in that window names nothing and is discarded, which is correct:
/// the body cannot have been consulted yet, so it cannot yet be deferred.
///
/// # Why liveness is a `Weak`
///
/// A `Waker` may be cloned and stored, and invoked long after the body that received it
/// was dropped. De-duplicating the ready set is not enough to bound it, because ids from
/// long-closed streams would still accumulate. The driver keeps one `Arc<()>` per live
/// stream and gives out `Weak` handles; when the stream closes the `Arc` goes and every
/// waker that ever named it becomes inert, without the driver having to find them.
#[derive(Debug)]
pub(crate) struct StreamWaker {
    stream: AtomicI32,
    shared: Arc<Shared>,
    liveness: Weak<()>,
}

impl StreamWaker {
    pub(crate) const fn new(shared: Arc<Shared>, liveness: Weak<()>) -> Self {
        Self {
            stream: AtomicI32::new(0),
            shared,
            liveness,
        }
    }

    /// Names the stream this waker belongs to, once submission has assigned one.
    pub(crate) fn bind(&self, stream: i32) {
        self.stream.store(stream, Ordering::Release);
    }

    /// The stream this waker names, or zero before submission has assigned one.
    ///
    /// Read by the outgoing body bridge, which needs to say which stream a trailing block
    /// belongs to and has no other way to know: the identifier does not exist until after
    /// the body has been handed to the session.
    pub(crate) fn stream(&self) -> i32 {
        self.stream.load(Ordering::Acquire)
    }
}

impl Wake for StreamWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let stream = self.stream.load(Ordering::Acquire);
        if self.shared.mark_ready(stream, &self.liveness) {
            // Only after the note is recorded, and only once the lock protecting it has
            // been released. A wake that was discarded need not disturb the driver at all.
            self.shared.wake_driver();
        }
    }
}
