//! Scaffolding for exercising the async layer, public but hidden.
//!
//! The crate takes no dev-dependencies by design, and integration tests are separate
//! crates that cannot reach `cfg(test)` items — so the machinery the tests need lives
//! here, marked `#[doc(hidden)]` to keep it out of the documented surface. It is not a
//! supported API and carries no compatibility promise.

use core::future::Future;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use bytes::{Bytes, BytesMut};

use super::transport::{Transport, TransportRead, TransportWrite};

/// Wakes a parked [`block_on`].
struct Unparker {
    woken: Mutex<bool>,
    signal: Condvar,
}

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().expect("wake flag") = true;
        self.signal.notify_one();
    }
}

/// Drives a future to completion on the calling thread.
///
/// A real waker rather than a no-op one, so a future that returns `Pending` genuinely
/// waits instead of being polled in a spin — which matters here, since several of the
/// properties under test are about *not* being polled.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let unparker = Arc::new(Unparker {
        woken: Mutex::new(false),
        signal: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&unparker));
    let mut context = Context::from_waker(&waker);

    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }

        let mut woken = unparker.woken.lock().expect("wake flag");
        while !*woken {
            woken = unparker.signal.wait(woken).expect("waiting for a wake");
        }
        *woken = false;
    }
}

/// One direction of an in-memory connection.
#[derive(Debug, Default)]
struct Pipe {
    bytes: VecDeque<u8>,
    closed: bool,
    waker: Option<Waker>,
}

impl Pipe {
    fn put(&mut self, data: &[u8]) {
        self.bytes.extend(data.iter().copied());
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn close(&mut self) {
        self.closed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// A transport wired directly to a peer, with no socket in between.
///
/// Reading blocks until the peer writes, so a test that deadlocks fails by hanging rather
/// than by silently reading zero and treating it as a clean close.
#[derive(Debug)]
pub struct Duplex {
    incoming: Arc<Mutex<Pipe>>,
    outgoing: Arc<Mutex<Pipe>>,
    /// Set when this transport overrides the borrowed-write path, so both drain
    /// strategies can be exercised against the same in-memory plumbing.
    borrowed_writes: bool,
    writes: Arc<Mutex<usize>>,
}

/// Creates a connected pair.
///
/// `borrowed_writes` selects which write path each side advertises, so a test can cover
/// the coalescing and zero-copy strategies without a second transport implementation.
pub fn duplex(borrowed_writes: bool) -> (Duplex, Duplex) {
    let one = Arc::new(Mutex::new(Pipe::default()));
    let two = Arc::new(Mutex::new(Pipe::default()));

    (
        Duplex {
            incoming: Arc::clone(&one),
            outgoing: Arc::clone(&two),
            borrowed_writes,
            writes: Arc::new(Mutex::new(0)),
        },
        Duplex {
            incoming: two,
            outgoing: one,
            borrowed_writes,
            writes: Arc::new(Mutex::new(0)),
        },
    )
}

impl Duplex {
    /// How many writes this half has issued.
    pub fn writes(&self) -> usize {
        *self.writes.lock().expect("write count")
    }

    /// A handle that keeps observing the write count after the transport is split.
    ///
    /// [`Transport::split`] consumes the transport, so a test driving a connection can no
    /// longer reach it — but the per-pass write counts are exactly what the later phases
    /// must assert. Taking a handle first is how that count stays observable.
    pub fn write_counter(&self) -> WriteCounter {
        WriteCounter {
            writes: Arc::clone(&self.writes),
        }
    }

    /// Signals end of stream to the peer.
    pub fn close(&self) {
        self.outgoing.lock().expect("outgoing pipe").close();
    }
}

/// Observes how many writes a transport has issued, across a split.
#[derive(Debug, Clone)]
pub struct WriteCounter {
    writes: Arc<Mutex<usize>>,
}

impl WriteCounter {
    /// Writes issued so far.
    pub fn get(&self) -> usize {
        *self.writes.lock().expect("write count")
    }

    /// Resets the count, so a test can measure one driver pass at a time.
    pub fn reset(&self) {
        *self.writes.lock().expect("write count") = 0;
    }
}

/// The reading half of a [`Duplex`].
#[derive(Debug)]
pub struct DuplexReader {
    incoming: Arc<Mutex<Pipe>>,
}

/// The writing half of a [`Duplex`].
#[derive(Debug)]
pub struct DuplexWriter {
    outgoing: Arc<Mutex<Pipe>>,
    borrowed_writes: bool,
    writes: Arc<Mutex<usize>>,
}

impl Transport for Duplex {
    type Reader = DuplexReader;
    type Writer = DuplexWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            DuplexReader {
                incoming: self.incoming,
            },
            DuplexWriter {
                outgoing: self.outgoing,
                borrowed_writes: self.borrowed_writes,
                writes: self.writes,
            },
        )
    }
}

impl TransportRead for DuplexReader {
    fn read(&mut self, mut buf: BytesMut) -> impl Future<Output = (io::Result<usize>, BytesMut)> {
        let incoming = Arc::clone(&self.incoming);
        async move {
            // Wait for something to read, or for the peer to close. Parking here rather
            // than returning zero is deliberate: a test that deadlocks should hang and
            // fail, not quietly look like a clean shutdown.
            let available = core::future::poll_fn(|cx: &mut Context<'_>| {
                let mut pipe = incoming.lock().expect("incoming pipe");
                if pipe.bytes.is_empty() {
                    if pipe.closed {
                        return Poll::Ready(0usize);
                    }
                    pipe.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(pipe.bytes.len())
            })
            .await;

            if available == 0 {
                return (Ok(0), buf);
            }

            let room = buf.capacity().saturating_sub(buf.len()).max(1);
            let take = available.min(room);
            let chunk: Vec<u8> = incoming
                .lock()
                .expect("incoming pipe")
                .bytes
                .drain(..take)
                .collect();
            buf.extend_from_slice(&chunk);
            (Ok(take), buf)
        }
    }
}

impl DuplexWriter {
    /// Writes issued by this half.
    pub fn writes(&self) -> usize {
        *self.writes.lock().expect("write count")
    }
}

impl Drop for DuplexWriter {
    /// Closing on drop is what lets a test model a peer hanging up, and is what a real
    /// socket does.
    fn drop(&mut self) {
        self.outgoing.lock().expect("outgoing pipe").close();
    }
}

impl TransportWrite for DuplexWriter {
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (io::Result<usize>, Bytes)> {
        *self.writes.lock().expect("write count") += 1;
        self.outgoing.lock().expect("outgoing pipe").put(&buf);
        let written = buf.len();
        core::future::ready((Ok(written), buf))
    }

    fn write_borrowed(&mut self, data: &[u8]) -> impl Future<Output = io::Result<usize>> {
        *self.writes.lock().expect("write count") += 1;
        self.outgoing.lock().expect("outgoing pipe").put(data);
        core::future::ready(Ok(data.len()))
    }

    fn writes_borrowed(&self) -> bool {
        self.borrowed_writes
    }
}
