//! The runtime seam, exercised through its in-memory implementation.
//!
//! Four properties, each of which a connection built on this seam depends on and none of
//! which is visible from the trait definition alone: that bytes cross in both directions,
//! that a parked reader is woken when its peer writes, that a writer which can take nothing
//! says so and is woken when the peer drains, and that the end of a stream is reported as
//! zero bytes rather than as an error.
//!
//! The waker is a real one -- an `Arc` implementing [`Wake`] that records whether it fired --
//! rather than [`Waker::noop`]. That is the whole point of the middle two tests: a harness
//! that polled with a no-op waker would pass them while proving nothing about the obligation
//! [`AsyncByteStream`] places on an implementation, which is precisely the obligation whose
//! breach stalls a connection silently.
//!
//! Nothing here needs an async runtime, or an executor of any kind. The futures are polled by
//! hand, which is evidence for the claim the layer makes about itself.

#![cfg(feature = "io")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ngnet_qmux::io::testing::{Fault, TestByteStream, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Written};

/// A waker that counts how many times it was woken.
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl CountingWaker {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }

    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Writes `bytes` in full, resuming across partial accepts, and returns the number of polls.
fn write_all(stream: &mut TestByteStream, cx: &mut Context<'_>, bytes: &[u8]) -> usize {
    let mut offset = 0;
    let mut polls = 0;
    while offset < bytes.len() {
        polls += 1;
        match stream.poll_write(cx, &bytes[offset..]) {
            Poll::Ready(Ok(Written::Accepted(n))) => offset += n,
            Poll::Ready(Ok(Written::NotNow)) => {}
            other => panic!("the write failed: {other:?}"),
        }
        assert!(polls < 1024, "the write made no progress");
    }
    polls
}

/// Reads until `wanted` bytes have arrived, resuming across short reads.
fn read_exact(stream: &mut TestByteStream, cx: &mut Context<'_>, wanted: usize) -> Vec<u8> {
    let mut collected = Vec::with_capacity(wanted);
    let mut buffer = [0u8; 64];
    while collected.len() < wanted {
        match stream.poll_read(cx, &mut buffer) {
            Poll::Ready(Ok(0)) => panic!("the stream ended early"),
            Poll::Ready(Ok(n)) => collected.extend_from_slice(&buffer[..n]),
            other => panic!("the read did not complete: {other:?}"),
        }
    }
    collected
}

#[test]
fn the_pair_transports_bytes_in_both_directions() {
    let waker = Waker::from(CountingWaker::new());
    let mut cx = Context::from_waker(&waker);
    let (mut client, mut server) = stream_pair();

    write_all(&mut client, &mut cx, b"a record from the client");
    assert_eq!(
        read_exact(&mut server, &mut cx, 24),
        b"a record from the client"
    );

    write_all(&mut server, &mut cx, b"and one back");
    assert_eq!(read_exact(&mut client, &mut cx, 12), b"and one back");

    // The same exchange with both directions capped to one byte per call, which is the shape
    // a real socket takes under pressure and the one that catches a caller assuming a read
    // returns whole records.
    client.set_write_cap(Some(1));
    server.set_read_cap(Some(1));
    let polls = write_all(&mut client, &mut cx, b"split");
    assert_eq!(polls, 5, "a capped write must be resumed once per byte");
    assert_eq!(read_exact(&mut server, &mut cx, 5), b"split");
}

#[test]
fn a_parked_reader_is_woken_when_its_peer_writes() {
    let counter = CountingWaker::new();
    let waker = Waker::from(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    let (mut client, mut server) = stream_pair();

    let mut buffer = [0u8; 16];
    assert!(
        server.poll_read(&mut cx, &mut buffer).is_pending(),
        "an empty stream must park rather than report end of stream"
    );
    assert_eq!(counter.wakes(), 0, "parking must not wake by itself");

    write_all(&mut client, &mut cx, b"wake up");
    assert_eq!(
        counter.wakes(),
        1,
        "the reader registered a waker and the write must fire it"
    );
    assert_eq!(
        server.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(7)),
        "the bytes are there once the reader is woken"
    );
}

#[test]
fn a_writer_that_can_accept_nothing_says_so_and_is_woken_when_drained() {
    let counter = CountingWaker::new();
    let waker = Waker::from(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    let (mut client, mut server) = stream_pair();

    client.set_capacity(Some(4));
    assert_eq!(
        client.poll_write(&mut cx, b"12345678"),
        Poll::Ready(Ok(Written::Accepted(4))),
        "a full pipe takes what fits and no more"
    );
    assert_eq!(
        client.poll_write(&mut cx, b"5678"),
        Poll::Ready(Ok(Written::NotNow)),
        "a pipe with no room reports it rather than accepting zero bytes"
    );
    assert_eq!(
        counter.wakes(),
        0,
        "a genuinely blocked writer is woken by the drain, not by a courtesy wake"
    );

    let drained = read_exact(&mut server, &mut cx, 4);
    assert_eq!(drained, b"1234");
    assert_eq!(counter.wakes(), 1, "draining must wake the blocked writer");

    assert_eq!(
        client.poll_write(&mut cx, b"5678"),
        Poll::Ready(Ok(Written::Accepted(4))),
        "the write resumes with the bytes that were refused"
    );
    assert_eq!(read_exact(&mut server, &mut cx, 4), b"5678");

    // The one-shot refusal is the other half of the contract: it clears itself and wakes
    // immediately, so a caller that retries makes progress and one that mistook `NotNow` for
    // success would have dropped the bytes.
    client.inject(Fault::WriteNotNow);
    let before = counter.wakes();
    assert_eq!(
        client.poll_write(&mut cx, b"once"),
        Poll::Ready(Ok(Written::NotNow))
    );
    assert_eq!(counter.wakes(), before + 1, "a transient refusal wakes now");
    assert_eq!(
        client.poll_write(&mut cx, b"once"),
        Poll::Ready(Ok(Written::Accepted(4)))
    );
}

#[test]
fn the_end_of_a_stream_is_reported_as_zero_bytes_read() {
    let waker = Waker::from(CountingWaker::new());
    let mut cx = Context::from_waker(&waker);
    let (mut client, mut server) = stream_pair();

    write_all(&mut client, &mut cx, b"last");
    assert_eq!(client.poll_shutdown(&mut cx), Poll::Ready(Ok(())));

    // Ordering, which is the property a connection close depends on: everything already
    // written is delivered first, and only then does the ending appear.
    let mut buffer = [0u8; 16];
    assert_eq!(server.poll_read(&mut cx, &mut buffer), Poll::Ready(Ok(4)));
    assert_eq!(&buffer[..4], b"last");
    assert_eq!(
        server.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(0)),
        "end of stream is zero bytes read, not an error and not Pending"
    );
    assert_eq!(
        server.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(0)),
        "and it stays reported, so a caller that polls again is not told to wait forever"
    );

    // An ending injected without a shutdown, which is how a test reaches the truncated case:
    // the peer stops mid-record and the reader has no way to know from the bytes alone.
    let (mut a, mut b) = stream_pair();
    write_all(&mut a, &mut cx, b"half a rec");
    b.inject(Fault::Ended);
    assert_eq!(
        b.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(0)),
        "an injected ending reports itself the same way a real one does"
    );
}

#[test]
fn the_seam_needs_no_send_and_its_error_still_boxes() {
    // Two claims that pull in opposite directions and both hold: the stream itself is not
    // `Send`, and the failure it reports converts into a sendable, shareable box -- which is
    // the bound the HTTP/3 transport abstraction will require of whatever reaches it.
    fn accepts_any_stream<S: AsyncByteStream>(_: &S) {}
    fn boxes<S: AsyncByteStream>(error: S::Error) -> Box<dyn core::error::Error + Send + Sync> {
        error.into()
    }

    let waker = Waker::from(CountingWaker::new());
    let mut cx = Context::from_waker(&waker);
    let (mut a, _b) = stream_pair();
    accepts_any_stream(&a);

    a.inject(Fault::Broken);
    let mut buffer = [0u8; 4];
    let Poll::Ready(Err(error)) = a.poll_read(&mut cx, &mut buffer) else {
        panic!("a broken stream must report its failure");
    };
    let boxed = boxes::<TestByteStream>(error);
    assert!(boxed.to_string().contains("broken deliberately"));
}
