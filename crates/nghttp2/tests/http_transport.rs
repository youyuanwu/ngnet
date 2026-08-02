//! The transport abstraction's contract, asserted mostly by compiling (Spec FR-013 to
//! FR-016, SC-015 in part).
//!
//! Three properties matter here and none of them is about behaviour, which is why the
//! assertions are largely type-level:
//!
//! * a completion-based transport can be written without mentioning the borrowed-write
//!   path at all;
//! * a readiness-based one can override it, and say so;
//! * neither is required to be `Send`, because the flagship completion runtimes are
//!   thread-per-core and build their I/O on `Rc`. A `Send` bound in the traits would
//!   exclude exactly the runtimes the abstraction exists to serve.

#![cfg(feature = "http")]

use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use nghttp2::http::testing::{block_on, duplex};
use nghttp2::http::{Transport, TransportRead, TransportWrite};

use bytes::{Bytes, BytesMut};

/// A completion-based transport: owns its buffers, ignores the borrowed-write path.
///
/// This is the shape `io_uring`-backed runtimes need, and it compiles without naming
/// `write_borrowed` — the default carries it.
struct Completion {
    written: Vec<u8>,
    to_read: Vec<u8>,
}

struct CompletionReader {
    to_read: Vec<u8>,
}

struct CompletionWriter {
    written: Vec<u8>,
}

impl Transport for Completion {
    type Reader = CompletionReader;
    type Writer = CompletionWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            CompletionReader {
                to_read: self.to_read,
            },
            CompletionWriter {
                written: self.written,
            },
        )
    }
}

impl TransportRead for CompletionReader {
    async fn read(&mut self, mut buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        let take = self.to_read.len().min(buf.capacity().max(1));
        buf.extend_from_slice(&self.to_read[..take]);
        self.to_read.drain(..take);
        (Ok(take), buf)
    }
}

impl TransportWrite for CompletionWriter {
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        self.written.extend_from_slice(&buf);
        let written = buf.len();
        (Ok(written), buf)
    }
}

/// A readiness-based transport: overrides the borrowed path and advertises it.
struct Readiness;

struct ReadinessHalf {
    borrowed: Rc<RefCell<usize>>,
}

impl Transport for Readiness {
    type Reader = ReadinessHalf;
    type Writer = ReadinessHalf;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let counter = Rc::new(RefCell::new(0));
        (
            ReadinessHalf {
                borrowed: Rc::clone(&counter),
            },
            ReadinessHalf { borrowed: counter },
        )
    }
}

impl TransportRead for ReadinessHalf {
    async fn read(&mut self, buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        (Ok(0), buf)
    }
}

impl TransportWrite for ReadinessHalf {
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }

    async fn write_borrowed(&mut self, data: &[u8]) -> io::Result<usize> {
        *self.borrowed.borrow_mut() += 1;
        Ok(data.len())
    }

    fn writes_borrowed(&self) -> bool {
        true
    }
}

#[test]
fn a_completion_transport_needs_no_borrowed_write_path() {
    // Compiling is most of the assertion: `Completion` above never mentions
    // `write_borrowed`. The default still works, which is what lets a completion-based
    // implementation ignore the readiness fast path entirely.
    let (mut reader, mut writer) = Completion {
        written: Vec::new(),
        to_read: b"from the peer".to_vec(),
    }
    .split();

    let (read, buf) = block_on(reader.read(BytesMut::with_capacity(64)));
    assert_eq!(read.unwrap(), b"from the peer".len());
    assert_eq!(&buf[..], b"from the peer");

    let written = block_on(writer.write_borrowed(b"to the peer")).unwrap();
    assert_eq!(written, b"to the peer".len());
    assert_eq!(writer.written, b"to the peer");

    assert!(
        !writer.writes_borrowed(),
        "a transport that has not overridden the borrowed path must not claim it"
    );
}

#[test]
fn a_readiness_transport_can_take_the_zero_copy_path() {
    let (_reader, mut writer) = Readiness.split();

    assert!(
        writer.writes_borrowed(),
        "a transport that overrides the borrowed path should say so, since that is how \
         the connection chooses between coalescing and zero-copy"
    );

    let written = block_on(writer.write_borrowed(b"borrowed")).unwrap();
    assert_eq!(written, b"borrowed".len());
    assert_eq!(
        *writer.borrowed.borrow(),
        1,
        "the override should have been taken, not the default"
    );
}

#[test]
fn a_transport_need_not_be_send() {
    // The property that matters most for Story P4, and the one a `Send` supertrait would
    // have silently destroyed. `ReadinessHalf` holds an `Rc`, so it is not `Send` — and
    // it still satisfies the traits. Thread-per-core completion runtimes look exactly
    // like this.
    fn accepts_any_transport<T: Transport>(_transport: T) {}
    accepts_any_transport(Readiness);

    fn is_send<T: Send>() {}
    is_send::<Completion>();

    // Deliberately *not* `is_send::<Readiness>()`: it need not be, and requiring it is the
    // mistake this test exists to prevent.
    let (reader, _writer) = Readiness.split();
    let not_send: Rc<()> = Rc::new(());
    drop((reader, not_send));
}

#[test]
fn the_in_memory_duplex_carries_bytes_both_ways() {
    // The scaffolding the later phases build on, exercised here so a fault in it is
    // attributed to the transport rather than to whatever is being tested with it.
    let (client, server) = duplex(false);
    let (_client_reader, mut client_writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    block_on(async {
        let (result, _buf) = client_writer.write(Bytes::from_static(b"ping")).await;
        assert_eq!(result.unwrap(), 4);

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), 4);
        assert_eq!(&buf[..], b"ping");
    });
}

#[test]
fn a_closed_duplex_reports_end_of_stream() {
    let (client, server) = duplex(false);
    let (mut server_reader, _sw) = server.split();

    // Dropping the writing half closes the pipe, which is what a peer hanging up looks
    // like. Note the halves must actually be dropped, not merely bound to `_`-prefixed
    // names, which keep them alive to the end of the scope.
    drop(client.split());

    block_on(async {
        let (read, _buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(
            read.unwrap(),
            0,
            "a closed peer should read as end of stream, not hang"
        );
    });
}

#[test]
fn write_counts_stay_observable_across_a_split() {
    // Splitting consumes the transport, so a test that drives a connection can no longer
    // reach it — yet the per-pass write counts are precisely what the later phases must
    // assert. Taking a counter handle first is how that stays possible, and this pins it
    // before anything depends on it.
    let (client, server) = duplex(false);
    let counter = client.write_counter();
    let (_reader, mut writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    assert_eq!(counter.get(), 0, "nothing written yet");

    block_on(async {
        let (result, _buf) = writer.write(Bytes::from_static(b"one")).await;
        result.unwrap();
        let (result, _buf) = writer.write(Bytes::from_static(b"two")).await;
        result.unwrap();

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), 6);
        assert_eq!(&buf[..], b"onetwo");
    });

    assert_eq!(counter.get(), 2, "two writes should have been counted");
    assert_eq!(
        writer.writes(),
        counter.get(),
        "the writer and the handle should agree"
    );

    counter.reset();
    assert_eq!(counter.get(), 0, "resetting lets a single pass be measured");
}

#[test]
fn a_borrowed_write_duplex_takes_the_zero_copy_path_and_still_counts() {
    // The other of the two shapes the in-memory transport can take. Both are used by the
    // later drain-strategy assertions, so both need coverage here rather than one being
    // assumed to work because the other does.
    let (client, server) = duplex(true);
    let counter = client.write_counter();
    let (_reader, mut writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    assert!(
        writer.writes_borrowed(),
        "this shape advertises the zero-copy write path"
    );

    block_on(async {
        writer.write_borrowed(b"borrowed").await.unwrap();

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), b"borrowed".len());
        assert_eq!(&buf[..], b"borrowed");
    });

    assert_eq!(
        counter.get(),
        1,
        "a borrowed write is still a write, and must be counted as one"
    );
}
