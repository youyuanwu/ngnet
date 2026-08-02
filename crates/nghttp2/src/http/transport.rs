//! The byte transport an asynchronous connection runs over.
//!
//! # Why the traits are shaped this way
//!
//! Two families of async I/O exist and they disagree about who owns the buffer.
//! Readiness-based APIs — tokio, `futures-io` — lend a borrowed buffer for the duration of
//! a call. Completion-based APIs — `io_uring`, IOCP, and the runtimes built on them — hand
//! the buffer to the kernel, which may still be writing into it after the future that
//! submitted the operation is dropped; they must therefore *own* it.
//!
//! Completion-shaped traits are the superset. A readiness-based transport implements them
//! with no copy at all: take the buffer, read into it, hand it back. The reverse does not
//! work — a completion-based transport behind a borrowed-buffer trait needs a stable
//! buffer of its own plus a copy out of it. So ownership is transferred here, and the
//! readiness case is served by an override rather than by compromising the shape.
//!
//! # What is deliberately absent
//!
//! There is no [`Send`] bound anywhere in these traits. The flagship completion runtimes
//! are thread-per-core and build their I/O on `Rc`, so requiring `Send` would exclude the
//! very runtimes this abstraction exists to serve — a worse outcome than being unable to
//! spawn on a work-stealing executor. Auto traits propagate instead: a connection over a
//! transport that happens to be `Send` is itself `Send`, which is what `spawn` needs, and
//! nothing has to be declared for that to hold.
//!
//! These traits are not object-safe, because the methods return `impl Future`. Transports
//! are taken generically; a boxed transport is out of scope.
//!
//! # Splitting
//!
//! A connection reads and writes at the same time — that is the point of multiplexing —
//! so the two directions must be able to hold separate borrows. [`Transport::split`]
//! divides a transport once, at construction, into halves that proceed independently. A
//! transport that genuinely cannot be split may implement `split` over a shared cell, but
//! that is a serialising fallback: it reintroduces exactly the head-of-line stall the
//! split exists to prevent, and an `Rc`-based cell additionally makes both halves
//! non-`Send`.

use core::future::Future;

use bytes::{Bytes, BytesMut};

/// A connected byte stream, divisible into independent halves.
pub trait Transport {
    /// The reading half.
    type Reader: TransportRead;
    /// The writing half.
    type Writer: TransportWrite;

    /// Divides the transport so both directions can proceed at once.
    fn split(self) -> (Self::Reader, Self::Writer);
}

/// The reading half of a transport.
pub trait TransportRead {
    /// Reads into `buf`, returning it along with how many octets were read.
    ///
    /// Ownership of the buffer passes to the implementation for the duration of the call
    /// and comes back with it, which is what lets a completion-based implementation keep
    /// the buffer stable while the kernel writes into it. Returning zero octets means the
    /// peer closed.
    ///
    /// The buffer is returned even on failure, so a caller never loses it to an error.
    fn read(&mut self, buf: BytesMut) -> impl Future<Output = (std::io::Result<usize>, BytesMut)>;
}

/// The writing half of a transport.
pub trait TransportWrite {
    /// Writes `buf`, returning it along with how many octets were written.
    ///
    /// As with [`TransportRead::read`], ownership passes in and comes back, and comes back
    /// even on failure.
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)>;

    /// Writes borrowed octets, for implementations that do not need to own them.
    ///
    /// A readiness-based transport should override this, and say so through
    /// [`TransportWrite::writes_borrowed`]. Doing so lets the connection hand over the
    /// session's own output directly, avoiding the copy into an owned buffer that
    /// [`TransportWrite::write`] would otherwise require — and that copy is the only cost
    /// the completion-shaped design imposes on a readiness-based runtime.
    ///
    /// The default copies and delegates, so a completion-based implementation may ignore
    /// this method entirely.
    fn write_borrowed(&mut self, data: &[u8]) -> impl Future<Output = std::io::Result<usize>> {
        async move {
            let (result, _buf) = self.write(Bytes::copy_from_slice(data)).await;
            result
        }
    }

    /// Whether this implementation writes borrowed octets without copying.
    ///
    /// The connection asks before deciding how to drain the session: an implementation
    /// that has overridden [`TransportWrite::write_borrowed`] is handed each block as it
    /// is produced, while one that has not gets a single coalesced write per pass. The two
    /// are mutually exclusive — the session invalidates each block when the next is
    /// requested, so blocks cannot be gathered without copying them — and this is how a
    /// transport states which trade it prefers.
    fn writes_borrowed(&self) -> bool {
        false
    }
}
