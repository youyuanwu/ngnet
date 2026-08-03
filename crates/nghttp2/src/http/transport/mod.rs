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

#[cfg(feature = "completion")]
mod compio;
#[cfg(feature = "tokio")]
mod tokio;

#[cfg(feature = "completion")]
pub use compio::{CompioHalf, CompioIo};
#[cfg(feature = "tokio")]
pub use tokio::{TokioIo, TokioReader, TokioWriter};

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
    /// Reads by *appending* into `buf`, returning it grown by the octets read.
    ///
    /// The buffer is a growable [`BytesMut`]; an implementation reads into its spare
    /// capacity and leaves whatever was already there untouched. The returned `usize` is
    /// an **end-of-input indicator only**: `Ok(0)` means the peer closed, and any nonzero
    /// value means at least one octet was appended — the driver reads the octets from the
    /// buffer itself, by how much it grew, not from this count. Returning a nonzero count
    /// without growing the buffer by that many octets is a contract violation the driver
    /// checks in debug builds.
    ///
    /// Ownership of the buffer passes to the implementation for the duration of the call
    /// and comes back with it, which is what lets a completion-based implementation keep
    /// the buffer stable while the kernel writes into it.
    ///
    /// The buffer is returned even on failure, so a caller never loses it to an error.
    fn read(&mut self, buf: BytesMut) -> impl Future<Output = (std::io::Result<usize>, BytesMut)>;
}

/// The writing half of a transport.
pub trait TransportWrite {
    /// Writes `buf`, returning it along with how many octets were written.
    ///
    /// "Written" does not have to mean "handed to the peer" the instant this returns: a
    /// buffering transport may hold the octets, so long as it releases them no later than
    /// [`commit`](TransportWrite::commit), which the driver calls before it waits on the
    /// peer.
    ///
    /// As with [`TransportRead::read`], ownership passes in and comes back, and comes back
    /// even on failure.
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)>;

    /// The zero-copy write strategy, taken whole or not at all.
    ///
    /// This is the transport's one say over how the driver drains a pass, and the decision
    /// and the operation are deliberately the same method. Returning `Some` *is* electing
    /// the borrowed path, and the future it carries *is* how that path writes; returning
    /// `None` — the default — leaves the owned path. So an implementation cannot advertise
    /// the fast path without supplying it, nor supply it without the driver taking it —
    /// the two ways a separate flag and method could silently disagree.
    ///
    /// The owned path coalesces a whole pass into one [`write`](TransportWrite::write): a
    /// syscall saved for an allocation and a copy of every outgoing octet, every pass. The
    /// borrowed path hands each of the session's own blocks over as it is produced,
    /// uncopied — a few small writes per pass for zero allocation, and the only path on
    /// which steady-state allocation reaches zero. The two are exclusive by construction:
    /// the session invalidates each block when the next is asked for, so blocks cannot be
    /// gathered into one write without copying them, which is why a single method chooses
    /// between the paths rather than the driver combining them.
    ///
    /// A completion-based transport cannot lend the kernel a borrowed buffer and so leaves
    /// this at its default; a readiness-based one overrides it. The choice must not depend
    /// on `data` — it is a fixed property of the transport, which the driver reads once per
    /// pass and holds for the rest of it.
    ///
    /// The two ways the earlier split form (a boolean `writes_borrowed` plus a separately
    /// overridable `write_borrowed`) could silently disagree are now compile errors. There
    /// is no boolean to set out of step with the method:
    ///
    /// ```compile_fail
    /// use nghttp2::http::transport::TransportWrite;
    /// use nghttp2::http::testing::bytes_crate::Bytes;
    ///
    /// struct SplitBrain;
    /// impl TransportWrite for SplitBrain {
    ///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
    ///         let n = buf.len();
    ///         (Ok(n), buf)
    ///     }
    ///     // No such method any more: the decision is not a separate override point.
    ///     fn writes_borrowed(&self) -> bool {
    ///         true
    ///     }
    /// }
    /// ```
    ///
    /// And electing the fast path is inseparable from supplying the write it runs — `Some`
    /// must carry a real future, so an implementation cannot claim the path without a write:
    ///
    /// ```compile_fail
    /// use nghttp2::http::transport::TransportWrite;
    /// use nghttp2::http::testing::bytes_crate::Bytes;
    ///
    /// struct ClaimsWithoutWriting;
    /// impl TransportWrite for ClaimsWithoutWriting {
    ///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
    ///         let n = buf.len();
    ///         (Ok(n), buf)
    ///     }
    ///     fn write_borrowed<'w>(
    ///         &'w mut self,
    ///         _data: &'w [u8],
    ///     ) -> Option<impl core::future::Future<Output = std::io::Result<usize>> + 'w> {
    ///         Some(()) // claims the borrowed path but `()` is not a write future
    ///     }
    /// }
    /// ```
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = std::io::Result<usize>> + 'w> {
        let _ = data;
        None::<core::future::Ready<std::io::Result<usize>>>
    }

    /// Commits everything written so far to the peer-visible byte stream.
    ///
    /// The driver guarantees it calls this once it has drained a write pass and before it
    /// parks awaiting readable input: it never waits on the peer while octets it has
    /// produced are still sitting in a transport-side buffer. An implementation whose
    /// writes are peer-visible the moment [`write`](TransportWrite::write) returns — a raw
    /// socket, a completion transport, the in-memory duplex — has nothing to do here, which
    /// is why the default does nothing. One that buffers, such as a `BufWriter` or a
    /// `BufStream`, must flush that buffer here; otherwise the driver awaits a response to a
    /// request the peer never received, and the connection silently hangs.
    fn commit(&mut self) -> impl Future<Output = std::io::Result<()>> {
        async { Ok(()) }
    }
}
