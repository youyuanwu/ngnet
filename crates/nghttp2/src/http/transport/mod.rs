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
//!
//! # How a pass gets drained
//!
//! [`TransportWrite`] offers two optional overrides, and between them they select one of
//! three strategies for turning a pass of session output into writes.
//!
//! | elected by | strategy | writes per pass | driver-side copy |
//! | --- | --- | --- | --- |
//! | neither (default) | owned | one | every octet, every pass |
//! | [`write_borrowed`](TransportWrite::write_borrowed) | borrowed | one per block | none |
//! | [`write_vectored`](TransportWrite::write_vectored) | vectored | one, plus one per large block | none of the large blocks |
//!
//! The vectored strategy exists because the first two are each wrong for half of the
//! traffic: under multiplexing a pass is dozens of tiny blocks, where one write per block
//! is the dominant cost, and with a large body it is a handful of 16 KiB blocks, where
//! copying them all to save three syscalls is the dominant cost. Gathering small blocks
//! into a buffer the driver owns while handing large ones to the socket uncopied gets both.
//!
//! `write_vectored` takes precedence where both are overridden. Neither election is a
//! separate flag: each is expressed by returning `Some(future)` from the method that also
//! performs the write, so an implementation cannot claim a path it does not supply.

use core::future::Future;

use bytes::{Bytes, BytesMut};

#[cfg(feature = "completion")]
mod compio;
#[cfg(feature = "tokio")]
mod tokio;

#[cfg(feature = "completion")]
pub use compio::{CompioIo, CompioReader, CompioWriter};
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
    /// This is one of the transport's two says over how the driver drains a pass, and the
    /// decision and the operation are deliberately the same method. Returning `Some` *is*
    /// electing the borrowed path, and the future it carries *is* how that path writes;
    /// returning `None` — the default — leaves the owned path, unless
    /// [`write_vectored`](TransportWrite::write_vectored) elects the vectored one. So an
    /// implementation cannot advertise the fast path without supplying it, nor supply it
    /// without the driver taking it — the two ways a separate flag and method could silently
    /// disagree.
    ///
    /// The owned path coalesces a whole pass into one [`write`](TransportWrite::write): a
    /// syscall saved for a copy of every outgoing octet, which the transport taking
    /// ownership requires. The buffer behind that copy is reused across passes, so it costs
    /// no allocation in steady state. The borrowed path hands each of the session's own
    /// blocks over as it is produced, uncopied — no allocation either, at one write per
    /// block.
    ///
    /// These two cannot be *combined*, and the reason is worth stating precisely because it
    /// is easy to overstate. The session lends one block at a time: asking for the next
    /// invalidates the last, and this crate's [`Session::send`] signature enforces it by
    /// borrowing the session for as long as the block lives. So several session blocks can
    /// never be gathered *with each other* into one write without copying them. What that
    /// does **not** foreclose is gathering one block with memory the driver itself owns,
    /// which is what [`write_vectored`](TransportWrite::write_vectored) offers and how the
    /// vectored path reaches one write per pass without copying large payloads.
    ///
    /// [`Session::send`]: crate::Session::send
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

    /// The gathering write strategy, taken whole or not at all.
    ///
    /// Elected exactly like [`write_borrowed`](TransportWrite::write_borrowed) — returning
    /// `Some` *is* the election and the future it carries *is* the write — and it takes
    /// precedence over it: a transport overriding both gets the vectored path, and
    /// `write_borrowed` becomes its fallback for the case below where the underlying I/O
    /// cannot really gather.
    ///
    /// # What the regions are
    ///
    /// `regions` is a sequence of octet runs to be written **in order**, as one operation,
    /// exactly as `writev` would. The return is the number of octets accepted across the
    /// whole sequence; a short write is normal and the driver re-offers what remains.
    ///
    /// This library's driver never offers more than two regions, because that is all the
    /// block lifetime permits: one live session block beside the driver's own accumulated
    /// octets. The contract itself imposes no count, so an implementation should not assume
    /// two. No region is ever empty.
    ///
    /// # How the election is read
    ///
    /// The driver decides once per pass, by calling this method and inspecting only whether
    /// the result is `Some`. **It may drop that future without ever polling it**, so
    /// constructing the returned future must have no side effect — an implementation that
    /// records or begins the write at construction time will count a write that never
    /// happened. For the same reason the decision must not depend on `regions`: it is a
    /// fixed property of the transport, and the driver may probe it with an empty sequence.
    ///
    /// Returning `None` from a later call in a pass that already elected this path is a
    /// contract violation. The driver tolerates it — it falls back to coalescing the
    /// remainder rather than failing the connection — but the octets get copied, which is
    /// the cost this path exists to avoid.
    ///
    /// # When not to elect it
    ///
    /// A transport whose underlying I/O merely *emulates* gathering — writing the first
    /// region and ignoring the rest, as the default `poll_write_vectored` does — should
    /// return `None` from the start and let one of the other paths run. Electing it there
    /// is worse than not electing it, since each call would then move only one region.
    ///
    /// # Why `Some(())` cannot compile
    ///
    /// As with the borrowed path, electing is inseparable from supplying the write:
    ///
    /// ```compile_fail
    /// use nghttp2::http::transport::TransportWrite;
    /// use nghttp2::http::testing::bytes_crate::Bytes;
    ///
    /// struct GathersNothing;
    /// impl TransportWrite for GathersNothing {
    ///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
    ///         let n = buf.len();
    ///         (Ok(n), buf)
    ///     }
    ///     fn write_vectored<'w>(
    ///         &'w mut self,
    ///         _regions: &'w [std::io::IoSlice<'w>],
    ///     ) -> Option<impl core::future::Future<Output = std::io::Result<usize>> + 'w> {
    ///         Some(()) // claims the vectored path but `()` is not a write future
    ///     }
    /// }
    /// ```
    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [std::io::IoSlice<'w>],
    ) -> Option<impl Future<Output = std::io::Result<usize>> + 'w> {
        let _ = regions;
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
