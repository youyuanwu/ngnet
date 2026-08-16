//! The byte stream a connection runs over, described rather than chosen.
//!
//! # Why a trait at all
//!
//! QMux runs over "an ordered, reliable, bidirectional byte stream", and the draft is
//! deliberate about not saying which one. A TCP socket, a unix socket, a TLS session over
//! either, a pair of pipes and an in-memory buffer are all legitimate substrates, and every
//! async runtime spells reading and writing them differently. Naming one runtime would make
//! the crate unusable from the others; naming all of them would make it depend on all of
//! them.
//!
//! So the connection takes a description of a byte stream and the caller supplies it. A
//! ready-made description for one widely used runtime ships behind an optional feature, and
//! [`crate::io::testing`] contains a second implementation that moves bytes in memory.
//!
//! # Three operations, and why shutdown is one of them
//!
//! Reading and writing are obvious. The write-side shutdown is here because closing a QMux
//! connection is a two-step act: a CONNECTION_CLOSE record has to reach the peer, and *then*
//! the write side of the byte stream has to end so the peer's read reports end-of-stream
//! rather than waiting forever. A description without a shutdown would force the layer to
//! drop the byte stream to end it, which loses the ordering between those two steps -- the
//! close record may still be sitting in the transport's own buffer when the drop discards it.
//!
//! There is deliberately no read-side shutdown. A connection that has stopped reading cannot
//! observe the peer's close, and half the outcomes this layer distinguishes are only
//! reachable by reading to the end.

use core::task::{Context, Poll};

/// What happened to a write.
///
/// A write is not a future, and a byte stream is not obliged to take everything it is
/// offered. Modelling the answer as "this many bytes, which may be fewer than offered" or
/// "none right now" lets the connection decide what to do about a partial accept -- resume
/// from the offset, hold back the next record, try again when woken -- rather than having
/// that decided for it by an await.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Written {
    /// The stream took this many bytes, counted from the start of what was offered.
    ///
    /// May be fewer than were offered, which is the ordinary case for a socket with a
    /// partially full send buffer. The remainder has **not** been written and must be offered
    /// again; a caller that treats a partial accept as a whole one truncates a record, and a
    /// truncated record desynchronises the stream permanently, because QMux has no
    /// resynchronisation point to recover at.
    Accepted(usize),

    /// The stream could take nothing now, and the waker will fire when it can.
    ///
    /// Distinct from `Accepted(0)`, which implementations must not return: zero bytes
    /// accepted carries no obligation to wake, so a caller offered it can only spin. This
    /// variant says the same thing and carries the obligation.
    NotNow,
}

/// An asynchronous, ordered, reliable, bidirectional byte stream.
///
/// # Contract
///
/// Implementations must register the [`Context`]'s waker whenever they return
/// [`Poll::Pending`] or [`Written::NotNow`], and must wake it once the condition clears.
///
/// An implementation that reports it cannot proceed and then never wakes **stalls the
/// connection with no error and no timeout**. The layer cannot detect it, because from here
/// "nothing to do" and "waiting forever" are the same observation: a connection with no bytes
/// to read, nothing left to write and nothing pending is *supposed* to be silent, and this
/// crate enforces no idle timeout that would eventually break the tie (see
/// [`crate::io::Clock`]). The failure presents as a connection that transferred some data and
/// then went quiet, which is among the more expensive things to diagnose from the far end.
///
/// The bytes must arrive in order and without loss or duplication. QMux delegates every
/// reliability concern to this stream: it has no sequence numbers, no retransmission and no
/// resynchronisation point of its own, so a substrate that reorders or drops bytes does not
/// produce a degraded connection but a protocol violation at whichever peer reads the mangled
/// record.
///
/// An error is fatal to the connection. An implementation that can tell a transient failure
/// from a permanent one should absorb the transient ones itself, by returning
/// [`Poll::Pending`] with the waker registered, rather than reporting them here.
pub trait AsyncByteStream {
    /// How this byte stream fails.
    ///
    /// Bounded so it converts into a sendable, shareable boxed error, rather than merely
    /// being printable as the QUIC socket seam has it. The bound comes from above:
    /// `ngnet-h3`'s transport abstraction requires
    /// `Error: Into<Box<dyn core::error::Error + Send + Sync>>` of any transport plugged into
    /// it, and the crate that joins QMux to HTTP/3 has to satisfy that with whatever failure
    /// type reaches it from here. Discovering the mismatch there rather than here would mean
    /// changing this trait after callers had implemented it.
    ///
    /// This constrains the *error* only. Neither the stream nor the clock carries a `Send`
    /// bound, so a thread-per-core runtime whose sockets are built on `Rc` remains welcome;
    /// what it must supply is an error value that can be moved between threads once produced,
    /// which is true of `std::io::Error` and of essentially every failure type in practice.
    type Error: Into<Box<dyn core::error::Error + Send + Sync>>;

    /// Attempts to read into `buffer`, reporting how many bytes were filled.
    ///
    /// `Ok(0)` means **end of stream**: the peer will send nothing further, and the
    /// connection classifies that as a clean or a truncated ending depending on whether it
    /// stands at a record boundary. It does not mean "nothing available right now", which is
    /// [`Poll::Pending`]; an implementation that confuses the two ends connections that were
    /// merely idle. A read into an empty `buffer` may report `Ok(0)` without meaning end of
    /// stream, which is why the layer never issues one.
    ///
    /// Returns [`Poll::Pending`] with the waker registered when nothing has arrived yet.
    ///
    /// # Errors
    ///
    /// A returned error is fatal to the connection.
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;

    /// Attempts to write `bytes`, reporting how many were accepted.
    ///
    /// Returning [`Written::NotNow`] means nothing was written and the same bytes must be
    /// offered again once the waker fires. Returning [`Written::Accepted`] with fewer bytes
    /// than were offered means the remainder must be offered again before anything else is:
    /// a record interleaved with the tail of its predecessor is not a record the peer can
    /// parse.
    ///
    /// # Errors
    ///
    /// A returned error is fatal to the connection.
    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<Written, Self::Error>>;

    /// Ends the write side, once everything already accepted has been handed on.
    ///
    /// Called after a connection close has been written, so the peer's read reports end of
    /// stream instead of waiting for bytes that will never come. An implementation that
    /// buffers must flush what it holds before reporting readiness, or the close it was asked
    /// to deliver is discarded by the shutdown that was meant to follow it.
    ///
    /// The read side stays open: the peer may still be sending, and a connection that stopped
    /// reading here would never see the peer's own close.
    ///
    /// # Errors
    ///
    /// A returned error is fatal to the connection.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
