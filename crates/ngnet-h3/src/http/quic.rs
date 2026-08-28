//! The QUIC connection an asynchronous HTTP/3 connection runs over.
//!
//! # Why this is not a byte transport
//!
//! `ngnet-h2` runs over one ordered byte stream, so its transport abstraction is a reader
//! and a writer. HTTP/3 has none of that shape. The caller owns a QUIC connection carrying
//! many streams; three unidirectional ones must be opened and bound before a single request
//! can move; the peer opens three of its own; each request is a bidirectional stream; and
//! either end can abandon either direction independently. There is no single byte sequence
//! to read or write, so there is no reader and no writer.
//!
//! # Why reads are one event stream and writes are pulled
//!
//! Both halves of this trait are shaped by what four real QUIC libraries can actually do,
//! not by what one of them made convenient.
//!
//! **Reads are a single connection-level event stream.** [`QuicConnection::poll_event`]
//! yields the next thing that happened on any stream. That is the natural shape for a
//! callback-driven library — msquic and ngtcp2 both deliver exactly this — and a
//! stream-oriented library reaches it by demultiplexing, which it must do internally
//! anyway. The alternative, a per-stream poll, would force the driver to hold N futures or
//! to spawn, and this layer does neither.
//!
//! **Writes are pulled by the transport, not pushed at it.** When the transport has room it
//! calls [`StreamSource::write_next`], which offers it the next stream the HTTP/3 state
//! machine wants to write. The obvious alternative — `write(stream, bytes) -> accepted` —
//! looks natural only because it is the shape of one library. It is incompatible with
//! ngtcp2, which is the QUIC library nghttp3 was designed alongside: ngtcp2 fills a *packet*
//! and asks the application for stream data as it goes, so a push-shaped adapter would have
//! to queue and copy every outgoing byte. That copy would defeat the retain contract, which
//! is the entire reason [`QuicEvent::Released`] exists. Pulling costs the other libraries
//! nothing: each becomes a loop around `write_next`.
//!
//! # The retain contract, which is memory-safety-critical
//!
//! nghttp3 does not copy outgoing body data. It borrows the application's buffers and reads
//! through them on every write until the application says they may be freed, and
//! [`QuicEvent::Released`] is how a transport says so. Two kinds of transport answer this
//! differently, and getting it wrong is a use-after-free:
//!
//! - A transport that **copies** what it is given — quinn does, and quiche and s2n-quic have
//!   no choice — owns the bytes the moment the write returns, so it may report `Released`
//!   immediately.
//! - A transport that **borrows** them must wait for the peer to acknowledge. msquic with
//!   send buffering disabled works this way, and ngtcp2's `acked_stream_data_offset`
//!   callback is precisely this event.
//!
//! Which one an implementation is, is declared by [`QuicConnection::RETAINS_BUFFERS`] rather
//! than left to a comment, so that the two cannot be confused by someone reusing the shape
//! of one adapter to write another.
//!
//! # What is deliberately absent
//!
//! **The handshake.** This trait begins with an established connection. No endpoint, TLS
//! configuration, certificate, private key or ALPN identifier appears anywhere in it, and
//! none of those concerns reaches this crate. Which QUIC library to use, how to authenticate
//! the peer and how to negotiate `h3` are the caller's, and they are decided before anything
//! here is called.
//!
//! **A [`Send`] bound.** There is none on the connection or its streams, because the
//! thread-per-core runtimes this abstraction exists to accommodate build their I/O on `Rc`.
//! Auto traits propagate instead: a driver over a `Send` backend is `Send` without anything
//! declaring it. Two honest qualifications: [`QuicConnection::Error`] must be convertible
//! into a `Send + Sync` boxed error, because it becomes the source of an error that crosses
//! task boundaries; and a library whose native callbacks run on its own threads will need
//! internal synchronisation whatever this trait asks for.
//!
//! **Timers.** quiche and ngtcp2 both require their caller to arm and fire a timer. This
//! trait has no timer concept, so an implementation over either owns one behind
//! `poll_event`. That is a real cost and it is deliberate: a timer here would be dead weight
//! for the libraries that manage their own.
//!
//! **Datagrams, stream priority and stream-limit signalling.** All four libraries expose
//! some of these and HTTP/3 has uses for each. They are out of scope for this trait today.

use core::task::{Context, Poll};
use std::io::IoSlice;

use bytes::Bytes;

use crate::error::ErrorCode;
use crate::stream::StreamId;

/// A monotonic timestamp, in nanoseconds.
///
/// Re-exported from the sans-I/O core, where [`crate::Conn::read_stream`] requires one on
/// every call. It is the backend's job to supply it — see [`QuicConnection::now`].
pub use crate::conn::Timestamp;

/// Something that happened on the connection.
///
/// Delivered one at a time by [`QuicConnection::poll_event`], from any stream. The variants
/// are the union of what the HTTP/3 layer must know about; an implementation that has no
/// source for one simply never produces it.
#[non_exhaustive]
#[derive(Debug)]
pub enum QuicEvent {
    /// Bytes arrived on a stream.
    ///
    /// A zero-length `bytes` with `fin` set is legal and must be delivered: it is how a
    /// stream ends without carrying a final byte, and several QUIC libraries signal
    /// end-of-stream exactly that way.
    ///
    /// Peer-opened *unidirectional* streams need no event of their own. Their bytes arrive
    /// here like any others, and nghttp3 reads the HTTP/3 stream-type prefix itself to
    /// discover whether it is looking at the peer's control stream or one of its QPACK
    /// streams.
    Data {
        /// The stream the bytes arrived on.
        stream: StreamId,
        /// The bytes. Owned, because the layer holds them while the state machine reads.
        bytes: Bytes,
        /// Whether these are the last bytes the peer will send on this stream.
        fin: bool,
    },

    /// The peer opened a bidirectional stream.
    ///
    /// Emitted only once the implementation is ready to be written to on that stream. An
    /// implementation that has to keep a handle to write with must store it *before*
    /// producing this event: the layer will answer on the stream, and a handle dropped in
    /// the meantime resets it under several QUIC libraries.
    Accepted {
        /// The newly accepted stream.
        stream: StreamId,
    },

    /// Bytes previously written to a stream may now be freed.
    ///
    /// The number of bytes is a *delta*, not a cumulative offset. See the module
    /// documentation for when an implementation may emit this — the answer depends on
    /// [`QuicConnection::RETAINS_BUFFERS`] and getting it wrong is a use-after-free.
    ///
    /// **Every byte reported as accepted must eventually be released, exactly once.** An
    /// implementation that under-reports holds the application's buffers for the life of the
    /// connection; one that over-reports tells the state machine more was acknowledged than
    /// was ever written, which releases a buffer nghttp3 may still be reading through. The
    /// sum of the deltas for a stream must never exceed what that stream accepted.
    Released {
        /// The stream the bytes were written to.
        stream: StreamId,
        /// How many further bytes may be freed.
        bytes: u64,
        /// Whether the bytes actually reached the peer.
        ///
        /// `false` means the buffer is the application's again but the data was cancelled —
        /// msquic reports exactly this. Such bytes are released without being reported to
        /// the state machine as acknowledged, because telling it they arrived when they did
        /// not is a protocol lie.
        delivered: bool,
    },

    /// The peer asked this endpoint to stop sending on a stream.
    StopSending {
        /// The stream the peer no longer wants.
        stream: StreamId,
        /// The application error code the peer supplied.
        code: ErrorCode,
    },

    /// The peer reset a stream, abandoning what it had left to send.
    Reset {
        /// The stream the peer abandoned.
        stream: StreamId,
        /// The application error code the peer supplied.
        code: ErrorCode,
    },

    /// A stream is finished in both directions and its state may be released.
    ///
    /// Carries both application error codes because the HTTP/3 state machine wants them per
    /// direction, and because an implementation keeping per-stream handles needs a defined
    /// point at which to drop them. Without this event such a map only ever grows.
    StreamClosed {
        /// The stream that closed.
        stream: StreamId,
        /// The error code for the receiving direction, if it ended with one.
        rx_code: Option<ErrorCode>,
        /// The error code for the sending direction, if it ended with one.
        tx_code: Option<ErrorCode>,
    },

    /// The connection is gone.
    ///
    /// No further events follow. `code` is the application error code where the peer or the
    /// transport supplied one.
    Closed {
        /// The application error code, if there was one.
        code: Option<ErrorCode>,
    },
}

/// What a transport did with one offer of stream data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteOutcome {
    /// This many bytes of the offer were taken, counting from the front.
    ///
    /// Fewer than were offered is normal and means the transport is congested; the stream
    /// is taken out of the running so the next offer is a different one. Taking all of them
    /// on an offer whose `fin` was set means the transport has also ended the stream.
    Accepted(usize),

    /// Nothing was taken, and the transport cannot take anything on this stream right now.
    ///
    /// The bytes are not lost — the same offer is made again once the stream is back in the
    /// running.
    Blocked,

    /// The stream no longer exists, so nothing more will ever be written to it.
    Gone,
}

/// The HTTP/3 layer's side of a write, offered to the transport one stream at a time.
///
/// Implemented by the driver, called by a [`QuicConnection`]. A transport never constructs
/// one of these; it receives one in [`QuicConnection::poll_transmit`] and pulls from it
/// while it has room.
pub trait StreamSource {
    /// Offers the next writable stream to `write`, and applies the verdict.
    ///
    /// Returns `false` when there is nothing to write at the moment, at which point the
    /// transport should stop pulling. Returns `true` when an offer was made and disposed of,
    /// which means there may be another.
    ///
    /// The closure receives the stream, the bytes to write as a vector list, and whether
    /// these are the last bytes on that stream. It must not assume the slices stay valid
    /// after it returns.
    ///
    /// # Errors are not returned here
    ///
    /// Deliberately a `bool` and not a `Result`. A failure inside this call belongs to the
    /// HTTP/3 layer and a transport has no way to represent it as its own
    /// [`QuicConnection::Error`], so it would have to be swallowed — and the failure in
    /// question can render the connection permanently unusable. The source stashes it and
    /// reports `false`; the driver collects it as soon as
    /// [`poll_transmit`](QuicConnection::poll_transmit) returns.
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool;
}

/// An established QUIC connection.
///
/// See the module documentation for the reasoning behind the shape, the retain contract, and
/// what is deliberately absent.
pub trait QuicConnection {
    /// How this transport fails.
    type Error: Into<Box<dyn core::error::Error + Send + Sync>>;

    /// Whether this transport reads through the buffers it is given.
    ///
    /// `false` means it takes a copy, so the bytes belong to the application again as soon
    /// as a write returns and [`QuicEvent::Released`] may be reported immediately. `true`
    /// means it holds the application's memory until the peer acknowledges, so `Released`
    /// must not be reported before then.
    ///
    /// **Declaring `false` while actually borrowing is a use-after-free.**
    ///
    /// Be clear about what this constant does and does not do. The layer does not branch on
    /// it: whichever value an implementation declares, it must still report
    /// [`QuicEvent::Released`] for every byte it accepts, and nothing is released until it
    /// does. What the constant buys is that the choice has to be *made and written down*,
    /// where a comment could be copied along with an adapter's shape and left unread. It is
    /// a declaration for the reader, not a switch for the driver.
    const RETAINS_BUFFERS: bool;

    /// The next thing that happened on the connection.
    ///
    /// # Boundedness is an obligation
    ///
    /// An implementation that reads ahead of the layer — anything with reader tasks does —
    /// must bound how far by the credit [`extend_credit`](Self::extend_credit) has granted.
    /// This holds *even for a transport whose QUIC library manages receive windows itself*:
    /// the window bounds what the peer may send, and this bounds what the implementation may
    /// hold on the layer's behalf. The two are different buffers, and only the second is the
    /// implementation's own. Reading without limit moves the memory bound out of QUIC and
    /// into the process, where a fast peer can exhaust it.
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>>;

    /// Drains what the layer has to write.
    ///
    /// Called when the transport can make progress on writing. It should pull from `source`
    /// while it has room, applying each offer, and return once `write_next` answers `false`
    /// or it can take no more. Returning [`Poll::Pending`] means nothing can be written at
    /// all right now; the transport must arrange for `cx` to be woken.
    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>>;

    /// Flushes output retained by the transport before the connection task suspends.
    ///
    /// The driver calls this only after another transport operation has returned
    /// [`Poll::Pending`] and the connection future is about to return `Pending` to its
    /// executor. It is not an end-of-pass hook: transports may accumulate bounded output
    /// across the driver's internal passes. In particular, a transport may use a self-woken
    /// `Pending` from [`poll_event`](Self::poll_event) to end one event batch while the driver
    /// continues synchronously with the batch it already collected; that boundary is not a
    /// task suspension and does not call this operation.
    ///
    /// [`Poll::Ready`] means the current output obligation has been discharged.
    /// [`Poll::Pending`] means progress cannot be made now and this call registered `cx` to
    /// be woken when it can. An implementation must not return an unwoken `Pending`.
    ///
    /// This operation introduces no new connection-ending channel. If flushing discovers
    /// an ending that an existing operation reports, the transport must arrange one wake
    /// for that pending operation to be polled again and preserve its existing result
    /// semantics. An error returned here is specifically a failure of flushing.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Opens a unidirectional stream.
    ///
    /// The layer opens exactly three of these, at startup, for the HTTP/3 control stream and
    /// the two QPACK streams. Returning [`Poll::Pending`] because the peer's stream limit is
    /// reached is expected and correct.
    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>>;

    /// Opens a bidirectional stream, on which the layer will send one request.
    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>>;

    /// Abandons what is left to send on a stream, telling the peer why.
    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error>;

    /// Asks the peer to stop sending on a stream, telling it why.
    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error>;

    /// Reports that received bytes have been consumed, so the peer may send more.
    ///
    /// `stream` names a stream, or is `None` for the connection as a whole. **The layer
    /// calls this twice for the same bytes**, once each way, because stream-level credit
    /// does not imply connection-level credit; an implementation that needs only one may
    /// ignore the other. Omitting it entirely stalls any transport that does not extend
    /// windows implicitly, which includes two of the four this trait was designed against.
    ///
    /// This is also the signal that bounds an implementation's own read-ahead — see
    /// [`poll_event`](Self::poll_event).
    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error>;

    /// Closes the connection, telling the peer why.
    ///
    /// `code` is an HTTP/3 application error code — `H3_NO_ERROR` for an orderly close, or
    /// whatever the failure implies. `reason` may be empty.
    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error>;

    /// The current time, as a monotonic count of nanoseconds.
    ///
    /// nghttp3 wants a timestamp on every read and the sans-I/O core will not invent one,
    /// which is what keeps a clock out of the core entirely. It is asked for here because a
    /// transport necessarily has a runtime and therefore a clock; ngtcp2 exposes
    /// `ngtcp2_conn_get_timestamp` for exactly this purpose.
    ///
    /// Must never go backwards.
    fn now(&self) -> Timestamp;
}
