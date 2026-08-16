//! Ready-made seam implementations for tokio.
//!
//! Behind the off-by-default `tokio` feature, following `ngnet-quic`'s endpoint seam and
//! `ngnet-h2`'s transport of the same name, and for the same reason: a caller on another
//! runtime should not compile this one, and describing a byte stream to this crate is a small
//! job for anyone who has to.
//!
//! # Why this wraps a trait pair rather than a socket
//!
//! [`TokioStream`] takes anything implementing [`AsyncRead`] and [`AsyncWrite`], rather than
//! one of tokio's socket types. That is the whole design of the feature rather than a generic
//! flourish. QMux's substrate is "an ordered, reliable, bidirectional byte stream", and the
//! deployments differ: TCP between hosts, a unix socket between processes on one, a TLS
//! session over either when the substrate is untrusted, a [`duplex`](tokio::io::duplex) pipe
//! in a test. Naming one socket type here would serve the first and leave the rest to write
//! this module again.
//!
//! It also keeps TLS out of this crate. A TLS session from `tokio-rustls` or
//! `tokio-native-tls` implements the same two traits a plain socket does, so a caller who
//! wants QMux over TLS hands one in and nothing here changes -- no TLS seam, no backend
//! feature, no certificate handling, and no dependency on a TLS crate. The alternative, a
//! wrapper per transport, would drag in exactly the machinery `ngnet-quic` needs and this
//! crate has no use for, because QMux delegates confidentiality to whatever carries the
//! bytes.
//!
//! Nothing here establishes a stream, either. There is no dialling, no listening and no
//! address in this file, because a layer that opened its own transport would have to know
//! which kind it was opening, which is the one thing the seam exists not to know.
//!
//! # Where the pin comes from, and why there is no `unsafe`
//!
//! tokio's traits are poll-shaped on `Pin<&mut Self>` and this crate's is poll-shaped on
//! `&mut self`, so the wrapper has to produce a pinned reference to the stream it owns. There
//! are three ways to do that. Projecting a pin through a struct field by hand is `unsafe`,
//! which the crate-level `#![deny(unsafe_code)]` rejects and this subtree is defined by not
//! needing, so it is not available at all. An `S: Unpin` bound would avoid an allocation, and
//! is rejected rather than unavailable: it excludes any stream that is not `Unpin`, and the
//! bound propagates outwards to every signature that mentions the connection, which is a
//! large thing to ask a caller for a small thing to save. So the stream is held in a [`Box`],
//! pinned once at construction: one allocation for the life of a connection, no bound on the
//! caller's type, and safe code throughout.
//!
//! The read delegation has the same shape of problem. tokio fills a
//! [`ReadBuf`](tokio::io::ReadBuf) rather than returning a count, and a `ReadBuf` may be built
//! over uninitialised memory, which is where the temptation to reach for `unsafe` comes from.
//! It is not needed: [`ReadBuf::new`](tokio::io::ReadBuf::new) over an ordinary `&mut [u8]`
//! records the whole slice as initialised -- which it is, because this seam's `poll_read`
//! takes `&mut [u8]` and the connection reads into a buffer it zeroed when it allocated it --
//! and `filled().len()` is then the count the seam asks for. What is given up is the ability
//! to hand the kernel uninitialised memory and skip that one zeroing per connection; what is
//! bought is a runtime integration that is ordinary Rust from top to bottom.
//!
//! # The clock mapping is the part worth reading
//!
//! [`Timestamp`] is an opaque nanosecond count in *the caller's* monotonic timescale, and
//! tokio measures time as an [`Instant`] with no public numeric value. Nothing converts
//! between them, so [`TokioClock`] picks an origin -- the instant it was constructed -- and
//! reports nanoseconds since then.
//!
//! Two consequences follow, and both are deliberate. Timestamps from two different
//! `TokioClock`s are not comparable, so a connection must be given one clock rather than a
//! fresh one per call; the state machine subtracts one unsigned reading from another, so
//! mixing origins does not produce a small error but an enormous elapsed time. And the origin
//! is not the process start or the epoch, so these values mean nothing outside this crate and
//! should not be logged as if they were wall time.
//!
//! The reading comes from tokio's [`Instant`] rather than the standard library's, which is
//! what `ngnet-quic`'s equivalent uses. The difference shows up in exactly one place and it is
//! the place that matters: a test that pauses tokio's clock moves tokio's instants and not the
//! standard library's, so a connection built on `std` time would hand the state machine
//! timestamps from a timescale the runtime around it did not share. Reading the same clock the
//! runtime reads keeps the two consistent, and costs nothing when time is not paused, because
//! tokio's reading *is* the standard library's then. It also keeps this subtree's own rule:
//! no file below `src/io/` names a standard-library time facility, and `tests/invariants.rs`
//! fails the build if one does.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

use super::clock::Clock;
use super::stream::{AsyncByteStream, Written};
use crate::time::Timestamp;

/// An [`AsyncByteStream`] over any tokio byte stream.
///
/// Wraps a TCP socket, a unix socket, a TLS session over either, a
/// [`duplex`](tokio::io::duplex) pipe, or anything else implementing [`AsyncRead`] and
/// [`AsyncWrite`]; see the module documentation for why the bound is the trait pair rather
/// than a socket type.
///
/// The connection owns the stream, because it must: a QMux connection is the only writer of
/// its byte stream, and a second writer interleaving bytes between two records produces a
/// stream neither peer can parse and which has no resynchronisation point to recover at.
///
/// The stream arrives already established, whatever it is -- which is why the example takes
/// one rather than opening one.
///
/// ```no_run
/// use ngnet_qmux::io::{Config, Connection, TokioClock, TokioStream};
/// use tokio::io::{AsyncRead, AsyncWrite};
///
/// fn client<S: AsyncRead + AsyncWrite>(
///     socket: S,
/// ) -> Result<Connection<TokioStream<S>, TokioClock>, Box<dyn std::error::Error>> {
///     Ok(Connection::client(
///         TokioStream::new(socket),
///         TokioClock::new(),
///         Config::new(),
///     )?)
/// }
/// ```
#[derive(Debug)]
pub struct TokioStream<S> {
    /// Pinned at construction, which is what lets every delegation below be safe code. See
    /// the module documentation for the two alternatives and why neither is taken.
    stream: Pin<Box<S>>,
}

impl<S> TokioStream<S> {
    /// Wraps an already-established stream.
    ///
    /// Establishing it -- connecting, listening, accepting, and any TLS handshake on top -- is
    /// the caller's, because this crate has no listener and no dialler by design.
    pub fn new(stream: S) -> Self {
        Self {
            stream: Box::pin(stream),
        }
    }

    /// The stream underneath, for a caller who needs to read an option off it.
    ///
    /// Shared rather than exclusive: an exclusive borrow would let a caller write to the
    /// stream behind the connection's back, which is the one thing that cannot be allowed.
    pub fn inner(&self) -> &S {
        self.stream.as_ref().get_ref()
    }
}

impl<S: AsyncRead + AsyncWrite> AsyncByteStream for TokioStream<S> {
    type Error = io::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut read = ReadBuf::new(buffer);
        match self.stream.as_mut().poll_read(cx, &mut read) {
            // tokio reports the end of a stream as a successful read that filled nothing,
            // which is what this seam calls `Ok(0)`; the two agree, so there is nothing to
            // translate and nothing that could mistake an idle stream for a finished one.
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read.filled().len())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            // tokio registered the waker as part of returning pending, which is exactly the
            // obligation this seam places on an implementation.
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        match self.stream.as_mut().poll_write(cx, bytes) {
            // A stream that accepts nothing from a non-empty offer, without saying it is not
            // ready, cannot be retried: `Written::Accepted(0)` carries no obligation to wake,
            // so the layer would be left offering the same bytes forever. `WriteZero` is what
            // the standard library calls this, and reporting it fails the connection with a
            // diagnosis instead of leaving it spinning against a stream that will not move.
            //
            // The guard is why this is a `WriteZero` and not a blanket rule: accepting none of
            // an *empty* offer is truthful rather than broken. This layer never makes one --
            // its flush loop runs while bytes remain -- but a wrapper that reported a caller's
            // own no-op as a failed connection would be lying about it.
            Poll::Ready(Ok(0)) if !bytes.is_empty() => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "the byte stream accepted none of a non-empty write without reporting that it \
                 was not ready",
            ))),
            Poll::Ready(Ok(taken)) => Poll::Ready(Ok(Written::Accepted(taken))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            // The one real translation in this file. tokio parks a write that cannot proceed
            // and registers the waker; this seam spells that answer `Written::NotNow`, which
            // says the same thing in the layer's own vocabulary -- the bytes are still the
            // caller's, none of them was consumed, offer them again when woken. A bare
            // `Poll::Pending` would be accepted by the layer too, but it is the weaker answer
            // to give from a *write*, because pending on a partial-accept API invites the
            // reading that some unknown prefix went out.
            //
            // A `WouldBlock` *error* is deliberately not folded in here, unlike in
            // `ngnet-quic`'s socket seam where the underlying call really can produce one.
            // tokio's `AsyncWrite` contract is that a not-ready write returns pending with the
            // waker registered, so a `WouldBlock` reaching this arm is a defect in the wrapped
            // stream; surfacing it as the error it is names the culprit, where converting it
            // into "not now" would produce a connection that waits for a wake nobody promised.
            Poll::Pending => Poll::Ready(Ok(Written::NotNow)),
        }
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // tokio's shutdown is this seam's: it flushes whatever the stream is holding before
        // reporting readiness, which is the property the close record depends on. A buffering
        // wrapper -- `BufWriter`, or a TLS session with a record part-built -- would otherwise
        // discard the CONNECTION_CLOSE it was asked to deliver, and the peer would see a
        // stream that ended rather than a connection that closed for a reason.
        self.stream.as_mut().poll_shutdown(cx)
    }
}

/// A [`Clock`] over tokio's.
///
/// Cheap to copy, and copies share an origin -- which matters, because timestamps from two
/// clocks with different origins are not comparable and the state machine would read the
/// difference as an enormous elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    origin: Instant,
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioClock {
    /// A clock whose origin is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Timestamp {
        let elapsed = self.origin.elapsed().as_nanos();
        // Saturating rather than wrapping, and the distinction is not academic: 584 years of
        // nanoseconds is beyond any process lifetime, but a clock that wrapped would go
        // backwards, and the state machine subtracts one unsigned reading from another. A
        // clock that stops is a connection whose timings are wrong; a clock that goes
        // backwards is one that believes an interval of nearly 600 years just elapsed.
        Timestamp::from_nanos(u64::try_from(elapsed).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;

    /// Polls once with a waker that does nothing, for answers that are immediate.
    fn poll_once<T>(f: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        f(&mut cx)
    }

    #[test]
    fn a_clocks_first_reading_is_near_its_origin() {
        let clock = TokioClock::new();
        // Not zero -- some time passes between construction and the call -- but the distance
        // from the origin is what is being pinned, since the whole mapping rests on it.
        assert!(clock.now().as_nanos() < 1_000_000_000);
    }

    #[test]
    fn a_clocks_readings_do_not_go_backwards() {
        let clock = TokioClock::new();
        let first = clock.now().as_nanos();
        let second = clock.now().as_nanos();
        assert!(
            second >= first,
            "the clock went backwards, which the state machine reads as an enormous elapsed \
             time"
        );
    }

    #[test]
    fn copies_share_an_origin() {
        // Load-bearing: a connection given a fresh clock per call would produce timestamps
        // from unrelated timescales, and the state machine compares them.
        let clock = TokioClock::new();
        let copy = clock;
        assert!(copy.now().as_nanos().abs_diff(clock.now().as_nanos()) < 1_000_000_000);
    }

    /// Bytes cross in both directions over a stream this file has never heard of.
    ///
    /// A `duplex` pipe is neither a socket nor this crate's in-memory implementation, which is
    /// the point: it exercises the delegation itself, with no reactor and no runtime, so a
    /// failure here is the wrapper's rather than the network's.
    #[test]
    fn the_wrapper_carries_bytes_in_both_directions() {
        let (client, server) = tokio::io::duplex(64);
        let mut client = TokioStream::new(client);
        let mut server = TokioStream::new(server);

        let written = poll_once(|cx| client.poll_write(cx, b"a record"));
        assert!(matches!(written, Poll::Ready(Ok(Written::Accepted(8)))));

        let mut buffer = [0u8; 16];
        let read = poll_once(|cx| server.poll_read(cx, &mut buffer));
        assert!(matches!(read, Poll::Ready(Ok(8))));
        assert_eq!(&buffer[..8], b"a record");
    }

    /// A stream with no bytes in it parks rather than reporting the end of the stream.
    ///
    /// The distinction the seam is most easily broken on: `Ok(0)` means the peer will send
    /// nothing further, and a wrapper that answered it here would end every connection that
    /// was merely idle.
    #[test]
    fn an_empty_stream_parks_rather_than_reporting_its_end() {
        let (client, _server) = tokio::io::duplex(64);
        let mut client = TokioStream::new(client);

        let mut buffer = [0u8; 16];
        assert!(poll_once(|cx| client.poll_read(cx, &mut buffer)).is_pending());
    }

    /// A full stream reports "not now" rather than accepting nothing.
    ///
    /// `duplex` has a fixed capacity and an unread far end, so the second write has nowhere to
    /// go. `Written::NotNow` is the answer that carries the obligation to wake; the layer
    /// treats an `Accepted(0)` as a contract breach and has to wake itself to avoid stalling.
    #[test]
    fn a_full_stream_reports_that_it_cannot_proceed() {
        let (client, _server) = tokio::io::duplex(8);
        let mut client = TokioStream::new(client);

        assert!(matches!(
            poll_once(|cx| client.poll_write(cx, b"12345678")),
            Poll::Ready(Ok(Written::Accepted(8)))
        ));
        assert!(matches!(
            poll_once(|cx| client.poll_write(cx, b"more")),
            Poll::Ready(Ok(Written::NotNow))
        ));
    }

    /// A shut-down write side is an end of stream at the far end, not an error.
    ///
    /// This is what makes a QMux close observable: the peer reads the CONNECTION_CLOSE record
    /// and then reads zero, rather than waiting for bytes that will never come.
    #[test]
    fn a_shutdown_write_side_ends_the_peers_reads() {
        let (client, server) = tokio::io::duplex(64);
        let mut client = TokioStream::new(client);
        let mut server = TokioStream::new(server);

        assert!(matches!(
            poll_once(|cx| client.poll_write(cx, b"last")),
            Poll::Ready(Ok(Written::Accepted(4)))
        ));
        assert!(matches!(
            poll_once(|cx| client.poll_shutdown(cx)),
            Poll::Ready(Ok(()))
        ));

        let mut buffer = [0u8; 16];
        assert!(
            matches!(
                poll_once(|cx| server.poll_read(cx, &mut buffer)),
                Poll::Ready(Ok(4))
            ),
            "the bytes written before the shutdown were delivered, not discarded by it"
        );
        assert!(
            matches!(
                poll_once(|cx| server.poll_read(cx, &mut buffer)),
                Poll::Ready(Ok(0))
            ),
            "and then the far end sees the end of the stream"
        );
    }

    /// A stream that swallows a write without taking it is reported broken, not retried.
    ///
    /// The one place this wrapper substitutes its own judgement for the stream's. tokio's
    /// `AsyncWrite` permits `Ok(0)`, and this seam does not: `Written::Accepted(0)` carries no
    /// obligation to wake, so a layer offered it can only offer the same bytes again, forever.
    /// Failing the connection turns a silent spin into a diagnosis naming the stream.
    #[test]
    fn a_stream_that_accepts_nothing_is_reported_broken_rather_than_retried() {
        let mut stream = TokioStream::new(AcceptsNothing);

        match poll_once(|cx| stream.poll_write(cx, b"bytes")) {
            Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::WriteZero),
            other => panic!("a write that went nowhere was reported as {other:?}"),
        }
    }

    /// And an offer of nothing is answered with nothing, which is not the same defect.
    ///
    /// Nothing in this crate makes such an offer, but a wrapper that reported a caller's own
    /// no-op as a broken transport would be blaming the stream for the caller's call.
    #[test]
    fn an_empty_offer_is_not_a_broken_stream() {
        let mut stream = TokioStream::new(AcceptsNothing);

        assert!(matches!(
            poll_once(|cx| stream.poll_write(cx, &[])),
            Poll::Ready(Ok(Written::Accepted(0)))
        ));
    }

    /// A stream that reports a successful write of no bytes, which the seam forbids.
    ///
    /// Written out rather than reached for: no real tokio stream does this on purpose, so the
    /// only way to exercise the wrapper's answer to it is to build one that does.
    struct AcceptsNothing;

    impl AsyncRead for AcceptsNothing {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for AcceptsNothing {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}
