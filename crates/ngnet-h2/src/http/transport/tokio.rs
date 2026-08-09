//! A ready-made transport for tokio.
//!
//! Enabled by the optional `tokio` feature, which is off by default: a caller on another
//! runtime should not pay for this one, and the whole point of the transport traits is that
//! writing the twenty lines below for an unsupported runtime is a small job. This exists so
//! that the common case is not a small job at all.
//!
//! # What it does with the buffer
//!
//! tokio is readiness-based, so nothing here needs to own a buffer while an operation is in
//! flight. Reads take the buffer, fill it and hand it straight back, with no copy. The writer
//! declares [`Readiness`] as its [`Model`](TransportWrite::Model) — which says only that its
//! writes lend a borrowed buffer, and obliges it to implement [`BorrowedWrite`]. Whether a
//! pass becomes one gathering write or one copied write follows from what this adapter
//! answers to [`is_write_vectored`](TransportWrite::is_write_vectored), which is a report
//! about the wrapped stream rather than a decision of its own.
//!
//! [`write_borrowed`](BorrowedWrite::write_borrowed) is the primitive; the gathering write is
//! provided in terms of it. This adapter overrides that provided default when — and only
//! when — the wrapped stream really scatter-gathers.
//!
//! # Asking the stream once, and passing the answer on
//!
//! Whether a stream really scatter-gathers is a property of that stream: tokio's default
//! `poll_write_vectored` writes the first region and reports that honestly, so forwarding to
//! it unconditionally would cost one syscall per region while still delivering every octet.
//! Slower, not wrong — but worth avoiding, so it is asked.
//!
//! It is asked exactly once, in [`Transport::split`], of tokio's own
//! [`AsyncWrite::is_write_vectored`], and cached in a field for the connection's life. That
//! cached answer is what this adapter returns from
//! [`TransportWrite::is_write_vectored`], so the question the h2 layer asks this adapter and
//! the question this adapter asks tokio are the *same question*, forwarded — which is the
//! point of shaping the trait method after tokio's.
//!
//! A revision in between kept the answer private, on the reasoning that selecting between a
//! real vectored write and the emulation was this adapter's own business and nothing above
//! needed to see it. What that overlooked is that the choice does not stop at this file: a
//! stream that does not scatter-gather makes the emulation the only gathering available, and
//! the emulation costs one write per region. Keeping the answer private did not remove that
//! cost, it only removed the layer above's ability to route around it. The answer is public
//! again for that reason, and the caching is unchanged.
//!
//! The field also still selects, privately, between forwarding to the stream's vectored
//! write and calling the shared emulation. That branch is now unreachable *from the driver* —
//! a `false` answer sends the driver to the coalescing drain, which never calls
//! [`write_vectored`](BorrowedWrite::write_vectored) at all — but [`BorrowedWrite`] is a
//! public trait and a direct caller can still reach the method. Without the branch such a
//! caller would be forwarded to tokio's first-region-only default and would silently move
//! one region where it asked for all of them, so the branch stays as a defence for that
//! caller rather than for the driver.
//!
//! [`AsyncWrite::is_write_vectored`]:
//!     https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html#method.is_write_vectored
//!
//! Because the `AsyncWrite` bound also admits buffering wrappers, whose `write` only fills a
//! buffer, [`TransportWrite::commit`] flushes: without it a `BufWriter` or `BufStream` would
//! strand the request and the driver would park on a response the peer never got.

use core::future::Future;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use super::{BorrowedWrite, Readiness, Transport, TransportRead, TransportWrite};

/// Carries any tokio byte stream into this crate's transport traits.
///
/// Works with anything implementing [`AsyncRead`] and [`AsyncWrite`] — a `TcpStream`, a
/// `UnixStream`, a duplex pipe — rather than naming one of them, since the connection does
/// not care what the octets travel over.
///
/// ```no_run
/// # use ngnet_h2::http::testing::Empty;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
/// let (requests, connection) =
///     ngnet_h2::http::handshake::<_, Empty>(ngnet_h2::http::transport::TokioIo::new(stream))?;
/// tokio::spawn(connection);
/// # let _ = requests;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TokioIo<T> {
    stream: T,
}

impl<T> TokioIo<T> {
    /// Wraps a tokio stream.
    pub const fn new(stream: T) -> Self {
        Self { stream }
    }

    /// Hands the stream back.
    pub fn into_inner(self) -> T {
        self.stream
    }
}

impl<T> Transport for TokioIo<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Reader = TokioReader<T>;
    type Writer = TokioWriter<T>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = tokio::io::split(self.stream);
        // The one place this question is asked, and the only place it is answered.
        // `is_write_vectored` is a virtual call whose answer is fixed for a given stream, so
        // it is settled here, once, and read from the field for the connection's life.
        let gathers = writer.is_write_vectored();
        (
            TokioReader { half: reader },
            TokioWriter {
                half: writer,
                gathers,
            },
        )
    }
}

/// The reading half of a [`TokioIo`].
#[derive(Debug)]
pub struct TokioReader<T> {
    half: ReadHalf<T>,
}

/// The writing half of a [`TokioIo`].
#[derive(Debug)]
pub struct TokioWriter<T> {
    half: WriteHalf<T>,
    /// Whether the wrapped stream really scatter-gathers, settled once in
    /// [`Transport::split`] rather than asked on every write.
    ///
    /// The field is private but its value is not: it is exactly what
    /// [`TransportWrite::is_write_vectored`] reports, so the h2 layer can route a
    /// non-gathering stream to the coalescing drain instead of paying the emulation's one
    /// write per region. It additionally selects, inside
    /// [`write_vectored`](BorrowedWrite::write_vectored), between forwarding to the stream
    /// and emulating.
    gathers: bool,
}

impl<T> TransportRead for TokioReader<T>
where
    T: AsyncRead,
{
    async fn read(&mut self, mut buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // Appends into the spare capacity the connection left, so the octets land where
        // they will be read from and nothing is copied afterwards.
        let result = self.half.read_buf(&mut buf).await;
        (result, buf)
    }
}

impl<T> TransportWrite for TokioWriter<T>
where
    T: AsyncWrite,
{
    /// tokio is a readiness runtime: it never needs to own the octets, so writes lend a
    /// borrowed buffer and the session's blocks can go out uncopied.
    type Model = Readiness;

    async fn commit(&mut self) -> std::io::Result<()> {
        // The bound is `AsyncWrite` alone, which admits buffering wrappers (`BufWriter`,
        // `BufStream`): their `write` only fills a buffer, so without this the driver would
        // park on a response to a request still sitting unflushed. Flushing here is the
        // driver's promise made good — a raw socket's flush is a no-op, so the honest cases
        // pay nothing.
        self.half.flush().await
    }

    /// Forwards the wrapped stream's own answer, cached in [`Transport::split`].
    ///
    /// This adapter has no opinion of its own to add. The h2 layer's question — does your
    /// gathering write reach a real scatter-gather call — is tokio's question one layer down,
    /// so the honest implementation is to pass tokio's answer through unchanged. A
    /// `TcpStream`, a `UnixStream` and an in-memory `DuplexStream` all say `true`, each
    /// having overridden `poll_write_vectored`; a stream that leaves `poll_write_vectored`
    /// at its provided default says `false` and is routed to the coalescing drain, which is
    /// the right place for it.
    ///
    /// Do not guess which side a given tokio type falls on — `DuplexStream` in particular
    /// reads like an in-memory toy that would not bother, and overrides it anyway. That
    /// guess was made once while writing a test for this method and the test failed on it.
    fn is_write_vectored(&self) -> bool {
        self.gathers
    }
}

impl<T> BorrowedWrite for TokioWriter<T>
where
    T: AsyncWrite,
{
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + 'w {
        // The readiness primitive, and a real write rather than a formality: it is what the
        // emulated gathering write is built from, so it runs for every region of every pass
        // whenever the wrapped stream does not scatter-gather.
        self.half.write(data)
    }

    // Written as `async fn` rather than `-> impl Future` with an `async move` body, unlike its
    // sibling above. The two branches produce distinct opaque future types, so they cannot be
    // returned directly from one `-> impl Future`; they have to be awaited inside a single
    // future, and `async fn` is that future without the hand-rolled block clippy objects to.
    async fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [std::io::IoSlice<'w>],
    ) -> std::io::Result<usize> {
        if self.gathers {
            self.half.write_vectored(regions).await
        } else {
            // Why this branch survives the capability change, when the driver can no longer
            // reach it. `is_write_vectored` above forwards `self.gathers`, so a writer that
            // takes this branch reports `false`, and the h2 layer routes `false` to the
            // coalescing drain — which uses `write_borrowed` and never calls this method. The
            // branch is therefore dead *on the driver's path*, and was kept anyway, because
            // `BorrowedWrite` is public and this method is part of its surface: anyone
            // holding a `TokioWriter` may call `write_vectored` directly, and the contract
            // that call is entitled to is "every octet of every region, in order". Deleting
            // the branch would silently narrow that to "the first region only" for exactly
            // the streams that most need the loop.
            //
            // The `AsyncWrite` bound admits implementations whose `poll_write_vectored`
            // is the provided default: it writes the first region and ignores the rest.
            // Forwarding there would cost one syscall per region *and* go through a
            // vectored call that only pretends. Emulating explicitly costs the same
            // syscalls without the pretence, and shares the crate's single emulation
            // implementation rather than growing a second one.
            //
            // Every type this crate puts behind `TokioIo` in practice — `TcpStream`,
            // `UnixStream`, `DuplexStream`, and the buffering wrappers — takes the branch
            // above.
            super::emulate_gathering(self, regions).await
        }
    }
}
