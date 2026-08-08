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
//! declares [`Gathering`] as its [`Strategy`](TransportWrite::Strategy), so the driver gathers
//! the session's small output blocks into one `writev` while handing large ones to the socket
//! uncopied — few syscalls per pass without copying a body. Declaring that strategy is what
//! obliges this type to implement [`VectoredWrite`]; it cannot claim the path without
//! supplying it.
//!
//! [`BorrowedWrite`] is required alongside, and here it is a live path rather than a
//! formality. It is what runs when the underlying I/O reports it does not really gather and
//! would otherwise write only the first region of each call.
//!
//! # Asking the stream once
//!
//! Whether a stream really scatter-gathers is a property of that stream — tokio's default
//! `poll_write_vectored` writes the first region and ignores the rest — so it has to be asked.
//! It is asked exactly once, in [`Transport::split`], and the answer is cached in a field for
//! the connection's life; [`VectoredWrite::gathers`] returns that field, and the driver reads
//! `gathers` once per connection immediately after splitting. Previously
//! `AsyncWrite::is_write_vectored` — a virtual call whose answer never changes for a given
//! stream — was consulted on every single vectored write, and the driver additionally probed
//! for the capability once per flush pass by constructing a future purely to drop it unpolled.
//! Neither happens now.
//!
//! Because the `AsyncWrite` bound also admits buffering wrappers, whose `write` only fills a
//! buffer, [`TransportWrite::commit`] flushes: without it a `BufWriter` or `BufStream` would
//! strand the request and the driver would park on a response the peer never got.

use core::future::Future;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use super::{BorrowedWrite, Gathering, Transport, TransportRead, TransportWrite, VectoredWrite};

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
        // The one place this question is asked. `is_write_vectored` is a virtual call whose
        // answer is fixed for a given stream, so it is settled here, once, and read from the
        // field for the connection's life. The driver in turn reads `gathers` once, just
        // after this split, so the whole connection costs a single consultation.
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
    /// tokio is a readiness runtime that never needs to own the octets, so it gathers:
    /// small blocks are accumulated into a driver-owned buffer while large ones are lent to
    /// the socket uncopied, reaching few writes without copying payloads.
    type Strategy = Gathering;

    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let result = self.half.write(&buf).await;
        (result, buf)
    }

    async fn commit(&mut self) -> std::io::Result<()> {
        // The bound is `AsyncWrite` alone, which admits buffering wrappers (`BufWriter`,
        // `BufStream`): their `write` only fills a buffer, so without this the driver would
        // park on a response to a request still sitting unflushed. Flushing here is the
        // driver's promise made good — a raw socket's flush is a no-op, so the honest cases
        // pay nothing.
        self.half.flush().await
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
        // tokio never needs to own the octets, so the session's blocks are handed over as
        // they are produced rather than gathered into one owned buffer first. This is not
        // dead weight beside `write_vectored`: it is the path that runs whenever `gathers`
        // is false, which is to say whenever the wrapped stream only emulates a gathering
        // write.
        self.half.write(data)
    }
}

impl<T> VectoredWrite for TokioWriter<T>
where
    T: AsyncWrite,
{
    fn gathers(&self) -> bool {
        // The `AsyncWrite` bound admits implementations whose `poll_write_vectored` is the
        // provided default: it writes the first region and silently ignores the rest, so
        // gathering there would move one region per syscall and be strictly worse than the
        // borrowed path. Reporting `false` sends the driver to `write_borrowed` instead.
        // Every type this crate puts behind `TokioIo` in practice — `TcpStream`,
        // `UnixStream`, `DuplexStream`, and the buffering wrappers — reports `true`.
        //
        // Read from the field rather than from the stream: the answer was settled once, in
        // `Transport::split`.
        self.gathers
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [std::io::IoSlice<'w>],
    ) -> impl Future<Output = std::io::Result<usize>> + 'w {
        self.half.write_vectored(regions)
    }
}
