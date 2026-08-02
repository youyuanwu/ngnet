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
//! flight. Reads take the buffer, fill it and hand it straight back, with no copy. Writes
//! elect [`TransportWrite::write_borrowed`], handing the session's own output blocks over
//! directly instead of gathering them into an owned buffer first — the one cost the
//! completion-shaped traits would otherwise impose on a runtime that does not need them.
//!
//! Because the `AsyncWrite` bound also admits buffering wrappers, whose `write` only fills a
//! buffer, [`TransportWrite::commit`] flushes: without it a `BufWriter` or `BufStream` would
//! strand the request and the driver would park on a response the peer never got.

use core::future::Future;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use super::{Transport, TransportRead, TransportWrite};

/// Carries any tokio byte stream into this crate's transport traits.
///
/// Works with anything implementing [`AsyncRead`] and [`AsyncWrite`] — a `TcpStream`, a
/// `UnixStream`, a duplex pipe — rather than naming one of them, since the connection does
/// not care what the octets travel over.
///
/// ```no_run
/// # use nghttp2::http::testing::Empty;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
/// let (requests, connection) =
///     nghttp2::http::handshake::<_, Empty>(nghttp2::http::transport::TokioIo::new(stream))?;
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
        (TokioReader { half: reader }, TokioWriter { half: writer })
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
    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let result = self.half.write(&buf).await;
        (result, buf)
    }

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = std::io::Result<usize>> + 'w> {
        // tokio never needs to own the octets, so the session's blocks are handed over as
        // they are produced rather than gathered into one owned buffer first — the zero-copy
        // path, elected by returning the write itself rather than by any separate flag.
        Some(self.half.write(data))
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
