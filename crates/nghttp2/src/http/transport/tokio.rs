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
//! override [`TransportWrite::write_borrowed`] and advertise it, so the connection hands
//! over the session's own output blocks directly instead of gathering them into an owned
//! buffer first — which is the one cost the completion-shaped traits would otherwise impose
//! on a runtime that does not need them.

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

    async fn write_borrowed(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.half.write(data).await
    }

    fn writes_borrowed(&self) -> bool {
        // tokio never needs to own the octets, so the connection is told to hand over the
        // session's blocks as they are produced rather than gathering them first.
        true
    }
}
