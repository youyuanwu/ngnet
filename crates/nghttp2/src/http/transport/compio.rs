//! A ready-made transport for compio, over io_uring.
//!
//! Enabled by the optional `completion` feature, which is off by default: a caller on another
//! runtime should not pay for this one. It exists because a completion runtime is where the
//! transport traits' shape actually earns itself, and leaving the only worked example in a
//! test file meant every such user had to write it again.
//!
//! # What it does with the buffer, and why that is the easy case here
//!
//! compio is completion-based: an operation hands the kernel a buffer and gets it back when
//! the operation finishes, so the buffer must be *owned* for the duration. That is exactly
//! what [`TransportRead::read`] and [`TransportWrite::write`] pass — ownership in, ownership
//! back — so the adapter below is a destructuring and nothing more. compio's
//! `BufResult<usize, B>` is `(io::Result<usize>, B)` with a different name.
//!
//! The traits were shaped from this side deliberately. A readiness runtime can always
//! satisfy an owned-buffer API, at worst by copying, and tokio avoids even that by electing
//! the borrowed-write path. A completion runtime cannot satisfy a borrowed API at all.
//! Shaping the traits the other way round would have made this transport impossible rather
//! than merely slower.
//!
//! # Why the borrowed-write path is not taken
//!
//! [`TransportWrite::write_borrowed`] returns `None` here, which is the default. This is not
//! a missing optimisation: a completion runtime cannot lend the kernel a borrowed slice,
//! because the operation outlives the call that started it. The owned coalescing path is the
//! only correct one, and a reader who knows the tokio transport returns `Some` should not
//! read this absence as an oversight.
//!
//! [`TransportWrite::commit`] is likewise left as its no-op default. A completion write is
//! committed when it completes — there is no buffering layer between this and the kernel of
//! the kind that makes a flush necessary for a `BufWriter`.
//!
//! # The backend
//!
//! The crate depends on compio with its `io-uring` backend and no other, so a runtime either
//! uses io_uring or fails to start. There is deliberately no fallback to a readiness driver:
//! a transport that quietly became epoll while still calling itself completion-based would
//! make every measurement taken through it a lie, and would answer a question the caller did
//! not ask. Where io_uring is unavailable, this feature is the wrong feature to enable.

use bytes::{Bytes, BytesMut};
use compio::buf::BufResult;
use compio::io::{AsyncReadExt, AsyncWrite};
use compio::net::TcpStream;

use super::{Transport, TransportRead, TransportWrite};

/// Carries a compio TCP stream into this crate's transport traits.
///
/// Named for a concrete stream type rather than generic over compio's I/O traits, because
/// compio splits a stream by cloning its underlying descriptor rather than through a generic
/// `split`, and that is a property of the socket types rather than of the traits.
///
/// ```no_run
/// # use nghttp2::http::testing::Empty;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = compio::net::TcpStream::connect("127.0.0.1:8080").await?;
/// let (requests, connection) =
///     nghttp2::http::handshake::<_, Empty>(nghttp2::http::transport::CompioIo::new(stream))?;
/// compio::runtime::spawn(connection).detach();
/// # let _ = requests;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct CompioIo {
    stream: TcpStream,
}

impl CompioIo {
    /// Wraps a compio stream.
    pub const fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    /// Hands the stream back.
    pub fn into_inner(self) -> TcpStream {
        self.stream
    }
}

impl Transport for CompioIo {
    type Reader = CompioHalf;
    type Writer = CompioHalf;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.stream.into_split();
        (CompioHalf { stream: reader }, CompioHalf { stream: writer })
    }
}

/// One direction of a [`CompioIo`].
///
/// Both halves are the same type because compio's split hands back two handles to the same
/// socket rather than two distinct types. They are still independent for the purpose the
/// split exists to serve: each can be borrowed and awaited without waiting on the other.
#[derive(Debug)]
pub struct CompioHalf {
    stream: TcpStream,
}

impl TransportRead for CompioHalf {
    async fn read(&mut self, buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // `append` rather than `read`, so octets land after whatever the buffer already
        // holds — the contract the connection relies on, and the same one tokio's `read_buf`
        // provides.
        let BufResult(result, buf) = self.stream.append(buf).await;
        (result, buf)
    }
}

impl TransportWrite for CompioHalf {
    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let BufResult(result, buf) = self.stream.write(buf).await;
        (result, buf)
    }

    // `write_borrowed` and `commit` are deliberately left at their defaults; see the module
    // documentation for why neither is available to a completion runtime.
}
