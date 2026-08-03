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
//! This crate depends on compio with its `io-uring` backend and asks for no readiness one, so
//! by default there is nothing to fall back to: a runtime either uses io_uring or fails to
//! start. That is deliberate. A transport that quietly became epoll while still calling itself
//! completion-based would make every measurement taken through it a lie, and would answer a
//! question the caller did not ask. Where io_uring is unavailable, this is the wrong feature
//! to enable.
//!
//! One caveat, because the guarantee is not absolute: cargo unifies features across a
//! dependency graph, so if *anything else* in your build enables compio's `polling` feature,
//! compio compiles its fusion driver and regains the silent epoll fallback — without this
//! crate asking for it or being able to prevent it. A caller who depends on running on
//! io_uring should check it rather than infer it from this crate's manifest.
//! `compio::runtime::Runtime::driver_type` reports what was obtained, though note it can only
//! reveal a *fallback that actually happened*: in a build without `polling` it is a
//! compile-time constant, and in a fusion build on a host that has io_uring it will report
//! io_uring quite correctly while the fallback sits armed for a host that does not.
//! `cargo tree -e features` is what shows whether `polling` reached the build at all, and this
//! repository's CI runs exactly that check on every change.

use bytes::{Bytes, BytesMut};
use compio::buf::BufResult;
use compio::io::util::Splittable;
use compio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use super::{Transport, TransportRead, TransportWrite};

/// Carries a compio byte stream into this crate's transport traits.
///
/// Generic over compio's [`Splittable`], so a `TcpStream` and a `UnixStream` are both
/// accepted rather than only the one this was first written for — the connection does not
/// care what the octets travel over.
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
pub struct CompioIo<T> {
    stream: T,
}

impl<T> CompioIo<T> {
    /// Wraps a compio stream.
    pub const fn new(stream: T) -> Self {
        Self { stream }
    }

    /// Hands the stream back.
    pub fn into_inner(self) -> T {
        self.stream
    }
}

impl<T> Transport for CompioIo<T>
where
    T: Splittable,
    T::ReadHalf: AsyncRead,
    T::WriteHalf: AsyncWrite,
{
    type Reader = CompioReader<T::ReadHalf>;
    type Writer = CompioWriter<T::WriteHalf>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        // compio's own split, which for a socket hands back two handles to one descriptor
        // rather than two distinct types. That is not the serialising fallback the transport
        // trait warns about: there is no lock between them, so neither direction waits on the
        // other and no head-of-line stall is reintroduced.
        let (reader, writer) = self.stream.split();
        (CompioReader { half: reader }, CompioWriter { half: writer })
    }
}

/// The reading half of a [`CompioIo`].
#[derive(Debug)]
pub struct CompioReader<R> {
    half: R,
}

/// The writing half of a [`CompioIo`].
#[derive(Debug)]
pub struct CompioWriter<W> {
    half: W,
}

impl<R: AsyncRead> TransportRead for CompioReader<R> {
    async fn read(&mut self, buf: BytesMut) -> (std::io::Result<usize>, BytesMut) {
        // `append` rather than `read`, so octets land after whatever the buffer already
        // holds — the contract the connection relies on, and the same one tokio's `read_buf`
        // provides.
        let BufResult(result, buf) = self.half.append(buf).await;
        (result, buf)
    }
}

impl<W: AsyncWrite> TransportWrite for CompioWriter<W> {
    async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
        let BufResult(result, buf) = self.half.write(buf).await;
        (result, buf)
    }

    // `write_borrowed` and `commit` are deliberately left at their defaults; see the module
    // documentation for why neither is available to a completion runtime.
}
