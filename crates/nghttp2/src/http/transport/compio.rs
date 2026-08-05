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
//! # The one fast write path a completion runtime can take
//!
//! [`TransportWrite::write_borrowed`] returns `None` here, which is the default. This is not
//! a missing optimisation: a completion runtime cannot lend the kernel a borrowed slice,
//! because the operation outlives the call that started it, so the tokio transport's
//! borrowed path has no counterpart on this side.
//!
//! [`TransportWrite::write_vectored`] is declined for the same reason. compio *does* support
//! gathering writes and does issue a real one to the kernel — `TcpStream::write_vectored`
//! reaches `IORING_OP_SENDMSG` with an iovec array — but its `IoVectoredBuf` is bound by
//! `'static`, since the kernel writes from the buffers after submission, while
//! `write_vectored` hands out borrowed `IoSlice`s that can never be `'static`. What blocks
//! that path is ownership, not capability.
//!
//! Ownership is exactly what the *owned-region* strategy provides, and this transport takes
//! it. [`TransportWrite::gathers_owned_regions`] returns `true`, and
//! [`TransportWrite::write_regions`] hands compio a `Vec<Bytes>` by value. This became
//! possible when the crate adopted libnghttp2's no-copy `DATA` facility: a handed-over
//! payload is now caller-owned [`Bytes`] rather than a borrow of libnghttp2's serialisation
//! buffer, and compio satisfies its `'static` ownership requirement directly — `Bytes`
//! implements compio's `IoBuf` and `Vec<T: IoBuf>` implements `IoVectoredBuf`, so a growable
//! list of payloads and driver-minted headers is a valid vectored buffer. `write_vectored`
//! takes the buffer by value and returns it inside compio's `BufResult`, so moving the list
//! in and taking it back out is the exact shape the API expects: the completion runtime now
//! issues a genuine gathering write for a no-copy body rather than coalescing it into an
//! intermediate buffer, and the payload is never copied. The blocks the session itself
//! produces are still copied — every one of them, since unlike the vectored path there is no
//! size threshold here: a block borrowed from the session cannot be owned without a copy. On
//! a handed-over body those blocks are the control and `HEADERS` frames rather than the
//! payload, which is the whole point; a *push-model* body on this transport still has its
//! `DATA` copied, because its octets were never the caller's to hand over.
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

    fn gathers_owned_regions(&self) -> bool {
        // A completion runtime owns its buffers, which is exactly what the owned-region
        // strategy requires and what lets this transport issue a genuine gathering write for
        // a no-copy body. See the module documentation for why this is the one fast path a
        // completion transport can take, and why the vectored one is not.
        true
    }

    async fn write_regions(&mut self, regions: Vec<Bytes>) -> (std::io::Result<usize>, Vec<Bytes>) {
        // `Vec<Bytes>` is a compio `IoVectoredBuf` (`Vec<T: IoBuf>`, `Bytes: IoBuf`), so this
        // is a real `writev` reaching `IORING_OP_SENDMSG`, not an emulation. compio takes the
        // list by value and returns it inside `BufResult`, which is precisely the ownership
        // round-trip the trait asks for: the driver gets its allocation back to reuse.
        let BufResult(result, regions) = self.half.write_vectored(regions).await;
        (result, regions)
    }

    // `write_borrowed` and `commit` are deliberately left at their defaults; see the module
    // documentation for why a completion runtime cannot lend a borrowed slice and has nothing
    // to flush.
}
