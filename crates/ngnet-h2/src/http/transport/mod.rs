//! The byte transport an asynchronous connection runs over.
//!
//! # Why the traits are shaped this way
//!
//! Two families of async I/O exist and they disagree about who owns the buffer.
//! Readiness-based APIs — tokio, `futures-io` — lend a borrowed buffer for the duration of
//! a call. Completion-based APIs — `io_uring`, IOCP, and the runtimes built on them — hand
//! the buffer to the kernel, which may still be writing into it after the future that
//! submitted the operation is dropped; they must therefore *own* it.
//!
//! Completion-shaped traits are the superset. A readiness-based transport implements them
//! with no copy at all: take the buffer, read into it, hand it back. The reverse does not
//! work — a completion-based transport behind a borrowed-buffer trait needs a stable
//! buffer of its own plus a copy out of it. So ownership is transferred here, and the
//! readiness case is served by an override rather than by compromising the shape.
//!
//! # What is deliberately absent
//!
//! There is no [`Send`] bound anywhere in these traits. The flagship completion runtimes
//! are thread-per-core and build their I/O on `Rc`, so requiring `Send` would exclude the
//! very runtimes this abstraction exists to serve — a worse outcome than being unable to
//! spawn on a work-stealing executor. Auto traits propagate instead: a connection over a
//! transport that happens to be `Send` is itself `Send`, which is what `spawn` needs, and
//! nothing has to be declared for that to hold.
//!
//! These traits are not object-safe, because the methods return `impl Future`. Transports
//! are taken generically; a boxed transport is out of scope.
//!
//! # Splitting
//!
//! A connection reads and writes at the same time — that is the point of multiplexing —
//! so the two directions must be able to hold separate borrows. [`Transport::split`]
//! divides a transport once, at construction, into halves that proceed independently. A
//! transport that genuinely cannot be split may implement `split` over a shared cell, but
//! that is a serialising fallback: it reintroduces exactly the head-of-line stall the
//! split exists to prevent, and an `Rc`-based cell additionally makes both halves
//! non-`Send`.
//!
//! # How a pass gets drained
//!
//! A writer names one strategy as an associated type. That declaration is the whole
//! election: there is no probe, no capability flag to keep in step with a method, and no
//! way to advertise a path without supplying it — naming a strategy obliges the writer, by
//! compiler error, to implement that strategy's operations.
//!
//! | declared [`Strategy`](TransportWrite::Strategy) | model | operations the writer must supply | writes per pass | driver-side copy |
//! | --- | --- | --- | --- | --- |
//! | [`Coalesced`] | either | [`write`](TransportWrite::write) | one | every octet, every pass |
//! | [`PerRegion`] | readiness | `write` + [`write_borrowed`](BorrowedWrite::write_borrowed) | one per region | none |
//! | [`Gathering`] | readiness | `write` + `write_borrowed` + [`write_vectored`](VectoredWrite::write_vectored) | one per large block and per region-cap flush, plus at most one for the remainder | none |
//! | [`OwnedRegions`] | completion | `write` + [`write_regions`](RegionWrite::write_regions) | one per region-cap flush, plus one for the remainder | every session block, never the payload |
//!
//! The gathering strategy exists because the first two are each wrong for half of the
//! traffic: under multiplexing a pass is dozens of tiny blocks, where one write per block
//! is the dominant cost, and with a large body it is a handful of 16 KiB blocks, where
//! copying them all to save three syscalls is the dominant cost. Gathering small blocks
//! into a buffer the driver owns while handing large ones to the socket uncopied gets both.
//!
//! [`OwnedRegions`] is the gathering strategy for a *completion* transport, which cannot
//! lend the kernel a borrowed [`IoSlice`](std::io::IoSlice): the kernel writes from the
//! buffers after submission, so they must be owned. It gathers a pass into a list of owned
//! [`Bytes`] instead — the session's blocks coalesced into a driver buffer, every one of
//! them, since a borrowed block cannot be owned without a copy, and each handed-over payload
//! as its own uncopied region — and hands the whole list to
//! [`write_regions`](RegionWrite::write_regions), which reaches a single `writev`. The
//! payload is never copied; the session's own blocks all are, with no size threshold,
//! because a block borrowed from the session cannot be owned without one.
//!
//! # One model, enforced rather than encouraged
//!
//! The two gathering strategies serve disjoint populations, and the reason is ownership, not
//! taste. [`Gathering`] lends borrowed slices and so suits only a readiness transport;
//! [`OwnedRegions`] owns its buffers and so suits only a completion one. The type system
//! carries that: [`BorrowedWrite`] and [`VectoredWrite`] are available only to a writer whose
//! strategy is a [`ReadinessStrategy`], [`RegionWrite`] only to one whose strategy is a
//! [`CompletionStrategy`], and no strategy is both. A writer therefore *cannot* implement
//! operations from both models — it is a compile error, not a convention.
//!
//! Which to prefer, if a transport could genuinely do either: [`Gathering`] over
//! [`OwnedRegions`], because it need not mint an owned `Bytes` per frame header. This is the
//! same reasoning the old runtime precedence rule encoded; it is now advice at the point of
//! declaration rather than arbitration at run time, because there is no longer anything to
//! arbitrate.
//!
//! # The one capability that is still read at run time
//!
//! Whether a stream *really* scatter-gathers is a property of the stream, not of the backend:
//! a tokio [`AsyncWrite`] whose `poll_write_vectored` is the default writes only the first
//! region. So [`VectoredWrite::gathers`] exists, and a [`Gathering`] writer that turns out
//! not to gather falls back to its borrowed write — which is why [`VectoredWrite`] requires
//! [`BorrowedWrite`], and why that borrowed write must be real rather than a stub.
//!
//! That capability is read **once per connection**, immediately after the transport is
//! split, and held for the connection's life. It is the only capability consultation in the
//! design.
//!
//! [`AsyncWrite`]: https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html

use core::future::Future;

use bytes::{Bytes, BytesMut};

#[cfg(feature = "completion")]
mod compio;
#[cfg(feature = "tokio")]
mod tokio;

#[cfg(feature = "completion")]
pub use compio::{CompioIo, CompioReader, CompioWriter};
#[cfg(feature = "tokio")]
pub use tokio::{TokioIo, TokioReader, TokioWriter};

/// A connected byte stream, divisible into independent halves.
pub trait Transport {
    /// The reading half.
    type Reader: TransportRead;
    /// The writing half.
    type Writer: TransportWrite;

    /// Divides the transport so both directions can proceed at once.
    fn split(self) -> (Self::Reader, Self::Writer);
}

/// The reading half of a transport.
pub trait TransportRead {
    /// Reads by *appending* into `buf`, returning it grown by the octets read.
    ///
    /// The buffer is a growable [`BytesMut`]; an implementation reads into its spare
    /// capacity and leaves whatever was already there untouched. The returned `usize` is
    /// an **end-of-input indicator only**: `Ok(0)` means the peer closed, and any nonzero
    /// value means at least one octet was appended — the driver reads the octets from the
    /// buffer itself, by how much it grew, not from this count. Returning a nonzero count
    /// without growing the buffer by that many octets is a contract violation the driver
    /// checks in debug builds.
    ///
    /// Ownership of the buffer passes to the implementation for the duration of the call
    /// and comes back with it, which is what lets a completion-based implementation keep
    /// the buffer stable while the kernel writes into it.
    ///
    /// The buffer is returned even on failure, so a caller never loses it to an error.
    fn read(&mut self, buf: BytesMut) -> impl Future<Output = (std::io::Result<usize>, BytesMut)>;
}

mod sealed {
    /// Closes the strategy set to this crate, so the driver's handling of it is exhaustive
    /// by construction and a downstream crate cannot invent a fifth.
    pub trait Sealed {}
}

/// Coalesce a whole pass into one owned [`write`](TransportWrite::write).
///
/// The default and the simplest thing that works: one write per pass, at the cost of copying
/// every outgoing octet into a buffer the driver owns. That buffer is reused across passes,
/// so it costs no allocation in steady state. Belongs to neither I/O model — a completion
/// transport and a readiness one can both take it — which is why it implements neither
/// [`ReadinessStrategy`] nor [`CompletionStrategy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Coalesced;

/// Hand each session block to the transport as it is produced, uncopied.
///
/// One write per region and no copying at all. Readiness only: the blocks are lent, not
/// owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PerRegion;

/// Gather small blocks into a driver-owned buffer and lend large ones directly.
///
/// The best of [`Coalesced`] and [`PerRegion`]: few writes *and* no copying of large
/// payloads. Readiness only, for the same reason as [`PerRegion`] — an
/// [`IoSlice`](std::io::IoSlice) is borrowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Gathering;

/// Gather a pass into a list of *owned* regions and write them as one operation.
///
/// [`Gathering`] for a completion transport. The kernel writes from these buffers after
/// submission, so they must be owned rather than lent; completion only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OwnedRegions;

/// One of the four ways the driver can drain a pass of session output into writes.
///
/// Sealed: the four strategies in this module are all there are.
pub trait WriteStrategy: sealed::Sealed {}

/// A strategy whose writes lend borrowed buffers, and so belong to a readiness-based
/// transport.
///
/// Implemented by [`PerRegion`] and [`Gathering`]. This bound is what makes the two I/O
/// models mutually exclusive: [`BorrowedWrite`] and [`VectoredWrite`] require it, and no
/// strategy implements both this and [`CompletionStrategy`].
pub trait ReadinessStrategy: WriteStrategy {}

/// A strategy whose writes take ownership of their buffers, and so belong to a
/// completion-based transport.
///
/// Implemented by [`OwnedRegions`] alone. See [`ReadinessStrategy`] for why this matters.
pub trait CompletionStrategy: WriteStrategy {}

impl sealed::Sealed for Coalesced {}
impl sealed::Sealed for PerRegion {}
impl sealed::Sealed for Gathering {}
impl sealed::Sealed for OwnedRegions {}

impl WriteStrategy for Coalesced {}
impl WriteStrategy for PerRegion {}
impl WriteStrategy for Gathering {}
impl WriteStrategy for OwnedRegions {}

impl ReadinessStrategy for PerRegion {}
impl ReadinessStrategy for Gathering {}

impl CompletionStrategy for OwnedRegions {}

/// The writing half of a transport.
///
/// Every writer supplies [`write`](TransportWrite::write) and names a
/// [`Strategy`](TransportWrite::Strategy). Naming anything other than [`Coalesced`] obliges
/// it to implement that strategy's operations too — see the [module
/// documentation](self#how-a-pass-gets-drained) for the table.
///
/// # Declaring a strategy you have not implemented will not compile
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{Gathering, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
///
/// struct ClaimsWithoutWriting;
/// impl TransportWrite for ClaimsWithoutWriting {
///     // `Gathering` requires `VectoredWrite`, which this type does not implement.
///     type Strategy = Gathering;
///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// ```
///
/// The same holds on the completion side, so neither model has a way to advertise a fast
/// path it has not supplied:
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{OwnedRegions, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
///
/// struct ClaimsRegionsWithoutWriting;
/// impl TransportWrite for ClaimsRegionsWithoutWriting {
///     // `OwnedRegions` requires `RegionWrite`, which this type does not implement.
///     type Strategy = OwnedRegions;
///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// ```
///
/// # A path cannot be withdrawn mid-pass
///
/// The older traits let a writer decline a fast path per call, by returning `None` from an
/// `Option`-returning method. The operations are no longer `Option`-shaped, so a writer that
/// tries to keep declining does not typecheck — it must report through its result instead.
///
/// ```compile_fail,E0053
/// use ngnet_h2::http::transport::{BorrowedWrite, PerRegion, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
/// use core::future::Future;
///
/// struct Withdraws {
///     healthy: bool,
/// }
/// impl TransportWrite for Withdraws {
///     type Strategy = PerRegion;
///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// impl BorrowedWrite for Withdraws {
///     // The trait's return type is the future, not an `Option` of one.
///     fn write_borrowed<'w>(
///         &'w mut self,
///         data: &'w [u8],
///     ) -> Option<impl Future<Output = std::io::Result<usize>> + 'w> {
///         if !self.healthy {
///             return None;
///         }
///         Some(async move { Ok(data.len()) })
///     }
/// }
/// ```
///
/// # The strategy set is closed
///
/// [`WriteStrategy`] is sealed, so a downstream crate cannot invent a fifth strategy and the
/// driver's handling of the four is exhaustive by construction.
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::WriteStrategy;
///
/// struct MyOwnStrategy;
/// impl WriteStrategy for MyOwnStrategy {}
/// ```
pub trait TransportWrite {
    /// How the driver drains a pass over this writer.
    ///
    /// This declaration *is* the election. The driver resolves it at compile time and never
    /// asks the writer which path to take.
    ///
    /// Stable Rust has no defaults for associated types, so even the simplest transport must
    /// write this line — `type Strategy = Coalesced;` — costing one line for the guarantee
    /// that a declared strategy is always a supplied one.
    type Strategy: WriteStrategy + Elects<Self>;

    /// Writes `buf`, returning it along with how many octets were written.
    ///
    /// "Written" does not have to mean "handed to the peer" the instant this returns: a
    /// buffering transport may hold the octets, so long as it releases them no later than
    /// [`commit`](TransportWrite::commit), which the driver calls before it waits on the
    /// peer.
    ///
    /// As with [`TransportRead::read`], ownership passes in and comes back, and comes back
    /// even on failure.
    fn write(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)>;

    /// Commits everything written so far to the peer-visible byte stream.
    ///
    /// The driver guarantees it calls this once it has drained a write pass and before it
    /// parks awaiting readable input: it never waits on the peer while octets it has
    /// produced are still sitting in a transport-side buffer. An implementation whose
    /// writes are peer-visible the moment [`write`](TransportWrite::write) returns — a raw
    /// socket, a completion transport, the in-memory duplex — has nothing to do here, which
    /// is why the default does nothing. One that buffers, such as a `BufWriter` or a
    /// `BufStream`, must flush that buffer here; otherwise the driver awaits a response to a
    /// request the peer never received, and the connection silently hangs.
    fn commit(&mut self) -> impl Future<Output = std::io::Result<()>> {
        async { Ok(()) }
    }
}

/// Writing a borrowed slice — the readiness model's zero-copy write.
///
/// Available only to a writer whose [`Strategy`](TransportWrite::Strategy) is a
/// [`ReadinessStrategy`]. A completion transport cannot implement this at all: the kernel
/// may still be writing from the buffer after the submitting future is dropped, so a
/// borrowed slice is unsound there, and the bound below makes the attempt a compile error
/// rather than a documented warning.
///
/// # A writer cannot implement both models
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{OwnedRegions, RegionWrite, BorrowedWrite, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
///
/// struct BothModels;
/// impl TransportWrite for BothModels {
///     type Strategy = OwnedRegions;
///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// impl RegionWrite for BothModels {
///     async fn write_regions(
///         &mut self,
///         regions: Vec<Bytes>,
///     ) -> (std::io::Result<usize>, Vec<Bytes>) {
///         (Ok(regions.iter().map(|r| r.len()).sum()), regions)
///     }
/// }
/// // `OwnedRegions` is not a `ReadinessStrategy`, so this impl cannot exist.
/// impl BorrowedWrite for BothModels {
///     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> std::io::Result<usize> {
///         Ok(data.len())
///     }
/// }
/// ```
pub trait BorrowedWrite: TransportWrite
where
    Self::Strategy: ReadinessStrategy,
{
    /// Writes the borrowed slice, returning how many octets were accepted.
    ///
    /// A short write is normal and the driver re-offers the remainder. An accepted write of
    /// zero octets is an error rather than something to spin on.
    ///
    /// There is no way to decline: the strategy was settled when the writer declared it, and
    /// a writer that cannot complete a write reports so through this result — a short count
    /// or an error — never by refusing the path.
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + 'w;
}

/// Writing several borrowed regions as one gathering operation.
///
/// Available only to a writer whose [`Strategy`](TransportWrite::Strategy) is a
/// [`ReadinessStrategy`] — in practice [`Gathering`].
///
/// Requires [`BorrowedWrite`] because [`gathers`](VectoredWrite::gathers) may report that the
/// underlying stream does not really scatter-gather, in which case the driver writes each
/// region borrowed instead. That fallback is a live path, not a formality: a
/// [`BorrowedWrite`] implementation here must be real.
pub trait VectoredWrite: BorrowedWrite
where
    Self::Strategy: ReadinessStrategy,
{
    /// Whether the underlying stream really writes every region, rather than emulating a
    /// gathering write by writing the first and ignoring the rest.
    ///
    /// The driver reads this **once per connection**, immediately after the transport is
    /// split, and holds the answer for the connection's life. It must therefore be a fixed
    /// property of this writer, not something that varies per call.
    ///
    /// The default is `true`. A transport whose I/O merely emulates gathering — as tokio's
    /// default `poll_write_vectored` does, writing only the first region — must return
    /// `false`, or every gathering write would move just one region.
    fn gathers(&self) -> bool {
        true
    }

    /// Writes `regions` in order, as one operation, exactly as `writev` would.
    ///
    /// The return is the number of octets accepted across the whole sequence; a short write
    /// is normal and the driver re-offers what remains. An accepted write of zero octets is
    /// an error.
    ///
    /// This library's driver offers at most `MAX_REGIONS + 1` regions, currently 65: a
    /// gathering write's descriptor list is capped at `MAX_REGIONS`, and one live session
    /// block may ride as its trailing region. A pass carrying no handed-over payloads offers
    /// at most two — one accumulated run beside one live block, which is all the block
    /// lifetime permits — and only records grow the list beyond that. The contract itself
    /// imposes no count, so an implementation should not assume any particular one. No region
    /// is ever empty.
    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [std::io::IoSlice<'w>],
    ) -> impl Future<Output = std::io::Result<usize>> + 'w;
}

/// Writing an owned list of regions as one gathering operation — the completion model's
/// zero-copy write.
///
/// Available only to a writer whose [`Strategy`](TransportWrite::Strategy) is a
/// [`CompletionStrategy`], which is to say [`OwnedRegions`].
///
/// # Why this takes ownership when [`VectoredWrite`] does not
///
/// A completion API hands the kernel its buffers and gets them back only when the operation
/// finishes, so it cannot lend the borrowed [`IoSlice`](std::io::IoSlice)s
/// [`VectoredWrite`] deals in — `compio`'s `IoVectoredBuf: 'static` bound is exactly this
/// constraint, spelled in the type system. It *can* gather a list of owned [`Bytes`], which
/// is what this writes.
///
/// Ownership passes in and back out. The driver reuses one growable allocation across
/// passes rather than building a fresh list each time, and never loses the regions to an
/// error.
pub trait RegionWrite: TransportWrite
where
    Self::Strategy: CompletionStrategy,
{
    /// Writes an owned list of regions as one gathering operation, returning the list so the
    /// driver can reuse its allocation.
    ///
    /// # What the regions are
    ///
    /// A sequence of owned octet runs to be written **in order**, as one operation, exactly
    /// as `writev` would — the session's blocks coalesced into driver-owned buffers (all of
    /// them: unlike [`VectoredWrite`] there is no size threshold, because a block borrowed
    /// from the session cannot be owned without a copy), and each handed-over payload as its
    /// own region in the caller's own memory, uncopied. No region is ever empty, and the
    /// list holds at most the driver's region cap (currently `MAX_REGIONS`, 64).
    ///
    /// # Short writes
    ///
    /// The accepted count may be less than the total. A short write is normal; the driver
    /// drops the fully written regions from the front of the list it gets back and advances
    /// the first partial one — both free, since [`Bytes`] is a view — then offers the
    /// remainder again. As on every other strategy, an accepted write of zero octets is an
    /// error rather than something to spin on.
    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (std::io::Result<usize>, Vec<Bytes>)>;
}

/// The driver state for one write pass, handed to [`Elects::drain`].
///
/// Public because [`Elects`] is, opaque because its contents are this crate's session and
/// buffers. A downstream crate can name this type but cannot construct one, which is what
/// keeps [`Elects::drain`] uncallable from outside even though it is a public method.
pub struct Pass<'a> {
    // Reached as field paths (`pass.inner.session`, `pass.inner.buffers`) and never through
    // accessor methods. Accessors would collapse the disjoint-field borrows the drain loop
    // depends on — it holds `&mut` to the session and to several buffers at once — and the
    // loop would stop compiling.
    pub(crate) inner: crate::http::driver::PassInner<'a>,
}

/// Resolves a [`WriteStrategy`] to the driver code that runs it.
///
/// Implemented by this crate for each strategy, over exactly the writers that supply that
/// strategy's operations — `Gathering` implements it only `where W: VectoredWrite`, and so
/// on. That is the mechanism behind
/// [`TransportWrite::Strategy`]'s guarantee: declaring a strategy
/// whose operations you have not written leaves this bound unsatisfiable, and the
/// `TransportWrite` impl itself fails to compile.
///
/// Sealed, and not something to implement or call. It appears in the public API only
/// because it is named in a public bound.
pub trait Elects<W: ?Sized>: sealed::Sealed {
    /// Per-connection state this strategy needs, resolved once by
    /// [`prepare`](Elects::prepare).
    #[doc(hidden)]
    type State;

    /// Resolves this strategy's per-connection state, once, just after the transport is
    /// split.
    ///
    /// This is the only place in the design where a transport capability may be consulted,
    /// and it happens once per connection.
    #[doc(hidden)]
    fn prepare(writer: &W) -> Self::State;

    /// Drains one pass.
    #[doc(hidden)]
    fn drain<'a>(
        writer: &'a mut W,
        state: &'a Self::State,
        pass: Pass<'a>,
    ) -> impl Future<Output = super::error::Result<()>> + 'a;
}
