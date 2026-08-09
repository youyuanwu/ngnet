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
//! For *reading*, completion-shaped traits are the superset, and [`TransportRead`] is
//! shaped that way for every transport. A readiness-based transport implements it with no
//! copy at all: take the buffer, read into it, hand it back. The reverse does not work — a
//! completion-based transport behind a borrowed-buffer trait needs a stable buffer of its
//! own plus a copy out of it.
//!
//! For *writing* that argument does not carry, and the traits no longer pretend it does.
//! Being a superset made the owned write universal, but a readiness transport can never
//! *use* the ownership: this crate's own tokio writer took the `Bytes` and immediately took
//! a reference to it, and the driver had to manufacture that ownership out of its own reused
//! coalescing buffer to feed it. So the write primitive belongs to the model rather than to
//! [`TransportWrite`]: [`BorrowedWrite`] lends, [`RegionWrite`] owns, and neither transport
//! is asked for a shape its API cannot produce.
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
//! A writer names one **I/O model** as an associated type, and that is all the *model* costs
//! it. The model says who owns the buffer — nothing about how many writes a pass should
//! become. Naming a model obliges the writer, by compiler error, to supply that model's
//! primitive: for the primitive there is no probe and no flag that could fall out of step
//! with the method, because the compiler will not let the two disagree.
//!
//! *How many writes* is a separate question, and the transport answers it — once. A writer
//! declares through [`is_write_vectored`](TransportWrite::is_write_vectored) whether its
//! gathering operation is *efficient*, meaning it reaches a real scatter-gather write rather
//! than a loop. This layer asks that question once per connection, when it splits the
//! transport and before it writes an octet, and routes every pass of that connection's life
//! on the answer. The caller is not consulted; there is no configuration knob for it,
//! because the caller does not know the answer and the transport does.
//!
//! Unlike the model, this *is* a flag that can fall out of step with the method it describes:
//! nothing makes a writer that overrides its gathering operation declare `true`, or stops one
//! that does not from declaring it anyway. That is deliberate — the declaration is about
//! *efficiency*, which the compiler cannot see, and an override is not proof of it — but it
//! means a declaration is a claim to be checked by a test rather than a fact the type system
//! guarantees.
//!
//! | [`Model`](TransportWrite::Model) | primitive the writer must supply | gathering operation | [`is_write_vectored`](TransportWrite::is_write_vectored) `== true` — **gathered** | `== false` — **coalesced** |
//! | --- | --- | --- | --- | --- |
//! | [`Readiness`] | [`write_borrowed`](BorrowedWrite::write_borrowed) | [`write_vectored`](BorrowedWrite::write_vectored), **defaulted** | one per large block and per region-cap flush, plus at most one for the remainder | one per pass, lending the buffer |
//! | [`Completion`] | [`write_owned`](RegionWrite::write_owned) | [`write_regions`](RegionWrite::write_regions), **defaulted** | one per region-cap flush, plus one for the remainder | one per pass, handing the buffer over |
//!
//! Each model asks for exactly one write primitive, and it is the one that model can
//! actually use: a readiness transport borrows, a completion transport owns. Neither is
//! offered the other's. The owned write used to sit on [`TransportWrite`] where both
//! inherited it, which obliged every readiness transport to accept a buffer it could only
//! take a reference to — and obliged the coalescing drain to manufacture that ownership out
//! of a buffer the driver already owned. [`commit`](TransportWrite::commit) is the one
//! write-side operation still common to both, because when octets become peer-visible is a
//! question about buffering rather than about ownership.
//!
//! Gathering exists because the two extremes are each wrong for half of the traffic: under
//! multiplexing a pass is dozens of tiny blocks, where one write per block is the dominant
//! cost, and with a large body it is a handful of 16 KiB blocks, where copying them all to
//! save three syscalls is the dominant cost. Gathering small blocks into a buffer the driver
//! owns while handing large ones to the socket uncopied gets both. That is the gathered
//! drain, and it is what a writer declaring `true` receives.
//!
//! # Every transport can gather; not every transport gathers *well*
//!
//! Both gathering operations are **provided methods**. A writer that overrides neither can
//! still be asked to gather, and will produce the right octets: the default loops over the
//! model's primitive, writing each region in turn, in order. A writer whose underlying I/O
//! really does scatter-gather overrides the default and reaches one `writev`.
//!
//! Correctness and efficiency therefore come apart, and
//! [`is_write_vectored`](TransportWrite::is_write_vectored) is the seam. It does not ask
//! *can you gather* — everyone can, that is what "provided" means — it asks *is your
//! gathering worth calling*. A writer that leaves the emulation in place should answer `no`,
//! and this layer will then coalesce the pass into one buffer and spend a single write,
//! rather than paying the emulation's one-write-per-region for a gather that was never
//! there.
//!
//! This is the same shape as [`std::io::Write`], where `write` is required and
//! `write_vectored` is provided in terms of it — and the default here is the stricter one:
//! `std`'s writes only the *first* non-empty buffer, ours writes all of them.
//!
//! The consequence worth stating plainly is that **the borrowed primitive must be real, not a
//! stub**, even for a writer that overrides the gathering operation. It is the emulation's
//! only foothold, and a writer that stubs it will appear to work right up until the override
//! is removed.
//!
//! Emulating costs one write per region rather than one per pass. It is bounded: this
//! layer accumulates sub-threshold blocks into a single run *before* any write happens, so a
//! multiplexed pass of hundreds of small blocks collapses to one region and one write whether
//! or not the transport gathers natively. The region list grows only with handed-over
//! no-copy payloads, and is capped.
//!
//! # One model, enforced rather than encouraged
//!
//! The two models serve disjoint populations, and the reason is ownership, not taste.
//! [`Readiness`] lends borrowed slices; [`Completion`] owns its buffers because the kernel may
//! still be writing from them after the submitting future is dropped. The type system carries
//! that: [`BorrowedWrite`] is available only to a writer whose model is a [`ReadinessModel`],
//! [`RegionWrite`] only to one whose model is a [`CompletionModel`], and no model is both. A
//! writer therefore *cannot* implement operations from both models — it is a compile error,
//! not a convention.
//!
//! Which to prefer, if a transport could genuinely do either: [`Readiness`] over
//! [`Completion`], because it need not mint an owned [`Bytes`] per frame header. That is
//! advice at the point of declaration rather than arbitration at run time, because there is
//! nothing left to arbitrate.
//!
//! # One capability, read once
//!
//! [`is_write_vectored`](TransportWrite::is_write_vectored) is the only thing this layer asks
//! a transport at run time, and it asks it once per connection, before the first write. It is
//! not a correctness question — this layer re-offers the remainder of a short write, so a
//! writer that answers wrongly in either direction still moves every octet in order — it is a
//! performance question, and the answer is cached for the connection's life because it cannot
//! change: it is a property of the underlying I/O, not of the connection's state.
//!
//! A revision between the two asked nothing at all, on the reasoning that a transport's
//! answer was already implicit in whether it overrode the default. That reasoning does not
//! survive contact with the two drains. The override is invisible from here — a provided
//! method and an overridden one are the same call — so "answered by whether it overrides"
//! was an answer nobody could read. What the layer actually did in its absence was assume
//! gathering was worth calling for everyone, and hand a non-gathering readiness transport to
//! the emulation, which cost one write per region for a gather that did not exist. The
//! capability makes that answer legible, and the default (`false`, see the method's own
//! docs) makes the conservative reading the one you get for free.
//!
//! The question is deliberately shaped like [`AsyncWrite::is_write_vectored`]: a plain
//! `&self` returning `bool`, answerable without I/O, meaning *efficient* rather than
//! *available*. Callers of tokio's method coalesce when it returns `false`; so does this
//! layer, for the same reason and to the same effect.
//!
//! [`AsyncWrite::is_write_vectored`]:
//!     https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html#method.is_write_vectored
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
    /// Closes the model set to this crate, so the driver's handling of it is exhaustive by
    /// construction and a downstream crate cannot invent a third.
    pub trait Sealed {}
}

/// The readiness I/O model: writes lend a borrowed buffer for the duration of the call.
///
/// tokio, `futures-io`, and anything built on non-blocking sockets. A writer declaring this
/// model supplies [`BorrowedWrite`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Readiness;

/// The completion I/O model: writes take ownership of their buffers.
///
/// `io_uring`, IOCP, and the runtimes built on them. The kernel may still be writing from a
/// buffer after the future that submitted the operation is dropped, so the buffer cannot be
/// lent. A writer declaring this model supplies [`RegionWrite`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Completion;

/// One of the two I/O models a transport's writes can follow.
///
/// Sealed: the two models in this module are all there are. Which of them a writer declares
/// settles *who owns the buffer*; how many writes a pass becomes is settled separately, by
/// the writer's answer to [`is_write_vectored`](TransportWrite::is_write_vectored).
pub trait WriteModel: sealed::Sealed {}

/// A model whose writes lend borrowed buffers, and so belong to a readiness-based transport.
///
/// Implemented by [`Readiness`] alone. This bound is what makes the two I/O models mutually
/// exclusive: [`BorrowedWrite`] requires it, [`RegionWrite`] requires [`CompletionModel`],
/// and no model implements both.
///
/// It is a distinct trait rather than a `Model = Readiness` equality bound so that the
/// mutual exclusion is stated once, in one place, and so that a third readiness-shaped model
/// could be added without rewriting every bound in the crate.
pub trait ReadinessModel: WriteModel {}

/// A model whose writes take ownership of their buffers, and so belong to a completion-based
/// transport.
///
/// Implemented by [`Completion`] alone. See [`ReadinessModel`] for why this matters.
pub trait CompletionModel: WriteModel {}

impl sealed::Sealed for Readiness {}
impl sealed::Sealed for Completion {}

impl WriteModel for Readiness {}
impl WriteModel for Completion {}

impl ReadinessModel for Readiness {}

impl CompletionModel for Completion {}

/// Emulates a gathering write by writing each region in turn through
/// [`BorrowedWrite::write_borrowed`].
///
/// The single implementation of readiness-side emulation in this crate: it backs
/// [`BorrowedWrite::write_vectored`]'s default body, and an adapter that gathers only
/// sometimes calls it directly for the other branch rather than writing a second copy.
///
/// # Contract
///
/// Regions are written in order. Empty regions are skipped. A short write **stops** — the
/// running total is returned and the next region is not attempted — because retrying is the
/// driver's job and doing it here as well would mean two nested authorities on short writes.
/// An accepted zero likewise stops, returning what was written so far, which the driver
/// converts into an error rather than spinning. An empty list returns `Ok(0)` without
/// writing.
pub(crate) async fn emulate_gathering<W>(
    writer: &mut W,
    regions: &[std::io::IoSlice<'_>],
) -> std::io::Result<usize>
where
    W: BorrowedWrite + ?Sized,
    W::Model: ReadinessModel,
{
    let mut total = 0;
    for region in regions {
        if region.is_empty() {
            continue;
        }
        let written = writer.write_borrowed(region).await?;
        total += written;
        if written < region.len() {
            break;
        }
    }
    Ok(total)
}

/// Emulates a gathering write of owned regions by writing each in turn through
/// [`RegionWrite::write_owned`].
///
/// The completion-side counterpart of [`emulate_gathering`], backing
/// [`RegionWrite::write_regions`]'s default body. Same contract: in order, short write stops,
/// empty list writes nothing.
///
/// The list is returned alongside the count so the driver keeps its allocation, exactly as a
/// native implementation would return it.
pub(crate) async fn emulate_region_gathering<W>(
    writer: &mut W,
    mut regions: Vec<Bytes>,
) -> (std::io::Result<usize>, Vec<Bytes>)
where
    W: RegionWrite + ?Sized,
    W::Model: CompletionModel,
{
    let mut total = 0;
    for index in 0..regions.len() {
        // Take the region out so `write_owned` can own it, and put it back where it came from.
        let region = core::mem::replace(&mut regions[index], Bytes::new());
        if region.is_empty() {
            regions[index] = region;
            continue;
        }
        let len = region.len();
        let (result, region) = writer.write_owned(region).await;
        regions[index] = region;
        match result {
            Ok(written) => {
                total += written;
                if written < len {
                    break;
                }
            }
            Err(error) => return (Err(error), regions),
        }
    }
    (Ok(total), regions)
}

/// The writing half of a transport.
///
/// Every writer names a [`Model`](TransportWrite::Model), and that is what obliges it to
/// implement that model's trait — which is where its write primitive lives. See the
/// [module documentation](self#how-a-pass-gets-drained) for the table. Naming a model does
/// *not* oblige a writer to implement that trait's gathering operation, which is provided.
///
/// # Declaring a model whose trait you have not implemented will not compile
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{Readiness, TransportWrite};
///
/// struct ClaimsWithoutWriting;
/// impl TransportWrite for ClaimsWithoutWriting {
///     // `Readiness` requires `BorrowedWrite`, which this type does not implement.
///     type Model = Readiness;
/// }
/// ```
///
/// The same holds on the completion side, so neither model has a way to declare itself
/// without supplying what it needs — there, the missing impl also means a missing
/// [`write_owned`](RegionWrite::write_owned).
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{Completion, TransportWrite};
///
/// struct ClaimsRegionsWithoutWriting;
/// impl TransportWrite for ClaimsRegionsWithoutWriting {
///     // `Completion` requires `RegionWrite`, which this type does not implement.
///     type Model = Completion;
/// }
/// ```
///
/// # A path cannot be withdrawn mid-pass
///
/// Older traits let a writer decline a fast path per call, by returning `None` from an
/// `Option`-returning method. The operations are no longer `Option`-shaped, so a writer that
/// tries to keep declining does not typecheck — it must report through its result instead.
///
/// ```compile_fail,E0053
/// use ngnet_h2::http::transport::{BorrowedWrite, Readiness, TransportWrite};
/// use core::future::Future;
///
/// struct Withdraws {
///     healthy: bool,
/// }
/// impl TransportWrite for Withdraws {
///     type Model = Readiness;
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
/// # The model set is closed
///
/// [`WriteModel`] is sealed, so a downstream crate cannot invent a third model and the
/// driver's handling of the two is exhaustive by construction.
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::WriteModel;
///
/// struct MyOwnModel;
/// impl WriteModel for MyOwnModel {}
/// ```
pub trait TransportWrite {
    /// Which I/O model this writer follows — who owns the buffer a write is given.
    ///
    /// This settles ownership, and nothing else. How many writes a pass becomes is settled
    /// separately by [`is_write_vectored`](TransportWrite::is_write_vectored), which is a
    /// statement about efficiency rather than about ownership; the two are orthogonal, and
    /// each of the four combinations is reachable.
    ///
    /// Stable Rust has no defaults for associated types, so even the simplest transport
    /// writes this line — `type Model = Readiness;` — costing one line for the guarantee
    /// that a declared model is always a supplied one. Beyond it each model asks for one
    /// write method and no more: the borrowed primitive on the readiness side, the owned one
    /// on the completion side. Both models' gathering operations are provided in terms of
    /// that primitive.
    ///
    /// This trait itself carries no write at all. It used to carry the owned one, which
    /// meant every readiness transport had to accept a buffer it could only borrow from —
    /// `TokioWriter`'s implementation took ownership and immediately took a reference. The
    /// write primitive belongs to the model because *who owns the buffer* is what the model
    /// is; only [`commit`](TransportWrite::commit), which is about buffering rather than
    /// ownership, is common to both.
    type Model: WriteModel + Drains<Self>;

    /// Commits everything written so far to the peer-visible byte stream.
    ///
    /// The driver guarantees it calls this once it has drained a write pass and before it
    /// parks awaiting readable input: it never waits on the peer while octets it has
    /// produced are still sitting in a transport-side buffer. An implementation whose
    /// writes are peer-visible the moment the model's write returns — a raw
    /// socket, a completion transport, the in-memory duplex — has nothing to do here, which
    /// is why the default does nothing. One that buffers, such as a `BufWriter` or a
    /// `BufStream`, must flush that buffer here; otherwise the driver awaits a response to a
    /// request the peer never received, and the connection silently hangs.
    ///
    /// This is the one write-side operation both models share, which is why it lives here
    /// rather than on either model's trait: when the octets become peer-visible is a
    /// property of the transport's buffering, not of who owns the buffer they were written
    /// from.
    fn commit(&mut self) -> impl Future<Output = std::io::Result<()>> {
        async { Ok(()) }
    }

    /// Whether this writer has an *efficient* gathering implementation.
    ///
    /// This is the crate's counterpart to [`tokio::io::AsyncWrite::is_write_vectored`], and
    /// it is deliberately the same question with the same contract. A writer that overrides
    /// its model's gathering operation —
    /// [`write_vectored`](BorrowedWrite::write_vectored) on the readiness side,
    /// [`write_regions`](RegionWrite::write_regions) on the completion side — with one that
    /// really does hand every region to the operating system in a single call should return
    /// `true`. A writer that leaves the provided emulation in place should return `false`,
    /// because that emulation is a loop over the single-buffer primitive: it is *correct*,
    /// but it is not gathering, and a caller that believes otherwise pays one write per
    /// region for nothing.
    ///
    /// The driver asks this exactly once per connection, immediately after splitting the
    /// transport and before any octet is written, and routes every write pass for that
    /// connection's whole life accordingly: `true` takes the gathered drain, `false` takes
    /// the coalesced drain, which packs the pass into one contiguous buffer and spends a
    /// single write on it. Because the answer is taken once and cached, it must not depend
    /// on connection state, and it must be answerable without I/O — hence a plain `&self`
    /// returning `bool` rather than a future or an `Option`.
    ///
    /// # Why the default is `false`
    ///
    /// The provided default returns `false`, which **inverts** this crate's previous stance:
    /// the `gathers()` method of an earlier revision defaulted to `true`, on the reasoning
    /// that every readiness transport gathers *somehow* because the emulation is always
    /// available. That reasoning conflates availability with efficiency. The emulation is
    /// always available, and it is never efficient; defaulting to `true` therefore told the
    /// layer above that a loop of N single-buffer writes was a gather, and the layer above
    /// believed it.
    ///
    /// Defaulting to `false` makes the wrong answer the conservative one. A transport that
    /// forgets to override this gets one write and a copy — never optimal, always correct
    /// and never surprising. A transport that overrides it wrongly, claiming an efficiency
    /// it does not have, gets N writes where it was promised one. Of those two failure
    /// modes the first is the one to make silent, which is the same judgement tokio makes.
    ///
    /// [`tokio::io::AsyncWrite::is_write_vectored`]:
    ///     https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html#method.is_write_vectored
    fn is_write_vectored(&self) -> bool {
        false
    }
}

/// The readiness model's writes: a borrowed primitive, and gathering built from it.
///
/// Available only to a writer whose [`Model`](TransportWrite::Model) is a
/// [`ReadinessModel`]. A completion transport cannot implement this at all: the kernel may
/// still be writing from the buffer after the submitting future is dropped, so a borrowed
/// slice is unsound there, and the bound below makes the attempt a compile error rather than
/// a documented warning.
///
/// [`write_borrowed`](BorrowedWrite::write_borrowed) is required;
/// [`write_vectored`](BorrowedWrite::write_vectored) is provided in terms of it. That is the
/// same division [`std::io::Write`] makes, and it means every readiness transport gathers
/// whether or not it says anything about gathering.
///
/// # A writer cannot implement both models
///
/// ```compile_fail,E0277
/// use ngnet_h2::http::transport::{BorrowedWrite, Completion, RegionWrite, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
///
/// struct BothModels;
/// impl TransportWrite for BothModels {
///     type Model = Completion;
/// }
/// impl RegionWrite for BothModels {
///     async fn write_owned(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// // `Completion` is not a `ReadinessModel`, so this impl cannot exist.
/// impl BorrowedWrite for BothModels {
///     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> std::io::Result<usize> {
///         Ok(data.len())
///     }
/// }
/// ```
pub trait BorrowedWrite: TransportWrite
where
    Self::Model: ReadinessModel,
{
    /// Writes the borrowed slice, returning how many octets were accepted.
    ///
    /// A short write is normal and the driver re-offers the remainder. An accepted write of
    /// zero octets is an error rather than something to spin on.
    ///
    /// There is no way to decline: the model was settled when the writer declared it, and a
    /// writer that cannot complete a write reports so through this result — a short count or
    /// an error — never by refusing the path.
    ///
    /// **This must be a real write even if [`write_vectored`](BorrowedWrite::write_vectored)
    /// is overridden.** It is the primitive gathering is emulated from, and a stub here is a
    /// latent stream corruption that only surfaces when the override is removed or bypassed.
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + 'w;

    /// Writes `regions` in order, as one operation, exactly as `writev` would.
    ///
    /// The return is the number of octets accepted across the whole sequence; a short write
    /// is normal and the driver re-offers what remains. An accepted write of zero octets is
    /// an error.
    ///
    /// # The default emulates, and that is a real implementation
    ///
    /// If not overridden, this writes each region in turn through
    /// [`write_borrowed`](BorrowedWrite::write_borrowed), in order, stopping at the first
    /// short write and returning the running total for the driver to resume from. Every
    /// octet arrives, in order; the cost is one write per region rather than one per pass.
    ///
    /// That default is stricter than [`std::io::Write`]'s, which writes only the first
    /// non-empty buffer.
    ///
    /// **Override this when the underlying stream really scatter-gathers**, and only then.
    /// A stream whose vectored write moves just the first region — as tokio's default
    /// `poll_write_vectored` does — should be left to the default, or dispatched to it, which
    /// is what the `tokio` feature's writer does after asking the stream once through
    /// `is_write_vectored`.
    ///
    /// # Regions
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
    ) -> impl Future<Output = std::io::Result<usize>> + 'w {
        emulate_gathering(self, regions)
    }
}

/// The completion model's writes: an owned region list, gathered.
///
/// Available only to a writer whose [`Model`](TransportWrite::Model) is a
/// [`CompletionModel`], which is to say [`Completion`].
///
/// # Why this takes ownership when [`BorrowedWrite`] does not
///
/// A completion API hands the kernel its buffers and gets them back only when the operation
/// finishes, so it cannot lend the borrowed [`IoSlice`](std::io::IoSlice)s
/// [`BorrowedWrite`] deals in — `compio`'s `IoVectoredBuf: 'static` bound is exactly this
/// constraint, spelled in the type system. It *can* gather a list of owned [`Bytes`], which
/// is what this writes.
///
/// Ownership passes in and back out. The driver reuses one growable allocation across passes
/// rather than building a fresh list each time, and never loses the regions to an error.
///
/// # What a completion transport must supply
///
/// One method: [`write_owned`](RegionWrite::write_owned), the owned counterpart of
/// [`BorrowedWrite::write_borrowed`]. [`write_regions`](RegionWrite::write_regions) is
/// provided in terms of it, emulating the gathering write exactly as
/// [`BorrowedWrite::write_vectored`]'s default does over the borrowed primitive. A transport
/// whose runtime submits a real vectored write overrides it; one that does not need write
/// nothing else.
///
/// # The old shape does not silently compile
///
/// A completion transport written against the previous trait put its owned write on
/// [`TransportWrite`] and left `impl RegionWrite for X {}` empty. That now fails twice over —
/// `E0407` for a `write` that is not a member of `TransportWrite`, and the `E0046` below for
/// the `write_owned` the `RegionWrite` impl no longer supplies. The migration is to move the
/// body across verbatim and rename it; the signature is unchanged.
///
/// ```compile_fail,E0046
/// use ngnet_h2::http::transport::{Completion, RegionWrite, TransportWrite};
/// use ngnet_h2::http::testing::bytes_crate::Bytes;
///
/// struct OldShape;
/// impl TransportWrite for OldShape {
///     type Model = Completion;
///     // The owned write no longer belongs here.
///     async fn write(&mut self, buf: Bytes) -> (std::io::Result<usize>, Bytes) {
///         let n = buf.len();
///         (Ok(n), buf)
///     }
/// }
/// // ...so this impl is missing `write_owned`.
/// impl RegionWrite for OldShape {}
/// ```
///
/// This method used to live on [`TransportWrite`], where both models inherited it, and the
/// minimal completion transport was correspondingly the empty block
/// `impl RegionWrite for MyType {}`. That arrangement obliged every *readiness* transport to
/// accept an owned buffer as well, which
/// no readiness transport can use — it can only borrow from it — so the ownership transfer
/// was manufactured for them and then thrown away. Moving the primitive here costs the
/// completion side a method it was already writing under another name, and relieves the
/// readiness side of one it never wanted.
pub trait RegionWrite: TransportWrite
where
    Self::Model: CompletionModel,
{
    /// Writes `buf`, returning it along with how many octets were written.
    ///
    /// The completion model's primitive, and the one operation a completion transport must
    /// supply. Ownership passes in and comes back, and comes back even on failure — the
    /// kernel may still be writing from the buffer after the submitting future is dropped,
    /// which is why this model cannot borrow and why the buffer has to survive the call.
    ///
    /// "Written" does not have to mean "handed to the peer" the instant this returns: a
    /// buffering transport may hold the octets, so long as it releases them no later than
    /// [`commit`](TransportWrite::commit), which the driver calls before it waits on the
    /// peer.
    ///
    /// A short write is normal and the driver re-offers the remainder. An accepted write of
    /// zero octets is an error rather than something to spin on.
    fn write_owned(&mut self, buf: Bytes) -> impl Future<Output = (std::io::Result<usize>, Bytes)>;

    /// Writes an owned list of regions as one gathering operation, returning the list so the
    /// driver can reuse its allocation.
    ///
    /// # What the regions are
    ///
    /// A sequence of owned octet runs to be written **in order**, as one operation, exactly
    /// as `writev` would — the session's blocks coalesced into driver-owned buffers (all of
    /// them: unlike the readiness side there is no size threshold, because a block borrowed
    /// from the session cannot be owned without a copy), and each handed-over payload as its
    /// own region in the caller's own memory, uncopied. No region is ever empty, and the list
    /// holds at most the driver's region cap (currently `MAX_REGIONS`, 64).
    ///
    /// # Short writes
    ///
    /// The accepted count may be less than the total. A short write is normal; the driver
    /// drops the fully written regions from the front of the list it gets back and advances
    /// the first partial one — both free, since [`Bytes`] is a view — then offers the
    /// remainder again. As everywhere else, an accepted write of zero octets is an error
    /// rather than something to spin on.
    ///
    /// # The default emulates
    ///
    /// If not overridden, this hands each region to [`write_owned`](RegionWrite::write_owned)
    /// in turn,
    /// stopping at the first short write and returning the running total. Every octet
    /// arrives, in order; the cost is one write per region rather than one per pass. Override
    /// it when the runtime offers a real vectored submission.
    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (std::io::Result<usize>, Vec<Bytes>)> {
        emulate_region_gathering(self, regions)
    }
}

/// The driver state for one write pass, handed to [`Drains::drain`].
///
/// Public because [`Drains`] is, opaque because its contents are this crate's session and
/// buffers. A downstream crate can name this type but cannot construct one, which is what
/// keeps [`Drains::drain`] uncallable from outside even though it is a public method.
pub struct Pass<'a> {
    // Reached as field paths (`pass.inner.session`, `pass.inner.buffers`) and never through
    // accessor methods. Accessors would collapse the disjoint-field borrows the drain loop
    // depends on — it holds `&mut` to the session and to several buffers at once — and the
    // loop would stop compiling.
    pub(crate) inner: crate::http::driver::PassInner<'a>,
}

/// Resolves a [`WriteModel`] to the driver code that drains a pass over it.
///
/// Implemented by this crate for each model, over exactly the writers that supply that
/// model's trait — [`Readiness`] implements it only `where W: BorrowedWrite`, and
/// [`Completion`] only `where W: RegionWrite`. That is the mechanism behind
/// [`TransportWrite::Model`]'s guarantee: declaring a model whose trait you have not
/// implemented leaves this bound unsatisfiable, and the `TransportWrite` impl itself fails to
/// compile.
///
/// It carries no per-connection state and does not ask the writer anything itself. Its one
/// runtime input is the `vectored` flag: the answer the writer gave to
/// [`TransportWrite::is_write_vectored`] when the driver split the transport, which the
/// driver resolved once and then holds for the connection's life. `true` selects the
/// gathered drain, `false` the coalesced one. No drain re-asks the writer, and none
/// re-derives the answer.
///
/// The flag is a `bool` and not a richer type on purpose. A named enum here would have to be
/// public, because this method is — and this crate has no honest public constructor to offer
/// for one, since the value is produced inside the driver from the transport's own answer
/// and never by a caller. The result would be a public enum that exists only to be a
/// parameter nobody outside can supply: nameable, unconstructable, and load-bearing for
/// nothing. A `bool` says exactly as much as there is to say — the writer answered a yes/no
/// question — and leaves no vestigial type in the public surface. The parameter is named
/// `vectored` rather than typed so that the name carries the meaning the type would have.
///
/// Sealed, and not something to implement or call. It appears in the public API only because
/// it is named in a public bound.
pub trait Drains<W: ?Sized>: sealed::Sealed {
    /// Drains one pass, gathering if `vectored` and coalescing otherwise.
    #[doc(hidden)]
    fn drain<'a>(
        writer: &'a mut W,
        vectored: bool,
        pass: Pass<'a>,
    ) -> impl Future<Output = super::error::Result<()>> + 'a;
}
