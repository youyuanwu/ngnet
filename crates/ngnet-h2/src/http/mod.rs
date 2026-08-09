//! An asynchronous HTTP/2 API over the sans-I/O core.
//!
//! Enabled by the default `http` feature. Disabling it returns the crate to a pure
//! state machine with exactly one dependency and no I/O of any kind.
//!
//! Everything in this subtree is confined to it: nothing outside `src/http/` acquires an
//! async facility, and a structural test enforces that. It contains no `unsafe` of its
//! own. The sans-I/O core is unchanged and remains usable on its own.
//!
//! # Shape
//!
//! A client connection is two objects. [`handshake`] hands back a cloneable handle for
//! making requests and a driver future that moves octets. A server connection is one:
//! [`serve`] takes a handler and hands back the driver. Nothing happens until the driver
//! is polled, and where it is polled is entirely the caller's business — this crate spawns
//! nothing and takes no executor, spawner or timer. Until it is polled no request is sent,
//! no response arrives, and a [`ResponseFuture`] never resolves;
//! dropping it fails every exchange it was carrying. It is [`#[must_use]`](Connection) for
//! exactly that reason.
//!
//! Both ends share one driver. What differs between them is small and named: where work
//! comes from, what a completed header block means, and when there is nothing left. Reads,
//! writes, flow control, buffer reuse and the park predicate are the same code at both
//! ends rather than the same idea written twice.
//!
//! Server handlers run concurrently without being spawned: they are futures the driver
//! holds, each woken by a waker naming its own stream. A handler that *blocks* rather than
//! returning `Pending` stalls its whole connection, since there is no other thread for the
//! connection to be on — see [`server`] for what to do about that.
//!
//! # What must be `Send`
//!
//! The transport need not be, deliberately: the completion-based runtimes this layer
//! exists to serve are thread-per-core and build their I/O on `Rc`. Auto traits propagate
//! instead, so a driver over a `Send` transport is `Send` without anything declaring it.
//!
//! Outgoing bodies are the exception. They are stored inside the session, which may be
//! moved between threads, so they inherit the sans-I/O core's `Send + 'static` bound — the
//! bound is on [`handshake`] and [`serve`], not on the transport. A caller whose body
//! producer is not `Send` bridges it into one that is: move the producer onto its own task
//! and let the body be the receiving end of a channel, which is `Send` whatever feeds it.
//! Received bodies carry no such bound; only the ones this crate must hold do.
//!
//! # Panics, and why the two layers differ
//!
//! A panic in a server handler and a panic in a sans-I/O callback do not end the same way,
//! and the difference is structural rather than a choice.
//!
//! A handler is an ordinary future the driver polls on its own task. A
//! panic in it unwinds through that poll and out of the driver, failing the connection —
//! every stream on it goes with the driver, which is the same outcome as dropping it.
//!
//! A caller's message body is different. The session pulls it synchronously from inside an
//! `extern "C"` callback, so a panic in a body's `poll_frame` — like a panic in any
//! sans-I/O callback — crosses the C frame libnghttp2 is executing inside, and unwinding
//! out of `extern "C"` is defined to **abort the process**. This is the sans-I/O core's
//! documented contract; the async layer inherits it wherever it hands the session a
//! caller's code to run. A body that might fail should return an error, not panic.
//!
//! # Cancellation
//!
//! Dropping a [`ResponseFuture`] before it resolves, or dropping
//! an unread response [`IncomingBody`], resets that stream: the peer is told to stop, and
//! its window is returned. A *server's* request body is exempt — a handler that ignores
//! the body it was given still has a response to make, so dropping it resets nothing.
//! [`SendRequest::shutdown`] is the connection-wide form:
//! it sends `GOAWAY`, refuses new requests, and lets the ones already in flight finish.
//!
//! A write in flight is not cancellable piecemeal. The driver awaits each write within a
//! pass, and a stream reset goes out as a later frame rather than by tearing the transport
//! out from under an outstanding write. The only thing that cancels a write is dropping
//! the whole driver, which drops the in-flight write future.
//!
//! The two models reach the same safety by opposite routes, which is why the write primitive
//! belongs to the model rather than to the transport trait. On the completion model,
//! ownership of the buffer passes *into* the transport for the duration of the call
//! ([`RegionWrite::write_owned`](transport::RegionWrite::write_owned) takes an owned
//! [`bytes::Bytes`]), so dropping the future
//! never leaves the kernel writing into memory this crate has reclaimed — the operation may
//! outlive the future, so the buffer must too. On the readiness model nothing outlives the
//! future: [`BorrowedWrite::write_borrowed`](transport::BorrowedWrite::write_borrowed) lends a
//! slice, the runtime copies out of it or
//! registers interest and returns, and there is no submitted operation left holding the
//! memory when the future is dropped. A borrow is sound there precisely because the model
//! guarantees the write is over when the future is.
//!
//! # Writing a transport for another runtime
//!
//! A runtime this crate ships no adapter for is a short job, not a blocked one. Implement
//! [`Transport`] — one method, [`split`](Transport::split), dividing the stream into a
//! reader and a writer so the two directions can proceed at once — then [`TransportRead`],
//! [`TransportWrite`], and the write trait for your I/O model.
//!
//! [`TransportRead`] is ownership-passing: the buffer goes in and comes back, which a
//! completion API (`io_uring`, IOCP) needs and a readiness API (tokio, `futures-io`)
//! satisfies with no copy. **The read side was deliberately not split** — a read must be
//! given somewhere to put the octets, and only the caller knows where, so ownership is the
//! honest shape on both models.
//!
//! Writing is not, because a write already has its octets and the only question is who owns
//! them for the duration. [`TransportWrite`] therefore carries no write primitive at all —
//! only `type Model` and `commit`. The primitive comes
//! from the model: implement
//! [`BorrowedWrite::write_borrowed`](transport::BorrowedWrite::write_borrowed) for a
//! readiness transport, which lends the driver's buffer and copies nothing, or
//! [`RegionWrite::write_owned`](transport::RegionWrite::write_owned) for a completion one,
//! which takes ownership because the kernel may still be reading the buffer after the future
//! is dropped. Declaring a model obliges you to implement that model's trait, by compiler
//! error.
//!
//! The writer additionally names its *I/O model* as an associated type — readiness or
//! completion — which settles who owns the buffer a write is given, and answers one yes/no
//! question, [`TransportWrite::is_write_vectored`],
//! which settles whether its gathering operation is efficient enough to be worth calling.
//! The h2 layer asks that question once, when it splits the transport, and drains every pass
//! of the connection accordingly: gathering if the answer was `true`, coalescing into one
//! buffer and one write if it was `false`. Naming a model obliges the writer to implement
//! that model's write trait, by compiler error, and the two models are disjoint, so no
//! transport can carry both. The models, the traits they oblige, and how each answer drains
//! are tabulated in
//! [the transport module's documentation](transport#how-a-pass-gets-drained).
//!
//! **Every transport gathers.** [`BorrowedWrite::write_vectored`](transport::BorrowedWrite::write_vectored)
//! and [`RegionWrite::write_regions`](transport::RegionWrite::write_regions) are both
//! *provided*, defaulting to a loop over the model's one required primitive. So a transport
//! that cannot gather natively gathers anyway, by emulation, and one that can does better by
//! overriding. There is nothing to opt into and nothing to decline.
//!
//! The other obligation is [`TransportWrite::commit`]: the driver calls it after draining a
//! pass and before it waits on the peer, so a transport that buffers its writes — a
//! `BufWriter`, a `BufStream` — must flush there. A transport whose writes are already
//! peer-visible leaves it at its no-op default. Omitting it for a buffering transport is a
//! silent hang, which is exactly what the driver's flush point exists to rule out.
//!
//! # Migrating a transport written against the shared owned write
//!
//! **This is the most recent migration, and the one most transports will hit.**
//! [`TransportWrite`] used to require an owned
//! `async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes)` from *every*
//! transport. It no longer has a write at all. The primitive belongs to the I/O model,
//! because who owns the buffer is what the model is:
//!
//! | if your transport declares | move the old `write` body to | leaving `TransportWrite` with |
//! | --- | --- | --- |
//! | `type Model = Readiness;` | **nowhere — delete it.** You already implement [`BorrowedWrite::write_borrowed`](transport::BorrowedWrite::write_borrowed), which is the readiness primitive, and the owned write was never used on this path | `Model`, and `commit` if you buffer |
//! | `type Model = Completion;` | [`RegionWrite::write_owned`](transport::RegionWrite::write_owned) — **verbatim**, the signature is unchanged; only the trait and the name differ | `Model`, and `commit` if you buffer |
//!
//! Both cases are caught by the compiler and neither can be got wrong silently. A readiness
//! transport that keeps its `write` gets `E0407` — not a member of `TransportWrite`. A
//! completion transport that keeps it gets the same, *plus* `E0046` for the `write_owned` its
//! `RegionWrite` impl now lacks.
//!
//! The reason is that a readiness transport can never use ownership. This crate's own tokio
//! transport was the proof: its `write` took the `Bytes` and immediately took a reference to
//! it. Worse, the driver had to *manufacture* that ownership out of its own reused coalescing
//! buffer — a `split().freeze()` whose only purpose was to hand over something the driver
//! already owned, at the cost of a pair of atomic refcount operations. Both are gone; the
//! readiness coalescing drain now lends the buffer and clears it.
//!
//! Nothing else about a transport changes. In particular that change moved where the write
//! primitive lives, not who decides how many writes a pass costs — that decision moved
//! separately, and later, when it became
//! [`TransportWrite::is_write_vectored`].
//!
//! # Migrating a transport written against the strategy traits
//!
//! The design *before* that had the *transport* declare which of four drain
//! strategies the driver should use: `Coalesced`, `PerRegion`, `Gathering`, `OwnedRegions`,
//! named through `TransportWrite::Strategy`. All four markers are gone, along with the
//! `VectoredWrite` trait and its `gathers()` predicate. What replaces them is two markers
//! naming only the I/O model, plus one `bool` — [`is_write_vectored`][iwv] — saying whether
//! the transport's gathering is real.
//!
//! [iwv]: transport::TransportWrite::is_write_vectored
//!
//! | old declaration | new declaration | operations move to |
//! | --- | --- | --- |
//! | `type Strategy = Coalesced;` (readiness) | `type Model = Readiness;` | [`BorrowedWrite`](transport::BorrowedWrite), whose `write_borrowed` is now **required** |
//! | `type Strategy = Coalesced;` (completion) | `type Model = Completion;` | [`RegionWrite`](transport::RegionWrite), whose `write_owned` is now **required** — move the old `write` body there verbatim |
//! | `type Strategy = PerRegion;` | `type Model = Readiness;` | unchanged: keep `write_borrowed`, drop the marker |
//! | `type Strategy = Gathering;` | `type Model = Readiness;` + `fn is_write_vectored(&self) -> bool { true }` | `write_vectored` moves from `VectoredWrite` onto [`BorrowedWrite`](transport::BorrowedWrite); `gathers()` becomes [`is_write_vectored`][iwv] on [`TransportWrite`] |
//! | `type Strategy = OwnedRegions;` | `type Model = Completion;` + `fn is_write_vectored(&self) -> bool { true }` | unchanged: keep `write_regions`, drop the marker |
//!
//! Note the first two rows: `Coalesced` used to mean "I have nothing to offer", and it was
//! one line either way. It is no longer a thing a transport can *name*, but it is still a
//! thing a transport can *say*: leaving [`is_write_vectored`][iwv] at its `false` default is
//! exactly that statement, and it is what puts the transport on the coalesced drain. What a
//! minimal transport says beyond that is which model it belongs to, and the minimum work is
//! one write method either way — `write_owned` on the completion side, `write_borrowed` on
//! the readiness side, each of which that model's API already provides, because that is what
//! the model *is*.
//!
//! Note also the last two rows' second half. `gathers()` used to be answered by the
//! transport and then thrown away: the driver never saw it, because the drain came from the
//! caller's `Config`. It is answered by the transport again now, and this time the driver
//! reads it. A transport that overrides `write_vectored` or `write_regions` with a real
//! scatter-gather call and *forgets* the declaration is not broken — it is merely coalesced,
//! and its override goes unused.
//!
//! ```
//! use std::io;
//! use ngnet_h2::http::testing::bytes_crate::Bytes;
//! use ngnet_h2::http::transport::{
//!     BorrowedWrite, Completion, Readiness, RegionWrite, TransportWrite,
//! };
//!
//! # struct Sink;
//! # impl Sink {
//! #     fn put(&mut self, _: &[u8]) {}
//! # }
//! // 1. Was `Coalesced`, readiness-based. Name the model and supply the one primitive.
//! //    `is_write_vectored` is left at its `false` default, which is the honest answer for a
//! //    transport with no scatter-gather call, and puts it on the coalesced drain — which is
//! //    what `Coalesced` used to mean, now said by the transport rather than the caller.
//! struct Simple(Sink);
//! impl TransportWrite for Simple {
//!     type Model = Readiness;
//! }
//! impl BorrowedWrite for Simple {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.0.put(data);
//!         Ok(data.len())
//!     }
//! }
//!
//! // 2. Was `PerRegion`. Identical body; only the declaration changed. There is no
//! //    per-region *drain* any more, and this transport declares `false` by omission, so the
//! //    driver coalesces each pass into one write — strictly fewer writes than `PerRegion`
//! //    ever cost it, at the price of a copy.
//! struct PerBlock(Sink);
//! impl TransportWrite for PerBlock {
//!     type Model = Readiness;
//! }
//! impl BorrowedWrite for PerBlock {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.0.put(data);
//!         Ok(data.len())
//!     }
//! }
//!
//! // 3. Was `Gathering`. `write_vectored` moves onto `BorrowedWrite` as an override of the
//! //    default, and `gathers()` becomes `is_write_vectored` on `TransportWrite` — see the
//! //    note below on what moved and what did not.
//! struct Vectoring(Sink);
//! impl TransportWrite for Vectoring {
//!     type Model = Readiness;
//!     // Without this line the override below is dead code: the driver would coalesce.
//!     fn is_write_vectored(&self) -> bool {
//!         true
//!     }
//! }
//! impl BorrowedWrite for Vectoring {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.0.put(data);
//!         Ok(data.len())
//!     }
//!     async fn write_vectored<'w>(&'w mut self, regions: &'w [io::IoSlice<'w>]) -> io::Result<usize> {
//!         let mut written = 0;
//!         for region in regions {
//!             self.0.put(region);
//!             written += region.len();
//!         }
//!         Ok(written)
//!     }
//! }
//!
//! // 4. Was `OwnedRegions`. The body is identical; the declaration gained the same line
//! //    case 3 did, and for the same reason.
//! struct Owned(Sink);
//! impl TransportWrite for Owned {
//!     type Model = Completion;
//!     fn is_write_vectored(&self) -> bool {
//!         true
//!     }
//! }
//! impl RegionWrite for Owned {
//!     async fn write_owned(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//!     async fn write_regions(&mut self, regions: Vec<Bytes>) -> (io::Result<usize>, Vec<Bytes>) {
//!         let mut written = 0;
//!         for region in &regions {
//!             self.0.put(region);
//!             written += region.len();
//!         }
//!         (Ok(written), regions)
//!     }
//! }
//!
//! // 5. Was `Coalesced`, completion-based. The whole obligation is the owned primitive: the
//! //    `write_regions` default loops the owned regions through it.
//! struct MinimalCompletion(Sink);
//! impl TransportWrite for MinimalCompletion {
//!     type Model = Completion;
//! }
//! impl RegionWrite for MinimalCompletion {
//!     async fn write_owned(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! // That one method is the whole completion-side obligation: `write_regions` is provided,
//! // and loops one `write_owned` per region. Override it — and declare
//! // `is_write_vectored` alongside it — only if the runtime has a real vectored write, as
//! // `CompioWriter` does.
//! ```
//!
//! ## `gathers()` came back, under tokio's name and with tokio's default
//!
//! `VectoredWrite::gathers()` existed because a stream may inherit tokio's default
//! `poll_write_vectored`, which writes the first region and ignores the rest. It answered
//! whether that was the case, and the driver routed around it. It was deleted when the drain
//! moved to `Config`, on the grounds that the transport should not vote on a decision that
//! was the caller's. It is back, as
//! [`TransportWrite::is_write_vectored`], because the decision was never the caller's
//! either: no caller can know whether the socket it is handing over has a real `writev`
//! behind it, and the transport always could.
//!
//! Three things are different this time, and all three matter.
//!
//! **The default inverted, from `true` to `false`.** `gathers()` was optimistic; a wrapper
//! that forgot to forward the question inherited "yes, I gather" and then quietly wrote one
//! region per pass. `is_write_vectored` is conservative, exactly as
//! `tokio::io::AsyncWrite::is_write_vectored` is: a wrapper that forgets inherits "no", and
//! the driver coalesces. Forgetting now costs a copy instead of a syscall storm, and the copy
//! is bounded by the pass while the syscalls were not.
//!
//! **It is asked once, not per write.** The driver reads it immediately after
//! [`Transport::split`] and keeps the answer for the
//! connection's life. It must therefore be answerable without I/O and must not change its
//! mind, which is why it is a plain `&self -> bool` and not a future.
//!
//! **It selects a drain rather than routing around one.** `gathers()` chose between the
//! gathered drain's two implementations; `is_write_vectored` chooses between the gathered
//! drain and the coalesced one. A transport that answers `false` no longer reaches
//! `write_vectored` from the driver at all — the emulating default is still there, still
//! correct, and still reached by a transport that answers `true` without overriding, but it
//! is no longer the fallback for a transport that cannot gather. That transport gets one
//! write and one copy instead.
//!
//! This is not free in every direction, and the crate does not claim it is. A readiness
//! transport that cannot gather used to take the gathered drain and reach the emulating
//! default, which — because the driver accumulates sub-threshold blocks into a single region
//! before writing — usually issued *one* write and copied only the accumulated blocks,
//! leaving handed-over payloads uncopied. It now issues one write and copies every outgoing
//! octet, payloads included. For a pass that was already one region that is a regression, and
//! a real one. It is accepted because it is the shape hyper has, because it is bounded and
//! predictable where the emulating loop's cost is not, and because the transport that pays it
//! is the one that declined to say it could gather.
//!
//! ## Behaviours that are no longer expressible
//!
//! **Naming a drain strategy.** There is no strategy type to name, so a transport cannot
//! select a drain *by name*. What it can do is answer [`is_write_vectored`][iwv], and the h2
//! layer routes a `false` to the coalescing drain and a `true` to the gathered one — which is
//! most of what the four strategies were used to express. The difference is that the
//! transport declares a *property of itself*, in one bit, and the h2 layer decides what that
//! is worth; it does not name the decision. `PerRegion` in particular is gone and stays gone:
//! no answer to a yes/no question can ask for one write per region.
//!
//! **Offering more than one fast path.** A transport still belongs to exactly one I/O model
//! and gets exactly one fast path with it. The old precedence advice — prefer gathering over
//! owned regions, because gathering need not mint an owned [`Bytes`](bytes::Bytes) per frame
//! header — is now moot for the same reason it always mostly was: the two belong to different
//! I/O models and a transport genuinely belongs to one of them.
//!
//! **Withdrawing a path mid-pass.** A writer that cannot complete a particular write reports
//! so through its result — a short count, which the driver re-offers, or an
//! [`io::Error`](std::io::Error) — rather than by refusing the path.

mod body;
pub mod client;
mod config;
mod connection;
mod driver;
mod error;
mod head;
pub mod server;
mod shared;
mod tasks;
pub mod transport;
mod waker;

pub use body::IncomingBody;
pub use client::{
    ResponseFuture, SendRequest, handshake, handshake_shared, handshake_shared_with, handshake_with,
};
pub use config::Config;
pub use connection::Connection;
pub use error::{Error, ErrorKind, Result};
pub use server::{Cancelled, serve, serve_shared, serve_shared_with, serve_with};
pub use transport::{Transport, TransportRead, TransportWrite};

#[doc(hidden)]
pub mod testing;
