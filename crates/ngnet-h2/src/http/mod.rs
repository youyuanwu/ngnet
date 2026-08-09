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
//! the whole driver, which drops the in-flight write future — and because ownership of the
//! buffer passed *into* the transport for the duration of the call, dropping the future
//! never leaves the kernel writing into memory this crate has reclaimed. That safety is
//! the whole reason [`TransportWrite::write`] takes an owned [`bytes::Bytes`] rather than a
//! borrow.
//!
//! # Writing a transport for another runtime
//!
//! A runtime this crate ships no adapter for is a short job, not a blocked one. Implement
//! [`Transport`] — one method, [`split`](Transport::split), dividing the stream into a
//! reader and a writer so the two directions can proceed at once — then [`TransportRead`]
//! and [`TransportWrite`]. Both are ownership-passing: the buffer goes in and comes back,
//! which a completion API (`io_uring`, IOCP) needs and a readiness API (tokio,
//! `futures-io`) satisfies with no copy.
//!
//! The writer additionally names its *I/O model* as an associated type — readiness or
//! completion — and that is the **only** thing it declares about how its writes are shaped.
//! It does not say how a pass should be drained: that is the h2 layer's decision, taken from
//! [`Config::write_policy`] and settled at handshake. Naming a model obliges the writer to
//! implement that model's write trait, by compiler error, and the two models are disjoint, so
//! no transport can carry both. The models, the traits they oblige, and how each policy drains
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
//! # Migrating a transport written against the strategy traits
//!
//! The immediately preceding design had the *transport* declare which of four drain
//! strategies the driver should use: `Coalesced`, `PerRegion`, `Gathering`, `OwnedRegions`,
//! named through `TransportWrite::Strategy`. All four markers are gone, along with the
//! `VectoredWrite` trait and its `gathers()` predicate. What replaces them is two markers
//! naming only the I/O model, and a policy the *caller* sets.
//!
//! | old declaration | new declaration | operations move to |
//! | --- | --- | --- |
//! | `type Strategy = Coalesced;` (readiness) | `type Model = Readiness;` | [`BorrowedWrite`](transport::BorrowedWrite), whose `write_borrowed` is now **required** |
//! | `type Strategy = Coalesced;` (completion) | `type Model = Completion;` | `impl RegionWrite for X {}` — no methods required |
//! | `type Strategy = PerRegion;` | `type Model = Readiness;` | unchanged: keep `write_borrowed`, drop the marker |
//! | `type Strategy = Gathering;` | `type Model = Readiness;` | `write_vectored` moves from `VectoredWrite` onto [`BorrowedWrite`](transport::BorrowedWrite); delete `gathers()` |
//! | `type Strategy = OwnedRegions;` | `type Model = Completion;` | unchanged: keep `write_regions`, drop the marker |
//!
//! Note the first two rows: `Coalesced` used to mean "I have nothing to offer", and it was
//! one line either way. It is no longer a thing a transport can say, because whether a pass
//! coalesces is not the transport's business. What a minimal transport says instead is which
//! model it belongs to, and the minimum work is still one line — an empty `RegionWrite` impl
//! on the completion side, and `write_borrowed` on the readiness side, which a readiness
//! transport can always supply because that is what a readiness API *is*.
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
//! // 1. Was `Coalesced`, readiness-based. Name the model and supply the one primitive; the
//! //    gathering default loops over it, so this transport gathers without saying so.
//! struct Simple(Sink);
//! impl TransportWrite for Simple {
//!     type Model = Readiness;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl BorrowedWrite for Simple {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.0.put(data);
//!         Ok(data.len())
//!     }
//! }
//!
//! // 2. Was `PerRegion`. Identical body; only the declaration changed. There is no
//! //    per-region *drain* any more — this transport now gathers, by emulation, and the
//! //    driver accumulates before offering, so it pays fewer writes than it used to.
//! struct PerBlock(Sink);
//! impl TransportWrite for PerBlock {
//!     type Model = Readiness;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl BorrowedWrite for PerBlock {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.0.put(data);
//!         Ok(data.len())
//!     }
//! }
//!
//! // 3. Was `Gathering`. `write_vectored` moves onto `BorrowedWrite` as an override of the
//! //    default, and `gathers()` is deleted outright — see the note below on why its
//! //    disappearance is a simplification rather than a loss.
//! struct Vectoring(Sink);
//! impl TransportWrite for Vectoring {
//!     type Model = Readiness;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
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
//! // 4. Was `OwnedRegions`. Identical body; only the declaration changed.
//! struct Owned(Sink);
//! impl TransportWrite for Owned {
//!     type Model = Completion;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl RegionWrite for Owned {
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
//! // 5. Was `Coalesced`, completion-based. The whole obligation is an empty impl: the
//! //    `write_regions` default loops the owned regions through `write`.
//! struct MinimalCompletion(Sink);
//! impl TransportWrite for MinimalCompletion {
//!     type Model = Completion;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl RegionWrite for MinimalCompletion {}
//! // That empty block is the whole completion-side obligation: `write_regions` is provided,
//! // and loops one owned `write` per region. Override it only if the runtime has a real
//! // vectored write, as `CompioWriter` does.
//! ```
//!
//! ## `gathers()` is gone, and the hazard it created with it
//!
//! `VectoredWrite::gathers()` existed because a stream may inherit tokio's default
//! `poll_write_vectored`, which writes the first region and ignores the rest. It answered
//! whether that was the case, and the driver routed around it.
//!
//! It was never a correctness mechanism. Such a stream reports the count it actually wrote,
//! which the driver treats as an ordinary short write and re-offers the remainder from — no
//! octet was ever at risk. What `gathers()` avoided was the *cost*: one syscall per region
//! with none of the gathering benefit. Removing it is affordable because the driver
//! accumulates sub-threshold blocks into a single region before any write happens, so the
//! emulating loop typically runs once, not once per block.
//!
//! Its removal also closes a documented footgun. `gathers()` defaulted to `true` while
//! tokio's `is_write_vectored()` defaults to `false` — opposite conservatism — so a
//! third-party wrapper that forgot to forward the question silently inherited the optimistic
//! answer and quietly wrote one region per pass. There is now no question to forget: a
//! wrapper that forwards nothing inherits the emulating default, which is correct and
//! bounded, and one that forwards `write_vectored` gets the native path.
//!
//! ## Behaviours that are no longer expressible
//!
//! **Declining to gather.** There is no `gathers()` and no strategy to name, so a transport
//! cannot route the driver away from gathering. A caller who wants that reaches for
//! [`Config::write_policy`] with [`WritePolicy::Coalesced`], which is a per-connection
//! decision made where connections are configured rather than a per-transport one baked into
//! a type.
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
pub use config::{Config, WritePolicy};
pub use connection::Connection;
pub use error::{Error, ErrorKind, Result};
pub use server::{Cancelled, serve, serve_shared, serve_shared_with, serve_with};
pub use transport::{Transport, TransportRead, TransportWrite};

#[doc(hidden)]
pub mod testing;
