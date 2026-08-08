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
//! The writer additionally names one *strategy* as an associated type, and that declaration
//! is the whole election — how the driver drains a pass, settled at compile time. Naming a
//! strategy obliges the writer to implement that strategy's operations, by compiler error;
//! there is no probe, no capability flag to keep in step with a method, and no way to
//! advertise a fast path without supplying it. The four strategies, what each costs, and
//! which I/O model each belongs to are tabulated in
//! [the transport module's documentation](transport#how-a-pass-gets-drained); the short
//! version is that [`transport::Coalesced`] is the one-line default that copies every octet,
//! [`transport::Gathering`] is what a readiness transport over a real socket wants, and
//! [`transport::OwnedRegions`] is its counterpart for a completion transport, which cannot
//! lend the kernel a borrowed slice at all.
//!
//! The other obligation is [`TransportWrite::commit`]: the driver calls it after draining a
//! pass and before it waits on the peer, so a transport that buffers its writes — a
//! `BufWriter`, a `BufStream` — must flush there. A transport whose writes are already
//! peer-visible leaves it at its no-op default. Omitting it for a buffering transport is a
//! silent hang, which is exactly what the driver's flush point exists to rule out.
//!
//! # Migrating a transport written against the older traits
//!
//! Before this crate separated the two I/O models, [`TransportWrite`] carried all five write
//! methods at once, and a transport declined the ones it could not serve at run time —
//! `write_borrowed` and `write_vectored` returned `Option<impl Future>`, with `None` meaning
//! "not this path", and `gathers_owned_regions` was a predicate paired with `write_regions`.
//! Neither shipped adapter could implement more than half of it, which is what the split
//! fixes. Migration is mechanical: name the strategy the old overrides amounted to, move
//! those methods to the trait that now carries them, and delete the `Option`.
//!
//! | old shape | new declaration | operations move to |
//! | --- | --- | --- |
//! | overrode nothing | `type Strategy = Coalesced;` | — |
//! | `write_borrowed` returned `Some` | `type Strategy = PerRegion;` | [`BorrowedWrite`](transport::BorrowedWrite) |
//! | `write_vectored` returned `Some` | `type Strategy = Gathering;` | [`BorrowedWrite`](transport::BorrowedWrite) + [`VectoredWrite`](transport::VectoredWrite) |
//! | `gathers_owned_regions` returned `true` | `type Strategy = OwnedRegions;` | [`RegionWrite`](transport::RegionWrite) |
//!
//! ```
//! use std::io;
//! use ngnet_h2::http::testing::bytes_crate::Bytes;
//! use ngnet_h2::http::transport::{
//!     BorrowedWrite, Coalesced, Gathering, OwnedRegions, PerRegion, RegionWrite,
//!     TransportWrite, VectoredWrite,
//! };
//!
//! # struct Sink;
//! # impl Sink {
//! #     fn put(&mut self, _: &[u8]) {}
//! # }
//! // 1. Overrode nothing before: one line, and nothing else changes.
//! struct Simple(Sink);
//! impl TransportWrite for Simple {
//!     type Strategy = Coalesced;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//!
//! // 2. Returned `Some` from `write_borrowed`: declare `PerRegion` and drop the `Option`.
//! struct PerBlock(Sink);
//! impl TransportWrite for PerBlock {
//!     type Strategy = PerRegion;
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
//! // 3. Returned `Some` from `write_vectored`: declare `Gathering`. Note that the borrowed
//! //    write is now *required* — it is the live fallback when `gathers` is false.
//! struct Vectoring {
//!     sink: Sink,
//!     really_gathers: bool,
//! }
//! impl TransportWrite for Vectoring {
//!     type Strategy = Gathering;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.sink.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl BorrowedWrite for Vectoring {
//!     async fn write_borrowed<'w>(&'w mut self, data: &'w [u8]) -> io::Result<usize> {
//!         self.sink.put(data);
//!         Ok(data.len())
//!     }
//! }
//! impl VectoredWrite for Vectoring {
//!     // Where the old code asked the stream on every call — tokio's `is_write_vectored`,
//!     // consulted inside `write_vectored` — the answer is now cached at construction and
//!     // read once per connection.
//!     fn gathers(&self) -> bool {
//!         self.really_gathers
//!     }
//!     async fn write_vectored<'w>(&'w mut self, regions: &'w [io::IoSlice<'w>]) -> io::Result<usize> {
//!         let mut written = 0;
//!         for region in regions {
//!             self.sink.put(region);
//!             written += region.len();
//!         }
//!         Ok(written)
//!     }
//! }
//!
//! // 4. Returned `true` from `gathers_owned_regions`: declare `OwnedRegions`. The predicate
//! //    is gone — the declaration says the same thing, and cannot fall out of step with the
//! //    write that implements it.
//! struct Completion(Sink);
//! impl TransportWrite for Completion {
//!     type Strategy = OwnedRegions;
//!     async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
//!         self.0.put(&buf);
//!         let n = buf.len();
//!         (Ok(n), buf)
//!     }
//! }
//! impl RegionWrite for Completion {
//!     async fn write_regions(&mut self, regions: Vec<Bytes>) -> (io::Result<usize>, Vec<Bytes>) {
//!         let mut written = 0;
//!         for region in &regions {
//!             self.0.put(region);
//!             written += region.len();
//!         }
//!         (Ok(written), regions)
//!     }
//! }
//! ```
//!
//! ## Two old behaviours are deliberately no longer expressible
//!
//! **Offering more than one fast path.** A transport used to be able to override several of
//! the five methods and let the driver arbitrate, by a runtime precedence rule: vectored,
//! then owned-region, then borrowed, then plain owned. There is nothing left to arbitrate —
//! a writer names exactly one strategy — so pick the one the old rule would have picked.
//! Its reasoning survives as advice at the point of declaration: prefer
//! [`Gathering`](transport::Gathering) over [`OwnedRegions`](transport::OwnedRegions),
//! because it need not mint an owned [`Bytes`](bytes::Bytes)
//! per frame header. In practice the question does not arise, because the two belong to
//! different I/O models and a transport genuinely belongs to one of them.
//!
//! **Withdrawing a path mid-pass.** Returning `None` after previously returning `Some` used
//! to make the driver fall back for the rest of the pass. That is gone: the strategy is
//! settled by the type, and a writer that cannot complete a particular write reports so
//! through its result — a short count, which the driver re-offers, or an
//! [`io::Error`](std::io::Error) — rather than by refusing the path. A transport whose
//! *stream* does not really scatter-gather has a different question and keeps its answer:
//! [`VectoredWrite::gathers`](transport::VectoredWrite::gathers), read once per connection,
//! which routes every pass down the borrowed write instead.

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
