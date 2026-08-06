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
//! and [`TransportWrite`], each a single required method. The traits are completion-shaped:
//! ownership of the buffer passes in and comes back, which a completion API (`io_uring`,
//! IOCP) needs and a readiness API (tokio, `futures-io`) satisfies with no copy.
//!
//! One decision is the writer's, and it is not free either way.
//! [`TransportWrite::write_borrowed`], [`TransportWrite::write_vectored`] and
//! [`TransportWrite::gathers_owned_regions`] choose how the session is drained, and each is a
//! single override point on purpose: overriding none —
//! the default — has the driver coalesce a whole pass into one owned buffer and issue a
//! single [`write`](TransportWrite::write), which is one syscall per pass but
//! copies every outgoing octet, every pass. `write_borrowed` returning `Some(future)` hands
//! each of the session's own regions over directly: **zero** allocation and zero copy, but
//! one write per block — and *two* per handed-over payload, since its frame header and its
//! octets are separate regions — which is the dominant cost when a pass is dozens of tiny
//! multiplexed blocks. `write_vectored` returning `Some(future)`
//! gathers those small blocks into a buffer the driver reuses and hands the socket that
//! buffer alongside any large block and any handed-over payload, in one `writev` — few
//! syscalls, no copy of large payloads, and still zero steady-state allocation.
//!
//! `gathers_owned_regions` returning `true` is the same bargain for a *completion* transport,
//! which cannot lend the kernel a borrowed slice at all — the kernel writes from the buffers
//! after submission, so they must be owned. The driver instead builds a list of owned
//! [`bytes::Bytes`] and passes it to [`TransportWrite::write_regions`], which takes ownership
//! and returns the list so the allocation can be reused: every session-produced block is
//! copied into a driver buffer there, with no size threshold, because a block borrowed from
//! the session cannot be owned without a copy — but each handed-over payload rides as its own
//! region in the caller's own memory, uncopied. That is the whole reason a completion
//! transport can gather at all, and it is available only for handed-over bodies; a
//! push-model body's `DATA` was never the caller's to hand over, so it is copied like any
//! other block.
//!
//! Precedence among the four, highest first: vectored, owned-region, borrowed, owned. The
//! vectored and borrowed elections carry both the choice and the write in one call — an
//! `Option`-returning method that gathers when it returns `Some` — so for those two an
//! adapter cannot advertise a fast path without providing it, nor provide it without the
//! driver taking it. The owned-region path is the deliberate exception: its election
//! ([`TransportWrite::gathers_owned_regions`], a plain predicate) is *split* from its write
//! ([`TransportWrite::write_regions`]). It has to be. A late `None` from a method already
//! handed the owned `Vec<Bytes>` would consume the regions and lose them, where a declined
//! borrowed slice can simply be dropped; ownership is the difference, and it is why the two
//! idioms are not the same shape.
//!
//! What the session's block lifetime forecloses is narrower than it first looks. Asking for
//! the next block invalidates the last, and [`Session::send`](crate::Session::send) enforces
//! that by borrowing the session for as long as the block lives — so several *session
//! blocks* can never be gathered with each other. But one block can be gathered with memory
//! the driver already owns — its accumulation buffer, and the handed-over payloads it holds
//! as lifetime-free descriptors — and that is enough: it is exactly what the vectored path
//! does. A readiness-based transport overrides the borrowed and vectored methods, which is
//! why the `TokioIo` adapter behind the `tokio` feature does. A completion transport cannot
//! use either — both lend the kernel a borrowed slice — so it leaves them at their defaults
//! and overrides the owned-region pair instead, which is what the shipped `CompioIo` adapter
//! does (`transport/compio.rs`); before handed-over bodies existed it had no fast path at all
//! and fell back to the plain owned `write`. `crates/ngnet-h2-tests/tests/http_compio.rs`
//! demonstrates both against a real io_uring socket.
//!
//! The other obligation is [`TransportWrite::commit`]: the driver calls it after draining a
//! pass and before it waits on the peer, so a transport that buffers its writes — a
//! `BufWriter`, a `BufStream` — must flush there. A transport whose writes are already
//! peer-visible leaves it at its no-op default. Omitting it for a buffering transport is a
//! silent hang, which is exactly what the driver's flush point exists to rule out.

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
