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
//! [`TransportWrite::write_borrowed`] chooses how the session is drained, and it is a single
//! override point on purpose: returning `None` — the default — has the driver coalesce a
//! whole pass into one owned buffer and issue a single [`write`](TransportWrite::write),
//! which is one syscall per pass but allocates and copies every outgoing octet, every pass.
//! Returning `Some(future)` hands each of the session's own blocks over directly: a few
//! small writes per pass, but **zero** allocation and zero copy, which is the only path on
//! which steady-state allocation reaches zero. Because the same method carries both the
//! choice and the write, an adapter cannot advertise the fast path without providing it, nor
//! provide it without the driver taking it. The two are genuinely exclusive — the session
//! invalidates each block when the next is requested, so blocks cannot be gathered without
//! copying them. A readiness-based transport overrides it, which is why the `TokioIo`
//! adapter behind the `tokio` feature does; a completion API leaves it at its default and
//! maps onto the owned methods with no adapter code to speak of — its `read`/`write` already
//! take and return the buffer — as `crates/nghttp2-tests/tests/http_compio.rs` demonstrates
//! against a real one.
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
pub use client::{ResponseFuture, SendRequest, handshake, handshake_with};
pub use config::Config;
pub use connection::Connection;
pub use error::{Error, ErrorKind, Result};
pub use server::{Cancelled, serve, serve_with};
pub use transport::{Transport, TransportRead, TransportWrite};

#[doc(hidden)]
pub mod testing;
