//! A pooling, connecting HTTP/2 client over [`ngnet-h2`](ngnet_h2).
//!
//! ```no_run
//! use http_body_util::{BodyExt, Full};
//! use bytes::Bytes;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ngnet_util::Client::new();
//!
//! let request = http::Request::get("http://127.0.0.1:8080/hello")
//!     .body(Full::new(Bytes::new()))?;
//!
//! let response = client.request(request).await?;
//! println!("{}", response.status());
//!
//! // Nothing above named a socket, a transport, a handshake or a driver task.
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # What this crate is for
//!
//! `ngnet-h2` gives you a *connection*: [`handshake`] returns a [`SendRequest`] handle and a
//! driver future, and everything after that is yours. You resolve the address, open the
//! socket, wrap it in [`TokioIo`], spawn the driver, hold the handle, and notice for yourself
//! when the connection goes closed or starts refusing. That is the right shape for a
//! connection-level crate and the wrong shape for anyone who just wants a response.
//!
//! This crate is the layer above: it keys connections by origin, dials one when there isn't
//! one, shares it across concurrent requests, replaces it when the peer gives up on it, and
//! shuts the whole lot down on request. It is to [`ngnet_h2::http::client`] what
//! `hyper-util`'s `client::legacy::Client` is to `hyper::client::conn`.
//!
//! It is cleartext HTTP/2 only. There is no TLS, no ALPN, no protocol negotiation and no
//! HTTP/1 fallback, because `ngnet-h2` is cleartext-only and inventing a negotiation over a
//! stack that can only speak one protocol would be ceremony. A URI with any scheme but `http`
//! is refused rather than downgraded.
//!
//! # Where the obvious design is wrong
//!
//! Five places, each of which this crate got wrong first.
//!
//! ## A pool of idle connections is the wrong mental model
//!
//! An HTTP/1 pool is a queue of idle sockets per origin, because a socket serves one request
//! at a time and concurrency means more sockets. HTTP/2 multiplexes, so the same structure
//! would be a queue with one thing in it. This pool holds **at most one connection eligible
//! for new requests per origin**, and concurrency is streams on it rather than sockets beside
//! it.
//!
//! The obvious refinement — open a second connection when the first hits its stream limit —
//! is not implemented, and the reason is not that it would be complicated. `ngnet-h2` does
//! not expose the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` anywhere in its public API, so
//! "this connection is saturated" is not a condition this layer can observe. A second
//! connection would have to be opened on a guess.
//!
//! ## `OnceCell` is not a deduplicator
//!
//! Ten concurrent requests to an origin with nothing pooled must open one connection, not
//! ten. [`tokio::sync::OnceCell::get_or_try_init`] looks exactly like the answer and is not:
//! when the initialiser fails, tokio does **not** hand that error to the callers already
//! waiting. It releases the permit and lets one of them try again, then another. A burst of
//! ten at an unreachable origin makes up to ten serial connection attempts, and different
//! callers in one burst see different outcomes.
//!
//! So the pool carries the dial state explicitly, and a caller that has waited takes whatever
//! it wakes to rather than starting an attempt of its own. A failure is fanned out to the
//! callers waiting on it, and is spent for everyone arriving afterwards.
//!
//! ## A `JoinSet` would cancel the connections it was meant to own
//!
//! The driver tasks need somewhere to live, and [`tokio::task::JoinSet`] is the obvious
//! home. But `JoinSet` **aborts every task it holds when it is dropped**, and dropping the
//! last [`Client`] must *not* cancel exchanges that are still in flight — it releases the
//! pool's interest in its connections, which then finish what they were doing. Plain
//! [`JoinHandle`](tokio::task::JoinHandle)s detach on drop, which is the required behaviour.
//!
//! ## The pool's locks are `std::sync::Mutex`, on purpose
//!
//! A pool that holds its map locked while dialling turns dial deduplication into a global
//! serialisation of every request to every origin. [`tokio::sync::Mutex`] would make that
//! mistake compile. The standard library's guard is not `Send`, so holding one across an
//! `await` fails to compile — the compiler enforces the invariant on every future change,
//! which is worth more than a test enforcing it on the code as written. The locks are held
//! for map lookups and state transitions and never across I/O.
//!
//! ## A connect failure is not where you think it is
//!
//! See [`ErrorKind::Connect`] — the boundary between "could not reach the origin" and "the
//! exchange failed" is not the boundary between connecting and requesting, because
//! `ngnet-h2` establishes a connection synchronously and does the wire handshake afterwards.
//!
//! # This is the layer that spawns
//!
//! `ngnet-h2` never spawns a task; it hands you a future and lets you decide. That is a
//! deliberate property of a sans-I/O-shaped crate and it is why it can be driven by any
//! runtime. This crate is where that changes: it requires tokio, and it spawns one task per
//! connection to drive it. Somebody has to, and pushing it up to the caller would mean the
//! caller was managing connections again, which is the entire thing this crate exists to
//! stop.
//!
//! # What this crate does not do
//!
//! Not because they are hard, but because each is a decision that belongs to a caller:
//! redirects, cookies, decompression, authentication, timeouts, and automatic retries of a
//! request that has already been handed to a connection. The last of those is not merely
//! unimplemented — see [`Error::is_retriable`] for why it is not implementable at this layer
//! as `ngnet-h2` currently stands.
//!
//! [`handshake`]: ngnet_h2::http::client::handshake
//! [`SendRequest`]: ngnet_h2::http::client::SendRequest
//! [`TokioIo`]: ngnet_h2::http::transport::tokio::TokioIo

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

mod client;
mod connect;
mod error;
mod origin;
mod pool;

pub use client::{Client, ResponseFuture};
pub use error::{Error, ErrorKind};
pub use origin::Origin;

/// The connection configuration, re-exported from `ngnet-h2` rather than wrapped.
///
/// A wrapper would have to mirror every knob and would fall behind the first time one was
/// added. The type is `ngnet-h2`'s, applies to every connection this pool dials, and is
/// re-exported so that a caller who wants to change a setting does not have to depend on
/// `ngnet-h2` directly to name the type they are changing.
pub use ngnet_h2::http::Config;

/// The response body, re-exported so it can be named without depending on `ngnet-h2`.
///
/// Every response this client returns carries one. A caller who wants to write the type of a
/// function returning a response would otherwise need `ngnet-h2` in their own manifest purely
/// to spell it — the unnameable-type problem `ngnet-axum`'s API surface test also pins.
pub use ngnet_h2::http::IncomingBody;

#[doc(hidden)]
pub mod testing;
