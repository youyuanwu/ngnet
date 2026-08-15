//! Run an axum [`Router`] over `ngnet-h2` instead of hyper.
//!
//! ```no_run
//! use axum::{Router, routing::get};
//! use tokio::net::TcpListener;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let router = Router::new().route("/hello", get(|| async { "world" }));
//! let listener = TcpListener::bind("127.0.0.1:8080").await?;
//!
//! // The server future resolves to `()`, not a `Result`: a per-connection failure is
//! // delivered to `Serve::on_error` rather than ending the server. Nothing here fails.
//! ngnet_axum::serve(listener, router).await;
//! # Ok(())
//! # }
//! ```
//!
//! # Why this is possible at all
//!
//! axum is usually described as being built on hyper, which suggests that replacing hyper
//! means replacing axum. It does not, because axum barely touches hyper. A [`Router`] is a
//! [`tower_service::Service`] taking an [`http::Request`] and returning an
//! [`http::Response`]; routing, extractors, middleware and state are all defined in those
//! terms. hyper's job is only to turn bytes on a socket into one of those requests and the
//! resulting response back into bytes. That job is separable, and this crate gives it to
//! `ngnet-h2`.
//!
//! # Where the obvious design is wrong
//!
//! The obvious shape for this crate is a pair of body adapters: one wrapping `ngnet-h2`'s
//! incoming body so axum will accept it, another wrapping axum's outgoing body so
//! `ngnet-h2` will send it. That was the assumption this crate started from, and it was
//! wrong in both directions. There are no body adapters here, because none is needed.
//!
//! `ngnet-h2`'s [`IncomingBody`] is an `http_body::Body` whose `Data` is `Bytes`,
//! and it is `Send + 'static`. axum's runnable impl is
//! `impl<B> Service<Request<B>> for Router<()> where B: HttpBody<Data = Bytes> + Send + 'static`.
//! `IncomingBody` satisfies that as it stands, so the request is handed to the `Router`
//! unchanged -- headers, extensions and all. Going the other way, axum's [`axum::body::Body`]
//! also has `Data = Bytes`, which is exactly what `ngnet-h2`'s response path requires, so the
//! response body is handed back without its payload being touched.
//!
//! One honest qualification, because "zero conversion" is the kind of claim that quietly
//! becomes false: axum's own `Service::call` boxes the request body internally. That is one
//! allocation per request, inside axum, which `axum::serve` also pays. No payload is copied
//! in either direction, which is the property worth having.
//!
//! # How this differs from `axum::serve`
//!
//! Five differences, stated here rather than in a footnote because each can surprise a
//! handler that was written against hyper.
//!
//! **Handlers must not block.** Handlers run *inside* the connection future rather than in
//! tasks of their own. They still run concurrently with each other, but a handler that
//! blocks the thread -- synchronous file I/O, a `std::sync::Mutex` held across contention, a
//! long computation -- stalls every other stream on that connection, not just its own. Use
//! [`tokio::task::spawn_blocking`] for work that blocks.
//!
//! **A panicking handler ends its connection.** Under `axum::serve` a panic costs one
//! request. Here it unwinds out of the driver and fails the whole connection, taking every
//! other stream on it. Other connections are unaffected. A handler that might panic should
//! catch it, or the router should carry a middleware layer that does.
//!
//! **A panic inside a response body aborts the process.** This one has no equivalent and no
//! recovery. Response body frames are pulled synchronously from inside a callback invoked
//! by C code, and a panic that reaches an `extern "C"` boundary aborts rather than unwinds.
//! A fallible body must return `Err` from `poll_frame`; it must not panic, and it must not
//! `unwrap`. The asymmetry with the handler case is not a design choice made here but a
//! consequence of where each one runs.
//!
//! **Graceful shutdown drains; it does not cancel.** [`Serve::with_graceful_shutdown`]
//! stops the server accepting new connections and tells every established peer to wind up:
//! each gets a `GOAWAY` naming the last request that connection will answer. Requests
//! already in flight are answered in full, requests begun after it are refused, and each
//! connection closes once its last stream finishes. The server future resolves when they
//! all have.
//!
//! There is deliberately **no deadline**. A handler that never returns holds its connection
//! open, and therefore holds the server open, forever. Bounding that is the caller's job,
//! because only the caller knows what its own handlers are allowed to take and what should
//! happen to a request that overruns -- wrap the server future in a timeout if you need one.
//!
//! **Peer addresses arrive as an extension, not `ConnectInfo`.** Handlers read
//! [`PeerAddr`] from the request extensions. axum's `ConnectInfo` extractor is gated behind
//! axum's `tokio` feature, which depends on `hyper-util` -- so supporting it would drag
//! hyper back into the dependency graph this crate exists to avoid. CI checks that it has
//! not returned.
//!
//! # What this crate does not do
//!
//! It serves h2c -- cleartext HTTP/2 with prior knowledge -- and nothing else. There is no
//! TLS, no ALPN, no HTTP/1.1 and no upgrade dance, because `ngnet-h2` is a cleartext h2
//! implementation. A peer that is not speaking HTTP/2 is an error, not a fallback. It is
//! server-side only, and tokio-only.
//!
//! **The number of simultaneously accepted connections is not capped.** The accept loop
//! accepts whatever arrives; a server under load will keep accepting until it runs out of
//! file descriptors or memory. That is stated plainly because it is a property a caller has
//! to plan around: a bound belongs in front of this crate -- a semaphore, a listener that
//! stops accepting, or something upstream -- and there is nothing here that will impose one
//! for you. [`Config::max_concurrent_streams`] bounds streams *within* a connection, which
//! is a different limit and not a substitute.
//!
//! # A trap worth knowing about
//!
//! A drain waits for streams, and a response body that is never read is a stream that never
//! finishes. This is most often met in tests: a test that checks a status code and lets the
//! response value live until the end of the function still has an open stream when it asks
//! the server to stop, and then waits out its timeout. The server is behaving correctly --
//! it owes that peer the rest of a body -- and the client is the one that has not finished.
//! Read response bodies to completion, or drop them.
//!
//! [`Router`]: axum::Router
//! [`IncomingBody`]: ngnet_h2::http::IncomingBody
//! [`Config::max_concurrent_streams`]: ngnet_h2::http::Config::max_concurrent_streams

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

mod connection;
mod error;
mod listener;
mod peer;
mod server;
mod transport;

pub use connection::serve_connection;
pub use error::{Error, ErrorKind};
pub use listener::{FallibleListener, Listener, RetryingListener};
pub use peer::PeerAddr;
pub use server::{Serve, serve};
pub use transport::{ServableTransport, require_spawnable};

/// The HTTP/2 configuration applied to each connection, re-exported from `ngnet-h2`.
///
/// Re-exported rather than wrapped: it is `ngnet-h2`'s type, a wrapper would have to be
/// kept in step with it for no gain, and a caller should be able to pass one to
/// [`Serve::config`] without naming a second crate in their manifest.
///
/// One setting behaves less strongly than its name suggests, and it is worth knowing
/// before relying on it: `max_header_list_size` is *advertised* to the peer, not enforced
/// on arrival. A client that ignores the advertised value and sends a larger header list
/// is served normally, and the handler runs. It is a hint, not a limit, and it is not a
/// defence against a hostile peer. The behaviour is pinned by a test rather than left to
/// be rediscovered.
pub use ngnet_h2::http::Config;

/// The connection future type returned by [`serve_connection`], re-exported from
/// `ngnet-h2`.
///
/// Without this a caller could call [`serve_connection`] but could not write down what it
/// returned — the type is reachable only through a crate they need not otherwise depend
/// on. `EngineError` is renamed on the way through because this crate already has an
/// [`Error`] of its own, which is the one reported to [`Serve::on_error`]; the two are
/// different types and silently sharing a name would be worse than the rename.
pub use ngnet_h2::http::{Connection, Error as EngineError, Result as EngineResult};

/// Wraps a tokio byte stream so it can be used as a transport, re-exported from `ngnet-h2`.
///
/// [`serve_connection`] takes a transport rather than a socket, and listener
/// implementations produce one. For anything built on tokio -- a TCP stream, a Unix-domain
/// stream, an in-memory pipe, a TLS session over any of them -- this is the wrapper that
/// turns it into one, and [`ServableTransport`] is implemented for every `TokioIo` over such
/// a stream.
///
/// Re-exported for the same reason [`Connection`] is: without it a caller could be *required*
/// to wrap a stream while being unable to name the wrapper without depending on a crate they
/// otherwise need not.
pub use ngnet_h2::http::transport::TokioIo;
