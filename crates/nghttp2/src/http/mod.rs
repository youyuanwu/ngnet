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
//! A connection is two objects. [`handshake`] hands back a cloneable handle for
//! making requests and a driver future that moves octets. Nothing happens until the
//! driver is polled, and where it is polled is entirely the caller's business — this
//! crate spawns nothing and takes no executor, spawner or timer.
//!
//! # What must be `Send`
//!
//! The transport need not be, deliberately: the completion-based runtimes this layer
//! exists to serve are thread-per-core and build their I/O on `Rc`. Auto traits propagate
//! instead, so a driver over a `Send` transport is `Send` without anything declaring it.
//!
//! Outgoing bodies are the exception. They are stored inside the session, which may be
//! moved between threads, so they inherit the sans-I/O core's `Send + 'static` bound.

mod body;
pub mod client;
mod driver;
mod error;
mod head;
mod shared;
pub mod transport;
mod waker;

pub use body::IncomingBody;
pub use client::{ResponseFuture, SendRequest, handshake};
pub use error::{Error, ErrorKind, Result};
pub use transport::{Transport, TransportRead, TransportWrite};

#[doc(hidden)]
pub mod testing;
