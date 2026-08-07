//! An asynchronous HTTP/3 API over the sans-I/O core.
//!
//! Enabled by the default `http` feature. Disabling it returns the crate to a pure state
//! machine with exactly one dependency and no asynchrony of any kind.
//!
//! Everything in this subtree is confined to it: nothing outside `src/http/` acquires an
//! async facility, and a structural test enforces that. It contains no `unsafe` of its own —
//! every FFI call and every `unsafe` block lives below it, in the core. The core is
//! unchanged by this layer's existence and remains usable on its own.
//!
//! # What this layer is for
//!
//! The core is correct and complete, and it is also unusable without writing a QUIC
//! integration by hand: three unidirectional streams to open and bind, a two-phase write to
//! drive, acknowledgement to report, flow-control credit to extend, and per-stream
//! bookkeeping throughout. The crate-level documentation lists thirteen such obligations.
//! This layer discharges all of them, so a caller who has an established QUIC connection
//! reaches HTTP/3 through the `http` crate's request and response types and nothing else.
//!
//! # The QUIC boundary
//!
//! What this layer does *not* supply is QUIC. A caller brings an established connection
//! behind [`QuicConnection`], so quinn, msquic, ngtcp2, s2n-quic, quiche or a test double
//! are all implementations rather than forks. That trait starts *after* the handshake: no
//! endpoint, TLS configuration, certificate handling or ALPN negotiation appears anywhere in
//! it, and none of those concerns reaches this crate.

mod body;
mod client;
mod config;
mod connection;
mod driver;
mod error;
mod events;
mod head;
pub mod quic;
mod server;
mod shared;
mod tasks;

#[doc(hidden)]
pub mod testing;

pub use body::IncomingBody;
pub use client::{ResponseFuture, SendRequest, handshake, handshake_with};
pub use config::Config;
pub use connection::Connection;
pub use error::{Error, ErrorKind, Result};
pub use quic::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
pub use server::{Cancelled, serve, serve_with};
