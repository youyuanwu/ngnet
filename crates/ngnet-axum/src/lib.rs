//! Run an axum [`Router`] over `ngnet-h2` instead of hyper.
//!
//! The crate-level documentation is written in a later phase; this is enough to satisfy
//! `missing_docs` while the pieces underneath it are built.
//!
//! [`Router`]: axum::Router

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

mod connection;
mod error;
mod peer;
mod server;

pub use connection::serve_connection;
pub use error::{Error, ErrorKind};
pub use peer::PeerAddr;
pub use server::{Serve, serve};

/// The HTTP/2 configuration applied to each connection, re-exported from `ngnet-h2`.
///
/// Re-exported rather than wrapped: it is `ngnet-h2`'s type, a wrapper would have to be
/// kept in step with it for no gain, and a caller should be able to pass one to
/// [`Serve::config`] without naming a second crate in their manifest.
pub use ngnet_h2::http::Config;
