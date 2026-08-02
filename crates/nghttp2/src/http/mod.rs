//! An asynchronous HTTP/2 API over the sans-I/O core.
//!
//! Enabled by the default `http` feature. Disabling it returns the crate to a pure
//! state machine with exactly one dependency and no I/O of any kind.
//!
//! Everything in this subtree is confined to it: nothing outside `src/http/` acquires an
//! async facility, and a structural test enforces that. The sans-I/O core is unchanged
//! and remains usable on its own.

pub mod transport;

pub use transport::{Transport, TransportRead, TransportWrite};

#[doc(hidden)]
pub mod testing;
