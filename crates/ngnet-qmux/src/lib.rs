//! Safe, sans-I/O Rust bindings to [dwnx], a C implementation of the [QMux] draft.
//!
//! # What QMux is
//!
//! QMux carries QUIC's stream operations over a single ordered, reliable byte stream. It is a
//! polyfill, not a transport: applications written against QUIC's stream API can run over TCP
//! or a unix socket without being rewritten. What it keeps from QUIC is the multiplexing --
//! many independent, flow-controlled streams over one connection. What it drops is everything
//! the underlying transport already provides: packets, connection ids, paths, loss recovery,
//! congestion control, and cryptography.
//!
//! That last point is worth stating plainly, because it is the usual surprise: **QMux
//! encrypts nothing and requires no TLS.** The draft delegates confidentiality, integrity and
//! protocol negotiation to whatever carries the byte stream, and explicitly permits substrates
//! that provide none of them -- it names unix sockets, where the operating system is trusted.
//! TLS over TCP is the recommended substrate because it supplies all of those at once, but
//! this crate neither requires nor provides it.
//!
//! # Sans-I/O
//!
//! This crate never touches a socket, spawns a thread, or reads a clock. A caller feeds
//! inbound bytes to [`Conn::read`] as they arrive, and asks [`Conn::write`] to serialise
//! outbound bytes into a buffer to hand to whatever carries them. Supplying and driving that
//! transport is the caller's job, which is what makes the crate usable on any runtime, or
//! none.
//!
//! # Panics in handlers abort
//!
//! Protocol events are delivered to caller-supplied closures that dwnx invokes across the FFI
//! boundary. A panic inside one cannot unwind through C, so it aborts the process. Handlers
//! should return an error to report failure rather than panicking.
//!
//! # Scope
//!
//! One connection at a time from the state machine, client or server. There is no endpoint
//! layer, no runtime integration, and no way to serialise a connection close -- dwnx exposes
//! no function for the last of these. See `docs/qmux/pending-work.md`.
//!
//! [dwnx]: https://github.com/ngtcp2/dwnx
//! [QMux]: https://datatracker.ietf.org/doc/html/draft-ietf-quic-qmux

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

// `unsafe` is confined to the modules that touch the raw bindings. The crate-level deny above
// makes a stray `unsafe` anywhere else a compile error, and a test reads this list back out of
// this file so the two cannot disagree.
//
// Module files are deliberately flat -- `conn.rs`, never `conn/mod.rs` -- because that test
// derives a module's name from its file stem.
#[allow(unsafe_code)]
mod ccerr;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod params;
#[allow(unsafe_code)]
mod settings;
#[allow(unsafe_code)]
mod stream;

mod time;

pub use ccerr::{CloseKind, CloseReason};
pub use error::{Error, ErrorKind, NativeCode};
pub use params::TransportParams;
pub use settings::Settings;
pub use stream::{Directionality, Initiator, StreamId};
pub use time::{Duration, Timestamp};

/// The raw FFI bindings this crate is built on.
///
/// Exposed for callers who need something the safe API does not yet cover. Everything here is
/// `unsafe` and carries dwnx's own contracts rather than this crate's.
pub use ngnet_qmux_sys as raw;

/// The default maximum QMux record size, in bytes.
///
/// dwnx overwrites any configured `max_record_size` with this value at construction, so it is
/// also the effective one; see [`TransportParams`].
pub const DEFAULT_MAX_RECORD_SIZE: u64 = raw::DWNX_DEFAULT_MAX_RECORD_SIZE as u64;
