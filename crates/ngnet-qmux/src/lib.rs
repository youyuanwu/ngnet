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
//! # Sans-I/O, and the layer above it
//!
//! The state machine never touches a socket, spawns a thread, or reads a clock. A caller feeds
//! inbound bytes to [`Conn::read`] as they arrive, and asks [`Conn::write`] to serialise
//! outbound bytes into a buffer to hand to whatever carries them.
//!
//! There are two builds of this crate, and which one a caller gets is a cargo feature:
//!
//! - **`--no-default-features`** is the state machine alone: one dependency, no asynchrony,
//!   and the public API listed below. Everything a caller needs to drive QMux by hand from
//!   blocking code, from a runtime this crate has never heard of, or from a test with no
//!   runtime at all.
//! - **default features** additionally compile the `io` layer, an asynchronous layer that owns
//!   an established byte stream and drives a connection over it. It still names no runtime:
//!   the caller describes their byte stream and their clock through two traits, and a
//!   ready-made description for tokio ships behind the off-by-default `tokio` feature.
//!
//! The layer is on by default because driving the state machine by hand is the unusual case,
//! not the common one. It costs a caller who does not want it exactly one feature flag, and
//! costs them nothing in dependencies either way.
//!
//! # Panics in handlers abort
//!
//! Protocol events are delivered to caller-supplied closures that dwnx invokes across the FFI
//! boundary. A panic inside one cannot unwind through C, so it aborts the process. Handlers
//! should return [`Err`] to report failure rather than panicking; see [`Handlers`].
//!
//! # Scope
//!
//! One connection at a time from the state machine, client or server. There is no endpoint
//! layer, no listener, no accept loop, and no way to serialise a connection close from the
//! state machine itself -- dwnx exposes no function for the last of these. See
//! `docs/qmux/pending-work.md`.
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
// derives a module's name from its file stem. The `io` subtree below is the one exception, and
// it earns it by containing no `unsafe` at all: it is declared without an allowance, so the
// crate-level deny above rejects any `unsafe` in it outright, and a separate test pins that no
// file beneath it grants itself one.
#[allow(unsafe_code)]
mod callbacks;
#[allow(unsafe_code)]
mod ccerr;
#[allow(unsafe_code)]
mod conn;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod params;
#[allow(unsafe_code)]
mod settings;
#[allow(unsafe_code)]
mod stream;
#[allow(unsafe_code)]
mod stream_io;
#[allow(unsafe_code)]
mod write;

mod handlers;
mod time;

// The asynchronous layer, behind the default-on `io` feature. Declared with **no**
// `#[allow(unsafe_code)]`, unlike the FFI modules above, so the crate-level deny makes any
// `unsafe` below it a compile error rather than a code-review question.
#[cfg(feature = "io")]
pub mod io;

// Doctest-only: each item is a `compile_fail` case pinning something the API makes
// impossible to write. Nothing is exported.
#[cfg(doctest)]
mod compile_fail;

pub use ccerr::{CloseKind, CloseReason};
pub use conn::{Conn, ConnBuilder, ReadOutcome, Role};
pub use error::{Error, ErrorKind, NativeCode};
pub use handlers::{
    HandlerError, HandlerResult, Handlers, StreamCloseEvent, StreamDataEvent, StreamLimitKind,
};
pub use params::TransportParams;
pub use settings::Settings;
pub use stream::{Directionality, Initiator, StreamId};
pub use stream_io::{OpenOutcome, Shutdown};
pub use time::{Duration, Timestamp};
pub use write::{Push, Record, RecordWriter, WriteRequest};

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
