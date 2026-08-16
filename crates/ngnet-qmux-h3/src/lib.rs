//! HTTP/3 over QMux.
//!
//! `ngnet-h3` speaks HTTP/3 over an abstract transport; `ngnet-qmux` speaks QMux over an
//! ordered, reliable byte stream. This crate is the join. Neither side depends on the other,
//! so a caller who wants HTTP/3 over some other transport, or QMux with no HTTP/3 at all,
//! pays for neither.
//!
//! # What a caller brings
//!
//! A byte stream that is already connected, and a clock. QMux does not dial, does not
//! listen and has no timer of its own: it runs over whatever ordered reliable substrate a
//! caller already has — a TCP socket, a TLS session, one half of an in-memory pair — and
//! this crate inherits all three properties. There is no runtime here and no I/O of this
//! crate's own beyond what the byte stream does.
//!
//! # Why the connection is shared
//!
//! `ngnet_h3::http::handshake` and `serve` take their transport **by value** and hold it for
//! the connection's life. More sharply, the HTTP/3 layer fills a record by calling into its
//! transport and expecting an answer before the call returns: `StreamSource::write_next`
//! hands out `IoSlice`s that are documented invalid the moment the closure ends, and
//! `close` is a synchronous method with no context to park on. There is no arrangement in
//! which those bytes reach another task in time.
//!
//! And yet the driver's *last* act is to call `close` and return. Whatever that call queued
//! is then in a buffer with nobody left to write it, because the driver will never poll the
//! transport again. So the connection is owned behind a lock, the HTTP/3 driver holds one
//! handle onto it, and [`Connection`] holds the other and runs the tail: flush the close,
//! shut the write side down, and only then resolve. A close that is encoded and never
//! written is worse than none — the peer waits out an idle timeout instead of learning why
//! the connection ended.
//!
//! # The pump
//!
//! Every entry point begins by pumping: flushing what is queued, producing what the state
//! machine now owes, and reading what has arrived. That is not tidiness, it is the
//! difference between working and deadlocking.
//!
//! The HTTP/3 driver's *first* action is to open three unidirectional streams, and it will
//! not do anything else until it has them. A QMux endpoint cannot open a stream before the
//! peer's transport parameters arrive — every limit is zero until they do — and they arrive
//! in a record that only gets read if something reads it. An implementation that only moved
//! bytes inside `poll_transmit` would never read that record, because `poll_transmit` is not
//! reached until the streams open, which is waiting on the record. The same shape strands
//! the window updates that would unblock a peer whose flow control is exhausted.
//!
//! # What this crate does not do
//!
//! No sockets, no runtime, no timer, no TLS. A byte stream and a clock come in; whoever
//! built them owns those concerns. It also holds no configuration of its own: the QMux
//! defaults are what a connection gets, and a caller who needs other ones is better served
//! by an argument this crate does not yet have than by a knob that only half works.

#![deny(missing_docs)]
// This crate is a join between two safe APIs and has no FFI boundary of its own. Unlike the
// crates underneath it it therefore needs no allowance list: any `unsafe` here would be a
// sign that something belongs in one of them instead.
#![deny(unsafe_code)]

mod connection;
mod error;
mod event;
mod pump;
mod transmit;

pub use connection::{Connected, Connection, QmuxConnection, connect, serve};
pub use error::{Error, ErrorKind, Result};
