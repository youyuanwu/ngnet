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
//! Productive event, open, and transmit calls may leave bounded output in QMux so adjacent
//! internal HTTP/3 passes can share one byte-stream write. A caller driving [`QmuxConnection`]
//! through the [`ngnet_h3::http::QuicConnection`] trait itself must poll
//! [`ngnet_h3::http::QuicConnection::poll_flush`] before its task returns `Pending`.
//! [`QmuxConnection::poll_finish`] is still required after the HTTP/3 driver resolves.
//!
//! # What this crate does not do
//!
//! No sockets, no runtime, no timer, no TLS. A byte stream and a clock come in; whoever
//! built them owns those concerns. It also holds no configuration of its own, and that is
//! still true now that the entry points take one: [`connect_with`] and [`serve_with`] carry
//! a [`TransportConfig`] and an [`HttpConfig`] straight through to the two layers that own
//! them, and this crate neither defaults them itself nor restates a field of either. A knob
//! of its own would be a third place to look for the answer to a question two crates already
//! answer.
//!
//! Both types are re-exported here so that reaching a `_with` entry point does not oblige a
//! caller to depend on `ngnet-qmux` or `ngnet-h3` directly merely to name an argument. They
//! are renamed on the way through because both are called `Config` where they live, and a
//! crate that hands a caller two of them cannot leave the reader to work out which is which.

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

pub use connection::{
    Connected, Connection, QmuxConnection, connect, connect_with, serve, serve_with,
};
pub use error::{Error, ErrorKind, Result};
/// The HTTP/3 layer's configuration, as [`connect_with`] and [`serve_with`] take it.
///
/// `ngnet_h3::http::Config` under a name that says which of this crate's two configurations
/// it is. Nothing is added and nothing is hidden: it is the same type, so a value built
/// through `ngnet-h3` directly is the same value.
pub use ngnet_h3::http::Config as HttpConfig;
/// The QMux transport's configuration, as [`connect_with`] and [`serve_with`] take it.
///
/// `ngnet_qmux::io::Config` under a name that says which of this crate's two configurations
/// it is. Nothing is added and nothing is hidden: it is the same type, so a value built
/// through `ngnet-qmux` directly is the same value.
pub use ngnet_qmux::io::Config as TransportConfig;
