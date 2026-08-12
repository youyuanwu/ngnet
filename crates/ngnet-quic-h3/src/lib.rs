//! HTTP/3 over ngtcp2.
//!
//! `ngnet-h3` speaks HTTP/3 over an abstract transport; `ngnet-quic` speaks QUIC over a UDP
//! socket. This crate is the join, and the only place in the workspace where the two families
//! meet — neither depends on the other, so a caller who wants HTTP/3 over some other QUIC
//! implementation, or QUIC with no HTTP/3 at all, pays for neither.
//!
//! # Why the connection has to be owned here
//!
//! `ngnet_h3::http::handshake` and `serve` take their transport **by value** and hold it for
//! the connection's life. More sharply, the HTTP/3 layer fills a packet by calling into its
//! transport and expecting an answer before the call returns: `StreamSource::write_next`
//! hands out `IoSlice`s that are documented invalid the moment the closure ends. There is no
//! arrangement in which those bytes reach another task in time.
//!
//! So the ngtcp2 connection lives here, and `ngnet-quic`'s endpoint hands it over once the
//! handshake is done. The endpoint keeps what is shared between connections — the socket,
//! the routing table, address validation, stateless reset — and this crate keeps the state
//! that admits exactly one owner.
//!
//! # The pump
//!
//! Every entry point begins by pumping: draining what arrived, firing the timer if it is
//! due, and producing whatever the connection now owes. That is not tidiness, it is the
//! difference between working and deadlocking.
//!
//! The HTTP/3 driver's *first* action is to open three unidirectional streams, and it will
//! not do anything else until it has them. A client cannot open a stream before the peer's
//! transport parameters arrive, and those arrive in a packet that only gets read if
//! something reads it. An implementation that only moved datagrams inside `poll_transmit`
//! would never send its first flight, because `poll_transmit` is not reached until the
//! stream opens — which is waiting on the flight. The same shape strands acknowledgements
//! and loss probes while the driver is parked on `poll_event`, and strands the
//! stream-limit notification that would unblock an exhausted peer limit.
//!
//! # What this crate does not do
//!
//! No socket, no runtime, no timer of its own beyond the connection's expiry. The endpoint
//! owns those. No TLS configuration: that belongs to whoever built the endpoint.

#![deny(missing_docs)]
// This crate is a join between two safe APIs and has no FFI boundary of its own. Unlike its
// two dependencies it therefore needs no allowance list: any `unsafe` here would be a sign
// that something belongs in one of them instead.
#![deny(unsafe_code)]

mod connection;
mod error;
mod event;
mod pump;
mod transmit;

pub use connection::{NgtcpConnection, accept, connect};
pub use error::{Error, ErrorKind, Result};
