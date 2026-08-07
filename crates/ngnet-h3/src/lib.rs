//! Safe, sans-I/O bindings to [nghttp3](https://nghttp2.org/nghttp3/) — HTTP/3 message
//! framing and QPACK.
//!
//! This crate performs no I/O. It opens no socket, blocks nowhere and creates no threads:
//! you hand it the bytes that arrived on a QUIC stream, and it hands back the bytes to
//! write and tells you which stream they belong to. Choosing and driving a QUIC
//! implementation is the caller's job, which is what makes the crate usable from blocking
//! code, from any async runtime, and from tests that wire a client to a server entirely in
//! memory.
//!
//! That boundary is not a limitation adopted for convenience; it is where nghttp3 itself
//! draws the line. nghttp3 depends on no QUIC transport and on no TLS library, and neither
//! does this crate.
//!
//! # Shape of an exchange
//!
//! HTTP/3 requires three connection-level unidirectional streams before anything else can
//! happen — one for control frames and two for QPACK. The caller opens them, because the
//! caller owns the QUIC connection, and then declares them:
//!
//! ```no_run
//! use ngnet_h3::{ConnBuilder, Role, StreamId, Timestamp};
//!
//! # fn main() -> Result<(), ngnet_h3::Error> {
//! let mut conn = ConnBuilder::<()>::new(Role::Client).build()?;
//!
//! // Client-initiated unidirectional stream ids: 2, 6, 10, ...
//! conn.bind_control_stream(StreamId::new(2)?)?;
//! conn.bind_qpack_streams(StreamId::new(6)?, StreamId::new(10)?)?;
//!
//! // The send half of a driver. Every line of it earns its place; see below.
//! while let Some(send) = conn.writev_stream(&mut ())? {
//!     let stream = send.stream();
//!     let offered = send.len();
//!     let fin = send.fin();
//!
//!     let accepted = write_to_quic(stream, send.slices());
//!     send.commit(accepted)?;                       // required even when accepted is 0
//!
//!     if accepted > 0 && transport_copied_the_bytes() {
//!         conn.add_ack_offset(stream, accepted as u64, &mut ())?;
//!     }
//!     if accepted < offered {
//!         conn.block_stream(stream)?;               // else this stream starves the rest
//!     } else if fin {
//!         finish_quic_stream(stream);               // only once every byte before it went
//!     }
//! }
//! # Ok(())
//! # }
//! # fn write_to_quic(_: ngnet_h3::StreamId, _: &[std::io::IoSlice<'_>]) -> usize { 0 }
//! # fn finish_quic_stream(_: ngnet_h3::StreamId) {}
//! # fn transport_copied_the_bytes() -> bool { true }
//! ```
//!
//! Five things there are not decoration:
//!
//! - **`commit` is not optional**, even with nothing on offer. A stream can end with an
//!   empty final write, and skipping that commit stalls the connection permanently.
//! - **`add_ack_offset` is the only thing that releases outgoing body buffers.** Reporting
//!   bytes written does not. When to call it depends on your transport: one that copies
//!   what it accepts (quinn's `write` does) frees the buffer immediately, so reporting on
//!   acceptance is sound. One that borrows the slices must wait until it is genuinely done
//!   with them.
//! - **`block_stream` on a short write.** The connection offers the highest-priority
//!   writable stream and goes on offering the same one until it has nothing left for it, so
//!   without this a stream whose window is exhausted is re-offered ahead of every other one
//!   forever. Clear it with [`Conn::unblock_stream`] when the transport says the stream is
//!   writable again. This is a different thing from a body source having nothing to give,
//!   which the source signals itself and which [`Conn::resume_stream`] clears.
//! - **The end of a stream travels with its last byte, not with the offer.** `fin` says
//!   this offer ends the stream *if all of it goes out*; finishing the QUIC stream after a
//!   short write truncates the message.
//! - **[`Conn::close_stream`] once a stream is done in both directions.** It is what
//!   releases the stream's body buffers and its send accounting; until then they are held.
//!
//! # What a whole driver also needs
//!
//! The loop above is the send half. A working client additionally has to:
//!
//! - submit something — [`Conn::submit_request`], and [`Conn::submit_trailers`] after a
//!   body that asked to be followed by one;
//! - feed inbound bytes and the end-of-stream marker to [`Conn::read_stream`], and extend
//!   QUIC flow control by the [`FlowCredit`] it returns;
//! - credit body bytes itself, since [`FlowCredit`] excludes them, and credit whatever
//!   arrives late through [`ConnBuilder::on_deferred_consume`] — see [`FlowCredit`] for why
//!   there are three sources rather than one;
//! - act on [`ConnBuilder::on_stop_sending`] and [`ConnBuilder::on_reset_stream`], which
//!   are instructions to the QUIC layer rather than information.
//!
//! `ngnet-h3-tests` in this repository is a complete worked example: a few hundred lines
//! joining this crate to quinn, including the parts that are easy to get wrong.
//!
//! # Two contracts worth knowing before you start
//!
//! **Sending is a transaction, not a copy.** [`Conn::writev_stream`] does not give you a
//! buffer you own; it lends you bytes and expects to be told how many the transport
//! accepted. The borrow is enforced: [`SendGuard`] holds the connection until you commit,
//! so the bytes cannot be used afterwards. Committing zero is normal and sometimes
//! required — a stream can end with an empty final write, and skipping that commit stalls
//! the connection.
//!
//! **Reading returns flow-control credit, not a byte count.** Everything you supply to
//! [`Conn::read_stream`] is consumed; there is never a remainder to re-present. What comes
//! back is how much QUIC flow-control credit you may now extend, which deliberately
//! excludes body payload — see [`FlowCredit`].
//!
//! # Outgoing bodies are borrowed, not copied
//!
//! nghttp3 has no copying data source. A [`BodySource`] hands over [`RetainedBytes`], and
//! the bytes behind them are read again on every write until the peer acknowledges them.
//! This crate holds those buffers for exactly as long as that, which means **reporting
//! acknowledgement through [`Conn::add_ack_offset`] is required, not an optimisation**: it
//! is the only thing that releases them. A caller that reports bytes written but never
//! bytes acknowledged will hold every body buffer it ever sent until the connection is
//! dropped.
//!
//! # Events you have to act on
//!
//! Four connection-level events are instructions to a QUIC layer this crate does not own,
//! so ignoring them has consequences it cannot make up for. [`ConnBuilder::on_stop_sending`]
//! and [`ConnBuilder::on_reset_stream`] ask for a stream to be stopped or reset — without
//! them, the peer keeps sending bytes nothing will read. [`ConnBuilder::on_shutdown`]
//! reports the peer beginning a graceful shutdown, and [`ConnBuilder::on_peer_settings`]
//! delivers the peer's limits.
//!
//! # Errors and unusable connections
//!
//! Most failures are recoverable and say what went wrong. A few are not: nghttp3 documents
//! that after the read or write path fails, calling anything but the destructor is
//! undefined behaviour, and marks out-of-memory and callback failure as fatal wherever
//! they appear. A connection latches those and refuses further work with
//! [`ErrorKind::ConnectionUnusable`], because a safe API must not be able to reach
//! undefined behaviour. Dropping it is always allowed and always cleans up.
//!
//! # Panics
//!
//! A panic inside a handler unwinds into a C stack frame, which aborts the process. Handle
//! errors in handlers rather than panicking in them.
//!
//! # Scope
//!
//! Cleartext framing only, in the sense that TLS and QUIC are the caller's concern. Server
//! push is not implemented, because nghttp3 does not implement it. There is deliberately
//! no asynchronous layer here; this crate is the core such a layer would be built on.

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

// `unsafe` is confined to the modules that touch the raw bindings. The crate-level deny
// above makes a stray `unsafe` anywhere else a compile error, which is a stronger
// guarantee than a test scanning for the keyword -- though there is one of those too.
#[allow(unsafe_code)]
mod alloc;
#[allow(unsafe_code)]
mod callbacks;
#[allow(unsafe_code)]
mod conn;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod send;
#[allow(unsafe_code)]
mod settings;

mod body;
mod handlers;
mod header;
mod state;
mod stream;

pub use body::{BodyOutcome, BodySource, FixedBody, RetainedBytes};
pub use conn::{Conn, ConnBuilder, FlowCredit, Role, Timestamp};
pub use error::{ALL_NATIVE_CODES, Error, ErrorCode, ErrorKind, NativeCode, Result};
pub use handlers::{FieldAction, FieldSection, FieldToken, PeerSettings, Shutdown, StreamClosed};
pub use header::Header;
pub use send::SendGuard;
pub use settings::Settings;
pub use stream::{Directionality, Initiator, StreamId};

/// The raw, unsafe FFI bindings this crate is built on.
///
/// Everything nghttp3 exposes is reachable here, including capabilities the safe API does
/// not yet cover. Using these items requires `unsafe` and upholding nghttp3's invariants
/// yourself — including the ones it checks only with `assert`, which aborts where it is
/// compiled in and checks nothing where it is not.
pub mod raw {
    pub use ngnet_h3_sys::*;
}
