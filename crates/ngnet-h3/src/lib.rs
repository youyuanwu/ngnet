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
//! // Ask what to send, write it, then say how much the transport took.
//! while let Some(send) = conn.writev_stream()? {
//!     let accepted = write_to_quic(send.stream(), send.slices(), send.fin());
//!     send.commit(accepted)?;
//! }
//! # Ok(())
//! # }
//! # fn write_to_quic(_: ngnet_h3::StreamId, _: &[std::io::IoSlice<'_>], _: bool) -> usize { 0 }
//! ```
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
mod stream;

pub use body::{BodyOutcome, BodySource, FixedBody, RetainedBytes};
pub use conn::{Conn, ConnBuilder, FlowCredit, Role, Timestamp};
pub use error::{ALL_NATIVE_CODES, Error, ErrorCode, ErrorKind, NativeCode, Result};
pub use handlers::{FieldAction, FieldSection, FieldToken, StreamClosed};
pub use header::Header;
pub use send::SendGuard;
pub use settings::Settings;
pub use stream::{Directionality, Initiator, StreamId};

/// The raw, unsafe FFI bindings this crate is built on.
///
/// Everything nghttp3 exposes is reachable here, including capabilities the safe API does
/// not yet cover. Using these items requires `unsafe` and upholding nghttp3's invariants
/// yourself — including the ones it checks only with `assert`, and therefore not at all in
/// a release build.
pub mod raw {
    pub use ngnet_h3_sys::*;
}
