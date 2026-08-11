//! Safe, sans-I/O Rust bindings to [ngtcp2], the QUIC transport library.
//!
//! This crate wraps `libngtcp2` the way [`ngnet-h3`] wraps `libnghttp3`: a state machine
//! that performs no I/O, owns no socket, spawns no thread and reads no clock. You hand it
//! datagrams that arrived and a timestamp; it hands you back datagrams to send and a
//! deadline by which it wants to be called again. What moves those bytes is entirely yours.
//!
//! [ngtcp2]: https://github.com/ngtcp2/ngtcp2
//! [`ngnet-h3`]: https://docs.rs/ngnet-h3
//!
//! # Why the clock is an argument
//!
//! QUIC is timer-driven in a way HTTP/2 and HTTP/3 framing are not: loss recovery, ACK
//! delay and the idle timeout all depend on time passing, and a connection that is never
//! told the time has passed simply stops. ngtcp2 therefore wants a timestamp on almost
//! every call. Reading a clock here would make the crate untestable and would pick a clock
//! on the caller's behalf, so [`Timestamp`] is an opaque count of nanoseconds in whatever
//! monotonic timescale you choose, and supplying it is your job.
//!
//! # Validation happens here, not in C
//!
//! ngtcp2 checks its preconditions with `assert()`. Those checks are compiled out whenever
//! `NDEBUG` is defined, which is every release build — so in exactly the builds you ship,
//! passing an out-of-range transport parameter is undefined behaviour rather than a crash.
//! This crate therefore validates its own inputs in Rust before calling in, and a
//! configuration error is an [`Error`] in debug and release alike.
//!
//! # The API names in ngtcp2's documentation are macros
//!
//! Most of ngtcp2's public functions are function-like macros that inject a struct-version
//! constant and forward to a `_versioned` symbol. `bindgen` does not emit function-like
//! macros, so those names do not exist in the generated bindings at all. This crate
//! reimplements them in a single internal module, which is the only place a version
//! constant appears.
//!
//! # Panics
//!
//! A panic inside a handler unwinds into a C stack frame, which aborts the process. Handle
//! errors in handlers rather than panicking in them.
//!
//! # Scope
//!
//! One connection at a time from the state machine, client or server. Above it, behind the
//! default-on `endpoint` feature, an asynchronous layer that owns a UDP socket and the
//! connections reachable through it. 0-RTT, unreliable datagrams, connection migration and
//! key update are not implemented.

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

// `unsafe` is confined to the modules that touch the raw bindings. The crate-level deny
// above makes a stray `unsafe` anywhere else a compile error, and a test reads this exact
// list back out of this file so the two cannot disagree.
//
// Core module files are deliberately flat -- `tls.rs`, never `tls/mod.rs`. That test derives
// a module's name from its file stem, so a nested file would produce a name this list never
// mentions and be reported as using `unsafe` without a grant. The `endpoint` subtree below
// is the one exception, and it earns it by containing no `unsafe` at all, which a separate
// test pins.
#[allow(unsafe_code)]
mod accept;
#[allow(unsafe_code)]
mod alloc;
#[allow(unsafe_code)]
mod callbacks;
mod cid;
#[allow(unsafe_code)]
mod conn;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod ffi;
#[allow(unsafe_code)]
mod packet;
#[allow(unsafe_code)]
mod params;
#[allow(unsafe_code)]
mod path;
#[allow(unsafe_code)]
mod retain;
#[allow(unsafe_code)]
mod settings;
#[allow(unsafe_code)]
mod stream_io;
#[allow(unsafe_code)]
mod tls;
#[cfg(feature = "tls-ossl")]
#[allow(unsafe_code)]
mod tls_ossl;

mod handlers;
mod rand;
mod stream;
mod time;
mod validate;

#[cfg(feature = "endpoint")]
pub mod endpoint;

pub use accept::{
    Inspection, VERSION_V1, inspect, is_acceptable_initial, supported_versions,
    write_version_negotiation,
};
pub use cid::{ConnectionId, MAX_LEN as MAX_CID_LEN, MIN_LEN as MIN_CID_LEN};
pub use conn::{Conn, ConnBuilder};
pub use error::{ApplicationErrorCode, Error, ErrorKind, NativeCode, Result, TransportErrorCode};
pub use handlers::{Handlers, StreamCloseReason};
pub use packet::{ExpiryOutcome, ReadOutcome, WriteOutcome};
pub use params::{
    DEFAULT_CONNECTION_DATA, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_STREAMS, DEFAULT_STREAM_DATA,
    TransportParams,
};
pub use rand::EntropySource;
pub use settings::Settings;
pub use stream::{Directionality, Initiator, StreamId};
pub use stream_io::StreamWrite;
pub use time::{Duration, Timestamp};
pub use tls::{NativeTlsHandle, Role, TlsBackend, TlsSession};

#[cfg(feature = "tls-ossl")]
pub use tls_ossl::{OsslBackend, OsslBackendBuilder, OsslSession, Verify};

/// The raw bindings, for capabilities the safe API does not cover yet.
///
/// Everything here is `unsafe` and upholding ngtcp2's invariants becomes your
/// responsibility. Reaching for this module is a sign something is missing from the safe
/// API; please say so.
pub mod raw {
    pub use ngnet_quic_sys::*;
}
