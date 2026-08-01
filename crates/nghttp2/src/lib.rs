//! Safe, sans-I/O Rust bindings to [libnghttp2](https://nghttp2.org) for cleartext
//! HTTP/2 (h2c).
//!
//! This crate owns no transport. It never opens a socket, never blocks, and creates no
//! threads. Callers hand it the bytes they read from wherever their data came from, and
//! it hands back the bytes that must be written. That makes it usable from blocking
//! code, from any async runtime, and from tests that connect a client and a server
//! entirely in memory.
//!
//! # Safety model
//!
//! `unsafe` is denied crate-wide and re-enabled only in the modules that wrap the raw
//! bindings. Callers never need to write `unsafe` to complete any supported operation.
//!
//! Callbacks registered with a session are invoked from C stack frames. A panic escaping
//! one of them aborts the process; this is the documented contract rather than a defect,
//! and it is what the `extern "C"` ABI does by construction.
//!
//! # Escape hatch
//!
//! Capabilities this crate does not yet wrap remain reachable through [`raw`], so a
//! missing wrapper is never a blocker and no second dependency is required.

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

// `unsafe` is confined to the modules that touch the raw bindings. The crate-level deny
// above makes a stray `unsafe` anywhere else a compile error.
#[allow(unsafe_code)]
#[path = "alloc.rs"]
mod alloc_state;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod options;
#[allow(unsafe_code)]
mod session;

mod settings;

pub use error::{ALL_NATIVE_CODES, Error, ErrorCode, ErrorKind, NativeCode, Result};
pub use session::{Session, SessionBuilder};
pub use settings::Setting;

/// The raw, unsafe FFI bindings this crate is built on.
///
/// Everything libnghttp2 exposes is reachable here, including capabilities the safe API
/// does not yet cover. Using these items requires `unsafe` and upholding libnghttp2's
/// invariants yourself.
pub mod raw {
    pub use nghttp2_sys::*;
}
