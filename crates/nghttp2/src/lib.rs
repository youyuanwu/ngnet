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
//! # Compile-time guarantees
//!
//! Several of this crate's safety properties are enforced by the type system rather than
//! by convention. Each is stated below as a pair of examples: one that must compile and
//! one that must not, so neither can pass for the wrong reason.
//!
//! ## A session may move between threads, but never be shared
//!
//! Moving is fine:
//!
//! ```
//! # use nghttp2::Session;
//! fn assert_send<T: Send>() {}
//! assert_send::<Session<()>>();
//! ```
//!
//! Sharing is not. libnghttp2 does no internal locking, so two threads must never touch
//! one session at once:
//!
//! ```compile_fail
//! # use nghttp2::Session;
//! fn assert_sync<T: Sync>() {}
//! assert_sync::<Session<()>>();
//! ```
//!
//! ## Pending output cannot outlive the call that invalidates it
//!
//! libnghttp2 invalidates the block returned by [`Session::send`] on the next send, so
//! the block borrows the session. Finish with it first:
//!
//! ```
//! # use nghttp2::SessionBuilder;
//! let mut session = SessionBuilder::<()>::client().build().unwrap();
//! let first = session.send(&mut ()).unwrap().unwrap().to_vec();
//! let second = session.send(&mut ()).unwrap();
//! # let _ = (first, second);
//! ```
//!
//! Holding one across another call does not compile:
//!
//! ```compile_fail
//! # use nghttp2::SessionBuilder;
//! let mut session = SessionBuilder::<()>::client().build().unwrap();
//! let held = session.send(&mut ()).unwrap().unwrap();
//! let _next = session.send(&mut ()).unwrap();
//! println!("{}", held.len());
//! ```
//!
//! ## Only header-phase handlers can cancel a stream
//!
//! libnghttp2 treats a nonzero return from the body, frame and stream-close callbacks as
//! fatal to the whole connection rather than to one stream, so only the header-phase
//! handlers offer cancellation:
//!
//! ```
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! SessionBuilder::<()>::client()
//!     .on_header(|_ctx, _frame, _name, _value| HeaderAction::CancelStream);
//! ```
//!
//! A body-chunk handler has no such return, so the mistake is unrepresentable:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! SessionBuilder::<()>::client()
//!     .on_data_chunk(|_ctx, _stream, _chunk| HeaderAction::CancelStream);
//! ```
//!
//! ## The context type is fixed when the session is built
//!
//! Handlers are registered before the session exists and a Rust closure cannot be generic
//! over the type it is later handed, so the context type is part of the session's own
//! type:
//!
//! ```
//! # use nghttp2::SessionBuilder;
//! let mut session = SessionBuilder::<Vec<u8>>::client().build().unwrap();
//! let mut log: Vec<u8> = Vec::new();
//! session.send(&mut log).unwrap();
//! ```
//!
//! Handing it a different type does not compile:
//!
//! ```compile_fail
//! # use nghttp2::SessionBuilder;
//! let mut session = SessionBuilder::<Vec<u8>>::client().build().unwrap();
//! let mut wrong = String::new();
//! session.send(&mut wrong).unwrap();
//! ```
//!
//! ## Handlers are never given the session
//!
//! A handler receives only the caller's context and the event, so it cannot re-enter the
//! session libnghttp2 is executing inside. A closure that tries to capture the session it
//! is being registered on does not compile, because the session does not yet exist:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! let mut session = SessionBuilder::<()>::client()
//!     .on_header(move |_ctx, _frame, _name, _value| {
//!         let _ = session.want_read();
//!         HeaderAction::Continue
//!     })
//!     .build()
//!     .unwrap();
//! ```
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
mod body;
#[allow(unsafe_code)]
mod callbacks;
#[allow(unsafe_code)]
mod error;
#[allow(unsafe_code)]
mod options;
#[allow(unsafe_code)]
mod session;

mod handlers;
mod header;
mod settings;
#[allow(unsafe_code)]
mod state;
mod stream;

pub use error::{ALL_NATIVE_CODES, Error, ErrorCode, ErrorKind, NativeCode, Result};
pub use body::{BodyError, BodyOutcome, BodySource, BytesBody};
pub use handlers::HeaderAction;
pub use header::Header;
pub use session::{Session, SessionBuilder};
pub use settings::Setting;
pub use stream::{FrameInfo, FrameType, StreamId};

/// The raw, unsafe FFI bindings this crate is built on.
///
/// Everything libnghttp2 exposes is reachable here, including capabilities the safe API
/// does not yet cover. Using these items requires `unsafe` and upholding libnghttp2's
/// invariants yourself.
pub mod raw {
    pub use nghttp2_sys::*;
}
