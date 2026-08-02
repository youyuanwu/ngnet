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
//! # Example
//!
//! A complete request and response, with a client and a server wired directly together
//! in memory. No socket is opened and nothing blocks — the caller moves every byte.
//!
//! ```
//! use nghttp2::{BytesBody, Header, HeaderAction, Session, SessionBuilder};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Whatever the caller wants to accumulate. Handlers receive it by mutable reference.
//! #[derive(Default)]
//! struct Response {
//!     status: String,
//!     body: Vec<u8>,
//! }
//!
//! let mut client = SessionBuilder::<Response>::client()
//!     .on_header(|res: &mut Response, _frame, name: &[u8], value: &[u8]| {
//!         if name == b":status" {
//!             res.status = String::from_utf8_lossy(value).into_owned();
//!         }
//!         HeaderAction::Continue
//!     })
//!     .on_data_chunk(|res: &mut Response, _stream, chunk: &[u8]| {
//!         res.body.extend_from_slice(chunk);
//!     })
//!     .build()?;
//!
//! let mut server = SessionBuilder::<()>::server().build()?;
//!
//! let stream = client.submit_request(&[
//!     Header::new(":method", "GET"),
//!     Header::new(":scheme", "http"),
//!     Header::new(":authority", "example.test"),
//!     Header::new(":path", "/hello"),
//! ])?;
//!
//! // Hand the client's output to the server. In a real program these bytes would come
//! // from, and go to, a socket the caller owns.
//! let mut response = Response::default();
//! while let Some(block) = client.send(&mut response)? {
//!     let block = block.to_vec();
//!     server.recv(&block, &mut ())?;
//! }
//!
//! server.submit_response_with_body(
//!     stream,
//!     &[Header::new(":status", "200")],
//!     BytesBody::new(b"hello".to_vec()),
//! )?;
//!
//! // ...and the server's output back to the client.
//! while let Some(block) = server.send(&mut ())? {
//!     let block = block.to_vec();
//!     client.recv(&block, &mut response)?;
//! }
//!
//! assert_eq!(response.status, "200");
//! assert_eq!(response.body, b"hello");
//! # Ok(())
//! # }
//! ```
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
//! The other three have no such return, so the mistake is unrepresentable. A body-chunk
//! handler:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! SessionBuilder::<()>::client()
//!     .on_data_chunk(|_ctx, _stream, _chunk| HeaderAction::CancelStream);
//! ```
//!
//! a completed-frame handler:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! SessionBuilder::<()>::client()
//!     .on_frame(|_ctx, _frame| HeaderAction::CancelStream);
//! ```
//!
//! and a stream-close handler:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! SessionBuilder::<()>::client()
//!     .on_stream_close(|_ctx, _stream, _code, _err| HeaderAction::CancelStream);
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
//! A handler receives only the caller's context and the event. It is handed no session,
//! so it cannot re-enter the one libnghttp2 is executing inside.
//!
//! What a handler does get is the caller's own state, by mutable reference:
//!
//! ```
//! # use nghttp2::{HeaderAction, SessionBuilder};
//! let mut count = 0usize;
//! let mut session = SessionBuilder::<usize>::client()
//!     .on_header(|seen: &mut usize, _frame, _name, _value| {
//!         *seen += 1;
//!         HeaderAction::Continue
//!     })
//!     .build()
//!     .unwrap();
//! session.send(&mut count).unwrap();
//! ```
//!
//! There is no parameter through which a session could arrive, so a handler expecting one
//! does not compile:
//!
//! ```compile_fail
//! # use nghttp2::{HeaderAction, Session, SessionBuilder};
//! SessionBuilder::<()>::client().on_header(
//!     |_session: &mut Session<()>, _ctx, _frame, _name, _value| HeaderAction::Continue,
//! );
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
#[cfg(feature = "http")]
pub mod http;

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
pub use stream::{FrameInfo, FrameType, Goaway, HeaderCategory, StreamId};

/// The raw, unsafe FFI bindings this crate is built on.
///
/// Everything libnghttp2 exposes is reachable here, including capabilities the safe API
/// does not yet cover. Using these items requires `unsafe` and upholding libnghttp2's
/// invariants yourself.
pub mod raw {
    pub use nghttp2_sys::*;
}
