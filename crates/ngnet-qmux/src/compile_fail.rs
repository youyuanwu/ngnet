//! Properties enforced by the compiler rather than at run time.
//!
//! These are the tests for claims the crate makes about what *cannot* be written. A runtime
//! test cannot express "this does not compile", so each case is a doctest marked
//! `compile_fail`, which the doc test harness runs and requires to fail.
//!
//! Using doctests rather than a `trybuild` fixture keeps the crate's single-dependency
//! invariant intact -- `trybuild` would be a dev-dependency, and a structural test forbids
//! those.

/// A handler cannot reach the connection it belongs to.
///
/// This is the whole of the crate's answer to dwnx's rule that `dwnx_conn_writev_stream` must
/// not be called from inside a callback. Rather than checking at run time, the API gives
/// handlers no way to name the connection: they are owned by it, and every entry point takes
/// `&mut self`, so the borrow checker rejects the attempt.
///
/// Note what the error actually is. A handler installed on a connection cannot name that
/// connection, because the binding does not exist yet when the handler is written -- the
/// handlers are an argument to the builder that produces it. That is the property, and
/// "cannot find value `conn`" is its exact expression, not a technicality standing in for it.
///
/// ```compile_fail
/// use ngnet_qmux::{Conn, Handlers, Role};
///
/// let mut conn = Conn::builder(Role::Client)
///     .handlers(Handlers::new().on_stream_open(move |_| {
///         let _ = conn.streams_bidi_left();
///         Ok(())
///     }))
///     .build()
///     .unwrap();
/// ```
///
/// Nor can one be smuggled in from outside, because a handler that captured a connection would
/// have to own it, and the connection it is installed on owns the handler.
///
/// ```compile_fail
/// use ngnet_qmux::{Conn, Handlers, Role, Timestamp, WriteRequest};
///
/// let mut other = Conn::builder(Role::Client).build().unwrap();
/// let mut conn = Conn::builder(Role::Client)
///     .handlers(Handlers::new().on_stream_open(move |_| {
///         let mut buf = [0u8; 4096];
///         let _ = other.write(&mut buf, WriteRequest::control_only(), Timestamp::from_nanos(0));
///         Ok(())
///     }))
///     .build()
///     .unwrap();
/// // `other` was moved into the handler, so it cannot also be used here.
/// let _ = other.streams_bidi_left();
/// ```
///
/// A handler is also required to be `Send`, so it cannot smuggle a thread-affine handle into a
/// connection that is itself `Send`.
///
/// ```compile_fail
/// use ngnet_qmux::{Conn, Handlers, Role};
/// use std::rc::Rc;
///
/// let shared = Rc::new(std::cell::Cell::new(0u32));
/// let captured = Rc::clone(&shared);
/// let _ = Conn::builder(Role::Client)
///     .handlers(Handlers::new().on_stream_open(move |_| {
///         captured.set(captured.get() + 1);
///         Ok(())
///     }))
///     .build();
/// ```
///
mod handlers_cannot_reach_the_connection {}

/// A connection cannot be shared between threads.
///
/// `Conn` is `Send` but deliberately not `Sync`: the callback bridge is written on every entry
/// point without synchronisation, which is sound only because those entry points require
/// `&mut self`.
///
/// ```compile_fail
/// use ngnet_qmux::{Conn, Role};
///
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<Conn<'static>>();
/// ```
mod conn_is_not_sync {}

/// The record buffer cannot be swapped mid-record.
///
/// dwnx retains the `dest` pointer for the whole `WRITE_MORE` sequence, so handing it a
/// different buffer partway through would have it writing into the first one. `RecordWriter`
/// borrows the buffer for its own lifetime, which makes the mistake unrepresentable.
///
/// ```compile_fail
/// use ngnet_qmux::{Conn, Role, Timestamp, WriteRequest};
///
/// let mut conn = Conn::builder(Role::Client).build().unwrap();
/// let mut first = [0u8; 4096];
/// let mut record = conn.record(&mut first, Timestamp::from_nanos(0));
/// let _ = record.push(WriteRequest::control_only());
/// // The buffer is borrowed by `record` until it is finished.
/// first[0] = 1;
/// let _ = record.finish();
/// ```
mod the_record_buffer_is_borrowed_for_the_whole_record {}
