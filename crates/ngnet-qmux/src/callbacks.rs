//! The bridge between dwnx's C callbacks and the caller's Rust closures.
//!
//! # The problem
//!
//! dwnx takes a table of `extern "C"` function pointers and a `void *user_data`, and calls
//! back into them from inside entry points like `dwnx_conn_read`. To run a Rust closure there,
//! something has to carry a pointer to the closure across the boundary and back, and that
//! pointer has to stay valid for as long as dwnx might use it.
//!
//! # The shape of the answer
//!
//! A [`BridgeSlot`] is boxed once, when the connection is built, and its address is what dwnx
//! receives as `user_data`. The box never moves, so the address stays good even if the `Conn`
//! that owns it does. The slot is empty most of the time.
//!
//! Before any entry point that can fire a callback, [`BridgeGuard`] writes a [`Bridge`] into
//! the slot -- a pair of live mutable borrows of the caller's handlers and of the connection's
//! own scratch state -- and clears it again on the way out, including when the stack is
//! unwinding. A callback that fires in between reads the slot and finds those borrows; one
//! that fires outside such a window, which should never happen, finds nothing and returns an
//! error rather than dereferencing a stale pointer.
//!
//! # Why there is no re-entrancy check
//!
//! The slot holds exactly one `Bridge`, so a nested entry point would overwrite the outer
//! one's borrows -- aliasing that Rust forbids. `ngnet-quic` handles this by relying on
//! ngtcp2's documented rule that its main entry points may not be called from callbacks.
//!
//! dwnx's rule is narrower: only `dwnx_conn_writev_stream` carries that prohibition. Rather
//! than police the difference at run time, this crate removes the capability. Handlers receive
//! event values only; they are never handed the connection, and because they are owned by it
//! and every entry point takes `&mut self`, the borrow checker will not let a handler hold one
//! either. There is nothing to call, so there is nothing to check.
//!
//! What remains is a debug assertion that the slot is empty when a guard installs itself. It
//! is not load-bearing -- it cannot be reached through the public API -- but if a future change
//! ever makes nesting possible, this turns it into a loud failure rather than a quiet one.
//!
//! # Panics
//!
//! A panic in a handler unwinds into a C stack frame, which is undefined behaviour, so the
//! process aborts. `BridgeGuard`'s `Drop` still runs during unwinding and clears the slot;
//! that is tidiness, not a recovery path.

use ngnet_qmux_sys as sys;

use core::cell::Cell;
use core::ffi::c_void;
use core::ptr;

use crate::handlers::{
    HandlerError, HandlerResult, Handlers, StreamCloseEvent, StreamDataEvent, StreamLimitKind,
};
use crate::params::TransportParams;
use crate::stream::StreamId;

/// Connection state that callbacks write to and the owning entry point reads afterwards.
///
/// This is how a handler's observations get out. Handlers cannot touch the connection, so
/// anything a callback needs to record -- the peer's transport parameters, the error a handler
/// returned -- lands here and is collected once control comes back to Rust.
#[derive(Default)]
pub(crate) struct Scratch {
    /// The peer's transport parameters, cached when `recv_transport_params` fires.
    ///
    /// dwnx has no getter for these -- `dwnx_conn_get_local_transport_params` returns the
    /// local set -- so caching them at the callback is the only way to offer them later.
    pub(crate) peer_params: Option<TransportParams>,
    /// The error a handler returned, if one did.
    ///
    /// dwnx normalises every nonzero callback return to `DWNX_ERR_CALLBACK_FAILURE`, so
    /// without this the caller's own message would be lost by the time the entry point
    /// returns.
    pub(crate) handler_error: Option<HandlerError>,
}

/// The live borrows a callback needs, valid only for the duration of one entry point.
///
/// `Copy` so it can be read out of the [`Cell`] without disturbing it.
#[derive(Clone, Copy)]
struct Bridge {
    handlers: *mut Handlers<'static>,
    scratch: *mut Scratch,
}

/// The stable, boxed cell whose address dwnx holds as `user_data`.
///
/// Interior-mutable on purpose, and this is a soundness matter rather than a convenience.
/// Writing through a `&mut BridgeSlot` held across the C call would leave two live `&mut` to
/// the same slot -- the guard's, and the one a callback forms from `user_data` -- which is
/// aliasing Rust forbids, even though the two never overlap in time in any observable way.
/// With a `Cell` no `&mut` to the slot ever exists, so the question does not arise. This is
/// the arrangement `ngnet-quic`'s bridge uses, for the same reason.
#[derive(Default)]
pub(crate) struct BridgeSlot {
    bridge: Cell<Option<Bridge>>,
}

impl BridgeSlot {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// Installs a [`Bridge`] for the duration of one entry point.
pub(crate) struct BridgeGuard<'a> {
    slot: &'a BridgeSlot,
}

impl<'a> BridgeGuard<'a> {
    /// Make the handlers and scratch reachable from callbacks until this guard drops.
    ///
    /// The lifetime transmute is the crux, so it is worth being explicit about why it is
    /// sound. `Handlers<'h>` borrows from the caller; the slot cannot name `'h` because it
    /// outlives any single call. What makes the erased pointer safe to follow is that the
    /// guard clears the slot before returning, so no callback can observe it after the real
    /// borrow ends -- and dwnx never retains `user_data` beyond the call, because it only ever
    /// passes it back to the callbacks it invokes synchronously.
    pub(crate) fn new<'h>(
        slot: &'a BridgeSlot,
        handlers: &mut Handlers<'h>,
        scratch: &mut Scratch,
    ) -> Self {
        let previous = slot.bridge.take();
        debug_assert!(
            previous.is_none(),
            "a bridge is already installed: an entry point was re-entered, which the API is \
             supposed to make impossible"
        );

        let handlers: *mut Handlers<'h> = handlers;
        slot.bridge.set(Some(Bridge {
            handlers: handlers.cast::<Handlers<'static>>(),
            scratch,
        }));
        Self { slot }
    }
}

impl Drop for BridgeGuard<'_> {
    fn drop(&mut self) {
        self.slot.bridge.set(None);
    }
}

/// Run `f` with the handlers and scratch, if a bridge is installed.
///
/// Returns `0` when there is no bridge or no handler: a callback that fires with nothing to
/// dispatch to is not an error, it is an unobserved event.
///
/// # Safety
///
/// `user_data` must be the pointer given to a dwnx constructor, i.e. the address of a live
/// boxed [`BridgeSlot`].
unsafe fn dispatch<F>(user_data: *mut c_void, f: F) -> i32
where
    F: FnOnce(&mut Handlers<'static>, &mut Scratch) -> HandlerResult,
{
    if user_data.is_null() {
        return 0;
    }

    // SAFETY: the caller guarantees `user_data` is the live boxed slot. Only a shared
    // reference is formed, so this cannot alias the guard's own borrow.
    let slot = unsafe { &*user_data.cast::<BridgeSlot>() };
    let Some(bridge) = slot.bridge.get() else {
        // No entry point is active. Unreachable through this crate's API; treated as "nobody
        // is listening" rather than as a failure.
        return 0;
    };

    let handlers = bridge.handlers;
    let scratch = bridge.scratch;

    // SAFETY: `BridgeGuard` installed these from live borrows and clears the slot before those
    // borrows end, so both are valid here. They do not alias: handlers and scratch are
    // distinct fields of the connection.
    let (handlers, scratch) = unsafe { (&mut *handlers, &mut *scratch) };

    match f(handlers, scratch) {
        Ok(()) => 0,
        Err(error) => {
            // Stash the caller's error before handing C the one code it understands.
            scratch.handler_error = Some(error);
            sys::DWNX_ERR_CALLBACK_FAILURE
        }
    }
}

/// Build the callback table dwnx is given.
///
/// The mandatory random source and every application event callback are populated. The
/// write-offset notification stays unset because this API reports accepted bytes synchronously
/// from each write, before the asynchronous layer can dequeue a stream-close event.
pub(crate) fn callbacks() -> sys::dwnx_callbacks {
    sys::dwnx_callbacks {
        rand: Some(fill_random),
        recv_transport_params: Some(on_recv_transport_params),
        recv_stream_data: Some(on_recv_stream_data),
        stream_open: Some(on_stream_open),
        stream_close: Some(on_stream_close),
        stream_reset: Some(on_stream_reset),
        stream_stop_sending: Some(on_stream_stop_sending),
        recv_stop_sending: Some(on_recv_stop_sending),
        extend_max_stream_data: Some(on_extend_max_stream_data),
        extend_max_local_streams_bidi: Some(on_extend_max_local_streams_bidi),
        extend_max_local_streams_uni: Some(on_extend_max_local_streams_uni),
        extend_max_remote_streams_bidi: Some(on_extend_max_remote_streams_bidi),
        extend_max_remote_streams_uni: Some(on_extend_max_remote_streams_uni),
        write_stream_data_offset: None,
    }
}

unsafe extern "C" fn fill_random(dest: *mut u8, destlen: usize) {
    if destlen == 0 {
        return;
    }
    if dest.is_null() {
        std::process::abort();
    }

    // SAFETY: dwnx provides a writable buffer of exactly `destlen` bytes.
    let dest = unsafe { core::slice::from_raw_parts_mut(dest, destlen) };
    if getrandom::fill(dest).is_err() {
        // dwnx's callback cannot report failure, and unwinding through C is undefined.
        std::process::abort();
    }
}

/// A stream id from C, which the protocol guarantees is in range.
///
/// dwnx produced it, so it satisfies the encoding; if it somehow does not, reporting a
/// callback failure is better than panicking across the boundary.
fn stream_id(raw: i64) -> Result<StreamId, HandlerError> {
    StreamId::new(raw).map_err(|_| HandlerError::new("dwnx supplied an invalid stream id"))
}

unsafe extern "C" fn on_recv_transport_params(
    _conn: *mut sys::dwnx_conn,
    params: *const sys::dwnx_transport_params,
    user_data: *mut c_void,
) -> i32 {
    let handle = |handlers: &mut Handlers<'static>, scratch: &mut Scratch| {
            if params.is_null() {
                return Err(HandlerError::new("dwnx supplied null transport parameters"));
            }
            // SAFETY: non-null and valid for the duration of the callback; copied out here so
            // nothing borrowed escapes.
            let params = TransportParams::from_native(unsafe { ptr::read(params) });

            // Cached unconditionally: dwnx offers no getter for the peer's parameters, so this
            // is the only opportunity to keep them.
            scratch.peer_params = Some(params.clone());

            match handlers.recv_transport_params.as_mut() {
                Some(handler) => handler(&params),
                None => Ok(()),
            }
    };

    // SAFETY: dwnx passes the `user_data` given to the constructor.
    unsafe { dispatch(user_data, handle) }
}

unsafe extern "C" fn on_recv_stream_data(
    _conn: *mut sys::dwnx_conn,
    flags: u32,
    stream_id_raw: i64,
    offset: u64,
    data: *const u8,
    datalen: usize,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    let handle = |handlers: &mut Handlers<'static>, _: &mut Scratch| {
            let Some(handler) = handlers.recv_stream_data.as_mut() else {
                return Ok(());
            };
            let stream_id = stream_id(stream_id_raw)?;

            // dwnx passes a null pointer with a zero length for an empty FIN-only delivery.
            let data = if data.is_null() || datalen == 0 {
                &[][..]
            } else {
                // SAFETY: dwnx guarantees `datalen` readable bytes, valid for this call. The
                // slice does not outlive the handler.
                unsafe { core::slice::from_raw_parts(data, datalen) }
            };

            handler(StreamDataEvent {
                stream_id,
                offset,
                data,
                fin: flags & sys::DWNX_STREAM_DATA_FLAG_FIN != 0,
            })
    };

    // SAFETY: as above.
    unsafe { dispatch(user_data, handle) }
}

unsafe extern "C" fn on_stream_open(
    _conn: *mut sys::dwnx_conn,
    stream_id_raw: i64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.stream_open.as_mut() else {
                return Ok(());
            };
            handler(stream_id(stream_id_raw)?)
        })
    }
}

unsafe extern "C" fn on_stream_close(
    _conn: *mut sys::dwnx_conn,
    flags: u32,
    stream_id_raw: i64,
    rx_app_error_code: u64,
    tx_app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.stream_close.as_mut() else {
                return Ok(());
            };
            // The flags say whether each code was actually set; without checking them, a
            // caller cannot tell "closed with code 0" from "closed cleanly".
            let rx_set = flags & sys::DWNX_STREAM_CLOSE_FLAG_RX_APP_ERROR_CODE_SET != 0;
            let tx_set = flags & sys::DWNX_STREAM_CLOSE_FLAG_TX_APP_ERROR_CODE_SET != 0;

            handler(StreamCloseEvent {
                stream_id: stream_id(stream_id_raw)?,
                rx_app_error_code: rx_set.then_some(rx_app_error_code),
                tx_app_error_code: tx_set.then_some(tx_app_error_code),
            })
        })
    }
}

unsafe extern "C" fn on_stream_reset(
    _conn: *mut sys::dwnx_conn,
    stream_id_raw: i64,
    final_size: u64,
    app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.stream_reset.as_mut() else {
                return Ok(());
            };
            handler(stream_id(stream_id_raw)?, final_size, app_error_code)
        })
    }
}

unsafe extern "C" fn on_stream_stop_sending(
    _conn: *mut sys::dwnx_conn,
    stream_id_raw: i64,
    app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.stream_stop_sending.as_mut() else {
                return Ok(());
            };
            handler(stream_id(stream_id_raw)?, app_error_code)
        })
    }
}

unsafe extern "C" fn on_recv_stop_sending(
    _conn: *mut sys::dwnx_conn,
    stream_id_raw: i64,
    app_error_code: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.recv_stop_sending.as_mut() else {
                return Ok(());
            };
            handler(stream_id(stream_id_raw)?, app_error_code)
        })
    }
}

unsafe extern "C" fn on_extend_max_stream_data(
    _conn: *mut sys::dwnx_conn,
    stream_id_raw: i64,
    max_data: u64,
    user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.extend_max_stream_data.as_mut() else {
                return Ok(());
            };
            handler(stream_id(stream_id_raw)?, max_data)
        })
    }
}

/// The four stream-limit callbacks differ only in which limit moved.
unsafe fn extend_max_streams(
    kind: StreamLimitKind,
    max_streams: u64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe {
        dispatch(user_data, |handlers, _| {
            let Some(handler) = handlers.extend_max_streams.as_mut() else {
                return Ok(());
            };
            handler(kind, max_streams)
        })
    }
}

unsafe extern "C" fn on_extend_max_local_streams_bidi(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe { extend_max_streams(StreamLimitKind::LocalBidi, max_streams, user_data) }
}

unsafe extern "C" fn on_extend_max_local_streams_uni(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe { extend_max_streams(StreamLimitKind::LocalUni, max_streams, user_data) }
}

unsafe extern "C" fn on_extend_max_remote_streams_bidi(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe { extend_max_streams(StreamLimitKind::RemoteBidi, max_streams, user_data) }
}

unsafe extern "C" fn on_extend_max_remote_streams_uni(
    _conn: *mut sys::dwnx_conn,
    max_streams: u64,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    unsafe { extend_max_streams(StreamLimitKind::RemoteUni, max_streams, user_data) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A callback firing with no bridge installed is inert rather than unsound.
    #[test]
    fn dispatch_without_a_bridge_is_a_no_op() {
        let slot = BridgeSlot::new();
        let slot_ptr: *const BridgeSlot = &slot;

        // SAFETY: a live slot with no bridge installed.
        let rv = unsafe { dispatch(slot_ptr.cast_mut().cast(), |_, _| Ok(())) };
        assert_eq!(rv, 0);
    }

    /// A null `user_data` is inert too.
    #[test]
    fn dispatch_with_null_user_data_is_a_no_op() {
        // SAFETY: null is explicitly handled.
        let rv = unsafe { dispatch(ptr::null_mut(), |_, _| Ok(())) };
        assert_eq!(rv, 0);
    }

    #[test]
    fn guard_installs_and_clears_the_slot() {
        let slot = BridgeSlot::new();
        let mut handlers = Handlers::new();
        let mut scratch = Scratch::default();

        {
            let guard = BridgeGuard::new(&slot, &mut handlers, &mut scratch);
            assert!(guard.slot.bridge.get().is_some());
        }
        assert!(slot.bridge.get().is_none());
    }

    /// A handler error becomes the one code C understands, and the message survives in scratch.
    #[test]
    fn handler_errors_are_stashed_and_reported() {
        let slot = BridgeSlot::new();
        let mut handlers = Handlers::new();
        let mut scratch = Scratch::default();
        let slot_ptr: *const BridgeSlot = &slot;

        let guard = BridgeGuard::new(&slot, &mut handlers, &mut scratch);
        // SAFETY: a bridge is installed for the lifetime of `guard`.
        let rv = unsafe {
            dispatch(slot_ptr.cast_mut().cast(), |_, _| {
                Err(HandlerError::new("nope"))
            })
        };
        drop(guard);

        assert_eq!(rv, sys::DWNX_ERR_CALLBACK_FAILURE);
        assert_eq!(scratch.handler_error, Some(HandlerError::new("nope")));
    }
}
