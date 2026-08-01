//! `extern "C"` trampolines and the context bridge they travel through.
//!
//! # How the caller's state reaches a handler
//!
//! libnghttp2 hands every callback the session's `user_data` pointer, fixed when the
//! session was built. The caller's application state, however, is supplied per call and
//! borrowed only for its duration. The two are reconciled by swapping `user_data` around
//! each FFI call: [`crate::session::Session`] builds a [`Bridge`] on the stack, installs
//! a pointer to it, makes the call, and clears the pointer again.
//!
//! The bridge deliberately does not hold the session. It holds disjoint mutable borrows
//! of individual session *fields*, which the borrow checker permits, so a trampoline can
//! release a body entry or park an error without ever aliasing the session that libnghttp2
//! is executing inside.
//!
//! # Panics
//!
//! These are plain `extern "C"` functions, so a panic escaping a caller's handler aborts
//! the process. That is the crate's documented contract, and it needs no code: unwinding
//! out of an `extern "C"` frame is defined to abort.

use core::ffi::c_void;

use nghttp2_sys as sys;

use crate::error::ErrorCode;
use crate::handlers::{HeaderAction, Handlers};
use crate::state::{BodyRegistry, PendingErrors, ResponseGuard};
use crate::stream::{FrameInfo, StreamId};

/// Everything a trampoline may touch during one FFI call.
///
/// Constructed on the stack for the duration of a single call into libnghttp2 and torn
/// down immediately afterwards.
pub(crate) struct Bridge<'a, C> {
    pub(crate) handlers: &'a mut Handlers<C>,
    pub(crate) context: &'a mut C,
    #[expect(dead_code, reason = "read by the phase that adds message bodies")]
    pub(crate) bodies: &'a mut BodyRegistry,
    #[expect(dead_code, reason = "read by the phase that adds message bodies")]
    pub(crate) pending: &'a mut PendingErrors,
    #[expect(dead_code, reason = "read by the phase that adds message submission")]
    pub(crate) responded: &'a mut ResponseGuard,
}

/// Recovers the bridge a callback was handed.
///
/// Returns `None` when no bridge is installed, which is what makes every trampoline a
/// no-op outside a call this crate made. `nghttp2_session_del` was verified not to invoke
/// callbacks, but the guard costs nothing and removes a class of teardown hazard.
///
/// # Safety
///
/// `user_data` must be either null or a pointer to a live `Bridge<'_, C>` with the same
/// `C` the session was built with. The returned borrow is given an unconstrained
/// lifetime, so it must not be held beyond the callback that produced it — every caller
/// below uses it and drops it within the same statement sequence.
unsafe fn bridge<'a, C>(user_data: *mut c_void) -> Option<&'a mut Bridge<'a, C>> {
    if user_data.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees this points at a live `Bridge<'_, C>` installed by
    // `Session::with_context`, which keeps it alive for the whole FFI call and does not
    // touch it meanwhile, so this is the only live borrow.
    Some(unsafe { &mut *user_data.cast::<Bridge<'a, C>>() })
}

/// Translates a handler's decision into libnghttp2's return convention.
const fn header_action_code(action: HeaderAction) -> i32 {
    match action {
        HeaderAction::Continue => 0,
        // Documented for the header-phase callbacks only: resets the stream rather than
        // failing the connection.
        HeaderAction::CancelStream => sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE,
    }
}

pub(crate) unsafe extern "C" fn on_begin_headers<C>(
    _session: *mut sys::nghttp2_session,
    frame: *const sys::nghttp2_frame,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.begin_headers.as_mut() else {
        return 0;
    };

    // SAFETY: libnghttp2 always passes a valid frame pointer to this callback, and the
    // frame lives at least for the duration of the call.
    let info = FrameInfo::from_header(unsafe { &(*frame).hd });

    // Server push is out of scope. Sessions advertise ENABLE_PUSH = 0, but a peer that
    // ignores that must still not reach a caller's handler.
    if info.kind() == crate::stream::FrameType::PUSH_PROMISE {
        return 0;
    }

    header_action_code(handler(bridge.context, info))
}

pub(crate) unsafe extern "C" fn on_header<C>(
    _session: *mut sys::nghttp2_session,
    frame: *const sys::nghttp2_frame,
    name: *const u8,
    namelen: usize,
    value: *const u8,
    valuelen: usize,
    _flags: u8,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.header.as_mut() else {
        return 0;
    };

    // SAFETY: libnghttp2 always passes a valid frame pointer to this callback.
    let info = FrameInfo::from_header(unsafe { &(*frame).hd });
    if info.kind() == crate::stream::FrameType::PUSH_PROMISE {
        return 0;
    }

    // SAFETY: libnghttp2 guarantees both pointers are non-null and reference `namelen`
    // and `valuelen` readable octets for the duration of this call. The slices borrow
    // that memory and do not escape the handler.
    let name = unsafe { core::slice::from_raw_parts(name, namelen) };
    // SAFETY: as above, for the value.
    let value = unsafe { core::slice::from_raw_parts(value, valuelen) };

    header_action_code(handler(bridge.context, info, name, value))
}

pub(crate) unsafe extern "C" fn on_data_chunk_recv<C>(
    _session: *mut sys::nghttp2_session,
    _flags: u8,
    stream_id: i32,
    data: *const u8,
    len: usize,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.data_chunk.as_mut() else {
        return 0;
    };

    // SAFETY: libnghttp2 guarantees `data` references `len` readable octets for the
    // duration of this call. The slice borrows that memory and does not escape.
    let chunk = unsafe { core::slice::from_raw_parts(data, len) };
    handler(bridge.context, StreamId::new(stream_id), chunk);

    // Anything nonzero here is fatal to the connection, so this callback offers the
    // caller no way to signal one, and always reports success.
    0
}

pub(crate) unsafe extern "C" fn on_frame_recv<C>(
    _session: *mut sys::nghttp2_session,
    frame: *const sys::nghttp2_frame,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.frame_recv.as_mut() else {
        return 0;
    };

    // SAFETY: libnghttp2 always passes a valid frame pointer to this callback.
    let info = FrameInfo::from_header(unsafe { &(*frame).hd });
    if info.kind() == crate::stream::FrameType::PUSH_PROMISE {
        return 0;
    }

    handler(bridge.context, info);
    0
}

pub(crate) unsafe extern "C" fn on_stream_close<C>(
    _session: *mut sys::nghttp2_session,
    stream_id: i32,
    error_code: u32,
    user_data: *mut c_void,
) -> i32 {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        return 0;
    };

    let stream = StreamId::new(stream_id);

    // Body entries and parked errors are released here whether or not the caller
    // registered a handler, so a stream never leaks state it accumulated.

    if let Some(handler) = bridge.handlers.stream_close.as_mut() {
        handler(bridge.context, stream, ErrorCode::new(error_code));
    }
    0
}
