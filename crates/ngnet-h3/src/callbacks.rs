//! The bridge between nghttp3's C callbacks and this crate's Rust handlers.
//!
//! # Why this is not the HTTP/2 crate's design
//!
//! `ngnet-h2` builds a [`Bridge`] on the stack for the duration of each call and installs
//! a pointer to it with `nghttp2_session_set_user_data`, clearing it again on the way out.
//! That gives callbacks access to borrows that only exist during the call, without the
//! session having to own them.
//!
//! **nghttp3 has no such setter.** Connection user data is accepted only by
//! `nghttp3_conn_client_new` / `_server_new`; the five `nghttp3_conn_set_*` functions it
//! exports set concurrency limits, stream user data and stream priority, and nothing else.
//! The pointer given at construction is the only one callbacks will ever receive.
//!
//! So the pointer handed over at construction is not a `Bridge` but a [`BridgeSlot`]: a
//! stable, heap-allocated cell that a `Bridge` pointer is written into for the duration of
//! each call and cleared from afterwards. The indirection costs one load per callback and
//! restores the property the HTTP/2 crate gets from the setter.
//!
//! The slot must be its own heap allocation rather than a field of the connection, because
//! the connection is `Send` and will be moved after it is constructed — a pointer into a
//! field would dangle at the first move, and nothing would report it.
//!
//! # Panics
//!
//! A panic in a callback unwinds into a C frame, which aborts. This matches `ngnet-h2` and
//! is the accepted contract: `catch_unwind` would have to invent a return value for a
//! callback whose contract has no "the handler is broken" case.

use core::cell::Cell;
use core::ffi::c_void;

use ngnet_h3_sys as sys;

use crate::error::ErrorCode;
use crate::handlers::{FieldAction, FieldSection, FieldToken, Handlers, StreamClosed};
use crate::stream::StreamId;

/// The stable indirection nghttp3 is given at construction.
///
/// Interior mutability is required because the slot is written through a shared reference
/// while nghttp3 holds a raw pointer to it.
pub(crate) struct BridgeSlot {
    current: Cell<*mut c_void>,
}

impl BridgeSlot {
    /// Allocates an empty slot.
    pub(crate) fn new() -> Box<Self> {
        Box::new(Self {
            current: Cell::new(core::ptr::null_mut()),
        })
    }

    /// The pointer to hand to a connection constructor.
    pub(crate) fn as_ptr(&self) -> *mut c_void {
        (self as *const Self).cast_mut().cast::<c_void>()
    }
}

// SAFETY: the slot holds a raw pointer that is only ever written and read while the
// connection is mutably borrowed, so no two threads can observe it at once. The
// connection is `Send` and not `Sync`, which is what enforces that.
unsafe impl Send for BridgeSlot {}

/// Everything a callback may reach, for the duration of one call.
///
/// Holds disjoint mutable borrows of the connection's parts plus the caller's own state,
/// which is what lets handlers mutate application state that was never captured.
pub(crate) struct Bridge<'a, C> {
    pub(crate) handlers: &'a mut Handlers<C>,
    pub(crate) context: &'a mut C,
}
/// Installs a bridge into the slot for as long as it is alive.
///
/// Clearing on drop rather than after the call is what makes a panic safe: the slot is
/// emptied while unwinding, so a later callback cannot follow a pointer to a dead frame.
pub(crate) struct BridgeGuard<'a> {
    slot: &'a BridgeSlot,
}

impl<'a> BridgeGuard<'a> {
    /// # Safety
    ///
    /// `bridge` must remain alive and unmoved for the lifetime of the returned guard, and
    /// `C` must match the type every callback will recover it as.
    pub(crate) unsafe fn install<C>(slot: &'a BridgeSlot, bridge: &mut Bridge<'_, C>) -> Self {
        let raw = (bridge as *mut Bridge<'_, C>).cast::<c_void>();
        slot.current.set(raw);
        Self { slot }
    }
}

impl Drop for BridgeGuard<'_> {
    fn drop(&mut self) {
        self.slot.current.set(core::ptr::null_mut());
    }
}

/// Recovers the bridge a callback was handed.
///
/// Returns `None` when no call is in progress, which is not a bug: nghttp3 can invoke a
/// callback from inside its own constructor, before any bridge has been installed.
///
/// # Safety
///
/// `user_data` must be null or the pointer given to a connection constructor by
/// [`BridgeSlot::as_ptr`], and `C` must be the type the installed bridge was built with.
pub(crate) unsafe fn bridge<'a, C>(user_data: *mut c_void) -> Option<&'a mut Bridge<'a, C>> {
    if user_data.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees this is the slot pointer, which lives in a box the
    // connection owns and outlives every callback.
    let slot = unsafe { &*(user_data.cast::<BridgeSlot>()) };
    let current = slot.current.get();
    if current.is_null() {
        return None;
    }
    // SAFETY: the slot holds a pointer to a live `Bridge<C>` installed by `BridgeGuard`,
    // which clears it before the bridge goes out of scope, including while unwinding.
    Some(unsafe { &mut *(current.cast::<Bridge<'a, C>>()) })
}

/// Reports stream data that nghttp3 consumed while a stream was blocked.
///
/// This is flow-control credit arriving late: the bytes were supplied to `read_stream`
/// earlier, but could not be counted then because QPACK had not yet unblocked the stream.
/// A caller that ignores it under-credits the peer and eventually stalls the connection.
pub(crate) unsafe extern "C" fn deferred_consume_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    consumed: usize,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: `conn_user_data` is the slot pointer installed at construction, and `C`
    // matches the connection's own parameter.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.deferred_consume.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        // nghttp3 will not produce an out-of-range identifier; ignoring it is preferable
        // to failing the connection over something the peer cannot have caused.
        return 0;
    };
    handler(bridge.context, stream, consumed as u64);
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_slot_yields_no_bridge() {
        let slot = BridgeSlot::new();
        // SAFETY: the pointer is the slot's own, and nothing is installed.
        let found = unsafe { bridge::<()>(slot.as_ptr()) };
        assert!(found.is_none());
    }

    #[test]
    fn a_null_user_data_yields_no_bridge() {
        // SAFETY: null is explicitly permitted.
        let found = unsafe { bridge::<()>(core::ptr::null_mut()) };
        assert!(found.is_none());
    }

    #[test]
    fn an_installed_bridge_is_recovered_and_then_cleared() {
        let slot = BridgeSlot::new();
        let mut handlers = Handlers::<u32>::default();
        let mut context = 7u32;

        {
            let mut carried = Bridge {
                handlers: &mut handlers,
                context: &mut context,
            };
            // SAFETY: `carried` outlives the guard, and `C` matches on recovery.
            let _guard = unsafe { BridgeGuard::install(&slot, &mut carried) };

            // SAFETY: a bridge is installed and `C` matches.
            let found = unsafe { bridge::<u32>(slot.as_ptr()) }.expect("bridge is installed");
            *found.context += 1;
        }

        assert_eq!(context, 8, "the handler mutated the caller's own state");

        // SAFETY: the guard has been dropped, so the slot is empty again.
        let found = unsafe { bridge::<u32>(slot.as_ptr()) };
        assert!(found.is_none(), "the guard must clear the slot on drop");
    }

    #[test]
    fn the_slot_pointer_survives_moving_the_box() {
        let slot = BridgeSlot::new();
        let before = slot.as_ptr();
        let moved = slot;
        assert_eq!(
            before,
            moved.as_ptr(),
            "the slot must be stable across moves, or a moved Conn would dangle"
        );
    }
}

/// Borrows the bytes an `nghttp3_rcbuf` holds, for the duration of one call.
///
/// Inbound field names and values arrive as reference-counted buffers rather than as a
/// pointer and a length -- a real difference from nghttp2, whose equivalent callback hands
/// over raw slices. The reference count is deliberately *not* incremented: the bytes are
/// valid for the callback's duration, which is all a borrowing handler needs, and taking a
/// reference would turn every delivered field into an allocation the caller must release.
///
/// # Safety
///
/// `buf` must be a buffer nghttp3 supplied to the callback currently running.
unsafe fn rcbuf_bytes<'a>(buf: *mut sys::nghttp3_rcbuf) -> &'a [u8] {
    if buf.is_null() {
        return &[];
    }
    // SAFETY: the caller guarantees this is a live buffer from the running callback.
    let vec = unsafe { sys::nghttp3_rcbuf_get_buf(buf) };
    if vec.base.is_null() || vec.len == 0 {
        return &[];
    }
    // SAFETY: nghttp3 guarantees the buffer is readable for `len` bytes and outlives the
    // callback, and the returned lifetime is confined to it by the caller.
    unsafe { core::slice::from_raw_parts(vec.base, vec.len) }
}

/// Turns a handler's decision into the integer nghttp3 expects.
fn field_action_code(action: FieldAction) -> i32 {
    match action {
        FieldAction::Continue => 0,
        // nghttp3 has no per-stream "reject this section" code the way nghttp2 does, so a
        // handler that wants to stop reads the remaining fields and resets the stream
        // itself. Failing here would take the whole connection down for one bad field.
        FieldAction::Stop => 0,
    }
}

macro_rules! section_boundary_cb {
    ($name:ident, $slot:ident, $kind:expr) => {
        pub(crate) unsafe extern "C" fn $name<C>(
            _conn: *mut sys::nghttp3_conn,
            stream_id: i64,
            conn_user_data: *mut c_void,
            _stream_user_data: *mut c_void,
        ) -> i32 {
            // SAFETY: `conn_user_data` is the slot installed at construction, and `C`
            // matches the connection's own parameter.
            let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
                return 0;
            };
            let Some(handler) = bridge.handlers.$slot.as_mut() else {
                return 0;
            };
            let Ok(stream) = StreamId::new(stream_id) else {
                return 0;
            };
            handler(bridge.context, stream, $kind);
            0
        }
    };
}

section_boundary_cb!(begin_headers_cb, section_begin, FieldSection::Headers);
section_boundary_cb!(begin_trailers_cb, section_begin, FieldSection::Trailers);

/// The end-of-section callbacks carry an extra `fin` flag that the begin ones do not, so
/// they cannot share the macro above.
macro_rules! section_end_cb {
    ($name:ident, $kind:expr) => {
        pub(crate) unsafe extern "C" fn $name<C>(
            _conn: *mut sys::nghttp3_conn,
            stream_id: i64,
            _fin: i32,
            conn_user_data: *mut c_void,
            _stream_user_data: *mut c_void,
        ) -> i32 {
            // SAFETY: `conn_user_data` is the slot installed at construction, and `C`
            // matches the connection's own parameter.
            let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
                return 0;
            };
            let Some(handler) = bridge.handlers.section_end.as_mut() else {
                return 0;
            };
            let Ok(stream) = StreamId::new(stream_id) else {
                return 0;
            };
            handler(bridge.context, stream, $kind);
            0
        }
    };
}

section_end_cb!(end_headers_cb, FieldSection::Headers);
section_end_cb!(end_trailers_cb, FieldSection::Trailers);

macro_rules! field_cb {
    ($name:ident, $kind:expr) => {
        pub(crate) unsafe extern "C" fn $name<C>(
            _conn: *mut sys::nghttp3_conn,
            stream_id: i64,
            token: i32,
            name: *mut sys::nghttp3_rcbuf,
            value: *mut sys::nghttp3_rcbuf,
            _flags: u8,
            conn_user_data: *mut c_void,
            _stream_user_data: *mut c_void,
        ) -> i32 {
            // SAFETY: as above.
            let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
                return 0;
            };
            let Some(handler) = bridge.handlers.field.as_mut() else {
                return 0;
            };
            let Ok(stream) = StreamId::new(stream_id) else {
                return 0;
            };
            // SAFETY: both buffers belong to the callback currently running, and the
            // borrows end when the handler returns.
            let (name, value) = unsafe { (rcbuf_bytes(name), rcbuf_bytes(value)) };
            field_action_code(handler(
                bridge.context,
                stream,
                $kind,
                FieldToken::from_raw(token),
                name,
                value,
            ))
        }
    };
}

field_cb!(recv_header_cb, FieldSection::Headers);
field_cb!(recv_trailer_cb, FieldSection::Trailers);

/// Delivers a chunk of body bytes.
///
/// Unlike field names and values, these arrive as a plain pointer and length, so the two
/// cannot share a conversion.
pub(crate) unsafe extern "C" fn recv_data_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    data: *const u8,
    datalen: usize,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.data.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    let chunk = if data.is_null() || datalen == 0 {
        &[][..]
    } else {
        // SAFETY: nghttp3 guarantees `data` is readable for `datalen` bytes for the
        // duration of this call, and the borrow ends when the handler returns.
        unsafe { core::slice::from_raw_parts(data, datalen) }
    };
    handler(bridge.context, stream, chunk);
    0
}

/// Reports that the peer has finished sending on a stream.
pub(crate) unsafe extern "C" fn end_stream_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.end_stream.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    handler(bridge.context, stream);
    0
}

/// Reports that a stream has closed, with the application error code it closed with.
pub(crate) unsafe extern "C" fn stream_close_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    flags: u32,
    stream_id: i64,
    rx_app_error_code: u64,
    tx_app_error_code: u64,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.stream_close.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    handler(
        bridge.context,
        stream,
        StreamClosed {
            receiving: (flags & sys::NGHTTP3_STREAM_CLOSE_FLAG_RX_APP_ERROR_CODE_SET != 0)
                .then(|| ErrorCode::new(rx_app_error_code)),
            sending: (flags & sys::NGHTTP3_STREAM_CLOSE_FLAG_TX_APP_ERROR_CODE_SET != 0)
                .then(|| ErrorCode::new(tx_app_error_code)),
        },
    );
    0
}
