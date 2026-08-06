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

use crate::handlers::Handlers;
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
