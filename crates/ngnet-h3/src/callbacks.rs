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

use crate::conn::Role;
use crate::error::ErrorCode;
use crate::handlers::{
    FieldAction, FieldSection, FieldToken, Handlers, PeerSettings, Shutdown, StreamClosed,
};
use crate::state::{BodyEnd, BodyRegistry, Deliveries, Handover};
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
    pub(crate) bodies: &'a mut BodyRegistry,
    /// Credit and field sections that a missing or opinionated handler would otherwise lose.
    pub(crate) deliveries: &'a mut Deliveries,
    pub(crate) context: &'a mut C,
    /// Which side this endpoint is.
    ///
    /// Carried because one callback -- graceful shutdown -- hands over an identifier whose
    /// meaning depends on the role, and nghttp3 does not tell the callback which it is.
    pub(crate) role: Role,
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
    let Ok(stream) = StreamId::new(stream_id) else {
        // nghttp3 will not produce an out-of-range identifier; ignoring it is preferable
        // to failing the connection over something the peer cannot have caused.
        return 0;
    };
    match bridge.handlers.deferred_consume.as_mut() {
        Some(handler) => handler(bridge.context, stream, consumed as u64),
        // Held rather than dropped. This credit is reported once and never again, so
        // discarding it under-credits the peer permanently and stalls a long-lived
        // connection by degrees -- a failure with no symptom until it is the only symptom.
        // `Conn::take_deferred_credit` is how a caller without a handler collects it.
        None => bridge.deliveries.hold_credit(stream, consumed as u64),
    }
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
            let mut bodies = crate::state::BodyRegistry::default();
            let mut deliveries = crate::state::Deliveries::default();
            let mut carried = Bridge {
                handlers: &mut handlers,
                bodies: &mut bodies,
                deliveries: &mut deliveries,
                context: &mut context,
                role: Role::Client,
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

/// Bit for the leading field section, in the silence mask.
const SECTION_HEADERS: u8 = 1;
/// Bit for the trailing field section.
const SECTION_TRAILERS: u8 = 2;

fn section_bit(section: FieldSection) -> u8 {
    match section {
        FieldSection::Headers => SECTION_HEADERS,
        FieldSection::Trailers => SECTION_TRAILERS,
    }
}

/// Turns a handler's decision into the integer nghttp3 expects.
/// Both decisions return zero to nghttp3, which has no per-section reject code the way
/// nghttp2 does. The difference between them is honoured here instead: `Stop` silences the
/// rest of that section, so the handler is not called again for fields it has said it does
/// not want. Failing the call instead would take the whole connection down over one field.
fn field_action_code(_action: FieldAction) -> i32 {
    0
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
            let Ok(stream) = StreamId::new(stream_id) else {
                return 0;
            };
            // Cleared before the handler and regardless of whether there is one: the
            // silence covers one section, and the next section starts fresh.
            bridge.deliveries.unsilence(stream, section_bit($kind));
            let Some(handler) = bridge.handlers.section_end.as_mut() else {
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
            let Ok(stream) = StreamId::new(stream_id) else {
                return 0;
            };
            // A handler that already said `Stop` for this section is not asked again.
            if bridge.deliveries.is_silenced(stream, section_bit($kind)) {
                return 0;
            }
            let Some(handler) = bridge.handlers.field.as_mut() else {
                return 0;
            };
            // SAFETY: both buffers belong to the callback currently running, and the
            // borrows end when the handler returns.
            let (name, value) = unsafe { (rcbuf_bytes(name), rcbuf_bytes(value)) };
            let action = handler(
                bridge.context,
                stream,
                $kind,
                FieldToken::from_raw(token),
                name,
                value,
            );
            if action == FieldAction::Stop {
                bridge.deliveries.silence(stream, section_bit($kind));
            }
            field_action_code(action)
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
///
/// This is the single detach point on the close path: the stream's body and send offsets
/// are dropped here, releasing any buffers still held for it. nghttp3 deletes the stream —
/// and with it the queue of pointers into those buffers — immediately after this returns,
/// so releasing here cannot leave it pointing at freed memory.
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
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    // Before the handler, so that a handler which panics cannot skip the release, and so
    // that a handler observing the connection sees a stream that is already gone.
    bridge.bodies.forget(stream);
    bridge.deliveries.forget(stream);

    let Some(handler) = bridge.handlers.stream_close.as_mut() else {
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

/// Asks a body source for the next vectors of an outgoing message body.
///
/// The rules encoded here are read from `lib/nghttp3_stream.c:602-700`, and none of them
/// are stated by the header:
///
/// * nghttp3 offers a fixed array of eight vectors. Anything a source produces beyond what
///   fits is held back and offered on the next call rather than dropped.
/// * A zero-length vector is skipped without being queued, so one must never be handed
///   over — an element retained for it would wait forever for an acknowledgement.
/// * `assert(datalen || EOF)` is only an assertion, so it aborts where it is compiled in
///   and lets a zero-length DATA frame be written where it is not. Neither is acceptable,
///   so "the source has nothing right now" becomes `NGHTTP3_ERR_WOULDBLOCK`, never a zero
///   return.
pub(crate) unsafe extern "C" fn read_data_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    vec: *mut sys::nghttp3_vec,
    veccnt: usize,
    pflags: *mut u32,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> sys::nghttp3_ssize {
    // A data reader is only ever installed alongside a registry entry, and this callback
    // only fires from the write path, which always installs a bridge. Anything else means
    // this crate's own invariants are broken, so failing the connection is right.
    // SAFETY: `conn_user_data` is the slot installed at construction, and `C` matches.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return sys::NGHTTP3_ERR_CALLBACK_FAILURE as sys::nghttp3_ssize;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return sys::NGHTTP3_ERR_CALLBACK_FAILURE as sys::nghttp3_ssize;
    };
    let Some(entry) = bridge.bodies.entry_mut(stream) else {
        return sys::NGHTTP3_ERR_CALLBACK_FAILURE as sys::nghttp3_ssize;
    };

    match entry.begin_round() {
        Handover::Defer => return sys::NGHTTP3_ERR_WOULDBLOCK as sys::nghttp3_ssize,
        // nghttp3 has exactly one failure code for this callback and it is
        // connection-fatal; the write path will poison the connection and drain the
        // registry, which is what releases the buffers already handed over.
        Handover::Fail => return sys::NGHTTP3_ERR_CALLBACK_FAILURE as sys::nghttp3_ssize,
        Handover::Ready => {}
    }

    let mut filled = 0usize;
    while filled < veccnt {
        let Some(piece) = entry.take_piece() else {
            break;
        };
        // Read once. Everything below uses these two values, and the retain queue stores
        // the length rather than measuring the buffer again later: a `RetainedBytes` built
        // from a caller-supplied owner reads through `AsRef`, which promises nothing about
        // answering the same way twice, and release accounting that disagreed with what
        // nghttp3 was told would free a buffer it is still reading through.
        let bytes = piece.as_slice();
        let base = bytes.as_ptr();
        let handed = bytes.len();

        // nghttp3 silently skips a zero-length vector without queueing it, so retaining one
        // would put an element at the front of the queue awaiting an acknowledgement that
        // can never arrive -- and every buffer behind it would wait with it. Filtered when
        // the source hands pieces over, and checked again here because the length is read
        // from the owner rather than from anything this crate controls.
        if handed == 0 {
            continue;
        }

        // SAFETY: `vec` is an array of at least `veccnt` entries supplied by nghttp3 for
        // this call, and `filled` is below `veccnt`.
        unsafe {
            (*vec.add(filled)).base = base.cast_mut();
            (*vec.add(filled)).len = handed;
        }
        // Retained *after* its address has been handed over, and before returning, so the
        // allocation behind that address outlives the write. `RetainedBytes` is an `Arc`,
        // so moving the handle into the queue does not move the bytes.
        entry.retain(piece, handed);
        filled += 1;
    }

    let end = entry.end_reached();
    if filled == 0 && end.is_none() {
        // The source produced nothing and did not end. Returning zero here would write a
        // zero-length DATA frame where the assertion is absent; deferring is what was meant.
        return sys::NGHTTP3_ERR_WOULDBLOCK as sys::nghttp3_ssize;
    }

    let flags = match end {
        None => sys::NGHTTP3_DATA_FLAG_NONE,
        Some(BodyEnd::Stream) => sys::NGHTTP3_DATA_FLAG_EOF,
        // The body ends but the stream must stay open, or the trailing field section
        // would have nowhere to go.
        Some(BodyEnd::Trailers) => {
            sys::NGHTTP3_DATA_FLAG_EOF | sys::NGHTTP3_DATA_FLAG_NO_END_STREAM
        }
    };
    // SAFETY: nghttp3 always supplies a valid pointer for the flags out-parameter.
    unsafe { *pflags = flags };

    filled as sys::nghttp3_ssize
}

/// Reports that the peer has acknowledged more of a stream's application-owned bytes.
///
/// `datalen` is a delta rather than a cumulative offset, and covers only the buffers this
/// crate supplied — nghttp3 skips its own serialisation buffers when reporting. That is
/// what lets the retain queue be drained by simple subtraction.
pub(crate) unsafe extern "C" fn acked_stream_data_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    datalen: u64,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    if let Some(entry) = bridge.bodies.entry_mut(stream) {
        entry.on_acked(datalen);
    }
    0
}

/// Asks the QUIC layer to send `STOP_SENDING` on a stream.
///
/// This is nghttp3 saying it will not read any more of what the peer is sending; the QUIC
/// layer has to be told, because this crate owns no transport. A caller that ignores it
/// leaves the peer sending bytes into a stream nothing will ever read.
pub(crate) unsafe extern "C" fn stop_sending_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    app_error_code: u64,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.stop_sending.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    handler(bridge.context, stream, ErrorCode::new(app_error_code));
    0
}

/// Asks the QUIC layer to reset a stream.
///
/// The counterpart of [`stop_sending_cb`] for the sending direction.
pub(crate) unsafe extern "C" fn reset_stream_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    stream_id: i64,
    app_error_code: u64,
    conn_user_data: *mut c_void,
    _stream_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.reset_stream.as_mut() else {
        return 0;
    };
    let Ok(stream) = StreamId::new(stream_id) else {
        return 0;
    };
    handler(bridge.context, stream, ErrorCode::new(app_error_code));
    0
}

/// Reports that the peer has begun a graceful shutdown.
///
/// The identifier's meaning depends on which side received it, and nghttp3 passes it
/// through raw, so the role recorded on the bridge is what disambiguates it here.
pub(crate) unsafe extern "C" fn shutdown_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    id: i64,
    conn_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let role = bridge.role;
    let Some(handler) = bridge.handlers.shutdown.as_mut() else {
        return 0;
    };
    handler(bridge.context, classify_shutdown(role, id));
    0
}

/// Turns the raw shutdown identifier into something that says what it means.
fn classify_shutdown(role: Role, id: i64) -> Shutdown {
    let raw = id as u64;
    match role {
        Role::Client if raw == sys::NGHTTP3_SHUTDOWN_NOTICE_STREAM_ID => Shutdown::Notice,
        Role::Server if raw == sys::NGHTTP3_SHUTDOWN_NOTICE_PUSH_ID => Shutdown::Notice,
        // A client is told the first stream identifier that will not be processed.
        Role::Client => match StreamId::new(id) {
            Ok(stream) => Shutdown::NoStreamsFrom(stream),
            // Out of range is not something nghttp3 produces; reporting the raw value is
            // better than dropping the event and leaving the caller thinking nothing
            // happened.
            Err(_) => Shutdown::NoPushesFrom(raw),
        },
        // A server is told a push identifier, which nghttp3 never generates because it
        // does not implement server push.
        Role::Server => Shutdown::NoPushesFrom(raw),
    }
}

/// Delivers the peer's settings, copied out of the struct that carried them.
pub(crate) unsafe extern "C" fn recv_settings_cb<C>(
    _conn: *mut sys::nghttp3_conn,
    settings: *const sys::nghttp3_proto_settings,
    conn_user_data: *mut c_void,
) -> i32 {
    // SAFETY: as above.
    let Some(bridge) = (unsafe { bridge::<C>(conn_user_data) }) else {
        return 0;
    };
    let Some(handler) = bridge.handlers.peer_settings.as_mut() else {
        return 0;
    };
    if settings.is_null() {
        return 0;
    }
    // SAFETY: nghttp3 supplies a fully initialised struct that outlives this call, and
    // every field is copied out before the handler runs.
    let raw = unsafe { &*settings };
    handler(
        bridge.context,
        PeerSettings {
            max_field_section_size: raw.max_field_section_size,
            qpack_max_dtable_capacity: raw.qpack_max_dtable_capacity as u64,
            qpack_blocked_streams: raw.qpack_blocked_streams as u64,
            enable_connect_protocol: raw.enable_connect_protocol != 0,
            h3_datagram: raw.h3_datagram != 0,
        },
    );
    0
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[test]
    fn a_client_reads_the_identifier_as_a_stream_cut_off() {
        let notice = sys::NGHTTP3_SHUTDOWN_NOTICE_STREAM_ID as i64;
        assert_eq!(classify_shutdown(Role::Client, notice), Shutdown::Notice);
        assert_eq!(
            classify_shutdown(Role::Client, 12),
            Shutdown::NoStreamsFrom(StreamId::new(12).unwrap())
        );
    }

    #[test]
    fn a_server_reads_the_identifier_as_a_push_cut_off() {
        let notice = sys::NGHTTP3_SHUTDOWN_NOTICE_PUSH_ID as i64;
        assert_eq!(classify_shutdown(Role::Server, notice), Shutdown::Notice);
        // The two notice constants differ, so a server must not mistake the client's for
        // its own -- which is exactly what a role-blind implementation would do.
        assert_ne!(
            sys::NGHTTP3_SHUTDOWN_NOTICE_STREAM_ID,
            sys::NGHTTP3_SHUTDOWN_NOTICE_PUSH_ID
        );
        assert_eq!(
            classify_shutdown(Role::Server, sys::NGHTTP3_SHUTDOWN_NOTICE_STREAM_ID as i64),
            Shutdown::NoPushesFrom(sys::NGHTTP3_SHUTDOWN_NOTICE_STREAM_ID)
        );
    }
}
