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

use ngnet_h2_sys as sys;

#[cfg(feature = "http")]
use crate::body::SharedOutcome;
use crate::body::{BodyError, BodyOutcome};
use crate::error::ErrorCode;
use crate::handlers::{Handlers, HeaderAction};
#[cfg(feature = "http")]
use crate::state::SendRecord;
use crate::state::{BodyEntry, BodyRegistry, PendingErrors, ResponseGuard, Source};
use crate::stream::{FrameInfo, Goaway, HeaderCategory, StreamId};

/// Everything a trampoline may touch during one FFI call.
///
/// Constructed on the stack for the duration of a single call into libnghttp2 and torn
/// down immediately afterwards.
pub(crate) struct Bridge<'a, C> {
    pub(crate) handlers: &'a mut Handlers<C>,
    pub(crate) context: &'a mut C,
    pub(crate) bodies: &'a mut BodyRegistry,
    pub(crate) pending: &'a mut PendingErrors,
    pub(crate) responded: &'a mut ResponseGuard,
    /// No-copy `DATA` frames the send callback recorded during this call, collected by
    /// the driver once the call returns.
    #[cfg(feature = "http")]
    pub(crate) records: &'a mut Vec<SendRecord>,
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

/// Reads the type-specific detail this crate exposes out of a frame.
///
/// # Safety
///
/// `frame` must point at a live `nghttp2_frame`. Which union member is readable is
/// decided by the frame header's type, which is why every read below is gated on it.
unsafe fn frame_info(frame: *const sys::nghttp2_frame) -> FrameInfo {
    // SAFETY: the caller guarantees `frame` is live, and `hd` is the common prefix of the
    // union, readable whatever the frame type.
    let hd = unsafe { &(*frame).hd };
    let kind = crate::stream::FrameType::new(hd.type_);

    let category = if kind == crate::stream::FrameType::HEADERS {
        // SAFETY: the header says this is a HEADERS frame, so `headers` is the live
        // member.
        HeaderCategory::from_native(unsafe { (*frame).headers.cat })
    } else {
        None
    };

    let goaway = if kind == crate::stream::FrameType::GOAWAY {
        // SAFETY: as above, for the `goaway` member.
        let raw = unsafe { &(*frame).goaway };
        Some(Goaway::new(
            StreamId::new(raw.last_stream_id),
            ErrorCode::new(raw.error_code),
        ))
    } else {
        None
    };

    FrameInfo::with_details(hd, category, goaway)
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
    let info = unsafe { frame_info(frame) };

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
    let info = unsafe { frame_info(frame) };
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
    let info = unsafe { frame_info(frame) };
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

    // Per-stream bookkeeping is released here whether or not the caller registered a
    // handler, so a stream never leaks state it accumulated. This is why the bridge must
    // be installed during send as well as receive: nghttp2 closes streams while it
    // serialises, not only while it parses.
    bridge.responded.release(stream);
    bridge.bodies.detach(stream);
    let body_error = bridge.pending.take(stream);

    if let Some(handler) = bridge.handlers.stream_close.as_mut() {
        handler(
            bridge.context,
            stream,
            ErrorCode::new(error_code),
            body_error,
        );
    }
    0
}

/// Asks a caller's body source for the next chunk of an outgoing message.
///
/// Runs inside `nghttp2_session_mem_send2`, which is the other reason the context bridge
/// must be installed for sends and not only receives.
///
/// This is the dispatcher: a push source writes into the buffer libnghttp2 offers
/// ([`read_push_body`]), while a no-copy shared source hands over octets it already owns
/// ([`read_shared_body`]) and leaves the buffer untouched. The two paths are kept
/// textually separate so the memset — needed only by the push path — is not reachable
/// from the shared one.
pub(crate) unsafe extern "C" fn read_body<C>(
    _session: *mut sys::nghttp2_session,
    stream_id: i32,
    buf: *mut u8,
    length: usize,
    data_flags: *mut u32,
    source: *mut sys::nghttp2_data_source,
    user_data: *mut c_void,
) -> sys::nghttp2_ssize {
    // The entry is reached through the data source alone, never by looking it up in the
    // registry: a lookup would reborrow through the bridge's mutable borrow, which is
    // live for the whole call.
    // SAFETY: libnghttp2 hands back the union this crate populated at submission, whose
    // `ptr` member is the address of a live `BodyEntry` owned by the session's registry.
    let entry = unsafe { &mut *(*source).ptr.cast::<BodyEntry>() };

    // Disjoint field borrows of the one entry: each helper takes the source it dispatches
    // on together with the sibling fields it must update, which the borrow checker accepts
    // because they name different fields.
    match &mut entry.source {
        Source::Push(source) => {
            // SAFETY: `buf` is writable for `length` octets, and `data_flags` points at
            // libnghttp2's flags word; both guarantees flow straight through from this
            // callback's own contract.
            unsafe {
                read_push_body::<C>(
                    source.as_mut(),
                    &mut entry.trailers_ready,
                    stream_id,
                    buf,
                    length,
                    data_flags,
                    user_data,
                )
            }
        }
        #[cfg(feature = "http")]
        Source::Shared(source) => {
            // SAFETY: `data_flags` points at libnghttp2's flags word. The shared path
            // never touches `buf`, so it is not passed one.
            unsafe {
                read_shared_body::<C>(
                    source.as_mut(),
                    &mut entry.trailers_ready,
                    &mut entry.staged,
                    stream_id,
                    length,
                    data_flags,
                    user_data,
                )
            }
        }
    }
}

/// The push path: zero the offered buffer and let the source write into it.
///
/// # Safety
///
/// `buf` must be writable for `length` octets and `data_flags` must point at libnghttp2's
/// flags word for this frame; both hold for the duration of the enclosing callback.
unsafe fn read_push_body<C>(
    source: &mut dyn crate::body::BodySource,
    trailers_ready: &mut bool,
    stream_id: i32,
    buf: *mut u8,
    length: usize,
    data_flags: *mut u32,
    user_data: *mut c_void,
) -> sys::nghttp2_ssize {
    // The buffer libnghttp2 hands over is a reused frame buffer that it does not
    // initialise. Two problems follow, and zeroing solves both.
    //
    // First, forming a `&mut [u8]` over uninitialised memory is undefined behaviour in
    // Rust, whether or not anything reads it.
    //
    // Second, `fill` receives a readable slice, so a body source could observe whatever
    // the previous frame left there — including another stream's body on the same
    // connection. Handing a caller a window onto unrelated plaintext is exactly the class
    // of hazard this crate exists to remove, so the cost of the memset is well spent.
    // SAFETY: libnghttp2 guarantees `buf` is writable for `length` octets.
    unsafe { core::ptr::write_bytes(buf, 0, length) };

    // SAFETY: `buf` is writable for `length` octets and was just fully initialised.
    let out = unsafe { core::slice::from_raw_parts_mut(buf, length) };

    // A source that claims to have written more than it was given would make libnghttp2
    // read past the buffer, so the claim is checked rather than trusted. It is also how
    // an absurd length avoids being cast into something that happens to collide with a
    // negative control code such as NGHTTP2_ERR_DEFERRED.
    let checked = |written: usize| -> Option<sys::nghttp2_ssize> {
        (written <= length).then_some(written as sys::nghttp2_ssize)
    };

    let overrun = |user_data: *mut c_void| -> sys::nghttp2_ssize {
        park_body_error::<C>(
            user_data,
            StreamId::new(stream_id),
            Box::new(BodyOverrun { length }),
        );
        sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE as sys::nghttp2_ssize
    };

    match source.fill(out) {
        BodyOutcome::Wrote(written) => match checked(written) {
            Some(written) => written,
            None => overrun(user_data),
        },
        BodyOutcome::Eof(written) if checked(written).is_none() => overrun(user_data),
        BodyOutcome::EofWithTrailers(written) if checked(written).is_none() => overrun(user_data),
        BodyOutcome::Eof(written) => {
            // SAFETY: libnghttp2 passes a valid pointer to its flags word.
            unsafe { *data_flags |= sys::NGHTTP2_DATA_FLAG_EOF };
            written as sys::nghttp2_ssize
        }
        BodyOutcome::EofWithTrailers(written) => {
            // Ending the body without closing the stream is what keeps a trailing header
            // block legal; without NO_END_STREAM the stream ends here and trailers could
            // never be sent.
            // SAFETY: libnghttp2 passes a valid pointer to its flags word.
            unsafe {
                *data_flags |= sys::NGHTTP2_DATA_FLAG_EOF | sys::NGHTTP2_DATA_FLAG_NO_END_STREAM;
            }
            *trailers_ready = true;
            written as sys::nghttp2_ssize
        }
        BodyOutcome::Fail(error) => {
            park_body_error::<C>(user_data, StreamId::new(stream_id), error);
            // Resets this stream rather than failing the connection.
            sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE as sys::nghttp2_ssize
        }
        // libnghttp2 lifts the DATA item out of the outbound queue and marks the stream
        // user-deferred. Nothing will consult this source again until
        // `nghttp2_session_resume_data` puts the item back.
        BodyOutcome::Defer => sys::NGHTTP2_ERR_DEFERRED as sys::nghttp2_ssize,
    }
}

/// The no-copy path: take a chunk the source already owns, stage it, and tell libnghttp2
/// the frame is no-copy so it serialises only the header.
///
/// Deliberately does **not** touch `buf`: under `NGHTTP2_DATA_FLAG_NO_COPY` libnghttp2
/// never reads the payload region, so there is nothing to initialise and no cross-stream
/// plaintext to hide — the payload never passes through libnghttp2's buffer at all. The
/// staged chunk is handed to the transport later, in [`send_data`].
///
/// # Safety
///
/// `data_flags` must point at libnghttp2's flags word for this frame, which holds for the
/// duration of the enclosing callback.
#[cfg(feature = "http")]
unsafe fn read_shared_body<C>(
    source: &mut dyn crate::body::SharedBodySource,
    trailers_ready: &mut bool,
    staged: &mut Option<bytes::Bytes>,
    stream_id: i32,
    length: usize,
    data_flags: *mut u32,
    user_data: *mut c_void,
) -> sys::nghttp2_ssize {
    let (chunk, eof, trailers) = match source.take(length) {
        SharedOutcome::Wrote(chunk) => (chunk, false, false),
        SharedOutcome::Eof(chunk) => (chunk, true, false),
        SharedOutcome::EofWithTrailers(chunk) => (chunk, true, true),
        // No chunk is staged and no frame is emitted; the stream waits to be resumed.
        SharedOutcome::Defer => return sys::NGHTTP2_ERR_DEFERRED as sys::nghttp2_ssize,
        SharedOutcome::Fail(error) => {
            park_body_error::<C>(user_data, StreamId::new(stream_id), error);
            // Resets this stream rather than failing the connection.
            return sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE as sys::nghttp2_ssize;
        }
    };

    // libnghttp2 is about to be told the frame is exactly `chunk.len()` octets long. A
    // chunk longer than the `length` it offered would make the header claim a length the
    // window never granted, so it is a source failure rather than something to truncate —
    // the same treatment `read_push_body` gives an over-long `fill`.
    if chunk.len() > length {
        park_body_error::<C>(
            user_data,
            StreamId::new(stream_id),
            Box::new(SharedOverrun {
                length,
                produced: chunk.len(),
            }),
        );
        return sys::NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE as sys::nghttp2_ssize;
    }

    let written = chunk.len() as sys::nghttp2_ssize;

    // Overwrite any chunk a previous pack left un-sent. That is the cancellation window:
    // libnghttp2 may pack a no-copy frame and then reset it without a send — if the stream
    // closed in between — leaving a chunk here that no `send_data` will ever collect.
    // Overwriting releases it, and if none follows it is released when the entry is
    // dropped at stream close, so it is released exactly once either way.
    *staged = Some(chunk);

    // NO_COPY makes libnghttp2 serialise the header only and defer the payload to
    // `send_data`; the EOF/trailer flags are set on exactly the same terms as the push
    // path, so end-of-stream and the trailer window behave identically.
    let mut flags = sys::NGHTTP2_DATA_FLAG_NO_COPY;
    if eof {
        flags |= sys::NGHTTP2_DATA_FLAG_EOF;
    }
    if trailers {
        flags |= sys::NGHTTP2_DATA_FLAG_NO_END_STREAM;
        *trailers_ready = true;
    }
    // SAFETY: libnghttp2 passes a valid pointer to its flags word.
    unsafe { *data_flags |= flags };

    written
}

/// Hands a no-copy `DATA` frame's header and payload to the driver's record sink.
///
/// libnghttp2 calls this once per frame that [`read_shared_body`] marked no-copy, in
/// place of writing the payload itself. Matches `nghttp2_send_data_callback`.
///
/// **Record, don't write.** The callback runs synchronously inside
/// `nghttp2_session_mem_send2`; it cannot perform or await I/O and a partial send would be
/// unrecoverable, so it does not write to any transport. It copies the nine-octet header
/// out, takes the staged payload, and deposits a [`SendRecord`] for the driver to write
/// once the send returns.
///
/// **Why `WOULDBLOCK` and `PAUSE` are not used.** Both exist to let a real send callback
/// signal a full or backpressured socket, but this callback never writes, so neither can
/// arise. `WOULDBLOCK` would in any case be unusable here: `Session::send` maps a
/// zero-length `mem_send2` to `Ok(None)`, indistinguishable from "nothing left", so a
/// `WOULDBLOCK` return would look like a finished connection. Backpressure is the driver's
/// job, applied to the records after the fact, not the callback's.
///
/// # Safety
///
/// Called by libnghttp2 with a serialized nine-octet `framehd`, a live `source` whose
/// `ptr` is the address of this stream's [`BodyEntry`], and the session's `user_data`.
#[cfg(feature = "http")]
pub(crate) unsafe extern "C" fn send_data<C>(
    _session: *mut sys::nghttp2_session,
    _frame: *mut sys::nghttp2_frame,
    framehd: *const u8,
    length: usize,
    source: *mut sys::nghttp2_data_source,
    user_data: *mut c_void,
) -> core::ffi::c_int {
    let Some(bridge) = (unsafe { bridge::<C>(user_data) }) else {
        // This callback fires only inside `nghttp2_session_mem_send2`, which
        // `with_context` always wraps with a bridge, so a null one cannot occur here. If
        // it somehow did there would be nowhere to record the frame, and returning 0
        // would tell libnghttp2 the payload was sent when it was silently dropped —
        // corrupting the stream. Fail the connection instead.
        return sys::NGHTTP2_ERR_CALLBACK_FAILURE;
    };

    // Reached through the data source alone, exactly as `read_body` does, so it does not
    // reborrow the registry while the bridge's borrow of it is live. The entry lives in
    // `bridge.bodies`, but as a raw pointer, so touching it here does not alias the
    // separate `bridge.records` field written below.
    // SAFETY: libnghttp2 hands back the union this crate populated at submission, whose
    // `ptr` member is the address of a live `BodyEntry` owned by the session's registry.
    let entry = unsafe { &mut *(*source).ptr.cast::<BodyEntry>() };

    let Some(payload) = entry.staged.take() else {
        // `read_shared_body` stages a chunk for every frame it marks no-copy, and
        // libnghttp2 invokes this callback exactly once per such frame, so a missing chunk
        // means the two callbacks have disagreed — a bug in this crate, not a runtime
        // condition. Fail loudly in tests and fatally in release.
        debug_assert!(
            false,
            "send_data fired with no staged chunk; read_shared_body and send_data disagree"
        );
        return sys::NGHTTP2_ERR_CALLBACK_FAILURE;
    };

    // Validate, never truncate (design decision D4). libnghttp2 reports the payload length
    // it serialised into the header; a staged chunk of any other length means the frame
    // header and the payload disagree, which would put the wrong number of octets on the
    // wire. That is a crate bug, so fail rather than silently send a prefix or a suffix.
    if payload.len() != length {
        debug_assert!(
            false,
            "send_data staged {} octets but libnghttp2 framed {length}",
            payload.len()
        );
        return sys::NGHTTP2_ERR_CALLBACK_FAILURE;
    }

    // Copy the nine header octets out now: `framehd` points into libnghttp2's own frame
    // buffer, which is reset the instant this frame completes — immediately after this
    // callback returns 0 — so the pointer must not be retained past the call.
    let mut header = [0u8; 9];
    // SAFETY: `framehd` points at `NGHTTP2_FRAME_HDLEN` (9) readable octets of serialized
    // header, valid for the duration of this call. No padding callback is installed, so
    // the header is exactly nine octets and the destination array is exactly nine long.
    unsafe { core::ptr::copy_nonoverlapping(framehd, header.as_mut_ptr(), 9) };

    bridge.records.push(SendRecord { header, payload });
    0
}

/// Reported when a body source claims to have written more than it was given.
#[derive(Debug)]
struct BodyOverrun {
    length: usize,
}

impl core::fmt::Display for BodyOverrun {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "body source reported writing more than the {} octets it was given",
            self.length
        )
    }
}

impl core::error::Error for BodyOverrun {}

/// Reported when a no-copy body source hands over more than the frame's limit.
#[cfg(feature = "http")]
#[derive(Debug)]
struct SharedOverrun {
    length: usize,
    produced: usize,
}

#[cfg(feature = "http")]
impl core::fmt::Display for SharedOverrun {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "shared body source handed over {} octets for a frame limited to {}",
            self.produced, self.length
        )
    }
}

#[cfg(feature = "http")]
impl core::error::Error for SharedOverrun {}

/// Parks a body failure so the stream-close handler can report it.
fn park_body_error<C>(user_data: *mut c_void, stream: StreamId, error: BodyError) {
    // SAFETY: `user_data` is whatever `with_context` installed, or null.
    if let Some(bridge) = unsafe { bridge::<C>(user_data) } {
        bridge.pending.park(stream, error);
    }
}
