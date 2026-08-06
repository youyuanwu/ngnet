//! HTTP/2 sessions: construction, teardown, and the outbound half of the sans-I/O loop.

use core::fmt;
use core::marker::PhantomData;
use std::sync::Arc;

use ngnet_h2_sys as sys;

use crate::alloc_state::{AllocState, mem_for};
#[cfg(feature = "http")]
use crate::body::SharedBodySource;
use crate::body::{BodyError, BodySource};
use crate::callbacks::{self, Bridge};
use crate::error::{Error, ErrorCode, ErrorKind, Result};
use crate::handlers::{Handlers, HeaderAction};
use crate::header::{self, Header};
use crate::options::Options;
use crate::settings::Setting;
#[cfg(feature = "http")]
use crate::state::SendRecord;
use crate::state::{BodyEntry, BodyRegistry, FrameProgress, PendingErrors, ResponseGuard};
use crate::stream::{FrameInfo, StreamId};

/// Which side of the connection a session drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Client,
    Server,
}

/// Builds a [`Session`].
///
/// The type parameter `C` is the caller's own application context: the type that will be
/// handed by mutable reference to [`Session::send`] and, once handlers exist, to every
/// handler invoked during a call. It is fixed when the session is built.
#[derive(Debug)]
pub struct SessionBuilder<C> {
    role: Role,
    settings: Vec<Setting>,
    manual_flow_control: bool,
    handlers: Handlers<C>,
    _context: PhantomData<fn(&mut C)>,
}

impl<C> SessionBuilder<C> {
    fn with_role(role: Role) -> Self {
        Self {
            role,
            settings: Vec::new(),
            manual_flow_control: false,
            handlers: Handlers::default(),
            _context: PhantomData,
        }
    }

    /// Starts building a session that drives the client side of a connection.
    pub fn client() -> Self {
        Self::with_role(Role::Client)
    }

    /// Starts building a session that drives the server side of a connection.
    pub fn server() -> Self {
        Self::with_role(Role::Server)
    }

    /// Advertises a setting to the peer.
    ///
    /// Settings are sent in the `SETTINGS` frame the session emits as soon as it is
    /// built. Calling this twice for the same identifier advertises both entries, in
    /// order, exactly as given.
    pub fn setting(mut self, setting: Setting) -> Self {
        self.settings.push(setting);
        self
    }

    /// Takes over flow-control replenishment.
    ///
    /// By default libnghttp2 replenishes windows automatically and
    /// [`Session::consume`] is unavailable. Opting in here makes `consume` the only way
    /// windows are replenished, so a caller that then never reports consumption will
    /// stall the connection.
    ///
    /// [`Session::consume`]: Session
    pub fn manual_flow_control(mut self, enabled: bool) -> Self {
        self.manual_flow_control = enabled;
        self
    }

    /// Called when a header block begins, before any of its headers are reported.
    ///
    /// Returning [`HeaderAction::CancelStream`] resets the stream.
    pub fn on_begin_headers(
        mut self,
        handler: impl FnMut(&mut C, FrameInfo) -> HeaderAction + Send + 'static,
    ) -> Self {
        self.handlers.begin_headers = Some(Box::new(handler));
        self
    }

    /// Called once per received header, with the name and value borrowed in place.
    ///
    /// Returning [`HeaderAction::CancelStream`] resets the stream.
    pub fn on_header(
        mut self,
        handler: impl FnMut(&mut C, FrameInfo, &[u8], &[u8]) -> HeaderAction + Send + 'static,
    ) -> Self {
        self.handlers.header = Some(Box::new(handler));
        self
    }

    /// Called for each chunk of a message body, borrowed in place.
    ///
    /// A chunk carrying the end-of-stream flag is not necessarily the last callback for
    /// that stream; wait for the stream-close handler to know it is finished.
    pub fn on_data_chunk(
        mut self,
        handler: impl FnMut(&mut C, StreamId, &[u8]) + Send + 'static,
    ) -> Self {
        self.handlers.data_chunk = Some(Box::new(handler));
        self
    }

    /// Called once a complete frame has been received.
    ///
    /// Header blocks are reported through the header handlers rather than here, and
    /// `CONTINUATION` frames are never reported at all.
    pub fn on_frame(mut self, handler: impl FnMut(&mut C, FrameInfo) + Send + 'static) -> Self {
        self.handlers.frame_recv = Some(Box::new(handler));
        self
    }

    /// Called when a stream closes, for any reason.
    ///
    /// The final argument carries the error a body source reported, if the stream ended
    /// because the caller's own body production failed.
    pub fn on_stream_close(
        mut self,
        handler: impl FnMut(&mut C, StreamId, ErrorCode, Option<BodyError>) + Send + 'static,
    ) -> Self {
        self.handlers.stream_close = Some(Box::new(handler));
        self
    }

    /// Builds the session and queues its initial `SETTINGS` frame.
    pub fn build(self) -> Result<Session<C>> {
        // Resolved before anything is allocated, so a rejected configuration cannot leave
        // a half-built session behind.
        let settings = self.resolve_settings()?;

        let allocation = Arc::new(AllocState::default());
        let mut mem = mem_for(&allocation);

        let mut options = Options::new()?;
        options.set_no_auto_window_update(self.manual_flow_control);

        let mut callbacks = Callbacks::new()?;
        callbacks.install::<C>();

        let mut raw: *mut sys::nghttp2_session = core::ptr::null_mut();
        let constructor = match self.role {
            Role::Client => sys::nghttp2_session_client_new3,
            Role::Server => sys::nghttp2_session_server_new3,
        };

        // SAFETY: `raw` is a valid out-parameter; `callbacks` and `options` are live for
        // this call and libnghttp2 copies what it needs from both rather than retaining
        // the pointers. `mem` is copied too, but the state it points at lives behind
        // `allocation`, which the session below takes ownership of — so the pointer stays
        // valid for as long as the session, and at a stable address because it is inside
        // an `Arc` allocation rather than inline in the session.
        let rc = unsafe {
            constructor(
                &mut raw,
                callbacks.as_ptr(),
                core::ptr::null_mut(),
                options.as_ptr(),
                &mut mem,
            )
        };
        if rc != 0 {
            return Err(Error::from_native("nghttp2_session_new3", rc));
        }
        debug_assert!(!raw.is_null());

        let mut session = Session {
            raw,
            allocation,
            handlers: self.handlers,
            manual_flow_control: self.manual_flow_control,
            bodies: BodyRegistry::default(),
            pending: PendingErrors::default(),
            responded: ResponseGuard::default(),
            #[cfg(feature = "http")]
            records: Vec::new(),
            frames: FrameProgress::new(self.role == Role::Server),
            _context: PhantomData,
        };

        session.submit_settings(&settings)?;
        Ok(session)
    }

    /// Resolves the settings actually advertised, applying this crate's push policy.
    ///
    /// Server push is out of scope, so a client advertises `ENABLE_PUSH = 0` unless the
    /// caller states a preference explicitly. A server may not advertise `ENABLE_PUSH = 1`
    /// at all — RFC 9113 forbids it and libnghttp2 only range-checks the value, so the
    /// rejection has to happen here.
    fn resolve_settings(&self) -> Result<Vec<Setting>> {
        // Every occurrence must be examined, not just the first. Duplicate identifiers are
        // legal on the wire and libnghttp2 applies the last one, so checking only the
        // first would let `EnablePush(false)` followed by `EnablePush(true)` through.
        if self.role == Role::Server
            && self
                .settings
                .iter()
                .any(|setting| matches!(setting, Setting::EnablePush(true)))
        {
            return Err(Error::new(
                "SessionBuilder::build",
                ErrorKind::InvalidInput,
                "a server may not advertise SETTINGS_ENABLE_PUSH = 1",
            ));
        }

        let states_push = self
            .settings
            .iter()
            .any(|setting| matches!(setting, Setting::EnablePush(_)));

        if self.role == Role::Client && !states_push {
            let mut settings = Vec::with_capacity(self.settings.len() + 1);
            settings.push(Setting::EnablePush(false));
            settings.extend_from_slice(&self.settings);
            return Ok(settings);
        }

        Ok(self.settings.clone())
    }
}

/// One HTTP/2 connection, in a fixed role.
///
/// A session owns the native parser state and every stream on the connection. Dropping
/// it releases all of that; no explicit teardown call is required.
///
/// The session performs no I/O. Call [`Session::send`] to collect bytes that must be
/// written to the peer, and hand bytes read from the peer to the receive half.
pub struct Session<C> {
    raw: *mut sys::nghttp2_session,
    // Order matters only for clarity: `Drop` below releases `raw` before this field is
    // dropped, so the allocator outlives every native free it must account for.
    allocation: Arc<AllocState>,
    // Reached from trampolines as disjoint mutable borrows, never through `self`.
    handlers: Handlers<C>,
    manual_flow_control: bool,
    bodies: BodyRegistry,
    pending: PendingErrors,
    responded: ResponseGuard,
    /// No-copy `DATA` frames recorded by the send callback during a `send_into` call, and
    /// moved out into the caller's sink before that call returns. Never holds anything
    /// across a call: it is filled by the trampoline and drained by `send_into` within the
    /// one `mem_send2`.
    #[cfg(feature = "http")]
    records: Vec<SendRecord>,
    frames: FrameProgress,
    _context: PhantomData<fn(&mut C)>,
}

// SAFETY: a session owns its native state exclusively and libnghttp2 performs no
// internal locking, so it is safe to move one between threads. It is deliberately NOT
// `Sync`: two threads must never touch one session concurrently, and the absence of a
// `Sync` impl is what enforces that. Everything the session owns is `Send`; once
// handlers and body sources are stored here they carry `+ Send` bounds for this reason.
unsafe impl<C> Send for Session<C> {}

impl<C> Session<C> {
    fn submit_settings(&mut self, settings: &[Setting]) -> Result<()> {
        let entries: Vec<sys::nghttp2_settings_entry> =
            settings.iter().copied().map(Setting::entry).collect();

        // SAFETY: `self.raw` is live. `entries` is a valid slice for the given length and
        // libnghttp2 copies every entry, so it need not outlive this call. When the list
        // is empty `as_ptr` yields a dangling-but-aligned pointer, which is sound because
        // libnghttp2 does not dereference it when the count is zero.
        let rc = unsafe {
            sys::nghttp2_submit_settings(
                self.raw,
                sys::NGHTTP2_FLAG_NONE as u8,
                entries.as_ptr(),
                entries.len(),
            )
        };
        if rc != 0 {
            return Err(Error::from_native("nghttp2_submit_settings", rc));
        }
        Ok(())
    }

    /// Collects the next block of bytes the session wants to transmit.
    ///
    /// Returns `Ok(None)` when nothing is pending. Call repeatedly until it returns
    /// `None` to drain everything the session currently has to say.
    ///
    /// The returned slice borrows the session, because libnghttp2 invalidates it on the
    /// next send. The borrow checker therefore prevents using the session again while a
    /// block is still held — write the block out first, then ask for the next.
    ///
    /// `context` is the caller's application state. Handlers can fire during this call,
    /// not only while receiving: libnghttp2 reports stream closure and asks body sources
    /// for payload while it serialises.
    pub fn send(&mut self, context: &mut C) -> Result<Option<&[u8]>> {
        let (data, len) = self.send_raw(context)?;
        if len == 0 {
            return Ok(None);
        }
        debug_assert!(!data.is_null());

        // SAFETY: libnghttp2 returned a non-null pointer to `len` initialised bytes,
        // valid until the next send on this session. The slice borrows `self`, so the
        // borrow checker prevents reaching that next call while it is still held.
        let bytes = unsafe { core::slice::from_raw_parts(data, len as usize) };
        Ok(Some(bytes))
    }

    /// Collects the next block, and any no-copy `DATA` records the same call produced,
    /// into a caller-owned sink.
    ///
    /// The no-copy counterpart of [`Session::send`], and the reason it exists: the block
    /// [`send`](Self::send) returns borrows `&mut self` for as long as it is held, so a
    /// caller cannot ask the session for the records the same call deposited afterwards.
    /// This moves the records out *before* the block is built, leaving only one borrow of
    /// `self` live, so the caller can read the sink while holding the block.
    ///
    /// **Records precede the block.** Whatever the send callback recorded during this call
    /// belongs on the wire *before* whatever block the call returns, because libnghttp2
    /// invokes the callback while serialising and only then returns the trailing block.
    /// The caller must write the sink's contents, in order, ahead of the returned block.
    ///
    /// **A `None` return may still have produced records.** libnghttp2 loops internally,
    /// and the no-copy branch consumes an item and calls the callback without contributing
    /// any octets to the returned block; the final call — the one that returns `None` —
    /// can therefore still have filled the sink. The sink must be drained after *every*
    /// call, the last one included, not only after those that return a block.
    ///
    /// The sink is appended to, never cleared, so a caller may accumulate across calls; it
    /// is the caller's to drain. Records are handed over even when the call fails, so a
    /// caller that abandons the connection on error still owns every payload handle the
    /// session took.
    #[cfg(feature = "http")]
    pub(crate) fn send_into<'a>(
        &'a mut self,
        context: &mut C,
        sink: &mut Vec<SendRecord>,
    ) -> Result<Option<&'a [u8]>> {
        // Drain unconditionally, *before* propagating any error. A failing
        // `mem_send2` may still have run the send callback for frames it serialised
        // before the failure, and those records hold the caller's payload handles. The
        // contract is that records a call deposits always reach the caller's sink, so
        // there is no path on which they are stranded in the session.
        let outcome = self.send_raw(context);

        // Move the records out before building the block. This is the whole point: once
        // the block below borrows `self`, `self.records` is unreachable, so the hand-off
        // has to happen first. `append` leaves `self.records` empty and keeps its
        // capacity for the next call.
        sink.append(&mut self.records);

        let (data, len) = outcome?;

        if len == 0 {
            return Ok(None);
        }
        debug_assert!(!data.is_null());

        // SAFETY: as in `send` — libnghttp2 returned a non-null pointer to `len`
        // initialised bytes valid until the next send, and the slice borrows `self`.
        let bytes = unsafe { core::slice::from_raw_parts(data, len as usize) };
        Ok(Some(bytes))
    }

    /// Runs one `nghttp2_session_mem_send2` and returns the raw block pointer and length,
    /// retaining no borrow of `self`.
    ///
    /// Shared by [`send`](Self::send) and [`send_into`](Self::send_into): each turns the
    /// raw pointer into a `&[u8]` borrowing `self` at its own call site, which is what lets
    /// `send_into` move the records out in between. A negative length is the only error;
    /// a zero length is reported faithfully so the callers can map it to `Ok(None)`.
    fn send_raw(&mut self, context: &mut C) -> Result<(*const u8, sys::nghttp2_ssize)> {
        let mut data: *const u8 = core::ptr::null();

        let len = self.with_context(context, |raw| {
            // SAFETY: `raw` is live and `data` is a valid out-parameter. Handlers may run
            // inside this call, which is why it goes through the bridge.
            unsafe { sys::nghttp2_session_mem_send2(raw, &mut data) }
        });

        if len < 0 {
            return Err(Error::from_native("nghttp2_session_mem_send2", len as i32));
        }
        Ok((data, len))
    }

    /// Submits bytes received from the peer, returning how many were consumed.
    ///
    /// Handlers registered on this session are invoked during the call, each receiving
    /// `context` by mutable reference.
    ///
    /// # Errors
    ///
    /// Only connection-fatal conditions produce an error: memory exhaustion, an invalid
    /// client preface on a server session, peer flooding, an excessive run of
    /// `CONTINUATION` frames, or an internal callback failure.
    ///
    /// Ordinary protocol violations are **not** errors. libnghttp2 handles them by
    /// queueing a `GOAWAY` or `RST_STREAM` for the peer and reporting the input as
    /// processed, so they surface through [`Session::send`] and the stream-close handler
    /// instead. A caller that treated a successful return as "the peer behaved" would be
    /// mistaken; check [`Session::want_read`] and the events it observed.
    pub fn recv(&mut self, input: &[u8], context: &mut C) -> Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }

        let consumed = self.with_context(context, |raw| {
            // SAFETY: `raw` is live and `input` is valid for `input.len()` octets for the
            // duration of the call. libnghttp2 does not retain the pointer afterwards.
            unsafe { sys::nghttp2_session_mem_recv2(raw, input.as_ptr(), input.len()) }
        });

        if consumed < 0 {
            return Err(Error::from_native(
                "nghttp2_session_mem_recv2",
                consumed as i32,
            ));
        }

        // Frame boundaries are counted over exactly the octets libnghttp2 accepted, which
        // is what keeps `mid_frame` in step with the session even when it stops short of
        // the whole buffer.
        let consumed = consumed as usize;
        self.frames.advance(&input[..consumed]);
        Ok(consumed)
    }

    /// Runs one call into libnghttp2 with the caller's context reachable from callbacks.
    ///
    /// The raw session pointer is copied out *before* any field is borrowed: borrowing a
    /// field and then calling an `&mut self` method would not compile, and letting a
    /// trampoline reach the whole `self` while `&mut self` is live would alias. What the
    /// bridge holds instead is disjoint mutable borrows of individual fields, which the
    /// borrow checker accepts and which never overlaps the session libnghttp2 is
    /// executing inside.
    fn with_context<R>(
        &mut self,
        context: &mut C,
        call: impl FnOnce(*mut sys::nghttp2_session) -> R,
    ) -> R {
        let raw = self.raw;

        let mut bridge = Bridge {
            handlers: &mut self.handlers,
            context,
            bodies: &mut self.bodies,
            pending: &mut self.pending,
            responded: &mut self.responded,
            #[cfg(feature = "http")]
            records: &mut self.records,
        };

        // Restores the session's user data however this scope is left, so a panic
        // escaping the call cannot leave libnghttp2 holding a pointer to a dead bridge.
        let _guard = UserDataGuard { raw };

        // SAFETY: `raw` is live, and `bridge` outlives the call below because it is a
        // local of this frame and `call` returns before it is dropped.
        unsafe {
            sys::nghttp2_session_set_user_data(raw, (&raw mut bridge).cast::<core::ffi::c_void>());
        }

        call(raw)
    }

    /// Submits a request, returning the stream it was assigned.
    ///
    /// The header set must carry its pseudo-header fields first and must already use
    /// lowercase names; it is validated before anything reaches libnghttp2, so a rejected
    /// set leaves the session untouched and usable.
    ///
    /// This phase submits requests without a body, so the headers carry end-of-stream.
    pub fn submit_request(&mut self, headers: &[Header<'_>]) -> Result<StreamId> {
        let nva = header::to_nv_vec(headers)?;

        // SAFETY: `self.raw` is live and `nva` is valid for its length. libnghttp2 copies
        // every name and value, so the borrowed header data need not outlive this call.
        // A null priority spec and null data provider are both documented as accepted.
        let rc = unsafe {
            sys::nghttp2_submit_request2(
                self.raw,
                core::ptr::null(),
                nva.as_ptr(),
                nva.len(),
                core::ptr::null(),
                core::ptr::null_mut(),
            )
        };

        if rc < 0 {
            return Err(Error::from_native("nghttp2_submit_request2", rc));
        }
        Ok(StreamId::new(rc))
    }

    /// Submits a response on an open stream.
    ///
    /// Submitting a second response for one stream is rejected here rather than passed
    /// on: libnghttp2 documents that as a programming error that may crash the process.
    ///
    /// A well-formed identifier naming a stream that is not open is accepted by
    /// libnghttp2 and simply produces no frame, so it is not reported as an error.
    ///
    /// This phase submits responses without a body, so the headers carry end-of-stream.
    pub fn submit_response(&mut self, stream: StreamId, headers: &[Header<'_>]) -> Result<()> {
        let nva = header::to_nv_vec(headers)?;

        // The duplicate guard only applies to streams that actually exist. libnghttp2
        // accepts a response for an unopened stream and silently drops the frame without
        // ever closing a stream, so claiming one here would leave an entry that nothing
        // releases — poisoning a later, genuine stream that reused the identifier.
        let track = self.stream_is_open(stream);

        if track && !self.responded.claim(stream) {
            return Err(Error::new(
                "nghttp2_submit_response2",
                ErrorKind::InvalidInput,
                "a response has already been submitted for this stream",
            ));
        }

        // SAFETY: as for `submit_request`, with the stream identifier checked by
        // libnghttp2 itself.
        let rc = unsafe {
            sys::nghttp2_submit_response2(
                self.raw,
                stream.get(),
                nva.as_ptr(),
                nva.len(),
                core::ptr::null(),
            )
        };

        if rc != 0 {
            // The claim is rolled back so a caller that corrects the arguments and retries
            // is not blocked by its own failed attempt.
            if track {
                self.responded.release(stream);
            }
            return Err(Error::from_native("nghttp2_submit_response2", rc));
        }
        Ok(())
    }

    /// Submits a non-final informational (`1xx`) response on an open stream.
    ///
    /// Hidden test scaffolding, not part of the promised surface: it exists so a peer in
    /// this crate's own tests can reproduce a server that sends `103 Early Hints` or
    /// `100 Continue` ahead of the real response, which the safe surface otherwise offers
    /// no way to do. Unlike [`submit_response`](Self::submit_response) it carries no
    /// end-of-stream, so the stream stays open for the final HEADERS that must follow —
    /// which is exactly the sequence libnghttp2 requires for an informational response.
    #[doc(hidden)]
    pub fn submit_informational(&mut self, stream: StreamId, headers: &[Header<'_>]) -> Result<()> {
        let nva = header::to_nv_vec(headers)?;

        // SAFETY: `self.raw` is live and `nva` is valid for its length; libnghttp2 copies
        // the contents. `NGHTTP2_FLAG_NONE` (no END_STREAM) is what makes this a non-final
        // response. A null priority spec and null stream user data are both accepted.
        let rc = unsafe {
            sys::nghttp2_submit_headers(
                self.raw,
                sys::NGHTTP2_FLAG_NONE as u8,
                stream.get(),
                core::ptr::null(),
                nva.as_ptr(),
                nva.len(),
                core::ptr::null_mut(),
            )
        };

        if rc < 0 {
            return Err(Error::from_native("nghttp2_submit_headers", rc));
        }
        Ok(())
    }

    /// Whether `stream` is currently open on this session.
    ///
    /// A server needs this before answering: a peer may reset or close a stream while its
    /// handler is still running, and submitting a response for a stream that is gone is
    /// rejected rather than silently dropped.
    ///
    /// Uses the half-closed predicate, which returns -1 exactly when no such stream
    /// exists and 0 or 1 otherwise. A window-size query would be the obvious probe but is
    /// wrong: a stream's local window legitimately goes negative when the local initial
    /// window size is reduced while data is in flight, so a negative result there does
    /// not mean the stream is absent.
    ///
    /// The connection stream is never open in this sense, so stream zero is always
    /// `false`.
    pub fn stream_is_open(&self, stream: StreamId) -> bool {
        if stream.is_connection() {
            return false;
        }
        // SAFETY: `self.raw` is live; this only inspects session state and accepts any
        // stream identifier.
        let state = unsafe { sys::nghttp2_session_get_stream_local_close(self.raw, stream.get()) };
        state >= 0
    }

    /// Submits a trailing header block on an open stream.
    ///
    /// Trailers may only follow a message whose body signalled that they were coming. A
    /// message submitted without a body carries end-of-stream on its headers, after which
    /// nothing further can be sent on that stream.
    pub fn submit_trailer(&mut self, stream: StreamId, headers: &[Header<'_>]) -> Result<()> {
        let nva = header::to_trailer_nv_vec(headers)?;

        // libnghttp2 accepts a trailer block for a stream that cannot carry one and then
        // emits nothing, which reads as success and is not. Trailers are legal only once
        // this stream's body has reported EofWithTrailers.
        if !self.bodies.trailers_ready(stream) {
            return Err(Error::new(
                "nghttp2_submit_trailer",
                ErrorKind::InvalidInput,
                "this stream has no open trailer window; a body source must first report \
                 BodyOutcome::EofWithTrailers",
            ));
        }

        // SAFETY: `self.raw` is live and `nva` is valid for its length; libnghttp2 copies
        // the contents.
        let rc =
            unsafe { sys::nghttp2_submit_trailer(self.raw, stream.get(), nva.as_ptr(), nva.len()) };

        if rc != 0 {
            return Err(Error::from_native("nghttp2_submit_trailer", rc));
        }
        Ok(())
    }

    /// Submits a request with a body.
    ///
    /// The body is produced progressively by `body` as capacity becomes available. To
    /// send trailers afterwards, the source must report
    /// [`BodyOutcome::EofWithTrailers`](crate::BodyOutcome::EofWithTrailers).
    pub fn submit_request_with_body(
        &mut self,
        headers: &[Header<'_>],
        body: impl BodySource + 'static,
    ) -> Result<StreamId> {
        let nva = header::to_nv_vec(headers)?;
        let entry = BodyRegistry::prepare(BodyEntry::new(Box::new(body)));
        let provider = Self::provider::<C>(entry);

        // SAFETY: `self.raw` is live, `nva` is valid for its length and is copied by
        // libnghttp2, and `provider` is copied too — the entry it points at is owned by
        // this session's registry and outlives the stream.
        let rc = unsafe {
            sys::nghttp2_submit_request2(
                self.raw,
                core::ptr::null(),
                nva.as_ptr(),
                nva.len(),
                &provider,
                core::ptr::null_mut(),
            )
        };

        if rc < 0 {
            BodyRegistry::discard(entry);
            return Err(Error::from_native("nghttp2_submit_request2", rc));
        }

        let stream = StreamId::new(rc);
        self.bodies.attach(stream, entry);
        Ok(stream)
    }

    /// Submits a response with a body.
    pub fn submit_response_with_body(
        &mut self,
        stream: StreamId,
        headers: &[Header<'_>],
        body: impl BodySource + 'static,
    ) -> Result<()> {
        let nva = header::to_nv_vec(headers)?;

        // A response *without* a body may name a stream that is not open: libnghttp2
        // accepts it and drops the frame. A response *with* one may not. libnghttp2 would
        // queue the outbound item holding this entry's address, but with no stream there
        // is nothing whose closure releases it, and a later submission for the same
        // identifier would replace the entry while the queued item still pointed at it.
        // Rejecting here is what keeps that address valid for exactly as long as C holds
        // it.
        if !self.stream_is_open(stream) {
            return Err(Error::new(
                "nghttp2_submit_response2",
                ErrorKind::InvalidInput,
                "cannot attach a body to a stream that is not open",
            ));
        }

        if !self.responded.claim(stream) {
            return Err(Error::new(
                "nghttp2_submit_response2",
                ErrorKind::InvalidInput,
                "a response has already been submitted for this stream",
            ));
        }

        let entry = BodyRegistry::prepare(BodyEntry::new(Box::new(body)));
        let provider = Self::provider::<C>(entry);

        // SAFETY: as for `submit_request_with_body`.
        let rc = unsafe {
            sys::nghttp2_submit_response2(
                self.raw,
                stream.get(),
                nva.as_ptr(),
                nva.len(),
                &provider,
            )
        };

        if rc != 0 {
            BodyRegistry::discard(entry);
            self.responded.release(stream);
            return Err(Error::from_native("nghttp2_submit_response2", rc));
        }

        self.bodies.attach(stream, entry);
        Ok(())
    }

    /// Builds the data provider libnghttp2 copies at submission.
    ///
    /// The union member is a bare pointer, which cannot hold a trait object, so it holds
    /// the address of the owning entry instead.
    fn provider<T>(entry: core::ptr::NonNull<BodyEntry>) -> sys::nghttp2_data_provider2 {
        sys::nghttp2_data_provider2 {
            source: sys::nghttp2_data_source {
                ptr: entry.as_ptr().cast::<core::ffi::c_void>(),
            },
            read_callback: Some(callbacks::read_body::<T>),
        }
    }

    /// The data provider for a no-copy body.
    ///
    /// Identical to [`provider`](Self::provider) but for the entry it points at: the same
    /// `read_body` dispatcher serves both, choosing the no-copy path once it sees the
    /// entry carries a [`SharedBodySource`]. libnghttp2 will invoke the registered send
    /// callback for each frame this source produces.
    #[cfg(feature = "http")]
    fn provider_shared<T>(entry: core::ptr::NonNull<BodyEntry>) -> sys::nghttp2_data_provider2 {
        sys::nghttp2_data_provider2 {
            source: sys::nghttp2_data_source {
                ptr: entry.as_ptr().cast::<core::ffi::c_void>(),
            },
            read_callback: Some(callbacks::read_body::<T>),
        }
    }

    /// Submits a request with a no-copy body.
    ///
    /// The no-copy counterpart of [`submit_request_with_body`](Self::submit_request_with_body):
    /// the body hands over octets it already owns rather than writing into a session
    /// buffer, and libnghttp2 serialises each frame's header only. To send trailers
    /// afterwards, the source must report
    /// [`SharedOutcome::EofWithTrailers`](crate::body::SharedOutcome::EofWithTrailers).
    #[cfg(feature = "http")]
    pub(crate) fn submit_request_with_shared_body(
        &mut self,
        headers: &[Header<'_>],
        body: impl SharedBodySource + 'static,
    ) -> Result<StreamId> {
        let nva = header::to_nv_vec(headers)?;
        let entry = BodyRegistry::prepare(BodyEntry::new_shared(Box::new(body)));
        let provider = Self::provider_shared::<C>(entry);

        // SAFETY: as for `submit_request_with_body` — `self.raw` is live, `nva` is valid
        // and copied by libnghttp2, and `provider` points at an entry this session's
        // registry owns for longer than the stream.
        let rc = unsafe {
            sys::nghttp2_submit_request2(
                self.raw,
                core::ptr::null(),
                nva.as_ptr(),
                nva.len(),
                &provider,
                core::ptr::null_mut(),
            )
        };

        if rc < 0 {
            BodyRegistry::discard(entry);
            return Err(Error::from_native("nghttp2_submit_request2", rc));
        }

        let stream = StreamId::new(rc);
        self.bodies.attach(stream, entry);
        Ok(stream)
    }

    /// Submits a response with a no-copy body.
    ///
    /// The no-copy counterpart of
    /// [`submit_response_with_body`](Self::submit_response_with_body), with the same
    /// open-stream and single-response guards.
    #[cfg(feature = "http")]
    pub(crate) fn submit_response_with_shared_body(
        &mut self,
        stream: StreamId,
        headers: &[Header<'_>],
        body: impl SharedBodySource + 'static,
    ) -> Result<()> {
        let nva = header::to_nv_vec(headers)?;

        // A response *with* a body may not name a stream that is not open, for the same
        // reason `submit_response_with_body` gives: libnghttp2 would queue an outbound item
        // holding this entry's address with nothing to release it at closure.
        if !self.stream_is_open(stream) {
            return Err(Error::new(
                "nghttp2_submit_response2",
                ErrorKind::InvalidInput,
                "cannot attach a body to a stream that is not open",
            ));
        }

        if !self.responded.claim(stream) {
            return Err(Error::new(
                "nghttp2_submit_response2",
                ErrorKind::InvalidInput,
                "a response has already been submitted for this stream",
            ));
        }

        let entry = BodyRegistry::prepare(BodyEntry::new_shared(Box::new(body)));
        let provider = Self::provider_shared::<C>(entry);

        // SAFETY: as for `submit_response_with_body`.
        let rc = unsafe {
            sys::nghttp2_submit_response2(
                self.raw,
                stream.get(),
                nva.as_ptr(),
                nva.len(),
                &provider,
            )
        };

        if rc != 0 {
            BodyRegistry::discard(entry);
            self.responded.release(stream);
            return Err(Error::from_native("nghttp2_submit_response2", rc));
        }

        self.bodies.attach(stream, entry);
        Ok(())
    }

    /// Whether this stream's body has announced that trailers may follow.
    ///
    /// Trailers are only legal once the body source has reported
    /// [`BodyOutcome::EofWithTrailers`](crate::BodyOutcome::EofWithTrailers), which
    /// happens while the session is serialising rather than when the caller submits. This
    /// reports when that moment has arrived.
    ///
    /// Note the session stops wanting to write once a body ends without closing its
    /// stream, so a loop that drains until idle must check this before concluding the
    /// exchange is over.
    pub fn trailers_ready(&self, stream: StreamId) -> bool {
        self.bodies.trailers_ready(stream)
    }

    /// Reports that received data has been consumed, replenishing flow-control windows.
    ///
    /// Only available on sessions built with
    /// [`SessionBuilder::manual_flow_control`]. Sessions replenish windows automatically
    /// by default, and calling this on one of those is rejected rather than silently
    /// doing nothing.
    pub fn consume(&mut self, stream: StreamId, len: usize) -> Result<()> {
        if !self.manual_flow_control {
            return Err(Error::new(
                "nghttp2_session_consume",
                ErrorKind::InvalidInput,
                "this session replenishes flow-control windows automatically; \
                 build it with manual_flow_control(true) to report consumption",
            ));
        }

        // libnghttp2 narrows the length internally, so a value that does not fit would be
        // silently truncated and corrupt the flow-control accounting rather than being
        // rejected. Check it here instead of claiming the callee validates it.
        if i32::try_from(len).is_err() {
            return Err(Error::new(
                "nghttp2_session_consume",
                ErrorKind::InvalidInput,
                "consumed length does not fit in the range libnghttp2 accounts in",
            ));
        }

        // SAFETY: `self.raw` is live and `len` has been range-checked above; the stream
        // identifier is validated by libnghttp2.
        let rc = unsafe { sys::nghttp2_session_consume(self.raw, stream.get(), len) };
        if rc != 0 {
            return Err(Error::from_native("nghttp2_session_consume", rc));
        }
        Ok(())
    }

    /// Cancels a single stream, so the peer observes a `RST_STREAM`.
    pub fn reset_stream(&mut self, stream: StreamId, code: ErrorCode) -> Result<()> {
        // SAFETY: `self.raw` is live. Flags are documented as ignored and must be NONE.
        let rc = unsafe {
            sys::nghttp2_submit_rst_stream(
                self.raw,
                sys::NGHTTP2_FLAG_NONE as u8,
                stream.get(),
                code.get(),
            )
        };

        if rc != 0 {
            return Err(Error::from_native("nghttp2_submit_rst_stream", rc));
        }
        Ok(())
    }

    /// Begins a graceful connection shutdown.
    ///
    /// `last_stream` names the highest peer-initiated stream that was processed; streams
    /// above it are abandoned and may be retried by the peer on a new connection.
    pub fn shutdown(&mut self, last_stream: StreamId, code: ErrorCode) -> Result<()> {
        // SAFETY: `self.raw` is live. The debug payload is empty, so a null pointer with
        // length zero is passed, which libnghttp2 documents as accepted.
        let rc = unsafe {
            sys::nghttp2_submit_goaway(
                self.raw,
                sys::NGHTTP2_FLAG_NONE as u8,
                last_stream.get(),
                code.get(),
                core::ptr::null(),
                0,
            )
        };

        if rc != 0 {
            return Err(Error::from_native("nghttp2_submit_goaway", rc));
        }
        Ok(())
    }

    /// Resumes a stream whose body previously returned [`BodyOutcome::Defer`](crate::BodyOutcome::Defer).
    ///
    /// Puts the deferred `DATA` frame back on the outbound queue, after which the next
    /// [`Session::send`] may consult that body again. Until this is called, a deferred
    /// stream is inert: nothing else will ask its body for data.
    ///
    /// Returns [`ErrorKind::InvalidInput`] when there is nothing to resume — the stream
    /// has closed, never existed, or has no deferred data. That is routinely benign
    /// rather than a fault: an asynchronous body may signal readiness just as its stream
    /// is being reset, so a caller draining readiness notifications should treat this
    /// outcome as a stale notification and carry on. Allocation failure surfaces
    /// separately as [`ErrorKind::Exhausted`], and is not benign.
    pub fn resume_body(&mut self, stream: StreamId) -> Result<()> {
        // SAFETY: `self.raw` is live; the call only inspects and requeues session state
        // and accepts any stream identifier.
        let rc = unsafe { sys::nghttp2_session_resume_data(self.raw, stream.get()) };

        if rc != 0 {
            return Err(Error::from_native("nghttp2_session_resume_data", rc));
        }
        Ok(())
    }

    /// Whether the session is part-way through receiving a frame.
    ///
    /// True once any part of a frame has arrived and the frame is not yet whole — its
    /// nine-octet header included. A transport reporting end-of-file while this holds
    /// means the peer truncated a frame rather than closing cleanly, which is a
    /// connection error rather than an orderly shutdown.
    ///
    /// This is counted from the octets handed to [`Session::recv`] rather than inferred
    /// from libnghttp2's callbacks, because not every frame that begins produces a
    /// frame-received callback — a valid `PRIORITY` frame does not, nor does a discarded
    /// payload — and a tracker built on that pairing would stick, turning a later clean
    /// close into a reported truncation.
    pub const fn mid_frame(&self) -> bool {
        self.frames.in_frame()
    }

    /// Whether the session still wants to read from the peer.
    pub fn want_read(&self) -> bool {
        // SAFETY: `self.raw` is live; this only inspects session state.
        unsafe { sys::nghttp2_session_want_read(self.raw) != 0 }
    }

    /// Whether the session still has anything to write.
    pub fn want_write(&self) -> bool {
        // SAFETY: `self.raw` is live; this only inspects session state.
        unsafe { sys::nghttp2_session_want_write(self.raw) != 0 }
    }

    /// Whether the connection may be closed.
    ///
    /// True once the session neither wants to read nor to write.
    pub fn is_finished(&self) -> bool {
        !self.want_read() && !self.want_write()
    }

    /// A handle on this session's native allocation accounting.
    ///
    /// Cloning it lets a caller observe the counters after the session itself has been
    /// dropped, which is how deterministic teardown is asserted.
    #[cfg(test)]
    fn allocation_state(&self) -> Arc<AllocState> {
        Arc::clone(&self.allocation)
    }
}

/// Clears a session's user data on the way out of [`Session::with_context`].
struct UserDataGuard {
    raw: *mut sys::nghttp2_session,
}

impl Drop for UserDataGuard {
    fn drop(&mut self) {
        // SAFETY: `raw` is live for as long as the session that created this guard, and
        // clearing the pointer is always valid.
        unsafe { sys::nghttp2_session_set_user_data(self.raw, core::ptr::null_mut()) };
    }
}

impl<C> fmt::Debug for Session<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately reports observable state rather than the raw pointer, which would
        // be noise to a caller and unstable between runs.
        f.debug_struct("Session")
            .field("want_read", &self.want_read())
            .field("want_write", &self.want_write())
            .finish_non_exhaustive()
    }
}

impl<C> Drop for Session<C> {
    fn drop(&mut self) {
        // SAFETY: `self.raw` was produced by a session constructor and is dropped exactly
        // once. `nghttp2_session_del` is null-safe. It frees through the allocator
        // recorded in the session, whose state is kept alive by `self.allocation` until
        // after this returns.
        unsafe { sys::nghttp2_session_del(self.raw) };

        // Teardown must return every block the session took. Asserting it here means any
        // debug-mode test that builds a session gets leak checking for free, rather than
        // only the tests that ask for it explicitly. This is also what keeps the
        // `allocation` field alive to the compiler: it exists to hold the allocator state
        // at a stable address for libnghttp2, which is a use Rust cannot otherwise see.
        debug_assert_eq!(
            self.allocation.live_blocks(),
            0,
            "session teardown leaked {} native block(s)",
            self.allocation.live_blocks()
        );
    }
}

/// Owned wrapper over `nghttp2_session_callbacks`.
///
/// Session constructors copy every callback member out of this object, so it only needs
/// to survive the construction call itself.
struct Callbacks {
    raw: *mut sys::nghttp2_session_callbacks,
}

impl Callbacks {
    fn new() -> Result<Self> {
        let mut raw: *mut sys::nghttp2_session_callbacks = core::ptr::null_mut();
        // SAFETY: `raw` is a valid out-parameter; on success it receives a freshly
        // allocated callbacks object that `Drop` releases.
        let rc = unsafe { sys::nghttp2_session_callbacks_new(&mut raw) };
        if rc != 0 {
            return Err(Error::from_native("nghttp2_session_callbacks_new", rc));
        }
        debug_assert!(!raw.is_null());
        Ok(Self { raw })
    }

    /// Points every callback slot this crate uses at its trampoline.
    ///
    /// Every trampoline is registered regardless of which handlers the caller supplied;
    /// each returns immediately when its slot is empty, which is what makes an
    /// unregistered event a silent no-op.
    fn install<C>(&mut self) {
        // SAFETY: `self.raw` is a live callbacks object. Each setter stores a function
        // pointer, and the session constructor later copies them out.
        unsafe {
            sys::nghttp2_session_callbacks_set_on_begin_headers_callback(
                self.raw,
                Some(callbacks::on_begin_headers::<C>),
            );
            sys::nghttp2_session_callbacks_set_on_header_callback(
                self.raw,
                Some(callbacks::on_header::<C>),
            );
            sys::nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
                self.raw,
                Some(callbacks::on_data_chunk_recv::<C>),
            );
            sys::nghttp2_session_callbacks_set_on_frame_recv_callback(
                self.raw,
                Some(callbacks::on_frame_recv::<C>),
            );
            sys::nghttp2_session_callbacks_set_on_stream_close_callback(
                self.raw,
                Some(callbacks::on_stream_close::<C>),
            );
            // The send callback is only ever invoked for a frame a no-copy read callback
            // marked, which only the `http`-gated shared body path produces. Registering it
            // unconditionally under that gate keeps the no-`http` build free of the
            // `bytes`-valued record machinery it cannot use.
            #[cfg(feature = "http")]
            sys::nghttp2_session_callbacks_set_send_data_callback(
                self.raw,
                Some(callbacks::send_data::<C>),
            );
        }
    }

    fn as_ptr(&self) -> *const sys::nghttp2_session_callbacks {
        self.raw
    }
}

impl Drop for Callbacks {
    fn drop(&mut self) {
        // SAFETY: `self.raw` came from `nghttp2_session_callbacks_new` and is freed once.
        unsafe { sys::nghttp2_session_callbacks_del(self.raw) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 24-byte client connection preface that opens every h2c connection.
    const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    fn drain(session: &mut Session<()>) -> Vec<u8> {
        let mut wire = Vec::new();
        while let Some(block) = session.send(&mut ()).expect("send failed") {
            wire.extend_from_slice(block);
        }
        wire
    }

    #[test]
    fn client_emits_preface_then_settings() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let wire = drain(&mut session);

        assert!(
            wire.starts_with(CLIENT_MAGIC),
            "expected the h2c client preface, got {:?}",
            &wire[..wire.len().min(24)]
        );

        // A session that never submits SETTINGS emits the preface alone, so this length
        // check is what proves the frame is actually queued.
        let frame = &wire[CLIENT_MAGIC.len()..];
        assert!(
            frame.len() >= 9,
            "expected a SETTINGS frame after the preface, got {} trailing bytes",
            frame.len()
        );
        assert_eq!(
            frame[3],
            sys::NGHTTP2_SETTINGS as u8,
            "the frame following the preface must be SETTINGS"
        );
    }

    #[test]
    fn server_emits_settings_without_a_preface() {
        let mut session = SessionBuilder::<()>::server().build().unwrap();
        let wire = drain(&mut session);

        assert!(
            !wire.is_empty(),
            "a server must still announce its SETTINGS"
        );
        assert!(
            !wire.starts_with(CLIENT_MAGIC),
            "only clients send the preface"
        );
        assert_eq!(wire[3], sys::NGHTTP2_SETTINGS as u8);
    }

    #[test]
    fn configured_settings_appear_in_the_emitted_frame() {
        let mut session = SessionBuilder::<()>::client()
            .setting(Setting::MaxConcurrentStreams(77))
            .setting(Setting::InitialWindowSize(4242))
            .build()
            .unwrap();
        let wire = drain(&mut session);

        // Each entry is six octets: a two-octet identifier and a four-octet value.
        let payload = &wire[CLIENT_MAGIC.len() + 9..];
        let entries: Vec<(u16, u32)> = payload
            .chunks_exact(6)
            .map(|c| {
                (
                    u16::from_be_bytes([c[0], c[1]]),
                    u32::from_be_bytes([c[2], c[3], c[4], c[5]]),
                )
            })
            .collect();

        assert!(
            entries.contains(&(0x03, 77)),
            "MAX_CONCURRENT_STREAMS missing from {entries:?}"
        );
        assert!(
            entries.contains(&(0x04, 4242)),
            "INITIAL_WINDOW_SIZE missing from {entries:?}"
        );
    }

    #[test]
    fn dropping_a_session_frees_every_native_block() {
        let counters = {
            let session = SessionBuilder::<()>::client().build().unwrap();
            let counters = session.allocation_state();
            assert!(
                counters.live_blocks() > 0,
                "building a session should have allocated something"
            );
            counters
        };

        assert_eq!(
            counters.live_blocks(),
            0,
            "every native block allocated by the session must be freed when it drops"
        );
        assert!(
            counters.total_allocations() > 0,
            "the balance assertion must not pass vacuously"
        );
    }

    #[test]
    fn many_sessions_leave_nothing_behind() {
        for _ in 0..256 {
            let counters = {
                let mut session = SessionBuilder::<()>::client()
                    .setting(Setting::MaxConcurrentStreams(10))
                    .build()
                    .unwrap();
                let _ = drain(&mut session);
                session.allocation_state()
            };
            assert_eq!(counters.live_blocks(), 0);
        }
    }

    /// Parses the SETTINGS payload that follows a client preface into (id, value) pairs.
    fn settings_entries(wire: &[u8], skip_preface: bool) -> Vec<(u16, u32)> {
        let start = if skip_preface { CLIENT_MAGIC.len() } else { 0 } + 9;
        wire[start..]
            .chunks_exact(6)
            .map(|c| {
                (
                    u16::from_be_bytes([c[0], c[1]]),
                    u32::from_be_bytes([c[2], c[3], c[4], c[5]]),
                )
            })
            .collect()
    }

    const ENABLE_PUSH_ID: u16 = 0x02;

    #[test]
    fn a_client_disables_push_by_default() {
        // Server push is out of scope, so PUSH_PROMISE must never reach a handler. The
        // cheapest way to guarantee that is to tell the peer not to send any.
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let entries = settings_entries(&drain(&mut session), true);

        assert!(
            entries.contains(&(ENABLE_PUSH_ID, 0)),
            "a client should advertise ENABLE_PUSH = 0 by default, got {entries:?}"
        );
    }

    #[test]
    fn an_explicit_client_push_preference_is_respected() {
        let mut session = SessionBuilder::<()>::client()
            .setting(Setting::EnablePush(true))
            .build()
            .unwrap();
        let entries = settings_entries(&drain(&mut session), true);

        assert!(
            entries.contains(&(ENABLE_PUSH_ID, 1)),
            "an explicit preference must win over the default, got {entries:?}"
        );
        assert_eq!(
            entries
                .iter()
                .filter(|(id, _)| *id == ENABLE_PUSH_ID)
                .count(),
            1,
            "the default must not be injected alongside an explicit value"
        );
    }

    #[test]
    fn a_server_may_not_advertise_push_enabled() {
        // RFC 9113 forbids it, and libnghttp2 only range-checks the value, so rejecting
        // it is this crate's job.
        let error = SessionBuilder::<()>::server()
            .setting(Setting::EnablePush(true))
            .build()
            .expect_err("a server advertising ENABLE_PUSH = 1 must be rejected");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("ENABLE_PUSH"));
    }

    #[test]
    fn a_server_may_advertise_push_disabled() {
        let mut session = SessionBuilder::<()>::server()
            .setting(Setting::EnablePush(false))
            .build()
            .expect("advertising ENABLE_PUSH = 0 is legal for a server");
        let entries = settings_entries(&drain(&mut session), false);

        assert!(entries.contains(&(ENABLE_PUSH_ID, 0)), "got {entries:?}");
    }

    #[test]
    fn a_server_cannot_smuggle_push_enabled_past_a_duplicate() {
        // Duplicate identifiers are legal on the wire and libnghttp2 applies the last
        // one, so a check that only inspected the first entry would be bypassed here.
        for settings in [
            vec![Setting::EnablePush(false), Setting::EnablePush(true)],
            vec![Setting::EnablePush(true), Setting::EnablePush(false)],
            vec![
                Setting::MaxConcurrentStreams(3),
                Setting::EnablePush(false),
                Setting::EnablePush(true),
            ],
        ] {
            let mut builder = SessionBuilder::<()>::server();
            for setting in &settings {
                builder = builder.setting(*setting);
            }

            let error = builder
                .build()
                .expect_err("ENABLE_PUSH = 1 anywhere in a server's settings must be rejected");
            assert_eq!(error.kind(), ErrorKind::InvalidInput, "for {settings:?}");
        }
    }

    #[test]
    fn a_client_with_duplicate_push_settings_gets_no_injection() {
        let mut session = SessionBuilder::<()>::client()
            .setting(Setting::EnablePush(true))
            .setting(Setting::EnablePush(false))
            .build()
            .unwrap();
        let entries = settings_entries(&drain(&mut session), true);

        assert_eq!(
            entries
                .iter()
                .filter(|(id, _)| *id == ENABLE_PUSH_ID)
                .count(),
            2,
            "the caller's entries should pass through untouched, got {entries:?}"
        );
    }

    #[test]
    fn a_server_gets_no_injected_push_setting() {
        let mut session = SessionBuilder::<()>::server().build().unwrap();
        let entries = settings_entries(&drain(&mut session), false);

        assert!(
            !entries.iter().any(|(id, _)| *id == ENABLE_PUSH_ID),
            "the client-side default must not leak into server sessions, got {entries:?}"
        );
    }

    #[test]
    fn a_fresh_session_wants_to_read_and_write() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();

        assert!(session.want_write(), "the preface and SETTINGS are pending");
        assert!(
            session.want_read(),
            "a fresh session expects the peer's SETTINGS"
        );
        assert!(!session.is_finished());

        let _ = drain(&mut session);
        assert!(!session.want_write(), "everything pending has been drained");
    }

    #[test]
    fn draining_an_idle_session_reports_nothing_pending() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let _ = drain(&mut session);

        assert!(
            session.send(&mut ()).unwrap().is_none(),
            "an idle session must report nothing pending rather than an empty block"
        );
    }
}

/// The no-copy shared-body seam: a `SharedBodySource` submitted to the session, driven
/// through `send_into`, produces `SendRecord`s whose headers and payloads the driver will
/// later write. Everything here is `http`-gated, since the whole seam is.
#[cfg(all(test, feature = "http"))]
mod shared_body_tests {
    use super::*;

    use std::collections::VecDeque;

    use bytes::Bytes;

    use crate::body::{BytesBody, SharedBodySource, SharedOutcome};
    use crate::state::SendRecord;

    /// A scripted no-copy body: each `take` returns the next outcome, or an empty EOF once
    /// the script is exhausted.
    struct Scripted {
        steps: VecDeque<SharedOutcome>,
    }

    impl Scripted {
        fn new(steps: Vec<SharedOutcome>) -> Self {
            Self {
                steps: steps.into(),
            }
        }
    }

    impl SharedBodySource for Scripted {
        fn take(&mut self, _limit: usize) -> SharedOutcome {
            self.steps
                .pop_front()
                .unwrap_or_else(|| SharedOutcome::Eof(Bytes::new()))
        }
    }

    fn request_headers() -> Vec<Header<'static>> {
        vec![
            Header::new(":method", "POST"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "example.test"),
            Header::new(":path", "/upload"),
        ]
    }

    /// What one `send_into` call produced: the records it deposited, and the block it
    /// returned (owned, so the borrow of the session ends before the next call).
    struct Emission {
        records: Vec<SendRecord>,
        block: Option<Vec<u8>>,
    }

    /// Drives the session to quiescence, one `send_into` per iteration, draining the sink
    /// after *every* call — the final `None`-returning one included, which is the whole
    /// point of the sink. Returns the per-call emissions in order.
    fn drive<C>(session: &mut Session<C>, context: &mut C) -> Vec<Emission> {
        let mut emissions = Vec::new();
        let mut sink: Vec<SendRecord> = Vec::new();
        loop {
            let block = session
                .send_into(context, &mut sink)
                .expect("send_into failed");
            // Take the owned block first so the borrow of `self` ends, then collect the
            // records the call deposited.
            let block = block.map(<[u8]>::to_vec);
            let records = std::mem::take(&mut sink);
            let done = block.is_none();
            emissions.push(Emission { records, block });
            if done {
                break;
            }
        }
        emissions
    }

    fn all_records(emissions: &[Emission]) -> Vec<&SendRecord> {
        emissions.iter().flat_map(|e| e.records.iter()).collect()
    }

    /// Unpacks a nine-octet `DATA` frame header: (length, type, flags, stream id).
    fn parse_data_header(header: &[u8; 9]) -> (usize, u8, u8, u32) {
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let kind = header[3];
        let flags = header[4];
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        (len, kind, flags, stream)
    }

    /// A context whose stream-close handler records every closure and its parked error.
    #[derive(Default)]
    struct Closed {
        closed: Vec<(i32, u32, Option<String>)>,
    }

    fn closing_client() -> SessionBuilder<Closed> {
        SessionBuilder::<Closed>::client().on_stream_close(
            |c: &mut Closed, stream: StreamId, code: ErrorCode, error| {
                c.closed
                    .push((stream.get(), code.get(), error.map(|e| e.to_string())));
            },
        )
    }

    /// A trivial body failure. Avoids `std::io::Error`, which the sans-I/O facility scan in
    /// `tests/invariants.rs` forbids anywhere under `src/`.
    #[derive(Debug)]
    struct BodyFault(&'static str);

    impl std::fmt::Display for BodyFault {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for BodyFault {}

    #[test]
    fn a_single_chunk_shared_body_produces_one_well_formed_record() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let stream = session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Eof(Bytes::from_static(b"hello"))]),
            )
            .expect("submitting the shared request");
        assert_eq!(stream.get(), 1);

        let emissions = drive(&mut session, &mut ());
        let records = all_records(&emissions);
        assert_eq!(records.len(), 1, "one chunk yields exactly one record");

        let (len, kind, flags, stream_id) = parse_data_header(&records[0].header);
        assert_eq!(kind, 0x0, "a DATA frame");
        assert_eq!(len, 5, "the header length equals the payload length");
        assert_eq!(stream_id, 1, "the record names its stream");
        assert_eq!(flags & 0x1, 0x1, "END_STREAM on the only, final frame");
        assert_eq!(
            &records[0].payload[..],
            b"hello",
            "the payload is handed over verbatim"
        );
    }

    #[test]
    fn a_shared_payload_is_never_copied_into_a_wire_block() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Eof(Bytes::from_static(
                    b"secretpayload",
                ))]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut ());
        for emission in &emissions {
            if let Some(block) = &emission.block {
                assert!(
                    !block
                        .windows(b"secretpayload".len())
                        .any(|w| w == b"secretpayload"),
                    "the payload must travel as a record, never copied into a returned block"
                );
            }
        }
        assert_eq!(
            all_records(&emissions).len(),
            1,
            "the payload did travel — as a record"
        );
    }

    #[test]
    fn a_record_rides_along_with_the_final_none_return() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Eof(Bytes::from_static(b"tail"))]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut ());

        // The no-copy branch deposits a record without contributing octets to the returned
        // block, so the DATA record arrives on a call that returns `None`. A sink drained
        // only after `Some` blocks would lose it.
        let on_some: usize = emissions
            .iter()
            .filter(|e| e.block.is_some())
            .map(|e| e.records.len())
            .sum();
        assert_eq!(
            on_some, 0,
            "no record accompanies a returned block in this exchange"
        );
        assert!(
            emissions
                .iter()
                .any(|e| e.block.is_none() && !e.records.is_empty()),
            "the record is delivered by a `None`-returning call, which is why the sink must \
             be drained after every call"
        );
    }

    /// A no-copy body source that hands out the next `limit` octets of a buffer it owns.
    ///
    /// Unlike [`Scripted`], the frame boundaries are libnghttp2's to choose rather than
    /// baked into the test, which is what lets the exchange below be compared octet for
    /// octet against the push path driving the same body.
    struct Chunker {
        data: Bytes,
        offset: usize,
    }

    impl Chunker {
        fn new(data: Bytes) -> Self {
            Self { data, offset: 0 }
        }
    }

    impl SharedBodySource for Chunker {
        fn take(&mut self, limit: usize) -> SharedOutcome {
            let take = (self.data.len() - self.offset).min(limit);
            let chunk = self.data.slice(self.offset..self.offset + take);
            self.offset += take;
            if self.offset == self.data.len() {
                SharedOutcome::Eof(chunk)
            } else {
                SharedOutcome::Wrote(chunk)
            }
        }
    }

    /// Flattens an exchange into the octets a driver would write, records first.
    ///
    /// `swap` inverts that rule for calls that produced both, which is the misordering
    /// this contract exists to forbid.
    fn flatten(emissions: &[Emission], swap: bool) -> Vec<u8> {
        let mut wire = Vec::new();
        for emission in emissions {
            let mut records = Vec::new();
            for record in &emission.records {
                records.extend_from_slice(&record.header);
                records.extend_from_slice(&record.payload);
            }
            let block = emission.block.clone().unwrap_or_default();
            if swap {
                wire.extend_from_slice(&block);
                wire.extend_from_slice(&records);
            } else {
                wire.extend_from_slice(&records);
                wire.extend_from_slice(&block);
            }
        }
        wire
    }

    #[test]
    fn records_and_a_block_from_one_call_match_the_push_path_octet_for_octet() {
        // The sibling test above pins the case where a record arrives *without* a block.
        // This one pins the other half of the contract — a single call producing both —
        // and the ordering rule that goes with it: whatever the call recorded belongs on
        // the wire *before* the block that same call returned.
        //
        // The oracle is the *push path*, not this module's own idea of a well-formed
        // stream. Records and blocks are each whole frame sequences, so concatenating them
        // in either order still parses as HTTP/2; a structural check could not tell the two
        // apart. Driving the identical body through the existing copying API produces the
        // octets libnghttp2 alone chose, with no ordering decision of ours in them, and
        // no-copy is only correct if it reproduces them exactly. The negative control at
        // the end proves this oracle can in fact see the difference.
        // Several frames, but comfortably inside the initial 65535-octet connection window
        // so that the *other* stream still gets a turn in the same pass — that turn is what
        // produces a block alongside these records.
        let body = Bytes::from((0..32 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());

        // The second stream keeps its *push* body in both sessions, and that is what makes
        // the interesting shape happen. After serialising a no-copy `DATA` libnghttp2
        // breaks to the next queued item; if that item copies, its octets land in the
        // buffer and the call returns them as a block. So a mixed connection — one shared
        // body, one push body — is the case where records and a block arrive together.
        let second = Bytes::from((0..48 * 1024).map(|i| (i % 241) as u8).collect::<Vec<u8>>());

        let mut push = SessionBuilder::<()>::client().build().unwrap();
        push.submit_request_with_body(&request_headers(), BytesBody::new(body.to_vec()))
            .unwrap();
        push.submit_request_with_body(&request_headers(), BytesBody::new(second.to_vec()))
            .unwrap();
        let mut push_wire = Vec::new();
        while let Some(block) = push.send(&mut ()).expect("push send failed") {
            push_wire.extend_from_slice(block);
        }

        let mut shared = SessionBuilder::<()>::client().build().unwrap();
        shared
            .submit_request_with_shared_body(&request_headers(), Chunker::new(body.clone()))
            .unwrap();
        shared
            .submit_request_with_body(&request_headers(), BytesBody::new(second.to_vec()))
            .unwrap();
        let emissions = drive(&mut shared, &mut ());

        assert!(
            emissions
                .iter()
                .any(|e| !e.records.is_empty() && e.block.is_some()),
            "this workload must exercise a call that produces records *and* a block; \
             without one the ordering rule is never put to the test"
        );

        assert_eq!(
            flatten(&emissions, false),
            push_wire,
            "draining records ahead of the same call's block must reproduce the push path's \
             octets exactly — no-copy changes who writes the payload, not what goes on the \
             wire"
        );

        // Negative control: without this, passing the assertion above would prove nothing
        // about ordering, only that the same octets appear somewhere.
        assert_ne!(
            flatten(&emissions, true),
            push_wire,
            "writing a call's block ahead of its records must corrupt the stream; if it \
             does not, the assertion above is not testing ordering at all"
        );

        // And the shared body really did span several frames, so there was ordering to get
        // wrong in the first place.
        assert!(
            all_records(&emissions).len() >= 2,
            "expected a multi-frame shared body, got {} records",
            all_records(&emissions).len()
        );
    }

    #[test]
    fn multi_chunk_records_arrive_in_order_and_byte_identical() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![
                    SharedOutcome::Wrote(Bytes::from_static(b"Hello, ")),
                    SharedOutcome::Wrote(Bytes::from_static(b"world")),
                    SharedOutcome::Eof(Bytes::from_static(b"!")),
                ]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut ());
        let records = all_records(&emissions);
        assert_eq!(records.len(), 3, "each chunk is one frame is one record");

        let joined: Vec<u8> = records
            .iter()
            .flat_map(|r| r.payload.iter().copied())
            .collect();
        assert_eq!(joined, b"Hello, world!", "payloads concatenate in order");

        let flags: Vec<u8> = records
            .iter()
            .map(|r| parse_data_header(&r.header).2)
            .collect();
        assert!(
            flags[..flags.len() - 1].iter().all(|f| f & 0x1 == 0),
            "only the last frame ends the stream"
        );
        assert_eq!(
            flags.last().copied().unwrap() & 0x1,
            0x1,
            "the last frame ends the stream"
        );
    }

    #[test]
    fn a_deferring_shared_body_produces_no_record_until_resumed() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let stream = session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![
                    SharedOutcome::Defer,
                    SharedOutcome::Eof(Bytes::from_static(b"later")),
                ]),
            )
            .unwrap();

        let first = drive(&mut session, &mut ());
        assert!(
            all_records(&first).is_empty(),
            "a deferred body stages nothing and emits no frame"
        );

        session.resume_body(stream).expect("resuming the stream");
        let second = drive(&mut session, &mut ());
        let records = all_records(&second);
        assert_eq!(
            records.len(),
            1,
            "resuming lets the body be consulted again"
        );
        assert_eq!(&records[0].payload[..], b"later");
    }

    #[test]
    fn an_empty_shared_body_emits_a_header_only_end_of_stream_record() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Eof(Bytes::new())]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut ());
        let records = all_records(&emissions);
        assert_eq!(
            records.len(),
            1,
            "one empty DATA frame is unavoidable — it carries end-of-stream"
        );

        let (len, kind, flags, _) = parse_data_header(&records[0].header);
        assert_eq!(kind, 0x0, "a DATA frame");
        assert_eq!(len, 0, "a header-only frame");
        assert_eq!(flags & 0x1, 0x1, "END_STREAM");
        assert!(records[0].payload.is_empty(), "no payload to hand over");
    }

    #[test]
    fn a_failing_shared_body_resets_the_stream_and_surfaces_the_error() {
        let mut context = Closed::default();
        let mut session = closing_client().build().unwrap();
        let stream = session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Fail(Box::new(BodyFault(
                    "the disk caught fire",
                )))]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut context);
        assert!(
            all_records(&emissions).is_empty(),
            "a failed body stages no chunk and records no frame"
        );
        assert!(
            context.closed.iter().any(|(s, _, e)| {
                *s == stream.get()
                    && e.as_deref()
                        .map(|m| m.contains("the disk caught fire"))
                        .unwrap_or(false)
            }),
            "the failure is reported to the stream-close handler, got {:?}",
            context.closed
        );
    }

    #[test]
    fn an_overlong_shared_chunk_resets_the_stream() {
        let mut context = Closed::default();
        let mut session = closing_client().build().unwrap();
        // 20_000 octets exceeds the 16_384 default maximum frame size that bounds the first
        // frame's limit, so the source over-produces and the stream is reset rather than
        // the header claiming a length the window never granted.
        let stream = session
            .submit_request_with_shared_body(
                &request_headers(),
                Scripted::new(vec![SharedOutcome::Eof(Bytes::from(vec![b'x'; 20_000]))]),
            )
            .unwrap();

        let emissions = drive(&mut session, &mut context);
        assert!(
            all_records(&emissions).is_empty(),
            "an over-limit chunk is never recorded"
        );
        assert!(
            context.closed.iter().any(|(s, _, e)| {
                *s == stream.get()
                    && e.as_deref()
                        .map(|m| m.contains("handed over"))
                        .unwrap_or(false)
            }),
            "the over-production is reported to the stream-close handler, got {:?}",
            context.closed
        );
    }

    #[test]
    fn an_idle_session_leaves_the_sink_empty() {
        let mut session = SessionBuilder::<()>::client().build().unwrap();
        let _ = drive(&mut session, &mut ());

        let mut sink: Vec<SendRecord> = Vec::new();
        let block = session.send_into(&mut (), &mut sink).unwrap();
        assert!(block.is_none(), "an idle session reports nothing pending");
        assert!(sink.is_empty(), "and deposits no record");
    }

    // [R4] The cancellation window (research/nghttp2-no-copy.md §7).
    #[test]
    fn a_staged_chunk_is_released_exactly_once_across_the_cancellation_window() {
        use std::sync::Arc;

        use crate::state::BodyEntry;

        // libnghttp2 may pack a no-copy frame — which stages a chunk in the entry — and
        // then reset that item WITHOUT invoking `send_data`, if the stream closed between
        // pack and send. That C-level reset (NGHTTP2_OB_SEND_NO_COPY with a now-closed
        // stream) is only reachable across a `mem_send2` boundary, which this design never
        // crosses because `send_data` never returns `WOULDBLOCK`; it therefore cannot be
        // driven through the public send loop. What the window demands of this crate is a
        // lifecycle guarantee — a staged chunk that no `send_data` ever collects is
        // released exactly once — and that is asserted directly here on the `BodyEntry`
        // that owns the staging slot.

        struct Idle;
        impl SharedBodySource for Idle {
            fn take(&mut self, _limit: usize) -> SharedOutcome {
                SharedOutcome::Defer
            }
        }

        // A refcounted owner behind each staged `Bytes` makes the release observable: the
        // strong count returns to one exactly when the chunk is freed. `Bytes::from_owner`
        // shares the owner across clones, so the count tracks distinct staged chunks, not
        // clones of one.
        struct ArcOwner(Arc<Vec<u8>>);
        impl AsRef<[u8]> for ArcOwner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        let owner = Arc::new(b"staged-but-never-sent".to_vec());
        assert_eq!(Arc::strong_count(&owner), 1);

        let mut entry = BodyEntry::new_shared(Box::new(Idle));

        // Stage a chunk, as `read_shared_body` would.
        entry.staged = Some(Bytes::from_owner(ArcOwner(owner.clone())));
        assert_eq!(
            Arc::strong_count(&owner),
            2,
            "the staged chunk holds the owner"
        );

        // Overwrite path: the next pack stages a new chunk, releasing the un-sent one.
        entry.staged = Some(Bytes::from_static(b"next"));
        assert_eq!(
            Arc::strong_count(&owner),
            1,
            "overwriting the staging slot releases the un-sent chunk"
        );

        // Drop-at-close path: stage again, then free the entry as stream close would.
        entry.staged = Some(Bytes::from_owner(ArcOwner(owner.clone())));
        assert_eq!(Arc::strong_count(&owner), 2, "re-staged");
        drop(entry);
        assert_eq!(
            Arc::strong_count(&owner),
            1,
            "dropping the entry at stream close releases the un-sent chunk"
        );
    }
}
