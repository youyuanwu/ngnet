//! HTTP/2 sessions: construction, teardown, and the outbound half of the sans-I/O loop.

use std::sync::Arc;
use core::fmt;
use core::marker::PhantomData;

use nghttp2_sys as sys;

use crate::alloc_state::{AllocState, mem_for};
use crate::callbacks::{self, Bridge};
use crate::error::{Error, ErrorCode, ErrorKind, Result};
use crate::handlers::{HeaderAction, Handlers};
use crate::header::{self, Header};
use crate::options::Options;
use crate::settings::Setting;
use crate::body::{BodyError, BodySource};
use crate::state::{BodyEntry, BodyRegistry, PendingErrors, ResponseGuard};
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
        let mut data: *const u8 = core::ptr::null();

        let len = self.with_context(context, |raw| {
            // SAFETY: `raw` is live and `data` is a valid out-parameter. Handlers may run
            // inside this call, which is why it goes through the bridge.
            unsafe { sys::nghttp2_session_mem_send2(raw, &mut data) }
        });

        if len < 0 {
            return Err(Error::from_native("nghttp2_session_mem_send2", len as i32));
        }
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
        Ok(consumed as usize)
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
        };

        // Restores the session's user data however this scope is left, so a panic
        // escaping the call cannot leave libnghttp2 holding a pointer to a dead bridge.
        let _guard = UserDataGuard { raw };

        // SAFETY: `raw` is live, and `bridge` outlives the call below because it is a
        // local of this frame and `call` returns before it is dropped.
        unsafe {
            sys::nghttp2_session_set_user_data(
                raw,
                (&raw mut bridge).cast::<core::ffi::c_void>(),
            );
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
        let track = self.stream_exists(stream);

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

    /// Whether `stream` is currently open on this session.
    ///
    /// Uses the half-closed predicate, which returns -1 exactly when no such stream
    /// exists and 0 or 1 otherwise. A window-size query would be the obvious probe but is
    /// wrong: a stream's local window legitimately goes negative when the local initial
    /// window size is reduced while data is in flight, so a negative result there does
    /// not mean the stream is absent.
    fn stream_exists(&self, stream: StreamId) -> bool {
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
        let rc = unsafe {
            sys::nghttp2_submit_trailer(self.raw, stream.get(), nva.as_ptr(), nva.len())
        };

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
        if !self.stream_exists(stream) {
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

        assert!(!wire.is_empty(), "a server must still announce its SETTINGS");
        assert!(!wire.starts_with(CLIENT_MAGIC), "only clients send the preface");
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
            entries.iter().filter(|(id, _)| *id == ENABLE_PUSH_ID).count(),
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
            entries.iter().filter(|(id, _)| *id == ENABLE_PUSH_ID).count(),
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
        assert!(session.want_read(), "a fresh session expects the peer's SETTINGS");
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
