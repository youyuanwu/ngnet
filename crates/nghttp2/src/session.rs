//! HTTP/2 sessions: construction, teardown, and the outbound half of the sans-I/O loop.

use std::sync::Arc;
use core::marker::PhantomData;

use nghttp2_sys as sys;

use crate::alloc_state::{AllocState, mem_for};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::settings::Setting;

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
pub struct SessionBuilder<C> {
    role: Role,
    settings: Vec<Setting>,
    manual_flow_control: bool,
    _context: PhantomData<fn(&mut C)>,
}

impl<C> SessionBuilder<C> {
    fn with_role(role: Role) -> Self {
        Self {
            role,
            settings: Vec::new(),
            manual_flow_control: false,
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

    /// Builds the session and queues its initial `SETTINGS` frame.
    pub fn build(self) -> Result<Session<C>> {
        let allocation = Arc::new(AllocState::default());
        let mut mem = mem_for(&allocation);

        let mut options = Options::new()?;
        options.set_no_auto_window_update(self.manual_flow_control);

        let callbacks = Callbacks::new()?;

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
            _context: PhantomData,
        };

        session.submit_settings(&self.settings)?;
        Ok(session)
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
        // libnghttp2 copies every entry, so it need not outlive this call. A null pointer
        // with length zero is what the library expects for an empty settings list.
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
        // Handlers arrive in a later phase; the context is threaded through the same
        // bridge that the receive half will install.
        let _ = context;

        let mut data: *const u8 = core::ptr::null();
        // SAFETY: `self.raw` is live and `data` is a valid out-parameter.
        let len = unsafe { sys::nghttp2_session_mem_send2(self.raw, &mut data) };

        if len < 0 {
            return Err(Error::from_native(
                "nghttp2_session_mem_send2",
                len as i32,
            ));
        }
        if len == 0 {
            return Ok(None);
        }
        debug_assert!(!data.is_null());

        // SAFETY: libnghttp2 returned a non-null pointer to `len` initialised bytes. The
        // slice borrows `self`, so it cannot outlive the next call that would invalidate
        // it.
        let bytes = unsafe { core::slice::from_raw_parts(data, len as usize) };
        Ok(Some(bytes))
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
