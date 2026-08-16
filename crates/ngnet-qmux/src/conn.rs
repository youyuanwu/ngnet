//! The connection: ownership, construction, and the read path.
//!
//! # What is boxed, and why
//!
//! dwnx copies the callbacks, settings and transport parameters it is given, so none of those
//! need outlive the constructor. What it does retain by address is `user_data`, which this
//! crate uses for the callback bridge. That has to stay at a fixed address for the life of the
//! connection even if the `Conn` value itself is moved, so the [`BridgeSlot`] is boxed.
//!
//! The handlers and scratch state are boxed for the same reason: the bridge stores raw
//! pointers to them for the duration of each entry point, and a move between entry points must
//! not invalidate what the next one installs.
//!
//! # Drop
//!
//! `dwnx_conn_del` runs first, while the boxes it might touch are still alive, and the pointer
//! is then nulled so a second drop cannot free twice.

use ngnet_qmux_sys as sys;

use core::ptr;

use crate::callbacks::{self, BridgeGuard, BridgeSlot, Scratch};
use crate::error::{Error, ErrorKind};
use crate::handlers::Handlers;
use crate::params::TransportParams;
use crate::settings::Settings;
use crate::time::Timestamp;

/// Which side of the connection this is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// The client, which opens the underlying transport.
    Client,
    /// The server, which accepts it.
    Server,
}

/// What submitting inbound bytes produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadOutcome {
    /// The bytes were processed. Any events they implied have already been delivered.
    Processed,
    /// The peer has closed the connection.
    ///
    /// Not a failure: the connection is finished, and this is how a well-behaved shutdown
    /// arrives. dwnx reports it as `DWNX_ERR_DRAINING`, which is easy to mistake for an error
    /// when it is really an end-of-life notification.
    PeerClosed,
}

/// Builds a [`Conn`].
pub struct ConnBuilder<'h> {
    role: Role,
    settings: Settings,
    params: TransportParams,
    handlers: Handlers<'h>,
}

impl<'h> ConnBuilder<'h> {
    /// Start building a connection in the given role.
    #[must_use]
    pub fn new(role: Role) -> Self {
        Self {
            role,
            settings: Settings::new(),
            params: TransportParams::new(),
            handlers: Handlers::new(),
        }
    }

    /// Use these settings.
    #[must_use]
    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Advertise these transport parameters.
    ///
    /// Note that dwnx's defaults permit no application data at all; see [`TransportParams`].
    #[must_use]
    pub fn transport_params(mut self, params: TransportParams) -> Self {
        self.params = params;
        self
    }

    /// React to protocol events with these handlers.
    #[must_use]
    pub fn handlers(mut self, handlers: Handlers<'h>) -> Self {
        self.handlers = handlers;
        self
    }

    /// Construct the connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport parameters would violate one of dwnx's own
    /// preconditions -- which it guards with `assert`, so passing them through would abort the
    /// process -- or if dwnx fails to allocate.
    pub fn build(self) -> Result<Conn<'h>, Error> {
        // Checked before the call, not after: these are assertions in C, not error returns.
        self.params.validate()?;

        let mut slot = Box::new(BridgeSlot::new());
        let mut handlers = Box::new(self.handlers);
        let mut scratch = Box::new(Scratch::default());

        let callbacks = callbacks::callbacks();
        let mut raw: *mut sys::dwnx_conn = ptr::null_mut();

        let user_data: *mut BridgeSlot = &mut *slot;
        let constructor = match self.role {
            Role::Client => sys::dwnx_conn_client_new,
            Role::Server => sys::dwnx_conn_server_new,
        };

        // SAFETY: every pointer is valid for the call. dwnx copies `callbacks`, `settings` and
        // `params`, so they need not outlive it; `user_data` is the boxed slot, which lives as
        // long as the `Conn` built below. A null `mem` selects dwnx's default allocator.
        let rv = unsafe {
            constructor(
                &mut raw,
                &callbacks,
                self.settings.as_raw(),
                self.params.as_raw(),
                ptr::null(),
                user_data.cast(),
            )
        };

        if rv != 0 {
            // dwnx returns NOBUF rather than the documented NOMEM when the connection
            // allocation fails; mapping what the code does rather than what the header says.
            return Err(Error::from_native(rv, "constructing a connection"));
        }
        if raw.is_null() {
            return Err(Error::validation(
                ErrorKind::Internal,
                "dwnx reported success but returned no connection",
            ));
        }

        // Take the pointers only after the connection exists, so the boxes are not left
        // dangling if construction failed.
        let _ = &mut handlers;
        let _ = &mut scratch;

        Ok(Conn {
            raw,
            role: self.role,
            slot,
            handlers,
            scratch,
        })
    }
}

/// A QMux connection.
///
/// Sans-I/O: this owns protocol state and nothing else. Bytes come in through [`Conn::read`]
/// and go out through the write path; how they reach the network is the caller's business.
pub struct Conn<'h> {
    /// The C connection. Nulled by `Drop` so a double free is impossible.
    raw: *mut sys::dwnx_conn,
    role: Role,
    /// Boxed because dwnx holds its address as `user_data` for the life of the connection.
    slot: Box<BridgeSlot>,
    /// Boxed because the bridge stores a raw pointer to it across each entry point.
    handlers: Box<Handlers<'h>>,
    scratch: Box<Scratch>,
}

impl<'h> Conn<'h> {
    /// Start building a connection.
    #[must_use]
    pub fn builder(role: Role) -> ConnBuilder<'h> {
        ConnBuilder::new(role)
    }

    /// Which side this is.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Whether this is the server, as dwnx reports it.
    #[must_use]
    pub fn is_server(&self) -> bool {
        // SAFETY: `raw` is non-null for the life of this value.
        unsafe { sys::dwnx_conn_is_server(self.raw) != 0 }
    }

    /// The peer's transport parameters, once they have arrived.
    ///
    /// `None` until the peer's `QX_TRANSPORT_PARAMETERS` frame has been read. dwnx exposes no
    /// getter for these -- `dwnx_conn_get_local_transport_params` returns the local set -- so
    /// this is a copy taken when the callback fired. If dwnx grows a real getter, this can
    /// forward to it instead.
    #[must_use]
    pub fn peer_transport_params(&self) -> Option<&TransportParams> {
        self.scratch.peer_params.as_ref()
    }

    /// The transport parameters this endpoint advertised.
    ///
    /// Not necessarily the ones that were configured: dwnx overwrites `max_record_size` at
    /// construction. This reports what the connection actually uses.
    #[must_use]
    pub fn local_transport_params(&self) -> TransportParams {
        // SAFETY: `raw` is non-null, and dwnx returns a pointer to storage it owns for the
        // life of the connection. The value is copied out immediately.
        unsafe {
            let params = sys::dwnx_conn_get_local_transport_params(self.raw);
            TransportParams::from_native(ptr::read(params))
        }
    }

    /// The timestamp of the most recent operation, as dwnx recorded it.
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        // SAFETY: `raw` is non-null for the life of this value.
        Timestamp::from_nanos(unsafe { sys::dwnx_conn_get_timestamp(self.raw) })
    }

    /// Submit inbound protocol bytes.
    ///
    /// Records may be split across calls in any way; dwnx buffers a partial record and resumes
    /// when the rest arrives. Any protocol events the bytes imply are delivered to the
    /// handlers before this returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes violate the protocol, or if a handler reported failure --
    /// in which case the handler's own message is preserved.
    pub fn read(&mut self, data: &[u8], now: Timestamp) -> Result<ReadOutcome, Error> {
        let rv = self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null, and `data` is valid for `len` bytes for the duration
            // of the call. dwnx does not retain it.
            unsafe { sys::dwnx_conn_read(raw, data.as_ptr(), data.len(), now.as_nanos()) }
        });

        match rv {
            0 => Ok(ReadOutcome::Processed),
            // Not a failure: the peer sent CONNECTION_CLOSE and the connection is finished.
            sys::DWNX_ERR_DRAINING => Ok(ReadOutcome::PeerClosed),
            rv => Err(self.error_from(rv, "reading inbound bytes")),
        }
    }

    /// Run a dwnx entry point with the callback bridge installed.
    ///
    /// Every operation that can fire a callback goes through here, so that handlers are
    /// reachable exactly while C might call them and not a moment longer. The closure gets the
    /// raw connection and nothing else; anything a callback recorded is read from `scratch`
    /// afterwards, once the guard has released its borrow.
    pub(crate) fn with_bridge<T>(&mut self, f: impl FnOnce(*mut sys::dwnx_conn) -> T) -> T {
        // Cleared before each entry point so a stale error from a previous call cannot be
        // mistaken for this one's.
        self.scratch.handler_error = None;

        let raw = self.raw;
        let guard = BridgeGuard::new(&self.slot, &mut self.handlers, &mut self.scratch);
        let result = f(raw);
        drop(guard);
        result
    }

    /// Build an error, attaching a handler's own message when C reported a callback failure.
    pub(crate) fn error_from(&mut self, rv: i32, context: &'static str) -> Error {
        if rv == sys::DWNX_ERR_CALLBACK_FAILURE
            && let Some(handler_error) = self.scratch.handler_error.take()
        {
            return Error::from_native(rv, handler_error.message());
        }
        Error::from_native(rv, context)
    }

    /// The raw connection, for the operations implemented in sibling modules.
    pub(crate) const fn raw(&self) -> *mut sys::dwnx_conn {
        self.raw
    }
}

// Written out rather than derived: the handlers are boxed closures, which have no useful
// representation, and the raw pointer's value is noise. What a reader wants is the role and
// whether the peer's parameters have arrived.
impl core::fmt::Debug for Conn<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Conn")
            .field("role", &self.role)
            .field("peer_params", &self.scratch.peer_params.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        if self.raw.is_null() {
            return;
        }
        // SAFETY: `raw` was produced by a dwnx constructor and has not been freed. It is freed
        // before the boxes it holds pointers to, and nulled afterwards so this cannot run
        // twice on the same pointer.
        unsafe { sys::dwnx_conn_del(self.raw) };
        self.raw = ptr::null_mut();
    }
}

// A connection may move between threads: dwnx keeps no thread-local state, and everything the
// bridge touches is owned here. It is deliberately not `Sync` -- the bridge slot is written on
// every entry point without synchronisation, which is sound only because every entry point
// takes `&mut self`.
//
// SAFETY: see above.
#[allow(unsafe_code)]
unsafe impl Send for Conn<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn workable_params() -> TransportParams {
        TransportParams::new().with_all_limits(1 << 20, 16)
    }

    #[test]
    fn builds_in_both_roles() {
        for role in [Role::Client, Role::Server] {
            let conn = Conn::builder(role)
                .transport_params(workable_params())
                .build()
                .unwrap();
            assert_eq!(conn.role(), role);
            assert_eq!(conn.is_server(), role == Role::Server);
        }
    }

    /// The C defaults construct fine, even though they permit no data.
    #[test]
    fn builds_from_unmodified_defaults() {
        Conn::builder(Role::Client).build().unwrap();
    }

    /// Parameters that would trip a C assertion are rejected before reaching it.
    #[test]
    fn rejects_parameters_dwnx_would_assert_on() {
        let bad = TransportParams::new().with_initial_max_data(u64::MAX);
        let error = Conn::builder(Role::Client)
            .transport_params(bad)
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.native().is_none(), "should not have reached dwnx");
    }

    /// dwnx overwrites max_record_size, so readback differs from what was configured.
    #[test]
    fn max_record_size_is_overwritten_by_the_library() {
        let params = workable_params();
        let conn = Conn::builder(Role::Client)
            .transport_params(params)
            .build()
            .unwrap();
        assert_eq!(
            conn.local_transport_params().max_record_size(),
            u64::from(sys::DWNX_DEFAULT_MAX_RECORD_SIZE)
        );
    }

    #[test]
    fn peer_params_are_absent_before_exchange() {
        let conn = Conn::builder(Role::Client)
            .transport_params(workable_params())
            .build()
            .unwrap();
        assert!(conn.peer_transport_params().is_none());
    }

    /// Construction and drop with no handlers at all, repeated, to shake out a double free.
    #[test]
    fn construct_and_drop_repeatedly() {
        for _ in 0..64 {
            let conn = Conn::builder(Role::Server)
                .transport_params(workable_params())
                .build()
                .unwrap();
            drop(conn);
        }
    }
}
