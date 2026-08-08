//! The TLS backend seam.
//!
//! QUIC and TLS are inseparable: the transport cannot send a packet before the handshake
//! has produced keys, and the handshake's own messages travel inside QUIC packets. ngtcp2
//! does not implement TLS. It expects the application to supply a TLS stack, and ships
//! *crypto helper* libraries that bridge to a specific one.
//!
//! This module is the seam that keeps the choice open. [`TlsBackend`] describes what a
//! connection needs from a TLS stack; [`crate::tls_ossl`] implements it over OpenSSL, and is
//! enabled by default.
//!
//! # Why the trait supplies callbacks and not just a handle
//!
//! It would be tidier for the trait to hand back an opaque handle and let the connection
//! install a fixed set of `ngtcp2_callbacks`. That does not work, for two reasons.
//!
//! Each backend compiles **its own copy** of ngtcp2's shared crypto code, with
//! backend-specific implementations behind identically-named symbols — `ngtcp2_crypto_ctx_tls`
//! exists separately in `ossl.c`, `wolfssl.c` and `gnutls.c`. The callback set is therefore
//! part of the backend, not something a generic connection can name.
//!
//! And those symbols do not always exist. `ngnet-quic-sys` includes the crypto headers only
//! when a backend feature is on (`wrapper.h:10-18`), so with `--no-default-features` there
//! is no `ngtcp2_crypto_*` symbol in the bindings at all. Code naming them directly would
//! not compile. Routing them through the trait is what lets the crate build with no TLS
//! stack, exposing the seam and nothing behind it.
//!
//! # Why the trait is `unsafe`
//!
//! Implementing it means promising things the compiler cannot check: that the native handle
//! stays valid for as long as the connection holds it, and that the objects behind it are
//! destroyed in the order the C library requires. Callers of a [`crate::Conn`] remain
//! entirely safe; only writing a *new backend* requires care.

// The accessors are consumed by the connection, which installs the handle and binds the
// conn ref. The seam is defined here first because the backend has to implement it before
// there is a connection to hold one.
#![allow(dead_code)]

use core::ffi::c_void;

use crate::error::Result;

/// The role an endpoint plays in the handshake.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    /// Initiates the connection.
    Client,
    /// Accepts it.
    Server,
}

impl Role {
    /// Whether this is the server role.
    pub const fn is_server(self) -> bool {
        matches!(self, Role::Server)
    }
}

/// The pointer a TLS backend gives ngtcp2 to represent its session.
///
/// A newtype rather than a bare `*mut c_void`, because the underlying API takes `void *`
/// and the value it wants is **not** the one an experienced OpenSSL user would reach for.
/// For the ossl backend it is the `ngtcp2_crypto_ossl_ctx *`, not the `SSL *`
/// (`deps/ngtcp2/examples/tls_session_base_ossl.cc:50-52`). Passing the `SSL *` compiles
/// cleanly and corrupts memory at run time.
///
/// Wrapping it means only a backend can produce one, so that mistake is not expressible
/// outside the backend that knows which is which.
#[derive(Clone, Copy)]
pub struct NativeTlsHandle(*mut c_void);

impl NativeTlsHandle {
    /// Wraps a pointer for ngtcp2.
    ///
    /// # Safety
    ///
    /// The pointer must be the one ngtcp2's crypto helper expects for this backend, and
    /// must remain valid for as long as the connection holds it.
    pub const unsafe fn new(ptr: *mut c_void) -> Self {
        Self(ptr)
    }

    /// The wrapped pointer.
    pub(crate) const fn as_ptr(self) -> *mut c_void {
        self.0
    }
}

impl core::fmt::Debug for NativeTlsHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "NativeTlsHandle({:p})", self.0)
    }
}

/// A TLS session bound to one connection.
///
/// # Safety
///
/// Implementors must guarantee:
///
/// - [`TlsSession::native_handle`] returns a pointer ngtcp2's crypto helper for this
///   backend accepts, valid for as long as `self` lives.
/// - The `ngtcp2_crypto_conn_ref` reachable from the TLS object outlives the TLS object
///   itself, and the objects are torn down in whatever order the backend's helper requires.
///   For OpenSSL that order is `SSL_set_app_data(ssl, NULL)`, then `SSL_free`, then
///   `ngtcp2_crypto_ossl_ctx_del` — and getting it wrong is a use-after-free, not a leak.
/// - [`TlsSession::install_callbacks`] fills only the crypto-related entries of the
///   callback struct, leaving the transport entries for the connection to set.
pub unsafe trait TlsSession {
    /// The handle to give `ngtcp2_conn_set_tls_native_handle`.
    fn native_handle(&self) -> NativeTlsHandle;

    /// Fills in the crypto half of ngtcp2's callback table.
    ///
    /// # Safety
    ///
    /// `callbacks` must point to a valid, writable callback struct. The implementation must
    /// write only the crypto-related entries.
    unsafe fn install_callbacks(&self, callbacks: *mut c_void);

    /// The application protocol the handshake negotiated, once it has completed.
    ///
    /// Returns `None` before the handshake finishes, or if no protocol was agreed.
    fn negotiated_alpn(&self) -> Option<Vec<u8>>;

    /// Why the handshake failed, in whatever terms the backend can offer.
    ///
    /// Used to turn a bare failure into something that names ALPN or certificate
    /// verification rather than saying only that the handshake did not complete.
    fn failure_reason(&self) -> Option<String> {
        None
    }
}

/// A configured TLS stack, from which per-connection sessions are made.
///
/// # Safety
///
/// Implementors must guarantee that sessions produced by [`TlsBackend::new_session`] remain
/// valid independently of one another, and that dropping the backend after its sessions is
/// sound.
pub unsafe trait TlsBackend {
    /// The session type this backend produces.
    type Session: TlsSession;

    /// Creates a session for one connection.
    ///
    /// `server_name` is used for SNI and certificate verification on the client side, and
    /// ignored by a server.
    fn new_session(&self, role: Role, server_name: Option<&str>) -> Result<Self::Session>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_role_knows_whether_it_is_a_server() {
        assert!(Role::Server.is_server());
        assert!(!Role::Client.is_server());
    }

    #[test]
    fn a_native_handle_carries_its_pointer() {
        let mut value = 0u8;
        let ptr: *mut c_void = (&raw mut value).cast();
        // SAFETY: not given to ngtcp2; only the wrapper's own accessor is exercised.
        let handle = unsafe { NativeTlsHandle::new(ptr) };
        assert_eq!(handle.as_ptr(), ptr);
    }
}
