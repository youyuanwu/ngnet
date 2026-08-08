//! The OpenSSL TLS backend.
//!
//! Implements [`crate::tls::TlsBackend`] over OpenSSL 3.5's QUIC TLS API, using ngtcp2's
//! `ngtcp2_crypto_ossl` helper. Enabled by the default-on `tls-ossl` feature.
//!
//! # The teardown rule, which is the whole difficulty
//!
//! Three C objects are involved in one connection's TLS: an `SSL`, an
//! `ngtcp2_crypto_ossl_ctx` that wraps it, and an `ngtcp2_crypto_conn_ref` that OpenSSL
//! holds as the `SSL`'s application data. They refer to each other in a cycle, and they must
//! be destroyed in exactly this order:
//!
//! ```text
//! SSL_set_app_data(ssl, NULL)  →  SSL_free(ssl)  →  ngtcp2_crypto_ossl_ctx_del(ctx)
//! ```
//!
//! (`deps/ngtcp2/examples/tls_session_base_ossl.cc:39-48`.)
//!
//! Every reason for that ordering is a use-after-free if it is broken. `SSL_free` releases
//! outstanding CRYPTO records, which calls back into `ossl_crypto_release_rcd`
//! (`deps/ngtcp2/crypto/ossl/ossl.c:1191`); that reads the app data, calls
//! `conn_ref->get_conn(conn_ref)`, dereferences the `ngtcp2_conn`, and writes through the
//! ossl ctx. Clearing the app data first makes it return early — the helper's own comment at
//! `ossl.c:1196-1200` says that is precisely why the escape hatch exists. And
//! `ngtcp2_crypto_ossl_ctx_del` frees a `remote_params` buffer OpenSSL may still be
//! borrowing (`ossl.c:1018-1039`), so it must come last.
//!
//! Rust drops struct fields in declaration order, which would be a silent, invisible
//! dependency on field ordering. So [`OsslSession`] owns all three and implements [`Drop`] by
//! hand, and the parts are never exposed as independently droppable values.
//!
//! # `ngtcp2_crypto_ossl_init` is process-global
//!
//! It prefetches static `EVP_*` objects into globals, with no reference counting
//! (`ossl.c:49-60`, `:62`, `:82`). The ngtcp2 examples pair it with a per-context
//! destructor, which means that with two TLS contexts, destroying the second frees objects
//! the first is still using. This crate calls `init` once behind a [`Once`] and **never**
//! calls `ngtcp2_crypto_ossl_free`: a bounded one-off leak is the correct trade against
//! corrupting a live connection.

// `bind_conn_ref` and the `conn_ref` field are wired up by the connection; the drop-order
// tests below already exercise them.
#![allow(dead_code)]

use core::ffi::{CStr, c_char, c_int, c_uchar, c_void};
use core::ptr;
use std::sync::Once;

use ngnet_quic_sys as sys;

use crate::error::{Error, ErrorKind, Result};
use crate::tls::{NativeTlsHandle, Role, TlsBackend, TlsSession};

/// `SSL_set_app_data` / `SSL_get_app_data` are macros over ex-data index 0.
const APP_DATA_INDEX: c_int = 0;

/// The `SSL_ctrl` command behind the `SSL_set_tlsext_host_name` macro.
const CTRL_SET_TLSEXT_HOSTNAME: c_int = sys::SSL_CTRL_SET_TLSEXT_HOSTNAME as c_int;

/// The `TLSEXT_NAMETYPE_host_name` argument to that command.
const NAMETYPE_HOST_NAME: core::ffi::c_long = sys::TLSEXT_NAMETYPE_host_name as core::ffi::c_long;

/// Runs `ngtcp2_crypto_ossl_init` exactly once per process.
fn ensure_ossl_init() -> Result<()> {
    static INIT: Once = Once::new();
    static mut RESULT: c_int = 0;

    INIT.call_once(|| {
        // SAFETY: called once, before any other crypto helper entry point.
        let rc = unsafe { sys::ngtcp2_crypto_ossl_init() };
        // SAFETY: written inside `call_once`, which synchronises with every later read.
        unsafe { RESULT = rc };
    });

    // SAFETY: `call_once` has returned, so the write above happened-before this read.
    let rc = unsafe { RESULT };
    if rc != 0 {
        return Err(Error::native(rc, "ngtcp2_crypto_ossl_init failed"));
    }
    Ok(())
}

/// Reads OpenSSL's error queue into a string, and clears it.
fn take_openssl_error() -> Option<String> {
    let mut messages: Vec<String> = Vec::new();
    loop {
        // SAFETY: reading the thread-local error queue has no preconditions.
        let code = unsafe { sys::ERR_get_error() };
        if code == 0 {
            break;
        }
        let mut buf = [0i8; 256];
        // SAFETY: `buf` is a valid writable buffer of the length given.
        unsafe { sys::ERR_error_string_n(code, buf.as_mut_ptr().cast::<c_char>(), buf.len()) };
        // SAFETY: `ERR_error_string_n` always NUL-terminates within the buffer.
        let text = unsafe { CStr::from_ptr(buf.as_ptr().cast::<c_char>()) };
        messages.push(text.to_string_lossy().into_owned());
    }
    if messages.is_empty() {
        None
    } else {
        Some(messages.join("; "))
    }
}

/// Encodes ALPN protocols into OpenSSL's length-prefixed wire form.
fn encode_alpn(protocols: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for protocol in protocols {
        if protocol.is_empty() || protocol.len() > 255 {
            return Err(Error::invalid_input(
                "an ALPN protocol must be between 1 and 255 bytes",
            ));
        }
        encoded.push(protocol.len() as u8);
        encoded.extend_from_slice(protocol);
    }
    Ok(encoded)
}

/// How a peer's certificate should be checked.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Verify {
    /// Verify the peer against the configured trust anchors. The default.
    #[default]
    Peer,
    /// Do not verify the peer at all.
    ///
    /// Named this way on purpose. There is no `Verify::None`, because "none" reads as an
    /// absence of configuration rather than as a decision; this reads as what it is.
    DangerouslyAcceptAnyCertificate,
}

/// Builder for an [`OsslBackend`].
pub struct OsslBackendBuilder {
    role: Role,
    alpn: Vec<Vec<u8>>,
    certificate_chain_pem: Option<Vec<u8>>,
    private_key_pem: Option<Vec<u8>>,
    trust_anchors_pem: Vec<Vec<u8>>,
    use_system_trust_store: bool,
    verify: Verify,
}

impl OsslBackendBuilder {
    /// Starts a configuration for the given role.
    pub fn new(role: Role) -> Self {
        Self {
            role,
            alpn: Vec::new(),
            certificate_chain_pem: None,
            private_key_pem: None,
            trust_anchors_pem: Vec::new(),
            use_system_trust_store: role == Role::Client,
            verify: Verify::Peer,
        }
    }

    /// Adds an ALPN protocol, in preference order.
    ///
    /// QUIC requires ALPN: a handshake with no protocol in common fails. There is no
    /// sensible default, because the protocol depends entirely on what runs over the
    /// connection.
    pub fn alpn(mut self, protocol: impl Into<Vec<u8>>) -> Self {
        self.alpn.push(protocol.into());
        self
    }

    /// Sets the certificate chain, PEM-encoded. Servers must supply one.
    pub fn certificate_chain_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.certificate_chain_pem = Some(pem.into());
        self
    }

    /// Sets the private key, PEM-encoded. Servers must supply one.
    pub fn private_key_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.private_key_pem = Some(pem.into());
        self
    }

    /// Adds a PEM-encoded certificate as a trust anchor.
    pub fn trust_anchor_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.trust_anchors_pem.push(pem.into());
        self
    }

    /// Whether to trust the system certificate store. On by default for clients.
    pub fn use_system_trust_store(mut self, use_it: bool) -> Self {
        self.use_system_trust_store = use_it;
        self
    }

    /// How to verify the peer. Defaults to [`Verify::Peer`].
    pub fn verify(mut self, verify: Verify) -> Self {
        self.verify = verify;
        self
    }

    /// Builds the backend.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the configuration is incomplete — a server
    /// without credentials, or any role without ALPN — and a native error if OpenSSL
    /// rejects the material.
    pub fn build(self) -> Result<OsslBackend> {
        ensure_ossl_init()?;

        if self.alpn.is_empty() {
            return Err(Error::invalid_input(
                "QUIC requires at least one ALPN protocol",
            ));
        }
        if self.role == Role::Server
            && (self.certificate_chain_pem.is_none() || self.private_key_pem.is_none())
        {
            return Err(Error::invalid_input(
                "a server needs both a certificate chain and a private key",
            ));
        }

        let alpn_wire = encode_alpn(&self.alpn)?;

        // SAFETY: `TLS_client_method`/`TLS_server_method` return static method tables.
        let method = unsafe {
            if self.role == Role::Server {
                sys::TLS_server_method()
            } else {
                sys::TLS_client_method()
            }
        };
        // SAFETY: `method` is a valid static method table.
        let raw = unsafe { sys::SSL_CTX_new(method) };
        if raw.is_null() {
            return Err(tls_error("SSL_CTX_new failed"));
        }
        let ctx = SslCtx(raw);

        if let (Some(chain), Some(key)) = (&self.certificate_chain_pem, &self.private_key_pem) {
            load_certificate_chain(ctx.0, chain)?;
            load_private_key(ctx.0, key)?;
            // SAFETY: `ctx` is valid and both credentials have been installed.
            if unsafe { sys::SSL_CTX_check_private_key(ctx.0) } != 1 {
                return Err(tls_error("the private key does not match the certificate"));
            }
        }

        if self.use_system_trust_store {
            // SAFETY: `ctx` is valid.
            unsafe { sys::SSL_CTX_set_default_verify_paths(ctx.0) };
        }
        for anchor in &self.trust_anchors_pem {
            add_trust_anchor(ctx.0, anchor)?;
        }

        let mode = match self.verify {
            Verify::Peer if self.role == Role::Client => sys::SSL_VERIFY_PEER as c_int,
            // A server that demanded a client certificate would reject every ordinary
            // client, so peer verification on the server means "do not require one".
            _ => sys::SSL_VERIFY_NONE as c_int,
        };
        // SAFETY: `ctx` is valid; a null callback means "use the default decision".
        unsafe { sys::SSL_CTX_set_verify(ctx.0, mode, None) };

        if self.role == Role::Server {
            // The selection callback reads the offer list through the context's app data.
            // Leaked deliberately: it must outlive every session made from this context,
            // and there is exactly one per backend.
            let offers = Box::into_raw(Box::new(alpn_wire.clone()));
            // SAFETY: `ctx` is valid and `offers` outlives it by construction.
            unsafe {
                sys::SSL_CTX_set_alpn_select_cb(
                    ctx.0,
                    Some(alpn_select_cb),
                    offers.cast::<c_void>(),
                )
            };
        }

        Ok(OsslBackend {
            ctx,
            alpn_wire,
            role: self.role,
            verify: self.verify,
        })
    }
}

/// Builds an error carrying whatever OpenSSL put on its error queue.
fn tls_error(context: &'static str) -> Error {
    // Draining the queue here also stops a stale entry from being attributed to the next
    // failure, which is a classic source of confusing OpenSSL diagnostics.
    let _ = take_openssl_error();
    Error::with_kind(ErrorKind::Crypto, context)
}

/// Loads a PEM certificate chain into a context.
fn load_certificate_chain(ctx: *mut sys::SSL_CTX, pem: &[u8]) -> Result<()> {
    let bio = Bio::from_bytes(pem)?;
    let mut first = true;
    loop {
        // SAFETY: `bio` is valid; the remaining arguments are the documented "no password"
        // form. Returns null when the BIO is exhausted.
        let cert = unsafe { sys::PEM_read_bio_X509(bio.0, ptr::null_mut(), None, ptr::null_mut()) };
        if cert.is_null() {
            break;
        }
        let cert = X509(cert);
        let rc = if first {
            // SAFETY: both pointers are valid.
            unsafe { sys::SSL_CTX_use_certificate(ctx, cert.0) }
        } else {
            // `SSL_CTX_add1_chain_cert` is a macro over `SSL_CTX_ctrl`, which bindgen does
            // not emit. The `1` argument selects the reference-taking variant, so `cert`
            // keeps its own reference and is still freed on drop.
            // SAFETY: both pointers are valid.
            unsafe {
                sys::SSL_CTX_ctrl(
                    ctx,
                    sys::SSL_CTRL_CHAIN_CERT as c_int,
                    1,
                    cert.0.cast::<c_void>(),
                ) as c_int
            }
        };
        if rc != 1 {
            return Err(tls_error("could not install a certificate"));
        }
        first = false;
    }
    if first {
        return Err(Error::invalid_input(
            "the certificate chain contained no PEM certificate",
        ));
    }
    // A parse that stopped early leaves an error on the queue; discard it, since reaching
    // the end of the chain is the normal way out of the loop above.
    let _ = take_openssl_error();
    Ok(())
}

/// Loads a PEM private key into a context.
fn load_private_key(ctx: *mut sys::SSL_CTX, pem: &[u8]) -> Result<()> {
    let bio = Bio::from_bytes(pem)?;
    // SAFETY: `bio` is valid; the remaining arguments are the "no password" form.
    let key =
        unsafe { sys::PEM_read_bio_PrivateKey(bio.0, ptr::null_mut(), None, ptr::null_mut()) };
    if key.is_null() {
        return Err(tls_error("could not parse the private key"));
    }
    let key = Pkey(key);
    // SAFETY: both pointers are valid.
    if unsafe { sys::SSL_CTX_use_PrivateKey(ctx, key.0) } != 1 {
        return Err(tls_error("could not install the private key"));
    }
    Ok(())
}

/// Adds a PEM certificate to a context's trust store.
fn add_trust_anchor(ctx: *mut sys::SSL_CTX, pem: &[u8]) -> Result<()> {
    let bio = Bio::from_bytes(pem)?;
    // SAFETY: `ctx` is valid; the store is owned by the context.
    let store = unsafe { sys::SSL_CTX_get_cert_store(ctx) };
    if store.is_null() {
        return Err(tls_error("the context has no certificate store"));
    }
    let mut added = false;
    loop {
        // SAFETY: `bio` is valid; null means exhausted.
        let cert = unsafe { sys::PEM_read_bio_X509(bio.0, ptr::null_mut(), None, ptr::null_mut()) };
        if cert.is_null() {
            break;
        }
        let cert = X509(cert);
        // SAFETY: both pointers are valid. `X509_STORE_add_cert` takes its own reference.
        if unsafe { sys::X509_STORE_add_cert(store, cert.0) } != 1 {
            return Err(tls_error("could not add a trust anchor"));
        }
        added = true;
    }
    if !added {
        return Err(Error::invalid_input(
            "the trust anchor contained no PEM certificate",
        ));
    }
    let _ = take_openssl_error();
    Ok(())
}

/// Chooses an ALPN protocol on the server side.
///
/// Runs inside OpenSSL during the handshake. Returning a fatal alert here is what turns "no
/// protocol in common" into a handshake failure that names ALPN, rather than a connection
/// that completes and then behaves strangely.
unsafe extern "C" fn alpn_select_cb(
    _ssl: *mut sys::SSL,
    out: *mut *const c_uchar,
    outlen: *mut c_uchar,
    client: *const c_uchar,
    client_len: core::ffi::c_uint,
    arg: *mut c_void,
) -> c_int {
    if arg.is_null() || client.is_null() {
        return sys::SSL_TLSEXT_ERR_ALERT_FATAL as c_int;
    }
    // SAFETY: `arg` is the leaked `Box<Vec<u8>>` installed at build time, which outlives
    // every session made from this context.
    let offers = unsafe { &*arg.cast::<Vec<u8>>() };
    // SAFETY: OpenSSL guarantees `client` is readable for `client_len` bytes.
    let client = unsafe { core::slice::from_raw_parts(client, client_len as usize) };

    // Server preference: walk our list in order and take the first the client also offered.
    let mut cursor = 0usize;
    while cursor < offers.len() {
        let len = offers[cursor] as usize;
        let start = cursor + 1;
        let end = start + len;
        if end > offers.len() {
            break;
        }
        let candidate = &offers[start..end];

        let mut peer = 0usize;
        while peer < client.len() {
            let peer_len = client[peer] as usize;
            let peer_start = peer + 1;
            let peer_end = peer_start + peer_len;
            if peer_end > client.len() {
                break;
            }
            if &client[peer_start..peer_end] == candidate {
                // SAFETY: `out` and `outlen` are valid out-parameters, and the slice we
                // point at lives in the leaked offers buffer.
                unsafe {
                    *out = offers.as_ptr().add(start);
                    *outlen = len as c_uchar;
                }
                return sys::SSL_TLSEXT_ERR_OK as c_int;
            }
            peer = peer_end;
        }
        cursor = end;
    }

    sys::SSL_TLSEXT_ERR_ALERT_FATAL as c_int
}

/// Hands the crypto helper the connection stored in the reference.
///
/// The helper calls this from six different callbacks, each having just recovered the
/// reference from the `SSL`'s application data.
unsafe extern "C" fn get_conn_cb(
    conn_ref: *mut sys::ngtcp2_crypto_conn_ref,
) -> *mut sys::ngtcp2_conn {
    if conn_ref.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: the reference is the boxed one this session owns, and its `user_data` is the
    // `ngtcp2_conn` pointer installed by `bind_connection`.
    unsafe { (*conn_ref).user_data.cast::<sys::ngtcp2_conn>() }
}

/// An OpenSSL `SSL_CTX`, freed on drop.
struct SslCtx(*mut sys::SSL_CTX);

impl Drop for SslCtx {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `SSL_CTX_new` and is freed exactly once.
        unsafe { sys::SSL_CTX_free(self.0) };
    }
}

// SAFETY: OpenSSL 3.x reference-counts `SSL_CTX` internally and permits use from multiple
// threads. This type only creates sessions from it.
unsafe impl Send for SslCtx {}
unsafe impl Sync for SslCtx {}

/// A memory BIO over borrowed bytes, freed on drop.
struct Bio(*mut sys::BIO);

impl Bio {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let len = c_int::try_from(bytes.len())
            .map_err(|_| Error::invalid_input("PEM input is too large"))?;
        // SAFETY: `bytes` is readable for `len`, and `BIO_new_mem_buf` only reads it. The
        // BIO does not outlive this function's callers' borrow.
        let bio = unsafe { sys::BIO_new_mem_buf(bytes.as_ptr().cast::<c_void>(), len) };
        if bio.is_null() {
            return Err(tls_error("BIO_new_mem_buf failed"));
        }
        Ok(Self(bio))
    }
}

impl Drop for Bio {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `BIO_new_mem_buf` and is freed exactly once.
        unsafe { sys::BIO_free(self.0) };
    }
}

/// An owned `X509`, freed on drop.
struct X509(*mut sys::X509);

impl Drop for X509 {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `PEM_read_bio_X509` and is freed exactly once.
        unsafe { sys::X509_free(self.0) };
    }
}

/// An owned `EVP_PKEY`, freed on drop.
struct Pkey(*mut sys::EVP_PKEY);

impl Drop for Pkey {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `PEM_read_bio_PrivateKey` and is freed exactly once.
        unsafe { sys::EVP_PKEY_free(self.0) };
    }
}

/// A configured OpenSSL TLS stack.
pub struct OsslBackend {
    ctx: SslCtx,
    alpn_wire: Vec<u8>,
    role: Role,
    verify: Verify,
}

impl OsslBackend {
    /// Starts building a backend for the given role.
    pub fn builder(role: Role) -> OsslBackendBuilder {
        OsslBackendBuilder::new(role)
    }
}

// SAFETY: `OsslBackend` owns an `SSL_CTX`, which OpenSSL 3.x permits sharing across
// threads, plus plain data.
unsafe impl Send for OsslBackend {}
unsafe impl Sync for OsslBackend {}

// SAFETY: sessions own their own `SSL` and ossl ctx, independent of one another; the
// backend's `SSL_CTX` is reference-counted by OpenSSL and by `SSL_new`.
unsafe impl TlsBackend for OsslBackend {
    type Session = OsslSession;

    fn new_session(&self, role: Role, server_name: Option<&str>) -> Result<Self::Session> {
        if role != self.role {
            return Err(Error::invalid_input(
                "the session role does not match the backend's role",
            ));
        }
        OsslSession::new(self, role, server_name)
    }
}

/// One connection's TLS session.
///
/// Owns all three C objects — the `SSL`, the `ngtcp2_crypto_ossl_ctx`, and the boxed
/// `ngtcp2_crypto_conn_ref` OpenSSL holds as app data — because they must be destroyed in a
/// specific order and Rust's field-order drop would make that ordering invisible. See the
/// module documentation.
pub struct OsslSession {
    /// The ossl helper context. **This**, not `ssl`, is what ngtcp2 wants as the native
    /// handle.
    ossl_ctx: *mut sys::ngtcp2_crypto_ossl_ctx,
    ssl: *mut sys::SSL,
    /// Boxed so its address is stable: OpenSSL holds a pointer to it, and the helper
    /// dereferences that pointer from six different callbacks.
    conn_ref: Box<sys::ngtcp2_crypto_conn_ref>,
    verify: Verify,
    role: Role,
}

impl OsslSession {
    fn new(backend: &OsslBackend, role: Role, server_name: Option<&str>) -> Result<Self> {
        // SAFETY: the backend's context is valid and outlives this call; `SSL_new` takes
        // its own reference on it.
        let ssl = unsafe { sys::SSL_new(backend.ctx.0) };
        if ssl.is_null() {
            return Err(tls_error("SSL_new failed"));
        }

        // From here on every early return must free `ssl`, so it is wrapped immediately.
        let mut session = Self {
            ossl_ctx: ptr::null_mut(),
            ssl,
            conn_ref: Box::new(sys::ngtcp2_crypto_conn_ref {
                get_conn: None,
                user_data: ptr::null_mut(),
            }),
            verify: backend.verify,
            role,
        };

        let mut ossl_ctx: *mut sys::ngtcp2_crypto_ossl_ctx = ptr::null_mut();
        // SAFETY: `ossl_ctx` is a valid out-parameter and `ssl` is valid.
        let rc = unsafe { sys::ngtcp2_crypto_ossl_ctx_new(&mut ossl_ctx, session.ssl) };
        if rc != 0 {
            return Err(Error::native(rc, "ngtcp2_crypto_ossl_ctx_new failed"));
        }
        session.ossl_ctx = ossl_ctx;

        // SAFETY: `ssl` is valid; this installs the QUIC TLS dispatch table.
        let rc = unsafe {
            if role == Role::Server {
                sys::ngtcp2_crypto_ossl_configure_server_session(session.ssl)
            } else {
                sys::ngtcp2_crypto_ossl_configure_client_session(session.ssl)
            }
        };
        if rc != 0 {
            return Err(Error::native(
                rc,
                "could not configure the QUIC TLS session",
            ));
        }

        // SAFETY: `ssl` is valid.
        unsafe {
            if role == Role::Server {
                sys::SSL_set_accept_state(session.ssl);
            } else {
                sys::SSL_set_connect_state(session.ssl);
            }
        }

        if role == Role::Client {
            // SAFETY: `ssl` is valid and the ALPN wire form outlives the call, which
            // copies it.
            let rc = unsafe {
                sys::SSL_set_alpn_protos(
                    session.ssl,
                    backend.alpn_wire.as_ptr(),
                    backend.alpn_wire.len() as core::ffi::c_uint,
                )
            };
            if rc != 0 {
                return Err(tls_error("SSL_set_alpn_protos failed"));
            }

            if let Some(name) = server_name {
                session.set_server_name(name)?;
            } else if backend.verify == Verify::Peer {
                // Verification without a name to check against would validate the chain and
                // then accept a certificate for any host, which is the failure mode most
                // worth refusing outright.
                return Err(Error::invalid_input(
                    "a verifying client must be given a server name to check against",
                ));
            }
        }

        Ok(session)
    }

    /// Sets SNI and, when verifying, the name the certificate must match.
    fn set_server_name(&mut self, name: &str) -> Result<()> {
        let c_name = std::ffi::CString::new(name)
            .map_err(|_| Error::invalid_input("a server name cannot contain a NUL byte"))?;

        // `SSL_set_tlsext_host_name` is a macro over `SSL_ctrl`; bindgen does not emit it.
        // SAFETY: `ssl` is valid; `SSL_ctrl` copies the name.
        let rc = unsafe {
            sys::SSL_ctrl(
                self.ssl,
                CTRL_SET_TLSEXT_HOSTNAME,
                NAMETYPE_HOST_NAME,
                c_name.as_ptr() as *mut c_void,
            )
        };
        if rc != 1 {
            return Err(tls_error("could not set the SNI server name"));
        }

        if self.verify == Verify::Peer {
            // Separate from SNI on purpose: SNI says which certificate to send, this says
            // which name the returned certificate must actually match. Setting only the
            // former is the classic way to build a client that appears to verify and does
            // not.
            // SAFETY: `ssl` is valid; `SSL_set1_host` copies the name.
            if unsafe { sys::SSL_set1_host(self.ssl, c_name.as_ptr()) } != 1 {
                return Err(tls_error("could not set the certificate verification name"));
            }
        }
        Ok(())
    }

    /// Wires the connection reference OpenSSL hands back to the crypto helper.
    ///
    /// # Safety
    ///
    /// `get_conn` must return a live `ngtcp2_conn` for as long as this session exists, and
    /// `user_data` must remain valid for the same period.
    unsafe fn bind_conn_ref(
        &mut self,
        get_conn: sys::ngtcp2_crypto_get_conn,
        user_data: *mut c_void,
    ) {
        self.conn_ref.get_conn = get_conn;
        self.conn_ref.user_data = user_data;
        let ptr: *mut sys::ngtcp2_crypto_conn_ref = &mut *self.conn_ref;
        // SAFETY: `ssl` is valid and `conn_ref` is boxed, so its address is stable for as
        // long as this session lives -- which, by the `Drop` order below, is longer than
        // OpenSSL will read it.
        unsafe { sys::SSL_set_ex_data(self.ssl, APP_DATA_INDEX, ptr.cast::<c_void>()) };
    }

    /// The result of certificate verification, once the handshake has run.
    pub(crate) fn verify_result(&self) -> core::ffi::c_long {
        // SAFETY: `ssl` is valid.
        unsafe { sys::SSL_get_verify_result(self.ssl) }
    }
}

impl Drop for OsslSession {
    fn drop(&mut self) {
        // The order here is the reason this type exists. See the module documentation.
        //
        // 1. Clear the app data, so the CRYPTO records released during `SSL_free` cannot
        //    reach a `ngtcp2_conn` that may already be gone. Every ossl dispatch callback
        //    begins by reading this back and returns early when it is null.
        if !self.ssl.is_null() {
            // SAFETY: `ssl` is valid and null is an accepted ex-data value.
            unsafe { sys::SSL_set_ex_data(self.ssl, APP_DATA_INDEX, ptr::null_mut()) };
            // 2. Free the `SSL`, which is what triggers those releases.
            // SAFETY: the pointer came from `SSL_new` and is freed exactly once.
            unsafe { sys::SSL_free(self.ssl) };
            self.ssl = ptr::null_mut();
        }

        // 3. Only now free the helper context, which owns a `remote_params` buffer OpenSSL
        //    may have been borrowing until the step above completed.
        if !self.ossl_ctx.is_null() {
            // SAFETY: the pointer came from `ngtcp2_crypto_ossl_ctx_new`, is freed exactly
            // once, and `SSL_free` has already run.
            unsafe { sys::ngtcp2_crypto_ossl_ctx_del(self.ossl_ctx) };
            self.ossl_ctx = ptr::null_mut();
        }

        // `conn_ref` is dropped last, by the compiler, after nothing can read it.
    }
}

// SAFETY: an `OsslSession` owns its `SSL`, its helper context and its boxed conn ref
// exclusively. None is shared, and OpenSSL permits an `SSL` to be used from any one thread
// at a time. It is deliberately not `Sync`.
unsafe impl Send for OsslSession {}

// SAFETY: `native_handle` returns the ossl helper context, which is what
// `ngtcp2_conn_set_tls_native_handle` expects for this backend -- see `NativeTlsHandle`.
// The teardown order required by the helper is enforced by `Drop` above.
unsafe impl TlsSession for OsslSession {
    unsafe fn bind_connection(&mut self, conn: *mut c_void) {
        // `user_data` is the `ngtcp2_conn` pointer itself rather than anything of ours.
        // That matters: ngtcp2 allocates the connection on the heap and never moves it, so
        // the pointer stays valid even though the Rust `Conn` wrapper around it is moved
        // when it is returned from its builder. Pointing at the wrapper would dangle.
        // SAFETY: the caller guarantees `conn` outlives this session.
        unsafe { self.bind_conn_ref(Some(get_conn_cb), conn) };
    }

    fn native_handle(&self) -> NativeTlsHandle {
        // The ossl ctx, NOT the `SSL`. The parameter is `void *`, so the wrong one would
        // compile and corrupt memory at run time.
        // SAFETY: the pointer is live for as long as `self`, and is the one the helper
        // expects.
        unsafe { NativeTlsHandle::new(self.ossl_ctx.cast::<c_void>()) }
    }

    unsafe fn install_callbacks(&self, callbacks: *mut c_void) {
        // SAFETY: the caller guarantees this points at a valid `ngtcp2_callbacks`.
        let callbacks = unsafe { &mut *callbacks.cast::<sys::ngtcp2_callbacks>() };

        // The crypto half of the table, supplied by ngtcp2's backend-independent helper.
        // These are the entries the assert block in `ngtcp2_conn_new` requires.
        callbacks.recv_crypto_data = Some(sys::ngtcp2_crypto_recv_crypto_data_cb);
        callbacks.encrypt = Some(sys::ngtcp2_crypto_encrypt_cb);
        callbacks.decrypt = Some(sys::ngtcp2_crypto_decrypt_cb);
        callbacks.hp_mask = Some(sys::ngtcp2_crypto_hp_mask_cb);
        callbacks.update_key = Some(sys::ngtcp2_crypto_update_key_cb);
        callbacks.delete_crypto_aead_ctx = Some(sys::ngtcp2_crypto_delete_crypto_aead_ctx_cb);
        callbacks.delete_crypto_cipher_ctx = Some(sys::ngtcp2_crypto_delete_crypto_cipher_ctx_cb);
        callbacks.get_path_challenge_data = Some(sys::ngtcp2_crypto_get_path_challenge_data_cb);
        callbacks.version_negotiation = Some(sys::ngtcp2_crypto_version_negotiation_cb);

        if self.role == Role::Server {
            callbacks.recv_client_initial = Some(sys::ngtcp2_crypto_recv_client_initial_cb);
        } else {
            callbacks.client_initial = Some(sys::ngtcp2_crypto_client_initial_cb);
            callbacks.recv_retry = Some(sys::ngtcp2_crypto_recv_retry_cb);
        }
    }

    fn negotiated_alpn(&self) -> Option<Vec<u8>> {
        let mut data: *const c_uchar = ptr::null();
        let mut len: core::ffi::c_uint = 0;
        // SAFETY: `ssl` is valid and both out-parameters are writable.
        unsafe { sys::SSL_get0_alpn_selected(self.ssl, &mut data, &mut len) };
        if data.is_null() || len == 0 {
            return None;
        }
        // SAFETY: OpenSSL guarantees the buffer is readable for `len` bytes and owned by
        // the `SSL`, so copying it out is what keeps the result usable.
        Some(unsafe { core::slice::from_raw_parts(data, len as usize) }.to_vec())
    }

    fn failure_reason(&self) -> Option<String> {
        // Certificate verification first, because it is the failure a caller is most likely
        // to have caused and the one OpenSSL's generic error queue describes worst.
        let verdict = self.verify_result();
        if verdict != sys::X509_V_OK as core::ffi::c_long {
            // SAFETY: the function accepts any long and returns a static string.
            let text = unsafe { sys::X509_verify_cert_error_string(verdict) };
            if !text.is_null() {
                // SAFETY: the returned string is static and NUL-terminated.
                let text = unsafe { CStr::from_ptr(text) };
                return Some(format!(
                    "certificate verification failed: {}",
                    text.to_string_lossy()
                ));
            }
        }
        take_openssl_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpn_encodes_with_length_prefixes() {
        let encoded = encode_alpn(&[b"h3".to_vec(), b"hq".to_vec()]).unwrap();
        assert_eq!(encoded, vec![2, b'h', b'3', 2, b'h', b'q']);
    }

    #[test]
    fn an_empty_or_oversized_alpn_protocol_is_rejected() {
        assert!(encode_alpn(&[Vec::new()]).is_err());
        assert!(encode_alpn(&[vec![0u8; 256]]).is_err());
        assert!(encode_alpn(&[vec![0u8; 255]]).is_ok());
    }

    #[test]
    fn a_backend_without_alpn_is_rejected() {
        // QUIC requires ALPN, so a backend that forgot it would fail every handshake with a
        // message about something else.
        let Err(err) = OsslBackend::builder(Role::Client).build() else {
            panic!("a backend with no ALPN must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn a_server_without_credentials_is_rejected() {
        let Err(err) = OsslBackend::builder(Role::Server).alpn("h3").build() else {
            panic!("a server with no certificate must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn a_client_backend_builds() {
        assert!(
            OsslBackend::builder(Role::Client)
                .alpn("h3")
                .build()
                .is_ok()
        );
    }

    #[test]
    fn a_verifying_client_session_needs_a_server_name() {
        // Without a name there is nothing for the certificate to be checked against, and
        // accepting one silently would be a client that looks verified and is not.
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .build()
            .unwrap();
        assert!(backend.new_session(Role::Client, None).is_err());
        assert!(
            backend
                .new_session(Role::Client, Some("example.com"))
                .is_ok()
        );
    }

    #[test]
    fn a_non_verifying_client_session_needs_no_server_name() {
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .verify(Verify::DangerouslyAcceptAnyCertificate)
            .build()
            .unwrap();
        assert!(backend.new_session(Role::Client, None).is_ok());
    }

    #[test]
    fn a_session_reports_the_ossl_context_as_its_native_handle() {
        // The single easiest catastrophic mistake in this whole crate is handing ngtcp2 the
        // `SSL *` instead. The parameter is `void *`, so nothing would complain.
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .verify(Verify::DangerouslyAcceptAnyCertificate)
            .build()
            .unwrap();
        let session = backend.new_session(Role::Client, None).unwrap();
        assert_eq!(session.native_handle().as_ptr(), session.ossl_ctx.cast());
        assert_ne!(session.native_handle().as_ptr(), session.ssl.cast());
    }

    #[test]
    fn a_role_mismatch_is_rejected() {
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .build()
            .unwrap();
        assert!(backend.new_session(Role::Server, None).is_err());
    }

    #[test]
    fn a_session_can_be_dropped_before_any_handshake_activity() {
        // The first of the lifecycle drop points. The rest need a connection to drive.
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .verify(Verify::DangerouslyAcceptAnyCertificate)
            .build()
            .unwrap();
        for _ in 0..16 {
            drop(backend.new_session(Role::Client, None).unwrap());
        }
    }

    #[test]
    fn a_session_can_be_dropped_after_its_conn_ref_was_bound() {
        // Binding the conn ref is what puts a pointer into OpenSSL's app data, so this is
        // the drop path where clearing it first actually matters.
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .verify(Verify::DangerouslyAcceptAnyCertificate)
            .build()
            .unwrap();
        let mut session = backend.new_session(Role::Client, None).unwrap();
        // SAFETY: `None` for `get_conn` is never called, and the user data is null, so
        // nothing is dereferenced. This exercises the ordering, not the callback.
        unsafe { session.bind_conn_ref(None, ptr::null_mut()) };
        drop(session);
    }

    #[test]
    fn dropping_the_backend_before_its_sessions_is_sound() {
        // `SSL_new` takes a reference on the context, so the context outliving the backend
        // handle is OpenSSL's problem rather than ours -- but it is worth pinning, because
        // the opposite would be an easy assumption to build on.
        let backend = OsslBackend::builder(Role::Client)
            .alpn("h3")
            .verify(Verify::DangerouslyAcceptAnyCertificate)
            .build()
            .unwrap();
        let session = backend.new_session(Role::Client, None).unwrap();
        drop(backend);
        drop(session);
    }

    #[test]
    fn a_malformed_certificate_is_rejected() {
        let Err(err) = OsslBackend::builder(Role::Server)
            .alpn("h3")
            .certificate_chain_pem("not a certificate")
            .private_key_pem("not a key")
            .build()
        else {
            panic!("malformed credentials must be rejected");
        };
        // Either kind is acceptable; what matters is that it fails rather than building a
        // server that cannot complete a handshake.
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidInput | ErrorKind::Crypto
        ));
    }

    #[test]
    fn initialising_the_helper_twice_is_harmless() {
        // It is process-global with no refcount, so this pins that the `Once` is doing its
        // job rather than that OpenSSL tolerates repetition.
        assert!(ensure_ossl_init().is_ok());
        assert!(ensure_ossl_init().is_ok());
    }
}
