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
use crate::tls::{
    CryptoError, DirectionalKeys, HP_MASK_LEN, HP_SAMPLE_LEN, HeaderKey, InitialKeys,
    NativeTlsHandle, PacketKey, Role, TlsBackend, TlsSession,
};

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

/// The algorithms of one cipher suite, as ngtcp2's crypto helper describes them.
///
/// Every field is a plain descriptor: a pointer to one of OpenSSL's immutable algorithm
/// objects, plus the numbers ngtcp2's core reads. None of them owns anything, which is why
/// this is `Copy` and why nothing here has a destructor.
///
/// The reason this exists at all is that the helper's derivation functions all want the
/// suite passed alongside the secret, and gathering it once per key would mean asking
/// OpenSSL the same question repeatedly at the exact points where a mismatch between the
/// derivation and the protection would be undetectable.
#[derive(Clone, Copy)]
struct Suite {
    /// The payload AEAD, carrying its own tag length in `max_overhead`.
    aead: sys::ngtcp2_crypto_aead,
    /// The hash the key schedule expands with.
    md: sys::ngtcp2_crypto_md,
    /// The header protection cipher.
    hp: sys::ngtcp2_crypto_cipher,
    /// How many packets this suite may protect before a key update is forced.
    max_encryption: u64,
    /// How many failed decryptions it may tolerate.
    max_decryption_failure: u64,
}

impl Suite {
    /// The suite QUIC fixes for Initial packets: AES-128-GCM with SHA-256.
    ///
    /// The usage limits are zero, which is what `ngtcp2_crypto_ctx_initial` itself sets
    /// (`ossl.c:252-259`). That is not an oversight in either place: ngtcp2 compares packet
    /// counts against these only for the application packet number space
    /// (`ngtcp2_conn.c:8743`, `:9474`), and Initial keys are discarded long before any count
    /// could matter.
    fn initial() -> Self {
        // SAFETY: the helper fills every field of the context it is given.
        let ctx = unsafe {
            let mut ctx: sys::ngtcp2_crypto_ctx = core::mem::zeroed();
            sys::ngtcp2_crypto_ctx_initial(&raw mut ctx);
            ctx
        };
        Self::from_ctx(&ctx)
    }

    /// The suite QUIC fixes for Retry integrity tags: AES-128-GCM, in every version.
    ///
    /// Separate from [`Suite::initial`] because it is a different thing that happens to use
    /// the same algorithm: the Retry tag has no hash and no header protection, and the key
    /// is a published constant rather than derived from anything.
    fn retry() -> Self {
        // SAFETY: the helper fills the descriptor it is given.
        let aead = unsafe {
            let mut aead: sys::ngtcp2_crypto_aead = core::mem::zeroed();
            sys::ngtcp2_crypto_aead_retry(&raw mut aead);
            aead
        };
        Self {
            aead,
            // SAFETY: a zeroed descriptor is a null algorithm, which nothing here consults:
            // the Retry key is a constant, so it is never expanded from a secret and no
            // header is masked with it.
            md: unsafe { core::mem::zeroed() },
            hp: unsafe { core::mem::zeroed() },
            max_encryption: 0,
            max_decryption_failure: 0,
        }
    }

    /// Reads the suite the handshake negotiated out of a crypto helper context.
    ///
    /// This is the one call the hybrid keeps the `ngtcp2_crypto_ossl` archive for on this
    /// side of the seam. It maps a TLS 1.3 cipher suite identifier onto an AEAD, a hash, a
    /// header protection cipher **and** both usage limits in one step — and getting any one
    /// of those four wrong produces a connection that fails in a way no test written against
    /// a single implementation would catch, because both ends would be wrong together.
    ///
    /// # Safety
    ///
    /// `ossl_ctx` must be a live `ngtcp2_crypto_ossl_ctx` whose `SSL` has negotiated a
    /// cipher suite.
    unsafe fn negotiated(ossl_ctx: *mut sys::ngtcp2_crypto_ossl_ctx) -> Result<Self> {
        let mut ctx: sys::ngtcp2_crypto_ctx = unsafe { core::mem::zeroed() };
        // SAFETY: the caller guarantees the context is live; the helper writes only into
        // `ctx`.
        let filled = unsafe { sys::ngtcp2_crypto_ctx_tls(&raw mut ctx, ossl_ctx.cast::<c_void>()) };
        if filled.is_null() {
            return Err(Error::backend("the negotiated cipher suite is unsupported"));
        }
        Ok(Self::from_ctx(&ctx))
    }

    /// Splits a filled crypto context into the parts this backend keeps.
    fn from_ctx(ctx: &sys::ngtcp2_crypto_ctx) -> Self {
        Self {
            aead: ctx.aead,
            md: ctx.md,
            hp: ctx.hp,
            max_encryption: ctx.max_encryption,
            max_decryption_failure: ctx.max_decryption_failure,
        }
    }

    /// How many bytes of key material the AEAD takes.
    fn key_len(&self) -> usize {
        // SAFETY: the descriptor names a live algorithm object.
        unsafe { sys::ngtcp2_crypto_aead_keylen(&raw const self.aead) }
    }

    /// How long the packet protection initialisation vector is.
    ///
    /// Deliberately `packet_protection_ivlen` rather than the AEAD's own nonce length: QUIC
    /// requires at least eight bytes so a packet number always fits, and the helper applies
    /// that floor (`shared.c:141-144`). Using the raw nonce length instead would silently
    /// produce short IVs for any future AEAD with a smaller nonce.
    fn iv_len(&self) -> usize {
        // SAFETY: the descriptor names a live algorithm object.
        unsafe { sys::ngtcp2_crypto_packet_protection_ivlen(&raw const self.aead) }
    }

    /// How many bytes protection adds to a payload.
    fn tag_len(&self) -> usize {
        self.aead.max_overhead
    }

    /// How long a secret for this suite's hash is.
    fn hash_len(&self) -> usize {
        // SAFETY: the descriptor names a live algorithm object.
        unsafe { sys::ngtcp2_crypto_md_hashlen(&raw const self.md) }
    }
}

/// A payload protection key backed by an OpenSSL cipher context.
///
/// # Why this is `Send` but not `Sync`
///
/// The `EVP_CIPHER_CTX` behind the handle is mutated by every protection operation — the
/// helper re-initialises the nonce on it each time (`ossl.c:936-940`, `:967-971`) — even
/// though the seam's methods take `&self`, because ngtcp2's callbacks hand it out as
/// `const`. Nothing observes that as aliasing: the mutation happens through a raw pointer,
/// which carries no uniqueness guarantee, and only one operation can be in flight at a time
/// because the type is not `Sync`. Making it `Sync` would be unsound, and nothing needs it.
pub struct OsslPacketKey {
    /// Which AEAD, and how much overhead it adds.
    aead: sys::ngtcp2_crypto_aead,
    /// The initialised cipher context. Freed exactly once, on drop.
    ctx: sys::ngtcp2_crypto_aead_ctx,
    /// Reported to ngtcp2, which forces a key update against it.
    max_encryption: u64,
    /// Reported to ngtcp2, which closes the connection against it.
    max_decryption_failure: u64,
}

// SAFETY: the key owns its cipher context outright and shares nothing but pointers to
// OpenSSL's immutable algorithm objects, which are valid for the life of the process.
// `Sync` is deliberately not claimed; see the type's documentation.
unsafe impl Send for OsslPacketKey {}

impl OsslPacketKey {
    /// Builds a key for protecting outbound payloads.
    fn for_encryption(suite: &Suite, key: &[u8]) -> Result<Self> {
        Self::init(suite, key, sys::ngtcp2_crypto_aead_ctx_encrypt_init)
    }

    /// Builds a key for unprotecting inbound payloads.
    fn for_decryption(suite: &Suite, key: &[u8]) -> Result<Self> {
        Self::init(suite, key, sys::ngtcp2_crypto_aead_ctx_decrypt_init)
    }

    /// The shared half of both, which differ only in which initialiser they call.
    fn init(
        suite: &Suite,
        key: &[u8],
        init: unsafe extern "C" fn(
            *mut sys::ngtcp2_crypto_aead_ctx,
            *const sys::ngtcp2_crypto_aead,
            *const u8,
            usize,
        ) -> c_int,
    ) -> Result<Self> {
        if key.len() != suite.key_len() {
            return Err(Error::backend("the key is the wrong length for the suite"));
        }
        // SAFETY: a zeroed context is the "not yet initialised" state the helper's own
        // callers start from, and is what the initialiser expects to be handed.
        let mut ctx: sys::ngtcp2_crypto_aead_ctx = unsafe { core::mem::zeroed() };
        // SAFETY: the descriptor is live, the key is at least `key_len` bytes, and the
        // context is uninitialised and writable.
        let rv = unsafe {
            init(
                &raw mut ctx,
                &raw const suite.aead,
                key.as_ptr(),
                suite.iv_len(),
            )
        };
        if rv != 0 {
            return Err(tls_error("could not initialise a payload protection key"));
        }
        Ok(Self {
            aead: suite.aead,
            ctx,
            max_encryption: suite.max_encryption,
            max_decryption_failure: suite.max_decryption_failure,
        })
    }
}

impl Drop for OsslPacketKey {
    fn drop(&mut self) {
        // SAFETY: the context was initialised by this type and is freed exactly once,
        // because nothing else holds a copy of the handle.
        unsafe { sys::ngtcp2_crypto_aead_ctx_free(&raw mut self.ctx) };
    }
}

impl PacketKey for OsslPacketKey {
    fn seal(
        &self,
        buf: &mut [u8],
        plaintext_len: usize,
        nonce: &[u8],
        aad: &[u8],
    ) -> core::result::Result<(), CryptoError> {
        // The helper writes the tag at `dest + plaintextlen` without checking that there is
        // room for it (`ossl.c:912-916`), so this bound is the only thing standing between a
        // short buffer and a heap overflow.
        let needed = plaintext_len
            .checked_add(self.tag_len())
            .ok_or(CryptoError::Fatal)?;
        if buf.len() < needed || nonce.len() < self.aead_nonce_len() {
            return Err(CryptoError::Fatal);
        }
        let dest = buf.as_mut_ptr();
        // SAFETY: `dest` and the plaintext are the same buffer, which ngtcp2 explicitly
        // permits (`ngtcp2.h:2818`); it is at least `plaintext_len + tag_len` bytes, the
        // nonce is long enough, and the context is initialised for encryption.
        let rv = unsafe {
            sys::ngtcp2_crypto_encrypt(
                dest,
                &raw const self.aead,
                &raw const self.ctx,
                dest.cast_const(),
                plaintext_len,
                nonce.as_ptr(),
                nonce.len(),
                aad.as_ptr(),
                aad.len(),
            )
        };
        if rv == 0 {
            Ok(())
        } else {
            Err(CryptoError::Fatal)
        }
    }

    fn open(
        &self,
        buf: &mut [u8],
        ciphertext_len: usize,
        nonce: &[u8],
        aad: &[u8],
    ) -> core::result::Result<usize, CryptoError> {
        // A packet too short to hold a tag is a malformed packet, not a broken backend: it
        // is exactly what a truncating attacker would send.
        let Some(plaintext_len) = ciphertext_len.checked_sub(self.tag_len()) else {
            return Err(CryptoError::Decrypt);
        };
        if buf.len() < ciphertext_len || nonce.len() < self.aead_nonce_len() {
            return Err(CryptoError::Fatal);
        }
        let dest = buf.as_mut_ptr();
        // SAFETY: as in `seal`, with `dest` and the ciphertext the same buffer
        // (`ngtcp2.h:2846`); it holds at least `ciphertext_len` bytes and the context is
        // initialised for decryption.
        let rv = unsafe {
            sys::ngtcp2_crypto_decrypt(
                dest,
                &raw const self.aead,
                &raw const self.ctx,
                dest.cast_const(),
                ciphertext_len,
                nonce.as_ptr(),
                nonce.len(),
                aad.as_ptr(),
                aad.len(),
            )
        };
        // The helper reports authentication failure and internal failure with the same `-1`
        // (`ossl.c:962-990`), so the two cannot be told apart here. Reporting the safer of
        // the two is not a compromise: a genuine internal failure will not decrypt anything
        // afterwards either, so the connection still ends — via the AEAD failure limit, and
        // without handing anyone who can send a datagram a way to close it.
        if rv == 0 {
            Ok(plaintext_len)
        } else {
            Err(CryptoError::Decrypt)
        }
    }

    fn tag_len(&self) -> usize {
        self.aead.max_overhead
    }

    fn confidentiality_limit(&self) -> u64 {
        self.max_encryption
    }

    fn integrity_limit(&self) -> u64 {
        self.max_decryption_failure
    }
}

impl OsslPacketKey {
    /// The nonce length the AEAD itself requires, which is what the helper writes through.
    ///
    /// Distinct from the suite's IV length: ngtcp2 builds the nonce from an IV that may have
    /// been padded up to eight bytes, and hands back however many bytes that produced. The
    /// helper ignores the length it is given and reads whatever the cipher wants
    /// (`ossl.c:924`, `:955` both discard `noncelen`), so this is checked here instead.
    fn aead_nonce_len(&self) -> usize {
        // SAFETY: the descriptor names a live algorithm object.
        unsafe { sys::ngtcp2_crypto_aead_noncelen(&raw const self.aead) }
    }
}

/// A header protection key backed by an OpenSSL cipher context.
///
/// The same `&self`-with-interior-mutation reasoning as [`OsslPacketKey`] applies, and for
/// ChaCha20 it is not merely theoretical: masking re-initialises the context with the sample
/// as its nonce (`ossl.c:945-949`).
pub struct OsslHeaderKey {
    /// Which cipher. Only its handle is read, by the helper.
    hp: sys::ngtcp2_crypto_cipher,
    /// The initialised cipher context. Freed exactly once, on drop.
    ctx: sys::ngtcp2_crypto_cipher_ctx,
}

// SAFETY: as for `OsslPacketKey` — the context is owned outright and `Sync` is not claimed.
unsafe impl Send for OsslHeaderKey {}

impl OsslHeaderKey {
    /// Builds a header protection key.
    ///
    /// Always an *encryption* context, in both directions: header protection is a keystream
    /// exclusive-ored over the header, so the same operation covers and uncovers it.
    fn new(suite: &Suite, key: &[u8]) -> Result<Self> {
        if key.len() != suite.key_len() {
            return Err(Error::backend(
                "the header protection key is the wrong length for the suite",
            ));
        }
        // SAFETY: a zeroed context is the uninitialised state the initialiser expects.
        let mut ctx: sys::ngtcp2_crypto_cipher_ctx = unsafe { core::mem::zeroed() };
        // SAFETY: the descriptor is live and the key is at least `key_len` bytes.
        let rv = unsafe {
            sys::ngtcp2_crypto_cipher_ctx_encrypt_init(
                &raw mut ctx,
                &raw const suite.hp,
                key.as_ptr(),
            )
        };
        if rv != 0 {
            return Err(tls_error("could not initialise a header protection key"));
        }
        Ok(Self { hp: suite.hp, ctx })
    }
}

impl Drop for OsslHeaderKey {
    fn drop(&mut self) {
        // SAFETY: initialised by this type, freed exactly once.
        unsafe { sys::ngtcp2_crypto_cipher_ctx_free(&raw mut self.ctx) };
    }
}

impl HeaderKey for OsslHeaderKey {
    fn mask(&self, sample: &[u8]) -> core::result::Result<[u8; HP_MASK_LEN], CryptoError> {
        if sample.len() < HP_SAMPLE_LEN {
            return Err(CryptoError::Fatal);
        }
        // The mask is five bytes, but the helper writes a **block**: sixteen for AES-ECB,
        // and sixteen plus a final call for ChaCha20 (`ossl.c:937-951`). ngtcp2's own core
        // allocates `uint8_t mask[NGTCP2_HP_SAMPLELEN]` for exactly this reason
        // (`ngtcp2_conn.c:5964`). Handing it a five-byte array would overflow the stack
        // frame on every single packet.
        let mut block = [0u8; HP_SAMPLE_LEN];
        // SAFETY: `block` is a full cipher block, the sample is at least `HP_SAMPLE_LEN`
        // bytes -- the only length the helper ever reads -- and the context is initialised.
        let rv = unsafe {
            sys::ngtcp2_crypto_hp_mask(
                block.as_mut_ptr(),
                &raw const self.hp,
                &raw const self.ctx,
                sample.as_ptr(),
            )
        };
        if rv != 0 {
            return Err(CryptoError::Fatal);
        }
        let mut mask = [0u8; HP_MASK_LEN];
        mask.copy_from_slice(&block[..HP_MASK_LEN]);
        Ok(mask)
    }
}

/// Derives one direction's keys from a traffic secret.
///
/// The QUIC key schedule is not reimplemented here. `derive_packet_protection_key` produces
/// the payload key, the initialisation vector and the header protection key from one secret
/// in one call, and applies the version-specific labels — `quic key` for version 1 and
/// `quicv2 key` for version 2 (`shared.c:159-165`). Choosing those labels by hand is the
/// single most attractive place in this file to introduce a bug that only shows up against
/// a different implementation.
fn derive_keys(
    suite: &Suite,
    version: u32,
    secret: &[u8],
) -> Result<DirectionalKeys<OsslPacketKey, OsslHeaderKey>> {
    derive_keys_with(suite, version, secret, OsslPacketKey::for_encryption)
}

/// The same, producing a key that unprotects rather than protects.
fn derive_rx_keys(
    suite: &Suite,
    version: u32,
    secret: &[u8],
) -> Result<DirectionalKeys<OsslPacketKey, OsslHeaderKey>> {
    derive_keys_with(suite, version, secret, OsslPacketKey::for_decryption)
}

/// The shared body of both, which differ only in the direction the AEAD is initialised for.
fn derive_keys_with(
    suite: &Suite,
    version: u32,
    secret: &[u8],
    make: fn(&Suite, &[u8]) -> Result<OsslPacketKey>,
) -> Result<DirectionalKeys<OsslPacketKey, OsslHeaderKey>> {
    let mut key = vec![0u8; suite.key_len()];
    let mut iv = vec![0u8; suite.iv_len()];
    let mut hp = vec![0u8; suite.key_len()];

    // SAFETY: every output buffer is exactly the length the helper derives for this suite,
    // and the descriptors are live.
    let rv = unsafe {
        sys::ngtcp2_crypto_derive_packet_protection_key(
            key.as_mut_ptr(),
            iv.as_mut_ptr(),
            hp.as_mut_ptr(),
            version,
            &raw const suite.aead,
            &raw const suite.md,
            secret.as_ptr(),
            secret.len(),
        )
    };
    if rv != 0 {
        return Err(Error::backend("could not derive packet protection keys"));
    }

    let packet = make(suite, &key)?;
    let header = OsslHeaderKey::new(suite, &hp)?;
    Ok(DirectionalKeys { packet, header, iv })
}

/// Derives both directions of the Initial keys for a connection identifier.
///
/// Initial keys owe nothing to the handshake: the identifier the client chose and a
/// version-specific salt are the whole input, which is why they can be derived before a
/// single handshake byte exists and why anyone watching can derive them too.
fn derive_initial_keys(
    role: Role,
    version: u32,
    dcid: &[u8],
) -> Result<InitialKeys<OsslPacketKey, OsslHeaderKey>> {
    if dcid.len() > sys::NGTCP2_MAX_CIDLEN as usize {
        return Err(Error::backend("the connection identifier is too long"));
    }
    let mut cid: sys::ngtcp2_cid = sys::ngtcp2_cid {
        datalen: dcid.len(),
        data: [0u8; 20],
    };
    cid.data[..dcid.len()].copy_from_slice(dcid);

    let suite = Suite::initial();
    let secret_len = sys::NGTCP2_CRYPTO_INITIAL_SECRETLEN as usize;
    let mut rx_secret = vec![0u8; secret_len];
    let mut tx_secret = vec![0u8; secret_len];
    let mut initial_secret = vec![0u8; secret_len];

    let side = if role.is_server() {
        sys::NGTCP2_CRYPTO_SIDE_SERVER
    } else {
        sys::NGTCP2_CRYPTO_SIDE_CLIENT
    };

    // SAFETY: all three buffers are the fixed length the helper writes, and the identifier
    // is a fully initialised `ngtcp2_cid`.
    let rv = unsafe {
        sys::ngtcp2_crypto_derive_initial_secrets(
            rx_secret.as_mut_ptr(),
            tx_secret.as_mut_ptr(),
            initial_secret.as_mut_ptr(),
            version,
            &raw const cid,
            side,
        )
    };
    if rv != 0 {
        return Err(Error::backend("could not derive the Initial secrets"));
    }

    Ok(InitialKeys {
        rx: derive_rx_keys(&suite, version, &rx_secret)?,
        tx: derive_keys(&suite, version, &tx_secret)?,
    })
}

/// The fixed key that authenticates a Retry packet's integrity tag.
///
/// Published constants, not secrets — one pair per QUIC version (RFC 9001 section 5.8 and
/// RFC 9369 section 3.3.3). A client must have this before it can accept a Retry at all,
/// because ngtcp2 verifies the tag through the ordinary encryption path before it will look
/// at anything else in the packet (`ngtcp2_conn.c:5543-5548`).
fn derive_retry_key(version: u32) -> Result<OsslPacketKey> {
    // The trailing NUL of each string literal is not part of the key.
    let key: &[u8] = match version {
        sys::NGTCP2_PROTO_VER_V1 => &sys::NGTCP2_RETRY_KEY_V1[..16],
        sys::NGTCP2_PROTO_VER_V2 => &sys::NGTCP2_RETRY_KEY_V2[..16],
        _ => return Err(Error::backend("no Retry key is defined for this version")),
    };
    OsslPacketKey::for_encryption(&Suite::retry(), key)
}

/// How a peer's certificate should be checked.
///
/// # What this means per role
///
/// For a **client**, [`Verify::Peer`] verifies the server's certificate chain against the
/// configured trust anchors *and* checks it was issued for the requested name. That is the
/// default, and the reason a verifying client must be given a server name.
///
/// For a **server**, [`Verify::Peer`] means "request no client certificate", because
/// ordinary QUIC clients present none and demanding one would reject every one of them.
/// Mutual TLS is not implemented; a server that needs it should not silently get something
/// weaker, so [`Verify::RequireClientCertificate`] exists and returns an error rather than
/// pretending.
/// Open rather than closed: a backend may grow a verification mode -- this one already
/// grew `RequireClientCertificate` -- and adding another must not break a caller that names
/// the ones it knows.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
#[non_exhaustive]
pub enum Verify {
    /// Verify the peer where there is a peer certificate to verify. The default.
    ///
    /// See the note above: this is asymmetric, because the roles are.
    #[default]
    Peer,
    /// Require and verify a client certificate. **Servers only, and not yet implemented.**
    ///
    /// Present so that asking for mutual TLS fails loudly rather than silently producing a
    /// server that accepts anyone.
    RequireClientCertificate,
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

        if self.verify == Verify::RequireClientCertificate {
            return Err(Error::invalid_input(
                "mutual TLS is not implemented; a server cannot require a client certificate",
            ));
        }

        let mode = match self.verify {
            Verify::Peer if self.role == Role::Client => sys::SSL_VERIFY_PEER as c_int,
            // A server that demanded a client certificate would reject every ordinary
            // client, so `Verify::Peer` on a server means "request none" -- documented on
            // the enum, and `RequireClientCertificate` is refused above rather than
            // silently downgraded to this.
            _ => sys::SSL_VERIFY_NONE as c_int,
        };
        // SAFETY: `ctx` is valid; a null callback means "use the default decision".
        unsafe { sys::SSL_CTX_set_verify(ctx.0, mode, None) };

        let mut alpn_offers = None;
        if self.role == Role::Server {
            // The selection callback reads the offer list through the callback argument,
            // which must stay at a fixed address for as long as any session made from this
            // context can run. Owning it here -- rather than leaking it, as an earlier
            // version did -- means it is freed when the backend is, and the field order
            // below ensures `SSL_CTX_free` runs first.
            let offers = Box::new(alpn_wire.clone());
            let ptr: *const Vec<u8> = &*offers;
            // SAFETY: `ctx` is valid, and `offers` is owned by the backend being returned,
            // so the pointer stays valid for the context's whole life.
            unsafe {
                sys::SSL_CTX_set_alpn_select_cb(
                    ctx.0,
                    Some(alpn_select_cb),
                    ptr.cast_mut().cast::<c_void>(),
                )
            };
            alpn_offers = Some(offers);
        }

        Ok(OsslBackend {
            ctx,
            alpn_wire,
            role: self.role,
            verify: self.verify,
            _alpn_offers: alpn_offers,
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
    /// Declared first so it is dropped first: `SSL_CTX_free` must run before the ALPN
    /// buffer its selection callback points at.
    ctx: SslCtx,
    alpn_wire: Vec<u8>,
    role: Role,
    verify: Verify,
    /// The server's ALPN offer list, at a fixed address for the selection callback.
    ///
    /// `Box<Vec<u8>>` rather than `Vec<u8>` deliberately, despite what the lint suggests:
    /// the callback recovers a `*const Vec<u8>` and dereferences it, so the `Vec` *struct*
    /// must not move. It would, when this backend is returned by value. Boxing pins it.
    #[allow(clippy::box_collection)]
    _alpn_offers: Option<Box<Vec<u8>>>,
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

    /// Turns a hexadecimal string into bytes, for transcribing published test vectors.
    fn hex(s: &str) -> Vec<u8> {
        assert!(
            s.len().is_multiple_of(2),
            "a hexadecimal vector has an even length"
        );
        (0..s.len() / 2)
            .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hexadecimal"))
            .collect()
    }

    #[test]
    fn the_initial_suite_has_the_dimensions_quic_fixes() {
        // AES-128-GCM with SHA-256, which QUIC mandates for Initial packets in every
        // version. If any of these is wrong every derived key is the wrong length and
        // nothing interoperates.
        let suite = Suite::initial();
        assert_eq!(suite.key_len(), 16);
        assert_eq!(suite.iv_len(), 12);
        assert_eq!(suite.tag_len(), 16);
        assert_eq!(suite.hash_len(), 32);
    }

    #[test]
    fn the_initial_suite_reports_no_usage_limits() {
        // Matching `ngtcp2_crypto_ctx_initial` exactly. Pinned because the seam documents
        // zero as a trap, and this is the one place where zero is correct: ngtcp2 checks
        // these only in the application packet number space.
        let suite = Suite::initial();
        assert_eq!(suite.max_encryption, 0);
        assert_eq!(suite.max_decryption_failure, 0);
    }

    #[test]
    fn a_payload_is_protected_and_recovered_in_one_buffer() {
        // The seam protects in place because ngtcp2's callbacks may pass the same pointer as
        // both source and destination, and two overlapping slices cannot be formed in safe
        // Rust. This is that case, exercised deliberately rather than incidentally.
        let suite = Suite::initial();
        let key = vec![0x2a; suite.key_len()];
        let nonce = vec![0x11; suite.iv_len()];
        let aad = b"a packet header";
        let plaintext = b"the payload of a QUIC packet";

        let seal = OsslPacketKey::for_encryption(&suite, &key).unwrap();
        let open = OsslPacketKey::for_decryption(&suite, &key).unwrap();

        let mut buf = vec![0u8; plaintext.len() + seal.tag_len()];
        buf[..plaintext.len()].copy_from_slice(plaintext);
        seal.seal(&mut buf, plaintext.len(), &nonce, aad).unwrap();
        assert_ne!(&buf[..plaintext.len()], &plaintext[..]);

        let protected_len = buf.len();
        let recovered = open.open(&mut buf, protected_len, &nonce, aad).unwrap();
        assert_eq!(recovered, plaintext.len());
        assert_eq!(&buf[..recovered], &plaintext[..]);
    }

    #[test]
    fn a_forged_payload_reports_a_failed_decryption_rather_than_a_failed_backend() {
        // The distinction the whole error type exists for. Anyone who can send a datagram
        // can produce this; reporting it as fatal would hand them the connection.
        let suite = Suite::initial();
        let key = vec![0x2a; suite.key_len()];
        let nonce = vec![0x11; suite.iv_len()];
        let open = OsslPacketKey::for_decryption(&suite, &key).unwrap();

        let mut buf = vec![0u8; 32];
        let len = buf.len();
        assert_eq!(
            open.open(&mut buf, len, &nonce, b"").unwrap_err(),
            CryptoError::Decrypt
        );
    }

    #[test]
    fn a_payload_too_short_to_hold_a_tag_is_a_failed_decryption() {
        // A truncating attacker's packet, not a broken backend.
        let suite = Suite::initial();
        let key = vec![0x2a; suite.key_len()];
        let nonce = vec![0x11; suite.iv_len()];
        let open = OsslPacketKey::for_decryption(&suite, &key).unwrap();

        let mut buf = vec![0u8; 8];
        assert_eq!(
            open.open(&mut buf, 8, &nonce, b"").unwrap_err(),
            CryptoError::Decrypt
        );
    }

    #[test]
    fn sealing_into_a_buffer_with_no_room_for_the_tag_fails_rather_than_overflowing() {
        // The helper writes the tag past the plaintext without checking (`ossl.c:912-916`),
        // so this bound is load-bearing rather than defensive.
        let suite = Suite::initial();
        let key = vec![0x2a; suite.key_len()];
        let nonce = vec![0x11; suite.iv_len()];
        let seal = OsslPacketKey::for_encryption(&suite, &key).unwrap();

        let mut buf = vec![0u8; 16];
        assert_eq!(
            seal.seal(&mut buf, 16, &nonce, b"").unwrap_err(),
            CryptoError::Fatal
        );
    }

    #[test]
    fn a_key_of_the_wrong_length_is_rejected() {
        let suite = Suite::initial();
        assert!(OsslPacketKey::for_encryption(&suite, &[0u8; 8]).is_err());
        assert!(OsslHeaderKey::new(&suite, &[0u8; 8]).is_err());
    }

    #[test]
    fn a_header_mask_needs_a_full_sample() {
        // The helper reads `NGTCP2_HP_SAMPLELEN` bytes regardless of what it is told, so a
        // short sample would be an out-of-bounds read rather than a short answer.
        let suite = Suite::initial();
        let key = vec![0x2a; suite.key_len()];
        let header = OsslHeaderKey::new(&suite, &key).unwrap();
        assert_eq!(header.mask(&[0u8; 8]).unwrap_err(), CryptoError::Fatal);
        assert!(header.mask(&[0u8; HP_SAMPLE_LEN]).is_ok());
    }

    #[test]
    fn the_two_sides_derive_each_others_initial_keys() {
        // Not a known-answer test -- the vectors below are that -- but the property that
        // actually has to hold on the wire: what a client encrypts with is what a server
        // decrypts with. It is checked through the public seam, so it also covers the
        // encryption/decryption context split.
        let dcid = hex("8394c8f03e515708");
        let version = sys::NGTCP2_PROTO_VER_V1;
        let client = derive_initial_keys(Role::Client, version, &dcid).unwrap();
        let server = derive_initial_keys(Role::Server, version, &dcid).unwrap();

        assert_eq!(client.tx.iv, server.rx.iv);
        assert_eq!(client.rx.iv, server.tx.iv);

        let plaintext = b"a client's first flight";
        let mut buf = vec![0u8; plaintext.len() + client.tx.packet.tag_len()];
        buf[..plaintext.len()].copy_from_slice(plaintext);
        client
            .tx
            .packet
            .seal(&mut buf, plaintext.len(), &client.tx.iv, b"header")
            .unwrap();
        let protected_len = buf.len();
        let len = server
            .rx
            .packet
            .open(&mut buf, protected_len, &server.rx.iv, b"header")
            .unwrap();
        assert_eq!(&buf[..len], &plaintext[..]);
    }

    #[test]
    fn a_retry_key_exists_for_every_version_quic_defines_one_for() {
        assert!(derive_retry_key(sys::NGTCP2_PROTO_VER_V1).is_ok());
        assert!(derive_retry_key(sys::NGTCP2_PROTO_VER_V2).is_ok());
        assert!(derive_retry_key(0xdead_beef).is_err());
    }

    #[test]
    fn a_connection_identifier_longer_than_quic_allows_is_rejected() {
        let too_long = vec![0u8; sys::NGTCP2_MAX_CIDLEN as usize + 1];
        assert!(derive_initial_keys(Role::Client, sys::NGTCP2_PROTO_VER_V1, &too_long).is_err());
    }

    /// The connection identifier every published Initial vector is derived from.
    const VECTOR_DCID: &str = "8394c8f03e515708";

    /// Builds the AEAD nonce the way QUIC does: the packet number, right-aligned in a
    /// zero-padded field the width of the initialisation vector, exclusive-ored with it
    /// (RFC 9001 section 5.3).
    ///
    /// Reproduced here rather than exposed on the seam because ngtcp2 constructs it itself
    /// and hands the result to the key — the seam never sees a packet number. Getting it
    /// right here is what makes the vectors below meaningful.
    fn nonce(iv: &[u8], packet_number: u64) -> Vec<u8> {
        let mut nonce = iv.to_vec();
        let pn = packet_number.to_be_bytes();
        let offset = nonce.len() - pn.len();
        for (n, p) in nonce[offset..].iter_mut().zip(pn) {
            *n ^= p;
        }
        nonce
    }

    #[test]
    fn the_version_1_initial_keys_match_rfc_9001() {
        // RFC 9001 appendix A.1. These are the published values, not this implementation's
        // own output -- which is the entire point: a test written from what the code
        // produces proves only that the code is consistent with itself, and a QUIC
        // implementation that is merely self-consistent talks to nobody.
        //
        // The initialisation vectors are compared directly. The payload and header keys
        // cannot be, because they are consumed into OpenSSL cipher contexts and never come
        // back out -- so they are checked below through what they produce, which is a
        // stronger claim anyway.
        let dcid = hex(VECTOR_DCID);
        let v1 = sys::NGTCP2_PROTO_VER_V1;
        let client = derive_initial_keys(Role::Client, v1, &dcid).unwrap();
        let server = derive_initial_keys(Role::Server, v1, &dcid).unwrap();

        assert_eq!(client.tx.iv, hex("fa044b2f42a3fd3b46fb255c"));
        assert_eq!(server.tx.iv, hex("0ac1493ca1905853b0bba03e"));
        // The roles agree on which key is whose, which is what actually has to hold.
        assert_eq!(client.rx.iv, server.tx.iv);
        assert_eq!(server.rx.iv, client.tx.iv);
    }

    #[test]
    fn the_version_1_header_masks_match_rfc_9001() {
        // RFC 9001 appendices A.2 and A.3: the sample taken from each side's protected
        // Initial packet, and the mask the published header protection key produces from it.
        // This is what proves the `quic hp` derivation, since the key itself is unreadable.
        let dcid = hex(VECTOR_DCID);
        let v1 = sys::NGTCP2_PROTO_VER_V1;
        let client = derive_initial_keys(Role::Client, v1, &dcid).unwrap();
        let server = derive_initial_keys(Role::Server, v1, &dcid).unwrap();

        let client_mask = client
            .tx
            .header
            .mask(&hex("d1b1c98dd7689fb8ec11d242b123dc9b"))
            .unwrap();
        assert_eq!(client_mask.as_slice(), hex("437b9aec36").as_slice());

        let server_mask = server
            .tx
            .header
            .mask(&hex("2cd0991cd25b0aac406a5816b6394100"))
            .unwrap();
        assert_eq!(server_mask.as_slice(), hex("2ec0d8356a").as_slice());
    }

    #[test]
    fn the_version_1_server_initial_packet_matches_rfc_9001() {
        // RFC 9001 appendix A.3, end to end: the server's real Initial packet, protected
        // with the real key, compared byte for byte against the RFC's ciphertext. This is
        // the vector that covers the payload key derivation, the nonce construction and the
        // AEAD together -- and it is a *known answer*, so nothing here can agree with itself
        // and be wrong.
        //
        // The additional authenticated data is the header **before** header protection is
        // applied, which is the subtlety most easily got backwards.
        let dcid = hex(VECTOR_DCID);
        let v1 = sys::NGTCP2_PROTO_VER_V1;
        let server = derive_initial_keys(Role::Server, v1, &dcid).unwrap();
        let client = derive_initial_keys(Role::Client, v1, &dcid).unwrap();

        let header = hex("c1000000010008f067a5502a4262b50040750001");
        let plaintext = hex(
            "02000000000600405a020000560303eefce7f7b37ba1d1632e96677825ddf73988cfc7\
             9825df566dc5430b9a045a1200130100002e00330024001d00209d3c940d89690b84d0\
             8a60993c144eca684d1081287c834d5311bcf32bb9da1a002b00020304",
        );
        let expected = hex(
            "5a482cd0991cd25b0aac406a5816b6394100f37a1c69797554780bb38cc5a99f5ede4c\
             f73c3ec2493a1839b3dbcba3f6ea46c5b7684df3548e7ddeb9c3bf9c73cc3f3bded74b\
             562bfb19fb84022f8ef4cdd93795d77d06edbb7aaf2f58891850abbdca3d20398c2764\
             56cbc42158407dd074ee",
        );

        let nonce = nonce(&server.tx.iv, 1);
        let mut buf = vec![0u8; plaintext.len() + server.tx.packet.tag_len()];
        buf[..plaintext.len()].copy_from_slice(&plaintext);
        server
            .tx
            .packet
            .seal(&mut buf, plaintext.len(), &nonce, &header)
            .unwrap();
        assert_eq!(buf, expected);

        // And the other side recovers it, using the key it derived independently.
        let protected_len = buf.len();
        let client_nonce = super::tests::nonce(&client.rx.iv, 1);
        let recovered = client
            .rx
            .packet
            .open(&mut buf, protected_len, &client_nonce, &header)
            .unwrap();
        assert_eq!(&buf[..recovered], plaintext.as_slice());
    }

    #[test]
    fn the_version_2_initial_keys_match_rfc_9369() {
        // RFC 9369 appendix A.1. Version 2 changes the Initial salt *and* the key schedule
        // labels -- `quicv2 key` rather than `quic key`. Deriving version 2 keys with
        // version 1 labels produces perfectly plausible keys that decrypt nothing, and this
        // is the only test that would notice.
        let dcid = hex(VECTOR_DCID);
        let v2 = sys::NGTCP2_PROTO_VER_V2;
        let client = derive_initial_keys(Role::Client, v2, &dcid).unwrap();
        let server = derive_initial_keys(Role::Server, v2, &dcid).unwrap();

        assert_eq!(client.tx.iv, hex("91f73e2351d8fa91660e909f"));
        assert_eq!(server.tx.iv, hex("dd13c276499c0249d3310652"));

        // And they differ from version 1's, which is what makes the assertion above mean
        // something rather than merely pass.
        let v1 = derive_initial_keys(Role::Client, sys::NGTCP2_PROTO_VER_V1, &dcid).unwrap();
        assert_ne!(client.tx.iv, v1.tx.iv);
    }

    #[test]
    fn the_version_2_header_masks_match_rfc_9369() {
        // RFC 9369 appendices A.2 and A.3.
        let dcid = hex(VECTOR_DCID);
        let v2 = sys::NGTCP2_PROTO_VER_V2;
        let client = derive_initial_keys(Role::Client, v2, &dcid).unwrap();
        let server = derive_initial_keys(Role::Server, v2, &dcid).unwrap();

        let client_mask = client
            .tx
            .header
            .mask(&hex("ffe67b6abcdb4298b485dd04de806071"))
            .unwrap();
        assert_eq!(client_mask.as_slice(), hex("94a0c95e80").as_slice());

        let server_mask = server
            .tx
            .header
            .mask(&hex("6f05d8a4398c47089698baeea26b91eb"))
            .unwrap();
        assert_eq!(server_mask.as_slice(), hex("4dd92e91ea").as_slice());
    }

    #[test]
    fn the_version_2_server_initial_packet_matches_rfc_9369() {
        // RFC 9369 appendix A.3 -- the same end-to-end claim as for version 1, against the
        // version 2 salt and labels.
        let dcid = hex(VECTOR_DCID);
        let v2 = sys::NGTCP2_PROTO_VER_V2;
        let server = derive_initial_keys(Role::Server, v2, &dcid).unwrap();

        let header = hex("d16b3343cf0008f067a5502a4262b50040750001");
        let plaintext = hex(
            "02000000000600405a020000560303eefce7f7b37ba1d1632e96677825ddf73988cfc7\
             9825df566dc5430b9a045a1200130100002e00330024001d00209d3c940d89690b84d0\
             8a60993c144eca684d1081287c834d5311bcf32bb9da1a002b00020304",
        );
        let protected_packet = hex(
            "dc6b3343cf0008f067a5502a4262b5004075d92faaf16f05d8a4398c47089698baeea2\
             6b91eb761d9b89237bbf87263017915358230035f7fd3945d88965cf17f9af6e16886c\
             61bfc703106fbaf3cb4cfa52382dd16a393e42757507698075b2c984c707f0a0812d8c\
             d5a6881eaf21ceda98f4bd23f6fe1a3e2c43edd9ce7ca84bed8521e2e140",
        );
        let expected = &protected_packet[header.len()..];

        let nonce = nonce(&server.tx.iv, 1);
        let mut buf = vec![0u8; plaintext.len() + server.tx.packet.tag_len()];
        buf[..plaintext.len()].copy_from_slice(&plaintext);
        server
            .tx
            .packet
            .seal(&mut buf, plaintext.len(), &nonce, &header)
            .unwrap();
        assert_eq!(buf.as_slice(), expected);
    }

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
