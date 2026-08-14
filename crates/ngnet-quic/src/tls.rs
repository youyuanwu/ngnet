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

    /// Tells the TLS session which connection it belongs to.
    ///
    /// The crypto helper reaches the `ngtcp2_conn` from inside its own callbacks, through a
    /// reference the TLS object carries. Without this the helper's callbacks fail, and the
    /// first thing that fails is the client's opening flight -- so this is not optional
    /// wiring, it is what makes a handshake possible at all.
    ///
    /// The backend decides how to represent the reference, because that representation is
    /// backend-specific.
    ///
    /// # Safety
    ///
    /// `conn` must be a live `ngtcp2_conn` that outlives this session.
    unsafe fn bind_connection(&mut self, conn: *mut c_void);

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

// ---------------------------------------------------------------------------------------
// The safe seam.
//
// Everything below replaces the two `unsafe` traits above. They coexist only while the
// backend is converted; nothing implements both.
// ---------------------------------------------------------------------------------------

/// The length of a header protection mask, from `ngtcp2.h:424`.
pub const HP_MASK_LEN: usize = 5;

/// The number of bytes sampled to produce that mask, from `ngtcp2.h:432`.
pub const HP_SAMPLE_LEN: usize = 16;

/// The stage of the handshake a key or a piece of handshake data belongs to.
///
/// Closed rather than open: QUIC defines exactly these four, and a fifth would be a new
/// protocol rather than a new variant.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Level {
    /// Before any handshake key exists. Protected with keys derived from a connection
    /// identifier, which is why an observer can read them.
    Initial,
    /// Early data. Not implemented by this crate, but part of the level space.
    ZeroRtt,
    /// The handshake itself, once the first keys are agreed.
    Handshake,
    /// Application data.
    OneRtt,
}

/// Which way a key protects.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction {
    /// Decrypting what the peer sent.
    Read,
    /// Encrypting what this endpoint sends.
    Write,
}

/// Why a cryptographic operation did not succeed.
///
/// The distinction is the whole point of the type. A packet that fails to decrypt is an
/// **ordinary event** on a QUIC connection: it may have been reordered past the point where
/// its key was discarded, or forged by anyone who can send a datagram. Treating it as fatal
/// hands any third party a way to close the connection, and would never show up in a
/// loopback test, which has neither reordering nor attackers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CryptoError {
    /// The payload did not authenticate. The packet is discarded and the connection
    /// continues.
    Decrypt,
    /// Something is wrong with the backend or its configuration. The connection ends.
    Fatal,
}

/// A key that protects one direction of one encryption level's payloads.
///
/// # Why this is an object rather than a method on the session
///
/// ngtcp2's `encrypt`, `decrypt` and `hp_mask` callbacks receive neither the connection nor
/// any user pointer (`ngtcp2.h:2824`, `:2853`, `:2882`). The only state they can reach is
/// what was stored in the cipher context handed to them. A key therefore has to be a
/// self-contained object, and this shape is forced by those signatures rather than chosen.
///
/// # Why protection happens in place
///
/// ngtcp2 says of both callbacks that "`dest` and `plaintext` may point to the same buffer"
/// (`ngtcp2.h:2818`, `:2846`) — and in the ordinary case they do, because it protects the
/// packet where it already sits. Two overlapping slices, one shared and one mutable, cannot
/// be formed in safe Rust at all. Working in place sidesteps that, and has the side benefit
/// of keeping the send path free of allocation.
///
/// # Why `'static` and `Send`
///
/// The object outlives every borrow of the session that made it: it is recovered from an
/// untyped handle long afterwards, and destroyed from inside `ngtcp2_conn_del`, which runs
/// before the session itself is dropped. A key that borrowed from its session would be a
/// dangling borrow with no lifetime left to catch it. `Send` is required because a
/// connection is `Send`, and state hidden behind a raw handle would otherwise escape that
/// bound.
pub trait PacketKey: Send + 'static {
    /// Protects `buf[..plaintext_len]` in place.
    ///
    /// On entry the plaintext occupies the first `plaintext_len` bytes. On success the
    /// ciphertext and its authentication tag occupy the first `plaintext_len + tag_len()`
    /// bytes, and `buf` must be at least that long.
    fn seal(
        &self,
        buf: &mut [u8],
        plaintext_len: usize,
        nonce: &[u8],
        aad: &[u8],
    ) -> core::result::Result<(), CryptoError>;

    /// Unprotects `buf[..ciphertext_len]` in place, returning the plaintext length.
    ///
    /// Returns [`CryptoError::Decrypt`] when the payload does not authenticate. That is not
    /// a failure of the backend and must not end the connection.
    fn open(
        &self,
        buf: &mut [u8],
        ciphertext_len: usize,
        nonce: &[u8],
        aad: &[u8],
    ) -> core::result::Result<usize, CryptoError>;

    /// How many bytes protection adds. ngtcp2 budgets every packet against this.
    fn tag_len(&self) -> usize;

    /// How many packets may be protected with this key before it must be retired.
    ///
    /// Not advisory. ngtcp2 compares a packet count against it and forces a key update, and
    /// a zero here means "immediately", so a backend that forgets it produces a connection
    /// that rekeys constantly rather than one that never does.
    fn confidentiality_limit(&self) -> u64;

    /// How many failed decryptions may be tolerated before the connection must close.
    ///
    /// Zero has the same trap as above, in the other direction: it makes the first forged
    /// packet fatal.
    fn integrity_limit(&self) -> u64;
}

/// A key that masks the parts of a packet header which are not covered by the payload AEAD.
///
/// Kept separate from [`PacketKey`] because ngtcp2 keeps them separate — they are installed
/// as different context objects, use different ciphers, and are rotated on different
/// schedules: a key update replaces packet keys and leaves header keys alone.
pub trait HeaderKey: Send + 'static {
    /// Derives the mask ngtcp2 will apply to a header, from a sample of the protected
    /// payload.
    ///
    /// `sample` is [`HP_SAMPLE_LEN`] bytes. Returning the mask rather than applying it is
    /// what ngtcp2 asks for; note that this is the one place a rustls-backed implementation
    /// would have work to do, since rustls only ever applies header protection in place and
    /// never surfaces the mask.
    fn mask(&self, sample: &[u8]) -> core::result::Result<[u8; HP_MASK_LEN], CryptoError>;
}

/// One direction's key material for one encryption level.
///
/// The initialisation vector travels alongside the keys rather than inside them because
/// ngtcp2 installs it separately and constructs each packet's nonce from it itself.
#[derive(Debug)]
pub struct DirectionalKeys<P, H> {
    /// Protects payloads.
    pub packet: P,
    /// Masks headers.
    pub header: H,
    /// The initialisation vector the nonce is built from.
    pub iv: Vec<u8>,
}

/// Both directions of the keys derived from a connection identifier.
///
/// Initial keys are the one part of QUIC's key schedule that owes nothing to the TLS
/// handshake: they come from the client's destination connection identifier and a
/// version-specific salt, which is why an observer can read Initial packets and why these
/// have to be derivable before any handshake byte has been exchanged.
#[derive(Debug)]
pub struct InitialKeys<P, H> {
    /// For decrypting the peer.
    pub rx: DirectionalKeys<P, H>,
    /// For encrypting to the peer.
    pub tx: DirectionalKeys<P, H>,
}

/// The next generation of application keys, produced by a key update.
///
/// A separate type from [`InitialKeys`], and deliberately narrower, because a key update is
/// a narrower operation than deriving a level's keys: it rotates payload protection only.
/// Header protection keys are **not** rotated (`shared.c:1049-1063` passes a null header key
/// to the derivation), so offering a place to put them would invite a backend to derive keys
/// that are then silently discarded.
///
/// The new traffic secrets are returned alongside the keys because ngtcp2 keeps them and
/// hands them back as the input to the *next* rotation. A backend that returns keys without
/// secrets has produced a connection that can rotate exactly once.
#[derive(Debug)]
pub struct RotatedKeys<P> {
    /// The new key for decrypting the peer.
    pub rx_packet: P,
    /// Its initialisation vector.
    pub rx_iv: Vec<u8>,
    /// The secret the generation after next is derived from.
    pub rx_secret: Vec<u8>,
    /// The new key for encrypting to the peer.
    pub tx_packet: P,
    /// Its initialisation vector.
    pub tx_iv: Vec<u8>,
    /// The secret the generation after next is derived from.
    pub tx_secret: Vec<u8>,
}

/// Everything a session tells the connection about, in the order it happened.
///
/// A single ordered stream rather than a set of accessors, because order is load-bearing and
/// easy to lose. A TLS stack reports a secret, some handshake bytes, the peer's transport
/// parameters and possibly an alert from inside one call, and the connection must apply them
/// in that order: keys before the bytes they protect, transport parameters before the keys
/// whose installation depends on them being set.
#[derive(Debug)]
pub enum SessionEvent<P, H> {
    /// Handshake bytes to send at this level.
    Handshake {
        /// The level to send them at.
        level: Level,
        /// The bytes.
        data: Vec<u8>,
    },
    /// A key became available.
    Keys {
        /// Which level it protects.
        level: Level,
        /// Which direction it protects.
        direction: Direction,
        /// The key material.
        keys: DirectionalKeys<P, H>,
        /// The traffic secret it was derived from, which a key update rotates.
        secret: Vec<u8>,
    },
    /// The peer's transport parameters, exactly as they arrived.
    PeerTransportParams(Vec<u8>),
    /// The TLS handshake completed. Distinct from the QUIC handshake being confirmed.
    HandshakeComplete,
    /// The peer must be told the handshake failed, with this alert code.
    Alert(u8),
}

/// One connection's TLS state.
///
/// # What this trait deliberately cannot do
///
/// It cannot reach the connection. Every ngtcp2 call the handshake requires — installing
/// keys, submitting handshake data, setting the alert, decoding transport parameters — is
/// made by the crate, from what this trait reports. That is the difference between this and
/// the seam it replaces, and it is what makes implementing it safe: there is no pointer to
/// misuse and no ordering to get wrong, because the ordering is not the backend's to choose.
///
/// It also has no source of randomness. The connection already has one, and the reason it
/// has exactly one is that two could diverge.
pub trait Session: Send + 'static {
    /// The payload protection key this backend produces.
    ///
    /// A concrete type rather than a boxed trait object, for a mechanical reason: the key is
    /// stored in a single untyped pointer, and a `Box<dyn Trait>` is two words wide. It
    /// would not fit. Monomorphisation also means only this session's keys can ever reach
    /// this session's callbacks, so a key from one backend cannot be handed to another.
    type PacketKey: PacketKey;

    /// The header protection key this backend produces.
    type HeaderKey: HeaderKey;

    /// Derives the Initial keys for a connection identifier and QUIC version.
    ///
    /// Called before the handshake starts, and again if the server sends a Retry — in which
    /// case the identifier is the one the Retry carried, and the previous Initial keys are
    /// discarded. Also called for a version the peer negotiated down to.
    fn initial_keys(
        &mut self,
        version: u32,
        dcid: &[u8],
    ) -> Result<InitialKeys<Self::PacketKey, Self::HeaderKey>>;

    /// The fixed key that authenticates a Retry packet's integrity tag.
    ///
    /// Clients only, and not optional for them: ngtcp2 verifies the tag before it will
    /// accept a Retry at all, using this key through the ordinary encryption path. The key
    /// is not secret — it is a constant in the specification, one per version — so this
    /// exists to say which constant, not to protect anything.
    fn retry_key(&mut self, version: u32) -> Result<Self::PacketKey>;

    /// Hands the session the local transport parameters to send.
    ///
    /// Called before they are needed, which for a client is before the first flight and for
    /// a server is before its handshake keys are installed.
    fn set_local_transport_params(&mut self, params: &[u8]) -> Result<()>;

    /// Starts the handshake. Clients only; a server starts when the client's first flight
    /// arrives.
    fn start_handshake(&mut self) -> Result<()>;

    /// Feeds the session handshake bytes that arrived at `level`.
    fn read_handshake(&mut self, level: Level, data: &[u8]) -> Result<()>;

    /// Takes the next thing the session has to report, or `None` when it has nothing.
    ///
    /// Drained to exhaustion after every call that can produce events.
    fn poll_event(&mut self) -> Option<SessionEvent<Self::PacketKey, Self::HeaderKey>>;

    /// Rotates the application keys, given the secrets currently in use.
    ///
    /// Returns the next generation for both directions, with the secrets the generation
    /// after that will be derived from. Only the session can do this: it alone knows which
    /// hash the negotiated cipher suite uses.
    fn rotate_keys(
        &mut self,
        rx_secret: &[u8],
        tx_secret: &[u8],
    ) -> Result<RotatedKeys<Self::PacketKey>>;

    /// The application protocol the handshake agreed on, once it has one.
    fn negotiated_alpn(&self) -> Option<Vec<u8>>;

    /// Why the handshake failed, in whatever terms the backend can offer.
    ///
    /// Exists so a failure can name certificate verification rather than only reporting that
    /// the handshake did not complete.
    fn failure_reason(&self) -> Option<String> {
        None
    }
}

/// A configured TLS stack, from which per-connection sessions are made.
pub trait Backend {
    /// The session type this backend produces.
    type Session: Session;

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

    /// The header protection lengths are restated here, so they must match the library's.
    ///
    /// They are restated rather than re-exported because they belong to the seam's
    /// vocabulary: a backend author should not have to reach into the raw bindings to learn
    /// how long a sample is. That convenience is only safe while the two agree, and a
    /// mismatch would be a buffer of the wrong length handed to a cipher, so it is pinned
    /// rather than assumed.
    #[test]
    fn the_header_protection_lengths_match_the_library() {
        assert_eq!(
            HP_MASK_LEN,
            ngnet_quic_sys::NGTCP2_HP_MASKLEN as usize,
            "header protection mask length"
        );
        assert_eq!(
            HP_SAMPLE_LEN,
            ngnet_quic_sys::NGTCP2_HP_SAMPLELEN as usize,
            "header protection sample length"
        );
    }

    /// A failed decryption is not a failed backend, and the type has to keep them apart.
    ///
    /// Trivial as an assertion, and worth having as a statement: every other error in this
    /// crate means something went wrong, and this one usually means the connection is
    /// working correctly in the presence of a reordered or forged packet.
    #[test]
    fn a_failed_decryption_is_distinguishable_from_a_fatal_error() {
        assert_ne!(CryptoError::Decrypt, CryptoError::Fatal);
    }

    /// Every encryption level is distinct, since they index key state.
    #[test]
    fn the_encryption_levels_are_distinct() {
        let levels = [
            Level::Initial,
            Level::ZeroRtt,
            Level::Handshake,
            Level::OneRtt,
        ];
        for (index, left) in levels.iter().enumerate() {
            for right in &levels[index + 1..] {
                assert_ne!(left, right, "two encryption levels compare equal");
            }
        }
        assert_ne!(Direction::Read, Direction::Write);
    }
}
