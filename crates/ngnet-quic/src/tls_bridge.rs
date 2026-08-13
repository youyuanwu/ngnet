//! The generic translation between ngtcp2's crypto callbacks and the safe TLS seam.
//!
//! This module is the whole reason [`crate::tls::Session`] can be safe. Everything ngtcp2
//! asks of a TLS stack arrives here as a C callback, and everything the handshake has to
//! tell ngtcp2 leaves here as a C call. A backend sees none of it: it is asked questions and
//! it answers them.
//!
//! # How a callback finds its session
//!
//! Not through `user_data`. ngtcp2 hands `user_data` to the transport callbacks, where
//! [`crate::callbacks`] uses it to reach the application's handlers, and reusing it here
//! would mean two different things wanting the same pointer.
//!
//! Instead the session is reached through the connection itself:
//! `ngtcp2_conn_set_tls_native_handle` stores a pointer ngtcp2 describes as "an opaque
//! pointer" and only ever hands back (`ngtcp2_conn.c:14149-14159`, verified — it stores and
//! returns it and does nothing else). Every callback that needs the session receives the
//! `ngtcp2_conn`, so every callback can recover it.
//!
//! That is not merely convenient, it removes a hazard. Extending the existing `Bridge` to
//! carry the session would have put a second `&mut` route to the same object behind a slot
//! that already hands out `&mut Bridge` — and the delete callbacks fire from *inside* other
//! callbacks when keys are discarded (`ngtcp2_conn.c:1727-1729`), which is exactly where two
//! live `&mut` would meet.
//!
//! # The delete callbacks touch nothing but their own argument
//!
//! They reconstruct the boxed key from the context handle and drop it. They do not look at
//! the session, the connection, or `user_data`. This is what makes them sound both at
//! teardown, when no session may be reachable, and mid-callback, when one is already
//! borrowed.
//!
//! # Which callbacks are here, and which deliberately are not
//!
//! `handshake_completed` is **not** a crypto callback. It is the application's, already
//! wired in [`crate::conn`], and telling ngtcp2 that TLS finished is a *call* —
//! `ngtcp2_conn_tls_handshake_completed` — not a callback.
//!
//! `get_path_challenge_data` is not here either. It needs unpredictable bytes, and the
//! connection already has a source of those in [`crate::rand`]; a second one on the TLS seam
//! would be two sources that could diverge.
//!
//! # Re-entrancy, and why it is allowed here
//!
//! [`crate::callbacks`] documents that ngtcp2 forbids re-entering it from a callback. That
//! is true of the *packet-processing* entry points — `read_pkt`, `writev_stream` and
//! `write_connection_close` all say so. It is not true of the calls below: ngtcp2 requires
//! `ngtcp2_conn_install_initial_key` and `ngtcp2_conn_submit_crypto_data` to be called from
//! inside `client_initial` and `recv_client_initial` (`ngtcp2.h:2641-2648`, `:2660-2666`).
//! Two different API subsets, and only one of them is forbidden.

// The bridge is consumed by the connection once the OpenSSL backend has moved onto the safe
// seam. It is written first, and separately, because it is the part whose correctness is
// hardest to review while a backend rewrite is also in flight.
#![allow(dead_code)]

use core::ffi::{c_int, c_void};

use ngnet_quic_sys as sys;

use crate::tls::{
    CryptoError, Direction, DirectionalKeys, HP_MASK_LEN, HP_SAMPLE_LEN, HeaderKey, InitialKeys,
    Level, PacketKey, Session, SessionEvent,
};

/// Recovers the session a connection was given.
///
/// # Safety
///
/// `conn` must be a live connection whose TLS native handle was set to a `*mut S` that is
/// still alive, and no other reference to that session may be live for the duration of the
/// returned borrow. Both hold inside a callback: the connection is mutably borrowed by the
/// call that is running, and the connection does not touch its own session field while
/// ngtcp2 is inside it.
unsafe fn session<'a, S: Session>(conn: *mut sys::ngtcp2_conn) -> Option<&'a mut S> {
    // SAFETY: the caller guarantees `conn` is live; the handle is one this crate stored.
    let handle = unsafe { sys::ngtcp2_conn_get_tls_native_handle(conn) };
    if handle.is_null() {
        return None;
    }
    // SAFETY: the handle is a `*mut S` this crate set, and the caller guarantees exclusivity.
    Some(unsafe { &mut *handle.cast::<S>() })
}

/// Wraps a key object in the C struct ngtcp2 stores it in.
///
/// The box is deliberately a thin pointer: `native_handle` is one word, and a trait object
/// would be two. This is why the seam uses concrete associated types.
fn box_packet_key<K: PacketKey>(key: K) -> sys::ngtcp2_crypto_aead_ctx {
    sys::ngtcp2_crypto_aead_ctx {
        native_handle: Box::into_raw(Box::new(key)).cast::<c_void>(),
    }
}

/// The same, for header protection.
fn box_header_key<K: HeaderKey>(key: K) -> sys::ngtcp2_crypto_cipher_ctx {
    sys::ngtcp2_crypto_cipher_ctx {
        native_handle: Box::into_raw(Box::new(key)).cast::<c_void>(),
    }
}

/// Reclaims a boxed key that was never handed over.
///
/// ngtcp2 takes ownership of a key context **only when the install succeeds**; on failure
/// "the caller is responsible to delete them" (`ngtcp2.h:4524-4528`). Nothing will call the
/// delete callback for a key that never got in, so without this it simply leaks.
///
/// # Safety
///
/// The handles must be ones [`box_packet_key`] and [`box_header_key`] produced for `K` and
/// `H`, and must not have been handed to ngtcp2 successfully.
unsafe fn reclaim<K: PacketKey, H: HeaderKey>(
    packet: &sys::ngtcp2_crypto_aead_ctx,
    header: &sys::ngtcp2_crypto_cipher_ctx,
) {
    if !packet.native_handle.is_null() {
        // SAFETY: the caller guarantees this is a `Box<K>` this module made.
        drop(unsafe { Box::from_raw(packet.native_handle.cast::<K>()) });
    }
    if !header.native_handle.is_null() {
        // SAFETY: the caller guarantees this is a `Box<H>` this module made.
        drop(unsafe { Box::from_raw(header.native_handle.cast::<H>()) });
    }
}

/// Describes a key's algorithm to ngtcp2.
///
/// Only three fields of this struct are read by ngtcp2's core: the AEAD's `max_overhead`,
/// which every packet's length is budgeted against, and the two usage limits, which packet
/// counts are compared against. The `native_handle` fields stay null because the core never
/// dereferences them (`ngtcp2_conn.c:543-564` null-checks and nothing more) and because the
/// only code that would use them — this crate's own callbacks — reaches its state through
/// the *context* handles instead.
fn crypto_ctx<K: PacketKey>(key: &K) -> sys::ngtcp2_crypto_ctx {
    let mut ctx: sys::ngtcp2_crypto_ctx = unsafe { core::mem::zeroed() };
    ctx.aead.max_overhead = key.tag_len();
    ctx.max_encryption = key.confidentiality_limit();
    ctx.max_decryption_failure = key.integrity_limit();
    ctx
}

/// Installs the Initial keys for both directions.
///
/// # Safety
///
/// `conn` must be live.
unsafe fn install_initial<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    keys: InitialKeys<S::PacketKey, S::HeaderKey>,
) -> c_int {
    let ctx = crypto_ctx(&keys.rx.packet);
    // SAFETY: `conn` is live; the context is a value ngtcp2 copies.
    unsafe { sys::ngtcp2_conn_set_initial_crypto_ctx(conn, &raw const ctx) };

    let ivlen = keys.rx.iv.len();
    if ivlen != keys.tx.iv.len() {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    }

    let rx_packet = box_packet_key(keys.rx.packet);
    let rx_header = box_header_key(keys.rx.header);
    let tx_packet = box_packet_key(keys.tx.packet);
    let tx_header = box_header_key(keys.tx.header);

    // SAFETY: `conn` is live, the contexts are freshly boxed, and the IVs outlive the call
    // (ngtcp2 copies them).
    let rv = unsafe {
        sys::ngtcp2_conn_install_initial_key(
            conn,
            &raw const rx_packet,
            keys.rx.iv.as_ptr(),
            &raw const rx_header,
            &raw const tx_packet,
            keys.tx.iv.as_ptr(),
            &raw const tx_header,
            ivlen,
        )
    };

    if rv != 0 {
        // SAFETY: the install failed, so ngtcp2 took ownership of none of them.
        unsafe {
            reclaim::<S::PacketKey, S::HeaderKey>(&rx_packet, &rx_header);
            reclaim::<S::PacketKey, S::HeaderKey>(&tx_packet, &tx_header);
        }
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    }
    0
}

/// Installs one direction's keys for a level above Initial.
///
/// # Safety
///
/// `conn` must be live.
unsafe fn install_level<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    level: Level,
    direction: Direction,
    keys: DirectionalKeys<S::PacketKey, S::HeaderKey>,
    secret: &[u8],
) -> c_int {
    let ctx = crypto_ctx(&keys.packet);
    // The crypto context is per packet number space rather than per direction, so setting it
    // twice with matching values is harmless; setting it not at all leaves ngtcp2 budgeting
    // packets against a zero overhead.
    match level {
        Level::ZeroRtt => {
            // SAFETY: `conn` is live; ngtcp2 copies the context.
            unsafe { sys::ngtcp2_conn_set_0rtt_crypto_ctx(conn, &raw const ctx) };
        }
        Level::Handshake | Level::OneRtt => {
            // SAFETY: as above.
            unsafe { sys::ngtcp2_conn_set_crypto_ctx(conn, &raw const ctx) };
        }
        Level::Initial => {}
    }

    let ivlen = keys.iv.len();
    let packet = box_packet_key(keys.packet);
    let header = box_header_key(keys.header);
    let iv = keys.iv.as_ptr();

    // SAFETY: `conn` is live, contexts are freshly boxed, and ngtcp2 copies the IV and the
    // secret.
    let rv = unsafe {
        match (level, direction) {
            (Level::Handshake, Direction::Read) => sys::ngtcp2_conn_install_rx_handshake_key(
                conn,
                &raw const packet,
                iv,
                ivlen,
                &raw const header,
            ),
            (Level::Handshake, Direction::Write) => sys::ngtcp2_conn_install_tx_handshake_key(
                conn,
                &raw const packet,
                iv,
                ivlen,
                &raw const header,
            ),
            (Level::OneRtt, Direction::Read) => sys::ngtcp2_conn_install_rx_key(
                conn,
                secret.as_ptr(),
                secret.len(),
                &raw const packet,
                iv,
                ivlen,
                &raw const header,
            ),
            (Level::OneRtt, Direction::Write) => sys::ngtcp2_conn_install_tx_key(
                conn,
                secret.as_ptr(),
                secret.len(),
                &raw const packet,
                iv,
                ivlen,
                &raw const header,
            ),
            (Level::ZeroRtt, _) => {
                sys::ngtcp2_conn_install_0rtt_key(conn, &raw const packet, iv, ivlen, &raw const header)
            }
            (Level::Initial, _) => sys::NGTCP2_ERR_INVALID_ARGUMENT,
        }
    };

    if rv != 0 {
        // SAFETY: the install failed, so ownership did not transfer.
        unsafe { reclaim::<S::PacketKey, S::HeaderKey>(&packet, &header) };
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    }
    0
}

/// Maps an encryption level onto ngtcp2's.
const fn to_native(level: Level) -> sys::ngtcp2_encryption_level {
    match level {
        Level::Initial => sys::NGTCP2_ENCRYPTION_LEVEL_INITIAL,
        Level::ZeroRtt => sys::NGTCP2_ENCRYPTION_LEVEL_0RTT,
        Level::Handshake => sys::NGTCP2_ENCRYPTION_LEVEL_HANDSHAKE,
        Level::OneRtt => sys::NGTCP2_ENCRYPTION_LEVEL_1RTT,
    }
}

/// Maps ngtcp2's encryption level onto the seam's.
pub(crate) const fn from_native(level: sys::ngtcp2_encryption_level) -> Option<Level> {
    match level {
        sys::NGTCP2_ENCRYPTION_LEVEL_INITIAL => Some(Level::Initial),
        sys::NGTCP2_ENCRYPTION_LEVEL_0RTT => Some(Level::ZeroRtt),
        sys::NGTCP2_ENCRYPTION_LEVEL_HANDSHAKE => Some(Level::Handshake),
        sys::NGTCP2_ENCRYPTION_LEVEL_1RTT => Some(Level::OneRtt),
        _ => None,
    }
}

/// Applies everything the session has to report, in the order it reported it.
///
/// Order is the point. A secret has to be installed before the handshake bytes it protects
/// are submitted, and the local transport parameters have to be set before the key whose
/// installation depends on them. Draining into a queue and applying it out of order would
/// break both, silently.
///
/// # Safety
///
/// `conn` must be live and `session` must be its session.
unsafe fn drain<S: Session>(conn: *mut sys::ngtcp2_conn, session: &mut S) -> c_int {
    while let Some(event) = session.poll_event() {
        let rv = match event {
            SessionEvent::Handshake { level, data } => {
                // ngtcp2 copies the buffer it is given here (`ngtcp2.h:5970-5980`), unlike
                // stream data, so nothing has to be retained past this call.
                //
                // Submitting is all-or-nothing on purpose: there is no way to tell ngtcp2
                // that only part of a flight was accepted, and reporting bytes consumed that
                // were not submitted would lose handshake data with no error anywhere.
                // SAFETY: `conn` is live; the slice outlives the call.
                unsafe {
                    sys::ngtcp2_conn_submit_crypto_data(
                        conn,
                        to_native(level),
                        data.as_ptr(),
                        data.len(),
                    )
                }
            }
            SessionEvent::Keys {
                level,
                direction,
                keys,
                secret,
            } => {
                // SAFETY: `conn` is live.
                unsafe { install_level::<S>(conn, level, direction, keys, &secret) }
            }
            SessionEvent::PeerTransportParams(params) => {
                // SAFETY: `conn` is live; ngtcp2 decodes out of the slice during the call.
                unsafe {
                    sys::ngtcp2_conn_decode_and_set_remote_transport_params(
                        conn,
                        params.as_ptr(),
                        params.len(),
                    )
                }
            }
            SessionEvent::HandshakeComplete => {
                // SAFETY: `conn` is live.
                unsafe { sys::ngtcp2_conn_tls_handshake_completed(conn) };
                0
            }
            SessionEvent::Alert(code) => {
                // SAFETY: `conn` is live.
                unsafe { sys::ngtcp2_conn_set_tls_alert(conn, code) };
                0
            }
        };

        if rv != 0 {
            return rv;
        }
    }
    0
}

// ---------------------------------------------------------------------------------------
// The callbacks themselves.
//
// Each is generic over the session type, so a connection installs function pointers that
// can only be reached with its own backend's keys. Type confusion between two backends is
// therefore not expressible, which is what makes recovering a key from an untyped handle
// sound.
// ---------------------------------------------------------------------------------------

/// Derives and installs the client's Initial keys, then starts the handshake.
unsafe extern "C" fn client_initial<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback, so the connection is live and exclusively borrowed.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };

    // SAFETY: `conn` is live; the identifier is borrowed for the call only.
    let dcid = unsafe { &*sys::ngtcp2_conn_get_dcid(conn) };
    let version = unsafe { sys::ngtcp2_conn_get_client_chosen_version(conn) };
    let Ok(keys) = session.initial_keys(version, &dcid.data[..dcid.datalen]) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: `conn` is live.
    let rv = unsafe { install_initial::<S>(conn, keys) };
    if rv != 0 {
        return rv;
    }

    // A client verifies the integrity tag on a Retry before ngtcp2 will accept one, using
    // the ordinary encryption path with a fixed, per-version key. Omitting this does not
    // fail here; it fails much later, by making every Retry look corrupt.
    let Ok(retry) = session.retry_key(version) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    let retry_ctx = crypto_ctx(&retry);
    let retry_key = box_packet_key(retry);
    // SAFETY: `conn` is live and takes ownership of the context.
    unsafe { sys::ngtcp2_conn_set_retry_aead(conn, &raw const retry_ctx.aead, &raw const retry_key) };

    if session.start_handshake().is_err() {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    }
    // SAFETY: `conn` is live and `session` is its session.
    unsafe { drain(conn, session) }
}

/// Derives and installs the server's Initial keys from the identifier the client chose.
unsafe extern "C" fn recv_client_initial<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    dcid: *const sys::ngtcp2_cid,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 passes a valid identifier for the duration of the call.
    let dcid = unsafe { &*dcid };
    // SAFETY: `conn` is live.
    let version = unsafe { sys::ngtcp2_conn_get_negotiated_version(conn) };
    let Ok(keys) = session.initial_keys(version, &dcid.data[..dcid.datalen]) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: `conn` is live.
    unsafe { install_initial::<S>(conn, keys) }
}

/// Feeds arriving handshake bytes to the session and applies whatever comes back.
unsafe extern "C" fn recv_crypto_data<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    level: sys::ngtcp2_encryption_level,
    _offset: u64,
    data: *const u8,
    datalen: usize,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    let Some(level) = from_native(level) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 guarantees the slice for the duration of the call, and `datalen > 0`.
    let data = unsafe { core::slice::from_raw_parts(data, datalen) };

    if session.read_handshake(level, data).is_err() {
        return sys::NGTCP2_ERR_CRYPTO;
    }
    // SAFETY: `conn` is live and `session` is its session.
    unsafe { drain(conn, session) }
}

/// Re-derives the client's Initial keys after a Retry.
unsafe extern "C" fn recv_retry<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    hd: *const sys::ngtcp2_pkt_hd,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 passes a valid header for the duration of the call. The *source*
    // identifier is the one to derive from: a Retry tells the client to start again against
    // the identifier the server chose.
    let scid = unsafe { &(*hd).scid };
    // SAFETY: `conn` is live.
    let version = unsafe { sys::ngtcp2_conn_get_client_chosen_version(conn) };
    let Ok(keys) = session.initial_keys(version, &scid.data[..scid.datalen]) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: `conn` is live.
    unsafe { install_initial::<S>(conn, keys) }
}

/// Installs Initial keys for a version the peer negotiated.
unsafe extern "C" fn version_negotiation<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    version: u32,
    client_dcid: *const sys::ngtcp2_cid,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 passes a valid identifier for the duration of the call.
    let dcid = unsafe { &*client_dcid };
    let Ok(keys) = session.initial_keys(version, &dcid.data[..dcid.datalen]) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };

    let ivlen = keys.rx.iv.len();
    let rx_packet = box_packet_key(keys.rx.packet);
    let rx_header = box_header_key(keys.rx.header);
    let tx_packet = box_packet_key(keys.tx.packet);
    let tx_header = box_header_key(keys.tx.header);

    // SAFETY: `conn` is live; ngtcp2 copies the IVs.
    let rv = unsafe {
        sys::ngtcp2_conn_install_vneg_initial_key(
            conn,
            version,
            &raw const rx_packet,
            keys.rx.iv.as_ptr(),
            &raw const rx_header,
            &raw const tx_packet,
            keys.tx.iv.as_ptr(),
            &raw const tx_header,
            ivlen,
        )
    };
    if rv != 0 {
        // SAFETY: the install failed, so ownership did not transfer.
        unsafe {
            reclaim::<S::PacketKey, S::HeaderKey>(&rx_packet, &rx_header);
            reclaim::<S::PacketKey, S::HeaderKey>(&tx_packet, &tx_header);
        }
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    }
    0
}

/// Protects a payload in place.
///
/// Receives neither the connection nor `user_data`: the key is the only reachable state,
/// which is why the seam makes it an object.
unsafe extern "C" fn encrypt<S: Session>(
    dest: *mut u8,
    aead: *const sys::ngtcp2_crypto_aead,
    aead_ctx: *const sys::ngtcp2_crypto_aead_ctx,
    plaintext: *const u8,
    plaintextlen: usize,
    nonce: *const u8,
    noncelen: usize,
    aad: *const u8,
    aadlen: usize,
) -> c_int {
    // SAFETY: ngtcp2 passes a context this module boxed for `S::PacketKey`.
    let Some(key) = (unsafe { key_from::<S::PacketKey>(aead_ctx) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 guarantees `aead` for the call.
    let overhead = unsafe { (*aead).max_overhead };

    // ngtcp2 permits `dest` and `plaintext` to be the same buffer, and ordinarily they are.
    // Two overlapping slices cannot be formed in safe Rust, so the plaintext is moved into
    // the destination first and the key works in place. When they already alias, the copy is
    // skipped rather than being a self-overlapping memmove.
    // SAFETY: `dest` has room for the ciphertext and tag, per the callback's contract.
    let buf = unsafe { core::slice::from_raw_parts_mut(dest, plaintextlen + overhead) };
    if !core::ptr::eq(dest.cast_const(), plaintext) {
        // SAFETY: the regions do not overlap, and both are valid for `plaintextlen`.
        unsafe { core::ptr::copy_nonoverlapping(plaintext, dest, plaintextlen) };
    }
    // SAFETY: ngtcp2 guarantees both slices for the call.
    let nonce = unsafe { core::slice::from_raw_parts(nonce, noncelen) };
    // SAFETY: as above.
    let aad = unsafe { core::slice::from_raw_parts(aad, aadlen) };

    match key.seal(buf, plaintextlen, nonce, aad) {
        Ok(()) => 0,
        Err(_) => sys::NGTCP2_ERR_CALLBACK_FAILURE,
    }
}

/// Unprotects a payload in place.
unsafe extern "C" fn decrypt<S: Session>(
    dest: *mut u8,
    _aead: *const sys::ngtcp2_crypto_aead,
    aead_ctx: *const sys::ngtcp2_crypto_aead_ctx,
    ciphertext: *const u8,
    ciphertextlen: usize,
    nonce: *const u8,
    noncelen: usize,
    aad: *const u8,
    aadlen: usize,
) -> c_int {
    // SAFETY: ngtcp2 passes a context this module boxed for `S::PacketKey`.
    let Some(key) = (unsafe { key_from::<S::PacketKey>(aead_ctx) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: `dest` has room for the plaintext, which is shorter than the ciphertext.
    let buf = unsafe { core::slice::from_raw_parts_mut(dest, ciphertextlen) };
    if !core::ptr::eq(dest.cast_const(), ciphertext) {
        // SAFETY: the regions do not overlap, and both are valid for `ciphertextlen`.
        unsafe { core::ptr::copy_nonoverlapping(ciphertext, dest, ciphertextlen) };
    }
    // SAFETY: ngtcp2 guarantees both slices for the call.
    let nonce = unsafe { core::slice::from_raw_parts(nonce, noncelen) };
    // SAFETY: as above.
    let aad = unsafe { core::slice::from_raw_parts(aad, aadlen) };

    match key.open(buf, ciphertextlen, nonce, aad) {
        Ok(_) => 0,
        // The distinction the seam exists to preserve. A payload that does not authenticate
        // is an ordinary event -- a forged datagram, or one reordered past its key's
        // retirement -- and ngtcp2 discards the packet and carries on. Reporting it as a
        // callback failure would let anyone able to send a datagram close the connection.
        Err(CryptoError::Decrypt) => sys::NGTCP2_ERR_DECRYPT,
        Err(CryptoError::Fatal) => sys::NGTCP2_ERR_CALLBACK_FAILURE,
    }
}

/// Produces the mask ngtcp2 applies to a packet header.
unsafe extern "C" fn hp_mask<S: Session>(
    dest: *mut u8,
    _hp: *const sys::ngtcp2_crypto_cipher,
    hp_ctx: *const sys::ngtcp2_crypto_cipher_ctx,
    sample: *const u8,
) -> c_int {
    // SAFETY: ngtcp2 passes a context this module boxed for `S::HeaderKey`.
    let Some(key) = (unsafe { header_key_from::<S::HeaderKey>(hp_ctx) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 guarantees a sample of this length for the call.
    let sample = unsafe { core::slice::from_raw_parts(sample, HP_SAMPLE_LEN) };

    match key.mask(sample) {
        Ok(mask) => {
            // SAFETY: the contract guarantees room for the mask.
            unsafe { core::ptr::copy_nonoverlapping(mask.as_ptr(), dest, HP_MASK_LEN) };
            0
        }
        Err(_) => sys::NGTCP2_ERR_CALLBACK_FAILURE,
    }
}

/// Rotates the application keys.
unsafe extern "C" fn update_key<S: Session>(
    conn: *mut sys::ngtcp2_conn,
    rx_secret: *mut u8,
    tx_secret: *mut u8,
    rx_aead_ctx: *mut sys::ngtcp2_crypto_aead_ctx,
    rx_iv: *mut u8,
    tx_aead_ctx: *mut sys::ngtcp2_crypto_aead_ctx,
    tx_iv: *mut u8,
    current_rx_secret: *const u8,
    current_tx_secret: *const u8,
    secretlen: usize,
    _user_data: *mut c_void,
) -> c_int {
    // SAFETY: inside a callback.
    let Some(session) = (unsafe { session::<S>(conn) }) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };
    // SAFETY: ngtcp2 guarantees both secrets for the call.
    let current_rx = unsafe { core::slice::from_raw_parts(current_rx_secret, secretlen) };
    // SAFETY: as above.
    let current_tx = unsafe { core::slice::from_raw_parts(current_tx_secret, secretlen) };

    let Ok(next) = session.rotate_keys(current_rx, current_tx) else {
        return sys::NGTCP2_ERR_CALLBACK_FAILURE;
    };

    // A key update rotates payload protection only; header protection keys stay as they
    // were, which is why nothing here touches them.
    // SAFETY: ngtcp2 provides buffers of `secretlen` and of the IV length it expects.
    unsafe {
        core::ptr::copy_nonoverlapping(next.rx.iv.as_ptr(), rx_iv, next.rx.iv.len());
        core::ptr::copy_nonoverlapping(next.tx.iv.as_ptr(), tx_iv, next.tx.iv.len());
        *rx_aead_ctx = box_packet_key(next.rx.packet);
        *tx_aead_ctx = box_packet_key(next.tx.packet);
    }

    // The new secrets are what the *next* rotation starts from, so they have to be handed
    // back rather than kept.
    match session.poll_event() {
        Some(SessionEvent::Keys { secret: rx, .. }) => {
            // SAFETY: the buffer is `secretlen` long.
            unsafe { core::ptr::copy_nonoverlapping(rx.as_ptr(), rx_secret, secretlen) };
        }
        _ => return sys::NGTCP2_ERR_CALLBACK_FAILURE,
    }
    match session.poll_event() {
        Some(SessionEvent::Keys { secret: tx, .. }) => {
            // SAFETY: the buffer is `secretlen` long.
            unsafe { core::ptr::copy_nonoverlapping(tx.as_ptr(), tx_secret, secretlen) };
        }
        _ => return sys::NGTCP2_ERR_CALLBACK_FAILURE,
    }
    0
}

/// Frees a payload protection key.
///
/// Reaches for nothing but its own argument. It is called at teardown, when no session need
/// be reachable, and also from inside other callbacks when a level's keys are discarded
/// (`ngtcp2_conn.c:1727-1729`) — where looking at the session would mean a second live
/// mutable borrow of one already borrowed by the callback in progress.
unsafe extern "C" fn delete_aead_ctx<S: Session>(
    _conn: *mut sys::ngtcp2_conn,
    aead_ctx: *mut sys::ngtcp2_crypto_aead_ctx,
    _user_data: *mut c_void,
) {
    // SAFETY: ngtcp2 passes a context this module boxed, and does so exactly once.
    unsafe {
        let handle = (*aead_ctx).native_handle;
        if !handle.is_null() {
            drop(Box::from_raw(handle.cast::<S::PacketKey>()));
            (*aead_ctx).native_handle = core::ptr::null_mut();
        }
    }
}

/// Frees a header protection key. Same reasoning as above.
unsafe extern "C" fn delete_cipher_ctx<S: Session>(
    _conn: *mut sys::ngtcp2_conn,
    cipher_ctx: *mut sys::ngtcp2_crypto_cipher_ctx,
    _user_data: *mut c_void,
) {
    // SAFETY: ngtcp2 passes a context this module boxed, and does so exactly once.
    unsafe {
        let handle = (*cipher_ctx).native_handle;
        if !handle.is_null() {
            drop(Box::from_raw(handle.cast::<S::HeaderKey>()));
            (*cipher_ctx).native_handle = core::ptr::null_mut();
        }
    }
}

/// Borrows the key behind a payload protection context.
///
/// # Safety
///
/// The handle must be one [`box_packet_key`] produced for `K`, still alive.
unsafe fn key_from<'a, K: PacketKey>(ctx: *const sys::ngtcp2_crypto_aead_ctx) -> Option<&'a K> {
    if ctx.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees the context is valid for the call.
    let handle = unsafe { (*ctx).native_handle };
    if handle.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees the handle is a live `Box<K>` this module made.
    Some(unsafe { &*handle.cast::<K>() })
}

/// Borrows the key behind a header protection context.
///
/// # Safety
///
/// The handle must be one [`box_header_key`] produced for `K`, still alive.
unsafe fn header_key_from<'a, K: HeaderKey>(
    ctx: *const sys::ngtcp2_crypto_cipher_ctx,
) -> Option<&'a K> {
    if ctx.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees the context is valid for the call.
    let handle = unsafe { (*ctx).native_handle };
    if handle.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees the handle is a live `Box<K>` this module made.
    Some(unsafe { &*handle.cast::<K>() })
}

/// Fills in the crypto half of a callback table for one session type.
///
/// The transport half is the connection's to fill; this writes only what TLS owns, which is
/// the same division the seam it replaces asked each backend to observe by hand.
pub(crate) fn install<S: Session>(callbacks: &mut sys::ngtcp2_callbacks) {
    callbacks.client_initial = Some(client_initial::<S>);
    callbacks.recv_client_initial = Some(recv_client_initial::<S>);
    callbacks.recv_crypto_data = Some(recv_crypto_data::<S>);
    callbacks.recv_retry = Some(recv_retry::<S>);
    callbacks.version_negotiation = Some(version_negotiation::<S>);
    callbacks.encrypt = Some(encrypt::<S>);
    callbacks.decrypt = Some(decrypt::<S>);
    callbacks.hp_mask = Some(hp_mask::<S>);
    callbacks.update_key = Some(update_key::<S>);
    callbacks.delete_crypto_aead_ctx = Some(delete_aead_ctx::<S>);
    callbacks.delete_crypto_cipher_ctx = Some(delete_cipher_ctx::<S>);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in key that counts its own destruction.
    struct CountingKey(*mut usize);

    // SAFETY: the pointer is only ever used from the test that owns the counter, which does
    // not move it across threads. The bound exists because the seam requires `Send`.
    unsafe impl Send for CountingKey {}

    impl PacketKey for CountingKey {
        fn seal(
            &self,
            _buf: &mut [u8],
            _plaintext_len: usize,
            _nonce: &[u8],
            _aad: &[u8],
        ) -> core::result::Result<(), CryptoError> {
            Ok(())
        }
        fn open(
            &self,
            _buf: &mut [u8],
            ciphertext_len: usize,
            _nonce: &[u8],
            _aad: &[u8],
        ) -> core::result::Result<usize, CryptoError> {
            Ok(ciphertext_len)
        }
        fn tag_len(&self) -> usize {
            16
        }
        fn confidentiality_limit(&self) -> u64 {
            1 << 23
        }
        fn integrity_limit(&self) -> u64 {
            1 << 52
        }
    }

    impl Drop for CountingKey {
        fn drop(&mut self) {
            // SAFETY: the counter outlives every key in these tests.
            unsafe { *self.0 += 1 };
        }
    }

    /// A key survives the trip through an untyped handle unchanged.
    ///
    /// This is the mechanism the whole seam rests on: ngtcp2 stores one pointer per key and
    /// gives it back to callbacks that have no other way to reach state.
    #[test]
    fn a_key_boxed_into_a_handle_comes_back_as_itself() {
        let mut count = 0usize;
        let ctx = box_packet_key(CountingKey(&raw mut count));

        // SAFETY: the handle is one `box_packet_key` just made.
        let borrowed = unsafe { key_from::<CountingKey>(&raw const ctx) };
        assert!(borrowed.is_some(), "a live key must be recoverable");
        assert_eq!(borrowed.unwrap().tag_len(), 16);
        assert_eq!(count, 0, "borrowing must not consume the key");

        // SAFETY: the key is still owned by the context and has not been handed to ngtcp2.
        unsafe { reclaim::<CountingKey, super::tests::NullHeaderKey>(&ctx, &empty_cipher_ctx()) };
        assert_eq!(count, 1, "reclaiming must drop the key exactly once");
    }

    /// A null handle is "no key installed", not a key to free.
    ///
    /// ngtcp2 leaves these null for levels that never had keys, and null-checks before
    /// dispatching a delete callback at all (`ngtcp2_conn.c:543-564`). Treating null as
    /// something to reconstruct would be a free of a pointer that never was.
    #[test]
    fn a_null_handle_is_not_a_key() {
        let ctx = sys::ngtcp2_crypto_aead_ctx {
            native_handle: core::ptr::null_mut(),
        };
        // SAFETY: the context is null, which the function is required to tolerate.
        assert!(unsafe { key_from::<CountingKey>(&raw const ctx) }.is_none());
        // SAFETY: nothing to reclaim; this must not fault.
        unsafe { reclaim::<CountingKey, NullHeaderKey>(&ctx, &empty_cipher_ctx()) };
    }

    /// The two level mappings are inverses.
    ///
    /// Worth pinning because a mistake here is silent: handshake bytes submitted at the
    /// wrong level are protected with the wrong key, which looks like a peer that sent
    /// garbage rather than like a mapping error.
    #[test]
    fn the_level_mapping_round_trips() {
        for level in [
            Level::Initial,
            Level::ZeroRtt,
            Level::Handshake,
            Level::OneRtt,
        ] {
            assert_eq!(
                from_native(to_native(level)),
                Some(level),
                "encryption level did not survive the round trip"
            );
        }
    }

    /// A level ngtcp2 does not define is rejected rather than guessed at.
    #[test]
    fn an_unknown_level_is_not_invented() {
        assert_eq!(from_native(u32::MAX as sys::ngtcp2_encryption_level), None);
    }

    /// What ngtcp2 is told about a key is what the key said.
    ///
    /// The usage limits are the part worth pinning. Left at zero they are not inert: ngtcp2
    /// compares packet counts against them, so a zero confidentiality limit forces a key
    /// update immediately and a zero integrity limit makes the first forged packet fatal.
    #[test]
    fn a_crypto_context_carries_the_overhead_and_the_usage_limits() {
        let mut count = 0usize;
        let key = CountingKey(&raw mut count);
        let ctx = crypto_ctx(&key);

        assert_eq!(ctx.aead.max_overhead, 16, "tag length");
        assert_eq!(ctx.max_encryption, 1 << 23, "confidentiality limit");
        assert_eq!(ctx.max_decryption_failure, 1 << 52, "integrity limit");
        assert!(
            ctx.aead.native_handle.is_null(),
            "the core never dereferences this, and leaving it null keeps that visible"
        );
    }

    /// A header protection key that does nothing, for the reclaim signatures above.
    pub(super) struct NullHeaderKey;

    impl HeaderKey for NullHeaderKey {
        fn mask(&self, _sample: &[u8]) -> core::result::Result<[u8; HP_MASK_LEN], CryptoError> {
            Ok([0; HP_MASK_LEN])
        }
    }

    fn empty_cipher_ctx() -> sys::ngtcp2_crypto_cipher_ctx {
        sys::ngtcp2_crypto_cipher_ctx {
            native_handle: core::ptr::null_mut(),
        }
    }
}
