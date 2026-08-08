//! The shape of the public API, pinned.
//!
//! Nothing here asserts behaviour. The technique is that **compiling is the assertion**:
//! every public item is named and used in a way that fixes its signature, so a change to
//! the surface breaks this file rather than breaking a downstream crate silently.
//!
//! Adding public API means extending this file, or the test fails to be a complete
//! description of what was promised.
//!
//! Whether an enumeration is matched exhaustively or with a wildcard is itself part of the
//! promise. A closed enum is one where a new variant is a change every caller must notice;
//! an open one is where adding a variant must not break anyone. Which is which is a
//! deliberate decision, recorded here by how it is matched.

use ngnet_quic::{
    ApplicationErrorCode, ConnBuilder, ConnectionId, Directionality, Duration, EntropySource,
    Error, ErrorKind, ExpiryOutcome, Handlers, Initiator, Inspection, NativeCode, NativeTlsHandle,
    ReadOutcome, Result, Role, Settings, StreamCloseReason, StreamId, StreamWrite, Timestamp,
    TlsBackend, TlsSession, TransportErrorCode, TransportParams, WriteOutcome,
};

#[test]
fn the_public_surface_still_has_the_shape_it_promised() {
    // --- Time -----------------------------------------------------------------------
    let ts: Result<Timestamp> = Timestamp::from_nanos(1);
    let ts = ts.expect("a valid timestamp");
    let _: u64 = ts.as_nanos();
    let d = Duration::from_nanos(1);
    let _: u64 = d.as_nanos();
    let _: Result<Duration> = Duration::from_millis(1);

    // --- Errors ---------------------------------------------------------------------
    let native = NativeCode::new(-1);
    let _: i32 = native.get();
    let _: bool = native.is_fatal();
    let _: &'static str = native.describe();

    let transport = TransportErrorCode::new(0);
    let _: u64 = transport.get();
    let _: TransportErrorCode = TransportErrorCode::infer(native);
    let _: TransportErrorCode = TransportErrorCode::NO_ERROR;

    let app = ApplicationErrorCode::new(0);
    let _: u64 = app.get();

    fn takes_error(e: &Error) {
        let _: ErrorKind = e.kind();
        let _: Option<NativeCode> = e.native_code();
        let _: bool = e.is_fatal();
        let _: Option<TransportErrorCode> = e.transport_error_code();
        // `Error` is an ordinary std error.
        let _: &dyn std::error::Error = e;
    }
    let _ = takes_error;

    // `ErrorKind` is **open**: ngtcp2 may grow conditions this crate has to classify, and
    // adding a variant must not break a caller. Matched with a wildcard, deliberately.
    fn classify(kind: ErrorKind) -> &'static str {
        match kind {
            ErrorKind::Protocol => "protocol",
            ErrorKind::Exhausted => "exhausted",
            ErrorKind::InvalidInput => "invalid",
            ErrorKind::Crypto => "crypto",
            ErrorKind::ConnectionUnusable => "unusable",
            ErrorKind::Closing => "closing",
            ErrorKind::Blocked => "blocked",
            ErrorKind::Internal => "internal",
            _ => "unknown",
        }
    }
    let _ = classify;

    // --- Connection identifiers -----------------------------------------------------
    let cid: Result<ConnectionId> = ConnectionId::new(&[1, 2, 3, 4, 5, 6, 7, 8]);
    let cid = cid.expect("a valid identifier");
    let _: &[u8] = cid.as_bytes();
    let _: usize = ngnet_quic::MAX_CID_LEN;
    let _: usize = ngnet_quic::MIN_CID_LEN;

    // --- Streams --------------------------------------------------------------------
    let stream: Result<StreamId> = StreamId::new(0);
    let stream = stream.expect("a valid identifier");
    let _: i64 = stream.get();
    let _: i64 = StreamId::MAX;
    let _: bool = stream.is_writable_by(true);

    // Both **closed**: QUIC defines exactly two initiators and two directionalities, and a
    // third of either would be a new protocol.
    match stream.initiator() {
        Initiator::Client | Initiator::Server => {}
    }
    match stream.directionality() {
        Directionality::Bidirectional | Directionality::Unidirectional => {}
    }

    // --- Packet outcomes ------------------------------------------------------------
    // **Closed.** Each variant demands a different response, so a new one is a change every
    // caller must be forced to notice.
    fn on_read(outcome: ReadOutcome) {
        match outcome {
            ReadOutcome::Processed
            | ReadOutcome::SendRetry
            | ReadOutcome::DropSilently
            | ReadOutcome::Draining
            | ReadOutcome::Closing => {}
        }
    }
    let _ = on_read;

    // **Closed**, and the most important one: conflating `Idle` with `Blocked` is the
    // classic stall bug, so a caller must handle each explicitly.
    fn on_write(outcome: WriteOutcome) {
        match outcome {
            WriteOutcome::Datagram { len: _ } | WriteOutcome::Idle | WriteOutcome::Blocked => {}
        }
    }
    let _ = on_write;

    // **Closed**, for the same reason: `IdleClose` must not be answered with a packet.
    fn on_expiry(outcome: ExpiryOutcome) {
        match outcome {
            ExpiryOutcome::Handled | ExpiryOutcome::IdleClose | ExpiryOutcome::Terminal => {}
        }
    }
    let _ = on_expiry;

    // **Closed.**
    fn on_stream_write(outcome: StreamWrite) {
        match outcome {
            StreamWrite::Datagram {
                len: _,
                accepted: _,
            }
            | StreamWrite::StreamBlocked
            | StreamWrite::ConnectionBlocked
            | StreamWrite::Blocked
            | StreamWrite::Idle => {}
        }
    }
    let _ = on_stream_write;

    // **Open.** A stream may come to end in ways QUIC does not define today, and adding one
    // must not break a caller that only cares about the two it knows.
    fn on_close(reason: StreamCloseReason) {
        match reason {
            StreamCloseReason::Finished => {}
            StreamCloseReason::Reset(_code) => {}
            _ => {}
        }
    }
    let _ = on_close;

    // --- Pre-connection inspection --------------------------------------------------
    let _: u32 = ngnet_quic::VERSION_V1;
    let _: &'static [u32] = ngnet_quic::supported_versions();

    // Named through function pointers rather than called. This file pins *shapes*; calling
    // these with placeholder arguments would be exercising behaviour, and a degenerate
    // input can trip an assertion inside ngtcp2 -- which is a job for the behavioural tests,
    // not for a signature check.
    let _: fn(&[u8]) -> bool = ngnet_quic::is_acceptable_initial;
    let _: fn(&[u8], usize) -> Result<Inspection> = ngnet_quic::inspect;
    // Named through an alias, which keeps the signature readable and keeps clippy from
    // objecting to a five-argument function pointer written inline.
    type WriteVersionNegotiation =
        fn(&mut [u8], u8, &ConnectionId, &ConnectionId, &[u32]) -> Result<usize>;
    let _: WriteVersionNegotiation = ngnet_quic::write_version_negotiation;

    // **Closed.** Each case leads somewhere different: build a connection, answer with a
    // Version Negotiation packet, or route to an existing connection.
    fn on_inspection(inspection: Inspection) {
        match inspection {
            Inspection::Supported {
                version: _,
                dcid: _,
                scid: _,
            }
            | Inspection::UnsupportedVersion {
                version: _,
                dcid: _,
                scid: _,
            }
            | Inspection::ShortHeader { dcid: _ } => {}
        }
    }
    let _ = on_inspection;

    // --- Configuration --------------------------------------------------------------
    let _settings = Settings::new(ts)
        .initial_rtt(d)
        .max_tx_udp_payload_size(1200)
        .max_window(1)
        .max_stream_window(1)
        .handshake_timeout(d)
        .initial_pkt_num(0)
        .no_pmtud(false);

    let _params = TransportParams::new()
        .initial_max_stream_data_bidi_local(1)
        .initial_max_stream_data_bidi_remote(1)
        .initial_max_stream_data_uni(1)
        .initial_max_data(1)
        .initial_max_streams_bidi(1)
        .initial_max_streams_uni(1)
        .max_idle_timeout(d)
        .active_connection_id_limit(2)
        .max_ack_delay(d)
        .original_dcid(&cid);

    let _: u64 = ngnet_quic::DEFAULT_STREAM_DATA;
    let _: u64 = ngnet_quic::DEFAULT_CONNECTION_DATA;
    let _: u64 = ngnet_quic::DEFAULT_MAX_STREAMS;
    let _: Duration = ngnet_quic::DEFAULT_IDLE_TIMEOUT;

    // --- Handlers -------------------------------------------------------------------
    let _handlers = Handlers::new()
        .on_stream_data(|_id, _bytes, _fin| {})
        .on_stream_open(|_id| {})
        .on_stream_close(|_id, _reason| {})
        .on_stream_reset(|_id, _code| {})
        .on_stop_sending(|_id, _code| {})
        .on_acked_stream_data(|_id, _len| {})
        .on_handshake_completed(|| {});

    // --- Entropy --------------------------------------------------------------------
    struct Source;
    impl EntropySource for Source {
        fn fill(&mut self, _dest: &mut [u8]) -> Result<()> {
            Ok(())
        }
    }
    let _: Box<dyn EntropySource + Send> = Box::new(Source);

    // --- TLS seam -------------------------------------------------------------------
    // **Closed.** There are two roles in a handshake and there will not be a third.
    match Role::Client {
        Role::Client | Role::Server => {}
    }
    let _: bool = Role::Server.is_server();

    fn takes_session<S: TlsSession>(s: &S) {
        let _: NativeTlsHandle = s.native_handle();
        let _: Option<Vec<u8>> = s.negotiated_alpn();
        let _: Option<String> = s.failure_reason();
    }
    let _ = takes_session::<DummySession>;

    fn takes_backend<B: TlsBackend>(b: &B) -> Result<B::Session> {
        b.new_session(Role::Client, Some("example.com"))
    }
    let _ = takes_backend::<DummyBackend>;
}

/// A backend that exists only so the generic bounds above have something to name.
struct DummyBackend;

/// Deliberately `Send`, so the auto-trait assertions above are about `Conn` rather than
/// about this stand-in.
struct DummySession;

// SAFETY: never used to build a connection; it exists to pin the trait's shape.
unsafe impl TlsSession for DummySession {
    unsafe fn bind_connection(&mut self, _conn: *mut core::ffi::c_void) {}
    unsafe fn install_callbacks(&self, _callbacks: *mut core::ffi::c_void) {}
    fn native_handle(&self) -> NativeTlsHandle {
        // SAFETY: never handed to ngtcp2.
        unsafe { NativeTlsHandle::new(core::ptr::null_mut()) }
    }
    fn negotiated_alpn(&self) -> Option<Vec<u8>> {
        None
    }
}

// SAFETY: as above.
unsafe impl TlsBackend for DummyBackend {
    type Session = DummySession;
    fn new_session(&self, _role: Role, _server_name: Option<&str>) -> Result<Self::Session> {
        Ok(DummySession)
    }
}

/// A `Conn` is `Send`, and it owns its handlers and its entropy source. Both must therefore
/// be `Send` themselves, or the unsafe impl on `Conn` launders non-`Send` state across a
/// thread boundary -- an `Rc` captured by a handler, cloned before the connection is moved,
/// is a data race on a non-atomic refcount reachable from entirely safe code.
///
/// The compiler will not catch that: `unsafe impl Send` is precisely the escape hatch that
/// silences it. So it is asserted here.
#[test]
fn the_types_a_connection_owns_are_send_because_the_connection_is() {
    fn assert_send<T: Send>() {}

    // The two things a `Conn` actually keeps. `Settings` and `TransportParams` are not
    // asserted because ngtcp2 copies them at construction and the connection does not hold
    // them; they wrap C structs containing raw pointers and are deliberately consumed by
    // value into `build`.
    assert_send::<Handlers<'static>>();
    assert_send::<Box<dyn EntropySource + Send>>();
    assert_send::<ConnectionId>();

    // And the connection itself, for a `Send` session.
    fn conn_is_send<S: TlsSession + Send>() {
        assert_send::<ngnet_quic::Conn<'static, S>>();
    }
    let _ = conn_is_send::<DummySession>;
}

#[test]
fn the_connection_surface_still_has_the_shape_it_promised() {
    // Named through a generic function rather than instantiated, so this pins the signatures
    // without needing a TLS backend or a live connection.
    fn uses_conn<S: TlsSession>(conn: &mut ngnet_quic::Conn<'_, S>, cause: &Error) -> Result<()> {
        let now = Timestamp::from_nanos(1)?;
        let mut buf = [0u8; 1500];

        let _: Role = conn.role();
        let _: &ConnectionId = conn.scid();
        let _: core::net::SocketAddr = conn.local_addr();
        let _: core::net::SocketAddr = conn.remote_addr();
        let _: bool = conn.is_handshake_completed();
        let _: Option<Vec<u8>> = conn.negotiated_alpn();
        let _: &S = conn.tls();

        let _: ReadOutcome = conn.read_pkt(&[0], now)?;
        let _: WriteOutcome = conn.write_pkt(&mut buf, now)?;
        let _: Option<Timestamp> = conn.expiry();
        let _: ExpiryOutcome = conn.handle_expiry(now)?;
        let _: bool = conn.in_closing_period();
        let _: bool = conn.in_draining_period();

        let stream: StreamId = conn.open_bidi_stream()?;
        let _: StreamId = conn.open_uni_stream()?;
        let _: StreamWrite = conn.write_stream(&mut buf, stream, &[0], true, now)?;
        conn.shutdown_stream(stream, ApplicationErrorCode::new(0))?;
        conn.reset_stream(stream, ApplicationErrorCode::new(0))?;
        conn.stop_sending(stream, ApplicationErrorCode::new(0))?;
        conn.extend_max_stream_offset(stream, 1)?;
        conn.extend_max_offset(1);
        conn.extend_max_streams_bidi(1);
        conn.extend_max_streams_uni(1);
        let _: u64 = conn.streams_bidi_left();
        let _: u64 = conn.streams_uni_left();
        let _: usize =
            conn.write_connection_close(&mut buf, ApplicationErrorCode::new(0), b"", now)?;
        let _: usize = conn.write_transport_close(&mut buf, cause, b"", now)?;
        let _: usize = conn.retained_bytes();
        Ok(())
    }
    let _ = uses_conn::<DummySession>;

    fn builds<S: TlsSession>(
        settings: Settings,
        params: TransportParams,
        entropy: Box<dyn EntropySource + Send>,
        tls: S,
        local: core::net::SocketAddr,
        remote: core::net::SocketAddr,
        cid: ConnectionId,
    ) -> Result<ngnet_quic::Conn<'static, S>> {
        ConnBuilder::new(Role::Client, settings, params, entropy, tls, local, remote)
            .dcid(cid)
            .scid(cid)
            .version(ngnet_quic::VERSION_V1)
            .cid_len(8)
            .build(Handlers::new())
    }
    let _ = builds::<DummySession>;
}

#[cfg(feature = "tls-ossl")]
#[test]
fn the_openssl_backend_surface_still_has_the_shape_it_promised() {
    use ngnet_quic::{OsslBackend, OsslBackendBuilder, OsslSession, Verify};

    let _: OsslBackendBuilder = OsslBackend::builder(Role::Client)
        .alpn("h3")
        .alpn(b"hq".to_vec())
        .certificate_chain_pem("")
        .private_key_pem("")
        .trust_anchor_pem("")
        .use_system_trust_store(false)
        .verify(Verify::Peer);

    // **Open.** A future backend may grow another verification mode, and adding one must
    // not break a caller that names the ones it knows.
    fn on_verify(verify: Verify) {
        match verify {
            Verify::Peer => {}
            Verify::RequireClientCertificate => {}
            Verify::DangerouslyAcceptAnyCertificate => {}
            _ => {}
        }
    }
    let _ = on_verify;

    fn takes_ossl_session(s: &OsslSession) -> Option<Vec<u8>> {
        s.negotiated_alpn()
    }
    let _ = takes_ossl_session;
}
