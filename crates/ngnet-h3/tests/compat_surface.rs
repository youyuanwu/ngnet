//! A compatibility fixture over the whole public surface.
//!
//! This test asserts almost nothing at runtime. Its value is in compiling: it names every
//! public item and uses each in a way that pins its shape, so that removing an item,
//! renaming it, or changing a signature fails the build even though no behavioural test
//! happened to exercise it.
//!
//! That distinction matters. The rest of the suite proves behaviour, and behaviour tests
//! cover only what someone thought to test; this covers the surface itself. It is the
//! mechanism behind the promise that capabilities may be *added* without anything existing
//! changing underneath a caller.
//!
//! # On exhaustive matching
//!
//! Enumerations that are **not** marked open to extension are matched exhaustively below.
//! Adding a variant to one of them is a breaking change for everyone who matched on it,
//! and this fixture is where that breakage surfaces first — deliberately, since the fix is
//! to mark the type open *before* extending it, not afterwards.
//!
//! Enumerations that *are* open cannot be matched exhaustively from another crate, which
//! is exactly the protection being bought. They are listed with a wildcard arm and a note,
//! so a reader can see the choice was made rather than overlooked.

use std::error::Error as StdError;
use std::io::IoSlice;

use ngnet_h3::{
    ALL_NATIVE_CODES, BodyOutcome, BodySource, Conn, ConnBuilder, Directionality, Error, ErrorCode,
    ErrorKind, FieldAction, FieldSection, FieldToken, FixedBody, FlowCredit, Header, Initiator,
    NativeCode, PeerSettings, Result, RetainedBytes, Role, SendGuard, Settings, Shutdown,
    StreamClosed, StreamId, Timestamp,
};

/// Every public enumeration that is closed, matched exhaustively.
///
/// If one of these gains a variant this stops compiling, which is the intended alarm
/// rather than a nuisance: a downstream caller's `match` would have stopped compiling too.
/// The stream-closure shapes, each named so a rename breaks the build.
fn stream_closure_shapes() {
    let clean: StreamClosed = StreamClosed::clean();
    let reset: StreamClosed = StreamClosed::reset_by_peer(ErrorCode::new(0x010c));
    let stopped: StreamClosed = StreamClosed::stopped_by_peer(ErrorCode::new(0x010b));
    let _: bool = clean.is_clean();
    let _: Option<ErrorCode> = reset.receiving;
    let _: Option<ErrorCode> = stopped.sending;
    let _: String = format!("{clean:?}");
    let _: StreamClosed = StreamClosed {
        receiving: None,
        sending: None,
    };
}

fn closed_enumerations_are_still_closed(
    role: Role,
    initiator: Initiator,
    directionality: Directionality,
    action: FieldAction,
    section: FieldSection,
) {
    match role {
        Role::Client | Role::Server => {}
    }
    match initiator {
        Initiator::Client | Initiator::Server => {}
    }
    match directionality {
        Directionality::Bidirectional | Directionality::Unidirectional => {}
    }
    match action {
        FieldAction::Continue | FieldAction::Stop => {}
    }
    match section {
        FieldSection::Headers | FieldSection::Trailers => {}
    }
}

/// Enumerations deliberately left open, so callers keep compiling when they grow.
///
/// The wildcard arms are the point: their presence records that openness was chosen and
/// verified here. Every known variant is still named above the wildcard, so a *removed*
/// variant is caught even though an added one is not.
fn open_enumerations_stay_open(kind: ErrorKind, shutdown: Shutdown, outcome: BodyOutcome) {
    match kind {
        ErrorKind::Protocol
        | ErrorKind::Exhausted
        | ErrorKind::InvalidInput
        | ErrorKind::ConnectionUnusable
        | ErrorKind::ClosedCriticalStream
        | ErrorKind::Internal => {}
        _ => {}
    }
    match shutdown {
        Shutdown::Notice | Shutdown::NoStreamsFrom(_) | Shutdown::NoPushesFrom(_) => {}
        _ => {}
    }
    match outcome {
        BodyOutcome::Wrote(_)
        | BodyOutcome::Eof(_)
        | BodyOutcome::EofWithTrailers(_)
        | BodyOutcome::Defer
        | BodyOutcome::Fail => {}
        _ => {}
    }
}

/// Identifiers, and the predicates that describe them.
fn stream_identifiers() -> Result<()> {
    let stream: StreamId = StreamId::new(0)?;
    let composed: StreamId =
        StreamId::compose(Initiator::Client, Directionality::Unidirectional, 0)?;
    let raw: i64 = stream.get();
    let initiator: Initiator = stream.initiator();
    let directionality: Directionality = stream.directionality();
    let _ = (raw, initiator, directionality, composed);
    let _: String = format!("{stream}");
    let _: String = format!("{stream:?}");
    let _: bool = stream == composed;
    Ok(())
}

/// Fields, including the never-indexed marker.
fn header_fields() -> Result<()> {
    let field: Header<'_> = Header::new(":method", "GET")?;
    let sensitive: Header<'_> = Header::new("authorization", "secret")?.sensitive();
    let name: &[u8] = field.name();
    let value: &[u8] = field.value();
    let is_sensitive: bool = sensitive.is_sensitive();
    let _ = (name, value, is_sensitive);
    let _: String = format!("{field:?}");
    Ok(())
}

/// Errors, and everything a caller can ask of one.
fn error_surface(error: Error) {
    let kind: ErrorKind = error.kind();
    let native: Option<NativeCode> = error.native_code();
    let fatal: bool = error.is_fatal();
    let app: Option<ErrorCode> = error.app_error_code();
    let _: &dyn StdError = &error;
    let _: String = error.to_string();
    let _: String = format!("{error:?}");
    let _ = (kind, native, fatal, app);

    for &raw in ALL_NATIVE_CODES {
        let code: NativeCode = NativeCode::new(raw);
        let _: i32 = code.get();
        let _: bool = code.is_fatal();
        let _: &'static str = code.describe();
        let _: String = format!("{code}");
    }

    let code: ErrorCode = ErrorCode::new(0x0100);
    let _: u64 = code.get();
    let _: String = format!("{code}");
}

/// Bodies: the trait, the outcome, the retained buffer and the ready-made source.
fn body_surface() {
    let mut buffer: RetainedBytes = RetainedBytes::new(b"payload".to_vec());
    let head: RetainedBytes = buffer.split_to(3);
    let _: &[u8] = head.as_slice();
    let _: usize = head.len();
    let _: bool = head.is_empty();
    let _: RetainedBytes = RetainedBytes::from(&b"borrowed"[..]);
    let _: RetainedBytes = RetainedBytes::from(b"owned".to_vec());
    let _: String = format!("{head:?}");

    let mut fixed: FixedBody = FixedBody::new(b"body".to_vec());
    let _: BodyOutcome = fixed.next();
    let _: String = format!("{fixed:?}");

    // The trait is object-safe, which is what lets a connection hold one.
    let boxed: Box<dyn BodySource> = Box::new(FixedBody::new(Vec::new()));
    let _ = boxed;
}

/// Settings, all four of them, chained as a caller would.
fn settings_surface() {
    let settings: Settings = Settings::new()
        .max_field_section_size(4096)
        .qpack_max_dtable_capacity(4096)
        .qpack_blocked_streams(16)
        .enable_connect_protocol(true);
    let _: String = format!("{settings:?}");
    let _: Settings = Settings::default();
    let _: Settings = settings.clone();
}

/// The peer's settings, read rather than constructed.
///
/// Deliberately not built with a struct expression: the type is marked open to extension,
/// so a caller cannot construct one — and that is the property being pinned. Reading every
/// field in the place a caller would receive one still fails the build if a field is
/// removed or retyped.
fn peer_settings_are_readable(peer: PeerSettings) {
    let _: u64 = peer.max_field_section_size;
    let _: u64 = peer.qpack_max_dtable_capacity;
    let _: u64 = peer.qpack_blocked_streams;
    let _: bool = peer.enable_connect_protocol;
    let _: bool = peer.h3_datagram;
    let _: String = format!("{peer:?}");
    let _: PeerSettings = peer;
}

/// Every builder hook, with the exact handler signature each promises.
fn builder_surface() -> Result<Conn<u32>> {
    ConnBuilder::<u32>::new(Role::Client)
        .settings(Settings::new())
        .on_deferred_consume(|state: &mut u32, _stream: StreamId, consumed: u64| {
            *state += consumed as u32;
        })
        .on_section_begin(|_state: &mut u32, _stream: StreamId, _section: FieldSection| {})
        .on_field(
            |_state: &mut u32,
             _stream: StreamId,
             _section: FieldSection,
             token: Option<FieldToken>,
             _name: &[u8],
             _value: &[u8]| {
                let _: Option<i32> = token.map(FieldToken::get);
                FieldAction::Continue
            },
        )
        .on_section_end(|_state: &mut u32, _stream: StreamId, _section: FieldSection| {})
        .on_data(|_state: &mut u32, _stream: StreamId, _chunk: &[u8]| {})
        .on_end_stream(|_state: &mut u32, _stream: StreamId| {})
        .on_stream_close(
            |_state: &mut u32, _stream: StreamId, closed: StreamClosed| {
                let _: Option<ErrorCode> = closed.receiving;
                let _: Option<ErrorCode> = closed.sending;
            },
        )
        .on_stop_sending(|_state: &mut u32, _stream: StreamId, _code: ErrorCode| {})
        .on_reset_stream(|_state: &mut u32, _stream: StreamId, _code: ErrorCode| {})
        .on_shutdown(|_state: &mut u32, _shutdown: Shutdown| {})
        .on_peer_settings(|_state: &mut u32, settings: PeerSettings| {
            peer_settings_are_readable(settings);
        })
        .build()
}

/// Every operation a connection offers, named with the type it returns.
fn connection_surface() -> Result<()> {
    let mut conn: Conn<u32> = builder_surface()?;
    let mut state = 0u32;

    let _: Role = conn.role();
    let _: bool = conn.is_usable();
    let _: bool = conn.is_bound();
    let _: String = format!("{conn:?}");

    let control = StreamId::new(2)?;
    conn.bind_control_stream(control)?;
    conn.bind_qpack_streams(StreamId::new(6)?, StreamId::new(10)?)?;

    let request = StreamId::new(0)?;
    conn.submit_request(request, &[Header::new(":method", "GET")?], None)?;
    conn.submit_request(
        StreamId::new(4)?,
        &[Header::new(":method", "POST")?],
        Some(Box::new(FixedBody::new(b"body".to_vec()))),
    )?;

    let credit: FlowCredit = conn.read_stream(
        StreamId::new(3)?,
        &[],
        false,
        Timestamp::from_nanos(1),
        &mut state,
    )?;
    let _: u64 = credit.bytes();
    let _: u64 = Timestamp::from_nanos(1).as_nanos();

    if let Some(send) = conn.writev_stream(&mut state)? {
        let _: StreamId = send.stream();
        let _: bool = send.fin();
        let _: usize = send.len();
        let _: bool = send.is_empty();
        let _: &[IoSlice<'_>] = send.slices();
        let _: String = format!("{send:?}");
        let taken = send.len();
        send.commit(taken)?;
    }
    if let Some(send) = conn.writev_stream(&mut state)? {
        let guard: SendGuard<'_, u32> = send;
        // The other way to end a transaction. Named here rather than left to a behavioural
        // test, which is the situation this fixture exists to stop relying on.
        guard.abandon();
    }

    conn.add_ack_offset(control, 0, &mut state)?;
    let _: bool = conn.has_deferred_credit();
    let _: Vec<(StreamId, u64)> = conn.take_deferred_credit();
    conn.block_stream(request)?;
    conn.unblock_stream(request)?;
    conn.resume_stream(request)?;
    let _: bool = conn.is_stream_writable(request)?;
    conn.shutdown_stream_write(request)?;
    conn.shutdown_stream_read(request)?;
    conn.set_max_concurrent_streams(16)?;
    conn.submit_shutdown_notice()?;
    conn.shutdown()?;

    // Server-only, so named on a server rather than left unpinned.
    let mut server: Conn<u32> = ConnBuilder::<u32>::new(Role::Server).build()?;
    server.bind_control_stream(StreamId::new(3)?)?;
    server.bind_qpack_streams(StreamId::new(7)?, StreamId::new(11)?)?;
    server.set_max_client_streams_bidi(100)?;
    let _: bool = server.is_drained()?;

    // Named but not run: these need a request stream the peer actually opened, and this
    // fixture is about shape rather than behaviour. Taking the function item still forces
    // every signature inside it to compile, which is the whole point; running it would
    // mean building an exchange, which the behavioural tests already do.
    let _: fn(&mut Conn<u32>, StreamId, &mut u32) -> Result<()> = server_message_surface;
    Ok(())
}

/// The server-side message operations, pinned without being executed.
fn server_message_surface(server: &mut Conn<u32>, stream: StreamId, state: &mut u32) -> Result<()> {
    server.submit_response(stream, &[Header::new(":status", "200")?], None)?;
    server.submit_response(
        stream,
        &[Header::new(":status", "200")?],
        Some(Box::new(FixedBody::new(Vec::new()))),
    )?;
    server.submit_info(stream, &[Header::new(":status", "103")?])?;
    server.submit_trailers(stream, &[Header::new("x-trailer", "v")?])?;
    server.close_stream(stream, state)?;
    server.close_stream_with(stream, StreamClosed::clean(), state)?;
    server.close_stream_with(
        stream,
        StreamClosed::reset_by_peer(ErrorCode::new(0x010c)),
        state,
    )?;
    server.close_stream_with(
        stream,
        StreamClosed::stopped_by_peer(ErrorCode::new(0x010c)),
        state,
    )?;
    let _: bool = StreamClosed::clean().is_clean();
    Ok(())
}

/// The raw escape hatch, which the no-unsafe claim deliberately excludes.
///
/// Its presence is part of the contract: capabilities the safe API does not yet cover stay
/// reachable, at the cost of upholding nghttp3's invariants yourself.
fn raw_escape_hatch_is_reachable() {
    let _: i32 = ngnet_h3::raw::NGHTTP3_ERR_INVALID_ARGUMENT;
    let _: u32 = ngnet_h3::raw::NGHTTP3_CALLBACKS_VERSION;
    // A function pointer, named but not called: taking it proves the item exists without
    // this fixture having to satisfy its preconditions.
    let _ = ngnet_h3::raw::nghttp3_version;
}

#[test]
fn the_public_surface_still_has_the_shape_it_promised() {
    // Compiling is the assertion. Calling the fixtures as well proves they were not
    // silently optimised into nothing and that the connection ones actually run.
    closed_enumerations_are_still_closed(
        Role::Client,
        Initiator::Client,
        Directionality::Bidirectional,
        FieldAction::Continue,
        FieldSection::Headers,
    );
    open_enumerations_stay_open(ErrorKind::Internal, Shutdown::Notice, BodyOutcome::Defer);
    stream_closure_shapes();
    stream_identifiers().expect("identifiers");
    header_fields().expect("fields");
    error_surface(Header::new("Bad-Name", "v").expect_err("an invalid field name"));
    body_surface();
    settings_surface();
    connection_surface().expect("the connection surface");
    raw_escape_hatch_is_reachable();
}

/// The asynchronous layer's public surface.
///
/// Behind the `http` feature, so this file pins two shapes rather than one: with the feature
/// off, everything above is the whole crate; with it on, everything below is added. Both are
/// promises, and a change to either should be deliberate.
#[cfg(feature = "http")]
mod asynchronous {
    use ngnet_h3::http::quic::Timestamp;
    use ngnet_h3::http::testing::bytes_crate::Bytes;
    use ngnet_h3::http::testing::http_body_crate::Body;
    use ngnet_h3::http::{
        Cancelled, Config, Connection, Error, ErrorKind, IncomingBody, QuicConnection, QuicEvent,
        ResponseFuture, Result, SendRequest, StreamSource, WriteOutcome, handshake, handshake_with,
        serve, serve_with,
    };
    use ngnet_h3::{ErrorCode, RetainedBytes, StreamId};

    /// `WriteOutcome` is closed: a transport answers an offer in exactly three ways, and a
    /// fourth would be a change every implementation has to notice.
    fn write_outcomes_are_closed(outcome: WriteOutcome) -> &'static str {
        match outcome {
            WriteOutcome::Accepted(_) => "accepted",
            WriteOutcome::Blocked => "blocked",
            WriteOutcome::Gone => "gone",
        }
    }

    /// `QuicEvent` and `ErrorKind` are open, so both are matched with a wildcard: adding a
    /// variant must not break a caller.
    fn open_enumerations_stay_open(event: QuicEvent, kind: ErrorKind) -> bool {
        let named = matches!(
            event,
            QuicEvent::Data { .. }
                | QuicEvent::Accepted { .. }
                | QuicEvent::Released { .. }
                | QuicEvent::StopSending { .. }
                | QuicEvent::Reset { .. }
                | QuicEvent::StreamClosed { .. }
                | QuicEvent::Closed { .. }
        );
        let categorised = matches!(
            kind,
            ErrorKind::Transport
                | ErrorKind::Connection
                | ErrorKind::Stream
                | ErrorKind::Protocol
                | ErrorKind::Closed
                | ErrorKind::Body
                | ErrorKind::Refused
        );
        named && categorised
    }

    /// Every builder on `Config`, and the fact that each is `#[must_use]`.
    fn config_surface() -> Config {
        Config::default()
            .max_concurrent_streams(64)
            .max_field_section_size(16 * 1024)
            .qpack_max_dtable_capacity(4096)
            .qpack_blocked_streams(8)
            .events_per_pass(32)
    }

    /// The error surface a caller reads.
    fn error_surface(error: &Error) -> (ErrorKind, bool, bool, Option<ErrorCode>) {
        let _: &dyn std::error::Error = error;
        let _ = format!("{error}");
        let _ = format!("{error:?}");
        (
            error.kind(),
            error.is_closed(),
            error.is_retriable(),
            error.code(),
        )
    }

    /// `IncomingBody` is an `http_body::Body` of `Bytes`, and says so in its own types.
    fn incoming_body_surface(body: &IncomingBody) -> bool {
        fn assert_body<B: Body<Data = Bytes, Error = Error>>() {}
        assert_body::<IncomingBody>();
        let _ = format!("{body:?}");
        Body::is_end_stream(body)
    }

    /// The client surface: a cloneable handle, a future, and the four things the handle does.
    fn client_surface<B>(handle: &SendRequest<B>) -> (bool, bool) {
        let cloned = handle.clone();
        cloned.shutdown();
        (cloned.is_closed(), cloned.is_refusing())
    }

    /// The cancellation signal a handler receives.
    async fn cancellation_surface(cancelled: Cancelled) -> bool {
        let copy = cancelled.clone();
        let _ = format!("{copy:?}");
        if copy.is_cancelled() {
            copy.cancelled().await;
            return true;
        }
        false
    }

    /// The backend trait, named in full so a change to it is a change here.
    fn backend_surface<Q: QuicConnection>(backend: &mut Q) {
        let _: bool = Q::RETAINS_BUFFERS;
        let _: Timestamp = backend.now();
        let _ = backend.reset(stream(), ErrorCode::new(0x10c));
        let _ = backend.stop_sending(stream(), ErrorCode::new(0x10c));
        let _ = backend.extend_credit(Some(stream()), 1);
        let _ = backend.extend_credit(None, 1);
        let _ = backend.close(ErrorCode::new(0x100), b"");
    }

    /// The source a transport pulls from.
    fn source_surface<S: StreamSource>(source: &mut S) -> bool {
        source.write_next(&mut |_stream, _slices, _fin| WriteOutcome::Accepted(0))
    }

    /// The erased-owner constructor the zero-copy body path needs.
    fn retained_surface() -> usize {
        let retained = RetainedBytes::from_owner(Bytes::from_static(b"pinned"));
        retained.len()
    }

    fn stream() -> StreamId {
        StreamId::new(0).expect("zero is a valid identifier")
    }

    /// A minimal body, since `Bytes` is not itself an `http_body::Body`.
    struct Nothing;

    impl Body for Nothing {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Option<Result2<ngnet_h3::http::testing::http_body_crate::Frame<Bytes>, Self::Error>>,
        > {
            std::task::Poll::Ready(None)
        }
    }

    type Result2<T, E> = core::result::Result<T, E>;
    type Backend = ngnet_h3::http::testing::Loopback;
    type Answer = core::future::Ready<http::Response<Nothing>>;
    type Handler = fn(http::Request<IncomingBody>) -> Answer;
    type Client<D> = Result<(SendRequest<Nothing>, Connection<D>)>;

    /// Names all four entry points, so their signatures are pinned even though calling them
    /// here would prove nothing about shape.
    fn entry_points_are_named() {
        let _: fn(Backend) -> Client<_> = handshake;
        let _: fn(Backend, Config) -> Client<_> = handshake_with;

        fn handler(_request: http::Request<IncomingBody>) -> Answer {
            core::future::ready(http::Response::new(Nothing))
        }
        let _: fn(Backend, Handler) -> Result<Connection<_>> = serve;
        let _: fn(Backend, Handler, Config) -> Result<Connection<_>> = serve_with;
        let _ = handler;
    }

    #[test]
    fn the_async_surface_still_has_the_shape_it_promised() {
        assert_eq!(write_outcomes_are_closed(WriteOutcome::Blocked), "blocked");
        assert!(open_enumerations_stay_open(
            QuicEvent::Closed { code: None },
            ErrorKind::Transport
        ));
        let _ = config_surface();
        assert_eq!(retained_surface(), 6);

        let (mut backend, _peer, _knobs) = ngnet_h3::http::testing::loopback();
        backend_surface(&mut backend);

        // Named so the compiler checks the signatures; not called, because they need a
        // driver to mean anything and this file is about shape rather than behaviour.
        let _ = entry_points_are_named;
        let _ = client_surface::<Nothing>;
        let _ = cancellation_surface;
        let _ = incoming_body_surface;
        let _ = error_surface;
        let _ = source_surface::<ngnet_h3::http::testing::ScriptedSource>;
        let _ = ResponseFuture::poll;
    }
}
