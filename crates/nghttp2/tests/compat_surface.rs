//! A compatibility fixture over the sans-I/O public surface (Spec SC-022, FR-037).
//!
//! This test asserts nothing at runtime. Its value is entirely in compiling: it names
//! every public item of the sans-I/O API and uses each in a way that pins its shape, so
//! that removing an item, renaming it, or changing a signature fails the build even
//! though no behavioural test happened to exercise it.
//!
//! That distinction matters. The rest of the suite proves behaviour, and behaviour tests
//! only cover what someone thought to test; this covers the surface itself. It is the
//! mechanism behind the promise that capabilities may be *added* to this crate without
//! anything existing changing underneath a caller.
//!
//! # On exhaustive matching
//!
//! Enumerations that are **not** marked open to extension are matched exhaustively below.
//! Adding a variant to one of them is a breaking change for every downstream caller who
//! matched on it, and this fixture is where that breakage surfaces first — deliberately,
//! since the fix is to mark the type open *before* extending it, not afterwards.
//!
//! Enumerations that *are* marked open cannot be matched exhaustively from another crate,
//! which is exactly the protection being bought. They are listed here with a wildcard arm
//! and a note, so the reader can see the choice was made rather than overlooked.

use std::error::Error as StdError;

use nghttp2::{
    ALL_NATIVE_CODES, BodyError, BodyOutcome, BodySource, BytesBody, Error, ErrorCode, ErrorKind,
    FrameInfo, FrameType, Goaway, Header, HeaderAction, HeaderCategory, NativeCode, Result,
    Session, SessionBuilder, Setting, StreamId,
};

/// Every public enumeration that is closed, matched exhaustively.
///
/// `HeaderAction` is the only one. If it gains a variant this stops compiling, which is
/// the intended alarm rather than a nuisance: a downstream caller's `match` would have
/// stopped compiling too.
fn closed_enumerations_are_still_closed(action: HeaderAction) {
    match action {
        HeaderAction::Continue | HeaderAction::CancelStream => {}
    }
}

/// Enumerations deliberately left open, so callers keep compiling when they grow.
///
/// The wildcard arms are the point. Their presence records that openness was chosen for
/// these types and verified here, rather than assumed. Every known variant is still named
/// above the wildcard, so a *removed* variant is caught even though an added one is not.
fn open_enumerations_stay_open(
    outcome: BodyOutcome,
    category: HeaderCategory,
    kind: ErrorKind,
    setting: Setting,
) {
    match outcome {
        BodyOutcome::Wrote(_)
        | BodyOutcome::Eof(_)
        | BodyOutcome::EofWithTrailers(_)
        | BodyOutcome::Defer
        | BodyOutcome::Fail(_) => {}
        _ => {}
    }

    match category {
        HeaderCategory::Request
        | HeaderCategory::Response
        | HeaderCategory::PushResponse
        | HeaderCategory::Trailing => {}
        _ => {}
    }

    match kind {
        ErrorKind::Protocol
        | ErrorKind::InvalidInput
        | ErrorKind::Exhausted
        | ErrorKind::Internal => {}
        _ => {}
    }

    match setting {
        Setting::HeaderTableSize(_)
        | Setting::EnablePush(_)
        | Setting::MaxConcurrentStreams(_)
        | Setting::InitialWindowSize(_)
        | Setting::MaxFrameSize(_)
        | Setting::MaxHeaderListSize(_)
        | Setting::EnableConnectProtocol(_)
        | Setting::NoRfc7540Priorities(_) => {}
        _ => {}
    }
}

/// The error model: constructors, accessors and trait implementations.
fn error_surface(error: &Error) {
    let _: &'static str = error.operation();
    let _: ErrorKind = error.kind();
    let _: Option<NativeCode> = error.native_code();
    let _: &'static str = error.kind().description();
    let _: String = error.to_string();
    let _: &dyn StdError = error;

    for code in ALL_NATIVE_CODES {
        let _: i32 = code.get();
        let _: Option<&'static str> = code.describe();
    }
    let _: NativeCode = NativeCode::new(-901);
    let _: Error = Error::from_native("op", -901);
}

/// Protocol error codes and their constants.
fn error_code_surface() {
    let codes = [
        ErrorCode::NO_ERROR,
        ErrorCode::PROTOCOL_ERROR,
        ErrorCode::INTERNAL_ERROR,
        ErrorCode::FLOW_CONTROL_ERROR,
        ErrorCode::SETTINGS_TIMEOUT,
        ErrorCode::STREAM_CLOSED,
        ErrorCode::FRAME_SIZE_ERROR,
        ErrorCode::REFUSED_STREAM,
        ErrorCode::CANCEL,
        ErrorCode::COMPRESSION_ERROR,
        ErrorCode::CONNECT_ERROR,
        ErrorCode::ENHANCE_YOUR_CALM,
        ErrorCode::INADEQUATE_SECURITY,
        ErrorCode::HTTP_1_1_REQUIRED,
    ];
    for code in codes {
        let _: u32 = code.get();
        let _: String = code.to_string();
    }
    let _: ErrorCode = ErrorCode::new(0);
}

/// Stream identifiers and the frame view handed to handlers.
fn stream_surface(info: FrameInfo) {
    let _: StreamId = StreamId::CONNECTION;
    let id = StreamId::new(1);
    let _: i32 = id.get();
    let _: bool = id.is_connection();
    let _: String = id.to_string();

    let kinds = [
        FrameType::DATA,
        FrameType::HEADERS,
        FrameType::RST_STREAM,
        FrameType::SETTINGS,
        FrameType::PUSH_PROMISE,
        FrameType::PING,
        FrameType::GOAWAY,
        FrameType::WINDOW_UPDATE,
        FrameType::CONTINUATION,
    ];
    for kind in kinds {
        let _: u8 = kind.get();
    }

    let _: StreamId = info.stream_id();
    let _: FrameType = info.kind();
    let _: u8 = info.flags();
    let _: usize = info.payload_len();
    let _: bool = info.is_end_stream();
    let _: bool = info.is_ack();
    let _: bool = info.is_end_headers();
    // Added alongside the async layer; pinned here from the moment it exists.
    let _: Option<HeaderCategory> = info.category();
    let _: bool = info.is_trailers();
    let _: Option<Goaway> = info.goaway();
}

fn goaway_surface(goaway: Goaway) {
    let _: StreamId = goaway.last_stream_id();
    let _: ErrorCode = goaway.code();
}

/// Header fields for outgoing messages.
fn header_surface() {
    let plain: Header<'_> = Header::new("name", "value");
    let _: Header<'_> = Header::from_bytes(b"name", b"value");
    let sensitive: Header<'_> = Header::new("authorization", "secret").sensitive();

    let _: &[u8] = plain.name();
    let _: &[u8] = plain.value();
    let _: bool = plain.is_sensitive();
    let _: bool = sensitive.is_sensitive();
}

/// Settings, including the accessors that translate them for the wire.
fn setting_surface() {
    let settings = [
        Setting::HeaderTableSize(4096),
        Setting::EnablePush(false),
        Setting::MaxConcurrentStreams(100),
        Setting::InitialWindowSize(65535),
        Setting::MaxFrameSize(16384),
        Setting::MaxHeaderListSize(8192),
        Setting::EnableConnectProtocol(false),
        Setting::NoRfc7540Priorities(true),
    ];
    for setting in settings {
        let _: i32 = setting.id();
        let _: u32 = setting.value();
    }
}

/// Outgoing bodies: the trait, its outcomes, and the in-memory implementation.
fn body_surface() {
    let mut body = BytesBody::new(b"payload".to_vec()).with_trailers();
    let mut buf = [0u8; 8];
    let _: BodyOutcome = body.fill(&mut buf);

    let _: BodyError = Box::new(std::io::Error::other("boxed"));

    fn accepts_any_source<B: BodySource + 'static>(_source: B) {}
    accepts_any_source(BytesBody::new(Vec::new()));
}

/// The session: construction, both roles, and every operation.
fn session_surface() -> Result<()> {
    let _: SessionBuilder<()> = SessionBuilder::<()>::server();

    let mut session: Session<()> = SessionBuilder::<()>::client()
        .setting(Setting::MaxConcurrentStreams(16))
        .manual_flow_control(true)
        .on_begin_headers(|_ctx, _info| HeaderAction::Continue)
        .on_header(|_ctx, _info, _name, _value| HeaderAction::Continue)
        .on_data_chunk(|_ctx, _stream, _chunk| {})
        .on_frame(|_ctx, _info| {})
        .on_stream_close(|_ctx, _stream, _code, _err| {})
        .build()?;

    let _: Option<&[u8]> = session.send(&mut ())?;
    let _: usize = session.recv(&[], &mut ())?;

    let stream = session.submit_request(&[Header::new(":method", "GET")]);
    let _: Result<StreamId> = stream;
    let _: Result<StreamId> =
        session.submit_request_with_body(&[Header::new(":method", "GET")], BytesBody::new(vec![]));
    let _: Result<()> = session.submit_response(StreamId::new(1), &[Header::new(":status", "200")]);
    let _: Result<()> = session.submit_response_with_body(
        StreamId::new(1),
        &[Header::new(":status", "200")],
        BytesBody::new(vec![]),
    );
    let _: Result<()> = session.submit_trailer(StreamId::new(1), &[Header::new("a", "b")]);

    let _: bool = session.trailers_ready(StreamId::new(1));
    let _: Result<()> = session.consume(StreamId::new(1), 0);
    let _: Result<()> = session.reset_stream(StreamId::new(1), ErrorCode::CANCEL);
    let _: Result<()> = session.shutdown(StreamId::CONNECTION, ErrorCode::NO_ERROR);
    let _: bool = session.want_read();
    let _: bool = session.want_write();
    let _: bool = session.is_finished();
    let _: String = format!("{session:?}");

    // Added alongside the async layer; pinned here from the moment they exist.
    let _: bool = session.stream_is_open(StreamId::new(1));
    let _: bool = session.mid_frame();
    let _: Result<()> = session.resume_body(StreamId::new(1));

    Ok(())
}

/// The raw escape hatch remains reachable.
fn raw_surface() {
    let _: i32 = nghttp2::raw::NGHTTP2_ERR_DEFERRED;
}

#[test]
fn the_sans_io_surface_is_unchanged() {
    // Naming each function as a typed pointer both uses it — so nothing here is dead
    // code — and pins its signature, which is the surface being guarded. The ones that
    // need no constructed values are then actually run, so the fixture cannot pass by
    // being compiled and never exercised.
    let _: fn(HeaderAction) = closed_enumerations_are_still_closed;
    let _: fn(BodyOutcome, HeaderCategory, ErrorKind, Setting) = open_enumerations_stay_open;
    let _: fn(&Error) = error_surface;
    let _: fn(FrameInfo) = stream_surface;
    let _: fn(Goaway) = goaway_surface;

    error_code_surface();
    header_surface();
    setting_surface();
    body_surface();
    raw_surface();
    error_surface(&Error::from_native("nghttp2_session_send", -901));
    session_surface().expect("the session surface should still work as well as compile");
}

/// The asynchronous surface, present only when the `http` feature is on.
///
/// Kept in the same fixture as the sans-I/O surface, and gated rather than split out, so
/// there is one place to look for "what does this crate promise". The feature is on by
/// default, so this compiles in the ordinary build.
#[cfg(feature = "http")]
mod asynchronous {
    use std::error::Error as StdError;

    use nghttp2::http::testing::{
        bytes_crate as bytes, http_body_crate as http_body, http_crate as http,
    };
    use nghttp2::http::{
        Config, Error, ErrorKind, IncomingBody, ResponseFuture, SendRequest, Transport,
        TransportRead, TransportWrite, handshake, handshake_with,
    };

    /// The connection configuration, pinned as a `Copy` builder with conservative
    /// defaults and one setter per advertised limit.
    pub(super) fn config_surface() {
        let _: Config = Config::default();
        let configured: Config = Config::default()
            .max_concurrent_streams(64)
            .max_header_list_size(32 * 1024);
        // `Copy`, so a caller can keep one and hand copies to several connections.
        let _: Config = configured;
        let _: Config = configured;
    }

    /// The receiving body, pinned as an `http_body::Body` over the ecosystem's types.
    ///
    /// The associated types are the whole contract here: a caller writing a generic
    /// function over `Body<Data = Bytes>` must keep compiling, and so must one that
    /// matches on this crate's error.
    pub(super) fn incoming_body_surface(body: &IncomingBody) {
        fn assert_body<B: http_body::Body<Data = bytes::Bytes, Error = Error>>(_body: &B) {}
        assert_body(body);
        let _: bool = http_body::Body::is_end_stream(body);
        let _: http_body::SizeHint = http_body::Body::size_hint(body);
    }

    /// The receiving body is what a response carries, and it is not `()`.
    pub(super) fn response_surface(response: http::Response<IncomingBody>) {
        let _: http::StatusCode = response.status();
        let (parts, body): (http::response::Parts, IncomingBody) = response.into_parts();
        drop((parts, body));
    }

    /// The error taxonomy, left open so it can grow without breaking a caller's match.
    pub(super) fn error_surface(error: &Error) {
        let _: ErrorKind = error.kind();
        let _: bool = error.is_closed();
        let _: Option<&(dyn StdError + 'static)> = StdError::source(error);
        let _: String = error.to_string();

        let _: Option<nghttp2::ErrorCode> = error.reason();
        let _: bool = error.is_retriable();

        match error.kind() {
            ErrorKind::Transport
            | ErrorKind::Connection
            | ErrorKind::Stream
            | ErrorKind::Protocol
            | ErrorKind::Closed
            | ErrorKind::Body
            | ErrorKind::Refused => {}
            // Deliberate: adding a kind must not break a downstream match.
            _ => {}
        }
    }

    /// The signal a server handler learns cancellation through.
    pub(super) fn cancelled_surface(lost: &nghttp2::http::Cancelled) {
        let cloned: nghttp2::http::Cancelled = lost.clone();
        let _: bool = cloned.is_cancelled();
        // Held rather than awaited: naming the future is what pins its shape.
        let waiting = cloned.cancelled();
        drop(waiting);
    }

    /// The client entry point, pinned by naming its shape rather than by calling it.
    ///
    /// `handshake` is generic over both the transport and the body, and returns an
    /// unnameable future, so it cannot be pinned with a `fn` pointer. Naming the pieces a
    /// caller would name is the next best thing.
    pub(super) fn client_surface<T, B>(transport: T) -> core::result::Result<(), Error>
    where
        T: Transport,
        T::Reader: TransportRead,
        T::Writer: TransportWrite,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let (requests, connection): (SendRequest<B>, nghttp2::http::Connection<_>) =
            handshake::<T, B>(transport)?;
        let cloned: SendRequest<B> = requests.clone();
        let _: bool = cloned.is_closed();
        let _: bool = cloned.is_refusing();
        let _: fn(&SendRequest<B>) -> bool = SendRequest::<B>::is_closed;
        let _: fn(&SendRequest<B>) = SendRequest::<B>::shutdown;
        let response: ResponseFuture = requests.send_request(
            http::Request::builder()
                .uri("http://example.test/")
                .body(unreachable_body::<B>())
                .expect("a request"),
        );
        drop((response, connection));
        Ok(())
    }

    /// The explicit-config client entry point.
    ///
    /// Additive over [`client_surface`]: it takes a [`Config`] by value and returns the
    /// same pair, so a caller that never wanted to configure anything keeps using
    /// `handshake` unchanged.
    pub(super) fn client_with_config_surface<T, B>(
        transport: T,
        config: Config,
    ) -> core::result::Result<(), Error>
    where
        T: Transport,
        T::Reader: TransportRead,
        T::Writer: TransportWrite,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let (requests, connection): (SendRequest<B>, nghttp2::http::Connection<_>) =
            handshake_with::<T, B>(transport, config)?;
        drop((requests, connection));
        Ok(())
    }

    /// The server entry point, pinned the same way as the client's.
    ///
    /// A handler is an ordinary `FnMut` returning an ordinary future; nothing here is a
    /// trait this crate defines, which is the promise being pinned.
    pub(super) fn server_surface<T, H, F, B>(
        transport: T,
        handler: H,
    ) -> core::result::Result<(), Error>
    where
        T: Transport,
        T::Reader: TransportRead,
        T::Writer: TransportWrite,
        H: FnMut(http::Request<IncomingBody>) -> F,
        F: core::future::Future<Output = http::Response<B>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let connection = nghttp2::http::server::serve(transport, handler)?;
        drop(connection);
        Ok(())
    }

    /// The explicit-config server entry point, additive over [`server_surface`].
    pub(super) fn server_with_config_surface<T, H, F, B>(
        transport: T,
        handler: H,
        config: Config,
    ) -> core::result::Result<(), Error>
    where
        T: Transport,
        T::Reader: TransportRead,
        T::Writer: TransportWrite,
        H: FnMut(http::Request<IncomingBody>) -> F,
        F: core::future::Future<Output = http::Response<B>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let connection = nghttp2::http::serve_with(transport, handler, config)?;
        drop(connection);
        Ok(())
    }

    /// The ready-made tokio transport, when the feature that provides it is on.
    ///
    /// Feature-gated surface is still surface: a caller who enabled it is entitled to the
    /// same promise as one who did not.
    #[cfg(feature = "tokio")]
    pub(super) fn tokio_transport_surface<T>(stream: T)
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    {
        use nghttp2::http::transport::{TokioIo, TokioReader, TokioWriter};

        let carried: TokioIo<T> = TokioIo::new(stream);
        let (reader, writer): (TokioReader<T>, TokioWriter<T>) = Transport::split(carried);
        write_half_surface::<TokioWriter<T>>();
        drop((reader, writer));
    }

    /// The ready-made compio transport, when the feature that provides it is on.
    ///
    /// Generic over compio's `Splittable`, so the halves are named through associated types
    /// rather than as one concrete pair — a caller wrapping a `UnixStream` is entitled to
    /// the same promise as one wrapping a `TcpStream`.
    #[cfg(feature = "completion")]
    pub(super) fn compio_transport_surface<T>(stream: T)
    where
        T: compio::io::util::Splittable,
        T::ReadHalf: compio::io::AsyncRead,
        T::WriteHalf: compio::io::AsyncWrite,
    {
        use nghttp2::http::transport::{CompioIo, CompioReader, CompioWriter};

        let carried: CompioIo<T> = CompioIo::new(stream);
        let (reader, writer): (CompioReader<T::ReadHalf>, CompioWriter<T::WriteHalf>) =
            Transport::split(carried);
        write_half_surface::<CompioWriter<T::WriteHalf>>();
        drop((reader, writer));
    }

    /// The writing half's contract, pinned by the shape of its three overridable points.
    ///
    /// Each fast path is a *single* override: `write_borrowed` and `write_vectored` each
    /// return an `Option` of a future, so the decision (`Some`/`None`) and the write are one
    /// method — a separate boolean flag would be a different, breakable, surface. `commit`
    /// returns a future of `()`. All are pinned as signatures rather than fn pointers
    /// because a return-position `impl Future` has no nameable type.
    pub(super) fn write_half_surface<W: TransportWrite>() {
        fn borrowed_is_one_optional_future<W: TransportWrite>(writer: &mut W, data: &[u8]) {
            fn assert_optional_future<F: core::future::Future<Output = std::io::Result<usize>>>(
                _: Option<F>,
            ) {
            }
            assert_optional_future(writer.write_borrowed(data));
        }
        fn vectored_is_one_optional_future<W: TransportWrite>(
            writer: &mut W,
            regions: &[std::io::IoSlice<'_>],
        ) {
            fn assert_optional_future<F: core::future::Future<Output = std::io::Result<usize>>>(
                _: Option<F>,
            ) {
            }
            assert_optional_future(writer.write_vectored(regions));
        }
        fn commit_returns_a_result_future<W: TransportWrite>(writer: &mut W) {
            fn assert_future<F: core::future::Future<Output = std::io::Result<()>>>(_: F) {}
            assert_future(writer.commit());
        }
        let _ = borrowed_is_one_optional_future::<W>;
        let _ = vectored_is_one_optional_future::<W>;
        let _ = commit_returns_a_result_future::<W>;
    }

    /// Never called. Its only job is to give the fixture above a `B` to hand over.
    fn unreachable_body<B>() -> B {
        unreachable!("the client surface fixture is never executed")
    }
}

#[cfg(feature = "http")]
#[test]
fn the_asynchronous_surface_is_unchanged() {
    use nghttp2::http::testing::{Duplex, Empty};

    let _: fn(&nghttp2::http::Error) = asynchronous::error_surface;
    let _: fn(Duplex) -> core::result::Result<(), nghttp2::http::Error> =
        asynchronous::client_surface::<Duplex, Empty>;
    let _: fn(&nghttp2::http::IncomingBody) = asynchronous::incoming_body_surface;
    let _: fn(&nghttp2::http::Cancelled) = asynchronous::cancelled_surface;
    // The writing half's three overridable points, pinned generically so the shape holds for
    // every transport, not only the ready-made tokio one.
    asynchronous::write_half_surface::<nghttp2::http::testing::DuplexWriter>();
    // The vectored testing transport and its observation handle. Hidden from the docs but
    // still public, and integration tests are separate crates that can reach nothing else.
    let _: fn() -> (Duplex, Duplex) = nghttp2::http::testing::duplex_vectored;
    let _: fn(&Duplex) -> nghttp2::http::testing::VectoredLog = Duplex::vectored_log;
    let _: fn(&nghttp2::http::testing::VectoredLog) -> Vec<Vec<usize>> =
        nghttp2::http::testing::VectoredLog::calls;
    let _: fn(&nghttp2::http::testing::VectoredLog) -> Vec<u8> =
        nghttp2::http::testing::VectoredLog::octets;
    let _: fn(&nghttp2::http::testing::VectoredLog) -> usize =
        nghttp2::http::testing::VectoredLog::retries;
    let _: fn(&nghttp2::http::testing::VectoredLog) = nghttp2::http::testing::VectoredLog::reset;
    let _: fn(&Duplex, Vec<usize>) = |duplex, caps| duplex.accept_at_most(caps);
    let _: fn() = asynchronous::config_surface;
    let _: fn(Duplex, nghttp2::http::Config) -> core::result::Result<(), nghttp2::http::Error> =
        asynchronous::client_with_config_surface::<Duplex, Empty>;
    #[cfg(feature = "tokio")]
    {
        let _: fn(tokio::net::TcpStream) = asynchronous::tokio_transport_surface::<_>;
    }
    #[cfg(feature = "completion")]
    {
        let _: fn(compio::net::TcpStream) = asynchronous::compio_transport_surface::<_>;
        let _: fn(compio::net::UnixStream) = asynchronous::compio_transport_surface::<_>;
    }

    // `serve` is reachable both through the module and at the top of `http`, and both are
    // part of the promise. A `fn` item as the handler and `Ready` as its future keep every
    // type here nameable, which a closure would not.
    type Answer = core::future::Ready<nghttp2::http::testing::http_crate::Response<Empty>>;
    fn answer(
        _: nghttp2::http::testing::http_crate::Request<nghttp2::http::IncomingBody>,
    ) -> Answer {
        core::future::ready(nghttp2::http::testing::http_crate::Response::new(Empty))
    }
    let (direct, _peer) = nghttp2::http::testing::duplex(false);
    drop(nghttp2::http::serve(direct, answer).expect("serving"));
    let (qualified, _peer) = nghttp2::http::testing::duplex(false);
    drop(nghttp2::http::server::serve(qualified, answer).expect("serving"));

    // And the generic shape a caller writes against, pinned separately from the concrete
    // call above: `serve` must stay usable from a function generic over all four.
    let (generic, _peer) = nghttp2::http::testing::duplex(false);
    asynchronous::server_surface(generic, answer).expect("serving");
    let (configured, _peer) = nghttp2::http::testing::duplex(false);
    asynchronous::server_with_config_surface(configured, answer, nghttp2::http::Config::default())
        .expect("serving");
    let (top_level, _peer) = nghttp2::http::testing::duplex(false);
    drop(
        nghttp2::http::serve_with(top_level, answer, nghttp2::http::Config::default())
            .expect("serving"),
    );
    let _: fn(nghttp2::http::testing::http_crate::Response<nghttp2::http::IncomingBody>) =
        asynchronous::response_surface;

    // The ecosystem types are part of the promise too: a caller hands over an
    // `http::Request` and gets back an `http::Response`, not a bespoke type.
    let (client_side, _peer) = nghttp2::http::testing::duplex(false);
    let (requests, connection) =
        nghttp2::http::handshake::<Duplex, Empty>(client_side).expect("handshake");
    let response: nghttp2::http::ResponseFuture = requests.send_request(
        nghttp2::http::testing::http_crate::Request::builder()
            .uri("http://example.test/")
            .body(Empty)
            .expect("a request"),
    );
    drop(connection);

    let error = nghttp2::http::testing::block_on(response).expect_err("the driver is gone");
    asynchronous::error_surface(&error);
    assert_eq!(error.kind(), nghttp2::http::ErrorKind::Closed);
}
