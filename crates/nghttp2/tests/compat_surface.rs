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
        | Setting::EnableConnectProtocol(_) => {}
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
    let _: Header<'_> = Header::new("name", "value");
    let _: Header<'_> = Header::from_bytes(b"name", b"value");
    let _: Header<'_> = Header::new("authorization", "secret").sensitive();
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
    body_surface();
    raw_surface();
    error_surface(&Error::from_native("nghttp2_session_send", -901));
    session_surface().expect("the session surface should still work as well as compile");
}
