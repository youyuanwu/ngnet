//! Stream identifiers and the frame view handed to handlers.
//!
//! `StreamId` is introduced here rather than alongside message submission because the
//! receive handlers need to name the stream an event concerns.

use core::fmt;

use ngnet_h2_sys as sys;

/// Identifies one request/response exchange within a connection.
///
/// Stream zero is the connection itself and never carries a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(i32);

impl StreamId {
    /// The connection control stream.
    pub const CONNECTION: Self = Self(0);

    /// Wraps a raw stream identifier.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// The underlying integer.
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Whether this names the connection rather than a stream.
    pub const fn is_connection(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The type of an HTTP/2 frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameType(u8);

impl FrameType {
    /// A `DATA` frame, carrying message payload.
    pub const DATA: Self = Self(sys::NGHTTP2_DATA as u8);
    /// A `HEADERS` frame, opening a message or carrying trailers.
    pub const HEADERS: Self = Self(sys::NGHTTP2_HEADERS as u8);
    /// A `RST_STREAM` frame, terminating one stream.
    pub const RST_STREAM: Self = Self(sys::NGHTTP2_RST_STREAM as u8);
    /// A `SETTINGS` frame.
    pub const SETTINGS: Self = Self(sys::NGHTTP2_SETTINGS as u8);
    /// A `PUSH_PROMISE` frame. Server push is not supported by this crate.
    pub const PUSH_PROMISE: Self = Self(sys::NGHTTP2_PUSH_PROMISE as u8);
    /// A `PING` frame.
    pub const PING: Self = Self(sys::NGHTTP2_PING as u8);
    /// A `GOAWAY` frame, shutting the connection down.
    pub const GOAWAY: Self = Self(sys::NGHTTP2_GOAWAY as u8);
    /// A `WINDOW_UPDATE` frame, replenishing a flow-control window.
    pub const WINDOW_UPDATE: Self = Self(sys::NGHTTP2_WINDOW_UPDATE as u8);
    /// A `CONTINUATION` frame. These are never reported to handlers.
    pub const CONTINUATION: Self = Self(sys::NGHTTP2_CONTINUATION as u8);

    /// The raw frame type octet.
    pub const fn get(self) -> u8 {
        self.0
    }

    pub(crate) const fn new(kind: u8) -> Self {
        Self(kind)
    }
}

/// What role a received header block plays in its message.
///
/// HTTP/2 uses one frame type for every header block, so the block that opens a message
/// and the block that trails it are distinguished only by this category. Without it a
/// trailing block is indistinguishable from a second set of response headers.
///
/// Marked non-exhaustive: the protocol's category list is libnghttp2's, not this crate's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HeaderCategory {
    /// Opens a request.
    Request,
    /// Opens a response.
    Response,
    /// Opens a pushed response. Server push is out of scope for this crate.
    PushResponse,
    /// A block that is neither of the above — on a stream that already carries a message,
    /// this is a trailing header block.
    Trailing,
}

impl HeaderCategory {
    pub(crate) fn from_native(cat: sys::nghttp2_headers_category) -> Option<Self> {
        match cat {
            sys::NGHTTP2_HCAT_REQUEST => Some(Self::Request),
            sys::NGHTTP2_HCAT_RESPONSE => Some(Self::Response),
            sys::NGHTTP2_HCAT_PUSH_RESPONSE => Some(Self::PushResponse),
            sys::NGHTTP2_HCAT_HEADERS => Some(Self::Trailing),
            _ => None,
        }
    }

    /// Whether this block trails a message rather than opening one.
    pub const fn is_trailing(self) -> bool {
        matches!(self, Self::Trailing)
    }
}

/// The payload of a received `GOAWAY` frame.
///
/// A peer sends this to say it is shutting the connection down and how much of what it
/// received it actually processed. Streams above [`Self::last_stream_id`] were not
/// processed and may safely be retried on another connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Goaway {
    last_stream_id: StreamId,
    code: crate::error::ErrorCode,
}

impl Goaway {
    pub(crate) const fn new(last_stream_id: StreamId, code: crate::error::ErrorCode) -> Self {
        Self {
            last_stream_id,
            code,
        }
    }

    /// The highest stream the peer processed. Anything above this was abandoned.
    pub const fn last_stream_id(self) -> StreamId {
        self.last_stream_id
    }

    /// Why the peer is going away.
    pub const fn code(self) -> crate::error::ErrorCode {
        self.code
    }
}

/// What a handler was told about the frame that triggered it.
///
/// This is a borrowed view over libnghttp2's frame header, valid only for the duration
/// of the handler call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameInfo {
    stream_id: StreamId,
    kind: FrameType,
    flags: u8,
    payload_len: usize,
    category: Option<HeaderCategory>,
    goaway: Option<Goaway>,
}

impl FrameInfo {
    pub(crate) fn from_header(hd: &sys::nghttp2_frame_hd) -> Self {
        Self {
            stream_id: StreamId::new(hd.stream_id),
            kind: FrameType(hd.type_),
            flags: hd.flags,
            payload_len: hd.length,
            category: None,
            goaway: None,
        }
    }

    /// Builds a view that also carries the type-specific detail this crate exposes.
    ///
    /// The union reads that produce `category` and `goaway` are deliberately left to
    /// `callbacks`, which is already permitted `unsafe`; this keeps the frame types free
    /// of it rather than widening the crate's confinement to reach one union member.
    pub(crate) fn with_details(
        hd: &sys::nghttp2_frame_hd,
        category: Option<HeaderCategory>,
        goaway: Option<Goaway>,
    ) -> Self {
        Self {
            category,
            goaway,
            ..Self::from_header(hd)
        }
    }

    /// What role this header block plays, for frames that carry one.
    ///
    /// `None` for every frame type other than `HEADERS`, and for a category this crate
    /// does not recognise.
    pub const fn category(self) -> Option<HeaderCategory> {
        self.category
    }

    /// Whether this frame is a trailing header block.
    pub const fn is_trailers(self) -> bool {
        matches!(self.category, Some(HeaderCategory::Trailing))
    }

    /// The `GOAWAY` payload, for frames that carry one.
    pub const fn goaway(self) -> Option<Goaway> {
        self.goaway
    }

    /// The stream this frame belongs to.
    pub const fn stream_id(self) -> StreamId {
        self.stream_id
    }

    /// The frame's type.
    pub const fn kind(self) -> FrameType {
        self.kind
    }

    /// The raw flags octet.
    pub const fn flags(self) -> u8 {
        self.flags
    }

    /// The frame payload length in octets, excluding the nine-octet header.
    pub const fn payload_len(self) -> usize {
        self.payload_len
    }

    /// Whether this frame closes its stream in the sending direction.
    ///
    /// Only meaningful for frames that carry message content. HTTP/2 reuses the `0x01`
    /// flag bit as ACK on `SETTINGS` and `PING`, so reporting it as end-of-stream there
    /// would be wrong — and those frames belong to the connection, not a stream.
    pub const fn is_end_stream(self) -> bool {
        (self.kind.0 == FrameType::DATA.0 || self.kind.0 == FrameType::HEADERS.0)
            && self.flags & (sys::NGHTTP2_FLAG_END_STREAM as u8) != 0
    }

    /// Whether this frame acknowledges an earlier one.
    ///
    /// Only `SETTINGS` and `PING` carry an acknowledgement, using the same flag bit that
    /// means end-of-stream elsewhere.
    pub const fn is_ack(self) -> bool {
        (self.kind.0 == FrameType::SETTINGS.0 || self.kind.0 == FrameType::PING.0)
            && self.flags & (sys::NGHTTP2_FLAG_ACK as u8) != 0
    }

    /// Whether this frame completes a header block.
    ///
    /// Only meaningful for the frame types that carry one. As with
    /// [`Self::is_end_stream`], the flag bit is reused for other purposes elsewhere.
    pub const fn is_end_headers(self) -> bool {
        (self.kind.0 == FrameType::HEADERS.0
            || self.kind.0 == FrameType::PUSH_PROMISE.0
            || self.kind.0 == FrameType::CONTINUATION.0)
            && self.flags & (sys::NGHTTP2_FLAG_END_HEADERS as u8) != 0
    }
}
