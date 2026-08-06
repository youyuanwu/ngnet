//! HTTP/2 connection settings.

use ngnet_h2_sys as sys;

/// A single HTTP/2 setting and the value to advertise for it.
///
/// These are the eight identifiers libnghttp2 v1.70.0 recognises. Values are advertised
/// to the peer in the `SETTINGS` frame the session emits when it is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Setting {
    /// Maximum size of the header compression table, in octets.
    HeaderTableSize(u32),
    /// Whether the peer may initiate pushed streams.
    ///
    /// This crate does not support server push, so client sessions advertise `false`
    /// unless the caller overrides it. A server may not advertise `true`.
    EnablePush(bool),
    /// Maximum number of concurrent streams the peer may open.
    MaxConcurrentStreams(u32),
    /// Initial flow-control window size for new streams, in octets.
    InitialWindowSize(u32),
    /// Largest frame payload the endpoint is willing to receive, in octets.
    MaxFrameSize(u32),
    /// Advisory maximum size of the header list, in octets.
    MaxHeaderListSize(u32),
    /// Whether the extended CONNECT protocol is supported (RFC 8441).
    EnableConnectProtocol(bool),
    /// Whether RFC 7540 stream priorities are disabled (RFC 9218).
    NoRfc7540Priorities(bool),
}

impl Setting {
    /// The wire identifier for this setting.
    pub const fn id(self) -> i32 {
        match self {
            Self::HeaderTableSize(_) => sys::NGHTTP2_SETTINGS_HEADER_TABLE_SIZE as i32,
            Self::EnablePush(_) => sys::NGHTTP2_SETTINGS_ENABLE_PUSH as i32,
            Self::MaxConcurrentStreams(_) => sys::NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS as i32,
            Self::InitialWindowSize(_) => sys::NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE as i32,
            Self::MaxFrameSize(_) => sys::NGHTTP2_SETTINGS_MAX_FRAME_SIZE as i32,
            Self::MaxHeaderListSize(_) => sys::NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE as i32,
            Self::EnableConnectProtocol(_) => {
                sys::NGHTTP2_SETTINGS_ENABLE_CONNECT_PROTOCOL as i32
            }
            Self::NoRfc7540Priorities(_) => sys::NGHTTP2_SETTINGS_NO_RFC7540_PRIORITIES as i32,
        }
    }

    /// The wire value for this setting.
    pub const fn value(self) -> u32 {
        match self {
            Self::HeaderTableSize(v)
            | Self::MaxConcurrentStreams(v)
            | Self::InitialWindowSize(v)
            | Self::MaxFrameSize(v)
            | Self::MaxHeaderListSize(v) => v,
            Self::EnablePush(v)
            | Self::EnableConnectProtocol(v)
            | Self::NoRfc7540Priorities(v) => v as u32,
        }
    }

    pub(crate) const fn entry(self) -> sys::nghttp2_settings_entry {
        sys::nghttp2_settings_entry {
            settings_id: self.id(),
            value: self.value(),
        }
    }
}
