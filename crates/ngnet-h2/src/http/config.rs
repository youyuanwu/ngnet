//! Connection limits for an asynchronous client or server.
//!
//! An asynchronous connection runs every handler on the task that polls its driver and
//! copies every header block it receives into owned storage before dispatch. Both are
//! unbounded in the number of streams the peer opens unless something bounds them, so a
//! single peer can force this crate to hold an unbounded number of handler futures or copy
//! an unbounded header list. libnghttp2's own local defaults do not help here — its
//! `SETTINGS_MAX_CONCURRENT_STREAMS` default is `0xFFFFFFFF` and its
//! `SETTINGS_MAX_HEADER_LIST_SIZE` default is `UINT32_MAX` — so this crate advertises its
//! own, and a caller that knows its peer can widen them.
//!
//! Everything here is advertised, and that is the type's whole remit. An earlier revision
//! also carried a local write-shaping knob here — how a pass of session output became
//! syscalls — on the reasoning that it was settled at the same moment, per connection, by
//! the same caller. The reasoning was sound about *when*; it was wrong about *who*. The
//! answer depends on whether the transport's gathering write reaches a real scatter-gather
//! call, which the caller generally does not know and the transport always does. It is now
//! asked of the transport, once per connection, through
//! [`TransportWrite::is_write_vectored`](super::transport::TransportWrite::is_write_vectored),
//! and there is nothing to configure.

/// How an asynchronous connection is configured: the limits it advertises to its peer and
/// enforces locally.
///
/// # Examples
///
/// The write-shaping knob this type used to carry is gone. It is not deprecated and not
/// hidden; the name does not resolve, because the decision it named is no longer the
/// caller's to make:
///
/// ```compile_fail,E0599
/// // `write_policy` is not a method on `Config`; how a pass becomes writes is now the
/// // transport's declaration, via `TransportWrite::is_write_vectored`.
/// let _ = ngnet_h2::http::Config::default().write_policy(todo!());
/// ```
///
/// ```compile_fail,E0433
/// // Nor is there a type to pass it. `WritePolicy` does not exist.
/// fn takes(_: ngnet_h2::http::WritePolicy) {}
/// ```
///
/// The defaults are deliberately conservative; the setters exist for a caller that wants
/// to trade that headroom away. This is an additive surface: [`handshake`] and [`serve`]
/// use the defaults, and [`handshake_with`] and [`serve_with`] take a value of this type.
///
/// [`handshake`]: super::handshake
/// [`serve`]: super::serve
/// [`handshake_with`]: super::handshake_with
/// [`serve_with`]: super::serve_with
#[derive(Debug, Clone, Copy)]
pub struct Config {
    max_concurrent_streams: u32,
    max_header_list_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // 128 concurrent streams is nginx's `http2_max_concurrent_streams` default and
            // sits below hyper's server default of 200. On this crate it is also the
            // ceiling on how many handler futures one peer can have in flight at once, so a
            // modest value keeps that structural bound tight while staying comfortably
            // above what an ordinary peer multiplexes.
            max_concurrent_streams: 128,
            // 64 KiB comfortably holds ordinary request and response header sets — cookies
            // included — while bounding the copy a hostile peer can force per stream.
            // Smaller than Go's 1 MiB and far smaller than hyper/h2's 16 MiB defaults,
            // chosen because h2c here is often an internal hop where headers stay small and
            // the copy, not interoperability, is the thing worth bounding.
            max_header_list_size: 64 * 1024,
        }
    }
}

impl Config {
    /// Sets the maximum number of streams the peer may have open at once.
    ///
    /// This is advertised in `SETTINGS_MAX_CONCURRENT_STREAMS` and, on a server, is also
    /// the ceiling on concurrently running handler futures.
    #[must_use]
    pub fn max_concurrent_streams(mut self, streams: u32) -> Self {
        self.max_concurrent_streams = streams;
        self
    }

    /// Sets the maximum header list size, in octets, this endpoint will accept.
    ///
    /// This is advertised in `SETTINGS_MAX_HEADER_LIST_SIZE`.
    #[must_use]
    pub fn max_header_list_size(mut self, octets: u32) -> Self {
        self.max_header_list_size = octets;
        self
    }

    pub(crate) fn concurrency(&self) -> u32 {
        self.max_concurrent_streams
    }

    pub(crate) fn header_list_size(&self) -> u32 {
        self.max_header_list_size
    }
}
