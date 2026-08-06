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

/// Limits an asynchronous connection advertises to its peer and enforces locally.
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
