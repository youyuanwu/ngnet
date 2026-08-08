//! What a connection advertises and how much of it will run at once.

/// Settings for an asynchronous HTTP/3 connection.
///
/// The defaults match the HTTP/2 crate beside this one wherever the two correspond, so a
/// reader moving between them does not have to relearn the numbers.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub(crate) max_concurrent_streams: u32,
    pub(crate) max_field_section_size: u64,
    pub(crate) qpack_max_dtable_capacity: usize,
    pub(crate) qpack_blocked_streams: usize,
    pub(crate) events_per_pass: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 128,
            max_field_section_size: 64 * 1024,
            qpack_max_dtable_capacity: 4096,
            qpack_blocked_streams: 16,
            events_per_pass: 64,
        }
    }
}

impl Config {
    /// How many exchanges may be outstanding at once.
    ///
    /// On a server this also caps how many handler futures the driver holds, including ones
    /// still running on streams the peer has abandoned.
    #[must_use]
    pub fn max_concurrent_streams(mut self, streams: u32) -> Self {
        self.max_concurrent_streams = streams;
        self
    }

    /// The largest field section this endpoint will accept, in bytes.
    ///
    /// Advertised to the peer, and it bounds the copy a hostile one can force per exchange.
    #[must_use]
    pub fn max_field_section_size(mut self, bytes: u64) -> Self {
        self.max_field_section_size = bytes;
        self
    }

    /// How much QPACK dynamic table the peer's encoder may use.
    ///
    /// Zero disables the dynamic table, which costs compression and removes the encoder's
    /// ability to make a stream wait on an insertion it has not seen yet.
    #[must_use]
    pub fn qpack_max_dtable_capacity(mut self, bytes: usize) -> Self {
        self.qpack_max_dtable_capacity = bytes;
        self
    }

    /// How many streams may be blocked waiting on QPACK insertions at once.
    #[must_use]
    pub fn qpack_blocked_streams(mut self, streams: usize) -> Self {
        self.qpack_blocked_streams = streams;
        self
    }

    /// How many transport events the driver takes before it must do other work.
    ///
    /// A bound rather than a tuning knob. Without one, a stream producing data faster than
    /// the driver consumes it would keep the event source perpetually ready and starve
    /// writes, handlers, releases and resets — so the pass takes at most this many and then
    /// moves on, whatever is left waiting.
    #[must_use]
    pub fn events_per_pass(mut self, events: usize) -> Self {
        self.events_per_pass = events.max(1);
        self
    }
}
