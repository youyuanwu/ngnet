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
//! Not everything here is advertised. [`WritePolicy`] is a purely local choice about the
//! shape of this endpoint's writes — how a pass of session output becomes syscalls. It is
//! carried on the same type because it is settled at the same moment, per connection, by the
//! same caller, and threading a second configuration value through the same four entry points
//! would buy nothing.

/// How an asynchronous connection is configured: limits it advertises to its peer and
/// enforces locally, plus the local shape of its writes.
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
    write_policy: WritePolicy,
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
            write_policy: WritePolicy::Gathered,
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

    /// Chooses how a pass of session output becomes writes on the transport.
    ///
    /// Defaults to [`WritePolicy::Gathered`]. See that type for what each policy costs and
    /// when turning gathering off is worth it.
    #[must_use]
    pub fn write_policy(mut self, policy: WritePolicy) -> Self {
        self.write_policy = policy;
        self
    }

    pub(crate) fn concurrency(&self) -> u32 {
        self.max_concurrent_streams
    }

    pub(crate) fn header_list_size(&self) -> u32 {
        self.max_header_list_size
    }

    pub(crate) fn policy(&self) -> WritePolicy {
        self.write_policy
    }
}

/// How a pass of session output becomes writes on the transport.
///
/// This is a decision for the layer that owns the accumulation buffer and knows the region
/// count — not for the transport, which knows only how to write. A transport declares its I/O
/// model ([`Readiness`] or [`Completion`]) and always supplies a gathering operation; this
/// chooses whether to use it.
///
/// [`Readiness`]: super::transport::Readiness
/// [`Completion`]: super::transport::Completion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WritePolicy {
    /// Gather each pass into as few writes as possible. **The default.**
    ///
    /// Small session blocks accumulate into a driver-owned run; large ones and handed-over
    /// payloads ride uncopied as their own regions; the whole list goes out as one gathering
    /// write. A multiplexed pass of hundreds of small blocks becomes one region and one
    /// write.
    ///
    /// Whether that write reaches a real `writev` depends on the transport: one that
    /// overrides its model's gathering operation reaches one syscall, and one that does not
    /// gets the provided default, which writes each region in turn. Both deliver the same
    /// octets in the same order — the difference is syscall count, and it is bounded, because
    /// the accumulation that collapses small blocks into one region happens either way.
    #[default]
    Gathered,
    /// Copy each pass into one contiguous driver-owned buffer and write that.
    ///
    /// One write offer per pass, at the cost of copying **every** outgoing octet, including
    /// payloads that would otherwise have been handed to the transport untouched. The buffer
    /// is reused across passes, so this costs no allocation in steady state. A short write is
    /// re-offered from the remainder, so "one write per pass" is the shape, not a promise
    /// about syscall count.
    ///
    /// This is worth choosing when a write costs more than a copy: a transport with real
    /// per-write overhead — a TLS record layer, a userspace stack, an encrypted tunnel — that
    /// does not implement a native gathering write. Under [`Gathered`](WritePolicy::Gathered)
    /// such a transport pays one write per region, and on a pass carrying many handed-over
    /// payloads that can be dozens.
    ///
    /// On a completion transport this is close to pure loss: it replaces one owned vectored
    /// submission with one owned contiguous write plus a copy of every octet, and there is no
    /// per-write overhead being saved. Its use there is diagnostic — bisecting whether a
    /// fault lies in the region path — rather than performance.
    Coalesced,
}
