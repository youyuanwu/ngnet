//! Observation hooks for this crate's own integration tests. Not a public API.
//!
//! Hidden for the same reason, and following the same precedent, as `ngnet-h2`'s module of
//! the same name: an integration test is a separate crate, so it cannot reach `#[cfg(test)]`
//! items, and a guarantee that cannot be observed cannot be tested. The alternative is
//! synchronising on elapsed time, which produces tests that pass on a fast machine and fail
//! on a loaded one — and, worse, tests that pass for the wrong reason.
//!
//! Everything here is a *read* of state the crate already maintains. Nothing here can change
//! the client's behaviour, and none of it is covered by any compatibility promise.

use bytes::Bytes;
use http_body::Body;

use crate::Client;

/// How many name resolutions this client has performed.
///
/// The only observable for "a request served by a pooled connection resolves nothing": a
/// lookup that did not happen leaves no trace at a server, which saw no new connection
/// either way, so without this the claim could only be asserted by reading the source.
pub fn resolution_count<B>(client: &Client<B>) -> usize
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    client.pool().resolution_count()
}

/// Whether the client currently holds a connection for `authority` that it would use again.
///
/// This is how the eviction test knows the client has *observed* the peer's `GOAWAY` rather
/// than merely having been given time to. Polling this until it turns false is a wait on the
/// actual event; sleeping is a wait on a guess about it.
pub fn has_eligible_connection<B>(client: &Client<B>, authority: &str) -> bool
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    client.pool().has_eligible_connection(authority)
}
