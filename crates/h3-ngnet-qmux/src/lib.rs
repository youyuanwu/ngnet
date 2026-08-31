//! Hyperium H3 transport traits over an established QMux connection.
//!
//! The caller first constructs an [`ngnet_qmux::io::Connection`] over an ordered byte stream,
//! then passes it to [`from_qmux`] or [`from_qmux_with_config`]. Construction returns an
//! H3-facing [`Connection`] and exactly one [`Driver`].
//!
//! # Progress and close invariant
//!
//! The driver must be polled concurrently for the entire lifetime of the H3 connection and
//! its stream handles. H3 operations may borrow one bounded progress turn, but the driver is
//! the stable lower-I/O wake target and the only owner of shutdown completion. In particular,
//! hyperium's `OpenStreams::close` is synchronous: it records the first close reason and wakes
//! the driver, but it cannot claim that the close reached the peer. If the driver and all
//! capable handles are dropped before another driver poll, buffered stream data and the close
//! are not guaranteed to be delivered.
//!
//! # Bounds and portability
//!
//! One adapter turn routes at most 64 QMux events and admits at most one lower read batch.
//! Pending peer streams are capped by [`AdapterConfig::pending_accept_limit`]. Each sending
//! handle may retain one generic hyperium `WriteBuf` without copying its body into another
//! body-sized adapter buffer. QMux separately bounds produced lower output.
//!
//! This crate owns no endpoint, listener, TLS, socket, runtime, executor, task, or timer. It
//! never spawns. Sendability follows the caller's byte stream, clock, and body-buffer types;
//! none is required to be `Send`.
#![deny(missing_docs, unsafe_code)]

mod connection;
#[cfg(feature = "diagnostics")]
pub mod diagnostics;
mod driver;
mod error;
mod state;
mod stream;

use std::sync::{Arc, Mutex};

use bytes::Buf;
use ngnet_qmux::io::{AsyncByteStream, Clock, Connection as QmuxConnection};

pub use connection::{Connection, Observer, OpenStreams, Snapshot};
pub use driver::Driver;
pub use error::Error;
pub use stream::{BidiStream, RecvStream, SendStream};

use state::{Core, LowerWake};
use stream::{SendSlots, Shared};

/// Default maximum number of peer streams waiting to be accepted by hyperium.
pub const DEFAULT_PENDING_ACCEPT_LIMIT: usize = 128;

/// Adapter-owned resource policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdapterConfig {
    pending_accept_limit: usize,
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            pending_accept_limit: DEFAULT_PENDING_ACCEPT_LIMIT,
        }
    }
}

impl AdapterConfig {
    /// The default adapter policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps peer streams discovered but not yet returned from an H3 accept poll.
    ///
    /// This is a local memory policy, not QMux stream-level backpressure. Discovering one
    /// stream beyond the limit closes the connection with `H3_EXCESSIVE_LOAD`, because
    /// hyperium's accept traits have no per-stream rejection result.
    #[must_use]
    pub const fn pending_accept_limit(mut self, limit: usize) -> Self {
        self.pending_accept_limit = limit;
        self
    }

    /// The configured pending-accept cap.
    #[must_use]
    pub const fn get_pending_accept_limit(self) -> usize {
        self.pending_accept_limit
    }
}

/// Adapts an established QMux connection using the default adapter policy.
///
/// `B` is hyperium's generic body-buffer type. It is retained by the driver-side send-slot
/// registry when a framed logical send is partially accepted, which lets peer stop events and
/// connection failure discard that buffer during routing rather than waiting for its owning
/// stream handle to be polled again.
#[must_use = "construction returns a driver which must be polled"]
pub fn from_qmux<B, S, C>(
    connection: QmuxConnection<S, C>,
) -> (Connection<S, C, B>, Driver<S, C, B>)
where
    B: Buf,
    S: AsyncByteStream,
    C: Clock,
{
    from_qmux_with_config(connection, AdapterConfig::new())
}

/// Adapts an established QMux connection using an explicit adapter policy.
#[must_use = "construction returns a driver which must be polled"]
pub fn from_qmux_with_config<B, S, C>(
    connection: QmuxConnection<S, C>,
    config: AdapterConfig,
) -> (Connection<S, C, B>, Driver<S, C, B>)
where
    B: Buf,
    S: AsyncByteStream,
    C: Clock,
{
    let core = Arc::new(Mutex::new(Core::new(
        connection,
        config.pending_accept_limit,
    )));
    let lower_wake = Arc::new(LowerWake::default());
    let shared = Shared { core, lower_wake };
    let slots = Arc::new(Mutex::new(SendSlots::default()));
    let connection = Connection::new(shared.clone(), Arc::clone(&slots), 0);
    let driver = Driver::new(shared, slots);
    (connection, driver)
}

/// Adapts a QMux connection built over [`diagnostics::ObservedStream`].
///
/// The handle is accepted as construction evidence and is also used by the caller to arm,
/// snapshot, and drain the combined lower-I/O and adapter interval.
#[cfg(feature = "diagnostics")]
#[must_use = "construction returns a driver which must be polled"]
pub fn from_qmux_with_diagnostics<B, S, C>(
    connection: QmuxConnection<S, C>,
    _lower: diagnostics::LowerIoHandle,
    config: AdapterConfig,
) -> (Connection<S, C, B>, Driver<S, C, B>)
where
    B: Buf,
    S: AsyncByteStream,
    C: Clock,
{
    from_qmux_with_config(connection, config)
}
