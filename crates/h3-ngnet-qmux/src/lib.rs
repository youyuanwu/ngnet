//! Hyperium H3 transport traits over an established QMux connection.
//!
//! The caller first constructs an [`ngnet_qmux::io::Connection`] over an ordered byte stream,
//! then passes it to [`from_qmux`]. Construction returns an
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
//! Pending peer streams are capped by the `pending_accept_limit` passed to [`from_qmux`].
//! Each stream may retain one hyperium `WriteBuf<Bytes>` directly in the shared core. QMux
//! separately bounds produced lower output.
//!
//! This crate owns no endpoint, listener, TLS, socket, runtime, executor, task, or timer. It
//! never spawns. Sendability follows the caller's byte stream and clock; neither is required
//! to be `Send`.
#![deny(missing_docs, unsafe_code)]

mod connection;

mod driver;
mod error;
mod state;
mod stream;

use std::sync::{Arc, Mutex};

use ngnet_qmux::io::{AsyncByteStream, Clock, Connection as QmuxConnection};

pub use connection::{Connection, OpenStreams};
pub use driver::Driver;
pub use error::Error;
pub use stream::{BidiStream, RecvStream, SendStream};

use state::{Core, LowerWake};
use stream::Shared;

/// Adapts an established QMux connection for hyperium H3 with `Bytes` framed bodies.
///
/// `pending_accept_limit` bounds peer streams discovered before H3 accepts them. Exceeding it
/// closes the connection with `H3_EXCESSIVE_LOAD`; it is a local resource policy, not QMux
/// stream backpressure.
#[must_use = "construction returns a driver which must be polled"]
pub fn from_qmux<S, C>(
    connection: QmuxConnection<S, C>,
    pending_accept_limit: usize,
) -> (Connection<S, C>, Driver<S, C>)
where
    S: AsyncByteStream,
    C: Clock,
{
    let core = Arc::new(Mutex::new(Core::new(connection, pending_accept_limit)));
    let lower_wake = Arc::new(LowerWake::default());
    let shared = Shared { core, lower_wake };
    let connection = Connection::new(shared.clone(), 0);
    let driver = Driver::new(shared);
    (connection, driver)
}
