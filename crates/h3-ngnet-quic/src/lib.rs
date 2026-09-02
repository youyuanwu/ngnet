//! Hyperium H3 transport traits over an established ngnet QUIC connection.
//!
//! The caller establishes a connection through [`ngnet_quic`]'s endpoint layer, detaches it,
//! and passes it to [`from_detached`]. What comes back is a [`Connection`] that hyperium's
//! `h3` accepts as its transport, for either role.
//!
//! ```ignore
//! let (endpoint, driver) = EndpointBuilder::new(socket, clock, backend).build_detachable()?;
//! tokio::spawn(driver);
//! let detached = endpoint.connect_detached(remote, Some("example.com")).await?;
//! let (h3, send_request) = h3::client::new(h3_ngnet_quic::from_detached(detached)).await?;
//! ```
//!
//! # What this crate does not own
//!
//! No endpoint, socket, listener, TLS configuration, runtime, executor, task or timer. It
//! never spawns. Connection establishment stays with the caller, as does keeping the
//! endpoint's own driver polled — this crate consumes a connection that is already up.
//!
//! One consequence is worth stating plainly, because it is easy to trip over. A detached
//! connection is driven by nothing but its owner. The handshake completes on the client
//! before its final flight has necessarily left, so a caller that establishes a client
//! connection and then waits for the *server* to accept, without ever polling anything, will
//! wait forever. Callers do not normally notice, because handing the connection straight to
//! hyperium is what polls it; but a test or a harness that accepts before it drives has to
//! interleave the two.
//!
//! # Driving, and why there is no driver future
//!
//! The transport is driven from inside the trait methods hyperium already calls, exactly as
//! the native `ngnet-quic-h3` stack drives it from inside its own. There is deliberately no
//! `Driver` future to poll:
//!
//! * it keeps the public surface to one constructor;
//! * it keeps the number of spawned tasks equal to the native stack's, which matters because
//!   the two are benchmarked against each other and a task the other side does not have is a
//!   difference that has nothing to do with HTTP/3.
//!
//! The one thing a driver future would have provided is a *stable* wake target for the
//! connection's expiry timer. That is provided directly instead: the core owns a waker built
//! with [`std::task::Wake`] and the expiry sleep is polled only under it, so loss recovery and
//! the idle timeout still fire during a quiet period in which no request task is alive. See
//! the `core` module documentation for the full argument.
//!
//! # Buffers
//!
//! ngtcp2 keeps a pointer to accepted stream data until it is acknowledged, so that data must
//! not move. This crate does not have to arrange that: `ngnet-quic`'s vectored stream write
//! stages its own bounded copy and retains that, so bytes offered here are free the moment the
//! call returns. What this crate does own is the ordinary obligation that follows from partial
//! acceptance — hold the unaccepted remainder of a hyperium `WriteBuf` and offer it again from
//! the right position, exactly once. See the `stream` module documentation.
//!
//! # Bounds
//!
//! Every loop is bounded, so no single poll can monopolise the executor: at most 64 datagrams
//! produced per pass, at most 64 write attempts per send, and at most two timer turns per pump.
#![deny(missing_docs, unsafe_code)]

mod connection;
mod core;
mod error;
mod pump;
mod stream;

use std::sync::{Arc, Mutex};

use ngnet_quic::Session;
use ngnet_quic::endpoint::DetachedConnection;

pub use connection::{Connection, OpenStreams};
pub use error::Error;
pub use stream::{BidiStream, RecvStream, SendStream};

/// Adapts an established, detached ngnet QUIC connection for hyperium H3.
///
/// Works for either role: the connection already knows whether it is the client or the
/// server, and peer-opened streams are told apart from local ones by their identifier.
///
/// The connection must already have completed its handshake — that is what
/// [`Endpoint::connect_detached`] and [`Endpoint::accept_detached`] return. The endpoint's own
/// driver must remain polled for the lifetime of the returned [`Connection`], because it still
/// owns the socket and routes this connection's datagrams.
///
/// [`Endpoint::connect_detached`]: ngnet_quic::endpoint::Endpoint::connect_detached
/// [`Endpoint::accept_detached`]: ngnet_quic::endpoint::Endpoint::accept_detached
pub fn from_detached<S: Session>(detached: DetachedConnection<S>) -> Connection<S> {
    let wakers = Arc::new(core::Wakers::default());
    let core = Arc::new(Mutex::new(core::Core::new(detached, &wakers)));
    Connection::new(core, wakers)
}
