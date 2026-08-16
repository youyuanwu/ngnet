//! Time, which this crate still refuses to read for itself.
//!
//! # One clock, not two
//!
//! The state machine takes a [`Timestamp`] on every call that can advance connection state,
//! documented as an opaque nanosecond reading in whatever monotonic timescale the caller
//! chose. This layer has to supply one on the caller's behalf, and the temptation is to treat
//! that as a second facility -- a clock for the layer, alongside whichever clock the caller
//! passes elsewhere -- which would then need reconciling every time the two disagreed.
//!
//! They are one facility. [`Clock::now`] returns a `Timestamp` in the same timescale the state
//! machine is given, and the layer passes it straight through. There is no second time source
//! and so nothing to reconcile: a value the caller's clock produced is the value dwnx records,
//! which is what makes [`crate::Conn::timestamp`] mean something to the caller.
//!
//! # Why there is no `sleep_until`, when the QUIC equivalent has one
//!
//! `ngnet-quic`'s clock offers a wait as well as a reading, because ngtcp2 has an expiry API:
//! a QUIC connection genuinely has work to do at a future instant -- loss recovery, pacing,
//! the idle timeout -- and a driver that stops rearming its timer sends one datagram and goes
//! quiet.
//!
//! dwnx has no such API. There is no `dwnx_conn_get_expiry`, no `dwnx_conn_handle_expiry`, and
//! nothing that becomes true merely because time passed: QMux delegates loss recovery and
//! congestion control to the byte stream underneath it, so the deadlines a QUIC driver exists
//! to service do not exist here. `max_idle_timeout` is the near miss. dwnx validates it and
//! advertises it in the transport parameters, and then never acts on it -- there is no code
//! path that fails a connection for being idle, in either direction.
//!
//! A `sleep_until` here would therefore be a facility with exactly one caller: an idle timeout
//! this layer had implemented itself, on top of a parameter the state machine does not
//! enforce, and which the peer's own idea of the timeout need not agree with. That is a
//! protocol decision taken in the wrong place. It is left out, and a caller who wants a
//! deadline applies one where deadlines belong -- around the future they are awaiting.
//!
//! The consequence, stated plainly because it is the thing to get wrong: **a QMux connection
//! that goes silent stays open**. Nothing in this crate will time it out. A caller who needs
//! liveness detection needs it from the substrate -- TCP keepalives -- or from their own
//! timeout around the operation.

use crate::time::Timestamp;

/// A monotonic clock.
///
/// # Contract
///
/// [`Clock::now`] must be monotonic: it may return the same value twice but must never go
/// backwards. dwnx computes elapsed time by subtracting one unsigned reading from another, so
/// a clock that went backwards produces not a negative interval but an enormous positive one.
///
/// The origin is arbitrary and never compared against wall-clock time, so a process-lifetime
/// monotonic source -- `Instant`, `CLOCK_MONOTONIC`, a runtime's cached tick -- is exactly
/// right and a wall clock is not: a wall clock is what goes backwards.
///
/// There is no wait. See the module documentation for why, and for what a caller who wanted
/// one should do instead.
pub trait Clock {
    /// The current time, in the caller's own monotonic timescale.
    fn now(&self) -> Timestamp;
}
