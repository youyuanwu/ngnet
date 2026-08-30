//! The one operation that drives the connection.
//!
//! Called from every entry point, for the reason set out in the crate documentation: the
//! HTTP/3 driver's first action is to open streams, and it reaches nothing else until it
//! has them. If datagrams only moved in `poll_transmit`, the first flight would never be
//! sent and the connection would deadlock before it began.

use core::task::{Context, Poll};

use ngnet_quic::endpoint::DetachedConnection;
use ngnet_quic::{Session, WriteOutcome};

use crate::connection::{Shared, State};
use crate::error::{Error, ErrorKind, Result};

/// The largest datagram this crate will produce.
///
/// Capacity, not permission: the connection decides how much of it may actually be used.
pub(crate) const MAX_DATAGRAM: usize = 1500;

/// Drives the connection one pass.
///
/// In order: take in whatever the endpoint routed here, fire the expiry timer if it is due,
/// then produce whatever the connection now owes — handshake flights, acknowledgements, loss
/// probes. Stream data is not written here; that is `poll_transmit`'s job, and it calls this
/// first.
///
/// Registers `cx` so the connection is polled again when a datagram arrives.
pub(crate) fn pump<S: Session>(
    detached: &mut DetachedConnection<S>,
    shared: &Shared,
    state: &mut State,
    cx: &mut Context<'_>,
) -> Result<()> {
    detached.register(cx.waker());

    // Read first. The lock is never held across a call into the connection: ngtcp2 invokes
    // this crate's handlers synchronously from inside `read_pkt`, and those handlers take
    // the same lock to record what they saw.
    let now = detached.now();
    let mut read_any = false;
    while let Some(datagram) = detached.next_inbound() {
        read_any = true;
        match detached.conn.read_pkt(&datagram, now) {
            Ok(
                ngnet_quic::ReadOutcome::Processed
                | ngnet_quic::ReadOutcome::SendRetry
                | ngnet_quic::ReadOutcome::DropSilently,
            ) => {}
            Ok(ngnet_quic::ReadOutcome::Draining | ngnet_quic::ReadOutcome::Closing) => {
                state.closed = true;
                shared.record_connection_closed(detached.conn.close_error());
                // Nothing further is read into a connection that has ended. Continuing
                // would re-enter the same branch for every datagram still queued and record
                // the close again for each.
                break;
            }
            Err(err) => {
                state.closed = true;
                shared.record_connection_closed_bare();
                return Err(Error::transport(err));
            }
        }
    }
    let _ = read_any;

    if state.closed {
        return Ok(());
    }

    // Then the timer. Its deadline already folds in the pacing deadline, so this is also
    // what releases a connection that is waiting to send rather than waiting to hear.
    if detached.conn.expiry().is_some_and(|at| at <= now) {
        match detached.conn.handle_expiry(now) {
            Ok(ngnet_quic::ExpiryOutcome::Handled) => {}
            Ok(ngnet_quic::ExpiryOutcome::IdleClose | ngnet_quic::ExpiryOutcome::Terminal) => {
                // An idle timeout is how a connection to a peer that stopped answering ends.
                // Reported as the connection closing, because otherwise it is silence.
                state.closed = true;
                shared.record_connection_closed(detached.conn.close_error());
                return Ok(());
            }
            Err(err) => {
                state.closed = true;
                shared.record_connection_closed_bare();
                return Err(Error::transport(err));
            }
        }
    }

    // Then send what is owed. Acknowledgements and probes come from here, not from the
    // stream-writing path, so a connection with nothing to say still says it.
    produce(detached, state)?;
    if !detached.outbound_has_room() {
        detached.register_outbound_capacity(cx.waker());
    }
    Ok(())
}

/// Produces datagrams the connection wants to send, subject to the queue having room.
pub(crate) fn produce<S: Session>(
    detached: &mut DetachedConnection<S>,
    state: &mut State,
) -> Result<()> {
    // Bounded so a connection that always has something to say cannot keep this pass from
    // returning.
    for _ in 0..64 {
        if !detached.outbound_has_room() {
            break;
        }
        let now = detached.now();
        // Write directly into the buffer that will be handed over. `scratch` carries one
        // reusable buffer between passes; a datagram consumes it and the next iteration
        // allocates its replacement, so a pass producing datagrams allocates exactly one
        // owned buffer each and a pass producing none allocates nothing.
        let mut datagram = core::mem::take(&mut state.scratch);
        datagram.resize(MAX_DATAGRAM, 0);
        match detached.conn.write_pkt(&mut datagram, now) {
            Ok(WriteOutcome::Datagram { len }) => {
                datagram.truncate(len);
                detached.send(datagram);
            }
            Ok(WriteOutcome::Blocked | WriteOutcome::Idle) => {
                // Nothing was produced, so this buffer is untouched storage: keep it for the
                // next pass rather than dropping it and reallocating one.
                datagram.clear();
                state.scratch = datagram;
                break;
            }
            Err(err) => {
                state.closed = true;
                return Err(Error::transport(err));
            }
        }
    }
    Ok(())
}

/// Arms nothing, but reports when the connection next wants attention.
///
/// The HTTP/3 driver parks on this crate's waker, and nothing else will wake it when a
/// timer expires — the endpoint's timer covers only connections the endpoint drives.
pub(crate) fn poll_timer<S: Session>(
    detached: &DetachedConnection<S>,
    state: &mut State,
    cx: &mut Context<'_>,
) -> Poll<()> {
    let Some(deadline) = detached.conn.expiry() else {
        state.sleeping = None;
        state.sleeping_until = None;
        return Poll::Pending;
    };

    // Rearm whenever the deadline moves, including after a pass that only wrote: ngtcp2
    // folds its pacing deadline into the same expiry, so a connection that rearmed only
    // after reading would send one datagram and then wait for the peer to speak.
    if state.sleeping_until != Some(deadline) {
        state.sleeping = Some(detached.sleep_until(deadline));
        state.sleeping_until = Some(deadline);
    }

    let Some(sleep) = state.sleeping.as_mut() else {
        return Poll::Pending;
    };
    match core::pin::Pin::new(sleep).poll(cx) {
        Poll::Ready(()) => {
            state.sleeping = None;
            state.sleeping_until = None;
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    }
}

/// The error a caller sees once the connection has ended.
pub(crate) fn ended() -> Error {
    Error::new(ErrorKind::Closed, "the connection has ended")
}
