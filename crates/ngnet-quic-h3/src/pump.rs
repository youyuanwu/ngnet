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

#[cfg(feature = "diagnostics")]
fn close_reason(close: &ngnet_quic::CloseError) -> &'static str {
    match close.reason() {
        ngnet_quic::CloseReason::Transport(_) => "peer-transport-close",
        ngnet_quic::CloseReason::Application(_) => "peer-application-close",
        ngnet_quic::CloseReason::VersionNegotiation => "version-negotiation",
        ngnet_quic::CloseReason::IdleTimeout => "idle-timeout",
        ngnet_quic::CloseReason::Dropped => "dropped",
        ngnet_quic::CloseReason::Retry => "retry",
        _ => "peer-close-other",
    }
}

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
    #[cfg(feature = "diagnostics")]
    let role = detached.conn.role();
    #[cfg(feature = "diagnostics")]
    let connection_id = detached.conn.diagnostic_id();
    #[cfg(feature = "diagnostics")]
    if detached.register(cx.waker()) {
        ngnet_quic::diagnostics::record_wake_registration(connection_id, role);
    }
    #[cfg(not(feature = "diagnostics"))]
    let _ = detached.register(cx.waker());
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
                let close = detached.conn.close_error();
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_terminal(connection_id, role, close_reason(&close));
                shared.record_connection_closed(close);
                // Nothing further is read into a connection that has ended. Continuing
                // would re-enter the same branch for every datagram still queued and record
                // the close again for each.
                break;
            }
            Err(err) => {
                state.closed = true;
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_terminal(
                    connection_id,
                    role,
                    "read-transport-error",
                );
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
    if let Some(deadline) = detached.conn.expiry().filter(|at| *at <= now) {
        #[cfg(not(feature = "diagnostics"))]
        let _ = deadline;
        #[cfg(feature = "diagnostics")]
        ngnet_quic::diagnostics::record_timer_fire(
            connection_id,
            role,
            now.as_nanos(),
            deadline.as_nanos(),
        );
        match detached.conn.handle_expiry(now) {
            Ok(ngnet_quic::ExpiryOutcome::Handled) => {}
            Ok(ngnet_quic::ExpiryOutcome::IdleClose) => {
                // An idle timeout is how a connection to a peer that stopped answering ends.
                // Reported as the connection closing, because otherwise it is silence.
                state.closed = true;
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_terminal(connection_id, role, "idle-timeout");
                shared.record_connection_closed(detached.conn.close_error());
                return Ok(());
            }
            Ok(ngnet_quic::ExpiryOutcome::Terminal) => {
                state.closed = true;
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_terminal(connection_id, role, "expiry-terminal");
                shared.record_connection_closed(detached.conn.close_error());
                return Ok(());
            }
            Err(err) => {
                state.closed = true;
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_terminal(
                    connection_id,
                    role,
                    "expiry-transport-error",
                );
                shared.record_connection_closed_bare();
                return Err(Error::transport(err));
            }
        }
    }

    // Then send what is owed. Acknowledgements and probes come from here, not from the
    // stream-writing path, so a connection with nothing to say still says it.
    produce(detached, state, Some(cx.waker()))?;
    Ok(())
}

/// Produces datagrams the connection wants to send, subject to the queue having room.
pub(crate) fn produce<S: Session>(
    detached: &mut DetachedConnection<S>,
    state: &mut State,
    capacity_waker: Option<&core::task::Waker>,
) -> Result<()> {
    #[cfg(feature = "diagnostics")]
    if state.capacity_parked
        && capacity_waker.is_some_and(|waker| detached.poll_outbound_capacity(waker))
    {
        ngnet_quic::diagnostics::record_retry(detached.conn.diagnostic_id(), detached.conn.role());
        state.capacity_parked = false;
    }

    // Bounded so a connection that always has something to say cannot keep this pass from
    // returning.
    for _ in 0..64 {
        let ready = match capacity_waker {
            Some(waker) => detached.poll_outbound_capacity(waker),
            None => detached.outbound_has_room(),
        };
        if !ready {
            #[cfg(feature = "diagnostics")]
            {
                state.capacity_parked = true;
            }
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
                #[cfg(feature = "diagnostics")]
                ngnet_quic::diagnostics::record_packet(
                    detached.conn.diagnostic_id(),
                    detached.conn.role(),
                    false,
                );
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
    // folds pacing into the same expiry, so arming only after reads can strand a write.
    let mut replaced_due = false;
    if state.sleeping_until != Some(deadline) {
        let now = detached.now();
        // Replacing an already-due sleep with ngtcp2's new (often idle) deadline must
        // preserve the ready edge. Otherwise a stream refused just before pacing elapsed
        // has neither a timer wake nor another event with which to retry.
        let previous_due = state.sleeping_until.filter(|previous| *previous <= now);
        replaced_due = previous_due.is_some();
        #[cfg(feature = "diagnostics")]
        if let Some(previous) = previous_due {
            ngnet_quic::diagnostics::record_timer_ready(
                detached.conn.diagnostic_id(),
                detached.conn.role(),
                now.as_nanos(),
                previous.as_nanos(),
            );
        }
        state.sleeping = Some(detached.sleep_until(deadline));
        state.sleeping_until = Some(deadline);
        #[cfg(feature = "diagnostics")]
        ngnet_quic::diagnostics::record_timer_rearm(
            detached.conn.diagnostic_id(),
            detached.conn.role(),
            now.as_nanos(),
            deadline.as_nanos(),
        );
    }

    let Some(sleep) = state.sleeping.as_mut() else {
        return Poll::Pending;
    };
    match core::pin::Pin::new(sleep).poll(cx) {
        Poll::Ready(()) => {
            #[cfg(feature = "diagnostics")]
            ngnet_quic::diagnostics::record_timer_ready(
                detached.conn.diagnostic_id(),
                detached.conn.role(),
                detached.now().as_nanos(),
                deadline.as_nanos(),
            );
            state.sleeping = None;
            state.sleeping_until = None;
            Poll::Ready(())
        }
        Poll::Pending if replaced_due => Poll::Ready(()),
        Poll::Pending => Poll::Pending,
    }
}

/// The error a caller sees once the connection has ended.
pub(crate) fn ended() -> Error {
    Error::new(ErrorKind::Closed, "the connection has ended")
}
