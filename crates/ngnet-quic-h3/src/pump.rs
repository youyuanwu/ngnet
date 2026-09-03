//! The one operation that drives the connection.
//!
//! Called from every entry point, for the reason set out in the crate documentation: the
//! HTTP/3 driver's first action is to open streams, and it reaches nothing else until it
//! has them. If datagrams only moved in `poll_transmit`, the first flight would never be
//! sent and the connection would deadlock before it began.

use core::task::{Context, Poll};

use ngnet_quic::endpoint::DetachedConnection;
use ngnet_quic::{Session, WriteOutcome};

use crate::connection::{Shared, Sleep, State};
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
/// Runtime timers may coalesce deadlines shorter than one scheduler tick.
const IMMINENT_EXPIRY_NANOS: u64 = 100_000;
/// Caps fallback repolls for one unchanged deadline, preventing an unbounded retry loop.
const MAX_IMMINENT_WAKES: u8 = 64;

struct TimerPoll {
    poll: Poll<()>,
    rearmed: bool,
    kicked: bool,
    ready: bool,
}

/// Polls the adapter-owned expiry state, including its bounded imminent fallback.
fn poll_timer_state(
    state: &mut State,
    now: ngnet_quic::Timestamp,
    deadline: Option<ngnet_quic::Timestamp>,
    cx: &mut Context<'_>,
    sleep_until: impl FnOnce(ngnet_quic::Timestamp) -> Sleep,
) -> TimerPoll {
    let Some(deadline) = deadline else {
        state.sleeping = None;
        state.sleeping_until = None;
        state.imminent_wake_until = None;
        state.imminent_wake_count = 0;
        return TimerPoll {
            poll: Poll::Pending,
            rearmed: false,
            kicked: false,
            ready: false,
        };
    };

    let rearmed = state.sleeping_until != Some(deadline);
    if rearmed {
        state.sleeping = Some(sleep_until(deadline));
        state.sleeping_until = Some(deadline);
        state.imminent_wake_until = None;
        state.imminent_wake_count = 0;
    }

    let remaining = deadline.as_nanos().saturating_sub(now.as_nanos());
    let kicked =
        remaining <= IMMINENT_EXPIRY_NANOS && state.imminent_wake_count < MAX_IMMINENT_WAKES;
    if kicked {
        state.imminent_wake_until = Some(deadline);
        state.imminent_wake_count += 1;
        cx.waker().wake_by_ref();
    }

    let Some(sleep) = state.sleeping.as_mut() else {
        return TimerPoll {
            poll: Poll::Pending,
            rearmed,
            kicked,
            ready: false,
        };
    };
    match core::pin::Pin::new(sleep).poll(cx) {
        Poll::Ready(()) => {
            state.sleeping = None;
            state.sleeping_until = None;
            state.imminent_wake_until = None;
            state.imminent_wake_count = 0;
            TimerPoll {
                poll: Poll::Ready(()),
                rearmed,
                kicked,
                ready: true,
            }
        }
        Poll::Pending => TimerPoll {
            poll: Poll::Pending,
            rearmed,
            kicked,
            ready: false,
        },
    }
}

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
    if detached.conn.expiry().is_some_and(|at| at <= now) {
        #[cfg(feature = "diagnostics")]
        ngnet_quic::diagnostics::record_timer_fire(connection_id, role);
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
    let now = detached.now();
    let result = poll_timer_state(state, now, detached.conn.expiry(), cx, |deadline| {
        detached.sleep_until(deadline)
    });
    #[cfg(feature = "diagnostics")]
    if result.rearmed {
        ngnet_quic::diagnostics::record_timer_rearm(
            detached.conn.diagnostic_id(),
            detached.conn.role(),
        );
    }
    #[cfg(feature = "diagnostics")]
    if result.kicked {
        #[cfg(feature = "diagnostics")]
        ngnet_quic::diagnostics::record_timer_kick(
            detached.conn.diagnostic_id(),
            detached.conn.role(),
        );
        // Some runtimes can coalesce a sub-tick sleep with the task currently being polled.
        // Preserve bounded scheduling edges until the deadline passes; the per-deadline cap
        // prevents this fallback from becoming an unconditional retry loop.
    }
    #[cfg(feature = "diagnostics")]
    if result.ready {
        ngnet_quic::diagnostics::record_timer_ready(
            detached.conn.diagnostic_id(),
            detached.conn.role(),
        );
    }
    #[cfg(not(feature = "diagnostics"))]
    let _ = (result.rearmed, result.kicked, result.ready);
    result.poll
}

/// The error a caller sees once the connection has ended.
pub(crate) fn ended() -> Error {
    Error::new(ErrorKind::Closed, "the connection has ended")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    use ngnet_quic::Timestamp;

    use super::{IMMINENT_EXPIRY_NANOS, MAX_IMMINENT_WAKES, State, poll_timer_state};

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll(
        state: &mut State,
        now: Timestamp,
        deadline: Option<Timestamp>,
        cx: &mut Context<'_>,
    ) -> super::TimerPoll {
        poll_timer_state(state, now, deadline, cx, |_| {
            Box::pin(core::future::pending())
        })
    }

    fn state() -> State {
        State {
            closed: false,
            reported_closed: false,
            emitted_since_pending: false,
            sleeping: None,
            sleeping_until: None,
            imminent_wake_until: None,
            imminent_wake_count: 0,
            #[cfg(feature = "diagnostics")]
            capacity_parked: false,
            #[cfg(feature = "diagnostics")]
            idle_parked: false,
            limit_wakers: Vec::new(),
            opened_bidi: std::collections::VecDeque::new(),
            opened_uni: std::collections::VecDeque::new(),
            scratch: Vec::new(),
        }
    }

    #[test]
    fn imminent_timer_fallback_is_thresholded_and_bounded_per_deadline() {
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let now = Timestamp::from_nanos(1_000_000).unwrap();
        let threshold = Timestamp::from_nanos(1_000_000 + IMMINENT_EXPIRY_NANOS).unwrap();
        let outside = Timestamp::from_nanos(1_000_001 + IMMINENT_EXPIRY_NANOS).unwrap();
        let mut state = state();

        let outside_result = poll(&mut state, now, Some(outside), &mut cx);
        assert!(outside_result.rearmed);
        assert!(!outside_result.kicked);
        assert_eq!(count.0.load(Ordering::Relaxed), 0);

        for _ in 0..=MAX_IMMINENT_WAKES {
            let _ = poll(&mut state, now, Some(threshold), &mut cx);
        }
        assert_eq!(
            count.0.load(Ordering::Relaxed),
            usize::from(MAX_IMMINENT_WAKES),
            "the exact threshold is included and the per-deadline budget is bounded"
        );

        let next = Timestamp::from_nanos(threshold.as_nanos() + 1).unwrap();
        let next_now = Timestamp::from_nanos(next.as_nanos() - 15).unwrap();
        let next_result = poll(&mut state, next_now, Some(next), &mut cx);
        assert!(next_result.rearmed);
        assert!(next_result.kicked);
        assert_eq!(
            count.0.load(Ordering::Relaxed),
            usize::from(MAX_IMMINENT_WAKES) + 1,
            "a new imminent deadline gets a fresh bounded wake budget"
        );
    }
}
