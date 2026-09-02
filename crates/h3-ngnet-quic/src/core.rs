//! Shared connection state, and the wake plumbing that keeps it live.
//!
//! # Why the wakers are split out of the core
//!
//! Three different things have to be woken, and they do not have the same source.
//!
//! Inbound datagrams are already handled by `ngnet-quic`: [`DetachedConnection::register`]
//! appends to a *list* of wakers and every routed datagram wakes all of them, so registering
//! the polling task each pass is enough and this crate must not reimplement it.
//!
//! Stream-level fan-out is this crate's job. Whichever task happens to pump routes data for
//! streams that other tasks are parked on, so the pump has to wake them.
//!
//! The connection's own expiry timer is the awkward one. It is a single [`Sleep`] future, and
//! the endpoint's timer deliberately does not cover detached connections. If it were re-armed
//! under whichever transient task pumped last, that task could finish and leave the timer
//! bound to a waker nobody polls — during a quiet period with no inbound datagram to rescue
//! it, loss recovery and the idle timeout would simply never fire. So [`Core`] owns a *stable*
//! waker built with [`std::task::Wake`], the sleep is polled only under that waker, and its
//! wake fans out to everyone. That is the guarantee a persistent driver task would otherwise
//! have provided, which is why this crate can do without one.
//!
//! The waker set therefore lives behind its own lock rather than inside [`Core`]'s: the stable
//! waker's `wake` runs the fan-out, and if the set were inside [`Core`] that fan-out would have
//! to re-enter a mutex a pump may already be holding.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Wake, Waker};

use bytes::Bytes;
use h3::quic::WriteBuf;
use ngnet_quic::endpoint::{DetachedConnection, Sleep};
use ngnet_quic::{Directionality, Initiator, Role, Session, StreamId, Timestamp};

use crate::error::{ConnectionTerminal, DirectionTerminal};

/// The largest datagram this crate will produce.
///
/// Capacity, not permission: the connection decides how much of it may actually be used.
pub(crate) const MAX_DATAGRAM: usize = 1500;

/// The most work any one pass will do before returning to the executor.
pub(crate) const WORK_BUDGET: usize = 64;

/// How many read/expire/produce turns one pump pass may take.
///
/// More than one because a fired timer produces work that wants reading and sending, and going
/// round again is cheaper than returning `Pending` and being woken. Bounded because a
/// connection that always has something to say must not keep a pass from returning.
pub(crate) const TIMER_TURNS: usize = 4;

/// Everything parked on this connection.
///
/// Both registries hold *lists*, which is load-bearing rather than defensive. More than one
/// task waits at connection level: the HTTP/3 driver parks in `poll_accept_*` while a request
/// task parks in `poll_open_*`. More than one waits on a single stream too, once a
/// bidirectional stream has been split and its halves sent to different tasks. A single slot
/// lets the second registration displace the first, and the displaced task is then reachable
/// only through `ngnet-quic`'s own inbound waker list — which does not carry the expiry timer.
#[derive(Default)]
struct WakerSet {
    /// The HTTP/3 driver, and anyone else waiting on stream opening or acceptance.
    connection: Vec<Waker>,
    /// Tasks parked in a stream's `poll_data`, `poll_ready` or `poll_finish`.
    streams: HashMap<i64, Vec<Waker>>,
}

/// Adds a waker to a list unless an equivalent one is already there.
fn remember(list: &mut Vec<Waker>, waker: &Waker) {
    if !list.iter().any(|held| held.will_wake(waker)) {
        list.push(waker.clone());
    }
}

/// The wake registry, held separately from [`Core`] so a fan-out never re-enters its lock.
#[derive(Default)]
pub(crate) struct Wakers(Mutex<WakerSet>);

impl Wakers {
    fn set(&self) -> MutexGuard<'_, WakerSet> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn register_connection(&self, waker: &Waker) {
        remember(&mut self.set().connection, waker);
    }

    pub(crate) fn register_stream(&self, stream: i64, waker: &Waker) {
        remember(self.set().streams.entry(stream).or_default(), waker);
    }

    pub(crate) fn forget_stream(&self, stream: i64) {
        self.set().streams.remove(&stream);
    }

    /// Wakes everything.
    ///
    /// Used by the stable timer waker and whenever the connection as a whole changed state.
    /// Waking the task that is currently pumping costs one extra poll, which finds no new
    /// work and parks; it does not spin, because the next pass records no change and so
    /// fans out nothing.
    pub(crate) fn wake_all(&self) {
        let (connection, streams) = {
            let mut set = self.set();
            (
                core::mem::take(&mut set.connection),
                set.streams.drain().collect::<Vec<_>>(),
            )
        };
        for waker in connection {
            waker.wake();
        }
        for (_, wakers) in streams {
            for waker in wakers {
                waker.wake();
            }
        }
    }

    pub(crate) fn wake_connection(&self) {
        let wakers = core::mem::take(&mut self.set().connection);
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn wake_stream(&self, stream: i64) {
        let wakers = self.set().streams.remove(&stream).unwrap_or_default();
        for waker in wakers {
            waker.wake();
        }
    }
}

/// The stable wake target the expiry sleep is polled under.
struct TimerWake(Arc<Wakers>);

impl Wake for TimerWake {
    fn wake(self: Arc<Self>) {
        self.0.wake_all();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake_all();
    }
}

/// One stream's state, from both directions.
///
/// The two directions are kept strictly apart. RESET_STREAM and STOP_SENDING are not two
/// spellings of "the stream ended": per RFC 9000 a peer's RESET_STREAM abandons the peer's
/// *sending* side, which is our *receiving* side, while a peer's STOP_SENDING asks us to
/// abandon our *sending* side. Hyperium draws the same line — its `StreamTerminated` doc
/// (`h3::quic`) assigns a reset to `poll_data` and a stop-sending to the send methods. Sharing
/// one field between them makes an ordinary exchange fail: a server that sends a complete
/// response and then stop-sends the request-body stream would make the client's `poll_data`
/// report an error for a response that arrived intact.
#[derive(Default)]
pub(crate) struct StreamState {
    /// Received but not yet handed to HTTP/3.
    pub(crate) incoming: VecDeque<Bytes>,
    /// The peer finished its sending side cleanly.
    pub(crate) finished: bool,
    /// The peer reset the stream it was sending on: our receiving side ended abnormally.
    pub(crate) recv_terminal: Option<DirectionTerminal>,
    /// The peer asked us to stop sending: our sending side ended abnormally.
    pub(crate) send_terminal_state: Option<DirectionTerminal>,
    /// One hyperium logical send, retained until the transport has taken all of it.
    pub(crate) writing: Option<WriteBuf<Bytes>>,
    /// This side emitted its FIN.
    pub(crate) send_finished: bool,
    /// This side reset the stream, so `Drop` must not reset it again.
    pub(crate) send_reset: bool,
    /// How many live handles refer to this stream.
    ///
    /// A bidirectional stream may be split into halves that are dropped independently and in
    /// either order, so the state cannot be discarded when the first half goes: the survivor
    /// still needs `writing`, `send_finished` and the terminals. Dropping it early would
    /// silently truncate an in-flight body and then reset a stream that had finished cleanly.
    pub(crate) handles: usize,
}

impl StreamState {
    /// Whether the sending side may still make progress.
    pub(crate) fn send_terminal(&self) -> Option<DirectionTerminal> {
        self.send_terminal_state
    }
}

/// The shared connection state.
pub(crate) struct Core<S: Session> {
    /// The transport, which this crate drives itself.
    pub(crate) detached: DetachedConnection<S>,
    /// Which side of the connection this is, so peer-opened streams can be told apart.
    initiator: Initiator,
    pub(crate) streams: HashMap<i64, StreamState>,
    /// Peer-opened bidirectional streams awaiting `poll_accept_bidi`.
    pub(crate) accept_bidi: VecDeque<StreamId>,
    /// Peer-opened unidirectional streams awaiting `poll_accept_recv`.
    pub(crate) accept_uni: VecDeque<StreamId>,
    /// Set once the connection has ended, and never cleared.
    pub(crate) terminal: Option<ConnectionTerminal>,
    /// Whether the endpoint has been told to stop routing here.
    pub(crate) released: bool,
    /// One reusable datagram buffer, so a pass that produces nothing allocates nothing.
    pub(crate) scratch: Vec<u8>,
    /// The armed expiry sleep, and the deadline it was armed for.
    pub(crate) sleeping: Option<Sleep>,
    pub(crate) sleeping_until: Option<Timestamp>,
    /// The stable waker the sleep is polled under. Never a caller's waker.
    timer_waker: Waker,
    /// The peer raised this endpoint's stream allowance, so a refused open may now succeed.
    pub(crate) streams_extended: bool,
}

impl<S: Session> Core<S> {
    pub(crate) fn new(detached: DetachedConnection<S>, wakers: &Arc<Wakers>) -> Self {
        let initiator = match detached.conn.role() {
            Role::Client => Initiator::Client,
            Role::Server => Initiator::Server,
        };
        Self {
            detached,
            initiator,
            streams: HashMap::new(),
            accept_bidi: VecDeque::new(),
            accept_uni: VecDeque::new(),
            terminal: None,
            released: false,
            scratch: Vec::new(),
            sleeping: None,
            sleeping_until: None,
            timer_waker: Waker::from(Arc::new(TimerWake(Arc::clone(wakers)))),
            streams_extended: false,
        }
    }

    /// The stable waker the expiry sleep is polled under.
    pub(crate) fn timer_waker(&self) -> Waker {
        self.timer_waker.clone()
    }

    /// Whether this endpoint opened the given stream.
    pub(crate) fn opened_locally(&self, stream: StreamId) -> bool {
        stream.initiator() == self.initiator
    }

    pub(crate) fn state(&mut self, stream: StreamId) -> &mut StreamState {
        self.streams.entry(stream.get()).or_default()
    }

    /// Records a new handle onto a stream.
    pub(crate) fn retain_handle(&mut self, stream: StreamId) {
        self.state(stream).handles += 1;
    }

    /// Drops a handle, discarding the state only once the last one is gone.
    pub(crate) fn release_handle(&mut self, stream: StreamId) -> bool {
        let Some(state) = self.streams.get_mut(&stream.get()) else {
            return true;
        };
        state.handles = state.handles.saturating_sub(1);
        if state.handles == 0 {
            self.streams.remove(&stream.get());
            return true;
        }
        false
    }

    /// Records the end of the connection, keeping the first reason.
    ///
    /// First rather than last because the first is the one that explains the others: a
    /// close observed while draining would otherwise overwrite the application code the
    /// peer actually sent.
    pub(crate) fn fail(&mut self, terminal: ConnectionTerminal) {
        if self.terminal.is_none() {
            self.terminal = Some(terminal);
            // Every retained send is now undeliverable.
            for state in self.streams.values_mut() {
                state.writing = None;
            }
        }
    }

    /// Queues a peer-opened stream for the matching accept queue.
    pub(crate) fn record_opened(&mut self, stream: StreamId) {
        if self.opened_locally(stream) {
            return;
        }
        self.streams.entry(stream.get()).or_default();
        match stream.directionality() {
            Directionality::Bidirectional => self.accept_bidi.push_back(stream),
            Directionality::Unidirectional => self.accept_uni.push_back(stream),
        }
    }
}
