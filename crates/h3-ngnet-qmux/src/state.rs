use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use h3::error::Code;
use ngnet_qmux::io::{AsyncByteStream, Clock, Connection as QmuxConnection, Event, StreamOpen};
use ngnet_qmux::{
    CloseReason, Directionality, Initiator, Role, Shutdown, StreamId, StreamLimitKind,
};

use crate::error::{ConnectionTerminal, DirectionTerminal, Error as AdapterError, close_reason};

pub(crate) const ROUTE_BUDGET: usize = 64;
pub(crate) const ABANDONED: u64 = Code::H3_REQUEST_CANCELLED.value();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcceptKind {
    Uni,
    Bidi,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpenKind {
    Uni,
    Bidi,
}

#[derive(Debug)]
pub(crate) struct Received {
    pub(crate) data: Bytes,
    pub(crate) fin: bool,
}

#[derive(Debug, Default)]
pub(crate) struct StreamState {
    pub(crate) pending_accept: bool,
    pub(crate) lower_closed: bool,
    pub(crate) send_handle: bool,
    pub(crate) recv_handle: bool,
    pub(crate) recv: VecDeque<Received>,
    pub(crate) recv_terminal: Option<DirectionTerminal>,
    pub(crate) send_terminal: Option<DirectionTerminal>,
    pub(crate) recv_shutdown_sent: bool,
    pub(crate) send_shutdown_sent: bool,
    pub(crate) recv_waiter: Option<Waker>,
    pub(crate) send_waiter: Option<Waker>,
    pub(crate) finish_waiter: Option<Waker>,
    pub(crate) send_waiting_output: bool,
    pub(crate) finish_waiting_output: bool,
}

#[derive(Debug, Default)]
struct OpenerWaiters {
    uni: Option<Waker>,
    bidi: Option<Waker>,
}

#[derive(Debug, Default)]
pub(crate) struct Effects {
    pub(crate) wakes: Vec<Waker>,
    pub(crate) discard: Vec<StreamId>,
    pub(crate) discard_all: bool,
    pub(crate) continuation: bool,
}

impl Effects {
    pub(crate) fn merge(&mut self, mut other: Self) {
        self.wakes.append(&mut other.wakes);
        self.discard.append(&mut other.discard);
        self.discard_all |= other.discard_all;
        self.continuation |= other.continuation;
    }
}

/// One stable lower-I/O waker, independent of all hyperium operation wakers.
#[derive(Debug, Default)]
pub(crate) struct LowerWake {
    ready: AtomicBool,
    defer: AtomicUsize,
    driver: Mutex<Option<Waker>>,
}

impl LowerWake {
    pub(crate) fn register_driver(&self, waker: &Waker) -> Option<Waker> {
        let mut held = self.driver.lock().unwrap_or_else(PoisonError::into_inner);
        match held.as_ref() {
            Some(current) if current.will_wake(waker) => None,
            _ => held.replace(waker.clone()),
        }
    }

    pub(crate) fn take_ready(&self) -> bool {
        self.ready.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn request_driver(&self) {
        self.wake_driver();
    }

    pub(crate) fn begin_defer(&self) {
        self.defer.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn end_defer(&self) {
        let previous = self.defer.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "lower-wake deferral underflow");
        if previous == 1 && self.ready.load(Ordering::Acquire) {
            self.wake_driver();
        }
    }

    fn lower_ready(&self) {
        self.ready.store(true, Ordering::Release);
        if self.defer.load(Ordering::Acquire) == 0 {
            self.wake_driver();
        }
    }

    fn wake_driver(&self) {
        let wake = self
            .driver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl Wake for LowerWake {
    fn wake(self: Arc<Self>) {
        self.lower_ready();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.lower_ready();
    }
}

pub(crate) struct Core<S: AsyncByteStream, C: Clock> {
    pub(crate) lower: QmuxConnection<S, C>,
    pub(crate) role: Role,
    pub(crate) pending_limit: usize,
    pub(crate) streams: BTreeMap<StreamId, StreamState>,
    pending_uni: VecDeque<StreamId>,
    pending_bidi: VecDeque<StreamId>,
    accept_uni: Option<Waker>,
    accept_bidi: Option<Waker>,
    openers: BTreeMap<u64, OpenerWaiters>,
    next_opener: u64,
    pub(crate) terminal: Option<ConnectionTerminal>,
    pub(crate) close: Option<CloseReason>,
    pub(crate) driver_complete: bool,
    pub(crate) driver_error: Option<AdapterError>,
    #[cfg(debug_assertions)]
    pub(crate) routed_events: u64,
}

impl<S: AsyncByteStream, C: Clock> Core<S, C> {
    pub(crate) fn new(lower: QmuxConnection<S, C>, pending_limit: usize) -> Self {
        let role = lower.role();
        let mut openers = BTreeMap::new();
        openers.insert(0, OpenerWaiters::default());
        Self {
            lower,
            role,
            pending_limit,
            streams: BTreeMap::new(),
            pending_uni: VecDeque::new(),
            pending_bidi: VecDeque::new(),
            accept_uni: None,
            accept_bidi: None,
            openers,
            next_opener: 1,
            terminal: None,
            close: None,
            driver_complete: false,
            driver_error: None,
            #[cfg(debug_assertions)]
            routed_events: 0,
        }
    }

    pub(crate) fn allocate_opener(&mut self) -> u64 {
        let id = self.next_opener;
        self.next_opener = self.next_opener.wrapping_add(1);
        self.openers.insert(id, OpenerWaiters::default());
        id
    }

    pub(crate) fn remove_opener(&mut self, id: u64) {
        self.openers.remove(&id);
    }

    pub(crate) fn drive_turn(&mut self, lower_wake: &Arc<LowerWake>) -> Effects {
        let mut effects = Effects::default();
        if self.terminal.is_some() {
            #[cfg(feature = "diagnostics")]
            crate::diagnostics::pump(false);
            return effects;
        }

        let credit_before = self.lower.send_credit();
        let queued_before = self.lower.queued_output();
        let mut routed = 0usize;
        while routed < ROUTE_BUDGET {
            let Some(event) = self.lower.try_next_event() else {
                break;
            };
            routed += 1;
            effects.merge(self.route(event));
            if self.terminal.is_some() {
                #[cfg(feature = "diagnostics")]
                crate::diagnostics::pump(true);
                return effects;
            }
        }

        if routed < ROUTE_BUDGET {
            let waker = Waker::from(Arc::clone(lower_wake));
            let mut cx = Context::from_waker(&waker);
            match self.lower.poll_next_event_bounded(&mut cx) {
                Poll::Ready(Ok(event)) => {
                    routed += 1;
                    effects.merge(self.route(event));
                }
                Poll::Ready(Err(error)) => {
                    effects.merge(self.fail(ConnectionTerminal::from_lower(&error)));
                }
                Poll::Pending => {}
            }

            while self.terminal.is_none() && routed < ROUTE_BUDGET {
                let Some(event) = self.lower.try_next_event() else {
                    break;
                };
                routed += 1;
                effects.merge(self.route(event));
            }
        }

        if self.terminal.is_none() && self.lower.send_credit() > credit_before {
            self.wake_all_senders(&mut effects);
        }
        if self.terminal.is_none() && self.lower.queued_output() < queued_before {
            self.wake_output_senders(&mut effects);
        }
        if routed == ROUTE_BUDGET {
            effects.continuation = true;
        }
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::pump(routed != 0);
        effects
    }

    fn route(&mut self, event: Event) -> Effects {
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::route(!matches!(
            &event,
            Event::StreamLimit { .. } | Event::PeerTransportParams(_)
        ));
        #[cfg(debug_assertions)]
        {
            self.routed_events = self.routed_events.saturating_add(1);
        }
        let mut effects = Effects::default();
        match event {
            Event::StreamData {
                stream_id,
                data,
                fin,
                ..
            } => {
                if !self.receive_direction_valid(stream_id) {
                    return self.fail(ConnectionTerminal::Internal(
                        "QMux delivered data on an impossible receive direction".into(),
                    ));
                }
                if !self.streams.contains_key(&stream_id)
                    && self.discover_peer(stream_id, &mut effects).is_err()
                {
                    return effects;
                }
                let state = self.streams.get_mut(&stream_id).expect("discovered stream");
                if state.recv_shutdown_sent {
                    if !data.is_empty()
                        && let Err(error) = self.lower.extend_connection_credit(data.len() as u64)
                    {
                        return self.fail(ConnectionTerminal::from_lower(&error));
                    }
                    #[cfg(feature = "diagnostics")]
                    if !data.is_empty() {
                        crate::diagnostics::connection_credit();
                    }
                    return effects;
                }
                if !data.is_empty() {
                    state.recv.push_back(Received {
                        data: Bytes::from(data),
                        fin,
                    });
                } else if fin && state.recv_terminal.is_none() {
                    state.recv_terminal = Some(DirectionTerminal::Finished);
                } else {
                    return effects;
                }
                take_waiter(&mut state.recv_waiter, &mut effects);
                #[cfg(feature = "diagnostics")]
                crate::diagnostics::receive_gauge(self.retained_receive_bytes());
            }
            Event::StreamOpened { stream_id } => {
                if stream_id.initiator() == self.local_initiator() {
                    return self.fail(ConnectionTerminal::Internal(
                        "QMux reported a locally initiated stream as peer-opened".into(),
                    ));
                }
                let _ = self.discover_peer(stream_id, &mut effects);
            }
            Event::StreamClosed {
                stream_id,
                rx_app_error_code,
                tx_app_error_code,
            } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state.lower_closed = true;
                    if state.recv_terminal.is_none() {
                        state.recv_terminal = Some(match rx_app_error_code {
                            Some(code) => DirectionTerminal::Reset(code),
                            None => DirectionTerminal::Finished,
                        });
                    }
                    if state.send_terminal.is_none() {
                        state.send_terminal = Some(match tx_app_error_code {
                            Some(code) => DirectionTerminal::Reset(code),
                            None => DirectionTerminal::Finished,
                        });
                    }
                    take_waiter(&mut state.recv_waiter, &mut effects);
                    take_waiter(&mut state.send_waiter, &mut effects);
                    take_waiter(&mut state.finish_waiter, &mut effects);
                }
                self.cleanup(stream_id);
            }
            Event::StreamReset {
                stream_id,
                app_error_code,
                ..
            } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state
                        .recv_terminal
                        .get_or_insert(DirectionTerminal::Reset(app_error_code));
                    take_waiter(&mut state.recv_waiter, &mut effects);
                }
            }
            Event::StopSending {
                stream_id,
                app_error_code,
            } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state
                        .send_terminal
                        .get_or_insert(DirectionTerminal::Stopped(app_error_code));
                    effects.discard.push(stream_id);
                    take_waiter(&mut state.send_waiter, &mut effects);
                    take_waiter(&mut state.finish_waiter, &mut effects);
                }
            }
            Event::StreamDataCredit { stream_id, .. } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    take_waiter(&mut state.send_waiter, &mut effects);
                    take_waiter(&mut state.finish_waiter, &mut effects);
                }
            }
            Event::StreamLimit { kind, .. } => match kind {
                StreamLimitKind::LocalBidi => self.wake_openers(OpenKind::Bidi, &mut effects),
                StreamLimitKind::LocalUni => self.wake_openers(OpenKind::Uni, &mut effects),
                StreamLimitKind::RemoteBidi | StreamLimitKind::RemoteUni => {}
            },
            Event::PeerTransportParams(_) => {
                self.wake_openers(OpenKind::Bidi, &mut effects);
                self.wake_openers(OpenKind::Uni, &mut effects);
                self.wake_all_senders(&mut effects);
            }
            _ => {
                return self.unsupported_event();
            }
        }
        effects
    }

    fn unsupported_event(&mut self) -> Effects {
        self.fail(ConnectionTerminal::Internal(
            "unsupported QMux event variant".into(),
        ))
    }

    fn discover_peer(&mut self, stream_id: StreamId, effects: &mut Effects) -> Result<(), ()> {
        if self.streams.contains_key(&stream_id) {
            return Ok(());
        }
        if stream_id.initiator() == self.local_initiator() {
            effects.merge(self.fail(ConnectionTerminal::Internal(
                "unknown locally initiated stream".into(),
            )));
            return Err(());
        }
        let pending = self.pending_uni.len() + self.pending_bidi.len();
        if pending >= self.pending_limit {
            let code = Code::H3_EXCESSIVE_LOAD.value();
            self.close
                .get_or_insert_with(|| close_reason(code, b"pending accept limit exceeded"));
            effects.merge(self.fail(ConnectionTerminal::Application(code)));
            return Err(());
        }

        let mut state = StreamState {
            pending_accept: true,
            ..StreamState::default()
        };
        let (queue, waiter) = match stream_id.directionality() {
            Directionality::Unidirectional => {
                state.recv_handle = false;
                (&mut self.pending_uni, &mut self.accept_uni)
            }
            Directionality::Bidirectional => {
                state.recv_handle = false;
                state.send_handle = false;
                (&mut self.pending_bidi, &mut self.accept_bidi)
            }
        };
        self.streams.insert(stream_id, state);
        queue.push_back(stream_id);
        take_waiter(waiter, effects);
        Ok(())
    }

    pub(crate) fn accept(
        &mut self,
        kind: AcceptKind,
        waker: &Waker,
        effects: &mut Effects,
    ) -> Result<Option<StreamId>, ConnectionTerminal> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.clone());
        }
        let (queue, waiter) = match kind {
            AcceptKind::Uni => (&mut self.pending_uni, &mut self.accept_uni),
            AcceptKind::Bidi => (&mut self.pending_bidi, &mut self.accept_bidi),
        };
        if let Some(stream_id) = queue.pop_front() {
            let state = self.streams.get_mut(&stream_id).expect("queued stream");
            state.pending_accept = false;
            state.recv_handle = true;
            if kind == AcceptKind::Bidi {
                state.send_handle = true;
            }
            return Ok(Some(stream_id));
        }
        replace_waiter(waiter, waker, effects);
        Ok(None)
    }

    pub(crate) fn open(
        &mut self,
        opener: u64,
        kind: OpenKind,
        waker: &Waker,
        effects: &mut Effects,
    ) -> Result<Option<StreamId>, ConnectionTerminal> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.clone());
        }
        let opened = match match kind {
            OpenKind::Uni => self.lower.try_open_uni(),
            OpenKind::Bidi => self.lower.try_open_bidi(),
        } {
            Ok(opened) => opened,
            Err(error) => {
                let terminal = ConnectionTerminal::from_lower(&error);
                effects.merge(self.fail(terminal.clone()));
                return Err(terminal);
            }
        };

        match opened {
            StreamOpen::Opened(stream_id) => {
                let state = StreamState {
                    send_handle: true,
                    recv_handle: kind == OpenKind::Bidi,
                    ..StreamState::default()
                };
                self.streams.insert(stream_id, state);
                Ok(Some(stream_id))
            }
            StreamOpen::Blocked => {
                let slots = self.openers.entry(opener).or_default();
                let waiter = match kind {
                    OpenKind::Uni => &mut slots.uni,
                    OpenKind::Bidi => &mut slots.bidi,
                };
                replace_waiter(waiter, waker, effects);
                Ok(None)
            }
        }
    }

    pub(crate) fn close(&mut self, code: u64, reason: &[u8]) -> Effects {
        if self.close.is_some() {
            return Effects::default();
        }
        self.close = Some(close_reason(code, reason));
        self.fail(ConnectionTerminal::Application(code))
    }

    pub(crate) fn fail(&mut self, terminal: ConnectionTerminal) -> Effects {
        if self.terminal.is_some() {
            return Effects::default();
        }
        self.terminal = Some(terminal);
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::terminal();
        let mut effects = Effects {
            discard_all: true,
            ..Effects::default()
        };
        take_waiter(&mut self.accept_uni, &mut effects);
        take_waiter(&mut self.accept_bidi, &mut effects);
        for opener in self.openers.values_mut() {
            take_waiter(&mut opener.uni, &mut effects);
            take_waiter(&mut opener.bidi, &mut effects);
        }
        for stream in self.streams.values_mut() {
            take_waiter(&mut stream.recv_waiter, &mut effects);
            take_waiter(&mut stream.send_waiter, &mut effects);
            take_waiter(&mut stream.finish_waiter, &mut effects);
        }
        effects
    }

    pub(crate) fn wake_all_senders(&mut self, effects: &mut Effects) {
        for stream in self.streams.values_mut() {
            take_waiter(&mut stream.send_waiter, effects);
            take_waiter(&mut stream.finish_waiter, effects);
            stream.send_waiting_output = false;
            stream.finish_waiting_output = false;
        }
    }

    pub(crate) fn wake_output_senders(&mut self, effects: &mut Effects) {
        for stream in self.streams.values_mut() {
            if stream.send_waiting_output {
                take_waiter(&mut stream.send_waiter, effects);
                stream.send_waiting_output = false;
            }
            if stream.finish_waiting_output {
                take_waiter(&mut stream.finish_waiter, effects);
                stream.finish_waiting_output = false;
            }
        }
    }

    fn wake_openers(&mut self, kind: OpenKind, effects: &mut Effects) {
        for opener in self.openers.values_mut() {
            let waiter = match kind {
                OpenKind::Uni => &mut opener.uni,
                OpenKind::Bidi => &mut opener.bidi,
            };
            take_waiter(waiter, effects);
        }
    }

    pub(crate) fn stream_error(
        &self,
        stream_id: StreamId,
        send: bool,
    ) -> Option<DirectionTerminal> {
        self.streams.get(&stream_id).and_then(|state| {
            if send {
                state.send_terminal
            } else {
                state.recv_terminal
            }
        })
    }

    pub(crate) fn reconcile_closed_send(
        &mut self,
        stream_id: StreamId,
        effects: &mut Effects,
    ) -> Result<DirectionTerminal, ConnectionTerminal> {
        for routed in 0..ROUTE_BUDGET {
            if let Some(terminal) = &self.terminal {
                return Err(terminal.clone());
            }
            if let Some(terminal) = self.stream_error(stream_id, true) {
                return Ok(terminal);
            }
            let Some(event) = self.lower.try_next_event() else {
                break;
            };
            effects.merge(self.route(event));
            if routed + 1 == ROUTE_BUDGET {
                effects.continuation = true;
            }
        }
        if let Some(terminal) = &self.terminal {
            return Err(terminal.clone());
        }
        if let Some(terminal) = self.stream_error(stream_id, true) {
            return Ok(terminal);
        }
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.send_terminal = Some(DirectionTerminal::Closed);
        }
        Ok(DirectionTerminal::Closed)
    }

    pub(crate) fn park_send(
        &mut self,
        stream_id: StreamId,
        finish: bool,
        waiting_output: bool,
        waker: &Waker,
        effects: &mut Effects,
    ) {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            let slot = if finish {
                state.finish_waiting_output = waiting_output;
                &mut state.finish_waiter
            } else {
                state.send_waiting_output = waiting_output;
                &mut state.send_waiter
            };
            replace_waiter(slot, waker, effects);
        }
    }

    pub(crate) fn park_recv(&mut self, stream_id: StreamId, waker: &Waker, effects: &mut Effects) {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            replace_waiter(&mut state.recv_waiter, waker, effects);
        }
    }

    pub(crate) fn discard_receive(
        &mut self,
        stream_id: StreamId,
        code: u64,
    ) -> Result<(), ConnectionTerminal> {
        let bytes = {
            let Some(state) = self.streams.get_mut(&stream_id) else {
                return Ok(());
            };
            if state.recv_terminal.is_none() && !state.recv_shutdown_sent {
                self.lower
                    .shutdown_stream(stream_id, Shutdown::Read, code)
                    .map_err(|error| ConnectionTerminal::from_lower(&error))?;
                state.recv_shutdown_sent = true;
            }
            if state.recv_terminal.is_none() {
                state.recv_terminal = Some(DirectionTerminal::Stopped(code));
            }
            state
                .recv
                .drain(..)
                .map(|item| item.data.len() as u64)
                .sum::<u64>()
        };
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::receive_gauge(self.retained_receive_bytes());
        if bytes != 0 {
            self.lower
                .extend_connection_credit(bytes)
                .map_err(|error| ConnectionTerminal::from_lower(&error))?;
            #[cfg(feature = "diagnostics")]
            crate::diagnostics::connection_credit();
        }
        Ok(())
    }

    pub(crate) fn reset_send(
        &mut self,
        stream_id: StreamId,
        code: u64,
    ) -> Result<(), ConnectionTerminal> {
        let Some(state) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if state.send_terminal.is_some() {
            return Ok(());
        }
        if !state.send_shutdown_sent {
            self.lower
                .shutdown_stream(stream_id, Shutdown::Write, code)
                .map_err(|error| ConnectionTerminal::from_lower(&error))?;
            state.send_shutdown_sent = true;
        }
        state.send_terminal = Some(DirectionTerminal::Reset(code));
        Ok(())
    }

    pub(crate) fn drop_direction(&mut self, stream_id: StreamId, send: bool) {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            if send {
                state.send_handle = false;
            } else {
                state.recv_handle = false;
            }
        }
        self.cleanup(stream_id);
    }

    pub(crate) fn cleanup(&mut self, stream_id: StreamId) {
        let remove = self.streams.get(&stream_id).is_some_and(|state| {
            !state.pending_accept
                && state.lower_closed
                && !state.send_handle
                && !state.recv_handle
                && state.recv.is_empty()
        });
        if remove {
            self.streams.remove(&stream_id);
            #[cfg(feature = "diagnostics")]
            crate::diagnostics::cleanup();
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(crate) fn retained_receive_bytes(&self) -> usize {
        self.streams
            .values()
            .flat_map(|state| state.recv.iter())
            .map(|item| item.data.len())
            .sum()
    }

    fn local_initiator(&self) -> Initiator {
        match self.role {
            Role::Client => Initiator::Client,
            Role::Server => Initiator::Server,
        }
    }

    fn receive_direction_valid(&self, stream_id: StreamId) -> bool {
        stream_id.directionality() == Directionality::Bidirectional
            || stream_id.initiator() != self.local_initiator()
    }
}

fn replace_waiter(slot: &mut Option<Waker>, waker: &Waker, effects: &mut Effects) {
    match slot.as_ref() {
        Some(current) if current.will_wake(waker) => {}
        _ => {
            let displaced = slot.replace(waker.clone());
            #[cfg(feature = "diagnostics")]
            crate::diagnostics::waiter(displaced.is_some());
            if let Some(displaced) = displaced {
                effects.wakes.push(displaced);
            }
        }
    }
}

fn take_waiter(slot: &mut Option<Waker>, effects: &mut Effects) {
    if let Some(waker) = slot.take() {
        effects.wakes.push(waker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use ngnet_qmux::TransportParams;
    use ngnet_qmux::io::Config;
    use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};

    fn make_core(limit: usize) -> Core<TestByteStream, TestClock> {
        let (near, _far) = stream_pair();
        let lower = QmuxConnection::client(near, TestClock::new(), Config::new())
            .expect("test QMux connection");
        Core::new(lower, limit)
    }

    fn stream(id: i64) -> StreamId {
        StreamId::new(id).expect("valid stream id")
    }

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn explicit_and_data_first_peer_streams_each_enter_one_accept_queue() {
        let mut core = make_core(4);
        let _ = core.route(Event::StreamOpened {
            stream_id: stream(1),
        });
        let _ = core.route(Event::StreamOpened {
            stream_id: stream(1),
        });
        let _ = core.route(Event::StreamData {
            stream_id: stream(3),
            offset: 0,
            data: b"uni".to_vec(),
            fin: true,
        });
        let mut effects = Effects::default();
        let waker = Waker::noop();
        assert_eq!(
            core.accept(AcceptKind::Bidi, waker, &mut effects)
                .expect("accept"),
            Some(stream(1))
        );
        assert_eq!(
            core.accept(AcceptKind::Bidi, waker, &mut effects)
                .expect("accept"),
            None
        );
        assert_eq!(
            core.accept(AcceptKind::Uni, waker, &mut effects)
                .expect("accept"),
            Some(stream(3))
        );
    }

    #[test]
    fn impossible_local_unidirectional_receive_is_connection_fatal() {
        let mut core = make_core(4);
        let effects = core.route(Event::StreamData {
            stream_id: stream(2),
            offset: 0,
            data: b"impossible".to_vec(),
            fin: false,
        });
        assert!(effects.discard_all);
        assert!(matches!(
            core.terminal,
            Some(ConnectionTerminal::Internal(_))
        ));
    }

    #[test]
    fn reset_stop_credit_and_closed_events_update_only_named_directions() {
        let mut core = make_core(4);
        let id = stream(1);
        core.streams.insert(
            id,
            StreamState {
                send_handle: true,
                recv_handle: true,
                ..StreamState::default()
            },
        );
        let recv_count = Arc::new(Count::default());
        let send_count = Arc::new(Count::default());
        core.streams.get_mut(&id).expect("stream").recv_waiter =
            Some(Waker::from(Arc::clone(&recv_count)));
        core.streams.get_mut(&id).expect("stream").send_waiter =
            Some(Waker::from(Arc::clone(&send_count)));

        let reset = core.route(Event::StreamReset {
            stream_id: id,
            final_size: 0,
            app_error_code: 11,
        });
        for wake in reset.wakes {
            wake.wake();
        }
        assert_eq!(recv_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(send_count.0.load(Ordering::SeqCst), 0);

        let stop = core.route(Event::StopSending {
            stream_id: id,
            app_error_code: 12,
        });
        for wake in stop.wakes {
            wake.wake();
        }
        assert_eq!(send_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(stop.discard, vec![id]);

        let _ = core.route(Event::StreamDataCredit {
            stream_id: id,
            max_data: 100,
        });
        let _ = core.route(Event::StreamClosed {
            stream_id: id,
            rx_app_error_code: Some(11),
            tx_app_error_code: Some(12),
        });
        let state = core.streams.get(&id).expect("stream");
        assert_eq!(state.recv_terminal, Some(DirectionTerminal::Reset(11)));
        assert_eq!(state.send_terminal, Some(DirectionTerminal::Stopped(12)));
    }

    #[test]
    fn stream_limits_and_transport_parameters_wake_matching_open_classes() {
        let mut core = make_core(4);
        let bidi = Arc::new(Count::default());
        let uni = Arc::new(Count::default());
        core.openers.get_mut(&0).expect("root opener").bidi = Some(Waker::from(Arc::clone(&bidi)));
        core.openers.get_mut(&0).expect("root opener").uni = Some(Waker::from(Arc::clone(&uni)));
        let event = core.route(Event::StreamLimit {
            kind: StreamLimitKind::LocalBidi,
            max_streams: 1,
        });
        for wake in event.wakes {
            wake.wake();
        }
        assert_eq!(bidi.0.load(Ordering::SeqCst), 1);
        assert_eq!(uni.0.load(Ordering::SeqCst), 0);

        let event = core.route(Event::PeerTransportParams(TransportParams::new()));
        for wake in event.wakes {
            wake.wake();
        }
        assert_eq!(uni.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn overload_and_unknown_event_policies_are_stable_connection_failures() {
        let mut core = make_core(1);
        let _ = core.route(Event::StreamOpened {
            stream_id: stream(1),
        });
        let effects = core.route(Event::StreamOpened {
            stream_id: stream(3),
        });
        assert!(effects.discard_all);
        assert!(matches!(
            core.terminal,
            Some(ConnectionTerminal::Application(code))
                if code == Code::H3_EXCESSIVE_LOAD.value()
        ));

        let mut core = make_core(1);
        let first = core.unsupported_event();
        let second = core.unsupported_event();
        assert!(first.discard_all);
        assert!(
            !second.discard_all,
            "the first terminal classification wins"
        );
        assert!(matches!(
            core.terminal,
            Some(ConnectionTerminal::Internal(_))
        ));
    }

    #[test]
    fn synchronous_lower_wake_is_deferred_until_the_core_poll_scope_ends() {
        let lower = Arc::new(LowerWake::default());
        let count = Arc::new(Count::default());
        let driver = Waker::from(Arc::clone(&count));
        assert!(lower.register_driver(&driver).is_none());
        lower.begin_defer();
        Waker::from(Arc::clone(&lower)).wake_by_ref();
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        lower.end_defer();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stream_credit_wakes_only_the_named_writer_while_connection_credit_wakes_all() {
        let mut core = make_core(4);
        let first_id = stream(1);
        let second_id = stream(5);
        core.streams.insert(
            first_id,
            StreamState {
                send_handle: true,
                ..StreamState::default()
            },
        );
        core.streams.insert(
            second_id,
            StreamState {
                send_handle: true,
                ..StreamState::default()
            },
        );
        let first = Arc::new(Count::default());
        let second = Arc::new(Count::default());
        core.streams.get_mut(&first_id).expect("first").send_waiter =
            Some(Waker::from(Arc::clone(&first)));
        core.streams
            .get_mut(&second_id)
            .expect("second")
            .send_waiter = Some(Waker::from(Arc::clone(&second)));

        let effects = core.route(Event::StreamDataCredit {
            stream_id: first_id,
            max_data: 8,
        });
        for wake in effects.wakes {
            wake.wake();
        }
        assert_eq!(first.0.load(Ordering::SeqCst), 1);
        assert_eq!(second.0.load(Ordering::SeqCst), 0);

        core.streams.get_mut(&first_id).expect("first").send_waiter =
            Some(Waker::from(Arc::clone(&first)));
        let mut effects = Effects::default();
        core.wake_all_senders(&mut effects);
        for wake in effects.wakes {
            wake.wake();
        }
        assert_eq!(first.0.load(Ordering::SeqCst), 2);
        assert_eq!(second.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropped_stream_is_retired_only_after_ordered_lower_close() {
        let mut core = make_core(4);
        let id = stream(0);
        core.streams.insert(
            id,
            StreamState {
                recv_shutdown_sent: true,
                send_terminal: Some(DirectionTerminal::Finished),
                recv_terminal: Some(DirectionTerminal::Stopped(ABANDONED)),
                ..StreamState::default()
            },
        );
        core.cleanup(id);
        assert!(core.streams.contains_key(&id));

        let effects = core.route(Event::StreamData {
            stream_id: id,
            offset: 0,
            data: b"late".to_vec(),
            fin: false,
        });
        assert!(!effects.discard_all);
        assert!(core.terminal.is_none());
        assert!(core.streams.contains_key(&id));

        let _ = core.route(Event::StreamClosed {
            stream_id: id,
            rx_app_error_code: None,
            tx_app_error_code: None,
        });
        assert!(!core.streams.contains_key(&id));
    }

    #[test]
    fn immediate_open_error_latches_terminal_and_fans_out_waiters() {
        let mut core = make_core(4);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(
            core.lower
                .poll_close(&mut cx, &CloseReason::application(9, b"closed")),
            Poll::Ready(Ok(()))
        ));

        let accept = Arc::new(Count::default());
        let opener = Arc::new(Count::default());
        core.accept_uni = Some(Waker::from(Arc::clone(&accept)));
        core.openers.get_mut(&0).expect("root opener").uni = Some(Waker::from(Arc::clone(&opener)));
        let mut effects = Effects::default();
        assert!(core.open(0, OpenKind::Uni, waker, &mut effects).is_err());
        assert!(core.terminal.is_some());
        assert!(effects.discard_all);
        for wake in effects.wakes {
            wake.wake();
        }
        assert_eq!(accept.0.load(Ordering::SeqCst), 1);
        assert_eq!(opener.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn closed_send_reconciliation_is_stream_scoped_without_an_event() {
        let mut core = make_core(4);
        let id = stream(0);
        core.streams.insert(
            id,
            StreamState {
                send_handle: true,
                ..StreamState::default()
            },
        );
        let mut effects = Effects::default();
        assert!(matches!(
            core.reconcile_closed_send(id, &mut effects),
            Ok(DirectionTerminal::Closed)
        ));
        assert_eq!(core.stream_error(id, true), Some(DirectionTerminal::Closed));
        assert!(core.terminal.is_none());
        assert!(!effects.discard_all);
    }
}
