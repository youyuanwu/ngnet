//! Private stream ownership and bidirectional lifecycle tracking.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use ngnet_h3::http::QuicEvent;
use ngnet_h3::{Directionality, ErrorCode, StreamId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Open,
    Ended(Option<ErrorCode>),
}

struct Bidi<Stop> {
    rx: Direction,
    tx: Direction,
    stop: Option<Stop>,
    close_queued: bool,
}

struct Send<Handle> {
    handle: Handle,
    finished: bool,
}

/// One bidirectional stream whose two directions have ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Closed {
    pub(crate) stream: StreamId,
    pub(crate) rx_code: Option<ErrorCode>,
    pub(crate) tx_code: Option<ErrorCode>,
}

/// All stream-owned adapter state.
///
/// The handle type is generic so the exact ownership path used with Quinn handles can be
/// exercised with inert handles in unit tests.
pub(crate) struct Streams<Handle, Stop> {
    sends: HashMap<i64, Send<Handle>>,
    bidis: HashMap<i64, Bidi<Stop>>,
    closing: VecDeque<Closed>,
}

impl<Handle, Stop> Streams<Handle, Stop> {
    pub(crate) fn new() -> Self {
        Self {
            sends: HashMap::new(),
            bidis: HashMap::new(),
            closing: VecDeque::new(),
        }
    }

    pub(crate) fn insert_uni(&mut self, stream: StreamId, send: Handle) {
        self.sends.insert(
            stream.get(),
            Send {
                handle: send,
                finished: false,
            },
        );
    }

    pub(crate) fn insert_bidi(&mut self, stream: StreamId, send: Handle, stop: Stop) {
        self.sends.insert(
            stream.get(),
            Send {
                handle: send,
                finished: false,
            },
        );
        self.bidis.insert(
            stream.get(),
            Bidi {
                rx: Direction::Open,
                tx: Direction::Open,
                stop: Some(stop),
                close_queued: false,
            },
        );
    }

    pub(crate) fn send_mut(&mut self, stream: StreamId) -> Option<&mut Handle> {
        self.sends
            .get_mut(&stream.get())
            .map(|send| &mut send.handle)
    }

    pub(crate) fn send_finished(&self, stream: StreamId) -> bool {
        self.sends
            .get(&stream.get())
            .is_some_and(|send| send.finished)
    }

    /// Records the first terminal send outcome.
    ///
    /// Returns whether this call changed the direction.
    pub(crate) fn finish_send(&mut self, stream: StreamId, code: Option<ErrorCode>) -> bool {
        let Some(send) = self.sends.get_mut(&stream.get()) else {
            return false;
        };
        if send.finished {
            return false;
        }
        send.finished = true;
        let changed = match self.bidis.get_mut(&stream.get()) {
            Some(bidi) if bidi.tx == Direction::Open => {
                bidi.tx = Direction::Ended(code);
                true
            }
            _ => false,
        };
        self.queue_close(stream);
        changed
    }

    /// Records the first terminal receive outcome.
    ///
    /// Returns whether this call changed the direction.
    pub(crate) fn finish_recv(&mut self, stream: StreamId, code: Option<ErrorCode>) -> bool {
        let changed = match self.bidis.get_mut(&stream.get()) {
            Some(bidi) if bidi.rx == Direction::Open => {
                bidi.rx = Direction::Ended(code);
                bidi.stop = None;
                true
            }
            _ => false,
        };
        self.queue_close(stream);
        changed
    }

    pub(crate) fn take_stop(&mut self, stream: StreamId) -> Option<Stop> {
        self.bidis
            .get_mut(&stream.get())
            .and_then(|bidi| bidi.stop.take())
    }

    pub(crate) fn pending_close(&self) -> Option<Closed> {
        self.closing.front().copied()
    }

    /// Removes every piece of state for the next fully ended stream.
    pub(crate) fn pop_close(&mut self) -> Option<Closed> {
        let closed = self.closing.pop_front()?;
        self.sends.remove(&closed.stream.get());
        self.bidis.remove(&closed.stream.get());
        Some(closed)
    }

    pub(crate) fn clear(&mut self) {
        self.closing.clear();
        self.bidis.clear();
        self.sends.clear();
    }

    fn queue_close(&mut self, stream: StreamId) {
        let Some(bidi) = self.bidis.get_mut(&stream.get()) else {
            return;
        };
        if bidi.close_queued {
            return;
        }
        let (Direction::Ended(rx_code), Direction::Ended(tx_code)) = (bidi.rx, bidi.tx) else {
            return;
        };
        bidi.close_queued = true;
        self.closing.push_back(Closed {
            stream,
            rx_code,
            tx_code,
        });
    }

    #[cfg(test)]
    fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.sends.len(),
            self.sends.values().filter(|send| send.finished).count(),
            self.bidis.len(),
            self.closing.len(),
        )
    }
}

/// A Quinn observation waiting to be ordered against releases and terminal events.
pub(crate) enum Incoming<Handle, Stop> {
    Data {
        stream: StreamId,
        bytes: Bytes,
        fin: bool,
    },
    Accepted {
        stream: StreamId,
        send: Handle,
        stop: Stop,
    },
    Reset {
        stream: StreamId,
        code: ErrorCode,
    },
    RecvStopped {
        stream: StreamId,
        code: ErrorCode,
    },
    StopSending {
        stream: StreamId,
        code: ErrorCode,
    },
    SendStopped {
        stream: StreamId,
        code: Option<ErrorCode>,
    },
    Closed,
}

/// The next action for the connection shell.
pub(crate) enum Step {
    Event(QuicEvent),
    Boundary,
    NeedInput,
    Finished,
}

/// Connection-independent event ordering and stream ownership.
pub(crate) struct Lifecycle<Handle, Stop> {
    streams: Streams<Handle, Stop>,
    released: VecDeque<(StreamId, u64)>,
    queued: VecDeque<Incoming<Handle, Stop>>,
    closed: bool,
    reported_closed: bool,
    batch: BatchGate,
}

impl<Handle, Stop> Lifecycle<Handle, Stop> {
    pub(crate) fn new() -> Self {
        Self {
            streams: Streams::new(),
            released: VecDeque::new(),
            queued: VecDeque::new(),
            closed: false,
            reported_closed: false,
            batch: BatchGate::new(),
        }
    }

    pub(crate) fn insert_uni(&mut self, stream: StreamId, send: Handle) {
        self.streams.insert_uni(stream, send);
    }

    pub(crate) fn insert_bidi(&mut self, stream: StreamId, send: Handle, stop: Stop) {
        self.streams.insert_bidi(stream, send, stop);
    }

    pub(crate) fn send_mut(&mut self, stream: StreamId) -> Option<&mut Handle> {
        self.streams.send_mut(stream)
    }

    pub(crate) fn send_finished(&self, stream: StreamId) -> bool {
        self.streams.send_finished(stream)
    }

    pub(crate) fn finish_send(&mut self, stream: StreamId, code: Option<ErrorCode>) -> bool {
        self.streams.finish_send(stream, code)
    }

    pub(crate) fn take_stop(&mut self, stream: StreamId) -> Option<Stop> {
        self.streams.take_stop(stream)
    }

    pub(crate) fn release(&mut self, stream: StreamId, bytes: u64) {
        self.released.push_back((stream, bytes));
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Queues one transport observation unless shutdown has already been latched.
    pub(crate) fn push(&mut self, event: Incoming<Handle, Stop>) {
        if self.closed {
            return;
        }
        if matches!(event, Incoming::Closed) {
            self.closed = true;
        } else {
            self.queued.push_back(event);
        }
    }

    /// Latches shutdown after preserving observations already queued at the latch point.
    pub(crate) fn latch_external_shutdown(
        &mut self,
        events: impl IntoIterator<Item = Incoming<Handle, Stop>>,
    ) {
        if self.closed {
            return;
        }
        for event in events {
            if matches!(event, Incoming::Closed) {
                break;
            }
            self.queued.push_back(event);
        }
        self.closed = true;
    }

    /// Records that the connection shell genuinely ran out of input.
    pub(crate) fn pending(&mut self) {
        self.batch.pending();
    }

    /// Returns the next externally observable event or scheduling action.
    pub(crate) fn next(&mut self) -> Step {
        if self.reported_closed {
            return Step::Finished;
        }

        loop {
            if !self.closed
                && let Some(closed) = self.streams.pending_close()
            {
                if let Some(position) = self
                    .released
                    .iter()
                    .position(|(stream, _)| *stream == closed.stream)
                {
                    let (stream, bytes) = self
                        .released
                        .remove(position)
                        .expect("the release position was observed above");
                    self.batch.emitted();
                    return Step::Event(QuicEvent::Released {
                        stream,
                        bytes,
                        delivered: true,
                    });
                }
                if self.batch.take_boundary() {
                    return Step::Boundary;
                }
                let closed = self
                    .streams
                    .pop_close()
                    .expect("the close was observed above");
                self.batch.emitted();
                return Step::Event(QuicEvent::StreamClosed {
                    stream: closed.stream,
                    rx_code: closed.rx_code,
                    tx_code: closed.tx_code,
                });
            }

            if let Some((stream, bytes)) = self.released.pop_front() {
                self.batch.emitted();
                return Step::Event(QuicEvent::Released {
                    stream,
                    bytes,
                    delivered: true,
                });
            }

            if let Some(event) = self.queued.pop_front() {
                match event {
                    Incoming::Data { stream, bytes, fin } => {
                        if fin {
                            self.streams.finish_recv(stream, None);
                        }
                        self.batch.emitted();
                        return Step::Event(QuicEvent::Data { stream, bytes, fin });
                    }
                    Incoming::Accepted { stream, send, stop } => {
                        self.streams.insert_bidi(stream, send, stop);
                        self.batch.emitted();
                        return Step::Event(QuicEvent::Accepted { stream });
                    }
                    Incoming::Reset { stream, code } => {
                        if matches!(stream.directionality(), Directionality::Unidirectional)
                            || self.streams.finish_recv(stream, Some(code))
                        {
                            self.batch.emitted();
                            return Step::Event(QuicEvent::Reset { stream, code });
                        }
                    }
                    Incoming::RecvStopped { stream, code } => {
                        self.streams.finish_recv(stream, Some(code));
                    }
                    Incoming::StopSending { stream, code } => {
                        if self.streams.finish_send(stream, Some(code)) {
                            self.batch.emitted();
                            return Step::Event(QuicEvent::StopSending { stream, code });
                        }
                    }
                    Incoming::SendStopped { stream, code } => {
                        self.streams.finish_send(stream, code);
                    }
                    Incoming::Closed => unreachable!("shutdown markers are never queued"),
                }
                continue;
            }

            if !self.closed {
                return Step::NeedInput;
            }
            if self.batch.take_boundary() {
                return Step::Boundary;
            }
            self.streams.clear();
            self.reported_closed = true;
            self.batch.emitted();
            return Step::Event(QuicEvent::Closed { code: None });
        }
    }

    #[cfg(test)]
    fn counts(&self) -> (usize, usize, usize, usize) {
        self.streams.counts()
    }
}

/// Separates terminal events from events already returned in the current driver batch.
struct BatchGate {
    emitted_since_pending: bool,
}

impl BatchGate {
    fn new() -> Self {
        Self {
            emitted_since_pending: false,
        }
    }

    /// Records that an event was returned to the driver.
    fn emitted(&mut self) {
        self.emitted_since_pending = true;
    }

    /// Records a genuine poll with no event.
    fn pending(&mut self) {
        self.emitted_since_pending = false;
    }

    /// Whether a terminal event must first force a fresh batch.
    fn take_boundary(&mut self) -> bool {
        if !self.emitted_since_pending {
            return false;
        }
        self.emitted_since_pending = false;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(value: i64) -> StreamId {
        StreamId::new(value).expect("a valid stream")
    }

    fn event(lifecycle: &mut Lifecycle<(), ()>) -> QuicEvent {
        match lifecycle.next() {
            Step::Event(event) => event,
            _ => panic!("expected an event"),
        }
    }

    #[test]
    fn receive_then_send_closes_once_with_both_codes() {
        let mut streams = Streams::new();
        let id = stream(0);
        streams.insert_bidi(id, (), ());

        assert!(streams.finish_recv(id, Some(ErrorCode::new(0x11))));
        assert_eq!(streams.pending_close(), None);
        assert!(streams.finish_send(id, Some(ErrorCode::new(0x22))));
        assert!(!streams.finish_send(id, None));
        assert!(!streams.finish_recv(id, None));

        assert_eq!(
            streams.pop_close(),
            Some(Closed {
                stream: id,
                rx_code: Some(ErrorCode::new(0x11)),
                tx_code: Some(ErrorCode::new(0x22)),
            })
        );
        assert_eq!(streams.pop_close(), None);
        assert_eq!(streams.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn send_then_receive_closes_cleanly() {
        let mut streams = Streams::new();
        let id = stream(4);
        streams.insert_bidi(id, (), ());

        assert!(streams.finish_send(id, None));
        assert_eq!(streams.pending_close(), None);
        assert!(streams.finish_recv(id, None));
        assert_eq!(
            streams.pop_close(),
            Some(Closed {
                stream: id,
                rx_code: None,
                tx_code: None,
            })
        );
    }

    #[test]
    fn a_thousand_completed_streams_leave_no_history() {
        let mut streams = Streams::new();
        for value in 0..1_000 {
            let id = stream(value * 4);
            streams.insert_bidi(id, (), ());
            assert!(streams.finish_send(id, None));
            assert!(streams.finish_recv(id, None));
            assert_eq!(streams.pop_close().map(|closed| closed.stream), Some(id));
        }
        assert_eq!(streams.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn late_send_observation_does_not_retain_closed_stream_history() {
        let mut streams = Streams::new();
        let id = stream(0);
        streams.insert_bidi(id, (), ());
        assert!(streams.finish_send(id, None));
        assert!(streams.finish_recv(id, None));
        assert_eq!(streams.pop_close().map(|closed| closed.stream), Some(id));

        assert!(!streams.finish_send(id, Some(ErrorCode::new(0x11))));
        assert!(!streams.send_finished(id));
        assert_eq!(streams.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn terminal_outcome_for_unknown_stream_is_not_remembered() {
        let mut streams = Streams::new();
        let id = stream(0);

        assert!(!streams.finish_send(id, Some(ErrorCode::new(0x11))));
        assert!(!streams.finish_recv(id, Some(ErrorCode::new(0x22))));
        assert_eq!(streams.counts(), (0, 0, 0, 0));

        streams.insert_bidi(id, (), ());
        assert!(!streams.send_finished(id));
        assert!(streams.finish_send(id, None));
        assert!(streams.finish_recv(id, None));
        assert_eq!(
            streams.pop_close(),
            Some(Closed {
                stream: id,
                rx_code: None,
                tx_code: None,
            })
        );
    }

    #[test]
    fn connection_shutdown_drops_active_and_pending_state() {
        let mut streams = Streams::new();
        streams.insert_bidi(stream(0), (), ());
        streams.insert_bidi(stream(4), (), ());
        streams.finish_send(stream(4), None);
        streams.finish_recv(stream(4), None);
        streams.insert_uni(stream(2), ());

        streams.clear();

        assert_eq!(streams.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn a_terminal_event_after_an_event_requires_one_boundary() {
        let mut gate = BatchGate::new();
        assert!(!gate.take_boundary());

        gate.emitted();
        assert!(gate.take_boundary());
        assert!(!gate.take_boundary());

        gate.emitted();
        gate.pending();
        assert!(!gate.take_boundary());
    }

    #[test]
    fn non_empty_final_data_starts_a_new_batch_before_close() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        lifecycle.insert_bidi(id, (), ());
        assert!(lifecycle.finish_send(id, None));
        lifecycle.push(Incoming::Data {
            stream: id,
            bytes: Bytes::from_static(b"final"),
            fin: true,
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Data { stream, bytes, fin: true }
                if stream == id && bytes == Bytes::from_static(b"final")
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: None,
                tx_code: None,
            } if stream == id
        ));
        assert_eq!(lifecycle.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn own_releases_precede_close_but_unrelated_releases_do_not_postpone_it() {
        let mut lifecycle = Lifecycle::new();
        let closing = stream(0);
        let other = stream(4);
        lifecycle.insert_bidi(closing, (), ());
        lifecycle.insert_bidi(other, (), ());
        lifecycle.release(other, 3);
        lifecycle.release(closing, 2);
        assert!(lifecycle.finish_send(closing, None));
        assert!(lifecycle.streams.finish_recv(closing, None));

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Released { stream, bytes: 2, delivered: true } if stream == closing
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed { stream, .. } if stream == closing
        ));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Released { stream, bytes: 3, delivered: true } if stream == other
        ));
    }

    #[test]
    fn peer_reset_and_send_observer_settle_without_a_later_write() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        let rx = ErrorCode::new(0x11);
        let tx = ErrorCode::new(0x22);
        lifecycle.insert_bidi(id, (), ());
        lifecycle.push(Incoming::Reset {
            stream: id,
            code: rx,
        });
        lifecycle.push(Incoming::SendStopped {
            stream: id,
            code: Some(tx),
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Reset { stream, code } if stream == id && code == rx
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: Some(actual_rx),
                tx_code: Some(actual_tx),
            } if stream == id && actual_rx == rx && actual_tx == tx
        ));
        assert_eq!(lifecycle.counts(), (0, 0, 0, 0));
    }

    #[test]
    fn queued_work_precedes_one_final_connection_close() {
        let mut lifecycle = Lifecycle::new();
        let active = stream(0);
        let half_closed = stream(4);
        let pending_close = stream(8);
        lifecycle.insert_bidi(active, (), ());
        lifecycle.insert_bidi(half_closed, (), ());
        lifecycle.insert_bidi(pending_close, (), ());
        lifecycle.finish_send(half_closed, None);
        lifecycle.finish_send(pending_close, None);
        lifecycle.streams.finish_recv(pending_close, None);
        lifecycle.release(active, 7);
        lifecycle.latch_external_shutdown([
            Incoming::Data {
                stream: active,
                bytes: Bytes::from_static(b"last"),
                fin: true,
            },
            Incoming::Closed,
            Incoming::Reset {
                stream: active,
                code: ErrorCode::new(0x33),
            },
        ]);
        lifecycle.push(Incoming::Closed);

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Released { stream, bytes: 7, delivered: true } if stream == active
        ));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Data { stream, bytes, fin: true }
                if stream == active && bytes == Bytes::from_static(b"last")
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Closed { code: None }
        ));
        assert_eq!(lifecycle.counts(), (0, 0, 0, 0));
        assert!(matches!(lifecycle.next(), Step::Finished));
    }

    #[test]
    fn internal_terminal_messages_stop_at_need_input_instead_of_spinning() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        lifecycle.insert_bidi(id, (), ());
        lifecycle.push(Incoming::RecvStopped {
            stream: id,
            code: ErrorCode::new(0x11),
        });

        assert!(matches!(lifecycle.next(), Step::NeedInput));
    }

    #[test]
    fn reset_on_unidirectional_stream_is_forwarded() {
        let mut lifecycle = Lifecycle::<(), ()>::new();
        let id = stream(3);
        let code = ErrorCode::new(0x11);
        lifecycle.push(Incoming::Reset { stream: id, code });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Reset { stream, code: actual } if stream == id && actual == code
        ));
    }

    #[test]
    fn late_stop_and_reset_notifications_preserve_first_terminal_outcomes() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        lifecycle.insert_bidi(id, (), ());
        assert!(lifecycle.finish_send(id, None));
        lifecycle.push(Incoming::StopSending {
            stream: id,
            code: ErrorCode::new(0x11),
        });
        lifecycle.push(Incoming::Data {
            stream: id,
            bytes: Bytes::new(),
            fin: true,
        });
        lifecycle.push(Incoming::RecvStopped {
            stream: id,
            code: ErrorCode::new(0x22),
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Data { stream, fin: true, .. } if stream == id
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: None,
                tx_code: None,
            } if stream == id
        ));
        assert!(matches!(lifecycle.next(), Step::NeedInput));
    }

    #[test]
    fn peer_stop_sending_is_reported_and_closes_after_receive_error() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        let tx = ErrorCode::new(0x11);
        let rx = ErrorCode::new(0x22);
        lifecycle.insert_bidi(id, (), ());
        lifecycle.push(Incoming::StopSending {
            stream: id,
            code: tx,
        });
        lifecycle.push(Incoming::Reset {
            stream: id,
            code: rx,
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StopSending { stream, code } if stream == id && code == tx
        ));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Reset { stream, code } if stream == id && code == rx
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: Some(actual_rx),
                tx_code: Some(actual_tx),
            } if stream == id && actual_rx == rx && actual_tx == tx
        ));
    }

    #[test]
    fn send_error_first_preserves_both_direction_codes() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        let tx = ErrorCode::new(0x11);
        let rx = ErrorCode::new(0x22);
        lifecycle.insert_bidi(id, (), ());
        lifecycle.push(Incoming::SendStopped {
            stream: id,
            code: Some(tx),
        });
        lifecycle.push(Incoming::Reset {
            stream: id,
            code: rx,
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Reset { stream, code } if stream == id && code == rx
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: Some(actual_rx),
                tx_code: Some(actual_tx),
            } if stream == id && actual_rx == rx && actual_tx == tx
        ));
    }

    #[test]
    fn peer_reset_wins_a_later_local_stop_race() {
        let mut lifecycle = Lifecycle::new();
        let id = stream(0);
        let peer = ErrorCode::new(0x11);
        lifecycle.insert_bidi(id, (), ());
        lifecycle.finish_send(id, None);
        lifecycle.push(Incoming::Reset {
            stream: id,
            code: peer,
        });
        lifecycle.push(Incoming::RecvStopped {
            stream: id,
            code: ErrorCode::new(0x22),
        });

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Reset { stream, code } if stream == id && code == peer
        ));
        assert!(matches!(lifecycle.next(), Step::Boundary));
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed {
                stream,
                rx_code: Some(actual),
                tx_code: None,
            } if stream == id && actual == peer
        ));
        assert!(matches!(lifecycle.next(), Step::NeedInput));
    }

    #[test]
    fn refused_write_after_finish_cannot_fabricate_a_send_error() {
        let mut streams = Streams::new();
        let id = stream(0);
        streams.insert_bidi(id, (), ());
        assert!(streams.finish_send(id, None));
        assert!(streams.send_finished(id));
        assert!(!streams.finish_send(id, Some(ErrorCode::new(0x11))));
        assert!(streams.finish_recv(id, None));
        assert_eq!(
            streams.pop_close(),
            Some(Closed {
                stream: id,
                rx_code: None,
                tx_code: None,
            })
        );
    }

    #[test]
    fn continuously_arriving_unrelated_releases_cannot_starve_close() {
        let mut lifecycle = Lifecycle::new();
        let closing = stream(0);
        let other = stream(4);
        lifecycle.insert_bidi(closing, (), ());
        lifecycle.insert_bidi(other, (), ());
        lifecycle.release(closing, 1);
        lifecycle.finish_send(closing, None);
        lifecycle.streams.finish_recv(closing, None);

        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::Released { stream, bytes: 1, .. } if stream == closing
        ));
        lifecycle.release(other, 1);
        assert!(matches!(lifecycle.next(), Step::Boundary));
        for _ in 0..100 {
            lifecycle.release(other, 1);
        }
        assert!(matches!(
            event(&mut lifecycle),
            QuicEvent::StreamClosed { stream, .. } if stream == closing
        ));
    }
}
