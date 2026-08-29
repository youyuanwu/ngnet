//! Private stream ownership and bidirectional lifecycle tracking.

use std::collections::{HashMap, HashSet, VecDeque};

use ngnet_h3::{ErrorCode, StreamId};

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
    sends: HashMap<i64, Handle>,
    finished: HashSet<i64>,
    bidis: HashMap<i64, Bidi<Stop>>,
    closing: VecDeque<Closed>,
}

impl<Handle, Stop> Streams<Handle, Stop> {
    pub(crate) fn new() -> Self {
        Self {
            sends: HashMap::new(),
            finished: HashSet::new(),
            bidis: HashMap::new(),
            closing: VecDeque::new(),
        }
    }

    pub(crate) fn insert_uni(&mut self, stream: StreamId, send: Handle) {
        self.sends.insert(stream.get(), send);
    }

    pub(crate) fn insert_bidi(&mut self, stream: StreamId, send: Handle, stop: Stop) {
        self.sends.insert(stream.get(), send);
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
        self.sends.get_mut(&stream.get())
    }

    pub(crate) fn send_finished(&self, stream: StreamId) -> bool {
        self.finished.contains(&stream.get())
    }

    /// Records the first terminal send outcome.
    ///
    /// Returns whether this call changed the direction.
    pub(crate) fn finish_send(&mut self, stream: StreamId, code: Option<ErrorCode>) -> bool {
        self.finished.insert(stream.get());
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
        self.finished.remove(&closed.stream.get());
        self.bidis.remove(&closed.stream.get());
        Some(closed)
    }

    pub(crate) fn clear(&mut self) {
        self.closing.clear();
        self.bidis.clear();
        self.finished.clear();
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
            self.finished.len(),
            self.bidis.len(),
            self.closing.len(),
        )
    }
}

/// Separates terminal events from events already returned in the current driver batch.
pub(crate) struct BatchGate {
    emitted_since_pending: bool,
}

impl BatchGate {
    pub(crate) fn new() -> Self {
        Self {
            emitted_since_pending: false,
        }
    }

    /// Records that an event was returned to the driver.
    pub(crate) fn emitted(&mut self) {
        self.emitted_since_pending = true;
    }

    /// Records a genuine poll with no event.
    pub(crate) fn pending(&mut self) {
        self.emitted_since_pending = false;
    }

    /// Whether a terminal event must first force a fresh batch.
    pub(crate) fn take_boundary(&mut self) -> bool {
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
}
