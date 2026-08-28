//! What this crate promises the HTTP/3 layer, checked against the QMux layer itself.
//!
//! These are translation tests, not protocol ones: no HTTP/3 driver runs here. Two QMux
//! connections are wired to each other over an in-memory byte stream and driven through the
//! [`QuicConnection`] surface directly, so what a test observes is exactly what the HTTP/3
//! layer would have observed and nothing is inferred from a request completing.
//!
//! The driving is deliberately explicit. Each connection is polled through a waker that
//! records whether it was woken, and events are collected until the connection is both
//! pending and has not asked to be polled again — because [`QuicConnection::poll_event`]
//! returns pending *with a wake* to start a fresh batch, and a loop that stopped at the
//! first pending would miss every event after a stream ended.

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use std::io::IoSlice;
use std::sync::Arc;
use std::task::Wake;

use ngnet_h3::ErrorCode;
use ngnet_h3::StreamId;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_qmux::io::AsyncByteStream;
use ngnet_qmux::io::testing::{Fault, TestByteStream, TestClock, WriteLog, stream_pair};
#[cfg(debug_assertions)]
use ngnet_qmux::io::testing::{FaultControl, ReadLog};
use ngnet_qmux_h3::QmuxConnection;

type Endpoint = QmuxConnection<TestByteStream, TestClock>;

/// A waker that records having been woken, and nothing else.
#[derive(Default)]
struct Woken(AtomicBool);

impl Woken {
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::SeqCst)
    }
}

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_flush_wakes_once_for_a_new_ending_and_never_spins_on_it() {
    let (client_io, _peer_io) = stream_pair();
    client_io.inject(Fault::Broken);
    let mut connection = QmuxConnection::client(client_io, TestClock::new()).expect("a client");
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    assert!(
        connection.poll_flush(&mut cx).is_ready(),
        "a newly discovered ending is ready for its original operation to report"
    );
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "a newly latched ending needs exactly one continuation"
    );

    match connection.poll_event(&mut cx) {
        Poll::Ready(Err(_)) => {}
        other => panic!("the pending event operation did not report the ending: {other:?}"),
    }
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "reporting the ending must not add a courtesy wake"
    );

    assert!(connection.poll_flush(&mut cx).is_ready());
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "an already latched ending must not self-wake into a spin"
    );
}

#[test]
fn a_suspension_flush_parks_on_backpressure_and_finishes_after_its_wake() {
    let (client_io, mut peer_io) = stream_pair();
    client_io.set_capacity(Some(1));
    #[cfg(debug_assertions)]
    let reads = client_io.read_log();
    let mut connection = QmuxConnection::client(client_io, TestClock::new()).expect("a client");
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    // The productive event poll may build the transport announcement, but it must leave the
    // sub-ceiling tail for the explicit suspension flush.
    #[cfg(debug_assertions)]
    let pumps = connection.pump_calls();
    assert!(connection.poll_event(&mut cx).is_pending());
    #[cfg(debug_assertions)]
    {
        assert_eq!(
            connection.pump_calls() - pumps,
            1,
            "write backpressure must not duplicate the initial event pump"
        );
        assert_eq!(
            reads.reads(),
            1,
            "the write-blocked pump must still reach and register its pending read"
        );
    }
    assert!(
        connection.poll_flush(&mut cx).is_pending(),
        "a one-byte substrate cannot take the whole announcement"
    );

    let before = wakes.0.load(Ordering::SeqCst);
    let mut byte = [0_u8; 1];
    assert!(matches!(
        peer_io.poll_read(&mut cx, &mut byte),
        Poll::Ready(Ok(1))
    ));
    assert!(
        wakes.0.load(Ordering::SeqCst) > before,
        "draining the backed-up substrate must wake the pending flush"
    );

    for _ in 0..256 {
        match connection.poll_flush(&mut cx) {
            Poll::Ready(Ok(())) => return,
            Poll::Ready(Err(error)) => panic!("the flush failed: {error}"),
            Poll::Pending => {
                let _ = peer_io.poll_read(&mut cx, &mut byte);
            }
        }
    }
    panic!("the short-accepting substrate never drained the buffered output");
}

/// One initial pump serves the empty event branch and leaves a real read wake behind.
#[cfg(debug_assertions)]
#[test]
fn an_empty_event_poll_uses_one_pump_and_one_transport_read() {
    let (client_io, mut peer_io) = stream_pair();
    let reads = client_io.read_log();
    let mut connection = QmuxConnection::client(client_io, TestClock::new()).expect("a client");
    let flag = Arc::new(Woken::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);

    reads.clear();
    let pumps = connection.pump_calls();
    assert!(connection.poll_event(&mut cx).is_pending());
    assert_eq!(connection.pump_calls() - pumps, 1);
    assert_eq!(reads.reads(), 1);

    assert!(peer_io.poll_write(&mut cx, b"\0").is_ready());
    assert!(
        flag.take(),
        "the single pending read must register the event poll's waker"
    );
}

/// A pair of endpoints that deliver to each other.
struct Pair {
    client: Endpoint,
    server: Endpoint,
    flag: Arc<Woken>,
    /// Everything each end has reported, from the beginning.
    ///
    /// Kept rather than returned from each drain because driving the pair is what moves
    /// bytes: a test that opened a stream or pushed a payload would otherwise have thrown
    /// away the events that arrived while it was doing so.
    seen: (Vec<QuicEvent>, Vec<QuicEvent>),
    /// What the client's byte stream was asked to write.
    ///
    /// The instrument for "this offer produced no record". Nothing else at this level can
    /// tell an offer that produced nothing from one that produced a record nobody looked at:
    /// both leave the peer with no data event, and only one of them costs a record on the
    /// wire. Cleared by the test that measures, because the transport-parameter announcement
    /// and any stream opening happened before its window began.
    log: WriteLog,
    #[cfg(debug_assertions)]
    reads: (ReadLog, ReadLog),
    #[cfg(debug_assertions)]
    faults: (FaultControl, FaultControl),
}

impl Pair {
    fn new() -> Self {
        let (client_io, server_io) = stream_pair();
        let clock = TestClock::new();
        // Taken before the connection is built: a connection takes its byte stream by value
        // and never gives it back, and construction already schedules an announcement.
        let log = client_io.write_log();
        #[cfg(debug_assertions)]
        let reads = (client_io.read_log(), server_io.read_log());
        #[cfg(debug_assertions)]
        let faults = (client_io.fault_control(), server_io.fault_control());
        Self {
            client: QmuxConnection::client(client_io, clock.clone()).expect("a client"),
            server: QmuxConnection::server(server_io, clock).expect("a server"),
            flag: Arc::new(Woken::default()),
            seen: (Vec::new(), Vec::new()),
            log,
            #[cfg(debug_assertions)]
            reads,
            #[cfg(debug_assertions)]
            faults,
        }
    }

    fn context(&self) -> (Waker, Arc<Woken>) {
        let flag = Arc::clone(&self.flag);
        (Waker::from(Arc::clone(&flag)), flag)
    }

    /// Collects everything one end has to report, moving bytes as it goes.
    ///
    /// `poll_event` pumps before it looks, so this is also what carries records between the
    /// two ends: there is no separate delivery step to forget.
    fn drain(&mut self, of: Side) {
        let (waker, flag) = self.context();
        let mut cx = Context::from_waker(&waker);
        let end = match of {
            Side::Client => &mut self.client,
            Side::Server => &mut self.server,
        };
        let seen = match of {
            Side::Client => &mut self.seen.0,
            Side::Server => &mut self.seen.1,
        };
        flag.take();
        loop {
            match end.poll_event(&mut cx) {
                Poll::Ready(Ok(event)) => seen.push(event),
                Poll::Ready(Err(error)) => panic!("the connection failed: {error}"),
                // A pending with a wake is the batching rule asking to be polled again, not
                // the connection running out of things to say.
                Poll::Pending if flag.take() => continue,
                Poll::Pending => {
                    match end.poll_flush(&mut cx) {
                        Poll::Ready(Ok(())) | Poll::Pending => {}
                        Poll::Ready(Err(error)) => {
                            panic!("flushing before the event consumer stopped: {error}")
                        }
                    }
                    break;
                }
            }
        }
    }

    /// Drives both ends until neither has anything more to do.
    fn settle(&mut self) {
        for _ in 0..8 {
            self.drain(Side::Client);
            self.drain(Side::Server);
        }
    }

    /// Everything one end has reported so far.
    fn seen(&self, of: Side) -> &[QuicEvent] {
        match of {
            Side::Client => &self.seen.0,
            Side::Server => &self.seen.1,
        }
    }

    /// Opens a stream from one end, once the peer's limits have arrived.
    fn open(&mut self, of: Side, bidi: bool) -> StreamId {
        for _ in 0..8 {
            let (waker, _) = self.context();
            let mut cx = Context::from_waker(&waker);
            let end = match of {
                Side::Client => &mut self.client,
                Side::Server => &mut self.server,
            };
            let opened = if bidi {
                end.poll_open_bi(&mut cx)
            } else {
                end.poll_open_uni(&mut cx)
            };
            match opened {
                Poll::Ready(Ok(stream)) => return stream,
                Poll::Ready(Err(error)) => panic!("could not open a stream: {error}"),
                // Waiting on the peer's transport parameters, which arrive only if
                // something reads.
                Poll::Pending => {
                    match end.poll_flush(&mut cx) {
                        Poll::Ready(Ok(())) | Poll::Pending => {}
                        Poll::Ready(Err(error)) => {
                            panic!("flushing before the open parked: {error}")
                        }
                    }
                    self.settle();
                }
            }
        }
        panic!("a stream never opened");
    }

    /// Offers what `source` has, once.
    fn transmit(&mut self, of: Side, source: &mut impl Drivable) {
        let (waker, _) = self.context();
        let mut cx = Context::from_waker(&waker);
        let end = match of {
            Side::Client => &mut self.client,
            Side::Server => &mut self.server,
        };
        assert!(
            end.poll_transmit(&mut cx, source).is_ready(),
            "a transmit pass must not park: it is called from a driver that has no way to \
             wait for one",
        );
    }
}

#[derive(Clone, Copy)]
enum Side {
    Client,
    Server,
}

/// A stream source with a fixed queue of offers, which records what was accepted.
///
/// Stands in for the HTTP/3 driver's own source. The accepted counts it keeps are the other
/// half of the release accounting: a test compares them against the `Released` events the
/// connection reported, and any disagreement in either direction is the bug.
struct Offer {
    pending: Vec<(StreamId, Vec<u8>, bool)>,
    accepted: Vec<(StreamId, u64)>,
    blocked: bool,
    gone: bool,
}

impl Offer {
    fn new(offers: Vec<(StreamId, Vec<u8>, bool)>) -> Self {
        Self {
            pending: offers,
            accepted: Vec::new(),
            blocked: false,
            gone: false,
        }
    }

    fn one(stream: StreamId, data: &[u8], fin: bool) -> Self {
        Self::new(vec![(stream, data.to_vec(), fin)])
    }

    fn total(&self) -> u64 {
        self.accepted.iter().map(|(_, bytes)| bytes).sum()
    }
}

impl StreamSource for Offer {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        let Some((stream, data, fin)) = self.pending.first().cloned() else {
            return false;
        };
        let slices = [IoSlice::new(&data)];
        match write(stream, &slices, fin) {
            WriteOutcome::Accepted(taken) => {
                if taken > 0 {
                    self.accepted.push((stream, taken as u64));
                }
                let front = &mut self.pending[0];
                front.1.drain(..taken);
                if front.1.is_empty() {
                    self.pending.remove(0);
                }
                true
            }
            WriteOutcome::Blocked => {
                self.blocked = true;
                false
            }
            WriteOutcome::Gone => {
                self.gone = true;
                self.pending.remove(0);
                true
            }
        }
    }
}

/// A stream source a test can drive to exhaustion.
///
/// [`StreamSource`] says whether there may be more *now*; a test needs to know whether there
/// is anything left at all, which is the source's own business and not the transport's.
trait Drivable: StreamSource {
    /// Whether everything the source held has been offered and taken.
    fn drained(&self) -> bool;
}

impl Drivable for Offer {
    fn drained(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Pushes everything a source holds through, pumping between passes.
fn send_all(pair: &mut Pair, of: Side, source: &mut impl Drivable) {
    for _ in 0..64 {
        pair.transmit(of, source);
        pair.settle();
        if source.drained() {
            return;
        }
    }
    panic!("the offers never drained");
}

fn data_on(events: &[QuicEvent], stream: StreamId) -> Vec<u8> {
    events
        .iter()
        .filter_map(|event| match event {
            QuicEvent::Data {
                stream: id, bytes, ..
            } if *id == stream => Some(bytes.to_vec()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn accepted(events: &[QuicEvent]) -> Vec<StreamId> {
    events
        .iter()
        .filter_map(|event| match event {
            QuicEvent::Accepted { stream } => Some(*stream),
            _ => None,
        })
        .collect()
}

/// SC-024. A peer-opened bidirectional stream is announced; a unidirectional one is not.
///
/// The suppression is not an optimisation. nghttp3 reads the HTTP/3 stream-type prefix off a
/// unidirectional stream itself to discover whether it is a control stream or a QPACK one,
/// and an `Accepted` event for it would have the layer treat it as a request to answer --
/// writing a response onto a stream the peer will never read.
#[test]
fn only_peer_opened_bidirectional_streams_are_announced() {
    let mut pair = Pair::new();
    let first_uni = pair.open(Side::Client, false);
    let second_uni = pair.open(Side::Client, false);
    let bidi = pair.open(Side::Client, true);

    let mut source = Offer::new(vec![
        (
            first_uni,
            b"on the first unidirectional stream".to_vec(),
            false,
        ),
        (
            second_uni,
            b"on the second unidirectional stream".to_vec(),
            false,
        ),
        (bidi, b"on the bidirectional stream".to_vec(), false),
    ]);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();
    let server = pair.seen(Side::Server);

    assert_eq!(
        accepted(server),
        vec![bidi],
        "exactly the peer's bidirectional stream is announced",
    );
    assert_eq!(
        data_on(server, first_uni),
        b"on the first unidirectional stream",
        "the first untranslated stream-open event is filtered without stalling",
    );
    assert_eq!(
        data_on(server, second_uni),
        b"on the second unidirectional stream",
        "filtering another untranslated event still progresses",
    );
    assert_eq!(
        data_on(server, bidi),
        b"on the bidirectional stream",
        "and the bidirectional one carries its data as well as its announcement",
    );
}

/// A zero-length delivery carrying end-of-stream reaches the layer.
///
/// It is how a peer that has already sent everything ends a stream, and a transport that
/// filtered empty deliveries would leave the request body of every such request unfinished.
#[test]
fn an_empty_final_delivery_is_not_swallowed() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);

    let mut body = Offer::one(stream, b"a body", false);
    send_all(&mut pair, Side::Client, &mut body);
    pair.settle();
    assert_eq!(data_on(pair.seen(Side::Server), stream), b"a body");

    let mut end = Offer::one(stream, b"", true);
    send_all(&mut pair, Side::Client, &mut end);
    pair.settle();
    let server = pair.seen(Side::Server);

    assert!(
        server.iter().any(|event| matches!(
            event,
            QuicEvent::Data { stream: id, bytes, fin: true } if *id == stream && bytes.is_empty()
        )),
        "the end of the stream must arrive on its own: {server:?}",
    );
}

#[test]
fn final_data_and_stream_close_are_separated_by_a_woken_boundary() {
    for expected in [b"last response bytes".as_slice(), b"".as_slice()] {
        let mut pair = Pair::new();
        let stream = pair.open(Side::Client, true);

        let mut request = Offer::one(stream, b"request", true);
        send_all(&mut pair, Side::Client, &mut request);

        // Do not settle after this transmit: the test has to observe the exact public boundary
        // between the final data and the close rather than only their eventual order.
        let mut response = Offer::one(stream, expected, true);
        pair.transmit(Side::Server, &mut response);
        let (waker, flag) = pair.context();
        let mut cx = Context::from_waker(&waker);
        assert!(pair.server.poll_flush(&mut cx).is_ready());

        loop {
            match pair.client.poll_event(&mut cx) {
                Poll::Ready(Ok(QuicEvent::Data {
                    stream: id,
                    bytes,
                    fin: true,
                })) if id == stream => {
                    assert_eq!(&bytes[..], expected);
                    break;
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => panic!("the connection failed: {error}"),
                Poll::Pending if flag.take() => {}
                Poll::Pending => panic!("the final response data never arrived"),
            }
        }

        flag.take();
        assert!(
            pair.client.poll_event(&mut cx).is_pending(),
            "a close in the final data's batch would be applied before those bytes"
        );
        assert!(
            flag.take(),
            "the correctness boundary must schedule the batch which carries the close"
        );
        #[cfg(debug_assertions)]
        let pumps = pair.client.pump_calls();
        #[cfg(debug_assertions)]
        pair.reads.0.clear();
        assert!(matches!(
            pair.client.poll_event(&mut cx),
            Poll::Ready(Ok(QuicEvent::StreamClosed {
                stream: id,
                ..
            })) if id == stream
        ));
        #[cfg(debug_assertions)]
        {
            assert_eq!(
                pair.client.pump_calls() - pumps,
                1,
                "a held translated event still performs exactly one initial pump"
            );
            assert_eq!(
                pair.reads.0.reads(),
                1,
                "the held-event pump reaches the transport once"
            );
        }
    }
}

/// A queued release is served after one pump, including when that pump discovers an error.
#[cfg(debug_assertions)]
#[test]
fn a_queued_release_uses_one_pump_and_precedes_a_new_terminal_error() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"queued for release", false);
    pair.transmit(Side::Client, &mut source);
    assert!(source.drained());

    pair.faults.0.inject(Fault::Broken);
    pair.reads.0.clear();
    let pumps = pair.client.pump_calls();
    let (waker, flag) = pair.context();
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
        pair.client.poll_event(&mut cx),
        Poll::Ready(Ok(QuicEvent::Released {
            stream: id,
            delivered: true,
            ..
        })) if id == stream
    ));
    assert_eq!(
        pair.client.pump_calls() - pumps,
        1,
        "queued releases must not skip or duplicate the initial pump"
    );

    assert!(
        pair.client.poll_event(&mut cx).is_pending(),
        "the ending must start a batch after the queued release"
    );
    assert!(flag.take(), "the ending batch boundary must self-wake");
    assert!(matches!(
        pair.client.poll_event(&mut cx),
        Poll::Ready(Err(_))
    ));
}

/// SC-013. Every accepted byte is released exactly once.
///
/// The assertion is an equality against what the writes actually accepted, which is what
/// makes it fail in both directions. Releasing too little holds the application's buffers
/// for the life of the connection; releasing too much tells the HTTP/3 state machine that
/// bytes it is still reading through have been dealt with, and it frees memory nghttp3 goes
/// on to read. A one-sided assertion -- "at least everything was released" -- would pass
/// while the second bug was live.
#[test]
fn accepted_bytes_are_released_exactly_once() {
    let mut pair = Pair::new();
    let first = pair.open(Side::Client, true);
    let second = pair.open(Side::Client, true);

    // Larger than one record, so the payload is split and several accepts make up each
    // stream's total: the interesting failures are the ones that lose or repeat a fragment.
    let payload: Vec<u8> = (0..200_000_u32).map(|index| index as u8).collect();
    let mut source = Offer::new(vec![
        (first, payload.clone(), true),
        (second, payload.clone(), true),
    ]);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();
    let client = pair.seen(Side::Client);
    let server = pair.seen(Side::Server);

    let released: u64 = client
        .iter()
        .filter_map(|event| match event {
            QuicEvent::Released {
                bytes, delivered, ..
            } => {
                assert!(delivered, "a transport that copies never cancels a send");
                Some(*bytes)
            }
            _ => None,
        })
        .sum();
    assert_eq!(
        released,
        source.total(),
        "released bytes must equal accepted bytes -- no more, no less",
    );
    assert_eq!(
        source.total(),
        2 * payload.len() as u64,
        "and the whole of both payloads must have been accepted",
    );

    for stream in [first, second] {
        let released: u64 = client
            .iter()
            .filter_map(|event| match event {
                QuicEvent::Released {
                    stream: id, bytes, ..
                } if *id == stream => Some(*bytes),
                _ => None,
            })
            .sum();
        assert_eq!(
            released,
            payload.len() as u64,
            "the release must be attributed to the stream that carried the bytes",
        );
        assert_eq!(
            data_on(server, stream).len(),
            payload.len(),
            "and the peer must receive each byte once, which is what over-reporting breaks",
        );
    }
}

/// FR-028. A peer's reset reaches the layer with the peer's application error code.
#[test]
fn a_peer_reset_arrives_with_its_code() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"a request", false);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    pair.client
        .reset(stream, ErrorCode::new(0x10c))
        .expect("resetting a live stream");
    pair.settle();
    let server = pair.seen(Side::Server);

    assert!(
        server.iter().any(|event| matches!(
            event,
            QuicEvent::Reset { stream: id, code } if *id == stream && code.get() == 0x10c
        )),
        "the peer's reset and its code must both arrive: {server:?}",
    );
}

/// FR-028. A peer's stop-sending reaches the layer with the peer's application error code.
#[test]
fn a_peer_stop_sending_arrives_with_its_code() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"a request", false);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    pair.client
        .stop_sending(stream, ErrorCode::new(0x10d))
        .expect("stopping a live stream");
    pair.settle();
    let server = pair.seen(Side::Server);

    assert!(
        server.iter().any(|event| matches!(
            event,
            QuicEvent::StopSending { stream: id, code } if *id == stream && code.get() == 0x10d
        )),
        "the peer's stop-sending and its code must both arrive: {server:?}",
    );
}

/// FR-028. A stream closing reports a code per direction, and keeps them apart.
///
/// The two halves fail independently and for different reasons -- this end refused to read
/// any more, the peer refused to send any more -- so a translation that collapsed them into
/// one code would tell the HTTP/3 layer that a request it abandoned had been rejected by the
/// server, or the reverse.
#[test]
fn a_stream_close_carries_a_code_for_each_direction() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"a request", false);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    pair.client
        .reset(stream, ErrorCode::new(0x111))
        .expect("resetting the write half");
    pair.client
        .stop_sending(stream, ErrorCode::new(0x222))
        .expect("stopping the read half");
    pair.settle();
    let client = pair.seen(Side::Client);

    let closed = client
        .iter()
        .find_map(|event| match event {
            QuicEvent::StreamClosed {
                stream: id,
                rx_code,
                tx_code,
            } if *id == stream => Some((*rx_code, *tx_code)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the stream must be reported closed: {client:?}"));

    assert_eq!(
        closed.1.map(ErrorCode::get),
        Some(0x111),
        "the write half carries the code this end reset with",
    );
    assert_eq!(
        closed.0.map(ErrorCode::get),
        Some(0x222),
        "and the read half the code it stopped sending with",
    );
}

/// A close asked for through the HTTP/3 surface reaches the peer, carrying its code.
///
/// `close` cannot write: it has no context to park on. So it records, and the tail --
/// `poll_finish` -- writes. This test exists because the failure it guards against is
/// silent: a close that is encoded and never written looks exactly like success from the
/// closing end, and the peer simply waits out an idle timeout.
#[test]
fn a_close_reaches_the_peer_with_its_code() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"a request", true);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    pair.client
        .close(ErrorCode::new(0x100), b"no error")
        .expect("recording the close");

    let (waker, _) = pair.context();
    let mut cx = Context::from_waker(&waker);
    assert!(
        pair.client.poll_finish(&mut cx).is_ready(),
        "the tail must finish once the byte stream has taken the close",
    );

    pair.settle();
    let server = pair.seen(Side::Server);
    assert!(
        server.iter().any(|event| matches!(
            event,
            QuicEvent::Closed { code: Some(code) } if code.get() == 0x100
        )),
        "the peer must be told the connection closed, and with which code: {server:?}",
    );
}

/// An orderly ending is an event; the connection is not failed for it.
///
/// A peer that hangs up politely once its exchanges are done is the ordinary end of a
/// connection. Reporting it as an error would turn every well-behaved client's disconnection
/// into a server-side protocol failure.
#[test]
fn a_peer_that_closes_is_an_event_and_not_a_failure() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    let mut source = Offer::one(stream, b"a request", true);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    pair.client
        .close(ErrorCode::new(0), b"")
        .expect("recording the close");
    let (waker, _) = pair.context();
    let mut cx = Context::from_waker(&waker);
    assert!(pair.client.poll_finish(&mut cx).is_ready());

    // `drain` panics on a failure, so reaching the assertion at all is half the claim.
    pair.drain(Side::Server);
    let server = pair.seen(Side::Server);
    assert!(
        server
            .iter()
            .any(|event| matches!(event, QuicEvent::Closed { .. })),
        "the ending must surface as a close: {server:?}",
    );
}

/// A stream source that lends its payload in several fragments, as the driver's own does.
///
/// [`Offer`] lends one slice, which is the shape almost every test here wants. This one exists
/// for the shape the HTTP/3 layer actually produces: `StreamSource::write_next` hands over a
/// stream's pending output as a *list* -- a request's encoded headers and the first of its
/// body, most often -- and what those fragments cost is the whole of Spec FR-010.
struct Fragmented {
    stream: StreamId,
    /// What is still to be lent, as separate fragments.
    fragments: Vec<Vec<u8>>,
    fin: bool,
    finished: bool,
    /// How many times the source was asked for something.
    ///
    /// The count Spec SC-009 is stated over: the claim is that a multi-fragment offer becomes
    /// as few records as the record size allows, and the first thing that has to be true for
    /// that is that the whole list goes down in one call.
    calls: usize,
    accepted: Vec<(StreamId, u64)>,
    /// Whether to overwrite the fragments as soon as the call that lent them returns.
    ///
    /// Spec SC-010. A source is entitled to do this: `RETAINS_BUFFERS` is false, which is a
    /// promise that a write copies what it takes before it returns.
    scrub: bool,
}

impl Fragmented {
    fn new(stream: StreamId, fragments: &[&[u8]], fin: bool) -> Self {
        Self {
            stream,
            fragments: fragments.iter().map(|f| f.to_vec()).collect(),
            fin,
            finished: false,
            calls: 0,
            accepted: Vec::new(),
            scrub: false,
        }
    }

    fn scrubbing(mut self) -> Self {
        self.scrub = true;
        self
    }

    fn total(&self) -> u64 {
        self.accepted.iter().map(|(_, bytes)| bytes).sum()
    }

    /// Drops the first `taken` bytes of the list, across fragments.
    fn consume(&mut self, mut taken: usize) {
        while taken > 0 {
            let Some(front) = self.fragments.first_mut() else {
                panic!("more bytes were taken than were lent");
            };
            let step = taken.min(front.len());
            front.drain(..step);
            taken -= step;
            if front.is_empty() {
                self.fragments.remove(0);
            }
        }
    }
}

impl Drivable for Fragmented {
    fn drained(&self) -> bool {
        self.finished
    }
}

impl StreamSource for Fragmented {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        if self.finished {
            return false;
        }
        self.calls += 1;
        let outcome = {
            let slices: Vec<IoSlice<'_>> = self.fragments.iter().map(|f| IoSlice::new(f)).collect();
            write(self.stream, &slices, self.fin)
        };
        match outcome {
            WriteOutcome::Accepted(taken) => {
                if taken > 0 {
                    self.accepted.push((self.stream, taken as u64));
                }
                self.consume(taken);
                if self.scrub {
                    // The lender reclaims what it lent, the instant the call returns and long
                    // before the bytes reach the byte stream. Anything that kept the pointers
                    // rather than the bytes puts this on the wire.
                    for fragment in &mut self.fragments {
                        fragment.fill(0xff);
                    }
                }
                let empty = self.fragments.iter().all(Vec::is_empty);
                if empty {
                    self.finished = true;
                }
                true
            }
            WriteOutcome::Blocked => false,
            WriteOutcome::Gone => {
                self.finished = true;
                true
            }
        }
    }
}

/// Spec SC-009. A list of fragments goes down in one call and is taken whole.
///
/// The property the vectored write exists for, stated where the HTTP/3 layer can see it. Each
/// fragment used to be a call of its own -- the join looped over the slices -- and a call is
/// where a record begins, so a three-fragment offer cost three records however small they
/// were. Now the list is one call, and what it costs on the wire is pinned in
/// `crates/ngnet-qmux/tests/io_vectored.rs` and `tests/ngnet-qmux-h3-tests/tests/fragmented_offers.rs`.
///
/// The release accounting is asserted alongside, because that is what the change could have
/// broken quietly: the count returned to the source is a total across the fragments, and a
/// release derived from anywhere else would agree with it on the easy cases and not on these.
#[test]
fn a_list_of_fragments_is_offered_once_and_taken_whole() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);

    let fragments: [&[u8]; 3] = [b"the headers, ", b"then the body, ", b"then a little more"];
    let expected: Vec<u8> = fragments.concat();
    let mut source = Fragmented::new(stream, &fragments, false);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    assert_eq!(
        source.calls, 1,
        "the three fragments took {} calls, so the join is still writing one slice at a time \
         and every fragment boundary is still a record boundary",
        source.calls
    );
    assert_eq!(
        source.total(),
        expected.len() as u64,
        "the offer was not taken whole"
    );
    assert_eq!(
        data_on(pair.seen(Side::Server), stream),
        expected,
        "the peer received the fragments in some other order, or received some of them twice \
         -- which is what a resumption walking the list against a total gets wrong, and what \
         nothing above this layer would notice"
    );
    let released: u64 = pair
        .seen(Side::Client)
        .iter()
        .filter_map(|event| match event {
            QuicEvent::Released {
                stream: id, bytes, ..
            } if *id == stream => Some(*bytes),
            _ => None,
        })
        .sum();
    assert_eq!(
        released,
        expected.len() as u64,
        "the bytes released do not match the bytes accepted; the release and the count the \
         source was given have stopped coming from the same total"
    );
}

/// Spec SC-010. A source may reclaim its fragments as soon as the call returns.
///
/// `ngnet-qmux-h3` declares `RETAINS_BUFFERS = false`, which tells the HTTP/3 layer the bytes
/// are its own again the moment a write returns. dwnx copies each vector into the record
/// during the call and retains only the destination buffer, so the declaration holds for the
/// vectored form as well -- but it holds by inspection of C, and this is the check that the
/// inspection was right.
#[test]
fn a_source_may_invalidate_its_fragments_once_the_write_returns() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);

    let fragments: [&[u8]; 4] = [b"first ", b"second ", b"third ", b"fourth"];
    let expected: Vec<u8> = fragments.concat();
    let mut source = Fragmented::new(stream, &fragments, false).scrubbing();
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    assert_eq!(
        data_on(pair.seen(Side::Server), stream),
        expected,
        "the peer received something other than what was lent, so a fragment was read after \
         the call that lent it had returned"
    );
}

/// Spec SC-033. An offer with nothing in it produces a record only for its end-of-stream.
///
/// Three offers that a caller cannot tell apart and the transport must. Two carry no bytes and
/// no marker, and must cost nothing at all -- no record, and no refusal either, because a
/// refusal has the driver stand the stream down and offer the same nothing again on its next
/// pass. The third carries no bytes and a marker, and must produce its record: it is the only
/// way a stream that has finished writing is ever ended, and a peer that never receives it
/// waits out an idle timeout for a body it already has.
///
/// This is why the short-circuit in `crates/ngnet-qmux-h3/src/transmit.rs` is conditioned on
/// the *absence* of the marker rather than on the offer being empty.
#[test]
fn an_empty_offer_costs_a_record_only_when_it_carries_the_end_of_stream() {
    for fragments in [&[][..], &[&b""[..], &b""[..]][..]] {
        let mut pair = Pair::new();
        let stream = pair.open(Side::Client, true);
        pair.settle();
        pair.log.clear();

        let mut source = Fragmented::new(stream, fragments, false);
        pair.transmit(Side::Client, &mut source);

        assert_eq!(
            source.total(),
            0,
            "an offer with no bytes in it reported bytes taken"
        );
        assert_eq!(
            pair.log.writes(),
            0,
            "an offer of {} empty fragments and no end-of-stream produced {:?} on the wire, so \
             a record was built to carry nothing",
            fragments.len(),
            pair.log.lengths()
        );
    }

    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);
    pair.settle();
    pair.log.clear();

    let mut source = Fragmented::new(stream, &[b"", b""], true);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    assert!(
        pair.log.writes() > 0,
        "an end-of-stream marker on an otherwise empty offer produced nothing on the wire"
    );
    assert!(
        pair.seen(Side::Server).iter().any(|event| matches!(
            event,
            QuicEvent::Data { stream: id, bytes, fin: true } if *id == stream && bytes.is_empty()
        )),
        "the stream never ended at the peer: {:?}",
        pair.seen(Side::Server)
    );
}

/// A trailing empty fragment does not take the end-of-stream marker away from the payload.
///
/// The hazard the per-slice loop had to work around by hand. With a call per slice the marker
/// rode the *last* slice, so a trailing empty one had to be found and skipped, or the marker
/// went out on an offer of nothing while the payload before it was still being written. There
/// is no index to compute now -- an empty fragment is not submitted at all, and dwnx applies
/// the marker to the record that takes the last byte -- and this is the guard on that
/// reasoning still holding.
#[test]
fn a_trailing_empty_fragment_does_not_take_the_end_of_stream_marker() {
    let mut pair = Pair::new();
    let stream = pair.open(Side::Client, true);

    let mut source = Fragmented::new(stream, &[b"a body", b""], true);
    send_all(&mut pair, Side::Client, &mut source);
    pair.settle();

    let server = pair.seen(Side::Server);
    assert_eq!(
        data_on(server, stream),
        b"a body",
        "the payload did not arrive whole"
    );
    assert!(
        server.iter().any(|event| matches!(
            event,
            QuicEvent::Data { stream: id, fin: true, .. } if *id == stream
        )),
        "the stream never ended, so the marker went somewhere other than the record that took \
         the last byte: {server:?}"
    );
}
