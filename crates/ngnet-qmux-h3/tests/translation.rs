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

use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use std::io::IoSlice;
use std::sync::Arc;
use std::task::Wake;

use ngnet_h3::ErrorCode;
use ngnet_h3::StreamId;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
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
}

impl Pair {
    fn new() -> Self {
        let (client_io, server_io) = stream_pair();
        let clock = TestClock::new();
        Self {
            client: QmuxConnection::client(client_io, clock.clone()).expect("a client"),
            server: QmuxConnection::server(server_io, clock).expect("a server"),
            flag: Arc::new(Woken::default()),
            seen: (Vec::new(), Vec::new()),
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
                Poll::Pending => break,
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
                Poll::Pending => self.settle(),
            }
        }
        panic!("a stream never opened");
    }

    /// Offers what `source` has, once.
    fn transmit(&mut self, of: Side, source: &mut Offer) {
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

/// Pushes everything a source holds through, pumping between passes.
fn send_all(pair: &mut Pair, of: Side, source: &mut Offer) {
    for _ in 0..64 {
        pair.transmit(of, source);
        pair.settle();
        if source.pending.is_empty() {
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
    let uni = pair.open(Side::Client, false);
    let bidi = pair.open(Side::Client, true);

    let mut source = Offer::new(vec![
        (uni, b"on the unidirectional stream".to_vec(), false),
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
        data_on(server, uni),
        b"on the unidirectional stream",
        "the unidirectional stream surfaces through its data alone",
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
