//! Scaffolding for exercising the async layer, public but hidden.
//!
//! The crate takes no dev-dependencies by design, and integration tests are separate crates
//! that cannot reach `cfg(test)` items — so the machinery the tests need lives here, marked
//! `#[doc(hidden)]` to keep it out of the documented surface. It is not a supported API and
//! carries no compatibility promise.
//!
//! # The loopback is a second implementation, not a mock
//!
//! [`loopback`] is a complete [`QuicConnection`] pair that moves bytes in memory. It shares
//! no code with any real transport, which is what makes it evidence: if the trait were
//! quietly shaped around one QUIC library, writing this would have been awkward, and it was
//! not. It is also the *sharper* of the two implementations this repository has, because it
//! sets [`QuicConnection::RETAINS_BUFFERS`] to `true` and therefore has to report release
//! explicitly — the case a copying transport never exercises.
//!
//! It is deliberately **not** [`Send`]. That is not an oversight or a simplification: it is
//! how the claim that the trait imposes no `Send` bound gets tested rather than asserted.

use core::future::Future;
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::IoSlice;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use bytes::Bytes;

use super::quic::{QuicConnection, QuicEvent, StreamSource, Timestamp, WriteOutcome};
use crate::error::ErrorCode;
use crate::stream::{Directionality, Initiator, StreamId};

/// The ecosystem crates the async layer is built on, re-exported for tests.
///
/// Integration tests are separate crates and can only reach what this one exposes. The
/// alternative would be dev-dependencies, which the crate deliberately does without.
pub use ::bytes as bytes_crate;
/// See [`bytes_crate`].
pub use ::http as http_crate;
/// See [`bytes_crate`].
pub use ::http_body as http_body_crate;

/// Wakes a parked [`block_on`].
struct Unparker {
    woken: Mutex<bool>,
    signal: Condvar,
}

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().expect("wake flag") = true;
        self.signal.notify_one();
    }
}

/// Drives a future to completion on the calling thread.
///
/// A real waker rather than a no-op one, so a future that returns `Pending` genuinely waits
/// instead of being polled in a spin — which matters here, since several of the properties
/// under test are about *not* being polled.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let unparker = Arc::new(Unparker {
        woken: Mutex::new(false),
        signal: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&unparker));
    let mut context = Context::from_waker(&waker);

    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        let mut woken = unparker.woken.lock().expect("wake flag");
        while !*woken {
            woken = unparker.signal.wait(woken).expect("waiting for a wake");
        }
        *woken = false;
    }
}

/// How many bytes an endpoint will hold before the layer has extended credit for them.
///
/// The initial allowance, in the same spirit as a QUIC flow-control window: enough to get a
/// connection going, small enough that a test can drive past it deliberately.
const INITIAL_CREDIT: u64 = 64 * 1024;

/// The largest run of bytes delivered as one event.
///
/// Real transports hand up bounded chunks rather than whatever was written in one call, and
/// the difference matters here: credit is checked per event, so a single event larger than
/// the whole window could never be delivered at all. Chunking makes the accounting granular
/// enough that running out of credit stalls delivery rather than deadlocking it.
const MAX_CHUNK: usize = 16 * 1024;

/// Which end of a loopback pair an endpoint is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum End {
    Client,
    Server,
}

impl End {
    fn initiator(self) -> Initiator {
        match self {
            Self::Client => Initiator::Client,
            Self::Server => Initiator::Server,
        }
    }
}

/// Knobs a test uses to make the transport behave badly on purpose.
///
/// Shared by both endpoints, because a test setting one usually wants it to apply to the
/// exchange rather than to a direction.
#[derive(Default)]
pub struct Controls {
    accept_at_most: Option<usize>,
    withhold_release: bool,
    undelivered: bool,
    fail_writes_after: Option<usize>,
    writes: usize,
    stalled: Vec<StreamId>,
}

/// A handle for adjusting a loopback pair mid-test.
#[derive(Clone)]
pub struct Knobs(Rc<RefCell<Controls>>);

impl Knobs {
    /// Caps how many bytes a single write may take, forcing short writes.
    pub fn accept_at_most(&self, bytes: usize) {
        self.0.borrow_mut().accept_at_most = Some(bytes);
    }

    /// Stops reporting release, so retained buffers stay retained.
    ///
    /// The sharpest tool here: it is what lets a test prove *when* a buffer is freed rather
    /// than merely that it eventually is.
    pub fn withhold_release(&self) {
        self.0.borrow_mut().withhold_release = true;
    }

    /// Resumes reporting release, and reports everything withheld so far.
    pub fn release_everything(&self) {
        self.0.borrow_mut().withhold_release = false;
    }

    /// Reports subsequent releases as not having reached the peer.
    pub fn report_undelivered(&self) {
        self.0.borrow_mut().undelivered = true;
    }

    /// Fails every write after this many have succeeded.
    pub fn fail_writes_after(&self, writes: usize) {
        self.0.borrow_mut().fail_writes_after = Some(writes);
    }

    /// Refuses to accept anything on a stream, so other streams must make progress instead.
    pub fn stall(&self, stream: StreamId) {
        self.0.borrow_mut().stalled.push(stream);
    }

    /// Lets a stalled stream proceed again.
    pub fn unstall(&self, stream: StreamId) {
        self.0.borrow_mut().stalled.retain(|s| *s != stream);
    }
}

/// One endpoint's incoming side.
#[derive(Default)]
struct Inbox {
    /// Events the endpoint may see now.
    ready: VecDeque<QuicEvent>,
    /// Data events held back because the endpoint has not extended credit for them.
    ///
    /// This is what makes the boundedness obligation in [`QuicConnection::poll_event`]
    /// testable: an endpoint that never extends credit stops receiving.
    held: VecDeque<QuicEvent>,
    /// How many further bytes may be moved from `held` to `ready`.
    credit: u64,
    waker: Option<Waker>,
    closed: bool,
}

impl Inbox {
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Moves as much held data into the ready queue as credit allows.
    ///
    /// Non-data events are never held: a reset must not be stuck behind a flood of body
    /// bytes, which is the fairness property the driver depends on.
    fn promote(&mut self) {
        while let Some(event) = self.held.front() {
            let cost = match event {
                QuicEvent::Data { bytes, .. } => bytes.len() as u64,
                _ => 0,
            };
            if cost > self.credit {
                break;
            }
            self.credit -= cost;
            let event = self.held.pop_front().expect("a front that was just seen");
            self.ready.push_back(event);
        }
    }

    fn deliver(&mut self, event: QuicEvent) {
        match event {
            QuicEvent::Data { stream, bytes, fin } => {
                // Split into bounded runs so credit is checked at a useful granularity. The
                // end marker rides on the last piece, never on an earlier one.
                if bytes.is_empty() {
                    self.held.push_back(QuicEvent::Data { stream, bytes, fin });
                } else {
                    let mut rest = bytes;
                    while !rest.is_empty() {
                        let take = rest.len().min(MAX_CHUNK);
                        let piece = rest.split_to(take);
                        let last = rest.is_empty();
                        self.held.push_back(QuicEvent::Data {
                            stream,
                            bytes: piece,
                            fin: fin && last,
                        });
                    }
                }
            }
            other => self.ready.push_back(other),
        }
        self.promote();
        self.wake();
    }
}

/// An in-memory [`QuicConnection`], one half of a [`loopback`] pair.
///
/// Not `Send`, deliberately — see the module documentation.
pub struct Loopback {
    end: End,
    inbox: Rc<RefCell<Inbox>>,
    peer: Rc<RefCell<Inbox>>,
    controls: Rc<RefCell<Controls>>,
    next_uni: u64,
    next_bi: u64,
    /// Release owed to this endpoint but withheld, per stream.
    withheld: HashMap<StreamId, u64>,
    /// Streams this endpoint may write to.
    ///
    /// Checked rather than assumed. A double that wrote to any identifier it was handed
    /// would let the layer invent one, which is exactly the bug a real QUIC library catches
    /// and an accommodating test double does not.
    writable: Vec<StreamId>,
    /// Bidirectional streams this endpoint has written to, so the peer is told once.
    announced: Vec<StreamId>,
    clock: u64,
}

/// A loopback transport failed.
#[derive(Debug)]
pub struct LoopbackError(&'static str);

impl core::fmt::Display for LoopbackError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "loopback transport: {}", self.0)
    }
}

impl core::error::Error for LoopbackError {}

/// Wires two in-memory endpoints together.
///
/// Returns the client end, the server end, and the knobs that make either misbehave.
pub fn loopback() -> (Loopback, Loopback, Knobs) {
    let client_inbox = Rc::new(RefCell::new(Inbox {
        credit: INITIAL_CREDIT,
        ..Inbox::default()
    }));
    let server_inbox = Rc::new(RefCell::new(Inbox {
        credit: INITIAL_CREDIT,
        ..Inbox::default()
    }));
    let controls = Rc::new(RefCell::new(Controls::default()));

    let client = Loopback {
        end: End::Client,
        inbox: Rc::clone(&client_inbox),
        peer: Rc::clone(&server_inbox),
        controls: Rc::clone(&controls),
        next_uni: 0,
        next_bi: 0,
        withheld: HashMap::new(),
        writable: Vec::new(),
        announced: Vec::new(),
        clock: 0,
    };
    let server = Loopback {
        end: End::Server,
        inbox: server_inbox,
        peer: client_inbox,
        controls: Rc::clone(&controls),
        next_uni: 0,
        next_bi: 0,
        withheld: HashMap::new(),
        writable: Vec::new(),
        announced: Vec::new(),
        clock: 0,
    };

    (client, server, Knobs(controls))
}

impl Loopback {
    /// Reports release for bytes just written, honouring the withholding knob.
    fn release(&mut self, stream: StreamId, bytes: u64) {
        let controls = self.controls.borrow();
        if controls.withhold_release {
            *self.withheld.entry(stream).or_default() += bytes;
            return;
        }
        let delivered = !controls.undelivered;
        drop(controls);
        self.inbox.borrow_mut().deliver(QuicEvent::Released {
            stream,
            bytes,
            delivered,
        });
    }

    /// Flushes anything withheld, once withholding is turned off.
    fn flush_withheld(&mut self) {
        if self.controls.borrow().withhold_release || self.withheld.is_empty() {
            return;
        }
        let delivered = !self.controls.borrow().undelivered;
        let owed: Vec<(StreamId, u64)> = self.withheld.drain().collect();
        let mut inbox = self.inbox.borrow_mut();
        for (stream, bytes) in owed {
            inbox.deliver(QuicEvent::Released {
                stream,
                bytes,
                delivered,
            });
        }
    }
}

impl QuicConnection for Loopback {
    type Error = LoopbackError;

    // The whole point of this implementation: it borrows nothing, so it must be told when a
    // buffer may be freed, which is the case a copying transport never exercises.
    const RETAINS_BUFFERS: bool = true;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        self.flush_withheld();

        let mut inbox = self.inbox.borrow_mut();
        inbox.promote();
        if let Some(event) = inbox.ready.pop_front() {
            // Answering a peer-opened stream means writing to it, so it becomes writable
            // here rather than through `poll_open_bi`, which the peer called, not us.
            if let QuicEvent::Accepted { stream } = &event {
                self.writable.push(*stream);
            }
            drop(inbox);
            return Poll::Ready(Ok(event));
        }
        if inbox.closed {
            return Poll::Ready(Ok(QuicEvent::Closed { code: None }));
        }
        inbox.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        _cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        let mut failed = None;
        let mut written: Vec<(StreamId, u64)> = Vec::new();

        {
            // Borrowed for the duration of the pull loop so the closure can reach them; the
            // release reporting below deliberately happens after, because reporting inside
            // would re-enter the source while it still has an offer open.
            let controls = &self.controls;
            let peer = &self.peer;
            let announced = &mut self.announced;
            let writable = &self.writable;
            let end = self.end;
            let failed = &mut failed;
            let written = &mut written;

            while source.write_next(&mut |stream, slices, fin| {
                // A stream this endpoint never opened and was never handed is not one it can
                // write to, and saying so is the whole reason a real transport catches a
                // layer that invents identifiers.
                if !writable.contains(&stream) {
                    return WriteOutcome::Gone;
                }

                let mut knobs = controls.borrow_mut();

                if knobs.stalled.contains(&stream) {
                    return WriteOutcome::Blocked;
                }
                if let Some(limit) = knobs.fail_writes_after
                    && knobs.writes >= limit
                {
                    *failed = Some(LoopbackError("write limit reached"));
                    return WriteOutcome::Blocked;
                }
                knobs.writes += 1;

                let offered: usize = slices.iter().map(|s| s.len()).sum();
                let cap = knobs.accept_at_most.unwrap_or(usize::MAX);
                let taken = offered.min(cap);
                drop(knobs);

                // A stream that ends with no bytes at all still has to reach the peer, or
                // the peer waits forever for an end it was never told about. That is the
                // one case where taking nothing is still progress.
                if taken == 0 && !(offered == 0 && fin) {
                    return WriteOutcome::Blocked;
                }

                let mut payload = Vec::with_capacity(taken);
                for slice in slices {
                    if payload.len() == taken {
                        break;
                    }
                    let want = taken - payload.len();
                    let end = want.min(slice.len());
                    payload.extend_from_slice(&slice[..end]);
                }
                let complete = taken == offered;

                // A bidirectional stream this end opened has to be announced before any
                // bytes arrive on it, so the peer has somewhere to route the answer.
                if stream.directionality() == Directionality::Bidirectional
                    && stream.initiator() == end.initiator()
                    && !announced.contains(&stream)
                {
                    announced.push(stream);
                    peer.borrow_mut().deliver(QuicEvent::Accepted { stream });
                }

                peer.borrow_mut().deliver(QuicEvent::Data {
                    stream,
                    bytes: Bytes::from(payload),
                    fin: fin && complete,
                });

                if taken > 0 {
                    written.push((stream, taken as u64));
                }
                WriteOutcome::Accepted(taken)
            }) {
                if failed.is_some() {
                    break;
                }
            }
        }

        // The retain contract in action. This endpoint declares `RETAINS_BUFFERS = true`,
        // so it owes an explicit release for every byte it took — and a test can withhold
        // it to prove the layer really is waiting rather than freeing on write.
        for (stream, bytes) in written {
            self.release(stream, bytes);
        }

        match failed {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_open_uni(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let sequence = self.next_uni;
        self.next_uni += 1;
        match StreamId::compose(
            self.end.initiator(),
            Directionality::Unidirectional,
            sequence,
        ) {
            Ok(stream) => {
                self.writable.push(stream);
                Poll::Ready(Ok(stream))
            }
            Err(_) => Poll::Ready(Err(LoopbackError("stream identifiers exhausted"))),
        }
    }

    fn poll_open_bi(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let sequence = self.next_bi;
        self.next_bi += 1;
        match StreamId::compose(
            self.end.initiator(),
            Directionality::Bidirectional,
            sequence,
        ) {
            Ok(stream) => {
                self.writable.push(stream);
                Poll::Ready(Ok(stream))
            }
            Err(_) => Poll::Ready(Err(LoopbackError("stream identifiers exhausted"))),
        }
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.peer
            .borrow_mut()
            .deliver(QuicEvent::Reset { stream, code });
        Ok(())
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.peer
            .borrow_mut()
            .deliver(QuicEvent::StopSending { stream, code });
        Ok(())
    }

    fn extend_credit(&mut self, _stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        // Connection-level and stream-level credit are one pool here. A real transport keeps
        // them apart; what this needs to model is that *without* the call, delivery stops.
        let mut inbox = self.inbox.borrow_mut();
        inbox.credit = inbox.credit.saturating_add(bytes);
        inbox.promote();
        inbox.wake();
        Ok(())
    }

    fn close(&mut self, code: ErrorCode, _reason: &[u8]) -> Result<(), Self::Error> {
        let mut peer = self.peer.borrow_mut();
        peer.closed = true;
        peer.deliver(QuicEvent::Closed { code: Some(code) });
        Ok(())
    }

    fn now(&self) -> Timestamp {
        // Monotonic without a clock: every call is one nanosecond after the last, which is
        // all nghttp3 requires and keeps tests reproducible.
        Timestamp::from_nanos(self.clock)
    }
}

/// Reports how many bytes an endpoint is holding back for want of credit.
///
/// Test-only visibility into the boundedness obligation.
pub fn held_bytes(endpoint: &Loopback) -> usize {
    endpoint
        .inbox
        .borrow()
        .held
        .iter()
        .map(|event| match event {
            QuicEvent::Data { bytes, .. } => bytes.len(),
            _ => 0,
        })
        .sum()
}

/// Counts polls, for tests asserting that something is *not* polled.
#[derive(Clone, Default)]
pub struct PollCount(Arc<AtomicUsize>);

impl PollCount {
    /// A fresh counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one poll.
    pub fn note(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// How many polls have been recorded.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// A [`StreamSource`] that offers a fixed script, for testing a transport on its own.
///
/// The driver supplies the real one; this exists so [`Loopback`] can be exercised before a
/// driver exists to drive it.
pub struct ScriptedSource {
    offers: VecDeque<(StreamId, Vec<u8>, bool)>,
    /// What each offer was answered with, in order.
    pub outcomes: Vec<WriteOutcome>,
}

impl ScriptedSource {
    /// A source that will offer each entry once, in order.
    pub fn new(offers: impl IntoIterator<Item = (StreamId, Vec<u8>, bool)>) -> Self {
        Self {
            offers: offers.into_iter().collect(),
            outcomes: Vec::new(),
        }
    }

    /// Whether every offer has been made.
    pub fn is_drained(&self) -> bool {
        self.offers.is_empty()
    }
}

impl StreamSource for ScriptedSource {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        let Some((stream, bytes, fin)) = self.offers.pop_front() else {
            return false;
        };
        let slices = [IoSlice::new(&bytes)];
        let outcome = write(stream, &slices, fin);
        self.outcomes.push(outcome);
        // A real source re-offers what was not taken; this one is a script, so it does not.
        true
    }
}

/// Head conversion, reachable from integration tests.
///
/// The conversions are `pub(crate)`: they are an implementation detail of the driver, not
/// API. Integration tests are separate crates and cannot see them, and the alternative —
/// testing the whole of RFC 9114's field-section rules only through a driver — would make a
/// rejected head indistinguishable from a driver bug. So they are re-exported here, hidden
/// and unsupported, exactly as this module's own documentation describes.
pub mod head {
    use crate::http::error::Result;

    /// Encodes a request head, returning the field section as name/value pairs.
    pub fn request_fields(parts: &http::request::Parts) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(pairs(&super::super::head::request_fields(parts)?))
    }

    /// Encodes a response head, returning the field section as name/value pairs.
    pub fn response_fields(parts: &http::response::Parts) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(pairs(&super::super::head::response_fields(parts)?))
    }

    /// Encodes a trailing field section as name/value pairs.
    pub fn trailer_fields(trailers: &http::HeaderMap) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(pairs(&super::super::head::trailer_fields(trailers)?))
    }

    /// Decodes a received request field section.
    pub fn request_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Request<()>> {
        super::super::head::request_head(fields)
    }

    /// Decodes a received response field section.
    pub fn response_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::Response<()>> {
        super::super::head::response_head(fields)
    }

    /// Decodes a received trailing field section.
    pub fn trailers(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<http::HeaderMap> {
        super::super::head::trailers(fields)
    }

    /// Whether a status code is informational, and so does not settle an exchange.
    pub fn is_informational(status: http::StatusCode) -> bool {
        super::super::head::is_informational(status)
    }

    fn pairs(fields: &super::super::head::OwnedFields) -> Vec<(Vec<u8>, Vec<u8>)> {
        fields
            .views()
            .expect("already validated during encoding")
            .iter()
            .map(|field| (field.name().to_vec(), field.value().to_vec()))
            .collect()
    }
}
