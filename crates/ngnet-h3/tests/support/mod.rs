//! A hand-driven HTTP/3 peer, and the pump that runs an exchange to completion.
//!
//! Deliberately built on the sans-I/O core directly rather than on the async layer. If both
//! ends of an exchange were the same driver, a bug in it would cancel out and the test would
//! pass; here the other end is a second, much simpler implementation, so a disagreement
//! shows up as a failure rather than as agreement.

#![allow(dead_code)]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::IoSlice;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::testing::http_body_crate::{Body, Frame, SizeHint};
use ngnet_h3::http::testing::{Knobs, Loopback, block_on, loopback};
use ngnet_h3::http::{Connection, QuicConnection, QuicEvent, StreamSource, WriteOutcome};
use ngnet_h3::{
    Conn, ConnBuilder, Directionality, ErrorCode, FieldAction, FieldSection, FixedBody, Header,
    Initiator, Role, StreamId, Timestamp,
};

/// A loopback pair, client end first.
pub fn pair() -> (Loopback, Loopback, Knobs) {
    loopback()
}

/// The stream a client's `index`th request is carried on.
///
/// Both loopback endpoints hand out stream identifiers in order from zero, so a test that
/// knows how many requests it made knows which stream each is on without having to read one
/// out of the layer.
pub fn request_stream(index: u64) -> StreamId {
    StreamId::compose(Initiator::Client, Directionality::Bidirectional, index)
        .expect("a request stream identifier")
}

// -------------------------------------------------------------------------------- bodies

/// The body type these tests send.
///
/// `bytes::Bytes` is not itself an `http_body::Body`, and the usual adapter lives in
/// `http-body-util`, which this crate does not depend on and will not acquire for a test.
/// One chunk and an optional poll counter covers every case here.
pub struct Payload {
    chunk: Option<Bytes>,
    /// A further chunk, given only once the gate has opened.
    ///
    /// Separate from `chunk` because a body that gives something, waits, and then gives more
    /// is the only way to have bytes reach the transport *before* a later pull fails — one
    /// pull of a body source drains it until it defers, ends or fails, so a chunk and an
    /// error in the same pull are gathered and discarded together.
    held: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
    polls: Option<Arc<AtomicUsize>>,
    /// When this body reports an error, if it ever does.
    fails: Failure,
    /// Withholds `held` until opened, to exercise deferral.
    gate: Option<Gate>,
    /// Where in a recorder's transmit sequence this body failed, if a test is watching.
    mark: Option<Log>,
}

/// When a body reports an error.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// Never: the body ends when it runs out.
    Never,
    /// On its very first pull, having produced nothing at all.
    AtOnce,
    /// Once everything it had to give has been given.
    AtTheEnd,
}

impl Default for Payload {
    fn default() -> Self {
        Self {
            chunk: None,
            held: None,
            trailers: None,
            polls: None,
            fails: Failure::Never,
            gate: None,
            mark: None,
        }
    }
}

impl Payload {
    /// Notes in a recorder's log where in the transmit sequence this body fails.
    ///
    /// The mark is what makes "the reset followed the failure within two passes" measurable:
    /// the failure happens inside a transmit, where nothing the transport was called with
    /// says so.
    pub fn marking(mut self, log: Log) -> Self {
        self.mark = Some(log);
        self
    }

    /// Reports the failure, noting where it happened for whoever is watching.
    fn fail(&self) -> BodyFailed {
        if let Some(log) = &self.mark {
            log.mark();
        }
        BodyFailed
    }
}

/// A body with nothing in it.
pub fn empty() -> Payload {
    Payload::default()
}

/// A body that yields one chunk and ends.
pub fn once(bytes: Bytes) -> Payload {
    Payload {
        chunk: Some(bytes),
        ..Payload::default()
    }
}

/// A body whose buffer reports when it is finally freed.
///
/// The measurement the retain contract needs. `Bytes::from_owner` keeps the owner alive for
/// exactly as long as any reference to the bytes exists — including the ones nghttp3 is
/// reading through — so the owner's `Drop` firing *is* the release.
pub fn tracked(bytes: Bytes, probe: Probe) -> Payload {
    let owner = Tracked {
        data: bytes.to_vec(),
        freed: probe.0,
    };
    once(Bytes::from_owner(owner))
}

/// A body that reports an error partway rather than ending cleanly.
pub fn failing() -> Payload {
    Payload {
        fails: Failure::AtOnce,
        ..Payload::default()
    }
}

/// A body that yields one chunk and then fails, in the same pull.
///
/// The other half of the pair with [`failing`]: one fails having produced nothing, this one
/// fails having produced something the stack must then throw away.
pub fn failing_after(bytes: Bytes) -> Payload {
    Payload {
        chunk: Some(bytes),
        fails: Failure::AtTheEnd,
        ..Payload::default()
    }
}

/// A body that yields one chunk, waits, and then yields another and fails.
///
/// The shape that separates bytes the transport was given from bytes it never was: `first`
/// is handed over in a pull of its own, and `held` is gathered by the pull that fails, so a
/// test can look for both patterns in what was written and find only one.
pub fn failing_after_resuming(first: Bytes, held: Bytes, gate: Gate) -> Payload {
    Payload {
        chunk: Some(first),
        held: Some(held),
        fails: Failure::AtTheEnd,
        gate: Some(gate),
        ..Payload::default()
    }
}

/// A body that has nothing to give until its gate is opened.
pub fn gated(bytes: Bytes, gate: Gate) -> Payload {
    Payload {
        held: Some(bytes),
        gate: Some(gate),
        ..Payload::default()
    }
}

/// A body that yields one chunk and then a trailing field section.
pub fn with_trailers(bytes: Bytes, trailers: http::HeaderMap) -> Payload {
    Payload {
        chunk: Some(bytes),
        trailers: Some(trailers),
        ..Payload::default()
    }
}

/// A body that reports each poll, for asserting on when it is pulled.
pub fn counting(bytes: Bytes, polls: Arc<AtomicUsize>) -> Payload {
    Payload {
        chunk: Some(bytes),
        polls: Some(polls),
        ..Payload::default()
    }
}

/// A buffer that says when it is freed.
struct Tracked {
    data: Vec<u8>,
    freed: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.freed.fetch_add(1, Ordering::Release);
    }
}

/// Watches whether a tracked buffer has been released.
#[derive(Clone)]
pub struct Probe(Arc<AtomicUsize>);

impl Probe {
    pub fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    /// Whether the buffer has been freed.
    pub fn freed(&self) -> bool {
        self.0.load(Ordering::Acquire) > 0
    }
}

impl Default for Probe {
    fn default() -> Self {
        Self::new()
    }
}

/// Lets a body be held back and then let go.
#[derive(Clone, Default)]
pub struct Gate(Arc<std::sync::Mutex<GateState>>);

#[derive(Default)]
struct GateState {
    open: bool,
    waker: Option<std::task::Waker>,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets the body proceed, waking whoever was waiting on it.
    pub fn open(&self) {
        let mut state = self.0.lock().expect("gate");
        state.open = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    fn poll(&self, context: &Context<'_>) -> bool {
        let mut state = self.0.lock().expect("gate");
        if state.open {
            return true;
        }
        state.waker = Some(context.waker().clone());
        false
    }
}

/// What a deliberately failing body reports.
#[derive(Debug)]
pub struct BodyFailed;

impl core::fmt::Display for BodyFailed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "the caller's body failed")
    }
}

impl std::error::Error for BodyFailed {}

impl Body for Payload {
    type Data = Bytes;
    type Error = BodyFailed;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(polls) = &self.polls {
            polls.fetch_add(1, Ordering::Relaxed);
        }
        if self.fails == Failure::AtOnce {
            return Poll::Ready(Some(Err(self.fail())));
        }
        if let Some(chunk) = self.chunk.take() {
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        if self.held.is_some() {
            if let Some(gate) = &self.gate
                && !gate.poll(context)
            {
                // Nothing to give yet. Distinct from a busy transport, and the layer must
                // not confuse the two.
                return Poll::Pending;
            }
            let held = self.held.take().expect("a chunk that was just seen");
            return Poll::Ready(Some(Ok(Frame::data(held))));
        }
        if let Some(trailers) = self.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        if self.fails == Failure::AtTheEnd {
            return Poll::Ready(Some(Err(self.fail())));
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.fails == Failure::Never
            && self.chunk.is_none()
            && self.held.is_none()
            && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Reads a body to the end.
pub async fn collect<B>(mut body: B) -> Result<Vec<u8>, B::Error>
where
    B: Body<Data = Bytes> + Unpin,
{
    let mut out = Vec::new();
    loop {
        let frame = core::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            None => return Ok(out),
            Some(Err(error)) => return Err(error),
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    out.extend_from_slice(&data);
                }
            }
        }
    }
}

// ------------------------------------------------------------- watching the transport

/// One thing the stack asked a transport to do.
///
/// Everything here is an argument one of [`QuicConnection`]'s or [`StreamSource`]'s own
/// methods was called with. Nothing is read out of the layer, which is what makes an
/// assertion over a log of these a statement about the transport seam — and therefore about
/// every transport this stack is carried over — rather than about one implementation's
/// internals.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Call {
    /// A call to `poll_transmit`, recorded so passes can be counted.
    Transmit,
    /// One offer of stream data, and what the transport did with it.
    Write {
        /// The stream the bytes were offered for.
        stream: StreamId,
        /// The bytes offered, in order, so a distinctive pattern can be looked for.
        bytes: Vec<u8>,
        /// Whether the offer was marked as the end of the stream.
        fin: bool,
        /// What the transport underneath answered.
        outcome: WriteOutcome,
    },
    /// A stream abandoned, with the code the peer will be told.
    Reset {
        /// The stream abandoned.
        stream: StreamId,
        /// The application error code.
        code: u64,
    },
    /// The peer asked to stop sending on a stream.
    StopSending {
        /// The stream no longer wanted.
        stream: StreamId,
        /// The application error code.
        code: u64,
    },
    /// The connection closed.
    Close {
        /// The application error code.
        code: u64,
    },
}

/// What a [`Recorder`] has seen, shared with the test watching it.
#[derive(Clone, Default)]
pub struct Log(Arc<std::sync::Mutex<Record>>);

#[derive(Default)]
struct Record {
    calls: Vec<Call>,
    /// Transmit counts at moments something outside the transport asked to be noted.
    marks: Vec<usize>,
}

impl Log {
    /// A fresh log.
    pub fn new() -> Self {
        Self::default()
    }

    fn note(&self, call: Call) {
        self.0.lock().expect("the recorder's log").calls.push(call);
    }

    /// Everything recorded so far, in order.
    pub fn calls(&self) -> Vec<Call> {
        self.0.lock().expect("the recorder's log").calls.clone()
    }

    /// How many transmit passes the transport has been asked for.
    pub fn transmits(&self) -> usize {
        self.0
            .lock()
            .expect("the recorder's log")
            .calls
            .iter()
            .filter(|call| **call == Call::Transmit)
            .count()
    }

    /// Notes where in the transmit sequence something happened that the transport was not
    /// told about — a body failing, which happens *inside* a transmit.
    pub fn mark(&self) {
        let mut record = self.0.lock().expect("the recorder's log");
        let transmits = record
            .calls
            .iter()
            .filter(|call| **call == Call::Transmit)
            .count();
        record.marks.push(transmits);
    }

    /// The transmit counts noted by [`mark`](Self::mark), in order.
    pub fn marks(&self) -> Vec<usize> {
        self.0.lock().expect("the recorder's log").marks.clone()
    }

    /// The error codes of every reset issued for a stream.
    pub fn resets(&self, stream: StreamId) -> Vec<u64> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Reset { stream: s, code } if s == stream => Some(code),
                _ => None,
            })
            .collect()
    }

    /// The error codes of every stop-sending issued for a stream.
    pub fn stops(&self, stream: StreamId) -> Vec<u64> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::StopSending { stream: s, code } if s == stream => Some(code),
                _ => None,
            })
            .collect()
    }

    /// How many offers for a stream carried an end-of-stream marker.
    ///
    /// Counted over offers rather than over what the transport accepted, deliberately: the
    /// claim being made is that the stack never *asks* for a failed stream to be ended, not
    /// merely that one particular transport declined to.
    pub fn end_markers(&self, stream: StreamId) -> usize {
        self.calls()
            .into_iter()
            .filter(
                |call| matches!(call, Call::Write { stream: s, fin, .. } if *s == stream && *fin),
            )
            .count()
    }

    /// Every byte offered for a stream, in order.
    pub fn offered(&self, stream: StreamId) -> Vec<u8> {
        let mut out = Vec::new();
        for call in self.calls() {
            if let Call::Write {
                stream: s, bytes, ..
            } = call
                && s == stream
            {
                out.extend_from_slice(&bytes);
            }
        }
        out
    }

    /// How many transmit passes had happened when a stream was first reset.
    ///
    /// `None` if it never was. Paired with [`marks`](Self::marks) this measures the distance
    /// between a body failing and the peer being told, in the only unit the transport seam
    /// has: passes.
    pub fn transmits_before_reset(&self, stream: StreamId) -> Option<usize> {
        let mut transmits = 0;
        for call in self.calls() {
            match call {
                Call::Transmit => transmits += 1,
                Call::Reset { stream: s, .. } if s == stream => return Some(transmits),
                _ => {}
            }
        }
        None
    }
}

/// A [`QuicConnection`] that records what it was asked to do and passes it on.
///
/// A decorator rather than a transport of its own, so the thing being observed is a real
/// exchange with a real peer underneath: it wraps [`Loopback`] where a live peer is wanted
/// and [`Stub`] where the point is that no peer says anything.
pub struct Recorder<Q> {
    inner: Q,
    log: Log,
}

impl<Q> Recorder<Q> {
    /// Wraps a transport, recording into `log`.
    pub fn new(inner: Q, log: Log) -> Self {
        Self { inner, log }
    }
}

impl<Q: QuicConnection> QuicConnection for Recorder<Q> {
    type Error = Q::Error;

    // Whatever the transport underneath is. Declaring anything else here would be declaring
    // something about someone else's memory.
    const RETAINS_BUFFERS: bool = Q::RETAINS_BUFFERS;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        self.inner.poll_event(cx)
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        self.log.note(Call::Transmit);
        // Split rather than borrowed whole: the transport underneath takes the source by
        // mutable reference and the wrapper it is handed has to hold the log at the same
        // time.
        let Self { inner, log } = self;
        let mut recording = Recording { source, log };
        inner.poll_transmit(cx, &mut recording)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.inner.poll_open_uni(cx)
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.inner.poll_open_bi(cx)
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.log.note(Call::Reset {
            stream,
            code: code.get(),
        });
        self.inner.reset(stream, code)
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        self.log.note(Call::StopSending {
            stream,
            code: code.get(),
        });
        self.inner.stop_sending(stream, code)
    }

    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        self.inner.extend_credit(stream, bytes)
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error> {
        self.log.note(Call::Close { code: code.get() });
        self.inner.close(code, reason)
    }

    fn now(&self) -> Timestamp {
        self.inner.now()
    }
}

/// The layer's write side, wrapped so every offer made through it is recorded.
///
/// Writes are pulled, so there is no call to intercept: the transport hands the source a
/// closure and the source decides what to offer it. The wrapper therefore interposes on the
/// *closure*, which is where the stream, the bytes and the end marker actually appear.
struct Recording<'a, S> {
    source: &'a mut S,
    log: &'a Log,
}

impl<S: StreamSource> StreamSource for Recording<'_, S> {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        let log = &self.log;
        self.source.write_next(&mut |stream, slices, fin| {
            let outcome = write(stream, slices, fin);
            let mut bytes = Vec::new();
            for slice in slices {
                bytes.extend_from_slice(slice);
            }
            log.note(Call::Write {
                stream,
                bytes,
                fin,
                outcome,
            });
            outcome
        })
    }
}

/// A transport that says nothing of its own accord.
///
/// For the tests whose subject is what this endpoint does *without* the peer — the ones that
/// must show a failure reaching the transport with nothing arriving to prompt it, and the
/// ones that must show the driver never went idle in between. Two of its properties are
/// obligations those tests depend on rather than conveniences:
///
/// **`poll_transmit` is always `Poll::Ready` and takes every byte offered.** The driver
/// *awaits* `poll_transmit`, so a stub answering `Pending` would suspend the driver there
/// rather than at the park — a test that then saw `Pending` would have learnt nothing about
/// parking. And a write left blocked survives the pass, which brings the driver back to the
/// park early on the next one.
///
/// **`poll_open_uni` always answers.** Nothing else in a pass runs until the three
/// unidirectional streams HTTP/3 needs are open and bound.
///
/// It reports no release for what it takes, so buffers stay retained for its lifetime. That
/// is a permitted way to be wrong-ish about memory and never about correctness, and these
/// tests run a handful of passes.
pub struct Stub {
    state: Rc<RefCell<StubState>>,
    next_uni: u64,
    next_bi: u64,
}

#[derive(Default)]
struct StubState {
    /// Events a test has chosen to deliver; empty is the normal state.
    events: VecDeque<QuicEvent>,
    /// How many bidirectional streams may be opened in all, if a test has capped it.
    open_at_most: Option<usize>,
    opened_bi: usize,
}

/// Makes a [`Stub`] speak, or stop handing out streams.
#[derive(Clone)]
pub struct StubControls(Rc<RefCell<StubState>>);

impl StubControls {
    /// Hands the driver one event, as a transport with news would.
    pub fn deliver(&self, event: QuicEvent) {
        self.0.borrow_mut().events.push_back(event);
    }

    /// How many test-supplied events the driver has not consumed yet.
    pub fn pending_events(&self) -> usize {
        self.0.borrow().events.len()
    }

    /// Opens no more than this many bidirectional streams, as a peer at its limit would.
    ///
    /// Further requests for one answer [`Poll::Pending`] and never resolve, which is exactly
    /// what an exhausted stream limit looks like to the driver.
    pub fn open_at_most(&self, streams: usize) {
        self.0.borrow_mut().open_at_most = Some(streams);
    }
}

/// A silent transport, and the handle that makes it speak.
///
/// The stub takes the client's side of the stream-identifier space, so it is a client's
/// transport; a server's would number its streams differently.
pub fn stub() -> (Stub, StubControls) {
    let state = Rc::new(RefCell::new(StubState::default()));
    let controls = StubControls(Rc::clone(&state));
    (
        Stub {
            state,
            next_uni: 0,
            next_bi: 0,
        },
        controls,
    )
}

impl QuicConnection for Stub {
    // It keeps nothing it is given, so it borrows nothing.
    type Error = core::convert::Infallible;

    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, _cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        // No waker is registered: everything using this drives the driver by hand, so a wake
        // would have nothing to deliver to.
        match self.state.borrow_mut().events.pop_front() {
            Some(event) => Poll::Ready(Ok(event)),
            None => Poll::Pending,
        }
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        _cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        while source.write_next(&mut |_stream, slices, _fin| {
            WriteOutcome::Accepted(slices.iter().map(|slice| slice.len()).sum())
        }) {}
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_open_uni(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let sequence = self.next_uni;
        self.next_uni += 1;
        Poll::Ready(Ok(StreamId::compose(
            Initiator::Client,
            Directionality::Unidirectional,
            sequence,
        )
        .expect("a unidirectional stream identifier")))
    }

    fn poll_open_bi(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        {
            let mut state = self.state.borrow_mut();
            if let Some(limit) = state.open_at_most
                && state.opened_bi >= limit
            {
                return Poll::Pending;
            }
            state.opened_bi += 1;
        }
        let sequence = self.next_bi;
        self.next_bi += 1;
        Poll::Ready(Ok(StreamId::compose(
            Initiator::Client,
            Directionality::Bidirectional,
            sequence,
        )
        .expect("a bidirectional stream identifier")))
    }

    fn reset(&mut self, _stream: StreamId, _code: ErrorCode) -> Result<(), Self::Error> {
        Ok(())
    }

    fn stop_sending(&mut self, _stream: StreamId, _code: ErrorCode) -> Result<(), Self::Error> {
        Ok(())
    }

    fn extend_credit(&mut self, _stream: Option<StreamId>, _bytes: u64) -> Result<(), Self::Error> {
        Ok(())
    }

    fn close(&mut self, _code: ErrorCode, _reason: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn now(&self) -> Timestamp {
        // Never goes backwards, trivially. nghttp3 wants a reading, not a clock.
        Timestamp::from_nanos(0)
    }
}

// -------------------------------------------------------------------------------- server

/// One field of a received section.
pub type Field = (Vec<u8>, Vec<u8>);

/// What the hand-driven server has seen.
#[derive(Default)]
pub struct Seen {
    /// Fields of the request on each stream.
    pub heads: HashMap<i64, Vec<Field>>,
    /// Body bytes received per stream.
    pub bodies: HashMap<i64, Vec<u8>>,
    /// Streams whose request is complete.
    pub ended: Vec<i64>,
    /// Trailing fields received per stream.
    pub trailers: HashMap<i64, Vec<Field>>,
}

/// A minimal HTTP/3 server built on the sans-I/O core.
pub struct Server {
    backend: Loopback,
    conn: Conn<Seen>,
    seen: Seen,
    bound: bool,
    answered: Vec<i64>,
    uni_streams: Vec<i64>,
    status: u16,
    body: Option<Vec<u8>>,
    echo_path: bool,
}

impl Server {
    pub fn new(backend: Loopback) -> Self {
        let conn = ConnBuilder::<Seen>::new(Role::Server)
            .on_field(|seen, stream, section, _token, name, value| {
                let into = match section {
                    FieldSection::Headers => &mut seen.heads,
                    FieldSection::Trailers => &mut seen.trailers,
                };
                into.entry(stream.get())
                    .or_default()
                    .push((name.to_vec(), value.to_vec()));
                FieldAction::Continue
            })
            .on_data(|seen, stream, chunk| {
                seen.bodies
                    .entry(stream.get())
                    .or_default()
                    .extend_from_slice(chunk);
            })
            .on_end_stream(|seen, stream| seen.ended.push(stream.get()))
            .build()
            .expect("a server connection");

        Self {
            backend,
            conn,
            seen: Seen::default(),
            bound: false,
            answered: Vec::new(),
            uni_streams: Vec::new(),
            status: 200,
            body: None,
            echo_path: false,
        }
    }

    /// Answers with this status instead of 200.
    pub fn answer_with_status(&mut self, status: u16) {
        self.status = status;
    }

    /// Answers with this body.
    pub fn answer_with_body(&mut self, body: Vec<u8>) {
        self.body = Some(body);
    }

    /// Answers each request with its own `:path`, so responses can be told apart.
    pub fn echo_path_in_body(&mut self) {
        self.echo_path = true;
    }

    /// How many requests have been received in full.
    pub fn requests_seen(&self) -> usize {
        self.seen.ended.len()
    }

    /// How many unidirectional streams the peer opened.
    pub fn saw_unidirectional_streams(&self) -> usize {
        self.uni_streams.len()
    }

    /// A trailing field the peer sent, by name.
    pub fn received_trailer(&self, name: &str) -> Option<String> {
        self.seen.trailers.values().find_map(|fields| {
            fields
                .iter()
                .find(|(field, _)| field == name.as_bytes())
                .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
        })
    }

    /// The body of the first request received.
    pub fn received_body(&self) -> Vec<u8> {
        self.seen
            .bodies
            .values()
            .next()
            .cloned()
            .unwrap_or_default()
    }

    fn bind(&mut self) {
        if self.bound {
            return;
        }
        let mut ids = Vec::new();
        for _ in 0..3 {
            match block_on(core::future::poll_fn(|cx| self.backend.poll_open_uni(cx))) {
                Ok(stream) => ids.push(stream),
                Err(_) => return,
            }
        }
        self.conn.bind_control_stream(ids[0]).expect("control");
        self.conn.bind_qpack_streams(ids[1], ids[2]).expect("qpack");
        self.bound = true;
    }

    /// Moves whatever it can, without blocking.
    pub fn pump(&mut self) {
        self.bind();

        loop {
            let event = poll_once(|cx| self.backend.poll_event(cx));
            let Some(Ok(event)) = event else { break };
            match event {
                QuicEvent::Data { stream, bytes, fin } => {
                    if stream.directionality() == Directionality::Unidirectional
                        && stream.initiator() == Initiator::Client
                        && !self.uni_streams.contains(&stream.get())
                    {
                        self.uni_streams.push(stream.get());
                    }
                    let now = self.backend.now();
                    let length = bytes.len() as u64;
                    if let Ok(credit) =
                        self.conn
                            .read_stream(stream, &bytes, fin, now, &mut self.seen)
                    {
                        let _ = self.backend.extend_credit(Some(stream), credit.bytes());
                        let _ = self.backend.extend_credit(None, credit.bytes());
                    }
                    // The body payload too, which the state machine deliberately leaves to
                    // the application. This peer reads everything immediately, so it
                    // credits everything immediately.
                    let _ = self.backend.extend_credit(Some(stream), length);
                    let _ = self.backend.extend_credit(None, length);
                }
                QuicEvent::Released { stream, bytes, .. } => {
                    let _ = self.conn.add_ack_offset(stream, bytes, &mut self.seen);
                }
                _ => {}
            }
        }

        self.answer();
        self.flush();
    }

    /// Answers every complete request that has not been answered.
    fn answer(&mut self) {
        let pending: Vec<i64> = self
            .seen
            .ended
            .iter()
            .copied()
            .filter(|stream| !self.answered.contains(stream))
            .collect();

        for raw in pending {
            let Ok(stream) = StreamId::new(raw) else {
                continue;
            };
            if stream.directionality() != Directionality::Bidirectional {
                continue;
            }
            self.answered.push(raw);

            let status = self.status.to_string();
            let fields = vec![Header::new(":status", status.as_str()).expect("a status")];

            let payload = if self.echo_path {
                self.seen
                    .heads
                    .get(&raw)
                    .and_then(|fields| {
                        fields
                            .iter()
                            .find(|(name, _)| name == b":path")
                            .map(|(_, value)| value.clone())
                    })
                    .unwrap_or_default()
            } else {
                self.body.clone().unwrap_or_default()
            };

            let body: Option<Box<dyn ngnet_h3::BodySource>> = if payload.is_empty() {
                None
            } else {
                Some(Box::new(FixedBody::new(payload)))
            };
            let _ = self.conn.submit_response(stream, &fields, body);
        }
    }

    /// Writes whatever the connection has ready.
    fn flush(&mut self) {
        let mut source = ServerOffers {
            conn: &mut self.conn,
            seen: &mut self.seen,
            blocked: Vec::new(),
            retried: false,
        };
        let _ = poll_once(|cx| self.backend.poll_transmit(cx, &mut source));
    }
}

/// The server's side of a write, mirroring what the driver does.
struct ServerOffers<'a> {
    conn: &'a mut Conn<Seen>,
    seen: &'a mut Seen,
    blocked: Vec<StreamId>,
    retried: bool,
}

impl StreamSource for ServerOffers<'_> {
    fn write_next(
        &mut self,
        write: &mut dyn FnMut(StreamId, &[IoSlice<'_>], bool) -> WriteOutcome,
    ) -> bool {
        loop {
            let Ok(offer) = self.conn.writev_stream(self.seen) else {
                return false;
            };
            let Some(guard) = offer else {
                if self.retried || self.blocked.is_empty() {
                    return false;
                }
                self.retried = true;
                for stream in core::mem::take(&mut self.blocked) {
                    let _ = self.conn.unblock_stream(stream);
                }
                continue;
            };

            let stream = guard.stream();
            let fin = guard.fin();
            let offered = guard.len();
            let outcome = write(stream, guard.slices(), fin);

            match outcome {
                WriteOutcome::Accepted(taken) => {
                    let taken = taken.min(offered);
                    let _ = guard.commit(taken);
                    if taken < offered {
                        self.blocked.push(stream);
                        let _ = self.conn.block_stream(stream);
                    }
                }
                WriteOutcome::Blocked => {
                    if offered == 0 && fin {
                        guard.abandon();
                    } else {
                        let _ = guard.commit(0);
                    }
                    self.blocked.push(stream);
                    let _ = self.conn.block_stream(stream);
                }
                WriteOutcome::Gone => {
                    guard.abandon();
                    let _ = self.conn.shutdown_stream_write(stream);
                }
            }
            return true;
        }
    }
}

// ---------------------------------------------------------------------------------- pump

/// Polls a future once, returning its output if it was ready.
fn poll_once<T>(mut f: impl FnMut(&mut Context<'_>) -> Poll<T>) -> Option<T> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match f(&mut context) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

/// Polls a future once by hand, answering `None` where it answered [`Poll::Pending`].
///
/// On a no-op waker and on this thread, so nothing runs between one poll and the next and a
/// test that counts polls is counting exactly what it asked for.
pub fn poll_now<F: Future>(future: &mut Pin<Box<F>>) -> Option<F::Output> {
    poll_once(|cx| future.as_mut().poll(cx))
}

/// How many rounds an exchange may take before it is declared stuck.
///
/// A bound rather than a timeout: without one a protocol bug is a hung test rather than a
/// failing one, and a hung test says nothing about what went wrong.
const ROUNDS: usize = 2_000;

/// What a request resolves to.
pub type Answer = Result<http::Response<ngnet_h3::http::IncomingBody>, ngnet_h3::http::Error>;

/// Runs one request to completion, interleaving the client driver and the server.
pub fn exchange<F, D>(
    driver: Connection<D>,
    server: &mut Server,
    submit: impl FnOnce() -> F,
) -> Answer
where
    F: Future<Output = Answer>,
    D: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    let future = submit();
    let mut results = drive(driver, server, vec![future]);
    results.pop().expect("one result")
}

/// Runs several requests to completion together.
pub fn exchange_many<F, D>(
    driver: Connection<D>,
    server: &mut Server,
    futures: Vec<F>,
) -> Vec<Answer>
where
    F: Future<Output = Answer>,
    D: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    drive(driver, server, futures)
}

fn drive<F, D>(driver: Connection<D>, server: &mut Server, futures: Vec<F>) -> Vec<Answer>
where
    F: Future<Output = Answer>,
    D: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    let mut driver = Box::pin(driver);
    let mut pending: Vec<Option<Pin<Box<F>>>> =
        futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut results: Vec<Option<Answer>> = (0..pending.len()).map(|_| None).collect();

    for _ in 0..ROUNDS {
        let _ = poll_once(|cx| driver.as_mut().poll(cx));
        server.pump();
        let _ = poll_once(|cx| driver.as_mut().poll(cx));

        for (index, slot) in pending.iter_mut().enumerate() {
            let Some(future) = slot else { continue };
            if let Some(output) = poll_once(|cx| future.as_mut().poll(cx)) {
                results[index] = Some(output);
                *slot = None;
            }
        }

        if results.iter().all(Option::is_some) {
            break;
        }
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| result.unwrap_or_else(|| panic!("request {index} never resolved")))
        .collect()
}

/// Runs a connection a controlled number of rounds at a time.
///
/// [`exchange`] runs an exchange to completion, which is what most tests want. The retain
/// tests want the opposite: to stop partway, look at what is retained, change something, and
/// carry on. That is what this is for.
pub struct Pump<D> {
    driver: Pin<Box<Connection<D>>>,
    server: Server,
    finished: Option<Result<(), ngnet_h3::http::Error>>,
}

impl<D> Pump<D>
where
    D: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    pub fn new(driver: Connection<D>, server: Server) -> Self {
        Self {
            driver: Box::pin(driver),
            server,
            finished: None,
        }
    }

    /// Runs `rounds` rounds, stopping early if the future resolves.
    pub fn rounds<F>(&mut self, rounds: usize, future: &mut Pin<Box<F>>) -> Option<Answer>
    where
        F: Future<Output = Answer>,
    {
        for _ in 0..rounds {
            if self.finished.is_none()
                && let Some(outcome) = poll_once(|cx| self.driver.as_mut().poll(cx))
            {
                self.finished = Some(outcome);
            }
            self.server.pump();
            if self.finished.is_none() {
                let _ = poll_once(|cx| self.driver.as_mut().poll(cx));
            }
            if let Some(answer) = poll_once(|cx| future.as_mut().poll(cx)) {
                return Some(answer);
            }
        }
        None
    }

    /// Whether the driver ended in failure.
    pub fn driver_failed(&self) -> bool {
        matches!(self.finished, Some(Err(_)))
    }

    /// The peer, once the pump is finished with.
    pub fn into_server(self) -> Server {
        self.server
    }

    /// The peer, while the pump is still running.
    pub fn server(&self) -> &Server {
        &self.server
    }
}

// -------------------------------------------------------------- this crate at both ends

impl Gate {
    /// Waits until the gate is opened.
    pub async fn wait(&self) {
        let gate = self.clone();
        core::future::poll_fn(move |cx| {
            if gate.poll(cx) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

/// Reads a body to the end, synchronously.
pub fn read_body(body: ngnet_h3::http::IncomingBody) -> Vec<u8> {
    block_on(read_body_async(body))
}

/// Reads a body to the end.
pub async fn read_body_async(mut body: ngnet_h3::http::IncomingBody) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let frame = core::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            None | Some(Err(_)) => return out,
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    out.extend_from_slice(&data);
                }
            }
        }
    }
}

/// Drives a client driver and a server driver against each other.
///
/// Both ends are this crate. That is the point — it is the first configuration in which the
/// whole layer is exercised — and also its weakness, since a shared misreading would agree
/// with itself. The hand-driven peer in [`Server`] is the counterweight for the client, and
/// the quinn integration is the counterweight for the transport.
pub struct BothEnds<C, S> {
    client: Pin<Box<Connection<C>>>,
    server: Pin<Box<Connection<S>>>,
    client_done: bool,
    server_done: bool,
}

impl<C, S> BothEnds<C, S>
where
    C: Future<Output = Result<(), ngnet_h3::http::Error>>,
    S: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    pub fn new(client: Connection<C>, server: Connection<S>) -> Self {
        Self {
            client: Box::pin(client),
            server: Box::pin(server),
            client_done: false,
            server_done: false,
        }
    }

    /// Runs one round: client, server, client again so an answer is seen when it arrives.
    pub fn round(&mut self) {
        if !self.client_done && poll_once(|cx| self.client.as_mut().poll(cx)).is_some() {
            self.client_done = true;
        }
        if !self.server_done && poll_once(|cx| self.server.as_mut().poll(cx)).is_some() {
            self.server_done = true;
        }
        if !self.client_done && poll_once(|cx| self.client.as_mut().poll(cx)).is_some() {
            self.client_done = true;
        }
    }

    /// Polls a future without running a round.
    pub fn peek<F: Future>(&mut self, future: &mut Pin<Box<F>>) -> Option<F::Output> {
        poll_once(|cx| future.as_mut().poll(cx))
    }

    /// Runs up to `rounds` rounds, stopping as soon as the future resolves.
    pub fn rounds<F: Future>(
        &mut self,
        rounds: usize,
        future: &mut Pin<Box<F>>,
    ) -> Option<F::Output> {
        for _ in 0..rounds {
            self.round();
            if let Some(output) = poll_once(|cx| future.as_mut().poll(cx)) {
                return Some(output);
            }
        }
        None
    }
}

/// Runs one request between this crate's client and this crate's server.
pub fn both_ends<F, C, S>(
    client: Connection<C>,
    server: Connection<S>,
    submit: impl FnOnce() -> F,
) -> Answer
where
    F: Future<Output = Answer>,
    C: Future<Output = Result<(), ngnet_h3::http::Error>>,
    S: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    let mut answers = both_ends_many(client, server, vec![submit()]);
    answers.pop().expect("one answer")
}

/// Runs several requests between this crate's client and this crate's server.
pub fn both_ends_many<F, C, S>(
    client: Connection<C>,
    server: Connection<S>,
    futures: Vec<F>,
) -> Vec<Answer>
where
    F: Future<Output = Answer>,
    C: Future<Output = Result<(), ngnet_h3::http::Error>>,
    S: Future<Output = Result<(), ngnet_h3::http::Error>>,
{
    let mut pump = BothEnds::new(client, server);
    let mut pending: Vec<Option<Pin<Box<F>>>> =
        futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut results: Vec<Option<Answer>> = (0..pending.len()).map(|_| None).collect();

    for _ in 0..ROUNDS {
        pump.round();
        for (index, slot) in pending.iter_mut().enumerate() {
            let Some(future) = slot else { continue };
            if let Some(output) = pump.peek(future) {
                results[index] = Some(output);
                *slot = None;
            }
        }
        if results.iter().all(Option::is_some) {
            break;
        }
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| result.unwrap_or_else(|| panic!("request {index} never resolved")))
        .collect()
}
