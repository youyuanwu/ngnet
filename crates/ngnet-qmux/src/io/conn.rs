//! The connection: ownership, the pump, and the operations a caller drives.
//!
//! # What this owns
//!
//! One byte stream, one clock, one [`Conn`], and the four buffers that stand between them: the
//! bytes read from the stream, the record being serialised, the bytes waiting to be written,
//! and the events the handlers recorded. Everything the module documentation calls "the loop
//! written once" is in [`Connection::pump`].
//!
//! # The pump order, which is the whole design
//!
//! **Flush, then produce, then read.** Not "produce everything, then flush", which is the
//! shape that suggests itself and is wrong twice over.
//!
//! Flushing first is what keeps at most one record outstanding. A record is produced only
//! into an *empty* outbound buffer, so the previous one has necessarily reached the byte
//! stream in full before the next exists. That bounds what a slow peer can make this side
//! hold to a single record -- 16382 bytes -- rather than to however much the caller queued;
//! and it makes a partial accept impossible to get wrong, because there is only ever one
//! record's worth of bytes to resume from. A layer that produced first would hold the whole
//! backlog in memory and would have to interleave records correctly on the way out, and a
//! record interleaved with the tail of its predecessor is not a record the peer can parse.
//!
//! Reading last, and in a particular order within the read, is what makes a peer's close
//! legible. The bytes go to [`RecordFramer`] *first* and to [`Conn::read`] *second*, and only
//! then is the outcome acted on. dwnx reports `PeerClosed` after consuming the close record,
//! possibly with more bytes still to come in the same chunk; feeding the framer first means
//! the close record is already latched when that report arrives, so the kind, code, frame type
//! and reason can be decoded out of it. Feeding the state machine first and the framer
//! afterwards would work too -- but only by accident, and only until someone reordered two
//! lines that look independent.
//!
//! A read is followed by one more write pass when it left something to say, so a window
//! extension or a ping response provoked by what just arrived leaves in the same wakeup
//! instead of waiting for the next one. That pass obeys the same flush-then-produce rule; it
//! is an extra turn of the same crank, not an exception to it.
//!
//! # A push error is fatal, and never retried
//!
//! [`RecordWriter::push`](crate::RecordWriter::push) returning an error drops the writer
//! mid-record. `Drop` finalises the record so dwnx is not left writing through a retained
//! pointer -- that much is safe -- but the produced bytes are discarded, and if the record had
//! already packed stream data then dwnx has *already advanced that stream's send offset*. The
//! bytes are gone and the peer will see a gap it can never fill.
//!
//! So a failed production ends the connection. Retrying the write would send the next chunk at
//! an offset the peer cannot reconcile, which presents as a stream that stalls rather than as
//! an error, and is the most expensive of the failures in this file to diagnose after the
//! fact.
//!
//! # Waiting, and never spinning
//!
//! Three things here cannot proceed on demand: an open the peer's stream limit forbids, a
//! write with no flow-control credit, and a read the caller has not made room for. Each parks
//! against the event that ends it -- a raised limit, an extended window, credit reported back
//! -- through [`Signals`], and none of them wakes itself. See that module for what a self-wake
//! costs and which callback fires which slot.
//!
//! [`try_write_stream`](Connection::try_write_stream) is the deliberate exception: it reports
//! [`StreamWrite::Blocked`] and returns, because the caller it exists for has no [`Context`]
//! to park with.
//!
//! An idle connection therefore arms nothing at all. It has no outbound bytes, so it never
//! offers the byte stream a write; it polls one read, which registers the byte stream's own
//! waker and returns pending; and there is no timer here to fire in the meantime. The next
//! poll happens when the peer says something, and not before.
//!
//! # How much this reads ahead of its caller
//!
//! [`ReadAhead`] bounds bytes *delivered and not yet credited back*, and the pump stops
//! reading from the byte stream while that figure is at the bound. Backpressure then flows
//! where it belongs: bytes pile up in the transport, the peer's own window closes, and the
//! sender stops. Only [`extend_connection_credit`](Connection::extend_connection_credit)
//! moves the figure; the scheduling module explains why counting the stream-level extension
//! as well would make the bound meaningless.
//!
//! # How a connection ends
//!
//! Every ending is latched the first time it is observed, and every later operation reports
//! the same one. There are five, and a caller can tell them apart because they are what
//! [`ErrorKind`] separates: the byte stream failed, it ended between records, it ended partway
//! through a record, the peer violated the protocol, or one of the two endpoints closed
//! deliberately. Only the first report carries the underlying cause as its source -- a boxed
//! error cannot be cloned -- so a caller who wants the transport's own message should keep the
//! first error rather than the last.

use core::task::{Context, Poll};

use crate::ccerr::CloseReason;
use crate::conn::{Conn, ReadOutcome, Role};
use crate::error::Error as CoreError;
use crate::handlers::Handlers;
use crate::io::clock::Clock;
use crate::io::close::encode_close_record;
use crate::io::error::{Error, ErrorKind, Result};
use crate::io::event::{Event, EventQueue};
use crate::io::framing::RecordFramer;
use crate::io::scheduling::{ReadAhead, Signals};
use crate::io::stream::{AsyncByteStream, Written};
use crate::params::TransportParams;
use crate::settings::Settings;
use crate::stream::{Directionality, StreamId};
use crate::stream_io::{OpenOutcome, Shutdown};
use crate::time::{Duration, Timestamp};
use crate::write::{Push, WriteRequest};

/// How many bytes the peer may send on any one stream before waiting for credit.
///
/// The same value for all three of the state machine's per-stream limits. Chosen to match
/// `ngnet-quic`'s equivalent, because the question -- how much in flight per stream is worth
/// buffering -- has nothing to do with which transport carries it.
pub const DEFAULT_STREAM_DATA: u64 = 256 * 1024;

/// How many bytes the peer may send across all streams before waiting for credit.
pub const DEFAULT_CONNECTION_DATA: u64 = 1024 * 1024;

/// How many streams of each kind the peer may open.
pub const DEFAULT_MAX_STREAMS: u64 = 100;

/// How many bytes may be delivered to the caller before it must report consuming some.
///
/// The same figure as [`DEFAULT_CONNECTION_DATA`], and deliberately: the connection window is
/// how much the peer may send before this side says anything, so a layer that held less than
/// that would refuse to read bytes the protocol has already permitted -- stalling a caller
/// that credits in batches rather than per event, which is a legitimate and common shape. A
/// larger figure would let the layer hold more than the protocol ever puts in flight, which
/// buys nothing and costs memory.
pub const DEFAULT_READ_AHEAD: u64 = DEFAULT_CONNECTION_DATA;

/// The size of the read buffer, and so the most bytes one read may deliver.
///
/// One record. A larger buffer would let a single read straddle several records, which is
/// harmless -- both the framer and the state machine accept any split -- but buys nothing,
/// since a record is the unit at which anything becomes actionable.
const READ_BUFFER: usize = crate::DEFAULT_MAX_RECORD_SIZE as usize;

/// What a connection advertises to its peer.
///
/// # Why this exists rather than [`TransportParams`]
///
/// Because [`TransportParams::new`] is all zeros. It reproduces `dwnx_transport_params_default`
/// faithfully and is documented as doing so, which is the right choice for a binding: "the
/// defaults" there means the library's defaults, not a second set invented in Rust. But a
/// connection built from them can open no streams and carry no data -- it has advertised
/// permission for none -- and it fails by hanging rather than by complaining.
///
/// A layer whose job is to be usable cannot inherit that. So this supplies working values of
/// its own, exactly as `ngnet-quic`'s endpoint configuration does for ngtcp2, and
/// `Config::default()` is a configuration that transfers data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    initial_max_stream_data: u64,
    initial_max_data: u64,
    max_streams_bidi: u64,
    max_streams_uni: u64,
    max_idle_timeout: Duration,
    read_ahead: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_max_stream_data: DEFAULT_STREAM_DATA,
            initial_max_data: DEFAULT_CONNECTION_DATA,
            max_streams_bidi: DEFAULT_MAX_STREAMS,
            max_streams_uni: DEFAULT_MAX_STREAMS,
            // Zero means "no idle timeout", and it is the honest value: nothing in dwnx or in
            // this layer enforces one, so advertising a number would invite the peer to
            // believe in a deadline that nobody is keeping. See [`crate::io::Clock`] for why
            // there is no timer here to keep it with.
            max_idle_timeout: Duration::from_nanos(0),
            read_ahead: DEFAULT_READ_AHEAD,
        }
    }
}

impl Config {
    /// The defaults, which are working values rather than the state machine's zeros.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many bytes the peer may send on each stream before waiting for credit.
    ///
    /// Maps onto all three of the state machine's per-stream limits at once --
    /// `initial_max_stream_data_bidi_local`, `_bidi_remote` and `_uni` -- because
    /// distinguishing them is a tuning decision this layer has no opinion about. A caller who
    /// wants them separate builds [`TransportParams`] directly and drives the state machine.
    #[must_use]
    pub const fn initial_max_stream_data(mut self, bytes: u64) -> Self {
        self.initial_max_stream_data = bytes;
        self
    }

    /// How many bytes the peer may send across all streams before waiting for credit.
    ///
    /// Setting this below [`Config::initial_max_stream_data`] lets one stream exhaust the
    /// whole connection window, which is a legitimate thing to want and an easy thing to do by
    /// accident.
    #[must_use]
    pub const fn initial_max_data(mut self, bytes: u64) -> Self {
        self.initial_max_data = bytes;
        self
    }

    /// How many bidirectional streams the peer may open.
    #[must_use]
    pub const fn max_streams_bidi(mut self, count: u64) -> Self {
        self.max_streams_bidi = count;
        self
    }

    /// How many unidirectional streams the peer may open.
    #[must_use]
    pub const fn max_streams_uni(mut self, count: u64) -> Self {
        self.max_streams_uni = count;
        self
    }

    /// How long the connection may sit idle, as advertised to the peer.
    ///
    /// Advertised and **not enforced**, in either direction: dwnx validates this parameter,
    /// encodes it, and has no code path that ends a connection for being idle. Setting it
    /// tells a peer that does enforce one what this side would tolerate; it does not give this
    /// side a timeout. A caller who needs liveness detection applies a deadline around the
    /// operation they are awaiting, or gets it from the substrate.
    #[must_use]
    pub const fn max_idle_timeout(mut self, timeout: Duration) -> Self {
        self.max_idle_timeout = timeout;
        self
    }

    /// How many bytes may be delivered to the caller before it must report consuming some.
    ///
    /// This layer's own bound on what it holds on the caller's behalf, and **not** a
    /// restatement of the protocol's receive window. It counts bytes handed over by
    /// [`Connection::poll_next_event`] that
    /// [`Connection::extend_connection_credit`] has not yet accounted for, so a caller that
    /// drains events into a buffer of its own without crediting them is stopped at this figure
    /// however diligently it drains. The number applies before any credit has been extended;
    /// afterwards each credited byte buys another byte of read-ahead.
    ///
    /// Setting it to zero means the layer reads nothing until the caller credits, which is a
    /// coherent configuration for a caller that wants to pull explicitly, and a deadlock for
    /// one that expects data to arrive unprompted.
    #[must_use]
    pub const fn read_ahead(mut self, bytes: u64) -> Self {
        self.read_ahead = bytes;
        self
    }

    /// The transport parameters this configuration describes.
    fn transport_params(self) -> TransportParams {
        TransportParams::new()
            .with_initial_max_stream_data_bidi_local(self.initial_max_stream_data)
            .with_initial_max_stream_data_bidi_remote(self.initial_max_stream_data)
            .with_initial_max_stream_data_uni(self.initial_max_stream_data)
            .with_initial_max_data(self.initial_max_data)
            .with_initial_max_streams_bidi(self.max_streams_bidi)
            .with_initial_max_streams_uni(self.max_streams_uni)
            .with_max_idle_timeout(self.max_idle_timeout)
    }
}

/// What a non-parking write did.
///
/// The answer [`Connection::try_write_stream`] gives, for a caller that has no
/// [`Context`] to park with. It is this layer's own type: the state machine's
/// [`Push`](crate::Push) describes the state of a *record being built* and invites another
/// push, which is a conversation only the code inside this file is in a position to have.
/// Exposing it would put dwnx's record-building protocol into the signature of every layer
/// above.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StreamWrite {
    /// This many bytes were taken, counted from the front of what was offered.
    ///
    /// May be fewer than offered, because a record holds a bounded amount and the peer's
    /// flow-control window may hold less again. The remainder is not lost and not sent: offer
    /// it again. A zero here means the offer was empty -- an end-of-stream marker carrying no
    /// data is accepted this way.
    Accepted(usize),

    /// Nothing was taken, and nothing will be until something changes.
    ///
    /// Either the peer's flow-control credit for this stream is exhausted and it must extend
    /// the window, or a record produced earlier has not finished reaching the byte stream and
    /// producing another would break the one-record-outstanding rule. A caller offers the same
    /// bytes again after the connection has been pumped.
    Blocked,

    /// The stream's write side is closed, so nothing will ever be taken.
    ///
    /// Distinct from [`StreamWrite::Blocked`] because retrying is pointless: the stream was
    /// finished, reset, or the peer asked this side to stop. A caller should abandon what it
    /// was sending rather than wait.
    Closed,
}

/// An asynchronous QMux connection over a caller-supplied byte stream.
///
/// Created from a byte stream the caller has **already established**; this crate connects
/// nothing and listens for nothing, which is why there is no third constructor. See the
/// [module documentation](super) for why the layer stops there.
///
/// Neither the byte stream nor the clock carries a `Send` bound, so a connection is `Send`
/// exactly when the caller's own values are.
pub struct Connection<S: AsyncByteStream, C: Clock> {
    stream: S,
    clock: C,
    conn: Conn<'static>,
    events: EventQueue,
    framer: RecordFramer,
    /// The read buffer, reused for the life of the connection.
    inbound: Vec<u8>,
    /// Produced record bytes on their way to the byte stream. Never more than one record.
    outbound: Vec<u8>,
    /// How much of `outbound` the byte stream has already accepted.
    written: usize,
    /// The buffer records are serialised into, reused for the life of the connection.
    scratch: Vec<u8>,
    /// Whether the state machine may have something to serialise.
    ///
    /// Set at construction -- which is what makes the transport-parameter announcement leave
    /// unprompted -- and again after every read and every operation that queues a frame.
    produce_pending: bool,
    /// The wakers parked by an operation that cannot proceed yet.
    signals: Signals,
    /// How far the layer has read ahead of the caller, and how far it may.
    read_ahead: ReadAhead,
    closing: Option<Closing>,
    terminal: Option<Terminal>,
}

/// How far a local close has got.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Closing {
    /// The close record is in the outbound buffer.
    Queued,
    /// It has reached the byte stream; the write side is being shut down.
    Written,
    /// The write side is down and the close is complete.
    Complete,
}

/// A latched ending.
///
/// Holds what can be reproduced on every later operation. The source of the original failure
/// is not here, because a `Box<dyn Error>` cannot be cloned and the alternative -- handing the
/// same box out once and nothing afterwards -- would make the error a caller sees depend on
/// how many times they asked.
#[derive(Debug)]
struct Terminal {
    kind: ErrorKind,
    context: &'static str,
    close: Option<CloseReason>,
}

impl Terminal {
    fn error(&self) -> Error {
        let error = Error::new(self.kind, self.context);
        match &self.close {
            Some(close) => error.with_close(close.clone()),
            None => error,
        }
    }
}

/// What one record's production achieved.
struct Produced {
    /// How many bytes of the offered payload went into the record.
    consumed: usize,
    /// How many record bytes were appended to the outbound buffer.
    bytes: usize,
    verdict: Verdict,
}

/// The stream-level answer a production came back with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    /// The record was built; `consumed` says how much of the payload it took.
    Packed,
    /// The nominated stream is flow-control blocked.
    Blocked,
    /// The nominated stream's write side is closed.
    Closed,
}

impl<S: AsyncByteStream, C: Clock> Connection<S, C> {
    /// A client connection over an established byte stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the state machine rejects the configuration or cannot allocate.
    pub fn client(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::new(Role::Client, stream, clock, config)
    }

    /// A server connection over an established byte stream.
    ///
    /// # Errors
    ///
    /// As [`Connection::client`].
    pub fn server(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::new(Role::Server, stream, clock, config)
    }

    fn new(role: Role, stream: S, clock: C, config: Config) -> Result<Self> {
        let events = EventQueue::new();
        let signals = Signals::new();
        let conn = Conn::builder(role)
            // Starting the connection's clock where the caller's clock is, rather than at
            // zero, so every interval dwnx computes is an interval in the caller's timescale.
            .settings(Settings::new().with_initial_timestamp(clock.now()))
            .transport_params(config.transport_params())
            .handlers(handlers(&events, &signals))
            .build()?;

        Ok(Self {
            stream,
            clock,
            conn,
            events,
            framer: RecordFramer::new(),
            inbound: vec![0; READ_BUFFER],
            outbound: Vec::new(),
            written: 0,
            scratch: vec![0; READ_BUFFER],
            // The announcement. Nothing can be opened until the peer's parameters arrive, and
            // they arrive only if the peer sent them -- so both sides must speak without being
            // spoken to, or two connections wait for each other and neither reports anything
            // wrong. Scheduling it here means the first pump emits it, whatever the first
            // entry point turns out to be.
            produce_pending: true,
            signals,
            read_ahead: ReadAhead::new(config.read_ahead),
            closing: None,
            terminal: None,
        })
    }

    /// Which side of the connection this is.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.conn.role()
    }

    /// The current time, from the caller's clock.
    #[must_use]
    pub fn now(&self) -> Timestamp {
        self.clock.now()
    }

    /// The timestamp of the most recent operation, as the state machine recorded it.
    ///
    /// A reading of the caller's own clock, not of a second one: every call this layer makes
    /// into the state machine passes [`Clock::now`] straight through, so this is a value the
    /// caller's clock produced.
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.conn.timestamp()
    }

    /// The peer's transport parameters, once they have arrived.
    #[must_use]
    pub fn peer_transport_params(&self) -> Option<&TransportParams> {
        self.conn.peer_transport_params()
    }

    /// Drives the connection: flush what is queued, produce what is pending, read what has
    /// arrived.
    ///
    /// Every other entry point does this first, so a caller never has to. It is public because
    /// a caller who is neither reading events nor writing -- one waiting on something else
    /// entirely -- still has to let the connection make progress.
    ///
    /// [`Poll::Ready`] means everything produced has reached the byte stream. [`Poll::Pending`]
    /// means bytes are still queued and the byte stream cannot take them yet; the waker fires
    /// when it can.
    ///
    /// # Errors
    ///
    /// Reports whichever ending the connection reached, including the orderly ones; see
    /// [`ErrorKind::is_orderly`].
    pub fn poll_pump(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.pump(cx) {
            return Poll::Ready(Err(error));
        }
        if self.outbound.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// The next thing that happened on the connection.
    ///
    /// Events are delivered in the order the protocol produced them, so several arising from a
    /// single read arrive as one sequence rather than collapsed into the last of them.
    ///
    /// Events queued before the connection ended are delivered *before* the ending is
    /// reported. A peer that sends its last record and its close in one write therefore has
    /// both observed, in that order, which is the difference between a clean shutdown and a
    /// lost final message.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending once the queue is empty.
    pub fn poll_next_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event>> {
        let pumped = self.pump(cx);
        if let Some(event) = self.events.pop() {
            if let Event::StreamData { data, .. } = &event {
                // Delivery is what read-ahead is measured in: from here the bytes are the
                // caller's, and the layer will read no further ahead than the caller has
                // credited back. Counted on the way out rather than when the event was queued,
                // because an event sitting in the queue is bounded by the protocol's window
                // and one the caller is holding is bounded by nothing else.
                self.read_ahead.delivered(data.len() as u64);
            }
            return Poll::Ready(Ok(event));
        }
        match pumped {
            Ok(()) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// How many delivered bytes the caller has yet to report consuming.
    ///
    /// The layer's own read-ahead figure, bounded by [`Config::read_ahead`] plus everything
    /// [`Connection::extend_connection_credit`] has accounted for. A caller that watches this
    /// climb to the bound and stay there is watching backpressure work: the layer has stopped
    /// reading, and the peer's window is closing behind it.
    #[must_use]
    pub const fn read_ahead(&self) -> u64 {
        self.read_ahead.outstanding()
    }

    /// Opens a bidirectional stream.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending. Exhausted stream capacity is not an error: it is
    /// [`Poll::Pending`], because the peer may raise the limit at any time.
    pub fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId>> {
        self.poll_open(cx, OpenKind::Bidi)
    }

    /// Opens a unidirectional stream.
    ///
    /// # Errors
    ///
    /// As [`Connection::poll_open_bidi`].
    pub fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId>> {
        self.poll_open(cx, OpenKind::Uni)
    }

    fn poll_open(&mut self, cx: &mut Context<'_>, kind: OpenKind) -> Poll<Result<StreamId>> {
        if let Err(error) = self.pump(cx) {
            return Poll::Ready(Err(error));
        }

        let opened = match kind {
            OpenKind::Bidi => self.conn.open_bidi_stream(),
            OpenKind::Uni => self.conn.open_uni_stream(),
        };

        match opened {
            Ok(OpenOutcome::Opened(stream)) => {
                self.produce_pending = true;
                Poll::Ready(Ok(stream))
            }
            // Capacity is the peer's to grant, and it grants it in a MAX_STREAMS frame this
            // side has yet to read. Parked against the `extend_max_streams` callback, which is
            // dwnx reporting exactly that frame; waking here instead would spin a whole core
            // for as long as the peer took to answer.
            Ok(OpenOutcome::Blocked) => {
                self.signals.park_open(cx);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(Error::from(error))),
        }
    }

    /// Writes to a stream, waiting when there is no credit for any of it.
    ///
    /// Returns how many bytes were taken, which may be fewer than offered: a record holds a
    /// bounded amount and the peer's window may hold less again. The payload is **split**
    /// across as many records as it needs rather than truncated to one, and the remainder is
    /// neither sent nor lost -- offer it again.
    ///
    /// `fin` marks the end of the stream. It is applied to the record that takes the last of
    /// the payload, so it survives a payload split across records; offering no data with `fin`
    /// set sends the end-of-stream marker on its own.
    ///
    /// This is the form for a caller that has a [`Context`]. Where an immediate answer is
    /// needed instead, see [`Connection::try_write_stream`].
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and reports a stream whose write side is closed as
    /// [`ErrorKind::Internal`] -- a request this side should not have made, rather than
    /// anything the peer did.
    pub fn poll_write_stream(
        &mut self,
        cx: &mut Context<'_>,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Poll<Result<usize>> {
        if let Err(error) = self.pump(cx) {
            return Poll::Ready(Err(error));
        }

        let mut written = 0usize;
        loop {
            let produced = match self.write_record(cx, stream, &data[written..], fin) {
                Err(error) => return Poll::Ready(Err(error)),
                // The byte stream cannot take the record that is already queued, so producing
                // another would break the one-record-outstanding rule. Its own waker is
                // registered, so no re-arm is needed here.
                Ok(None) => {
                    return if written > 0 {
                        Poll::Ready(Ok(written))
                    } else {
                        Poll::Pending
                    };
                }
                Ok(Some(produced)) => produced,
            };

            // Counted before the verdict is read, and for every verdict. A record that took
            // part of the payload and *then* ran out of window reports both the count and the
            // refusal, and the bytes it took are already on their way to the peer: dropping
            // the count because the verdict was a refusal would have the caller offer them a
            // second time and the stream would carry them twice.
            written += produced.consumed;

            match produced.verdict {
                Verdict::Closed if written == 0 => {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Internal,
                        "the stream's write side is closed",
                    )));
                }
                Verdict::Closed => return Poll::Ready(Ok(written)),
                Verdict::Blocked => return self.park_write(cx, written),
                Verdict::Packed => {
                    if written == data.len() {
                        return Poll::Ready(Ok(written));
                    }
                    // Nothing taken from a non-empty payload: dwnx reports an exhausted
                    // *connection* window this way rather than as a blocked stream, and
                    // producing again would spin.
                    if produced.consumed == 0 {
                        return self.park_write(cx, written);
                    }
                }
            }
        }
    }

    /// Writes to a stream without ever waiting.
    ///
    /// The form for a caller with no [`Context`] to park with -- which is not a hypothetical
    /// audience: the HTTP/3 layer offers its outbound bytes through a synchronous closure that
    /// is handed a stream, some slices and a verdict to return, and a transport that could
    /// only park would have nothing legal to do inside it.
    ///
    /// The payload is split rather than truncated, exactly as in
    /// [`Connection::poll_write_stream`]; what differs is that exhausted credit comes back as
    /// [`StreamWrite::Blocked`] instead of parking, and a finished stream as
    /// [`StreamWrite::Closed`] instead of an error. Nothing is written to the byte stream
    /// here: the record joins the outbound buffer and leaves on the next pump.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and a failed production, which is fatal.
    pub fn try_write_stream(
        &mut self,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<StreamWrite> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        // Producing into a buffer that still holds a record would queue two, and the second
        // cannot be written before the first regardless. Reporting it as blocked is honest and
        // needs no new variant: the caller's response -- offer the same bytes again later --
        // is the same one exhausted credit calls for.
        if !self.outbound.is_empty() {
            return Ok(StreamWrite::Blocked);
        }

        let produced = self.produce(WriteRequest::stream(stream, data).with_fin(fin))?;
        // The count outranks the verdict. A record can take part of the payload and only then
        // find the window exhausted, and those bytes leave on the next pump whatever the
        // verdict says; answering `Blocked` and losing the count would have the caller offer
        // them again and the peer would receive them twice.
        if produced.consumed > 0 {
            return Ok(StreamWrite::Accepted(produced.consumed));
        }
        Ok(match produced.verdict {
            Verdict::Closed => StreamWrite::Closed,
            Verdict::Blocked => StreamWrite::Blocked,
            Verdict::Packed if !data.is_empty() => StreamWrite::Blocked,
            Verdict::Packed => StreamWrite::Accepted(0),
        })
    }

    /// Shuts down one or both halves of a stream, telling the peer why.
    ///
    /// The read half sends STOP_SENDING and the write half RESET_STREAM, so either is visible
    /// to the peer with the application error code supplied.
    ///
    /// The frames this queues leave on the next pump. Shutting down a stream that does not
    /// exist is not an error -- the state machine looks the id up and reports success when it
    /// finds nothing, and that behaviour is passed through rather than papered over.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and anything the state machine refuses.
    pub fn shutdown_stream(
        &mut self,
        stream: StreamId,
        half: Shutdown,
        app_error_code: u64,
    ) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.shutdown_stream(stream, half, app_error_code)?;
        self.produce_pending = true;
        Ok(())
    }

    /// Reports bytes consumed on a stream, so the peer may send that much more.
    ///
    /// This moves the *protocol's* per-stream window and nothing else. It does not relieve
    /// this layer's read-ahead bound: a caller reporting the same bytes to both windows --
    /// which is what the HTTP/3 layer above does, because stream-level credit does not imply
    /// connection-level credit -- would otherwise credit each consumed byte twice, and a bound
    /// that fell twice as fast as it rose would never bind at all. See
    /// [`Connection::extend_connection_credit`], which is the one that counts.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and anything the state machine refuses -- extending a
    /// stream this endpoint never receives on, for instance.
    pub fn extend_stream_credit(&mut self, stream: StreamId, bytes: u64) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.extend_max_stream_data(stream, bytes)?;
        self.produce_pending = true;
        Ok(())
    }

    /// Reports bytes consumed across the connection, so the peer may send that much more.
    ///
    /// Separate from [`Connection::extend_stream_credit`] because the two windows are separate:
    /// stream-level credit does not imply connection-level credit, and a caller who extends
    /// only one leaves the other to run out.
    ///
    /// This is also the call that governs the layer's own read-ahead. Bytes credited here
    /// cancel bytes delivered by [`Connection::poll_next_event`], and a connection that
    /// stopped reading because the caller was behind resumes on this call. A caller that
    /// consumes events and never credits will be read to exactly once and then stopped, which
    /// is the bound doing its job rather than a fault.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending.
    pub fn extend_connection_credit(&mut self, bytes: u64) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.extend_max_data(bytes);
        self.read_ahead.credited(bytes);
        if !self.read_ahead.is_exhausted() {
            // The pump stopped reading and registered its waker here rather than with the byte
            // stream, so nothing else will ever fire it: the byte stream has been ready this
            // whole time and the layer was declining to look.
            self.signals.wake_read_ahead();
        }
        self.produce_pending = true;
        Ok(())
    }

    /// Permits the peer to open `count` more streams of one kind.
    ///
    /// The counterpart to [`Connection::extend_stream_credit`] for the other resource a peer
    /// can exhaust. Stream capacity is *not* returned when a stream closes -- neither dwnx nor
    /// this layer recycles it -- so a connection that never calls this stops accepting new
    /// streams for good once the configured limit has been reached, which presents as a peer
    /// whose opens hang rather than as an error at either end.
    ///
    /// The MAX_STREAMS frame this queues leaves on the next pump, and the peer's blocked open
    /// wakes when it arrives.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending.
    pub fn extend_stream_limit(&mut self, kind: Directionality, count: usize) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        match kind {
            Directionality::Bidirectional => self.conn.extend_max_streams_bidi(count),
            Directionality::Unidirectional => self.conn.extend_max_streams_uni(count),
        }
        self.produce_pending = true;
        Ok(())
    }

    /// Closes the connection, telling the peer why.
    ///
    /// Four steps, in an order that matters. Whatever is already queued goes out first, so the
    /// close does not overtake a record the peer is midway through reading. The encoded close
    /// record is appended. It is flushed. Then the write side of the byte stream is shut down,
    /// so the peer's read reports end of stream rather than waiting for bytes that will never
    /// come.
    ///
    /// Nothing further is produced once the close is queued: the connection is over, and a
    /// record serialised after the close would arrive after it or not at all.
    ///
    /// Poll until [`Poll::Ready`]. Abandoning this partway leaves the close in a buffer, and a
    /// peer that never receives one cannot tell a deliberate shutdown from a crash.
    ///
    /// # Errors
    ///
    /// Reports a byte-stream failure encountered while writing the close or shutting down.
    pub fn poll_close(&mut self, cx: &mut Context<'_>, reason: &CloseReason) -> Poll<Result<()>> {
        if self.closing == Some(Closing::Complete) {
            return Poll::Ready(Ok(()));
        }

        if self.closing.is_none() {
            match self.flush(cx) {
                Ok(true) => {}
                Ok(false) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
            self.outbound
                .extend_from_slice(&encode_close_record(reason));
            self.closing = Some(Closing::Queued);
            // Latched now rather than when the shutdown completes, so an operation issued
            // between the two reports the close that is already on its way rather than
            // appearing to succeed.
            let _ = self.fail(
                Error::new(
                    ErrorKind::LocallyClosed,
                    "the connection was closed locally",
                )
                .with_close(reason.clone()),
            );
        }

        if self.closing == Some(Closing::Queued) {
            match self.flush(cx) {
                Ok(true) => self.closing = Some(Closing::Written),
                Ok(false) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }

        match self.stream.poll_shutdown(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.closing = Some(Closing::Complete);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(
                self.fail_stream(error, "the byte stream failed while shutting down")
            )),
        }
    }

    /// Flush, produce, read -- and one more write pass for whatever the read left to say.
    fn pump(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }

        self.write_side(cx)?;
        self.read_side(cx)?;
        if self.produce_pending {
            self.write_side(cx)?;
        }
        Ok(())
    }

    /// Writes what is queued and produces what is pending, alternating.
    ///
    /// The alternation is the point: production happens only into an empty outbound buffer, so
    /// each record is fully written before the next exists.
    fn write_side(&mut self, cx: &mut Context<'_>) -> Result<()> {
        loop {
            if !self.flush(cx)? {
                return Ok(());
            }
            if !self.produce_pending || self.closing.is_some() {
                return Ok(());
            }

            let produced = self.produce(WriteRequest::control_only())?;
            if produced.bytes == 0 || produced.verdict != Verdict::Packed {
                // An empty record means the state machine had nothing queued. The other two
                // verdicts name a stream, and a control-only request names none, so they
                // cannot arise here -- but they are answered rather than ignored, because the
                // alternative is a loop that never ends if that ever stops being true.
                self.produce_pending = false;
                return Ok(());
            }
        }
    }

    /// Offers the outbound buffer to the byte stream until it is empty or refuses.
    ///
    /// Returns whether the buffer is now empty, which is also the question "may another record
    /// be produced".
    fn flush(&mut self, cx: &mut Context<'_>) -> Result<bool> {
        while self.written < self.outbound.len() {
            match self.stream.poll_write(cx, &self.outbound[self.written..]) {
                Poll::Pending | Poll::Ready(Ok(Written::NotNow)) => return Ok(false),
                Poll::Ready(Err(error)) => {
                    return Err(self.fail_stream(error, "the byte stream failed while writing"));
                }
                Poll::Ready(Ok(Written::Accepted(0))) => {
                    // Forbidden by the contract, because zero bytes accepted carries no
                    // obligation to wake and a caller offered it can only spin. This is the
                    // one wake this layer issues to itself, and it is not a scheduling
                    // placeholder: it is what an implementation that broke the rule gets
                    // instead of a connection that stalls in silence, and it is unreachable
                    // for one that keeps it.
                    cx.waker().wake_by_ref();
                    return Ok(false);
                }
                Poll::Ready(Ok(Written::Accepted(taken))) => {
                    self.written = self.outbound.len().min(self.written + taken);
                }
            }
        }

        self.outbound.clear();
        self.written = 0;
        Ok(true)
    }

    /// Reads from the byte stream until it has nothing more, feeding the framer and then the
    /// state machine.
    ///
    /// Stops early when the caller is behind. That is the whole of the read-ahead bound: an
    /// unread byte stream is backpressure the peer can feel, and it costs this side nothing to
    /// hold.
    fn read_side(&mut self, cx: &mut Context<'_>) -> Result<()> {
        loop {
            if self.read_ahead.is_exhausted() {
                // No read is issued, so the byte stream registers nothing -- which is why the
                // waker goes here instead, to be fired by the credit that makes room. A read
                // issued anyway would deliver bytes the caller has no room for and defeat the
                // bound; parking without registering anything would strand the connection.
                self.signals.park_read_ahead(cx);
                return Ok(());
            }

            let filled = match self.stream.poll_read(cx, &mut self.inbound) {
                Poll::Pending => return Ok(()),
                Poll::Ready(Err(error)) => {
                    return Err(self.fail_stream(error, "the byte stream failed while reading"));
                }
                Poll::Ready(Ok(0)) => return Err(self.ended()),
                Poll::Ready(Ok(filled)) => filled.min(self.inbound.len()),
            };

            // The framer first. It is what latches the peer's close record, and the state
            // machine may report that close before this chunk is exhausted -- so the record
            // has to be in hand before the outcome below is acted on.
            if let Err(error) = self.framer.consume(&self.inbound[..filled]) {
                return Err(self.fail(error));
            }

            let now = self.clock.now();
            // Sampled across the read because a MAX_DATA frame raises this and raises nothing
            // else: dwnx applies it to the connection's send window and invokes no callback
            // (`deps/dwnx/lib/dwnx_conn.c:1045-1056`), so a write parked on an exhausted
            // connection window has no event to wait for and this comparison is its wakeup.
            // Waking on any inbound bytes would have done as well, and would have spun a
            // blocked writer once per record for as long as the peer kept sending.
            let credit_before = self.conn.max_data_left();
            let outcome = self.conn.read(&self.inbound[..filled], now);
            if self.conn.max_data_left() > credit_before {
                self.signals.wake_credit();
            }
            // Whatever arrived may have queued a response -- a window extension, a ping
            // answer -- and the pump's trailing write pass is what sends it.
            self.produce_pending = true;

            match outcome {
                Ok(ReadOutcome::Processed) => {}
                Ok(ReadOutcome::PeerClosed) => return Err(self.peer_closed()),
                Err(error) => return Err(self.fail(Error::from(error))),
            }
        }
    }

    /// Flushes, then produces one record for `stream`, then flushes again.
    ///
    /// Returns [`None`] when the outbound buffer could not be emptied, in which case nothing
    /// was produced.
    fn write_record(
        &mut self,
        cx: &mut Context<'_>,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<Option<Produced>> {
        if !self.flush(cx)? {
            return Ok(None);
        }
        let produced = self.produce(WriteRequest::stream(stream, data).with_fin(fin))?;
        self.flush(cx)?;
        Ok(Some(produced))
    }

    /// Serialises one record and appends it to the outbound buffer.
    ///
    /// A failure here is fatal and is latched as such; see the module documentation for why a
    /// retry would desynchronise the stream.
    fn produce(&mut self, request: WriteRequest<'_>) -> Result<Produced> {
        let now = self.clock.now();
        // Spelled out as a free function over three fields rather than as a method, because
        // the record writer borrows the connection and the scratch buffer for as long as the
        // record is being built, and the produced bytes are then copied out of the scratch
        // buffer into the outbound one. Splitting the borrows by field is what makes that
        // legal without a copy through a temporary.
        match pack(
            &mut self.conn,
            &mut self.scratch,
            &mut self.outbound,
            request,
            now,
        ) {
            Ok(produced) => Ok(produced),
            Err(error) => Err(self.fail(Error::from(error).with_context(
                "serialising a record failed, which loses whatever it had already packed",
            ))),
        }
    }

    /// Classifies a byte stream that reported end of stream.
    fn ended(&mut self) -> Error {
        if let Some(close) = self.framer.close_reason() {
            return self.fail(
                Error::new(ErrorKind::PeerClosed, "the peer closed the connection")
                    .with_close(close),
            );
        }
        if self.framer.at_boundary() {
            self.fail(Error::new(
                ErrorKind::EndOfStream,
                "the byte stream ended between records",
            ))
        } else {
            self.fail(Error::new(
                ErrorKind::TruncatedRecord,
                "the byte stream ended partway through a record",
            ))
        }
    }

    /// Builds the peer's close out of the record the framer latched.
    fn peer_closed(&mut self) -> Error {
        let error = Error::new(ErrorKind::PeerClosed, "the peer closed the connection");
        let error = match self.framer.close_reason() {
            Some(close) => error.with_close(close),
            // The state machine reported a close the framer did not find, which means the
            // record carried a frame the decoder could not walk past. The ending is still a
            // peer close; only the explanation is missing.
            None => error,
        };
        self.fail(error)
    }

    /// Latches an ending, if this is the first, and hands the error back.
    fn fail(&mut self, error: Error) -> Error {
        if self.terminal.is_none() {
            self.terminal = Some(Terminal {
                kind: error.kind(),
                context: error.context(),
                close: error.close_reason().cloned(),
            });
        }
        error
    }

    /// Latches a byte-stream failure, keeping the transport's own error as the source.
    fn fail_stream(&mut self, source: S::Error, context: &'static str) -> Error {
        self.fail(Error::new(ErrorKind::ByteStream, context).with_boxed_source(source.into()))
    }

    /// Reports a partial write, or waits for the peer to extend a window.
    ///
    /// Parked against [`Signals::park_credit`], which the `extend_max_stream_data` callback
    /// fires for a stream window and [`Connection::read_side`] fires for the connection window
    /// -- dwnx raises no callback for the latter. A partial write is reported rather than
    /// waited on, because the bytes that were taken are already the peer's problem and a
    /// caller told nothing about them would send them twice.
    fn park_write(&self, cx: &mut Context<'_>, written: usize) -> Poll<Result<usize>> {
        if written > 0 {
            return Poll::Ready(Ok(written));
        }
        self.signals.park_credit(cx);
        Poll::Pending
    }
}

/// Which kind of stream an open is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OpenKind {
    Bidi,
    Uni,
}

/// Builds one record into `scratch` and appends it to `outbound`.
///
/// Free rather than a method so the connection, the scratch buffer and the outbound buffer are
/// borrowed as three separate things: the record writer holds the first two for as long as the
/// record is being built, and the third receives the bytes afterwards.
///
/// # The error path
///
/// `?` on a push is what makes a failure fatal. It drops the writer mid-record, whose `Drop`
/// finalises the record so dwnx stops writing through the buffer -- and discards the bytes,
/// having already advanced the send offset of any stream whose data went in. The caller must
/// fail the connection rather than try again.
///
/// One class of failure is exempt, and the exemption is load-bearing. A push naming a stream
/// the state machine no longer has -- because it was reset, by either end, since the caller
/// last looked -- is refused before the record is begun, so nothing has been packed and
/// nothing has been lost. It is reported as a closed stream, which is what it is. Treating it
/// as fatal instead kills a working connection every time a caller offers bytes for a stream
/// that was reset while those bytes were queued, which is the ordinary shape of a cancelled
/// exchange rather than an edge case: the caller above holds a backlog, the reset discards
/// it, and the next offer names a stream that is gone.
///
/// The exemption is conditional on nothing having been consumed yet. Once part of the
/// payload is in the record, a refusal is no longer the simple "this stream cannot take
/// bytes" it appears to be, and the caller has an accepted count to be told about instead.
fn pack(
    conn: &mut Conn<'static>,
    scratch: &mut [u8],
    outbound: &mut Vec<u8>,
    request: WriteRequest<'_>,
    now: Timestamp,
) -> core::result::Result<Produced, CoreError> {
    let mut consumed = 0usize;
    let mut remaining = request.data;
    let mut verdict = Verdict::Packed;

    let mut writer = conn.record(scratch, now);
    loop {
        let step = WriteRequest {
            stream: request.stream,
            data: remaining,
            fin: request.fin,
        };
        let pushed = match writer.push(step) {
            Ok(pushed) => pushed,
            Err(error) if consumed == 0 && error.kind() == crate::ErrorKind::Stream => {
                verdict = Verdict::Closed;
                break;
            }
            Err(error) => return Err(error),
        };
        match pushed {
            Push::Accepted { consumed: taken } => {
                let taken = taken.unwrap_or(0);
                consumed += taken;
                remaining = &remaining[taken..];
                if remaining.is_empty() {
                    break;
                }
            }
            Push::Complete { consumed: taken } => {
                consumed += taken.unwrap_or(0);
                break;
            }
            Push::StreamBlocked => {
                verdict = Verdict::Blocked;
                break;
            }
            Push::StreamClosed => {
                verdict = Verdict::Closed;
                break;
            }
        }
    }

    // Finished even when a stream said no: the record may still carry control frames that were
    // packed before the stream was consulted, and abandoning it would discard them.
    let record = writer.finish()?;
    let bytes = record.bytes().unwrap_or(&[]);
    outbound.extend_from_slice(bytes);

    Ok(Produced {
        consumed,
        bytes: bytes.len(),
        verdict,
    })
}

/// The handlers the layer installs on the state machine.
///
/// They capture the event queue and the signal set and nothing else, which is what satisfies
/// the state machine's `Send` bound on handlers without imposing one on the caller's byte
/// stream or clock. A handler cannot reach the connection by design, so each one records and
/// returns; the pump acts once the entry point that provoked it has returned.
///
/// Two of them do one thing more: they fire a waker. That is not the connection being reached
/// into -- a waker is a scheduling primitive, not a connection handle, and the operation it
/// wakes still runs from a poll like any other. It is how a blocked open and a blocked write
/// learn that the frame they were waiting for has arrived, rather than by asking again on
/// every pass.
fn handlers(events: &EventQueue, signals: &Signals) -> Handlers<'static> {
    let data = events.clone();
    let opened = events.clone();
    let closed = events.clone();
    let reset = events.clone();
    let stop_sending = events.clone();
    let stream_credit = events.clone();
    let limits = events.clone();
    let params = events.clone();
    let credit_signal = signals.clone();
    let limit_signal = signals.clone();

    Handlers::new()
        .on_stream_data(move |event| {
            data.push(Event::StreamData {
                stream_id: event.stream_id,
                offset: event.offset,
                data: event.data.to_vec(),
                fin: event.fin,
            });
            Ok(())
        })
        .on_stream_open(move |stream_id| {
            opened.push(Event::StreamOpened { stream_id });
            Ok(())
        })
        .on_stream_close(move |event| {
            closed.push(Event::StreamClosed {
                stream_id: event.stream_id,
                rx_app_error_code: event.rx_app_error_code,
                tx_app_error_code: event.tx_app_error_code,
            });
            Ok(())
        })
        .on_stream_reset(move |stream_id, final_size, app_error_code| {
            reset.push(Event::StreamReset {
                stream_id,
                final_size,
                app_error_code,
            });
            Ok(())
        })
        .on_recv_stop_sending(move |stream_id, app_error_code| {
            stop_sending.push(Event::StopSending {
                stream_id,
                app_error_code,
            });
            Ok(())
        })
        .on_extend_max_stream_data(move |stream_id, max_data| {
            stream_credit.push(Event::StreamDataCredit {
                stream_id,
                max_data,
            });
            // The event a write parked on an exhausted stream window is waiting for. dwnx
            // raises this both for a MAX_STREAM_DATA frame and for the peer's transport
            // parameters granting a stream its first window, which is exactly the pair of
            // occasions on which a blocked write can make progress.
            credit_signal.wake_credit();
            Ok(())
        })
        .on_extend_max_streams(move |kind, max_streams| {
            limits.push(Event::StreamLimit { kind, max_streams });
            // The event a blocked open is waiting for, and the only one: stream capacity is
            // the peer's to grant and it grants it here.
            limit_signal.wake_open();
            Ok(())
        })
        .on_transport_params(move |received| {
            params.push(Event::PeerTransportParams(received.clone()));
            Ok(())
        })
}

// Written out rather than derived: neither the byte stream nor the clock is required to be
// `Debug`, and the buffers are noise. What a reader wants is the role and how far along the
// connection is.
impl<S: AsyncByteStream, C: Clock> core::fmt::Debug for Connection<S, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("role", &self.role())
            .field("outbound", &(self.outbound.len() - self.written))
            .field("read_ahead", &self.read_ahead.outstanding())
            .field("closing", &self.closing)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the state machine's own defaults do not have, and the reason this type
    /// exists at all.
    #[test]
    fn the_default_configuration_permits_data_and_streams() {
        let params = Config::default().transport_params();
        assert!(params.initial_max_data() > 0);
        assert!(params.initial_max_stream_data_bidi_local() > 0);
        assert!(params.initial_max_stream_data_bidi_remote() > 0);
        assert!(params.initial_max_stream_data_uni() > 0);
        assert!(params.initial_max_streams_bidi() > 0);
        assert!(params.initial_max_streams_uni() > 0);
    }

    /// The state machine's defaults, for contrast: every one of them is zero, which is what a
    /// connection that inherited them would advertise.
    #[test]
    fn the_state_machines_defaults_permit_nothing() {
        let params = TransportParams::new();
        assert_eq!(params.initial_max_data(), 0);
        assert_eq!(params.initial_max_streams_bidi(), 0);
    }

    #[test]
    fn the_builders_reach_the_parameters_they_name() {
        let params = Config::new()
            .initial_max_stream_data(7)
            .initial_max_data(11)
            .max_streams_bidi(3)
            .max_streams_uni(5)
            .max_idle_timeout(Duration::from_nanos(13))
            .transport_params();

        assert_eq!(params.initial_max_stream_data_bidi_local(), 7);
        assert_eq!(params.initial_max_stream_data_bidi_remote(), 7);
        assert_eq!(params.initial_max_stream_data_uni(), 7);
        assert_eq!(params.initial_max_data(), 11);
        assert_eq!(params.initial_max_streams_bidi(), 3);
        assert_eq!(params.initial_max_streams_uni(), 5);
        assert_eq!(params.max_idle_timeout(), Duration::from_nanos(13));
    }

    /// The read-ahead allowance is the one knob here that never reaches the wire: it governs how
    /// far this layer will run ahead of its caller, not what the peer is told. Getting it wrong in
    /// the direction of zero would stall the connection, so it is checked separately from the
    /// transport parameters.
    #[test]
    fn the_read_ahead_allowance_is_a_local_knob_with_a_non_zero_default() {
        assert_eq!(Config::default().read_ahead, DEFAULT_READ_AHEAD);
        const { assert!(DEFAULT_READ_AHEAD > 0) };
        assert_eq!(Config::new().read_ahead(64).read_ahead, 64);
    }

    /// The configuration dwnx would abort on, rejected as an ordinary error instead.
    #[test]
    fn a_configuration_the_state_machine_asserts_on_is_refused() {
        let params = Config::new().initial_max_data(u64::MAX).transport_params();
        assert!(params.validate().is_err());
    }
}
