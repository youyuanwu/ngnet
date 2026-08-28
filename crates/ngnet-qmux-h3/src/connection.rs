//! The transport the HTTP/3 layer runs over.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use http_body::Body;
use ngnet_h3::http::{
    Config as H3Config, Connection as H3Connection, IncomingBody, QuicConnection, QuicEvent,
    Result as H3Result, SendRequest, StreamSource,
};
use ngnet_h3::{ErrorCode, StreamId as H3StreamId, Timestamp as H3Timestamp};
use ngnet_qmux::io::{
    AsyncByteStream, Clock, Config, Connection as LayerConnection, Error as LayerError,
    ErrorKind as LayerErrorKind,
};
use ngnet_qmux::{CloseKind, CloseReason, Initiator, Role, Shutdown, StreamId as LayerStreamId};

use crate::error::{Error, ErrorKind, Result};
use crate::event::{ends_a_stream, qmux_stream, stream_id, translate};
use crate::{pump, transmit};

/// How the connection ended, kept so every later call can report the same thing.
struct Ending {
    /// The failure a caller sees, rendered once. See [`Error`] for why it is not the
    /// original.
    error: Error,
    /// Whether this was an ending rather than a failure.
    ///
    /// The distinction decides what the HTTP/3 layer is told. An orderly ending becomes
    /// [`QuicEvent::Closed`], which the driver treats as "the peer is gone" and winds down
    /// on, reporting success to a caller whose exchanges had already finished. A failure is
    /// returned as an error, which fails the connection. Reporting a peer that hung up
    /// politely as a failure would turn every well-behaved client's disconnection into a
    /// server-side protocol error.
    orderly: bool,
    /// The application error code the close carried, where there was one.
    code: Option<u64>,
}

impl Ending {
    fn new(error: &LayerError) -> Self {
        // Only an application close carries a code the HTTP/3 layer can use. A transport
        // close, or a stream that simply ended, is "the connection is over" as far as it is
        // concerned, and inventing a code for it would be a lie about what the peer said.
        let code = error
            .close_reason()
            .filter(|reason| reason.kind() == CloseKind::Application)
            .map(CloseReason::error_code);
        Self {
            error: Error::layer(error),
            orderly: error.kind().is_orderly(),
            code,
        }
    }
}

/// Window extensions the HTTP/3 layer has reported and this connection has not yet applied.
///
/// # Why they are held at all
///
/// The HTTP/3 driver does not coalesce them. `Driver::extend`
/// (`crates/ngnet-h3/src/http/driver.rs`) makes **two** `extend_credit` calls for the same
/// bytes -- the stream's window and the connection's, which are separate and neither implies
/// the other -- and it is reached from three places in one pass: once per `QuicEvent::Data`
/// applied, once per stream whose deferred credit was released, and once per credit entry the
/// caller returned by reading. A pass that delivers a body in eight parts therefore forwards
/// sixteen calls, and every one of them reached the layer below.
///
/// Each of those calls costs a call into dwnx and sets `produce_pending`, and each *connection*
/// extension additionally fires the read-ahead waker. None of that is expensive on its own and
/// all of it is redundant: the frames dwnx emits depend on the window's total, not on how many
/// times it was told about it, so applying one sum has exactly the effect of applying its
/// parts. What changes is the number of times the layer below is disturbed.
///
/// # Why holding them is safe
///
/// A run of these is flushed at the *first* transport interaction that follows it --
/// `poll_event`, `poll_transmit`, an open, a reset, a stop-sending, a close, or the tail --
/// so no credit outlives the run of `extend_credit` calls that produced it. That is stricter
/// than "flush in `poll_transmit`", which would be enough for the driver as it is written
/// today: the pass reaches `poll_transmit` on every iteration, but it can *park* before it at
/// `poll_open_bi` waiting for stream capacity, and credit stranded there is a window the peer
/// never hears about while both ends wait for the other. The rejected alternative is that
/// narrower rule, and it was rejected because it makes this crate's correctness depend on the
/// shape of a loop in another crate.
///
/// `now` is the one method that does not flush, because it touches nothing: the driver calls
/// it in the middle of a run of credit reports, and flushing there would break exactly the run
/// this exists to coalesce.
#[derive(Default)]
struct PendingCredit {
    /// Per-stream sums, in the order the streams were first reported.
    ///
    /// A `Vec` with a linear scan rather than a map: a run holds one entry per stream that
    /// delivered in the pass, which is single digits in every workload measured, and a hash
    /// per report would cost more than the scan it replaced. Bounded by [`MAX_PENDING_STREAMS`]
    /// so the scan cannot grow without limit.
    streams: Vec<(LayerStreamId, u64)>,
    /// The connection-level sum, which every stream contributes to.
    connection: u64,
}

impl PendingCredit {
    fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.connection == 0
    }
}

/// How many streams a run may accumulate before it is applied early.
///
/// A bound rather than a tuning knob. The scan above is linear, and a pass that delivered on
/// hundreds of streams would make it quadratic in the number of streams; applying early costs
/// exactly what not batching cost, so the worst case is the behaviour this replaced rather
/// than something worse. Sixty-four is the driver's own per-pass offer bound, which is the
/// nearest thing to a natural figure available.
const MAX_PENDING_STREAMS: usize = 64;

/// Everything the HTTP/3 layer's transport needs, behind one lock.
///
/// See the [crate documentation](crate) for why it is shared at all, and why the lock is a
/// `std::sync::Mutex` when nothing here is threaded.
pub(crate) struct Inner<S: AsyncByteStream, C: Clock> {
    /// The QMux connection. Owned here and reachable from nowhere else.
    pub(crate) conn: LayerConnection<S, C>,
    /// How the connection ended, once it has.
    ending: Option<Ending>,
    /// Which end this is, for deciding what a peer-opened stream means.
    local: Initiator,
    /// Releases awaiting collection, produced by the transmit pass.
    releases: VecDeque<QuicEvent>,
    /// One translated event held back, so a stream ending can start its own batch.
    ///
    /// A lookahead rather than a wholesale drain of the layer's queue: that queue is what
    /// the layer's read-ahead bound counts, and emptying it into a buffer here would tell
    /// the layer its reader had caught up when the bytes had merely moved.
    next: Option<QuicEvent>,
    /// Whether any event has been handed over since the last time `poll_event` was pending.
    ///
    /// The HTTP/3 driver drains events in batches and applies the control-plane ones before
    /// the data ones *within* a batch. A stream ending therefore has to start a batch of its
    /// own: put in the same batch as the last bytes of that stream, the close is applied
    /// first, the stream is released, and the bytes that follow are read against a stream
    /// the state machine has already forgotten. That is a protocol error on the ordinary
    /// path where a response ends, not an edge case.
    emitted_since_pending: bool,
    /// Whether the ending has been handed to the HTTP/3 layer.
    reported_ending: bool,
    /// What [`QuicConnection::close`] asked for, waiting to be written.
    close: Option<CloseReason>,
    /// Whether the tail has run to completion.
    finished: bool,
    /// Window extensions reported by the HTTP/3 layer and not yet applied.
    credit: PendingCredit,
    /// How many times credit has been applied to the layer below.
    ///
    /// The instrument the batching is stated over. It counts *applications*, one per
    /// `extend_stream_credit` or `extend_connection_credit` call, so it is directly
    /// comparable with the number of `extend_credit` calls the HTTP/3 driver made: before
    /// batching the two were equal by construction, and the distance between them now is what
    /// the batching is worth. Kept in release builds as well as test ones because it is one
    /// `u64` add on a path that already takes a lock, and a counter compiled out of the build
    /// that is measured is not evidence about that build.
    credit_applications: u64,
    /// How many of those were connection-window extensions.
    ///
    /// Counted apart because it is the sharper of the two figures. Every stream that
    /// delivered in a pass needs its own stream-window extension whether or not the reports
    /// are batched, so the stream half can only shrink when one stream delivers twice; the
    /// connection window is one window shared by all of them, and the driver reports it once
    /// per delivery. It is also the extension that wakes the read-ahead pump, so this is the
    /// count of wakeups the batching removes.
    connection_credit_applications: u64,
}

impl<S: AsyncByteStream, C: Clock> Inner<S, C> {
    fn new(conn: LayerConnection<S, C>) -> Self {
        let local = match conn.role() {
            Role::Client => Initiator::Client,
            Role::Server => Initiator::Server,
        };
        Self {
            conn,
            ending: None,
            local,
            releases: VecDeque::new(),
            next: None,
            emitted_since_pending: false,
            reported_ending: false,
            close: None,
            finished: false,
            credit: PendingCredit::default(),
            credit_applications: 0,
            connection_credit_applications: 0,
        }
    }

    /// Records credit the HTTP/3 layer reported, without applying it.
    ///
    /// See [`PendingCredit`] for why it is held and for how long.
    fn defer_credit(&mut self, stream: Option<LayerStreamId>, bytes: u64) {
        if bytes == 0 {
            return;
        }
        match stream {
            Some(stream) => {
                if let Some(entry) = self.credit.streams.iter_mut().find(|(id, _)| *id == stream) {
                    entry.1 = entry.1.saturating_add(bytes);
                } else {
                    self.credit.streams.push((stream, bytes));
                }
            }
            // The connection window, shared across every stream. The layer reports the same
            // bytes to both levels, and extending only the stream stalls the whole connection
            // once enough has flowed in total -- late, and with nothing to explain it.
            None => self.credit.connection = self.credit.connection.saturating_add(bytes),
        }
        if self.credit.streams.len() >= MAX_PENDING_STREAMS {
            self.flush_credit();
        }
    }

    /// Applies everything [`Inner::defer_credit`] has accumulated.
    ///
    /// Called from every entry point that reaches the layer below, so a run of credit reports
    /// is applied before anything can observe or depend on the windows it moves.
    fn flush_credit(&mut self) {
        if self.credit.is_empty() {
            return;
        }
        if self.ending.is_some() {
            // Nothing below will act on it, and the layer below reports an ended connection
            // as an error rather than ignoring the call. Dropped rather than applied, which
            // is what the unbatched path did by returning early for the same reason.
            self.credit.streams.clear();
            self.credit.connection = 0;
            return;
        }
        for (stream, bytes) in self.credit.streams.drain(..) {
            // Absorbed rather than propagated. The layer below refuses to extend a stream
            // this endpoint never receives on, which is exactly what the HTTP/3 layer asks
            // for when it consumes from a stream that has since gone: it reports the bytes
            // it read without asking whether the stream is still there, and it has no way to
            // tell such a refusal from a real one. Failing the connection over it would kill
            // a healthy connection on the ordinary path where a request is cancelled.
            let _ = self.conn.extend_stream_credit(stream, bytes);
            self.credit_applications += 1;
        }
        let connection = core::mem::take(&mut self.credit.connection);
        if connection > 0 {
            let _ = self.conn.extend_connection_credit(connection);
            self.credit_applications += 1;
            self.connection_credit_applications += 1;
        }
    }

    /// Whether the connection has ended, in either sense.
    pub(crate) fn has_ended(&self) -> bool {
        self.ending.is_some()
    }

    /// Latches how the connection ended, keeping the first answer.
    ///
    /// The first is the true one: everything after it is the layer reproducing its own
    /// terminal, and a later call that overwrote it would replace "the peer sent us a
    /// malformed record" with "the connection is closed".
    pub(crate) fn end(&mut self, error: &LayerError) {
        if self.ending.is_none() {
            self.ending = Some(Ending::new(error));
        }
    }

    /// Queues a release for bytes the transport has taken a copy of.
    pub(crate) fn record_released(&mut self, stream: H3StreamId, bytes: u64) {
        self.releases.push_back(QuicEvent::Released {
            stream,
            bytes,
            // Never false. This transport copies what it accepts into the record it is
            // building, and nothing here cancels a send and hands the buffer back; `false`
            // exists for transports that do.
            delivered: true,
        });
    }

    /// The failure to report for an operation on a connection that has ended.
    fn ended(&self) -> Error {
        self.ending.as_ref().map_or_else(
            || Error::new(ErrorKind::Closed, "the connection has ended"),
            |ending| ending.error.clone(),
        )
    }

    /// Pulls from the layer until there is an event to hand over, or nothing left to pull.
    fn fill(&mut self, cx: &mut Context<'_>) {
        while self.next.is_none() {
            match self.conn.poll_next_event_buffered(cx) {
                Poll::Ready(Ok(event)) => self.next = translate(event, self.local),
                Poll::Ready(Err(error)) => {
                    self.end(&error);
                    break;
                }
                Poll::Pending => break,
            }
        }
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent>> {
        // Reading an event is an interaction with the layer below like any other. It is not
        // where the saving is -- the driver drains events before it reports anything -- but
        // the rule is worth more than the exception: a run of credit reports ends at the next
        // interaction, without a list of which kinds count.
        self.flush_credit();

        // Queued work still pumps first so a terminal discovered here remains latched
        // behind that work. With no queued work, `fill` performs the one required pump
        // through `poll_next_event_buffered`; pumping here as well would read the transport
        // twice before checking the same lower event queue.
        if !self.releases.is_empty() || self.next.is_some() {
            pump::pump_buffered(self, cx);
        }

        // Releases first. They belong to bytes the layer handed over earlier and hold its
        // buffers until they are delivered, so nothing is served by making them queue behind
        // whatever the peer happens to have sent.
        if let Some(event) = self.releases.pop_front() {
            self.emitted_since_pending = true;
            return Poll::Ready(Ok(event));
        }

        if self.next.is_none() {
            self.fill(cx);
        }
        if let Some(event) = &self.next {
            if ends_a_stream(event) && self.emitted_since_pending {
                // Starts a fresh batch. See `emitted_since_pending`.
                self.emitted_since_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let event = self.next.take().expect("just observed");
            self.emitted_since_pending = true;
            return Poll::Ready(Ok(event));
        }

        if let Some(ending) = self.ending.as_ref().filter(|_| !self.reported_ending) {
            let (orderly, code, error) = (ending.orderly, ending.code, ending.error.clone());
            if self.emitted_since_pending {
                // The ending closes every stream at once, so it obeys the same batching rule
                // for the same reason.
                self.emitted_since_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.reported_ending = true;
            self.emitted_since_pending = true;
            return if orderly {
                Poll::Ready(Ok(QuicEvent::Closed {
                    code: code.map(ErrorCode::new),
                }))
            } else {
                Poll::Ready(Err(error))
            };
        }

        // Once the ending has been reported there is nothing further to say, and saying it
        // twice would restart the driver's wind-down. Parking here is safe rather than a
        // hang: the driver checks whether the peer is gone before it parks on this call.
        self.emitted_since_pending = false;
        Poll::Pending
    }

    fn poll_open(&mut self, cx: &mut Context<'_>, bidi: bool) -> Poll<Result<H3StreamId>> {
        // The one that must not be forgotten. This can park -- an open waits for stream
        // capacity the peer has not granted -- and credit stranded behind a park is a window
        // the peer is never told about while both ends wait for the other.
        self.flush_credit();
        pump::pump_buffered(self, cx);
        if self.ending.is_some() {
            return Poll::Ready(Err(self.ended()));
        }
        let opened = if bidi {
            self.conn.poll_open_bidi_buffered(cx)
        } else {
            self.conn.poll_open_uni_buffered(cx)
        };
        match opened {
            // The open itself is only a record. Keep it with the request or control bytes the
            // driver is about to offer; `poll_flush` writes it before any operation can park.
            Poll::Ready(Ok(id)) => {
                pump::pump_buffered(self, cx);
                Poll::Ready(Ok(stream_id(id)))
            }
            Poll::Ready(Err(error)) => {
                self.end(&error);
                Poll::Ready(Err(self.ended()))
            }
            // The peer's stream limit, not a failure. The layer waits, and the wake the
            // layer below registered is what releases it.
            Poll::Pending => Poll::Pending,
        }
    }

    /// Runs whatever has to happen after the HTTP/3 driver has finished with the connection.
    ///
    /// See [`QmuxConnection::poll_finish`], which is the public face of this.
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.finished {
            return Poll::Ready(());
        }
        // The tail writes what is queued and shuts the write side down. Credit still held
        // here would be a window the peer was owed and never told about.
        self.flush_credit();
        let done = match &self.close {
            // Encodes the close if it has not been encoded, appends it behind whatever is
            // still queued, flushes, and shuts the write side down. Its failure is
            // deliberately discarded: this is the last thing that happens to a connection
            // that is already over, and there is nobody left to tell.
            Some(reason) => self.conn.poll_close(cx, reason).is_ready(),
            // No close was asked for -- the driver failed, or was dropped. What it wrote
            // before that still has to reach the peer, and then the write side has to be
            // shut down like any other ending. Pumping alone would be enough for a bare
            // socket, whose drop produces a FIN anyway, and wrong for every stream that
            // wraps one: `poll_shutdown` is what flushes a buffered writer or finishes a
            // part-built TLS record, so without it the bytes just pumped are discarded.
            None => self.conn.poll_pump(cx).is_ready() && self.conn.poll_finish(cx).is_ready(),
        };
        if done {
            self.finished = true;
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// An established QMux connection, in the shape the HTTP/3 layer runs over.
///
/// Built by [`QmuxConnection::client`] or [`QmuxConnection::server`] from a byte stream that
/// is already connected, and normally reached through [`connect`](crate::connect) or
/// [`serve`](crate::serve) rather than directly.
///
/// A caller driving the [`QuicConnection`] methods itself must poll
/// [`QuicConnection::poll_flush`] before returning `Pending` to its executor. Productive event,
/// open, and transmit calls may retain bounded QMux output so that adjacent internal passes can
/// coalesce it; the suspension flush drains that output or registers its write wake.
/// [`poll_finish`](Self::poll_finish) remains the separate completion boundary.
///
/// # Why this is a handle and not the connection
///
/// The HTTP/3 driver takes its transport **by value** and holds it for the connection's
/// life, and it calls [`close`](QuicConnection::close) and then returns without polling the
/// transport again. Something outside the driver therefore has to write the close that call
/// queued, which means something outside the driver has to be able to reach the connection.
/// So this is a handle onto shared state, and [`poll_finish`](Self::poll_finish) is the
/// other end of it.
///
/// The lock is a `std::sync::Mutex` rather than a `RefCell`. Not for threading — the two
/// holders are never polled at once, and this crate spawns nothing. It is because a
/// `Mutex<T>` can be built for a `T` that is neither `Send` nor `Sync` while still being
/// `Send` when `T` is, so a connection over a `Send` byte stream can be handed to a work
/// stealing runtime and a connection over an `Rc`-based one is still served. A `RefCell`
/// would rule out the first; an `Arc<Mutex<..>>` demanded of the byte stream would rule out
/// the second.
pub struct QmuxConnection<S: AsyncByteStream, C: Clock> {
    shared: Arc<Mutex<Inner<S, C>>>,
}

impl<S: AsyncByteStream, C: Clock> QmuxConnection<S, C> {
    /// Starts a client connection over an established byte stream.
    ///
    /// The byte stream is expected to be connected already: QMux runs over an ordered,
    /// reliable substrate and has no notion of dialling one.
    ///
    /// # Errors
    ///
    /// Fails if the QMux state machine cannot be built.
    pub fn client(stream: S, clock: C) -> Result<Self> {
        Self::client_with(stream, clock, Config::new())
    }

    /// Starts a client connection with transport settings other than the defaults.
    ///
    /// [`Self::client`] is this with [`Config::new`], and nothing else: the defaults are an
    /// argument like any other rather than a separate code path, so a connection built here
    /// and a connection built there differ only in what they advertise.
    ///
    /// # What the configuration governs
    ///
    /// Everything on [`Config`] except `read_ahead` becomes a transport parameter this end
    /// *advertises to the peer*, which makes every one of them a permission granted rather
    /// than a limit imposed on oneself. `initial_max_stream_data` and `initial_max_data` are
    /// how many bytes the peer may send on one stream and across the connection before it
    /// has to wait for credit; `max_streams_bidi` and `max_streams_uni` are how many streams
    /// of each kind the peer may open. So a caller who wants *its own* requests to be able to
    /// send a large body sets the credit on the **other** end's configuration, and a client
    /// that wants to open many streams needs the **server** to have raised
    /// `max_streams_bidi`. `read_ahead` is the exception and is purely local: it bounds how
    /// many bytes this layer will hand upward before the HTTP/3 layer reports having consumed
    /// some, and setting it below `initial_max_data` throttles a peer that was told it could
    /// send more.
    ///
    /// HTTP/3 itself needs three unidirectional streams before it will do anything at all, so
    /// a `max_streams_uni` below three yields a connection whose peer can never start. That
    /// is the same hazard as the one below, met sooner.
    ///
    /// # The stream allowance is not recycled
    ///
    /// Worth stating here because it is where a caller meets it, and because the failure it
    /// produces does not look like a failure. Stream capacity is **not** returned when a
    /// stream closes — neither dwnx nor the QMux layer recycles it — and this crate never
    /// calls [`ngnet_qmux::io::Connection::extend_stream_limit`]. A connection therefore
    /// admits exactly `max_streams_bidi` client-opened requests over its whole life, however
    /// long ago the earlier ones finished, and the one after that **hangs**: the peer's open
    /// simply never completes, no error is reported at either end, and a caller sees a future
    /// that never resolves rather than something it can handle.
    ///
    /// A caller reusing one connection across many exchanges should therefore size this for
    /// the connection's whole lifetime rather than for its concurrency, which is what it
    /// would mean in a stack that recycled. The ceiling is dwnx's `DWNX_MAX_STREAMS`, which is
    /// `1 << 60`; a value above it is not rejected here but is rejected by the peer when it
    /// decodes the parameters, which fails the connection during setup.
    ///
    /// # Errors
    ///
    /// Fails if the QMux state machine cannot be built, which includes a configuration whose
    /// values do not fit the encoding the transport parameters use.
    pub fn client_with(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::build(LayerConnection::client(stream, clock, config))
    }

    /// Starts a server connection over an established byte stream. See [`Self::client`].
    ///
    /// # Errors
    ///
    /// Fails if the QMux state machine cannot be built.
    pub fn server(stream: S, clock: C) -> Result<Self> {
        Self::server_with(stream, clock, Config::new())
    }

    /// Starts a server connection with transport settings other than the defaults.
    ///
    /// The server half of [`Self::client_with`], which describes what the configuration
    /// governs and why a low stream allowance presents as a hang rather than as an error.
    /// The description applies unchanged, but the *consequences* land differently, because
    /// what a QMux end advertises is what its peer may do: this configuration's
    /// `max_streams_bidi` is the number of requests the client may ever open, and its
    /// `initial_max_stream_data` is the size of request body the client may send. A server
    /// that means to accept large uploads or long-lived connections sets them here; nothing
    /// the client configures for itself can raise them.
    ///
    /// # Errors
    ///
    /// Fails if the QMux state machine cannot be built, which includes a configuration whose
    /// values do not fit the encoding the transport parameters use.
    pub fn server_with(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::build(LayerConnection::server(stream, clock, config))
    }

    fn build(built: ngnet_qmux::io::Result<LayerConnection<S, C>>) -> Result<Self> {
        let conn = built.map_err(|error| Error::layer(&error))?;
        Ok(Self {
            shared: Arc::new(Mutex::new(Inner::new(conn))),
        })
    }

    /// Finishes the connection off after the HTTP/3 layer has let go of it.
    ///
    /// Writes the close [`QuicConnection::close`] queued, or flushes what is left if there
    /// was none, and shuts the write side of the byte stream down. Resolves once there is
    /// nothing further this end can do, including when the connection has already failed —
    /// a peer that is gone is not something to keep waiting for.
    ///
    /// [`Connection`] does this for a caller. It is public because a caller who drove
    /// [`ngnet_h3::http::handshake`] themselves has the same obligation and no other way to
    /// discharge it: the driver's last act is to call `close` and return, so a connection
    /// nobody polled afterwards leaves the close in a buffer and the peer waiting out its
    /// idle timeout. Such a hand-driven caller must also use
    /// [`QuicConnection::poll_flush`] before task suspension, as described on
    /// [`QmuxConnection`].
    pub fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.with(|inner| inner.poll_finish(cx))
    }

    /// How many times this connection has applied window credit to the layer below.
    ///
    /// One per `extend_stream_credit` or `extend_connection_credit` call made on the QMux
    /// connection, which is directly comparable with the number of
    /// [`QuicConnection::extend_credit`] calls the HTTP/3 driver made: before those calls were
    /// batched the two figures were equal by construction, so the distance between them is
    /// what the batching is worth. That is the measurement Spec FR-037 asks for, and it is
    /// taken in `tests/ngnet-qmux-h3-tests/tests/credit_batching.rs`.
    ///
    /// `#[doc(hidden)]` for the same reason `ngnet_qmux::io::testing` is: a test instrument
    /// has to be reachable from a test in another crate, and the only way to do that is to
    /// make it public. It is not part of the supported surface and carries no stability
    /// promise. The alternative -- a counter compiled only under `cfg(test)` -- was rejected
    /// because it is not reachable from an integration test at all, and a figure taken from a
    /// build nobody ships is not evidence about the build they do.
    #[doc(hidden)]
    #[must_use]
    pub fn credit_applications(&self) -> u64 {
        self.with(|inner| inner.credit_applications)
    }

    /// How many of those were connection-window extensions.
    ///
    /// The sharper half of the measurement, and the one that counts read-ahead wakeups. See
    /// [`Self::credit_applications`] for why this is reachable at all.
    #[doc(hidden)]
    #[must_use]
    pub fn connection_credit_applications(&self) -> u64 {
        self.with(|inner| inner.connection_credit_applications)
    }

    /// How many times the lower QMux pump has been entered.
    ///
    /// Debug-only and doc-hidden like the lower counter it forwards. Tests that use it carry
    /// the same gate, and release benchmarks therefore do not measure the instrument.
    #[doc(hidden)]
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn pump_calls(&self) -> u64 {
        self.with(|inner| inner.conn.pump_calls())
    }

    /// A second handle onto the same connection, for the tail to hold.
    pub(crate) fn share(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }

    fn with<R>(&self, act: impl FnOnce(&mut Inner<S, C>) -> R) -> R {
        // Poisoning is not a failure mode worth propagating here: the only way to poison
        // this lock is to panic inside the HTTP/3 driver, and the state it guards is
        // consistent at every point one of these closures can unwind from.
        act(&mut self.shared.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl<S: AsyncByteStream, C: Clock> core::fmt::Debug for QmuxConnection<S, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.with(|inner| {
            f.debug_struct("QmuxConnection")
                .field("role", &inner.conn.role())
                .field("ended", &inner.ending.is_some())
                .finish_non_exhaustive()
        })
    }
}

impl<S: AsyncByteStream, C: Clock> QuicConnection for QmuxConnection<S, C> {
    type Error = Error;

    /// This transport copies what it is given.
    ///
    /// A write is packed into the record being built, which is the connection's own buffer,
    /// so the HTTP/3 layer's memory is its own again the moment the write returns and
    /// release is reported from the same place as the accepted count.
    ///
    /// Retaining instead was considered and rejected. QMux has no acknowledgement signal to
    /// release on — the substrate underneath it is already reliable and ordered, which is
    /// the whole reason this protocol exists — so a retaining implementation would have to
    /// invent a moment to release at, and the only honest one is the moment the copy is
    /// made.
    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent>> {
        self.with(|inner| inner.poll_event(cx))
    }

    fn poll_transmit<Src: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut Src,
    ) -> Poll<Result<()>> {
        self.with(|inner| {
            // Before the pass, so the frames the extensions queue leave with it.
            inner.flush_credit();
            transmit::drain(inner, cx, source);
            // A failure met here is not reported here. The driver reaches `poll_event`
            // after every transmit pass, and reporting the ending from one place keeps the
            // "orderly endings are an event, failures are an error" decision in one place
            // too.
            Poll::Ready(Ok(()))
        })
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.with(|inner| {
            inner.flush_credit();
            let ended_before = inner.has_ended();
            if pump::pump(inner, cx) {
                return Poll::Ready(Ok(()));
            }
            if inner.has_ended() {
                // The operation which parked will report this ending in its existing shape.
                // Only a newly discovered ending needs a wake; repeating it would spin.
                if !ended_before {
                    cx.waker().wake_by_ref();
                }
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        })
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<H3StreamId>> {
        self.with(|inner| inner.poll_open(cx, false))
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<H3StreamId>> {
        self.with(|inner| inner.poll_open(cx, true))
    }

    fn reset(&mut self, stream: H3StreamId, code: ErrorCode) -> Result<()> {
        self.shutdown(stream, Shutdown::Write, code)
    }

    fn stop_sending(&mut self, stream: H3StreamId, code: ErrorCode) -> Result<()> {
        self.shutdown(stream, Shutdown::Read, code)
    }

    /// Records the extension; the layer below is told at the next interaction with it.
    ///
    /// The HTTP/3 driver forwards these one at a time and twice per delivery -- see the
    /// `PendingCredit` type in this module for the count, for why holding a run of them is safe,
    /// and for what was rejected in its place. It is private, so this is a prose reference
    /// rather than a link: the batching is an implementation detail of this seam and nothing a
    /// caller can name. The audit FR-037 asked for came back "not coalesced", and
    /// this is where they are coalesced instead: `ngnet-h3` is shared with the QUIC stack,
    /// and a change made at this seam leaves it untouched.
    fn extend_credit(&mut self, stream: Option<H3StreamId>, bytes: u64) -> Result<()> {
        self.with(|inner| {
            if inner.ending.is_some() {
                return Ok(());
            }
            inner.defer_credit(stream.map(qmux_stream), bytes);
            Ok(())
        })
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<()> {
        self.with(|inner| {
            // The close is written by the tail, and the tail flushes credit too -- but the
            // rule is that a run of credit reports ends at the next interaction of any kind,
            // and an exception here would be one more thing to be right about.
            inner.flush_credit();
            // Recorded, not written. This method has no `Context`, and writing a close means
            // waiting for a byte stream that may not be taking bytes — so the close is
            // encoded and flushed by `poll_finish`, which does have one. A transport that
            // tried to write it here could only drop it when the stream was full, and a
            // dropped close leaves the peer waiting out an idle timeout instead of learning
            // why the connection ended.
            if inner.close.is_none() {
                inner.close = Some(CloseReason::application(code.get(), reason));
            }
            Ok(())
        })
    }

    fn now(&self) -> H3Timestamp {
        // The connection's clock, never one of this crate's own: the layer below stamps its
        // idle timeout against it, and a second clock with a different origin makes every
        // timestamp the HTTP/3 layer records incomparable with it.
        H3Timestamp::from_nanos(self.with(|inner| inner.conn.now()).as_nanos())
    }
}

impl<S: AsyncByteStream, C: Clock> QmuxConnection<S, C> {
    fn shutdown(&mut self, stream: H3StreamId, half: Shutdown, code: ErrorCode) -> Result<()> {
        self.with(|inner| {
            // A reset or a stop-sending is an interaction with the layer below, so a run of
            // credit reports ends here as it does anywhere else.
            inner.flush_credit();
            if inner.ending.is_some() {
                return Ok(());
            }
            match inner
                .conn
                .shutdown_stream(qmux_stream(stream), half, code.get())
            {
                Ok(()) => Ok(()),
                // A refusal from the state machine is absorbed, for the same reason as in
                // `extend_credit`: the HTTP/3 layer resets streams it has stopped tracking
                // as a matter of course -- a cancelled request whose peer had already
                // finished, for instance -- and it has no way to distinguish a stream that
                // is gone from one that never existed. Failing the connection over a frame
                // that would have told the peer something it already knows would kill a
                // healthy connection on the ordinary cancellation path.
                Err(error) if error.kind() == LayerErrorKind::Internal => Ok(()),
                Err(error) => {
                    inner.end(&error);
                    Err(inner.ended())
                }
            }
        })
    }
}

/// This crate's connection future: the HTTP/3 driver, plus what has to happen after it.
///
/// Resolves with whatever the HTTP/3 driver resolved with, but not until the byte stream has
/// been dealt with. **Nothing moves until it is polled**, and dropping it early abandons the
/// connection without telling the peer.
#[must_use = "a connection does nothing until it is polled"]
pub struct Connection<S: AsyncByteStream, C: Clock, D> {
    driving: Option<D>,
    outcome: Option<H3Result<()>>,
    tail: QmuxConnection<S, C>,
}

impl<S: AsyncByteStream, C: Clock, D> Connection<S, C, D> {
    fn new(driving: D, tail: QmuxConnection<S, C>) -> Self {
        Self {
            driving: Some(driving),
            outcome: None,
            tail,
        }
    }
}

impl<S, C, D> Future for Connection<S, C, D>
where
    S: AsyncByteStream,
    C: Clock,
    D: Future<Output = H3Result<()>> + Unpin,
{
    type Output = H3Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Every field is `Unpin`, so the pinning is a formality and this crate needs no
        // projection and no `unsafe` to see through it.
        let this = self.get_mut();

        if let Some(driving) = this.driving.as_mut() {
            match Pin::new(driving).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(outcome) => {
                    this.outcome = Some(outcome);
                    this.driving = None;
                }
            }
        }

        // The tail. The driver's last act is to call `close` and return, so this is the only
        // thing that will ever write it — and the outcome is held back until it has, because
        // a caller who saw `Ok(())` and dropped the connection would drop the close with it.
        match this.tail.poll_finish(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => Poll::Ready(this.outcome.take().unwrap_or(Ok(()))),
        }
    }
}

impl<S: AsyncByteStream, C: Clock, D> core::fmt::Debug for Connection<S, C, D> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("driving", &self.driving.is_some())
            .finish_non_exhaustive()
    }
}

/// What [`connect`] hands back: a request sender and the connection that serves it.
///
/// An alias only so the signature can be read at a glance. `F` is the HTTP/3 driver's own
/// future, which has no name of its own.
pub type Connected<S, C, B, F> = (SendRequest<B>, Connection<S, C, H3Connection<F>>);

/// Starts an HTTP/3 client over an established byte stream.
///
/// Returns a handle for making requests and the connection that performs them; the
/// connection must be polled for anything to happen, and polling it to completion is what
/// gets a close onto the wire.
///
/// # Errors
///
/// Fails if the QMux connection or the HTTP/3 layer cannot be built.
pub fn connect<S, C, B>(
    stream: S,
    clock: C,
) -> Result<Connected<S, C, B, impl Future<Output = H3Result<()>>>>
where
    S: AsyncByteStream,
    C: Clock,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    connect_with::<S, C, B>(stream, clock, Config::new(), H3Config::default())
}

/// Starts an HTTP/3 client with settings other than the defaults.
///
/// [`connect`] is this with [`Config::new`] and [`ngnet_h3::http::Config::default`], so the
/// two agree by construction rather than by being kept in step.
///
/// # Why two configurations rather than one
///
/// They belong to different layers and answer different questions, and this crate is the
/// join between those layers rather than a restatement of either. `transport` is QMux's:
/// flow-control credit, how many streams the peer may open, the advertised idle timeout, the
/// local read-ahead bound — see [`QmuxConnection::client_with`] for what each governs.
/// `http` is HTTP/3's: the bound on a header block (`max_field_section_size`), the QPACK
/// dynamic table and blocked-stream allowances, how many events the driver takes per pass,
/// and — on a server — how many handler futures may be in flight at once. Merging them into
/// one type would oblige this crate to restate an HTTP/3 configuration surface it does not
/// own and would have to track, which is exactly the coupling the join exists to avoid.
///
/// One quantity is worth naming because it lives on both: a limit on concurrent streams. The
/// HTTP/3 configuration's `max_concurrent_streams` bounds the server's in-flight handlers and
/// never reaches the wire, while the transport configuration's `max_streams_bidi` is a
/// transport parameter the peer is bound by. They are not two names for one setting and
/// setting one does not set the other; a caller who means to permit N concurrent requests
/// has to say so in both places.
///
/// # The stream allowance is not recycled
///
/// Restated here because this is the entry point most callers use, and because it is the one
/// way a plausible configuration produces a connection that neither works nor fails. QMux
/// stream capacity is spent permanently: the transport configuration's `max_streams_bidi` is
/// the number of requests the peer may open over the connection's *whole life*, not the
/// number it may have outstanding, and this crate never extends it. A client that reuses one
/// connection for more exchanges than the server allowed finds the next request's future
/// simply never resolving — the open waits for capacity that will not arrive, and neither end
/// reports an error. Size it for the connection's lifetime, and treat a request that hangs on
/// a long-lived connection as this until proven otherwise.
///
/// # Errors
///
/// Fails if the QMux connection or the HTTP/3 layer cannot be built, which includes a
/// transport configuration whose values do not fit the encoding the transport parameters use.
pub fn connect_with<S, C, B>(
    stream: S,
    clock: C,
    transport: Config,
    http: H3Config,
) -> Result<Connected<S, C, B, impl Future<Output = H3Result<()>>>>
where
    S: AsyncByteStream,
    C: Clock,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let backend = QmuxConnection::client_with(stream, clock, transport)?;
    let tail = backend.share();
    let (sender, driving) =
        ngnet_h3::http::handshake_with::<_, B>(backend, http).map_err(Error::http3)?;
    Ok((sender, Connection::new(driving, tail)))
}

/// Serves HTTP/3 over an established byte stream.
///
/// Each request the peer makes is passed to `handler`, whose response is sent back. The
/// connection resolves when the peer is done with it. See [`connect`] for the client side.
///
/// # Errors
///
/// Fails if the QMux connection or the HTTP/3 layer cannot be built.
pub fn serve<S, C, H, F, B>(
    stream: S,
    clock: C,
    handler: H,
) -> Result<Connection<S, C, H3Connection<impl Future<Output = H3Result<()>>>>>
where
    S: AsyncByteStream,
    C: Clock,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    serve_with(stream, clock, handler, Config::new(), H3Config::default())
}

/// Serves HTTP/3 with settings other than the defaults.
///
/// The server half of [`connect_with`], which explains why the two configurations stay
/// separate and how the two concurrency limits differ. What changes on this side is who is
/// constrained by what: a QMux end advertises permissions to its peer, so the `transport`
/// argument here is what decides how many requests *the client* may open
/// (`max_streams_bidi`) and how large a request body it may send
/// (`initial_max_stream_data`, `initial_max_data`). A client cannot raise either by
/// configuring itself. The `http` argument's `max_concurrent_streams`, by contrast, is
/// entirely local: it bounds how many handler futures this driver holds at once and is never
/// advertised.
///
/// # The stream allowance is not recycled
///
/// A server sets the number that binds, so this is its hazard more than the client's. QMux
/// stream capacity is never returned when a stream closes and this crate never extends it, so
/// `max_streams_bidi` is the number of requests this connection will serve over its whole
/// life. Once they are spent, further opens are not refused — they are simply never granted,
/// which the client sees as a request that hangs and this end sees as a connection that has
/// gone quiet. A server holding connections open across many exchanges should size the
/// allowance for the connection's lifetime rather than for the concurrency it expects.
///
/// # Errors
///
/// Fails if the QMux connection or the HTTP/3 layer cannot be built, which includes a
/// transport configuration whose values do not fit the encoding the transport parameters use.
pub fn serve_with<S, C, H, F, B>(
    stream: S,
    clock: C,
    handler: H,
    transport: Config,
    http: H3Config,
) -> Result<Connection<S, C, H3Connection<impl Future<Output = H3Result<()>>>>>
where
    S: AsyncByteStream,
    C: Clock,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let backend = QmuxConnection::server_with(stream, clock, transport)?;
    let tail = backend.share();
    let driving = ngnet_h3::http::serve_with(backend, handler, http).map_err(Error::http3)?;
    Ok(Connection::new(driving, tail))
}

const _: () = {
    // The abstraction imposes no `Send` bound, and this implementation adds none beyond what
    // its parts already require. A caller on a thread-per-core runtime hands in an
    // `Rc`-based byte stream and gets a connection that is simply not `Send`.
    fn _assert_error_is_boxable<E: Into<Box<dyn core::error::Error + Send + Sync>>>() {}
    fn _check() {
        _assert_error_is_boxable::<Error>();
    }
};
