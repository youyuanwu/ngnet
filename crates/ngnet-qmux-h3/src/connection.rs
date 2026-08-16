//! The transport the HTTP/3 layer runs over.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use http_body::Body;
use ngnet_h3::http::{
    Connection as H3Connection, IncomingBody, QuicConnection, QuicEvent, Result as H3Result,
    SendRequest, StreamSource,
};
use ngnet_h3::{ErrorCode, StreamId as H3StreamId, Timestamp as H3Timestamp};
use ngnet_qmux::io::{
    AsyncByteStream, Clock, Config, Connection as LayerConnection, Error as LayerError,
    ErrorKind as LayerErrorKind,
};
use ngnet_qmux::{CloseKind, CloseReason, Initiator, Role, Shutdown};

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
            match self.conn.poll_next_event(cx) {
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
        pump::pump(self, cx);

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
        pump::pump(self, cx);
        if self.ending.is_some() {
            return Poll::Ready(Err(self.ended()));
        }
        let opened = if bidi {
            self.conn.poll_open_bidi(cx)
        } else {
            self.conn.poll_open_uni(cx)
        };
        match opened {
            // The open itself is only a record; pumping again is what puts it on the wire,
            // and the peer will not answer on a stream it has not heard of.
            Poll::Ready(Ok(id)) => {
                pump::pump(self, cx);
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
        Self::build(LayerConnection::client(stream, clock, Config::new()))
    }

    /// Starts a server connection over an established byte stream. See [`Self::client`].
    ///
    /// # Errors
    ///
    /// Fails if the QMux state machine cannot be built.
    pub fn server(stream: S, clock: C) -> Result<Self> {
        Self::build(LayerConnection::server(stream, clock, Config::new()))
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
    /// idle timeout.
    pub fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.with(|inner| inner.poll_finish(cx))
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
            transmit::drain(inner, cx, source);
            // A failure met here is not reported here. The driver reaches `poll_event`
            // after every transmit pass, and reporting the ending from one place keeps the
            // "orderly endings are an event, failures are an error" decision in one place
            // too.
            Poll::Ready(Ok(()))
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

    fn extend_credit(&mut self, stream: Option<H3StreamId>, bytes: u64) -> Result<()> {
        self.with(|inner| {
            if inner.ending.is_some() {
                return Ok(());
            }
            let extended = match stream {
                Some(stream) => inner.conn.extend_stream_credit(qmux_stream(stream), bytes),
                // The connection window, shared across every stream. The layer reports the
                // same bytes to both levels, and extending only the stream stalls the whole
                // connection once enough has flowed in total — late, and with nothing to
                // explain it.
                None => inner.conn.extend_connection_credit(bytes),
            };
            // Absorbed rather than propagated. The layer below refuses to extend a stream
            // this endpoint never receives on, which is exactly what the HTTP/3 layer asks
            // for when it consumes from a stream that has since gone: it reports the bytes
            // it read without asking whether the stream is still there, and it has no way to
            // tell such a refusal from a real one. Failing the connection over it would kill
            // a healthy connection on the ordinary path where a request is cancelled.
            let _ = extended;
            Ok(())
        })
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<()> {
        self.with(|inner| {
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
    let backend = QmuxConnection::client(stream, clock)?;
    let tail = backend.share();
    let (sender, driving) = ngnet_h3::http::handshake::<_, B>(backend).map_err(Error::http3)?;
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
    let backend = QmuxConnection::server(stream, clock)?;
    let tail = backend.share();
    let driving = ngnet_h3::http::serve(backend, handler).map_err(Error::http3)?;
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
