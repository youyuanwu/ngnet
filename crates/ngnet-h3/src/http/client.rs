//! Making HTTP/3 requests.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::Bytes;
use http_body::Body;

use super::body::{Ending, IncomingBody, Outgoing, ending_pending};
use super::config::Config;
use super::connection::Connection;
use super::driver::{self, Driver, DriverGuard, REQUEST_CANCELLED, Role};
use super::error::{Error, ErrorKind, Result};
use super::events::Events;
use super::head;
use super::quic::QuicConnection;
use super::shared::{Command, Incoming, Queue, Registry, Shared, Slot};
use crate::conn::{Conn, Role as CoreRole};
use crate::error::ErrorCode;
use crate::stream::StreamId;

/// Starts a client connection over an established QUIC connection.
///
/// Returns a handle for making requests and the driver that performs them. **Nothing moves
/// until the driver is polled**, which is why it is `#[must_use]` — see [`Connection`].
///
/// The three unidirectional streams HTTP/3 requires are opened and bound by the driver; a
/// caller never names them.
pub fn handshake<Q, B>(
    backend: Q,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    Q: QuicConnection,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    handshake_with(backend, Config::default())
}

/// Starts a client connection with settings other than the defaults.
pub fn handshake_with<Q, B>(
    backend: Q,
    config: Config,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    Q: QuicConnection,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let shared = Arc::new(Shared::new());
    let registry = Arc::new(Registry::new());
    let queue = Arc::new(Queue::new());
    let handles = Arc::new(());

    let conn = driver::build_conn(CoreRole::Client, &config, &shared)?;
    let core = Driver::new(
        backend,
        conn,
        Arc::clone(&shared),
        Arc::clone(&registry),
        config,
    );

    let shared_for_guard = Arc::clone(&shared);
    let registry_for_guard = Arc::clone(&registry);

    let role = ClientRole {
        shared: Arc::clone(&shared),
        registry: Arc::clone(&registry),
        queue: Arc::clone(&queue),
        handles: Arc::downgrade(&handles),
        spare: Vec::new(),
        endings: Vec::new(),
    };

    let handle = SendRequest {
        shared,
        queue,
        _handles: handles,
    };

    // The guard is built here rather than inside `run`, so that a driver which is created
    // and then dropped without ever being polled still fails everything in flight. That is
    // exactly the mistake the `#[must_use]` warns about, and it should be defined rather
    // than a hang.
    let guard = DriverGuard::new(Arc::clone(&shared_for_guard), registry_for_guard, role);
    Ok((handle, Connection::new(driver::run(core, guard))))
}

/// A cloneable handle for submitting requests to one connection.
///
/// Cloning is cheap and the clones are equal: several tasks may hold one and submit at once,
/// and their requests are multiplexed over the same connection.
pub struct SendRequest<B> {
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    /// Dropped with the last handle, which is how the driver learns nobody will ask for
    /// anything more and it may finish once the exchanges in flight are done.
    _handles: Arc<()>,
}

// Written out rather than derived: `derive(Clone)` would demand `B: Clone`, and nothing here
// holds a `B`.
impl<B> Clone for SendRequest<B> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            queue: Arc::clone(&self.queue),
            _handles: Arc::clone(&self._handles),
        }
    }
}

impl<B> SendRequest<B> {
    /// Submits a request and returns a future for its response.
    ///
    /// Never blocks and never fails here: a connection that is gone, or refusing, fails the
    /// returned future instead, so a caller has one place to handle failure rather than two.
    pub fn send_request(&self, request: http::Request<B>) -> ResponseFuture {
        let slot = Arc::new(Slot::new());
        let incoming = Arc::new(Incoming::new());

        if self.shared.is_gone() {
            slot.fail(Error::new(ErrorKind::Closed, "the connection is gone"));
        } else if self.shared.is_refusing() {
            slot.fail(Error::new(
                ErrorKind::Refused,
                "the peer went away before this exchange began",
            ));
        } else {
            self.queue.push(Command {
                request,
                slot: Arc::clone(&slot),
                incoming: Arc::clone(&incoming),
            });
            self.shared.wake_driver();
        }

        ResponseFuture {
            slot,
            incoming,
            shared: Arc::clone(&self.shared),
            settled: false,
        }
    }

    /// Whether the connection has gone away.
    ///
    /// Advisory: it can become true immediately after returning false.
    pub fn is_closed(&self) -> bool {
        self.shared.is_gone()
    }

    /// Whether new requests will be refused.
    pub fn is_refusing(&self) -> bool {
        self.shared.is_refusing()
    }

    /// Begins a graceful shutdown.
    ///
    /// Exchanges already in flight finish. New ones fail with [`ErrorKind::Refused`], which
    /// [`Error::is_retriable`] reports as safe to retry on a fresh connection. Idempotent.
    pub fn shutdown(&self) {
        self.shared.request_shutdown();
        self.shared.wake_driver();
    }
}

/// The pending result of one request.
///
/// Resolves to the response head; its body is read from the [`IncomingBody`] the response
/// carries. Dropping this before it resolves abandons the exchange, telling the peer to stop.
#[must_use = "a response future does nothing unless awaited"]
pub struct ResponseFuture {
    slot: Arc<Slot>,
    incoming: Arc<Incoming>,
    shared: Arc<Shared>,
    settled: bool,
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<IncomingBody>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.slot.poll(context.waker()) {
            None => Poll::Pending,
            Some(Err(error)) => {
                self.settled = true;
                Poll::Ready(Err(error))
            }
            Some(Ok(head)) => {
                self.settled = true;
                let (parts, ()) = head.into_parts();
                // Known by now: a response cannot have arrived on a stream the driver had
                // not yet chosen.
                let stream = self
                    .slot
                    .stream()
                    .expect("a response arrived, so its stream is bound");
                let body = IncomingBody::new(
                    stream,
                    // A client reading a response: giving up on it gives up on the
                    // exchange, so the peer is told to stop.
                    true,
                    Arc::clone(&self.incoming),
                    Arc::clone(&self.shared),
                    Arc::new(()),
                );
                Poll::Ready(Ok(http::Response::from_parts(parts, body)))
            }
        }
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // The caller gave up before a response arrived. Telling the peer stops it producing
        // one nobody will read; saying nothing would leave it writing into a void.
        self.slot.fail(Error::new(
            ErrorKind::Stream,
            "the caller abandoned this exchange",
        ));
        // Only if it actually went out. A request still queued is cancelled by failing its
        // slot above; there is no stream to abandon and asking the transport to reset one
        // would name a stream that does not exist.
        if let Some(stream) = self.slot.stream() {
            self.shared.reset(stream, ErrorCode::new(REQUEST_CANCELLED));
            self.shared.wake_driver();
        }
    }
}

/// What a client does that a server does not.
pub(crate) struct ClientRole<B> {
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    queue: Arc<Queue<B>>,
    handles: std::sync::Weak<()>,
    /// Streams the transport has opened and the role has not yet used.
    spare: Vec<StreamId>,
    /// Bodies still running, and where each will report how it ended.
    ///
    /// A body cannot submit its own trailers: it is pulled from inside an FFI call, where
    /// the connection is already mutably borrowed. So it leaves word here and the next pass
    /// acts on it.
    endings: Vec<(StreamId, Arc<std::sync::Mutex<Option<Ending>>>)>,
}

impl<B> Role for ClientRole<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    fn needs_stream(&self) -> bool {
        // One per queued request. Asked before `advance`, so a request never has to invent
        // an identifier the transport has not agreed to.
        self.spare.is_empty() && !self.queue.is_empty()
    }

    fn give_stream(&mut self, stream: StreamId) {
        self.spare.push(stream);
    }

    fn advance(&mut self, conn: &mut Conn<Events>, _events: &mut Events) -> Result<()> {
        self.finish_bodies(conn)?;

        while !self.spare.is_empty() {
            let Some(command) = self.queue.pop() else {
                break;
            };

            // The caller may have dropped the future between submitting and now. Sending
            // anyway would perform a side effect they have already given up on and possibly
            // retried elsewhere, which for a non-idempotent request is the worst kind of
            // duplicate.
            if command.slot.is_settled() {
                continue;
            }

            let stream = self.spare.remove(0);

            let (parts, body) = command.request.into_parts();
            let fields = match head::request_fields(&parts) {
                Ok(fields) => fields,
                Err(error) => {
                    // Rejected before anything reaches the wire, so a bad head costs the
                    // caller an error rather than the connection a protocol violation.
                    command.slot.fail(error);
                    continue;
                }
            };
            let nva = fields.nva()?;

            let ending = Arc::new(std::sync::Mutex::new(None));
            let has_body = !body.is_end_stream();
            let source: Option<Box<dyn crate::body::BodySource>> = if !has_body {
                None
            } else {
                Some(Box::new(Outgoing::new(
                    body,
                    stream,
                    Arc::clone(&self.shared),
                    Arc::clone(&ending),
                )))
            };

            // A recoverable refusal -- the peer sent GOAWAY, so this exchange was never
            // looked at -- must settle the caller's future rather than escape as a
            // connection error. The command has already been popped, so nothing else will
            // ever settle it, and a future nobody settles is a hang.
            if let Err(error) = conn.submit_request_nva(stream, &nva, source) {
                let refused = error.is_fatal();
                command.slot.fail(if refused {
                    Error::from(error)
                } else {
                    Error::new(
                        ErrorKind::Refused,
                        "the connection stopped accepting new exchanges before this one began",
                    )
                });
                if refused {
                    return Err(Error::new(
                        ErrorKind::Connection,
                        "the connection became unusable while submitting a request",
                    ));
                }
                continue;
            }
            // Recorded only after submission succeeded, so a failed submission leaves no
            // stream for the future to try to abandon.
            command.slot.bind(stream);

            // Only a body that exists can ever report how it ended. Recording a slot for
            // one that cannot would grow this list with every request the connection ever
            // carried.
            if has_body {
                self.endings.push((stream, ending));
            }

            let liveness = Arc::new(());
            self.registry.insert(
                stream,
                super::shared::Entry {
                    slot: Some(Arc::clone(&command.slot)),
                    incoming: Arc::clone(&command.incoming),
                    _liveness: liveness,
                },
            );
        }
        Ok(())
    }

    fn head(
        &mut self,
        _conn: &mut Conn<Events>,
        _events: &mut Events,
        stream: StreamId,
        fields: &[head::ReceivedField],
    ) -> Result<()> {
        let head = head::received_response_head(fields)?;
        // An informational response is not the answer. Settling on one would resolve the
        // caller's future and then leave a second head arriving on a stream it believes is
        // finished.
        if head::is_informational(head.status()) {
            return Ok(());
        }
        if let Some(slot) = self.registry.slot(stream) {
            slot.complete(head);
        }
        Ok(())
    }

    fn closed(&mut self, _stream: StreamId) {}

    fn busy(&self) -> bool {
        // As on the server: an ending that no pass has acted on is a reset that has not
        // been queued yet, and the driver must not go quiet while one is owed.
        !self.queue.is_empty() || ending_pending(&self.endings)
    }

    fn done(&self) -> bool {
        // Every handle has gone, so nothing further will ever be submitted.
        self.handles.strong_count() == 0 && self.queue.is_empty()
    }

    fn abandon(&mut self) {
        self.queue.abandon();
    }

    fn settle(&mut self, conn: &mut Conn<Events>) -> Result<()> {
        self.finish_bodies(conn)
    }
}

impl<B> ClientRole<B> {
    /// Acts on bodies that have finished since the last pass.
    fn finish_bodies(&mut self, conn: &mut Conn<Events>) -> Result<()> {
        let mut done = Vec::new();
        for (index, (stream, ending)) in self.endings.iter().enumerate() {
            let Ok(mut slot) = ending.lock() else {
                continue;
            };
            let Some(ending) = slot.take() else { continue };
            done.push(index);

            match ending {
                Ending::Clean => {}
                Ending::Trailers(trailers) => {
                    // Submitted here rather than by the body, which is pulled from inside an
                    // FFI call and cannot reach the connection.
                    let fields = head::trailer_fields(&trailers)?;
                    conn.submit_trailers_nva(*stream, &fields.nva()?)?;
                }
                Ending::Failed => {
                    // One caller's body failing takes down one exchange. Reporting it to the
                    // state machine as a body failure would poison the connection and take
                    // every unrelated exchange with it.
                    //
                    // The caller is told here rather than left to infer it from the reset,
                    // and told the truth: their body failed, which is not a protocol error
                    // and not the peer's doing.
                    if let Some(entry) = self.registry.remove(*stream) {
                        if let Some(slot) = &entry.slot {
                            slot.fail(Error::new(
                                ErrorKind::Body,
                                "the caller's message body reported an error",
                            ));
                        }
                        entry.incoming.fail(Error::new(
                            ErrorKind::Body,
                            "the caller's message body reported an error",
                        ));
                    }
                    self.shared
                        .reset(*stream, ErrorCode::new(REQUEST_CANCELLED));
                }
            }
        }
        for index in done.into_iter().rev() {
            self.endings.swap_remove(index);
        }
        // A body that was dropped without ending — its exchange abandoned, its stream
        // closed — leaves a slot here that nothing will ever fill. Nothing else removes
        // one: a client learns of a stream closing only as the state machine finishing
        // with it, which says nothing about which body it belonged to. The slot's own
        // reference count does say: once the connection has released the body, this list
        // holds the last handle to it. Pruned every pass because `busy` walks this list
        // every pass, and a list that only ever grew would make an idle connection slower
        // the longer it had been useful.
        self.endings
            .retain(|(_, ending)| Arc::strong_count(ending) > 1);
        Ok(())
    }
}
