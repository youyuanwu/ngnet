//! Making HTTP/3 requests.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::Bytes;
use http_body::Body;

use super::body::{IncomingBody, Outgoing};
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
use crate::stream::{Directionality, Initiator, StreamId};

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
        next_stream: 0,
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
    /// The sequence number of the next request stream, so identifiers are 0, 4, 8, …
    next_stream: u64,
}

impl<B> Role for ClientRole<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    fn advance(&mut self, conn: &mut Conn<Events>, _events: &mut Events) -> Result<()> {
        while let Some(command) = self.queue.pop() {
            let stream = StreamId::compose(
                Initiator::Client,
                Directionality::Bidirectional,
                self.next_stream,
            )
            .map_err(|_| Error::new(ErrorKind::Connection, "stream identifiers exhausted"))?;
            self.next_stream += 1;

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
            let views = fields.views()?;

            let ending = Arc::new(std::sync::Mutex::new(None));
            let source: Option<Box<dyn crate::body::BodySource>> = if body.is_end_stream() {
                None
            } else {
                Some(Box::new(Outgoing::new(
                    body,
                    stream,
                    Arc::clone(&self.shared),
                    Arc::clone(&ending),
                )))
            };

            conn.submit_request(stream, &views, source)?;
            // Recorded only after submission succeeded, so a failed submission leaves no
            // stream for the future to try to abandon.
            command.slot.bind(stream);

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
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        let head = head::response_head(fields)?;
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
        !self.queue.is_empty()
    }

    fn done(&self) -> bool {
        // Every handle has gone, so nothing further will ever be submitted.
        self.handles.strong_count() == 0 && self.queue.is_empty()
    }

    fn abandon(&mut self) {
        self.queue.abandon();
    }
}
