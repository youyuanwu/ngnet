//! Making requests over an asynchronous connection.
//!
//! [`handshake`] turns a transport into two things: a handle for making requests, and a
//! driver that must be run. Splitting them is what keeps this layer runtime-agnostic —
//! the driver is an ordinary future, and *where* it runs is entirely the caller's choice.
//! Spawn it, join it, or poll it alongside something else; this crate never spawns
//! anything and takes no executor, spawner or timer.
//!
//! ```no_run
//! # use nghttp2::http::{Transport, client};
//! # async fn example<T: Transport, B>(transport: T, body: B) -> Result<(), Box<dyn std::error::Error>>
//! # where B: http_body::Body + Send + 'static, B::Data: Send,
//! #       B::Error: Into<Box<dyn std::error::Error + Send + Sync>> {
//! let (requests, connection) = client::handshake::<T, B>(transport)?;
//!
//! // Run `connection` wherever the caller's runtime puts work. Until it runs, nothing
//! // moves: the handle only enqueues.
//! let response = requests
//!     .send_request(http::Request::get("http://example.test/").body(body)?)
//!     .await?;
//! # let _ = (response, connection);
//! # Ok(())
//! # }
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::error::Error as StdError;
use std::sync::Arc;

use http_body::Body;

use std::sync::Weak;

use super::body::IncomingBody;
use super::driver::{self, DriverGuard, Events, Role, Signals};
use super::error::{Error, ErrorKind, Result};
use super::head;
use super::shared::{Command, HandleToken, Incoming, Queue, Registry, Shared, Slot};
use super::transport::Transport;
use crate::{ErrorCode, Session, StreamId};

/// Starts a client connection over `transport`.
///
/// Returns a handle for making requests and the connection's driver. The driver does
/// nothing until it is polled, and the handle does nothing but enqueue until it is — so
/// the two must be run together, and the caller decides how.
///
/// Dropping the driver fails every request it was carrying, including ones enqueued but
/// not yet submitted, so a caller can never be left waiting on a connection that is gone.
///
/// # Errors
///
/// Fails only if the underlying session cannot be created.
pub fn handshake<T, B>(transport: T) -> Result<(SendRequest<B>, impl Future<Output = Result<()>>)>
where
    T: Transport,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let session = driver::client_session()?;

    let shared = Arc::new(Shared::default());
    let queue = Arc::new(Queue::<B>::default());
    let registry = Arc::new(Registry::default());
    let token = Arc::new(HandleToken::new(Arc::clone(&shared)));

    let guard = DriverGuard::new(
        Arc::clone(&shared),
        Arc::clone(&registry),
        ClientRole {
            shared: Arc::clone(&shared),
            queue: Arc::clone(&queue),
            registry: Arc::clone(&registry),
            handles: Arc::downgrade(&token),
        },
    );

    let connection = driver::run(transport, session, Arc::clone(&shared), registry, guard);

    Ok((
        SendRequest {
            shared,
            queue,
            _token: token,
        },
        connection,
    ))
}

/// What a client end of a connection does that a server end does not.
///
/// Requests arrive from handles rather than from the wire, and a completed header block is
/// an answer rather than a job.
struct ClientRole<B> {
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    registry: Arc<Registry>,
    /// Weak, so the last handle going away is what tells the driver no more can come.
    handles: Weak<HandleToken>,
}

impl<B> ClientRole<B>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Submits one request, linking its stream to the slot that will answer it.
    fn submit(
        &self,
        session: &mut Session<Events>,
        request: http::Request<B>,
        slot: &Arc<Slot>,
    ) -> Result<()> {
        let (parts, body) = request.into_parts();
        let headers = head::request_headers(&parts)?;
        let views = headers.views();

        // Dropped when the stream leaves the registry, at which point every waker that
        // names this stream stops being able to enqueue anything.
        let liveness = Arc::new(());
        // Created with the stream rather than with the response head, so payload that
        // arrives before anything has looked for it still has somewhere to go.
        let incoming = Arc::new(Incoming::default());

        let stream = if body.is_end_stream() {
            session.submit_request(&views)?
        } else {
            let (source, waker) =
                driver::outgoing_body(&self.shared, Arc::downgrade(&liveness), body);
            let stream = session.submit_request_with_body(&views, source)?;
            // The identifier only exists now. Nothing can have consulted the body yet —
            // that happens inside `Session::send` — so no wake can have been lost.
            waker.bind(stream.get());
            stream
        };

        self.registry
            .insert(stream.get(), Some(Arc::clone(slot)), incoming, liveness);
        Ok(())
    }
}

impl<B> Role for ClientRole<B>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn advance(&mut self, session: &mut Session<Events>) -> Result<()> {
        for command in self.queue.drain() {
            let Command::SendRequest { request, slot } = command;
            // A request this crate will not send is that request's failure, not the
            // connection's: the handle that made it hears about it and every other
            // exchange carries on.
            if let Err(error) = self.submit(session, request, &slot) {
                slot.fail(error);
            }
        }
        Ok(())
    }

    fn head(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        fields: &[(Vec<u8>, Vec<u8>)],
        incoming: &Arc<Incoming>,
    ) -> Result<()> {
        let Some(slot) = self.registry.slot(stream) else {
            return Ok(());
        };

        match head::response_head(fields) {
            Ok(head) => slot.complete(head.map(|()| {
                IncomingBody::new(stream, Arc::clone(incoming), Arc::clone(&self.shared))
            })),
            Err(error) => {
                slot.fail(error);
                incoming.fail(Error::new(
                    ErrorKind::Protocol,
                    "the peer sent a response head this crate could not accept",
                ));
                // No body was handed out, so nothing will ever read or drop one — and
                // anything already queued would hold the connection-level window shut for
                // the rest of the connection's life. Abandoning here is what returns it,
                // and marks the stream so later arrivals are credited on the spot rather
                // than accumulating behind a reader that does not exist.
                let unread = incoming.abandon();
                session.reset_stream(StreamId::new(stream), ErrorCode::PROTOCOL_ERROR)?;
                if unread > 0 {
                    session.consume(StreamId::new(stream), unread)?;
                }
            }
        }
        Ok(())
    }

    fn closed(&mut self, _stream: i32) {}

    fn signals(&self) -> Signals {
        let queue = Arc::clone(&self.queue);
        let handles = self.handles.clone();
        Signals::new(
            move || !queue.is_empty(),
            move || handles.strong_count() == 0,
        )
    }

    fn abandon(&mut self) {
        for command in self.queue.drain() {
            let Command::SendRequest { slot, .. } = command;
            slot.fail(Error::closed());
        }
    }
}

/// Makes requests on a connection.
///
/// Cloneable and usable from any task: submitting only appends to a queue and wakes the
/// driver, so a handle never needs the session and never blocks on the driver.
///
/// Whether this is `Send` follows from the body type. Nothing here declares it, which is
/// deliberate — a thread-per-core runtime with a non-`Send` body gets a non-`Send` handle
/// and everything still works.
pub struct SendRequest<B> {
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    /// Kept so the driver can tell when the last handle is gone.
    _token: Arc<HandleToken>,
}

// Written by hand: `derive(Clone)` would demand `B: Clone`, but nothing here holds a `B`.
impl<B> Clone for SendRequest<B> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            queue: Arc::clone(&self.queue),
            _token: Arc::clone(&self._token),
        }
    }
}

// Written by hand: the queue names the body type, which need not be `Debug`.
impl<B> core::fmt::Debug for SendRequest<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SendRequest")
            .field("closed", &self.shared.is_gone())
            .finish_non_exhaustive()
    }
}

impl<B> SendRequest<B> {
    /// Sends a request, returning a future that resolves once its response head arrives.
    ///
    /// The future resolves as soon as the response's header block is complete, not when
    /// the exchange ends — that is what makes streaming responses observable.
    ///
    /// This does not block and does not fail: a request for a connection that has already
    /// gone away is accepted here and reported through the returned future, so there is
    /// only one place to handle failure.
    pub fn send_request(&self, request: http::Request<B>) -> ResponseFuture {
        let slot = Arc::new(Slot::default());

        if self.shared.is_gone() {
            slot.fail(Error::closed());
            return ResponseFuture { slot };
        }

        self.queue.push(Command::SendRequest {
            request,
            slot: Arc::clone(&slot),
        });
        self.shared.wake_driver();

        // The driver may have stopped between the check above and the push. Nothing would
        // drain the command in that case, so the guard's sweep is the backstop — but it
        // may already have run, which is what this second look catches.
        if self.shared.is_gone() {
            for command in self.queue.drain() {
                let Command::SendRequest { slot, .. } = command;
                slot.fail(Error::closed());
            }
        }

        ResponseFuture { slot }
    }

    /// Whether the connection has stopped.
    ///
    /// Advisory: a connection may go away immediately after this returns `false`. Use it
    /// to retire a handle, not to decide whether a request will succeed.
    pub fn is_closed(&self) -> bool {
        self.shared.is_gone()
    }

    /// How many streams are waiting to be resumed.
    ///
    /// Reachable only through the hidden testing module. The size of this set is a
    /// property the design has to guarantee — stale wakers must not accumulate in it —
    /// and a guarantee that cannot be observed cannot be tested.
    pub(crate) fn pending_wakes(&self) -> usize {
        self.shared.ready_len()
    }

    /// The most chunks any one outgoing body has held back at once.
    ///
    /// Reachable only through the hidden testing module, for the same reason as
    /// [`Self::pending_wakes`]: the send path promises to retain at most one unconsumed
    /// chunk per stream, and a promise that cannot be observed cannot be tested.
    pub(crate) fn buffered_chunks(&self) -> usize {
        self.shared.buffered_high_water()
    }
}

/// Resolves when a request's response head arrives.
#[derive(Debug)]
pub struct ResponseFuture {
    slot: Arc<Slot>,
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<IncomingBody>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.slot.poll(context.waker())
    }
}
