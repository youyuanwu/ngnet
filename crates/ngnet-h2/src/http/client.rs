//! Making requests over an asynchronous connection.
//!
//! [`handshake`] turns a transport into two things: a handle for making requests, and a
//! driver that must be run. Splitting them is what keeps this layer runtime-agnostic —
//! the driver is an ordinary future, and *where* it runs is entirely the caller's choice.
//! Spawn it, join it, or poll it alongside something else; this crate never spawns
//! anything and takes no executor, spawner or timer.
//!
//! ```no_run
//! # use ngnet_h2::http::{Transport, client};
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
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use http_body::Body;

use std::sync::Weak;

use super::body::{Direction, IncomingBody};
use super::config::Config;
use super::connection::Connection;
use super::driver::{self, BodyPlan, DriverGuard, Events, PushBodies, Role, SharedBodies, Signals};
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
pub fn handshake<T, B>(
    transport: T,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    T: Transport,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    handshake_with(transport, Config::default())
}

/// Starts a client connection over `transport` with an explicit [`Config`].
///
/// Identical to [`handshake`] but for the limits advertised to the peer. See [`Config`] for
/// what those limits bound and why the defaults are conservative.
///
/// How this connection drains a pass to the transport is *not* configured here. It is asked
/// of the transport itself, once, through
/// [`TransportWrite::is_write_vectored`](crate::http::transport::TransportWrite::is_write_vectored).
///
/// # Errors
///
/// Fails only if the underlying session cannot be created.
pub fn handshake_with<T, B>(
    transport: T,
    config: Config,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    T: Transport,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    handshake_planned::<T, B, PushBodies>(transport, config)
}

/// Starts a client connection whose request bodies are handed over uncopied.
///
/// The no-copy counterpart of [`handshake`]. Identical in every respect a caller can see —
/// the same handle, the same driver, the same `http::Request`/`http::Response` types —
/// except that the request body's octets travel to the transport without being copied into
/// libnghttp2's serialisation buffer. That is possible only when the body's `Data` is
/// [`bytes::Bytes`], which the crate can hand over rather than copy, so this entry point
/// bounds `B::Data = Bytes` where [`handshake`] does not.
///
/// The choice is whole-connection: every request on a connection started here hands its
/// body over, and a caller who needs both kinds on one connection supplies a body that is
/// itself a choice between them (its `Data` is still `Bytes`). See [`handshake`] for the
/// shape of what is returned and how the two objects must be run together.
///
/// # Errors
///
/// Fails only if the underlying session cannot be created.
pub fn handshake_shared<T, B>(
    transport: T,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    T: Transport,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    handshake_shared_with(transport, Config::default())
}

/// Starts a no-copy client connection with an explicit [`Config`].
///
/// The no-copy counterpart of [`handshake_with`], and additive over [`handshake_shared`]
/// in exactly the way [`handshake_with`] is over [`handshake`]: it takes a [`Config`] by
/// value and returns the same pair.
///
/// # Errors
///
/// Fails only if the underlying session cannot be created.
pub fn handshake_shared_with<T, B>(
    transport: T,
    config: Config,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    T: Transport,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    handshake_planned::<T, B, SharedBodies>(transport, config)
}

/// The shared body of the four public client entry points, parameterised by body plan.
///
/// The plain and `_shared` forms differ only in which [`BodyPlan`] they fix and the bound
/// that plan needs, so the connection wiring lives here once rather than four times. `P`
/// never escapes: the handle, the driver and every returned type are the same whichever
/// plan is chosen.
fn handshake_planned<T, B, P>(
    transport: T,
    config: Config,
) -> Result<(SendRequest<B>, Connection<impl Future<Output = Result<()>>>)>
where
    T: Transport,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
{
    let session = driver::client_session(&config)?;

    let shared = Arc::new(Shared::default());
    let queue = Arc::new(Queue::<B>::default());
    let registry = Arc::new(Registry::default());
    let token = Arc::new(HandleToken::new(Arc::clone(&shared)));

    let guard = DriverGuard::new(
        Arc::clone(&shared),
        Arc::clone(&registry),
        ClientRole::<B, P> {
            shared: Arc::clone(&shared),
            queue: Arc::clone(&queue),
            registry: Arc::clone(&registry),
            handles: Arc::downgrade(&token),
            plan: PhantomData,
        },
    );

    let connection = Connection::new(
        driver::run(transport, session, Arc::clone(&shared), registry, guard),
        Arc::clone(&shared),
    );

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
struct ClientRole<B, P> {
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    registry: Arc<Registry>,
    /// Weak, so the last handle going away is what tells the driver no more can come.
    handles: Weak<HandleToken>,
    /// Names the body plan this connection was built with. Zero-sized; it selects the
    /// submission path in [`submit`](ClientRole::submit) and nothing else.
    plan: PhantomData<fn() -> P>,
}

impl<B, P> ClientRole<B, P>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
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
            // The body plan chooses whether these octets are copied or handed over; the
            // rest of the submission — binding the waker, linking the slot — is the same
            // either way.
            let (stream, waker) = P::submit_request(
                &self.shared,
                session,
                Arc::downgrade(&liveness),
                &views,
                body,
            )?;
            // The identifier only exists now. Nothing can have consulted the body yet —
            // that happens inside `Session::send` — so no wake can have been lost.
            waker.bind(stream.get());
            stream
        };

        // Named now, so a response future dropped before its answer arrives knows which
        // stream to stop — and if it was dropped while this call was in flight, that is
        // what `bind` reports, since the drop could not have seen a stream to name.
        let unwanted = slot.bind(stream.get());
        self.registry
            .insert(stream.get(), Some(Arc::clone(slot)), incoming, liveness);
        if unwanted {
            session.reset_stream(stream, crate::ErrorCode::CANCEL)?;
        }
        Ok(())
    }
}

impl<B, P> Role for ClientRole<B, P>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
{
    fn advance(&mut self, session: &mut Session<Events>) -> Result<()> {
        for command in self.queue.drain() {
            let Command::SendRequest { request, slot } = command;
            // Dropped before it was ever sent, so there is nothing to send and nothing to
            // reset. Cheaper than submitting and immediately taking it back, and the peer
            // never sees a request nobody wanted.
            if slot.is_abandoned() {
                continue;
            }
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
            Ok(head) => {
                // An informational `1xx` head is not the answer — it precedes the real
                // response. libnghttp2 surfaces `103 Early Hints` / `100 Continue` to this
                // callback before the final head arrives and before it marks the stream as
                // expecting a final response, so settling the future on the first head
                // would resolve it with a provisional status and discard the actual `200`
                // that follows. Ignored here, leaving the slot unsettled for the final
                // head; the stream stays open and `IncomingBody` is not handed out for
                // something that carries no body. A stream that only ever carries `1xx`
                // and then ends leaves the slot unsettled, which the stream-close path
                // then fails rather than hanging.
                if head.status().is_informational() {
                    return Ok(());
                }
                slot.complete(head.map(|()| {
                    IncomingBody::new(
                        stream,
                        Direction::Response,
                        Arc::clone(incoming),
                        Arc::clone(&self.shared),
                    )
                }));
            }
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

    fn trailers(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        fields: &[(Vec<u8>, Vec<u8>)],
        incoming: &Arc<Incoming>,
    ) -> Result<()> {
        // libnghttp2 categorises the final response that follows a `1xx` as an ordinary
        // trailing header block, indistinguishable at the frame level from genuine
        // trailers on an answered stream. The slot disambiguates them: a slot that is not
        // yet settled has seen only `1xx` heads, so this block is the awaited final
        // response head rather than trailers. Once the slot is settled a further block is
        // real trailers and is delivered as such.
        if let Some(slot) = self.registry.slot(stream)
            && !slot.is_settled()
        {
            return self.head(session, stream, fields, incoming);
        }
        driver::deliver_trailers(session, stream, fields, incoming)
    }

    fn started(&self, stream: i32) -> bool {
        // A client opens odd-numbered streams; even ones can only have come from a push,
        // which this crate does not accept.
        stream % 2 == 1
    }

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
            return ResponseFuture { slot, shared: None };
        }

        // Refused rather than closed: the connection is still carrying its earlier
        // exchanges, and this one was never begun — which is what makes it safe to retry.
        if self.shared.is_refusing() {
            slot.fail(Error::refused());
            return ResponseFuture { slot, shared: None };
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

        ResponseFuture {
            slot,
            shared: Some(Arc::clone(&self.shared)),
        }
    }

    /// Whether the connection has stopped.
    ///
    /// Advisory: a connection may go away immediately after this returns `false`. Use it
    /// to retire a handle, not to decide whether a request will succeed.
    pub fn is_closed(&self) -> bool {
        self.shared.is_gone()
    }

    /// Whether new requests are being refused.
    ///
    /// True once this end has asked to [shut down](Self::shutdown), or the peer has said
    /// it is going away. Exchanges already in flight are unaffected; only new ones are
    /// turned away, and they are turned away as [retriable](Error::is_retriable).
    pub fn is_refusing(&self) -> bool {
        self.shared.is_refusing()
    }

    /// Winds the connection down, letting exchanges already in flight finish.
    ///
    /// Tells the peer this end is going away and stops accepting new requests: anything
    /// submitted afterwards fails as [`ErrorKind::Refused`], which a caller may retry on
    /// another connection. Requests already sent run to completion, and the driver
    /// finishes when they do.
    ///
    /// Idempotent, and safe to call from any task. It does not wait — the connection ends
    /// when its driver does.
    pub fn shutdown(&self) {
        // Zero, because a `GOAWAY` names the last stream *the peer* opened, and a client
        // that accepts no pushed streams has honoured none. Its own requests are unaffected
        // — they are the ones that get to finish.
        self.shared.request_shutdown(0, crate::ErrorCode::NO_ERROR);
        self.shared.wake_driver();
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

    /// The read-buffer pool's current size, and the largest it has ever reached.
    ///
    /// Reachable only through the hidden testing module. The pool is a local of the driver
    /// future and reaches nothing else, so the promise that it settles to a fixed size and
    /// stops growing can only be tested through a gauge the driver keeps for the purpose.
    pub(crate) fn pool_size(&self) -> usize {
        self.shared.pool_size()
    }

    /// See [`Self::pool_size`].
    pub(crate) fn pool_high_water(&self) -> usize {
        self.shared.pool_high_water()
    }
}

/// Resolves when a request's response head arrives.
///
/// Dropping one before it resolves cancels that exchange: the peer observes a stream reset
/// and stops working on it. A request that was never submitted — because the connection had
/// already gone — has no stream to reset and simply disappears.
#[derive(Debug)]
pub struct ResponseFuture {
    slot: Arc<Slot>,
    shared: Option<Arc<Shared>>,
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        // A request that has already been sent can only be taken back with a reset. One
        // that has not is simply never sent — which `cancel` records, under the same lock
        // the driver binds the stream with, so the two cannot pass each other.
        if let Some(stream) = self.slot.cancel() {
            shared.reset(stream, crate::ErrorCode::CANCEL);
        }
        shared.wake_driver();
    }
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<IncomingBody>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.slot.poll(context.waker())
    }
}
