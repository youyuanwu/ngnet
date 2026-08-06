//! Answering requests over an asynchronous connection.
//!
//! [`serve`] turns a transport and a handler into a driver future. The handler is an
//! ordinary `async` closure: it is given an [`http::Request`] whose body is an
//! [`IncomingBody`], and it returns an [`http::Response`]. Nothing else is required — no
//! executor, no spawner, no `Service` trait.
//!
//! ```no_run
//! # use ngnet_h2::http::{IncomingBody, Transport, server};
//! # use ngnet_h2::http::testing::{Empty, http_crate as http};
//! # async fn example<T: Transport>(transport: T) -> Result<(), Box<dyn std::error::Error>> {
//! let connection = server::serve(transport, |request: http::Request<IncomingBody>| {
//!     let path = request.uri().path().to_owned();
//!     async move {
//!         http::Response::builder()
//!             .status(200)
//!             .header("x-path", path)
//!             .body(Empty)
//!             .expect("a well-formed response")
//!     }
//! })?;
//!
//! // Run it wherever the caller's runtime puts work.
//! connection.await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Concurrency, and its one sharp edge
//!
//! Handlers run concurrently on one connection without anything being spawned: they are
//! futures held by the driver and polled between passes of moving octets, each woken by a
//! waker naming its own stream. A handler that returns `Pending` costs nothing and lets
//! every other stream proceed.
//!
//! A handler that **blocks** is a different matter. There is no other thread for the
//! connection to run on, so a handler that sleeps, or waits on a lock, or does a long
//! synchronous computation, stops the whole connection for that time — not just its own
//! stream. Work like that belongs on a thread pool the caller owns, awaited through a
//! channel.

use core::future::Future;
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body::Body;

use crate::{ErrorCode, Session, StreamId};

use super::body::{Direction, IncomingBody};
use super::config::Config;
use super::connection::Connection;
use super::driver::{self, BodyPlan, DriverGuard, Events, PushBodies, Role, SharedBodies, Signals};
use super::error::Result;
use super::head;
use super::shared::{Incoming, Registry, Shared};
use super::tasks::Tasks;
use super::transport::Transport;

/// Serves requests arriving over `transport`, answering each with `handler`.
///
/// The returned future is the connection. It does nothing until polled, finishes when the
/// peer goes away, and where it runs is entirely the caller's choice.
///
/// Handlers run concurrently without anything being spawned — see the [module
/// documentation](self) for what that does and does not buy, in particular what happens if
/// a handler blocks rather than returning `Pending`.
///
/// # Errors
///
/// Fails if the underlying session cannot be created. Failures afterwards are reported by
/// the returned future.
pub fn serve<T, H, F, B>(
    transport: T,
    handler: H,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serve_with(transport, handler, Config::default())
}

/// Serves requests over `transport` with an explicit [`Config`].
///
/// Identical to [`serve`] but for the limits advertised to the peer and enforced locally.
/// The concurrency limit in particular is the ceiling on how many handler futures one peer
/// can have running at once — including handlers retained after their stream was reset —
/// so it is a real bound on this connection's memory, not merely advice to the peer. See
/// [`Config`] for the defaults and why they are conservative.
///
/// # Errors
///
/// Fails if the underlying session cannot be created. Failures afterwards are reported by
/// the returned future.
pub fn serve_with<T, H, F, B>(
    transport: T,
    handler: H,
    config: Config,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serve_planned::<T, H, F, B, PushBodies>(transport, handler, config)
}

/// Serves requests whose response bodies are handed over uncopied.
///
/// The no-copy counterpart of [`serve`]. Everything a handler sees is unchanged — the same
/// `http::Request`/`http::Response`, the same concurrency model — except that each
/// response body's octets reach the transport without being copied into libnghttp2's
/// serialisation buffer. That is possible only when the body's `Data` is [`bytes::Bytes`],
/// which the crate can hand over rather than copy, so this entry point bounds
/// `B::Data = Bytes` where [`serve`] does not.
///
/// The choice is whole-connection: every response on a connection served here hands its
/// body over. See [`serve`] for the returned future and how it is run.
///
/// # Errors
///
/// Fails if the underlying session cannot be created. Failures afterwards are reported by
/// the returned future.
pub fn serve_shared<T, H, F, B>(
    transport: T,
    handler: H,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serve_shared_with(transport, handler, Config::default())
}

/// Serves no-copy responses with an explicit [`Config`].
///
/// The no-copy counterpart of [`serve_with`], and additive over [`serve_shared`] in
/// exactly the way [`serve_with`] is over [`serve`]: it takes a [`Config`] by value.
///
/// # Errors
///
/// Fails if the underlying session cannot be created. Failures afterwards are reported by
/// the returned future.
pub fn serve_shared_with<T, H, F, B>(
    transport: T,
    handler: H,
    config: Config,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    serve_planned::<T, H, F, B, SharedBodies>(transport, handler, config)
}

/// The shared body of the four public server entry points, parameterised by body plan.
///
/// The plain and `_shared` forms differ only in which [`BodyPlan`] they fix and the bound
/// that plan needs, so the connection wiring lives here once rather than four times. `P`
/// never escapes: the returned future and everything a handler sees are the same whichever
/// plan is chosen.
fn serve_planned<T, H, F, B, P>(
    transport: T,
    handler: H,
    config: Config,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
{
    let session = driver::server_session(&config)?;

    let shared = Arc::new(Shared::default());
    let registry = Arc::new(Registry::default());

    let role = ServerRole::<H, F, B, P> {
        shared: Arc::clone(&shared),
        registry: Arc::clone(&registry),
        handler,
        tasks: Tasks::new(Arc::clone(&shared)),
        losses: BTreeMap::new(),
        max_concurrent_streams: config.concurrency(),
        woken: Vec::new(),
        body: core::marker::PhantomData,
        plan: core::marker::PhantomData,
    };

    let guard = DriverGuard::new(Arc::clone(&shared), Arc::clone(&registry), role);
    Ok(Connection::new(driver::run(
        transport, session, shared, registry, guard,
    )))
}

/// Tells a handler that its stream is gone.
///
/// Placed in every request's [extensions](http::Extensions), so a handler that wants to
/// know can ask and one that does not need never mention it.
///
/// A handler on a stream the peer reset is not cancelled — this crate does not drop it
/// part-way, because a dropped future is told nothing and may have cleanup to do. It runs
/// to completion and its response is discarded. This is how it can stop early instead, and
/// it is the only signal that works for a request whose body had already ended: reading
/// the body would report the loss, but a body that is already finished has nothing left to
/// report.
///
/// ```no_run
/// # use ngnet_h2::http::{Cancelled, IncomingBody};
/// # use ngnet_h2::http::testing::{Empty, http_crate as http};
/// # async fn handler(request: http::Request<IncomingBody>) -> http::Response<Empty> {
/// let lost = request.extensions().get::<Cancelled>().cloned();
/// // ... partway through some long piece of work ...
/// if lost.is_some_and(|lost| lost.is_cancelled()) {
///     return http::Response::new(Empty);
/// }
/// # http::Response::new(Empty)
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Cancelled {
    state: Arc<CancelState>,
}

#[derive(Debug, Default)]
struct CancelState {
    lost: Mutex<(bool, Vec<core::task::Waker>)>,
}

impl Cancelled {
    /// Whether the stream this request arrived on has gone.
    ///
    /// Once true, always true. A response produced after this is discarded rather than
    /// sent, so a handler that checks can stop doing work nobody will receive.
    pub fn is_cancelled(&self) -> bool {
        self.state.lock().0
    }

    /// Resolves when the stream this request arrived on goes.
    ///
    /// Never resolves for a stream that completes normally, so it belongs in a `select`
    /// against the handler's real work rather than being awaited on its own.
    pub async fn cancelled(&self) {
        core::future::poll_fn(|context| {
            let mut lost = self.state.lock();
            if lost.0 {
                return core::task::Poll::Ready(());
            }
            if !lost.1.iter().any(|held| held.will_wake(context.waker())) {
                lost.1.push(context.waker().clone());
            }
            core::task::Poll::Pending
        })
        .await;
    }
}

impl CancelState {
    fn lock(&self) -> std::sync::MutexGuard<'_, (bool, Vec<core::task::Waker>)> {
        self.lost
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Trips the signal, waking everything waiting on it.
    fn trip(&self) {
        let waiting = {
            let mut lost = self.lock();
            if lost.0 {
                return;
            }
            lost.0 = true;
            core::mem::take(&mut lost.1)
        };
        // Outside the lock: a waker may run arbitrary code, including asking again.
        for waker in waiting {
            waker.wake();
        }
    }
}

/// What a server end of a connection does that a client end does not.
///
/// Work arrives from the wire rather than from a handle, and a completed header block is a
/// job rather than an answer.
struct ServerRole<H, F, B, P> {
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    handler: H,
    tasks: Tasks<F>,
    /// One signal per running handler, tripped when its stream goes.
    losses: BTreeMap<i32, Arc<CancelState>>,
    /// The ceiling on concurrently running handlers, from [`Config`].
    max_concurrent_streams: u32,
    /// Scratch buffer of woken streams, reused across passes so draining the ready set
    /// costs no allocation on the steady-state path — mirrors the body path's discipline.
    woken: Vec<i32>,
    /// Names the response body type, which only appears inside the handler's future.
    body: core::marker::PhantomData<fn() -> B>,
    /// Names the body plan this connection was built with. Zero-sized; it selects the
    /// submission path in [`respond`](ServerRole::respond) and nothing else.
    plan: core::marker::PhantomData<fn() -> P>,
}

impl<H, F, B, P> ServerRole<H, F, B, P>
where
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
{
    /// Puts a finished handler's response on the wire, if there is still a stream for it.
    fn respond(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        response: http::Response<B>,
    ) -> Result<()> {
        // The handler has finished, so there is nothing left to tell it.
        self.losses.remove(&stream);

        // The peer may have reset this stream while the handler was running. Submitting
        // anyway is not merely wasted work: libnghttp2 rejects it, and a discarded
        // response is not a reason to fail a connection that is otherwise healthy.
        if !session.stream_is_open(StreamId::new(stream)) {
            return Ok(());
        }

        let (parts, body) = response.into_parts();
        let headers = match head::response_headers(&parts) {
            Ok(headers) => headers,
            Err(error) => {
                // A response this crate will not send is one exchange's failure. The
                // handler is told through the only channel it still has — the request body
                // it was given — and the connection carries on.
                if let Some(incoming) = self.registry.incoming(stream) {
                    incoming.fail(error);
                }
                session.reset_stream(StreamId::new(stream), ErrorCode::INTERNAL_ERROR)?;
                return Ok(());
            }
        };
        let views = headers.views();

        if body.is_end_stream() {
            session.submit_response(StreamId::new(stream), &views)?;
            return Ok(());
        }

        let Some(liveness) = self.registry.liveness(stream) else {
            // Removed from the registry between the open check and here, which only the
            // close path does. Nothing to answer.
            return Ok(());
        };
        // The body plan chooses whether these octets are copied or handed over; naming the
        // stream and binding the waker is the same either way.
        let waker = P::submit_response(
            &self.shared,
            session,
            liveness,
            StreamId::new(stream),
            &views,
            body,
        )?;
        // The stream identifier was known all along here, unlike on the client, but the
        // binding still has to happen before anything consults the body — and nothing can,
        // until the next `Session::send`.
        waker.bind(stream);
        Ok(())
    }
}

impl<H, F, B, P> Role for ServerRole<H, F, B, P>
where
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    P: BodyPlan<B>,
{
    fn advance(&mut self, session: &mut Session<Events>) -> Result<()> {
        // Only the handlers that were woken, which is the whole point of giving each its
        // own waker: a connection carrying a hundred streams polls one future when one of
        // them becomes ready, not a hundred. The woken set is drained into a reused scratch
        // buffer, and iterated by index so a handler that finishes can be polled and
        // responded to without holding a borrow of that buffer across the call.
        self.tasks.take_woken_into(&mut self.woken);
        let mut index = 0;
        while index < self.woken.len() {
            let stream = self.woken[index];
            index += 1;
            if let Some(response) = self.tasks.poll(stream) {
                self.respond(session, stream, response)?;
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
        // libnghttp2 enforces the advertised `MAX_CONCURRENT_STREAMS` against the streams
        // it still counts as open, and drops a reset stream from that count. This crate
        // deliberately keeps a handler running after its stream is reset, so the number of
        // handler futures alive here can exceed what libnghttp2 is counting. Enforcing the
        // cap against the running handlers — retained ones included — makes it a real
        // structural bound on this connection's memory rather than only advice to the peer.
        if self.tasks.len() >= self.max_concurrent_streams as usize {
            incoming.abandon();
            session.reset_stream(StreamId::new(stream), ErrorCode::REFUSED_STREAM)?;
            return Ok(());
        }

        let head = match head::request_head(fields) {
            Ok(head) => head,
            Err(_error) => {
                // A request this crate cannot understand is refused without troubling a
                // handler with it, and without troubling the rest of the connection.
                incoming.abandon();
                session.reset_stream(StreamId::new(stream), ErrorCode::PROTOCOL_ERROR)?;
                return Ok(());
            }
        };

        let mut request = head.map(|()| {
            IncomingBody::new(
                stream,
                Direction::Request,
                Arc::clone(incoming),
                Arc::clone(&self.shared),
            )
        });

        let state = Arc::new(CancelState::default());
        self.losses.insert(stream, Arc::clone(&state));
        request.extensions_mut().insert(Cancelled { state });

        self.tasks.start(stream, (self.handler)(request));
        Ok(())
    }

    fn closed(&mut self, stream: i32) {
        // Deliberately not dropping the handler. A stream the peer reset still has a
        // handler that may need to notice, and dropping a future tells it nothing. It runs
        // on, learns through the signal tripped here, and its response is discarded when it
        // eventually offers one.
        if let Some(state) = self.losses.remove(&stream) {
            state.trip();
        }
    }

    fn started(&self, stream: i32) -> bool {
        // A server opens only pushed streams, which are even — and this crate pushes
        // nothing, so it starts nothing. A client's `GOAWAY` therefore abandons none of the
        // requests being served, which is the whole point of asking.
        stream % 2 == 0
    }

    fn abandon(&mut self) {
        for (_, state) in core::mem::take(&mut self.losses) {
            state.trip();
        }
        self.tasks.abandon_all();
    }

    fn signals(&self) -> Signals {
        let ready = self.tasks.ready();
        Signals::new(
            move || ready.any(),
            // A server does not decide when it is finished; the peer does, by going away.
            // Ending because no stream happens to be open would close a connection between
            // two requests.
            || false,
        )
    }
}
