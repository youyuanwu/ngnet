//! Answering requests over an asynchronous connection.
//!
//! [`serve`] turns a transport and a handler into a driver future. The handler is an
//! ordinary `async` closure: it is given an [`http::Request`] whose body is an
//! [`IncomingBody`], and it returns an [`http::Response`]. Nothing else is required — no
//! executor, no spawner, no `Service` trait.
//!
//! ```no_run
//! # use nghttp2::http::{IncomingBody, Transport, server};
//! # use nghttp2::http::testing::{Empty, http_crate as http};
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
use std::error::Error as StdError;
use std::sync::Arc;

use http_body::Body;

use crate::{ErrorCode, Session, StreamId};

use super::body::IncomingBody;
use super::driver::{self, DriverGuard, Events, Role, Signals};
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
pub fn serve<T, H, F, B>(transport: T, handler: H) -> Result<impl Future<Output = Result<()>>>
where
    T: Transport,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let session = driver::server_session()?;

    let shared = Arc::new(Shared::default());
    let registry = Arc::new(Registry::default());

    let role = ServerRole {
        shared: Arc::clone(&shared),
        registry: Arc::clone(&registry),
        handler,
        tasks: Tasks::new(Arc::clone(&shared)),
        body: core::marker::PhantomData,
    };

    let guard = DriverGuard::new(Arc::clone(&shared), Arc::clone(&registry), role);
    Ok(driver::run(transport, session, shared, registry, guard))
}

/// What a server end of a connection does that a client end does not.
///
/// Work arrives from the wire rather than from a handle, and a completed header block is a
/// job rather than an answer.
struct ServerRole<H, F, B> {
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    handler: H,
    tasks: Tasks<F>,
    /// Names the response body type, which only appears inside the handler's future.
    body: core::marker::PhantomData<fn() -> B>,
}

impl<H, F, B> ServerRole<H, F, B>
where
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Puts a finished handler's response on the wire, if there is still a stream for it.
    fn respond(
        &mut self,
        session: &mut Session<Events>,
        stream: i32,
        response: http::Response<B>,
    ) -> Result<()> {
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
        let (source, waker) = driver::outgoing_body(&self.shared, liveness, body);
        session.submit_response_with_body(StreamId::new(stream), &views, source)?;
        // The stream identifier was known all along here, unlike on the client, but the
        // binding still has to happen before anything consults the body — and nothing can,
        // until the next `Session::send`.
        waker.bind(stream);
        Ok(())
    }
}

impl<H, F, B> Role for ServerRole<H, F, B>
where
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn advance(&mut self, session: &mut Session<Events>) -> Result<()> {
        // Only the handlers that were woken, which is the whole point of giving each its
        // own waker: a connection carrying a hundred streams polls one future when one of
        // them becomes ready, not a hundred.
        for stream in self.tasks.woken() {
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

        let request = head
            .map(|()| IncomingBody::new(stream, Arc::clone(incoming), Arc::clone(&self.shared)));
        self.tasks.start(stream, (self.handler)(request));
        Ok(())
    }

    fn closed(&mut self, _stream: i32) {
        // Deliberately not dropping the handler. A stream the peer reset still has a
        // handler that may need to notice, and it notices through the request body it was
        // given, which the close path fails a moment after this. Its response is discarded
        // when it eventually offers one.
    }

    fn abandon(&mut self) {
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
