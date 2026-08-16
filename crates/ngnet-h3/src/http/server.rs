//! Serving HTTP/3 requests.

use core::future::Future;
use core::task::{Context, Poll, Waker};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body::Body;

use super::body::{Ending, IncomingBody, Outgoing, ending_pending};
use super::config::Config;
use super::connection::Connection;
use super::driver::{self, Driver, DriverGuard, REQUEST_CANCELLED, Role};
use super::error::Result;
use super::events::Events;
use super::head;
use super::quic::QuicConnection;
use super::shared::{Entry, Incoming, Registry, Shared};
use super::tasks::Tasks;
use crate::conn::{Conn, Role as CoreRole};
use crate::error::ErrorCode;
use crate::stream::StreamId;

/// Serves an established QUIC connection with a request handler.
///
/// Returns the driver. Nothing is served until it is polled.
///
/// Handlers run concurrently without being spawned: they are futures the driver holds, each
/// polled with a waker naming its own stream. This layer takes no executor, so there is
/// nowhere to spawn to.
///
/// The consequence is worth stating plainly: a handler that *blocks* rather than returning
/// `Pending` stalls its whole connection, because there is no other thread for that
/// connection to be on. A handler with blocking work in it should move that work elsewhere
/// and await the result.
pub fn serve<Q, H, F, B>(
    backend: Q,
    handler: H,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    Q: QuicConnection,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    serve_with(backend, handler, Config::default())
}

/// Serves a connection with settings other than the defaults.
pub fn serve_with<Q, H, F, B>(
    backend: Q,
    handler: H,
    config: Config,
) -> Result<Connection<impl Future<Output = Result<()>>>>
where
    Q: QuicConnection,
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    let shared = Arc::new(Shared::new());
    let registry = Arc::new(Registry::new());

    let conn = driver::build_conn(CoreRole::Server, &config, &shared)?;
    let core = Driver::new(
        backend,
        conn,
        Arc::clone(&shared),
        Arc::clone(&registry),
        config,
    );

    let role = ServerRole {
        handler,
        tasks: Tasks::new(Arc::clone(&shared)),
        cancels: Vec::new(),
        endings: Vec::new(),
        shared: Arc::clone(&shared),
        registry: Arc::clone(&registry),
        max_concurrent: config.max_concurrent_streams as usize,
        woken: Vec::new(),
        _body: core::marker::PhantomData,
    };

    let guard = DriverGuard::new(shared, registry, role);
    Ok(Connection::new(driver::run(core, guard)))
}

/// Tells a handler its exchange is over.
///
/// Placed in every request's extensions. A handler on an abandoned stream is **not**
/// dropped — it runs to completion and its response is discarded — because dropping a future
/// at an arbitrary await point is not something a caller can reason about. This is the
/// cooperative signal instead: a handler that cares can stop, and one that does not simply
/// finishes and has its answer thrown away.
#[derive(Clone)]
pub struct Cancelled {
    state: Arc<CancelState>,
}

#[derive(Default)]
struct CancelState {
    cancelled: Mutex<bool>,
    waker: Mutex<Option<Waker>>,
}

impl Cancelled {
    fn new() -> Self {
        Self {
            state: Arc::new(CancelState::default()),
        }
    }

    fn trip(&self) {
        *self
            .state
            .cancelled
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = true;
        if let Some(waker) = self
            .state
            .waker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            waker.wake();
        }
    }

    /// Whether the exchange has been abandoned.
    pub fn is_cancelled(&self) -> bool {
        *self
            .state
            .cancelled
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Resolves when the exchange is abandoned, and never otherwise.
    ///
    /// Intended to be raced against a handler's real work.
    pub async fn cancelled(&self) {
        core::future::poll_fn(|context: &mut Context<'_>| {
            if self.is_cancelled() {
                return Poll::Ready(());
            }
            *self.state.waker.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(context.waker().clone());
            if self.is_cancelled() {
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
    }
}

impl core::fmt::Debug for Cancelled {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Cancelled")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// What a server does that a client does not.
pub(crate) struct ServerRole<H, F, B> {
    handler: H,
    tasks: Tasks<F>,
    /// Cancellation signals, so a stream ending can reach the handler still running on it.
    cancels: Vec<(StreamId, Cancelled)>,
    /// Response bodies still running, and where each will report how it ended.
    endings: Vec<(StreamId, Arc<Mutex<Option<Ending>>>)>,
    shared: Arc<Shared>,
    registry: Arc<Registry>,
    max_concurrent: usize,
    woken: Vec<StreamId>,
    _body: core::marker::PhantomData<fn() -> B>,
}

impl<H, F, B> Role for ServerRole<H, F, B>
where
    H: FnMut(http::Request<IncomingBody>) -> F,
    F: Future<Output = http::Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn core::error::Error + Send + Sync>>,
{
    fn advance(&mut self, conn: &mut Conn<Events>, _events: &mut Events) -> Result<()> {
        self.finish_bodies(conn)?;

        self.woken.clear();
        self.tasks.take_woken(&mut self.woken);
        let woken = core::mem::take(&mut self.woken);

        for stream in woken {
            let Some(response) = self.tasks.poll(stream) else {
                continue;
            };

            // The exchange may have been abandoned while the handler ran. Its answer is
            // discarded rather than submitted onto a stream that is gone.
            if !self.registry.contains(stream) {
                self.forget(stream);
                continue;
            }

            let (parts, body) = response.into_parts();
            let fields = match head::response_fields(&parts) {
                Ok(fields) => fields,
                Err(_) => {
                    // A handler produced a head HTTP/3 will not carry. That is this
                    // endpoint's fault, not the peer's, so the exchange is abandoned rather
                    // than the connection failed.
                    self.shared.reset(stream, ErrorCode::new(REQUEST_CANCELLED));
                    self.forget(stream);
                    continue;
                }
            };

            let ending = Arc::new(Mutex::new(None));
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

            conn.submit_response(stream, &fields.views()?, source)?;
            // Only a body that exists can report how it ended; recording a slot for one
            // that cannot would grow this list with every request ever answered.
            if has_body {
                self.endings.push((stream, ending));
            }
        }
        Ok(())
    }

    fn head(
        &mut self,
        conn: &mut Conn<Events>,
        _events: &mut Events,
        stream: StreamId,
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        // Refused rather than queued. A peer that opens more exchanges than this endpoint
        // will run concurrently must be told so, or the queue is an unbounded allocation a
        // peer controls.
        if self.tasks.len() >= self.max_concurrent {
            let _ = conn.shutdown_stream_read(stream);
            self.shared.reset(stream, ErrorCode::new(REQUEST_CANCELLED));
            return Ok(());
        }

        let head = match head::request_head(fields) {
            Ok(head) => head,
            Err(_) => {
                // The peer sent something HTTP/3 forbids. One exchange is refused; the
                // connection carries on.
                let _ = conn.shutdown_stream_read(stream);
                self.shared.reset(stream, ErrorCode::new(REQUEST_CANCELLED));
                return Ok(());
            }
        };

        let incoming = Arc::new(Incoming::new());
        let liveness = Arc::new(());
        self.registry.insert(
            stream,
            Entry {
                slot: None,
                incoming: Arc::clone(&incoming),
                _liveness: Arc::clone(&liveness),
            },
        );

        let body = IncomingBody::new(
            stream,
            // A server's *request* body. Dropping it unread must not abandon the exchange:
            // a handler that ignores the body it was given still owes a response, and
            // abandoning here would destroy an exchange that is very much alive. This is
            // the asymmetry with a client's response body, and it is not cosmetic.
            false,
            incoming,
            Arc::clone(&self.shared),
            liveness,
        );

        let cancelled = Cancelled::new();
        self.cancels.push((stream, cancelled.clone()));

        let (mut parts, ()) = head.into_parts();
        parts.extensions.insert(cancelled);
        let request = http::Request::from_parts(parts, body);

        let future = (self.handler)(request);
        self.tasks.start(stream, future);
        Ok(())
    }

    fn closed(&mut self, stream: StreamId) {
        // The handler is *not* dropped. It runs to completion and its answer is discarded;
        // this only tells it, so a handler that wants to stop early can.
        //
        // The signal is dropped once tripped, though. Keeping one per exchange for the life
        // of the connection would make an ordinary sequence of requests an unbounded
        // allocation, which is the same failure as not having a limit at all.
        if let Some(index) = self.cancels.iter().position(|(s, _)| *s == stream) {
            let (_, cancelled) = self.cancels.swap_remove(index);
            cancelled.trip();
        }
        self.endings.retain(|(s, _)| *s != stream);
    }

    fn busy(&self) -> bool {
        // An ending nobody has read yet is a stream that has stopped producing bytes and
        // has not been reset. Parking on one would leave the peer waiting on a message
        // this endpoint has already abandoned, until the peer happened to say something.
        self.tasks.any_woken() || ending_pending(&self.endings)
    }

    fn done(&self) -> bool {
        // A server never runs out of work of its own accord: it waits for the peer, and the
        // driver decides the connection is over when the peer goes away.
        false
    }

    fn abandon(&mut self) {
        for (_, cancelled) in &self.cancels {
            cancelled.trip();
        }
        self.tasks.abandon_all();
    }

    fn settle(&mut self, conn: &mut Conn<Events>) -> Result<()> {
        self.finish_bodies(conn)
    }
}

impl<H, F: Future, B> ServerRole<H, F, B> {
    fn forget(&mut self, stream: StreamId) {
        self.tasks.forget(stream);
        self.cancels.retain(|(s, _)| *s != stream);
    }

    /// Acts on response bodies that have finished since the last pass.
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
                    let fields = head::trailer_fields(&trailers)?;
                    conn.submit_trailers(*stream, &fields.views()?)?;
                }
                Ending::Failed => {
                    // A handler's body failed. One exchange is abandoned; every other one
                    // on this connection carries on.
                    self.shared
                        .reset(*stream, ErrorCode::new(REQUEST_CANCELLED));
                }
            }
        }
        for index in done.into_iter().rev() {
            self.endings.swap_remove(index);
        }
        Ok(())
    }
}
