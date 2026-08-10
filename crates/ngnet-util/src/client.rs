//! The client itself: what a caller holds, clones and sends requests through.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Body;
use ngnet_h2::http::{Config, IncomingBody};

use crate::error::{Error, Reason};
use crate::origin::Origin;
use crate::pool::Pool;

/// A pooling HTTP/2 client.
///
/// Cheap to clone — every clone shares one pool, one set of connections and one shutdown, so
/// cloning it into tasks is the intended way to use it rather than something to avoid. It is
/// *not* a way to get an independent client: [`Client::shutdown`] called on any clone closes
/// them all, because they are the same client.
///
/// # The body type
///
/// `B` is the request body, fixed per client rather than per request. That is a real
/// constraint — a client cannot send a `Full<Bytes>` for one request and a streaming body for
/// the next — and it is inherited rather than chosen: `ngnet-h2`'s connection is generic over
/// its request body, so a pool of connections is too. A caller needing both should box:
/// `BoxBody<Bytes, E>` satisfies the bound and erases the difference.
///
/// Note what is *not* here: any body adapter. `ngnet-h2` already accepts any
/// `http_body::Body<Data = Bytes>` outbound and returns an [`IncomingBody`] that is already
/// `http_body::Body<Data = Bytes> + Send + 'static` inbound. The same finding `ngnet-axum`
/// records on the server side holds on the client side, so no payload is copied and no
/// conversion type exists to be maintained.
pub struct Client<B> {
    pool: Arc<Pool<B>>,
}

impl<B> Clone for Client<B> {
    /// Hand-written rather than derived: the derive would require `B: Clone`, which is not
    /// needed — only the `Arc` is cloned — and would exclude most body types for no reason.
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
        }
    }
}

impl<B> fmt::Debug for Client<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("closed", &self.pool.is_closed())
            .finish_non_exhaustive()
    }
}

impl<B> Default for Client<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<B> Client<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// A client with the default connection configuration.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Starts building a client with a non-default configuration.
    pub fn builder() -> Builder {
        Builder {
            config: Config::default(),
        }
    }

    /// Sends a request, opening or reusing a connection to the URI's origin.
    ///
    /// The returned future resolves when the response *head* arrives, not when the exchange
    /// ends — the body is streamed and can be read incrementally, which is what makes a large
    /// or slow response usable rather than something to buffer.
    ///
    /// Dropping the returned future cancels the exchange: the peer sees a stream reset.
    ///
    /// # Errors
    ///
    /// See [`ErrorKind`](crate::ErrorKind) for the four categories and, more usefully, for
    /// where the lines between them fall — two of them are not where they first appear.
    ///
    /// # Waiting
    ///
    /// If a dial to this origin is already in progress, this waits for it rather than
    /// starting a second one. That wait is **unbounded**: there is no connect timeout here,
    /// deliberately. A caller that wants one has exactly one place to put it — around this
    /// future, with [`tokio::time::timeout`] — and a timeout chosen here would be a guess
    /// applied to every caller, overridable by none of them.
    pub fn request(&self, request: http::Request<B>) -> ResponseFuture {
        let pool = Arc::clone(&self.pool);

        ResponseFuture {
            inner: Box::pin(async move {
                // Checked before the URI is even parsed, so a request offered to a closed
                // client cannot resolve a name or open a socket on its way to being refused.
                if pool.is_closed() {
                    return Err(Error::closed(Reason("the client has been shut down")));
                }

                let origin = Origin::from_uri(request.uri())?;
                let handle = pool.acquire(&origin).await?;

                // Handing the request over is the point of no return, and it is the reason
                // this crate does not retry. `send_request` consumes the request and returns
                // only a response future: there is no error path that gives it back, so a
                // retry would need a copy made before every request on the chance that one
                // was needed. That impossibility *is* the safety property — no request can be
                // silently replayed, because none can be held.
                handle.send_request(request).await.map_err(|source| {
                    // A refusal observed while this client is shutting down is this end's
                    // doing, not the peer's, even though `ngnet-h2` reports one category for
                    // both. It is not retriable: repeating it against a closing client would
                    // fail identically for ever.
                    if source.is_retriable() && pool.is_closed() {
                        return Error::closed(source);
                    }
                    Error::exchange(source.is_retriable(), source)
                })
            }),
        }
    }

    /// Winds every connection down and waits for them all to end.
    ///
    /// Each connection is told to go away — the peer receives a `GOAWAY` — and exchanges
    /// already in flight run to completion. This resolves when the last of them has finished
    /// and every driver task has ended, so when it returns, the connections really are gone.
    ///
    /// Requests offered after this starts fail with [`ErrorKind::Closed`](crate::ErrorKind::Closed).
    ///
    /// Concurrent and repeated calls all report the same completion: a second caller does not
    /// return early on the strength of the flag already being set, because that would be
    /// reporting a drain it had not observed.
    ///
    /// # There is no deadline
    ///
    /// Deliberately, and consistently with the server side. A response body the caller never
    /// reads holds its exchange open, which holds this pending — that is a real trap and it is
    /// the caller's to avoid, because only the caller knows how long is too long. A deadline
    /// chosen here would be a guess that silently truncated somebody's upload. Wrap this in
    /// [`tokio::time::timeout`] if you want one.
    pub async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    /// Whether this client has been shut down.
    ///
    /// Advisory, in the way such predicates always are: it can become true immediately after
    /// being read. Useful for a supervisor deciding whether to build a replacement; not
    /// useful as a guard before [`Client::request`], which performs its own check.
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// The pool behind this client, for the hidden testing module.
    pub(crate) fn pool(&self) -> &Arc<Pool<B>> {
        &self.pool
    }
}

/// Builds a [`Client`] with a non-default connection configuration.
#[derive(Debug, Clone)]
pub struct Builder {
    config: Config,
}

impl Builder {
    /// Sets the configuration every connection this client dials is created with.
    ///
    /// One configuration for the whole pool rather than one per origin. Per-origin settings
    /// would need a way to name an origin before it has been dialled, which is a bigger API
    /// than any evidence so far justifies.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Builds the client.
    pub fn build<B>(self) -> Client<B>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Client {
            pool: Arc::new(Pool::new(self.config)),
        }
    }
}

/// Resolves when a request's response head arrives.
///
/// A named type rather than `impl Future`, so it can be written in a signature, stored in a
/// struct, or returned from a trait method — which the [`tower_service::Service`] impl
/// requires.
///
/// The boxed future inside is one allocation per request. It is acknowledged rather than
/// hidden: acquiring a connection is genuinely async and genuinely branching, and writing it
/// as a hand-rolled state machine would trade a single allocation for a large amount of
/// `unsafe`-adjacent code in a crate that denies `unsafe`. No payload is copied either way.
pub struct ResponseFuture {
    inner: Pin<Box<dyn Future<Output = Result<http::Response<IncomingBody>, Error>> + Send>>,
}

impl fmt::Debug for ResponseFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish_non_exhaustive()
    }
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<IncomingBody>, Error>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

/// The client as a [`tower_service::Service`].
///
/// This is the mirror of how `ngnet-axum` consumes axum's `Router` on the server side: there,
/// a `Service` is *called*; here, one is *provided*. It means any tower middleware —
/// timeouts, retries, rate limits, tracing — layers over this client without either side
/// knowing about the other, and it is why retries are not built in: `tower::retry` already
/// exists and knows things about the caller's requests that this crate cannot.
///
/// The inherent [`Client::request`] is the primary API and this delegates to it, rather than
/// the reverse. A caller who wants a response should not have to learn what a `Service` is
/// first.
impl<B> tower_service::Service<http::Request<B>> for Client<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = http::Response<IncomingBody>;
    type Error = Error;
    type Future = ResponseFuture;

    /// Always ready, and **this is not backpressure**.
    ///
    /// The honest reason: readiness is asked before the request exists, and everything that
    /// could make this client unready — whether a connection to *that origin* exists, whether
    /// it is refusing — is a property of an origin that arrives with the request. Reporting
    /// pending here would mean blocking every origin on one, which is worse than useless.
    ///
    /// So this dials nothing and reserves nothing. A caller wanting to limit concurrency
    /// should use a tower layer that does, over the top of this.
    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        Client::request(self, request)
    }
}
