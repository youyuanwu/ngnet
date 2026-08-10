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
    ///
    /// Note the inference wart, which is real and is not hidden: this sits on `Client<B>`,
    /// so writing `Client::builder()` where `B` cannot be worked out from context does not
    /// compile — and it usually cannot, because [`Builder::build`] chooses its own `B`
    /// independently. Either name it, `Client::<Full<Bytes>>::builder()`, or start from
    /// [`Builder::new`], which has no type parameter to infer. The method is kept because it
    /// is where a reader looks first.
    pub fn builder() -> Builder {
        Builder::new()
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
                handle
                    .send_request(request)
                    .await
                    .map_err(|source| classify(source, pool.is_closed()))
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

/// Decides which of this crate's categories a failed exchange belongs to.
///
/// A free function, and not inlined at the one call site, for a reason worth stating: the
/// branch below is all but unreachable on purpose. `Client::request` checks for a closed
/// client before it does anything else, so reaching `send_request` at all means the client
/// was open at that moment, and `closed` can only be true if a shutdown started in the
/// window between the two. A test cannot reliably win that race — one that tried would pass
/// most of the time without ever entering the branch, which is worse than no test.
///
/// So the classification is separated from the timing, and the timing is left untested while
/// the classification is tested directly.
///
/// # What it decides
///
/// `ngnet-h2` reports one category for both "the peer refused this stream" and "this end is
/// shutting down", because from a connection's point of view they are the same event: a
/// stream that will not be begun. They are not the same for a caller. A peer's refusal names
/// a stream that was provably never acted on, so replaying it elsewhere is safe. Our own
/// shutdown will refuse the next attempt identically, for ever, so calling that retriable
/// invites a caller into a loop.
fn classify(source: ngnet_h2::http::Error, closed: bool) -> Error {
    if source.is_retriable() && closed {
        return Error::closed(source);
    }
    Error::exchange(source.is_retriable(), source)
}

/// Builds a [`Client`] with a non-default connection configuration.
#[derive(Debug, Clone)]
pub struct Builder {
    config: Config,
}

impl Builder {
    /// A builder with the default connection configuration.
    ///
    /// The entry point that always works. [`Client::builder`] is the same thing behind a
    /// type parameter that has to be inferred first.
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

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

impl Default for Builder {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    //! The one classification a socket cannot be made to produce on demand.

    use super::*;
    use crate::ErrorKind;

    /// A refusal, as `ngnet-h2` reports one.
    ///
    /// Obtained by asking a client that has been shut down to send a request, which is the
    /// only way to get one without a peer: the error type has no public constructor, and
    /// rightly so.
    async fn a_refusal() -> ngnet_h2::http::Error {
        let (transport, _peer) = tokio::io::duplex(1024);
        let (handle, _driver) = ngnet_h2::http::client::handshake_shared::<
            _,
            http_body_util::Full<Bytes>,
        >(ngnet_h2::http::transport::TokioIo::new(transport))
        .expect("a session can be created over a duplex pipe");

        handle.shutdown();
        handle
            .send_request(
                http::Request::get("http://example.com/")
                    .body(http_body_util::Full::new(Bytes::new()))
                    .expect("a valid request"),
            )
            .await
            .expect_err("a shut-down connection refuses new requests")
    }

    #[tokio::test]
    async fn a_refusal_from_an_open_client_is_a_retriable_exchange_failure() {
        let error = classify(a_refusal().await, false);
        assert_eq!(error.kind(), ErrorKind::Exchange);
        assert!(
            error.is_retriable(),
            "a stream the peer never began is safe to replay elsewhere"
        );
    }

    #[tokio::test]
    async fn a_refusal_while_this_end_is_closing_is_reported_as_closed() {
        let error = classify(a_refusal().await, true);
        assert_eq!(
            error.kind(),
            ErrorKind::Closed,
            "our own shutdown is not the peer refusing us"
        );
        assert!(
            !error.is_retriable(),
            "retrying against a closing client fails identically for ever"
        );
    }
}
