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

use super::body::IncomingBody;
use super::driver::{self, DriverGuard};
use super::error::{Error, Result};
use super::shared::{Command, HandleToken, Queue, Registry, Shared, Slot};
use super::transport::Transport;

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
        Arc::clone(&queue),
        Arc::clone(&registry),
    );

    let connection = driver::run(
        transport,
        session,
        Arc::clone(&shared),
        Arc::clone(&queue),
        Arc::clone(&registry),
        Arc::downgrade(&token),
        guard,
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
