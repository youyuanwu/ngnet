//! The accept loop, and the builder that configures it.
//!
//! [`serve`] is shaped to match `axum::serve`, so that swapping the engine is a one-line
//! change: the listener and the router go in, the returned value is a future, and every
//! builder method is optional. What it does *between* accepting and serving is where the
//! two differ, and those differences are documented at the point of use rather than
//! collected somewhere a caller will not look.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::{Future, IntoFuture, pending, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;

use axum::Router;
use ngnet_h2::http::{Config, Drain};
use tokio::task::AbortHandle;

use crate::error::{Error, HandlerPanic};
use crate::listener::Listener;
use crate::transport::ServableTransport;

/// The callback failures are reported through, if the caller installed one.
///
/// Generic over the address because the listener chooses it; see [`Error`] for why the
/// address is carried as a type rather than erased to a string.
type Observer<A> = Box<dyn FnMut(Error<A>) + Send>;

/// The observer once the server is running, shared with every connection task.
///
/// Connection tasks report their own outcomes, so the callback has to be reachable from all
/// of them at once. `Arc<Mutex<_>>` is what makes that possible **without changing
/// [`Serve::on_error`]'s bound**: `Mutex<T>: Sync` requires only `T: Send`, which the
/// existing `FnMut(..) + Send + 'static` already gives, and the mutex supplies the interior
/// mutability an `FnMut` needs. A caller's closure is unaffected.
type SharedObserver<A> = Arc<Mutex<Observer<A>>>;

/// Serves `router` on every connection `listener` accepts, over cleartext HTTP/2.
///
/// The returned [`Serve`] is a future; awaiting it runs the server. It is also a builder,
/// and every method on it is optional, so the common case is the one-line substitution this
/// crate exists for:
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let router = axum::Router::new();
/// let tcp = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
///
/// ngnet_axum::serve(ngnet_axum::TcpListener::new(tcp), router).await;
/// # Ok(())
/// # }
/// ```
///
/// Any [`Listener`] will do. TCP is one implementation among several rather than the shape
/// the server is built around: [`UnixListener`](crate::UnixListener) ships too, and a
/// third-party listener over TLS or an in-memory pipe is served by the same call.
///
/// Awaiting works directly, but [`tokio::spawn`] takes a [`Future`] rather than an
/// [`IntoFuture`], so a server that runs on its own task needs an explicit
/// `.into_future()`. `axum::serve` has the same wrinkle for the same reason.
///
/// The server accepts an unbounded number of simultaneous connections. There is no cap and
/// no way to set one; a deployment that needs a bound must impose it in front of the
/// listener.
pub fn serve<L: Listener>(listener: L, router: Router) -> Serve<L> {
    Serve {
        listener,
        router,
        config: Config::default(),
        observer: None,
        stop: None,
    }
}

/// A configured server, and the future that runs it. Built by [`serve`].
#[must_use = "a Serve does nothing until awaited"]
pub struct Serve<L: Listener> {
    listener: L,
    router: Router,
    config: Config,
    observer: Option<Observer<L::Addr>>,
    stop: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<L: Listener> Serve<L> {
    /// Sets the HTTP/2 configuration applied to every connection this server accepts.
    ///
    /// Replaces the configuration wholesale rather than merging, so build the [`Config`]
    /// with the setters it provides and pass the result.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Installs a callback invoked once per failure, whether accepting or on a connection.
    ///
    /// Failures cannot be returned: a running accept loop outlives them and is expected to
    /// keep going. So they are reported here, and **if no observer is installed they are
    /// dropped silently**. That is deliberate rather than an oversight — a library writing
    /// to a log the caller never configured is harder to live with than one that says
    /// nothing — but it does mean a server built without this method fails invisibly.
    ///
    /// # Where the callback runs, and what that means for it
    ///
    /// Almost always on the connection task that failed, not on the accept loop. A
    /// connection reports its own outcome the moment it has one, so a failure is reported
    /// when it happens rather than when the loop next gets round to looking. The one
    /// exception is a failure setting a connection *up*, which happens before any task
    /// exists and is therefore reported from the accept loop.
    ///
    /// Three consequences worth knowing:
    ///
    /// - **Invocations are serialised**, so the callback sees one failure at a time and
    ///   needs no locking of its own. It still should not block: it holds that lock while it
    ///   runs, and a slow callback delays every other connection trying to report.
    /// - **Order across connections is unspecified.** Two connections failing at once are
    ///   reported in whichever order they reach the lock. Failures on a single connection
    ///   keep their order, there being at most one.
    /// - **It must not re-enter the server** — awaiting this server's future, or dropping
    ///   it, from inside the callback is not supported.
    ///
    /// A callback that panics while reporting from a connection task does not disable
    /// reporting for the others. One that panics while reporting a connection *setup*
    /// failure ends the server, because that one runs on the accept loop; a panic in caller
    /// code is not something to swallow and carry on from there.
    pub fn on_error(mut self, observer: impl FnMut(Error<L::Addr>) + Send + 'static) -> Self {
        self.observer = Some(Box::new(observer));
        self
    }

    /// Drains when `signal` resolves: stops accepting, tells every connected peer to wind
    /// up, lets what is in flight finish, and then resolves.
    ///
    /// Precisely, on the signal: the listener is dropped, so the port is released and
    /// nothing new is accepted; every live connection is sent a `GOAWAY` naming the last
    /// request it had begun, so the peer learns which of its requests will be answered and
    /// may retry the rest elsewhere; requests already being served run to completion and
    /// their responses are delivered normally; each connection closes as its last stream
    /// finishes; and this future resolves when the last connection has closed.
    ///
    /// Handlers are *not* cancelled and their futures are not dropped. A request that was
    /// in flight when the signal arrived gets the same response it would have got had the
    /// signal never come.
    ///
    /// # There is no deadline, deliberately
    ///
    /// A drain waits for the requests in flight, and a handler that never finishes will
    /// hold its connection — and therefore this future — open indefinitely. Nothing here
    /// can tell such a handler apart from a slow one, and inventing a timeout would mean
    /// choosing, on the caller's behalf, a duration after which their users' requests get
    /// truncated. Bound it outside if it needs bounding: race this future against a timer,
    /// or drop it, which ends every connection at once. `axum::serve` leaves the same
    /// choice to its caller for the same reason.
    pub fn with_graceful_shutdown(
        mut self,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        self.stop = Some(Box::pin(signal));
        self
    }
}

impl<L> IntoFuture for Serve<L>
where
    L: Listener,
    L::Addr: Clone + Debug + Send + Sync + 'static,
{
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(run(self))
    }
}

impl<L: Listener> std::fmt::Debug for Serve<L> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The router, the observer and the stop signal are all opaque; naming the fields
        // without pretending to show them is more use than deriving nothing.
        //
        // The listener is shown by type name rather than by value. `Listener` does not
        // require `Debug` -- axum's does not either -- and adding the bound here would
        // propagate to every caller that merely wants a `Serve` to be printable, which is a
        // steep price for one field. The type name is what a reader is actually looking for
        // when debugging a transport-generic server anyway: which listener is this.
        formatter
            .debug_struct("Serve")
            .field("listener", &std::any::type_name::<L>())
            .field("config", &self.config)
            .field("observer", &self.observer.is_some())
            .field("stop_signal", &self.stop.is_some())
            .finish()
    }
}

/// Hands `error` to the observer, if one was installed.
///
/// The single point where the silently-dropped default lives, and the only place the
/// observer is invoked -- which now matters more than it did, because the callers are spread
/// across every connection task rather than confined to the accept loop.
///
/// A poisoned lock is *recovered from* rather than propagated. Poisoning here means one
/// caller callback panicked while reporting; taking that as a reason to stop reporting on
/// every other connection would turn one bad report into a silent server. The mutex guards a
/// callback, not an invariant that a panic could have left half-updated.
fn report<A>(observer: &Option<SharedObserver<A>>, error: Error<A>) {
    if let Some(observe) = observer {
        let mut observe = observe.lock().unwrap_or_else(PoisonError::into_inner);
        observe(error);
    }
}

/// What the accept loop keeps for a connection it has spawned.
///
/// Only what *shutdown* needs, which is why there is no peer address here any more: the task
/// reports its own outcome and carries its own peer, so nothing has to be looked up by id.
///
/// The drain handle is here because by the time the loop wants it, the connection itself has
/// been moved into its task. The abort handle is here because dropping the server future has
/// to end every connection at once, and a bare [`JoinHandle`](tokio::task::JoinHandle)
/// *detaches* when dropped where a [`JoinSet`](tokio::task::JoinSet) aborted -- so the
/// aborting has to be done deliberately now that the set is gone.
struct Live {
    drain: Drain,
    abort: Option<AbortHandle>,
}

/// The live connections, shared so each task can remove its own entry when it finishes.
type Registry = Arc<Mutex<HashMap<u64, Live>>>;

/// Ends every still-registered connection when the server future is dropped.
///
/// [`Serve::with_graceful_shutdown`] documents that dropping this future "ends every
/// connection at once", and until this guard existed that was delivered only as a side
/// effect of [`JoinSet`](tokio::task::JoinSet) aborting its tasks on drop. Spawning
/// individually gives up that side effect silently -- a dropped
/// [`JoinHandle`](tokio::task::JoinHandle) detaches, leaving the connection running with
/// nothing left to stop it -- so the guarantee is now made explicitly, by this type,
/// or not at all.
struct AbortLive(Registry);

impl Drop for AbortLive {
    fn drop(&mut self) {
        let live = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for connection in live.values() {
            if let Some(abort) = &connection.abort {
                abort.abort();
            }
        }
    }
}

/// Runs `connection` to completion, converting a panic into a value instead of an unwind.
///
/// A handler panic unwinds out of the connection future. It used to be observed as a
/// [`JoinError`](tokio::task::JoinError) once the accept loop joined the task, which is no
/// longer possible: a task that reports its own outcome cannot report that it died. So the
/// panic is caught here, on the task's own stack, before it can take the task with it.
///
/// [`pin!`](std::pin::pin) pins the connection to this function's stack frame, so projecting
/// through it needs neither `unsafe` -- which this crate denies -- nor a pin-projection
/// dependency. [`AssertUnwindSafe`] is sound because a caught panic is terminal for this
/// connection: nothing polls it again, so nothing can observe whatever state the unwind left
/// behind.
///
/// Catching is also strictly more informative than joining was. A `JoinError` said only that
/// the task panicked; the payload here still carries the panic's own message.
async fn catch_panics<F: Future>(connection: F) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut connection = pin!(connection);

    poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| connection.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

async fn run<L>(server: Serve<L>)
where
    L: Listener,
    L::Addr: Clone + Debug + Send + Sync + 'static,
{
    let Serve {
        mut listener,
        router,
        config,
        observer,
        stop,
    } = server;

    // Wrapped once, here, rather than in the builder: `Serve` is also a plain value a caller
    // can hold and reconfigure, and it has no business carrying a lock it is not yet using.
    let observer: Option<SharedObserver<L::Addr>> = observer.map(|observe| Arc::new(Mutex::new(observe)));

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    let mut next_id: u64 = 0;

    // The barrier shutdown waits on. Every connection task holds a receiver for exactly as
    // long as it runs, so when the last one is gone the sender's `closed()` resolves. This
    // is axum's arrangement, and it replaces joining a `JoinSet` -- which cannot be done
    // from a two-arm loop, because joining is the third arm.
    let (close, close_token) = tokio::sync::watch::channel(());

    // Held for the rest of this function. If the server future is dropped instead of
    // completing, this is what ends the connections that were still live.
    let _abort_live = AbortLive(Arc::clone(&registry));

    let mut stop = stop.unwrap_or_else(|| Box::pin(pending()));

    loop {
        // Two arms, which is the whole shape of this loop and worth stating plainly: the
        // stop signal, which breaks, and accepting, which continues. `axum::serve` has the
        // same two and no more.
        //
        // That matters to anyone implementing `Listener`. `select!` drops the futures of
        // the arms it did not take, so every arm that fires *and continues the loop* forces
        // the accept future to be built again from scratch. This loop used to have a third
        // such arm -- harvesting finished connections -- and it fired constantly, which made
        // a relative sleep inside `accept` useless: the sleep never survived long enough to
        // elapse. Connection outcomes are now reported by the connections themselves, that
        // arm is gone, and the accept future is dropped at most once per server: at
        // shutdown.
        //
        // `biased` gives the stop signal strict priority, because a flat `select!` chooses
        // *at random* among ready branches: with a stop signal already fired and a client
        // already queued, half the time the loop would admit that connection and then
        // immediately drain it, having served nothing.
        tokio::select! {
            biased;

            () = &mut stop => break,

            // No error arm: `Listener::accept` yields a connection or does not return,
            // because acceptance failure is the listener's to classify and pace. That is
            // axum's shape, and it is now safe here for axum's reason rather than in spite
            // of a difference -- see `Listener::accept`.
            (transport, peer) = listener.accept() => {
                // `serve_connection` can fail *here*, before any task exists, because
                // creating the HTTP/2 session is fallible. There is no connection task to
                // report such a failure from, so it is reported on the spot; it is the easy
                // path to miss, since it looks like the infallible half of setup.
                //
                // The peer is cloned rather than moved because it is needed twice: once
                // inside the connection, for handlers to read, and once by the task, to name
                // the connection in any failure it reports.
                match transport.serve(router.clone(), peer.clone(), config) {
                    Ok(connection) => {
                        // Taken before the connection is spawned, because afterwards the
                        // connection has been moved into the task and there is nothing left
                        // here to ask.
                        let drain = connection.drain_handle();

                        let id = next_id;
                        next_id = next_id.wrapping_add(1);

                        // Registered *before* spawning, not after. `tokio::spawn` can have
                        // run the task to completion before it returns, and a task that
                        // finished removes its own entry -- so registering afterwards can
                        // insert an entry nobody will ever remove, one per connection, for
                        // the life of the server.
                        registry
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(id, Live { drain, abort: None });

                        let task_observer = observer.clone();
                        let task_registry = Arc::clone(&registry);
                        let task_token = close_token.clone();

                        let handle = tokio::spawn(async move {
                            // Held, never explicitly dropped. This is what orders the
                            // barrier after deregistration: the token cannot be released
                            // before the task body finishes, so `close.closed()` cannot
                            // resolve while any entry is still registered.
                            let _token = task_token;

                            let outcome = catch_panics(connection).await;

                            // Deregistered before reporting, so the caller's callback never
                            // runs with this lock held. This is the *only* place an entry is
                            // removed: a task reaches this line unless it panicked -- which
                            // `catch_panics` prevents -- or was aborted, which happens only
                            // when the server future is dropped and the registry with it.
                            task_registry
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .remove(&id);

                            match outcome {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => report(&task_observer, Error::connection(peer, error)),
                                // A panic in a *response body* never reaches here: it is
                                // pulled from inside an `extern "C"` callback and aborts the
                                // process instead.
                                Err(payload) => report(
                                    &task_observer,
                                    Error::connection(peer, HandlerPanic::new(payload)),
                                ),
                            }
                        });

                        // Backfilled with `get_mut`, never `insert`: the task may already
                        // have finished and removed itself, and re-inserting would resurrect
                        // the entry permanently.
                        //
                        // The entry is momentarily `abort: None`, which is safe against
                        // `AbortLive` only because this future can be dropped solely while
                        // suspended at the `select!` above, and insert -> spawn -> backfill
                        // is synchronous. **No `.await` may be introduced between them.**
                        if let Some(live) = registry
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .get_mut(&id)
                        {
                            live.abort = Some(handle.abort_handle());
                        }
                    }
                    Err(error) => report(&observer, Error::connection(peer, error)),
                }
            },
        }
    }

    // Dropping the listener releases the port and stops anything further being queued.
    // Done first, so that the window between deciding to stop and having stopped is as
    // short as it can be.
    drop(listener);

    // Then tell every live connection to wind up. This is the half that makes it a drain
    // rather than a quiesce: each peer gets a `GOAWAY` naming the last request that
    // connection had begun, so it learns which of its requests will still be answered and
    // may take the rest elsewhere. Requests already in flight are untouched and their
    // handlers keep running; the connection ends when the last of them has been answered.
    //
    // Every live connection, not just the idle ones. A connection in the middle of serving
    // a request is exactly the one whose peer most needs to be told, and telling it does
    // not disturb the request in flight.
    //
    // Collected out from under the lock before any of them is used, so that nothing else is
    // held while a connection is touched.
    let draining: Vec<Drain> = registry
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .values()
        .map(|live| live.drain.clone())
        .collect();

    for drain in draining {
        drain.drain();
    }

    // Then wait for them. Releasing this loop's own token first is what lets the count reach
    // zero at all.
    drop(close_token);
    close.closed().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    /// Collects everything reported, so a test can assert on it.
    fn collector() -> (Option<SharedObserver<SocketAddr>>, Arc<Mutex<Vec<Error>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let observer: Observer<SocketAddr> =
            Box::new(move |error| sink.lock().expect("a lock").push(error));
        (Some(Arc::new(Mutex::new(observer))), seen)
    }

    /// A reported failure reaches the observer with its peer intact.
    ///
    /// There is no accept-level counterpart any more, and its absence is the point of this
    /// change: a listener classifies and paces its own acceptance failures, so none can
    /// reach an observer.
    #[test]
    fn a_connection_failure_is_reported_with_its_peer() {
        let (observer, seen) = collector();
        let peer: SocketAddr = "127.0.0.1:5555".parse().expect("an address");

        report(
            &observer,
            Error::connection(peer, std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        );

        let seen = seen.lock().expect("a lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].peer_addr().0, peer);
    }

    /// SC-016: a server is debug-formattable even when its listener is not.
    ///
    /// `Listener` does not require `Debug`, and this pins that `Serve` did not quietly
    /// acquire the bound anyway -- the double here deliberately does not implement it. The
    /// listener's *type* still has to appear, because "which transport is this server on"
    /// is the question a reader is asking when they print one.
    #[test]
    fn a_server_is_debug_formattable_even_when_its_listener_is_not() {
        struct Undebuggable;

        impl Listener for Undebuggable {
            type Io = ngnet_h2::http::transport::TokioIo<tokio::io::DuplexStream>;
            type Addr = SocketAddr;

            async fn accept(&mut self) -> (Self::Io, Self::Addr) {
                std::future::pending().await
            }
        }

        let rendered = format!("{:?}", serve(Undebuggable, Router::new()));

        assert!(
            rendered.contains("Undebuggable"),
            "the listener's type should be named, got {rendered}"
        );
    }

    /// The documented default. Silence is a choice, so it is pinned like any other
    /// behaviour rather than left to be discovered.
    #[test]
    fn without_an_observer_a_failure_is_dropped_rather_than_panicking() {
        let observer: Option<SharedObserver<SocketAddr>> = None;
        let peer: SocketAddr = "127.0.0.1:5555".parse().expect("an address");

        report(
            &observer,
            Error::connection(peer, std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        );
    }
}
