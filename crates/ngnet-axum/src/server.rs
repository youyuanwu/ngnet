//! The accept loop, and the builder that configures it.
//!
//! [`serve`] is shaped to match `axum::serve`, so that swapping the engine is a one-line
//! change: the listener and the router go in, the returned value is a future, and every
//! builder method is optional. What it does *between* accepting and serving is where the
//! two differ, and those differences are documented at the point of use rather than
//! collected somewhere a caller will not look.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::{Future, IntoFuture, pending};
use std::pin::Pin;

use axum::Router;
use ngnet_h2::http::{Config, Drain};
use tokio::task::{Id, JoinSet};

use crate::error::Error;
use crate::listener::Listener;
use crate::transport::ServableTransport;

/// The callback failures are reported through, if the caller installed one.
///
/// Generic over the address because the listener chooses it; see [`Error`] for why the
/// address is carried as a type rather than erased to a string.
type Observer<A> = Box<dyn FnMut(Error<A>) + Send>;

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
    /// The callback runs on the accept loop's own task, so it should not block.
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
/// Trivial, and factored out for two reasons: it is the single point where the
/// silently-dropped default lives, and it is the only place the observer is invoked.
fn report<A>(observer: &mut Option<Observer<A>>, error: Error<A>) {
    if let Some(observe) = observer.as_mut() {
        observe(error);
    }
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
        mut observer,
        stop,
    } = server;

    let mut connections = JoinSet::new();
    // A panicked task returns nothing and its `JoinError` carries no payload of ours, so
    // the peer address cannot ride along with the task's own output — it has to be kept
    // here and looked up by task id. Bounded by the number of live connections: every
    // harvest removes its entry.
    let mut peers: HashMap<Id, Live<L::Addr>> = HashMap::new();
    let mut stop = stop.unwrap_or_else(|| Box::pin(pending()));

    loop {
        // Two levels, and the nesting is the point. The stop signal is given strict
        // priority via `biased`, because a flat `select!` chooses *at random* among ready
        // branches: with a stop signal already fired and a client already queued, half the
        // time the loop would admit that connection and then immediately drain it, having
        // served nothing. Serving a connection accepted after the stop signal is the
        // one thing FR-011 forbids.
        //
        // Below that, accepting and harvesting arbitrate *unbiased* against each other. A
        // single biased list would have to rank them, and either order starves the other
        // under sustained load: accept-first leaves finished connections unreported while
        // clients keep arriving, harvest-first stops accepting while connections keep
        // ending.
        tokio::select! {
            biased;

            () = &mut stop => break,

            () = async {
                tokio::select! {
            // No error arm: `Listener::accept` yields a connection or does not return,
            // because acceptance failure is the listener's to classify and pace. That is
            // axum's shape, and the reason it is safe here despite this loop dropping the
            // accept future is that the pacing state lives in the listener rather than in
            // the future -- see `Listener::accept`, which says so where an implementor
            // will read it.
            (transport, peer) = listener.accept() => {
                // `serve_connection` can fail *here*, before any task exists, because
                // creating the HTTP/2 session is fallible. Such a failure can never
                // arrive through the JoinSet, so it is reported on the spot; it is the
                // easy path to miss, since it looks like the infallible half of setup.
                //
                // The peer is cloned rather than moved because it is needed twice: once
                // inside the connection, for handlers to read, and once here, to name the
                // connection in any failure it reports later.
                match transport.serve(router.clone(), peer.clone(), config) {
                    Ok(connection) => {
                        // Taken before the connection is spawned, because afterwards
                        // the connection has been moved into the task and there is
                        // nothing left here to ask.
                        let drain = connection.drain_handle();
                        let handle = connections.spawn(connection);
                        peers.insert(handle.id(), Live { peer, drain });
                    }
                    Err(error) => report(&mut observer, Error::connection(peer, error)),
                }
            },

            // Harvested here rather than after the loop so that a connection which fails
            // while the server is still accepting is reported when it fails, instead of
            // whenever the next client happens to turn up. The guard matters: on an empty
            // set this is immediately ready with `None`, which would spin.
            Some(joined) = connections.join_next_with_id(), if !connections.is_empty() => {
                harvest(&mut observer, &mut peers, joined);
            }
                }
            } => {}
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
    for live in peers.values() {
        live.drain.drain();
    }

    while let Some(joined) = connections.join_next_with_id().await {
        harvest(&mut observer, &mut peers, joined);
    }
}

/// What the accept loop keeps for a connection it has spawned.
///
/// The address is here because a panicked task carries no payload of ours and the peer has
/// to be recoverable by task id. The drain handle is here because by the time the loop
/// wants it, the connection itself has been moved into its task.
struct Live<A> {
    peer: A,
    drain: Drain,
}

/// Reports one finished connection, and forgets its peer.
///
/// The two arms take the task id from different places, which is easy to get wrong: a
/// successful join returns it alongside the output, while a panicked one carries it on the
/// [`JoinError`](tokio::task::JoinError). Taking it from the wrong place leaks a map entry
/// on exactly the path that most needs the address.
fn harvest<A>(
    observer: &mut Option<Observer<A>>,
    peers: &mut HashMap<Id, Live<A>>,
    joined: Result<(Id, ngnet_h2::http::Result<()>), tokio::task::JoinError>,
) {
    match joined {
        Ok((id, outcome)) => {
            let peer = peers.remove(&id).map(|live| live.peer);
            if let (Err(error), Some(peer)) = (outcome, peer) {
                report(observer, Error::connection(peer, error));
            }
        }
        Err(join_error) => {
            let peer = peers.remove(&join_error.id()).map(|live| live.peer);
            // A handler panic unwinds out of the connection future and so out of its task,
            // which is what makes it observable here at all. A panic in a *response body*
            // never reaches this point: it is pulled from inside an `extern "C"` callback
            // and aborts the process instead.
            if let Some(peer) = peer {
                report(observer, Error::connection(peer, join_error));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    /// Collects everything reported, so a test can assert on it.
    fn collector() -> (Option<Observer<SocketAddr>>, Arc<Mutex<Vec<Error>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let observer: Option<Observer<SocketAddr>> = Some(Box::new(move |error| {
            sink.lock().expect("a lock").push(error)
        }));
        (observer, seen)
    }

    /// A reported failure reaches the observer with its peer intact.
    ///
    /// There is no accept-level counterpart any more, and its absence is the point of this
    /// change: a listener classifies and paces its own acceptance failures, so none can
    /// reach an observer.
    #[test]
    fn a_connection_failure_is_reported_with_its_peer() {
        let (mut observer, seen) = collector();
        let peer: SocketAddr = "127.0.0.1:5555".parse().expect("an address");

        report(
            &mut observer,
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
        let mut observer: Option<Observer<SocketAddr>> = None;
        let peer: SocketAddr = "127.0.0.1:5555".parse().expect("an address");

        report(
            &mut observer,
            Error::connection(peer, std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        );
    }
}
