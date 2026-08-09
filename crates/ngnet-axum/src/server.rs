//! The accept loop, and the builder that configures it.
//!
//! [`serve`] is shaped to match `axum::serve`, so that swapping the engine is a one-line
//! change: the listener and the router go in, the returned value is a future, and every
//! builder method is optional. What it does *between* accepting and serving is where the
//! two differ, and those differences are documented at the point of use rather than
//! collected somewhere a caller will not look.

use std::collections::HashMap;
use std::future::{Future, IntoFuture, pending};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use axum::Router;
use ngnet_h2::http::Config;
use tokio::net::TcpListener;
use tokio::task::{Id, JoinSet};

use crate::connection::serve_connection;
use crate::error::Error;

/// How long to wait before accepting again after a failure that will recur.
///
/// A transient accept error is reported and retried at once. A systemic one — the process
/// being out of file descriptors is the usual case — is true of the *listener* rather than
/// of one client, so retrying immediately produces an unbounded stream of identical
/// failures and no progress. Backing off turns that into one report a second, which a
/// caller can see and act on. The value matches `axum::serve`'s.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// The callback failures are reported through, if the caller installed one.
type Observer = Box<dyn FnMut(Error) + Send>;

/// Serves `router` on every connection `listener` accepts, over cleartext HTTP/2.
///
/// The returned [`Serve`] is a future; awaiting it runs the server. It is also a builder,
/// and every method on it is optional, so the common case is the one-line substitution this
/// crate exists for:
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let router = axum::Router::new();
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
///
/// ngnet_axum::serve(listener, router).await;
/// # Ok(())
/// # }
/// ```
///
/// Awaiting works directly, but [`tokio::spawn`] takes a [`Future`] rather than an
/// [`IntoFuture`], so a server that runs on its own task needs an explicit
/// `.into_future()`. `axum::serve` has the same wrinkle for the same reason.
///
/// The server accepts an unbounded number of simultaneous connections. There is no cap and
/// no way to set one; a deployment that needs a bound must impose it in front of the
/// listener.
pub fn serve(listener: TcpListener, router: Router) -> Serve {
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
pub struct Serve {
    listener: TcpListener,
    router: Router,
    config: Config,
    observer: Option<Observer>,
    stop: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Serve {
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
    pub fn on_error(mut self, observer: impl FnMut(Error) + Send + 'static) -> Self {
        self.observer = Some(Box::new(observer));
        self
    }

    /// Stops accepting when `signal` resolves, and finishes once existing connections end.
    ///
    /// **This is quiescence, not a drain, and the difference matters.** A draining shutdown
    /// tells connected peers to stop sending — over HTTP/2 that is a `GOAWAY` — and then
    /// waits for what is already in flight. This cannot do that: `ngnet-h2` exposes no
    /// server-side `GOAWAY`, so there is no way to tell a peer anything. What happens
    /// instead is that the listener is dropped, so nothing new is accepted, and established
    /// connections continue for as long as their peers keep them open.
    ///
    /// The practical consequence is that a peer holding an idle connection open holds the
    /// server open with it, and this future will not resolve. If shutdown must complete
    /// within a bounded time, impose that bound outside — race this future against a
    /// timeout, or drop it, which ends every connection at once.
    ///
    /// See `docs/h2/pending-work.md` for what would be needed to make a real drain
    /// possible.
    pub fn with_stop_signal(mut self, signal: impl Future<Output = ()> + Send + 'static) -> Self {
        self.stop = Some(Box::pin(signal));
        self
    }
}

impl IntoFuture for Serve {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(run(self))
    }
}

impl std::fmt::Debug for Serve {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The router, the observer and the stop signal are all opaque; naming the fields
        // without pretending to show them is more use than deriving nothing.
        formatter
            .debug_struct("Serve")
            .field("listener", &self.listener)
            .field("config", &self.config)
            .field("observer", &self.observer.is_some())
            .field("stop_signal", &self.stop.is_some())
            .finish()
    }
}

/// Hands `error` to the observer, if one was installed.
///
/// Trivial, and factored out for two reasons: it is the single point where the
/// silently-dropped default lives, and it is the only way to test the accept-error
/// reporting path. `TcpListener::accept` cannot be made to fail on demand, so the accept
/// arm of that path is proven by calling this directly rather than through a socket.
fn report(observer: &mut Option<Observer>, error: Error) {
    if let Some(observe) = observer.as_mut() {
        observe(error);
    }
}

/// Whether an accept failure is about one client rather than about the listener.
///
/// A client that vanishes between the kernel queueing its connection and the loop reaching
/// it produces one of these, and the next accept will succeed. Anything else — `EMFILE`
/// being the case that matters — is a property of the process or the listener and will
/// recur immediately, so it is backed off instead.
fn is_transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

async fn run(server: Serve) {
    let Serve {
        listener,
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
    let mut peers: HashMap<Id, SocketAddr> = HashMap::new();
    let mut stop = stop.unwrap_or_else(|| Box::pin(pending()));

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    // Nagle would otherwise hold back the small writes that HTTP/2 control
                    // frames are made of, waiting for data that is not coming.
                    let _ = stream.set_nodelay(true);

                    // `serve_connection` can fail *here*, before any task exists, because
                    // creating the HTTP/2 session is fallible. Such a failure can never
                    // arrive through the JoinSet, so it is reported on the spot; it is the
                    // easy path to miss, since it looks like the infallible half of setup.
                    match serve_connection(stream, router.clone(), peer, config) {
                        Ok(connection) => {
                            let handle = connections.spawn(connection);
                            peers.insert(handle.id(), peer);
                        }
                        Err(error) => report(&mut observer, Error::connection(peer, error)),
                    }
                }
                Err(error) => {
                    let transient = is_transient(&error);
                    report(&mut observer, Error::accept(error));
                    if !transient {
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                    }
                }
            },

            // Harvested here rather than after the loop so that a connection which fails
            // while the server is still accepting is reported when it fails, instead of
            // whenever the next client happens to turn up. The guard matters: on an empty
            // set this is immediately ready with `None`, which would spin.
            Some(joined) = connections.join_next_with_id(), if !connections.is_empty() => {
                harvest(&mut observer, &mut peers, joined);
            }

            () = &mut stop => break,
        }
    }

    // Dropping the listener is the whole of "stop accepting": the port is released and
    // nothing further is queued. Established connections are untouched, because there is
    // no way to tell their peers anything — see `with_stop_signal`.
    drop(listener);

    while let Some(joined) = connections.join_next_with_id().await {
        harvest(&mut observer, &mut peers, joined);
    }
}

/// Reports one finished connection, and forgets its peer.
///
/// The two arms take the task id from different places, which is easy to get wrong: a
/// successful join returns it alongside the output, while a panicked one carries it on the
/// [`JoinError`](tokio::task::JoinError). Taking it from the wrong place leaks a map entry
/// on exactly the path that most needs the address.
fn harvest(
    observer: &mut Option<Observer>,
    peers: &mut HashMap<Id, SocketAddr>,
    joined: Result<(Id, ngnet_h2::http::Result<()>), tokio::task::JoinError>,
) {
    match joined {
        Ok((id, outcome)) => {
            let peer = peers.remove(&id);
            if let (Err(error), Some(peer)) = (outcome, peer) {
                report(observer, Error::connection(peer, error));
            }
        }
        Err(join_error) => {
            let peer = peers.remove(&join_error.id());
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
    use std::sync::{Arc, Mutex};

    use crate::error::ErrorKind;

    /// Collects everything reported, so a test can assert on it.
    fn collector() -> (Option<Observer>, Arc<Mutex<Vec<Error>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let observer: Option<Observer> = Some(Box::new(move |error| {
            sink.lock().expect("a lock").push(error)
        }));
        (observer, seen)
    }

    /// SC-018's accept-level half. `TcpListener::accept` cannot be made to fail on demand
    /// in a test, so the reporting path is driven directly with the error the kernel would
    /// have produced. The connection-level half is proven over a real socket instead.
    #[test]
    fn an_accept_failure_is_reported_without_a_peer() {
        let (mut observer, seen) = collector();

        report(
            &mut observer,
            Error::accept(io::Error::from(io::ErrorKind::OutOfMemory)),
        );

        let seen = seen.lock().expect("a lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].kind(), ErrorKind::Accept);
        assert!(
            seen[0].peer().is_none(),
            "an accept failure has no peer to name"
        );
    }

    /// The other variant, and the reason `kind` exists: a caller tells "my listener is
    /// dead" from "one client had a bad time" without matching on a message.
    #[test]
    fn a_connection_failure_names_its_peer() {
        let (mut observer, seen) = collector();
        let peer: SocketAddr = "127.0.0.1:5555".parse().expect("an address");

        report(
            &mut observer,
            Error::connection(peer, io::Error::from(io::ErrorKind::ConnectionReset)),
        );

        let seen = seen.lock().expect("a lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].kind(), ErrorKind::Connection);
        assert_eq!(seen[0].peer().map(|peer| peer.0), Some(peer));
    }

    /// The documented default. Silence is a choice, so it is pinned like any other
    /// behaviour rather than left to be discovered.
    #[test]
    fn without_an_observer_a_failure_is_dropped_rather_than_panicking() {
        let mut observer: Option<Observer> = None;

        report(
            &mut observer,
            Error::accept(io::Error::from(io::ErrorKind::OutOfMemory)),
        );
    }

    /// The classification that decides whether the loop backs off. `EMFILE` is the case
    /// that matters: it is true of the process, not of a client, so it recurs at once.
    #[test]
    fn only_per_client_accept_failures_are_treated_as_transient() {
        assert!(is_transient(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
        assert!(!is_transient(&io::Error::from(io::ErrorKind::OutOfMemory)));
    }
}
