//! The accept loop has two arms, and what follows from that.
//!
//! This crate's accept loop used to have three: a stop signal, accepting, and harvesting
//! finished connections out of a `JoinSet`. `select!` drops the futures of the arms it did
//! not take, so the third arm -- which fired every time any connection ended -- rebuilt the
//! accept future constantly. An implementor of [`Listener`] could not hold anything across
//! an `accept` that did not return, and a relative `sleep` inside `accept` never elapsed,
//! because it was dropped and started again before it could.
//!
//! `axum::serve` has two arms and its non-accept arm breaks, so its accept future is dropped
//! at most once per server. This crate now matches it. The tests here are the oracle for
//! that claim: one counts entries into `accept` and demands an exact number, and the rest
//! pin the behaviours that had to be rebuilt once the harvest arm was gone -- reporting,
//! panics, and ending connections when the server future is dropped.
//!
//! # Every listener here is bounded, deliberately
//!
//! The accept loop puts no cap on simultaneous connections. A listener whose `accept` is
//! always ready is therefore an unbounded allocator: each poll spawns another connection
//! task carrying another HTTP/2 session, for as long as the test runs. One written that way
//! exhausted this machine's memory and it had to be rebooted. Every listener below yields a
//! fixed budget and is permanently pending afterwards, which is also what a real listener
//! with nothing to accept does.

use std::future::IntoFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::routing::get;
use ngnet_axum::{Error, Listener, serve};
use ngnet_h2::http::transport::TokioIo;
use tokio::io::{AsyncReadExt, DuplexStream};

/// A listener with a fixed budget of in-memory pipes, counting entries into `accept`.
///
/// `entered` is incremented at the top of `accept`, before the budget is consulted, so it
/// counts the final permanently-pending call too. A two-arm loop enters `accept` exactly
/// `budget + 1` times: once per connection, and once more for the call that never returns.
struct Counting {
    budget: usize,
    entered: Arc<AtomicUsize>,
    /// Client ends, handed back so a test can decide when the connections end.
    clients: Option<tokio::sync::mpsc::UnboundedSender<DuplexStream>>,
}

impl Listener for Counting {
    type Io = TokioIo<DuplexStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.entered.fetch_add(1, Ordering::SeqCst);

        if self.budget == 0 {
            // Permanently pending. This is the bound; without it the loop below spawns
            // connection tasks without limit.
            std::future::pending::<()>().await;
        }
        self.budget -= 1;

        let (server_side, client_side) = tokio::io::duplex(1024);
        match &self.clients {
            // Handed to the test, which decides when this connection ends.
            Some(clients) => drop(clients.send(client_side)),
            // Dropped at once, so the connection fails its handshake and ends promptly.
            None => drop(client_side),
        }

        (
            TokioIo::new(server_side),
            "127.0.0.1:5555".parse().expect("an address"),
        )
    }
}

/// Spins until `condition` holds, or gives up. Bounded, and yields rather than sleeps.
async fn until(mut condition: impl FnMut() -> bool) -> bool {
    for _ in 0..20_000 {
        if condition() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

/// Collects reported failures.
fn collector() -> (impl FnMut(Error) + Send + 'static, Arc<Mutex<Vec<String>>>) {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    (
        move |error: Error| sink.lock().expect("a lock").push(format!("{error}")),
        seen,
    )
}

/// SC-030: the accept loop enters `accept` exactly once per connection, plus once more.
///
/// This is the measurement the whole change rests on, and it is an **exact equality** on
/// purpose. `<=` would pass for a loop that rebuilt the accept future half as often as
/// before, which is not the claim; the claim is that nothing rebuilds it at all. Measured
/// against the three-arm loop this replaced, the same probe read 6 where it should read 4.
///
/// Synchronising on "all three connections finished" would not do. The counter can still
/// read 3 at that moment simply because the loop has not been polled back into the fourth
/// `accept` yet, so the test would pass or fail on scheduling rather than on loop shape.
/// It waits for entry into the fourth `accept` instead, and only then insists there is no
/// fifth.
#[tokio::test]
async fn the_accept_future_is_built_once_per_connection_and_once_more() {
    const BUDGET: usize = 3;

    let entered = Arc::new(AtomicUsize::new(0));
    let (observe, reported) = collector();

    let listener = Counting {
        budget: BUDGET,
        entered: Arc::clone(&entered),
        clients: None,
    };

    let server = tokio::spawn(
        serve(listener, Router::new())
            .on_error(observe)
            .into_future(),
    );

    // The budget is spent and the loop is parked in the accept that never returns.
    assert!(
        until(|| entered.load(Ordering::SeqCst) > BUDGET).await,
        "the loop never drained the listener's budget"
    );

    // Every connection has also *finished*, which is what used to drive the harvest arm and
    // rebuild the accept future. If a third arm survived anywhere, this is when it fires.
    assert!(
        until(|| reported.lock().expect("a lock").len() >= BUDGET).await,
        "only {} of {BUDGET} connections reported",
        reported.lock().expect("a lock").len()
    );

    // And give any such arm ample opportunity to fire before the count is read.
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        entered.load(Ordering::SeqCst),
        BUDGET + 1,
        "the accept future was rebuilt: {BUDGET} connections should mean {} entries into \
         accept, so anything more means an arm other than the stop signal is firing and \
         continuing the loop",
        BUDGET + 1
    );

    server.abort();
}

/// SC-031: a relative sleep inside `accept` elapses.
///
/// This is the property the change exists to restore, and the reason `FallibleListener` and
/// `RetryingListener` could be deleted. Under the three-arm loop an implementor had to hold
/// backoff as an absolute deadline in the listener's own state, because a `sleep` created
/// inside `accept` was dropped before it could finish. Written the obvious way -- create a
/// sleep, await it -- it now works, which is what `axum`'s own listeners have always been
/// able to assume.
///
/// The listener is bounded: it sleeps once, yields one connection, and is then permanently
/// pending. Virtual time makes bounding *more* important rather than less -- with the clock
/// auto-advancing there is no wall-clock delay to slow an unbounded listener down.
#[tokio::test(start_paused = true)]
async fn a_relative_sleep_inside_accept_now_elapses() {
    struct Sleeper {
        yielded: Arc<AtomicUsize>,
        /// Raised just before the sleep is awaited, so the test advances the clock only
        /// once there is a timer to advance past. Advancing first would prove nothing.
        armed: Arc<AtomicUsize>,
        done: bool,
    }

    impl Listener for Sleeper {
        type Io = TokioIo<DuplexStream>;
        type Addr = std::net::SocketAddr;

        async fn accept(&mut self) -> (Self::Io, Self::Addr) {
            if self.done {
                std::future::pending::<()>().await;
            }

            // The whole point: a plain relative sleep, held across an await inside `accept`,
            // with no deadline kept anywhere outside this future.
            let sleep = tokio::time::sleep(std::time::Duration::from_secs(1));
            self.armed.fetch_add(1, Ordering::SeqCst);
            sleep.await;

            self.done = true;
            self.yielded.fetch_add(1, Ordering::SeqCst);

            let (server_side, client_side) = tokio::io::duplex(1024);
            drop(client_side);

            (
                TokioIo::new(server_side),
                "127.0.0.1:5555".parse().expect("an address"),
            )
        }
    }

    let yielded = Arc::new(AtomicUsize::new(0));
    let armed = Arc::new(AtomicUsize::new(0));
    let listener = Sleeper {
        yielded: Arc::clone(&yielded),
        armed: Arc::clone(&armed),
        done: false,
    };

    let server = tokio::spawn(serve(listener, Router::new()).into_future());

    // Wait for the sleep to exist before moving the clock past it.
    assert!(
        until(|| armed.load(Ordering::SeqCst) == 1).await,
        "the listener was never polled"
    );

    // The sleep has not elapsed yet, and nothing has yet been accepted.
    assert_eq!(
        yielded.load(Ordering::SeqCst),
        0,
        "the listener yielded before its backoff had elapsed"
    );

    tokio::time::advance(std::time::Duration::from_secs(2)).await;

    assert!(
        until(|| yielded.load(Ordering::SeqCst) == 1).await,
        "the sleep never elapsed, so the accept future is still being dropped and restarted"
    );

    server.abort();
}

/// SC-034a: dropping the server future ends every live connection at once.
///
/// [`Serve::with_graceful_shutdown`](ngnet_axum::Serve::with_graceful_shutdown) documents
/// this as the way to bound a drain that has no deadline. It used to happen by accident:
/// the connections lived in a `JoinSet`, and dropping a `JoinSet` aborts its tasks. Spawning
/// them individually gives that up silently, because a dropped `JoinHandle` *detaches* --
/// the connection would go on running with nothing left to stop it, and no test would have
/// noticed. It is now done deliberately, and this is what says so.
#[tokio::test]
async fn dropping_the_server_future_ends_a_live_connection() {
    let (clients, mut incoming) = tokio::sync::mpsc::unbounded_channel();

    let listener = Counting {
        budget: 1,
        entered: Arc::new(AtomicUsize::new(0)),
        clients: Some(clients),
    };

    let server = serve(listener, Router::new()).into_future();
    let mut server = Box::pin(server);

    // Drive the server far enough to have accepted the connection.
    let mut client = {
        let accepted = tokio::select! {
            () = &mut server => unreachable!("the server should still be running"),
            client = incoming.recv() => client,
        };
        accepted.expect("a connection")
    };

    // Still open: the connection is live and nothing has ended it.
    drop(server);

    // The server half is gone, so the pipe reaches end-of-file. Read to EOF rather than
    // asserting the first read is empty: the session may have written its SETTINGS frame
    // before it was ended, and those bytes sit in the pipe's buffer waiting to be read. The
    // claim is that the stream *finishes*, not that it was silent.
    //
    // Had the task merely been detached -- which is what a dropped `JoinHandle` does -- it
    // would still be holding its end of the pipe and this would run to the deadline.
    let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut buffer = [0_u8; 256];
        loop {
            match client.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    })
    .await;

    assert!(
        drained.is_ok(),
        "the connection outlived the server future: the pipe never reached end-of-file"
    );
}

/// SC-033c: one callback that panics does not silence reporting for other connections.
///
/// Reports now happen on connection tasks and are serialised by a mutex, so a callback that
/// panics poisons that mutex. Treating poisoning as fatal would turn a single bad report
/// into a server that reports nothing ever again -- and silently, since there is nowhere
/// left to report *that* to. The lock guards a callback, not an invariant a panic could have
/// left half-updated, so it is recovered from.
///
/// Bounded: two connections, then permanently pending.
#[tokio::test]
async fn a_callback_that_panics_does_not_disable_reporting() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));

    let listener = Counting {
        budget: 2,
        entered: Arc::new(AtomicUsize::new(0)),
        clients: None,
    };

    let observer = {
        let calls = Arc::clone(&calls);
        let seen = Arc::clone(&seen);
        move |_error: Error| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("deliberate: poisoning the observer lock");
            }
            seen.fetch_add(1, Ordering::SeqCst);
        }
    };

    let server = tokio::spawn(
        serve(listener, Router::new())
            .on_error(observer)
            .into_future(),
    );

    assert!(
        until(|| seen.load(Ordering::SeqCst) >= 1).await,
        "reporting stopped after a callback panicked: the second connection's failure never \
         reached the observer"
    );

    server.abort();
}

/// SC-039: a connection that fails while the server is still accepting is reported then.
///
/// The harvest arm existed partly for this: the old loop deliberately joined finished
/// connections *inside* the accept loop so that a failure was reported when it happened
/// rather than whenever the next client turned up. Task-side reporting is strictly better on
/// that axis -- there is no polling interval at all -- and this pins it: the report lands
/// while the listener is parked, with no further connection arriving to prompt it.
#[tokio::test]
async fn a_failure_is_reported_without_waiting_for_the_next_connection() {
    let entered = Arc::new(AtomicUsize::new(0));
    let (observe, reported) = collector();

    let listener = Counting {
        budget: 1,
        entered: Arc::clone(&entered),
        clients: None,
    };

    let server = tokio::spawn(
        serve(listener, Router::new())
            .on_error(observe)
            .into_future(),
    );

    // No second connection will ever arrive -- the listener's budget is one and it is
    // permanently pending afterwards. If the report needed the loop to go round again, it
    // would never come.
    assert!(
        until(|| reported.lock().expect("a lock").len() == 1).await,
        "the failure was not reported while the server sat idle"
    );

    server.abort();
}

/// SC-033b: an aborted connection is not reported as a panic.
///
/// Cancellation and panicking are different events that used to arrive through the same
/// channel, as a `JoinError` that had to be asked which it was. They are now separated by
/// construction: a panic is caught inside the task and reported there, and a cancelled task
/// reports nothing because it never reaches its reporting code. This pins that dropping the
/// server -- the only thing that cancels a connection -- produces no report.
#[tokio::test]
async fn an_aborted_connection_reports_nothing() {
    let (clients, mut incoming) = tokio::sync::mpsc::unbounded_channel();
    let (observe, reported) = collector();

    let listener = Counting {
        budget: 1,
        entered: Arc::new(AtomicUsize::new(0)),
        clients: Some(clients),
    };

    let mut server = Box::pin(
        serve(listener, Router::new())
            .on_error(observe)
            .into_future(),
    );

    let client = {
        let accepted = tokio::select! {
            () = &mut server => unreachable!("the server should still be running"),
            client = incoming.recv() => client,
        };
        accepted.expect("a connection")
    };

    drop(server);
    drop(client);

    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }

    assert!(
        reported.lock().expect("a lock").is_empty(),
        "a cancelled connection was reported as a failure: {:?}",
        reported.lock().expect("a lock")
    );
}

/// A sanity check that the bounded listener actually serves, so the tests above are not
/// measuring a listener the server rejects out of hand.
#[tokio::test]
async fn the_bounded_listener_serves_a_real_request() {
    use http_body_util::{BodyExt, Empty};
    use hyper::client::conn::http2 as hyper_client;
    use hyper_util::rt::{TokioExecutor, TokioIo as HyperIo};

    let (clients, mut incoming) = tokio::sync::mpsc::unbounded_channel();

    let listener = Counting {
        budget: 1,
        entered: Arc::new(AtomicUsize::new(0)),
        clients: Some(clients),
    };

    let router = Router::new().route("/fine", get(|| async { "fine" }));
    let server = tokio::spawn(serve(listener, router).into_future());

    let client = incoming.recv().await.expect("a connection");
    let (mut sender, connection) =
        hyper_client::handshake(TokioExecutor::new(), HyperIo::new(client))
            .await
            .expect("a handshake");
    tokio::spawn(connection);

    let response = sender
        .send_request(
            hyper::Request::builder()
                .uri("http://in-memory/fine")
                .body(Empty::<bytes::Bytes>::new())
                .expect("a request"),
        )
        .await
        .expect("a response");

    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    assert_eq!(&body[..], b"fine");

    server.abort();
}
