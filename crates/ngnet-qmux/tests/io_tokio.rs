//! The same exchange, over a real socket.
//!
//! # What this file is evidence for
//!
//! Everything else in this crate's asynchronous suite runs over the in-memory byte stream in
//! [`ngnet_qmux::io::testing`], which is fast, deterministic, and written by the same people
//! as the layer it exercises. A seam quietly shaped around that one implementation would pass
//! all of it. So this file runs the *same* exchange bodies -- [`client_exchange`] and
//! [`server_exchange`] from `io_harness`, not copies of them -- over a loopback TCP socket
//! wrapped in [`TokioStream`], and asserts the outcome with the same [`assert_exchange`] the
//! in-memory tests use.
//!
//! That is the whole of Spec SC-007, and the reason it is a manual reading as well as an
//! automated one: if a maintainer ever finds it easier to fork the body than to keep it
//! generic, this file stops being evidence of anything and becomes a second, similar test.
//!
//! # What a real socket adds that the in-memory pair cannot
//!
//! Three things, and each is a way a layer that works in memory fails on a wire. Reads and
//! writes are genuinely partial and genuinely asynchronous, so every record boundary falls
//! wherever the kernel put it rather than wherever the harness chose. The waker is the
//! runtime's rather than a test's, so a connection that failed to register one parks forever
//! instead of being rescued by a driver that polls in a loop. And the two connections run
//! against each other in real time rather than in lockstep, so an ordering the in-memory
//! round-robin happened to make safe is no longer guaranteed.
//!
//! # Why every test has a deadline
//!
//! The in-memory harness detects a stalled exchange precisely: a pass in which neither side
//! finished and nothing was woken cannot be followed by a different pass, so it fails with a
//! diagnosis. Nothing over a socket can say that, because a wake may still be in flight in the
//! kernel. The deadline is the substitute, and it is generous enough that only a genuine stall
//! reaches it -- a connection that parked without registering a waker, which is the failure
//! this layer's contract exists to prevent and the one that otherwise presents as a test suite
//! that hangs with no output.

// Without the runtime feature there is no `TokioStream` to test and no tokio to test it with;
// the file is inert rather than broken, exactly as the rest of the suite is without `io`.
#![cfg(feature = "tokio")]

mod io_harness;

use std::time::Duration;

use io_harness::{assert_exchange, client_exchange, server_exchange};
use ngnet_qmux::io::{Clock, Config, Connection, TokioClock, TokioStream};
use tokio::net::{TcpListener, TcpStream};

const REQUEST: &[u8] = b"the client's half of the conversation";
const RESPONSE: &[u8] = b"and the server's answer to it";

/// Long enough that only a genuine stall reaches it, short enough to fail rather than hang.
const DEADLINE: Duration = Duration::from_secs(30);

/// A connected client and server over a loopback TCP socket.
///
/// Port zero, so concurrent runs of this suite -- and anything else on the machine -- cannot
/// collide over a fixed one. The listener is dropped as soon as the pair exists: this crate
/// has no accept loop and wants none, and holding it open would leave a socket listening for
/// the rest of the test.
async fn loopback_pair(
    config: Config,
    clock: TokioClock,
) -> (
    Connection<TokioStream<TcpStream>, TokioClock>,
    Connection<TokioStream<TcpStream>, TokioClock>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding a loopback listener");
    let address = listener.local_addr().expect("the bound address");

    // Connecting before accepting is safe on loopback because the kernel completes the
    // handshake from the backlog, so this does not deadlock the way it would if both sides
    // needed the other to move first.
    let client_socket = TcpStream::connect(address)
        .await
        .expect("connecting to the listener");
    let (server_socket, _) = listener.accept().await.expect("accepting the connection");

    // Not a performance tweak. QMux's first flight is a small transport-parameters record and
    // each side waits for the other's before it can open a stream; with Nagle's algorithm
    // holding a small write until the previous one is acknowledged, and delayed acknowledgement
    // holding that in turn, a test that measures correctness would instead measure a 40ms
    // interaction between two TCP heuristics.
    client_socket.set_nodelay(true).expect("disabling Nagle");
    server_socket.set_nodelay(true).expect("disabling Nagle");

    // Both sides take a copy of one clock rather than a fresh one each. Copies share an
    // origin, so the timestamps the two connections hand their state machines are readings on
    // one timescale -- which is what lets a test outside them compare its own reading against
    // theirs, and what a caller running many connections wants for the same reason.
    let client = Connection::client(TokioStream::new(client_socket), clock, config)
        .expect("a client over the socket");
    let server = Connection::server(TokioStream::new(server_socket), clock, config)
        .expect("a server over the socket");
    (client, server)
}

/// Runs the shared exchange bodies against each other on one task, under a deadline.
///
/// Joined rather than spawned, and that is deliberate: `join!` needs neither `Send` nor
/// `'static` of the connections, so this test also demonstrates that the layer's refusal to
/// impose a `Send` bound survives contact with a real runtime. Both futures are polled on
/// every wakeup, which is what a connection that must drain reads and produce writes in one
/// pass requires.
async fn run_exchange(
    config: Config,
    clock: TokioClock,
    request: &[u8],
    response: &[u8],
) -> (
    Connection<TokioStream<TcpStream>, TokioClock>,
    Connection<TokioStream<TcpStream>, TokioClock>,
) {
    let (mut client, mut server) = loopback_pair(config, clock).await;

    let (received_response, received_request) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(
            client_exchange(&mut client, request),
            server_exchange(&mut server, response),
        )
    })
    .await
    .expect(
        "the exchange did not finish within the deadline; over a socket that means one side \
         parked without registering a waker, or is waiting for bytes the other never wrote",
    );

    assert_exchange(&received_request, request, &received_response, response);
    (client, server)
}

/// The canonical exchange, over a socket instead of a buffer.
///
/// Deliberately the same body and the same assertions as
/// `io_transfer::a_client_and_a_server_complete_a_bidirectional_transfer`. If this passes and
/// that one fails, or the reverse, the difference is the byte stream underneath and nothing
/// else -- which is the only way this test tells anyone anything.
#[tokio::test]
async fn a_transfer_completes_over_a_loopback_socket() {
    run_exchange(Config::new(), TokioClock::new(), REQUEST, RESPONSE).await;
}

/// A payload far larger than one record, and far larger than a socket buffer.
///
/// The case the small exchange cannot reach: the kernel's send buffer fills, writes come back
/// partial and then refuse entirely, and the layer has to resume each record at the offset the
/// socket stopped at while the peer's flow-control window is extended underneath it. A layer
/// that treated a partial accept as a whole one truncates a record here, and a truncated record
/// desynchronises the stream permanently.
///
/// Neither payload is a repeated byte: a duplicated or reordered chunk inside a run of one
/// value is invisible, so each byte is a function of its offset and the assertion catches order
/// rather than merely length.
#[tokio::test]
async fn a_payload_far_larger_than_one_record_survives_a_loopback_socket() {
    let request: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let response: Vec<u8> = (0..150_000u32).map(|i| (i % 241) as u8).collect();

    let config = Config::new()
        .initial_max_stream_data(1 << 20)
        .initial_max_data(1 << 21);

    run_exchange(config, TokioClock::new(), &request, &response).await;
}

/// The timestamps a running connection recorded came from the clock the test can read.
///
/// A transfer succeeds whatever the clock says: dwnx records the readings and, having no timer
/// and no expiry, never acts on them, so a clock with the wrong origin or the wrong scaling
/// would pass every other test in this file while making
/// [`Connection::timestamp`](ngnet_qmux::io::Connection::timestamp) meaningless to the caller
/// it is reported to. The check is therefore an ordering across the exchange: the connection's
/// last recorded timestamp must lie between a reading taken before it started and one taken
/// after it finished, on the same clock. A per-call origin, or nanoseconds confused with
/// milliseconds, breaks that.
#[tokio::test]
async fn a_connections_timestamps_are_readings_of_the_clock_it_was_given() {
    let clock = TokioClock::new();
    let before = clock.now();

    let (client, _server) = run_exchange(Config::new(), clock, REQUEST, RESPONSE).await;

    let after = clock.now();
    let recorded = client.timestamp();
    assert!(
        recorded >= before && recorded <= after,
        "the connection recorded {recorded:?}, which is outside the window {before:?}..={after:?} \
         the exchange ran in; the layer is passing the state machine readings from some other \
         timescale"
    );
    assert!(
        after > before,
        "the clock did not move across a whole exchange over a socket, which no monotonic \
         nanosecond clock can honestly report"
    );
}
