//! Two endpoints, one in-memory socket pair, and no runtime at all.
//!
//! What these pin is the endpoint layer's central claim: that it takes no executor. The
//! whole "runtime" below is a loop calling `poll` with a no-op waker, and if the layer
//! needed anything more than that, none of it would work.
//!
//! The sockets come from [`ngnet_quic::endpoint::testing`], are built on `Rc`, and are
//! deliberately **not** `Send`. That is how the claim that this layer imposes no `Send`
//! bound is tested rather than asserted: if it did, this file would not compile.

use core::future::Future;
use core::net::SocketAddr;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use ngnet_quic::endpoint::testing::{TestClock, TestSocket, socket_pair};
use ngnet_quic::endpoint::{
    Config, Connection, Endpoint, EndpointBuilder, EndpointDriver, ErrorKind,
};
use ngnet_quic::{OsslBackend, OsslSession, Role};
use ngnet_quic_tests::{TEST_ALPN, TEST_SERVER_NAME, TestCredentials, TestEntropy};

/// The endpoint driver these tests run.
type Driver = EndpointDriver<TestSocket, TestClock, OsslBackend>;

fn addrs() -> (SocketAddr, SocketAddr) {
    (
        "127.0.0.1:4433".parse().expect("valid"),
        "127.0.0.1:4434".parse().expect("valid"),
    )
}

fn client(socket: TestSocket, clock: TestClock, credentials: &TestCredentials) -> (Endpoint<OsslSession>, Driver) {
    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");

    EndpointBuilder::new(socket, clock, backend)
        .config(Config::new())
        .entropy(|| TestEntropy::new(0x1234_5678))
        .build()
        .expect("a client endpoint")
}

fn server(socket: TestSocket, clock: TestClock, credentials: &TestCredentials) -> (Endpoint<OsslSession>, Driver) {
    let backend = OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");

    EndpointBuilder::new(socket, clock, backend)
        .config(Config::new())
        .entropy(|| TestEntropy::new(0x8765_4321))
        .accepts(true)
        .build()
        .expect("a server endpoint")
}

/// The entire executor: poll both drivers, poll the caller's work, let time pass.
///
/// Deliberately this small. It is the evidence for "this crate takes no executor" — there
/// is no runtime here, nothing is spawned, and no thread exists but this one.
///
/// `passes` bounds the loop, so a future that never resolves fails the test rather than
/// hanging it.
fn run<T>(
    drivers: &mut [Pin<Box<Driver>>],
    clock: &TestClock,
    mut work: impl FnMut(&mut Context<'_>) -> Poll<T>,
    passes: usize,
) -> Option<T> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for _ in 0..passes {
        for driver in drivers.iter_mut() {
            let _ = driver.as_mut().poll(&mut cx);
        }
        if let Poll::Ready(done) = work(&mut cx) {
            return Some(done);
        }
        // ngtcp2 paces its sending, so a clock that never advances yields one datagram and
        // then silence -- indistinguishable from a broken connection. A real event loop
        // lets time pass between wakeups; this does the same, deliberately.
        clock.advance(2_000_000);
    }
    None
}

/// Brings up a connected client and server, returning both connections and the drivers.
fn connected(
    credentials: &TestCredentials,
) -> (Connection, Connection, Vec<Pin<Box<Driver>>>, TestClock) {
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, server_socket) = socket_pair(client_addr, server_addr);

    let (client_handle, client_driver) = client(client_socket, clock.clone(), credentials);
    let (server_handle, server_driver) = server(server_socket, clock.clone(), credentials);

    let mut drivers = vec![Box::pin(client_driver), Box::pin(server_driver)];

    let mut connecting = Box::pin(client_handle.connect(server_addr, Some(TEST_SERVER_NAME)));
    let mut accepting = Box::pin(server_handle.accept());
    let mut client_side = None;
    let mut server_side = None;

    let done = run(
        &mut drivers,
        &clock,
        |cx| {
            if client_side.is_none()
                && let Poll::Ready(result) = connecting.as_mut().poll(cx)
            {
                client_side = Some(result.expect("the client handshake failed"));
            }
            if server_side.is_none()
                && let Poll::Ready(result) = accepting.as_mut().poll(cx)
            {
                server_side = Some(result.expect("the server accept failed"));
            }
            if client_side.is_some() && server_side.is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        },
        400,
    );

    assert!(
        done.is_some(),
        "the handshake did not finish within the bound, which means it stalled rather than \
         failed -- the usual cause is a driver that stopped rearming its timer"
    );

    (
        client_side.expect("a client connection"),
        server_side.expect("a server connection"),
        drivers,
        clock,
    )
}

#[test]
fn two_endpoints_complete_a_handshake_with_no_runtime() {
    let credentials = TestCredentials::generate();
    let (client_conn, server_conn, _drivers, _clock) = connected(&credentials);

    assert!(client_conn.is_established(), "the client is not established");
    assert!(server_conn.is_established(), "the server is not established");
    assert!(!client_conn.is_closed());
    assert!(!server_conn.is_closed());
}

#[test]
fn a_handshake_needs_several_datagrams_and_therefore_several_passes() {
    // Guards the pacing property from the other side. A handshake is not one round trip, so
    // completing it at all means the driver kept rearming its timer and kept sending -- the
    // failure this would catch is a driver that sends its first flight and then sleeps.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, server_socket) = socket_pair(client_addr, server_addr);

    let sent_before = client_socket.sent();
    let (client_handle, client_driver) = client(client_socket, clock.clone(), &credentials);
    let (server_handle, server_driver) = server(server_socket, clock.clone(), &credentials);
    let mut drivers = vec![Box::pin(client_driver), Box::pin(server_driver)];

    let mut connecting = Box::pin(client_handle.connect(server_addr, Some(TEST_SERVER_NAME)));
    let mut accepting = Box::pin(server_handle.accept());
    // The accepted connection must be *kept*. Dropping a `Connection` closes it, so letting
    // the accept result fall out of scope would tear the server down mid-handshake -- which
    // is correct behaviour, and was worth discovering here rather than in production.
    let mut server_side = None;

    let finished = run(
        &mut drivers,
        &clock,
        |cx| {
            if server_side.is_none()
                && let Poll::Ready(Ok(connection)) = accepting.as_mut().poll(cx)
            {
                server_side = Some(connection);
            }
            connecting.as_mut().poll(cx)
        },
        400,
    );

    assert!(
        finished.expect("the handshake stalled").is_ok(),
        "the handshake failed"
    );
    assert_eq!(sent_before, 0, "nothing was sent before the driver ran");
    assert!(clock.timers_armed() > 0, "the driver never armed a timer");
}

#[test]
fn nothing_happens_until_the_driver_is_polled() {
    // The driver guarantee. A connect that resolved without its driver running would mean
    // work was happening somewhere the caller did not put it.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, _server_socket) = socket_pair(client_addr, server_addr);

    let (handle, _driver) = client(client_socket, clock.clone(), &credentials);
    let mut connecting = Box::pin(handle.connect(server_addr, Some(TEST_SERVER_NAME)));

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..32 {
        assert!(
            connecting.as_mut().poll(&mut cx).is_pending(),
            "a connect resolved without its driver being polled"
        );
        clock.advance(2_000_000);
    }
}

#[test]
fn dropping_the_driver_fails_a_pending_connect_rather_than_hanging() {
    // Defined rather than undefined. Holding a handle whose driver is gone is a mistake the
    // compiler cannot catch, and the useful behaviour is failing at once instead of waiting
    // for something that will never run.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, _server_socket) = socket_pair(client_addr, server_addr);

    let (handle, driver) = client(client_socket, clock, &credentials);
    let mut connecting = Box::pin(handle.connect(server_addr, Some(TEST_SERVER_NAME)));
    drop(driver);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match connecting.as_mut().poll(&mut cx) {
        Poll::Ready(Err(err)) => assert_eq!(err.kind(), ErrorKind::DriverGone),
        Poll::Ready(Ok(_)) => panic!("a connect against a dropped driver succeeded"),
        Poll::Pending => panic!("a connect against a dropped driver stayed pending"),
    }
}

#[test]
fn connecting_after_the_driver_is_gone_fails_immediately() {
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, _server_socket) = socket_pair(client_addr, server_addr);

    let (handle, driver) = client(client_socket, clock, &credentials);
    drop(driver);

    let mut connecting = Box::pin(handle.connect(server_addr, Some(TEST_SERVER_NAME)));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match connecting.as_mut().poll(&mut cx) {
        Poll::Ready(Err(err)) => assert_eq!(err.kind(), ErrorKind::DriverGone),
        other => panic!("expected an immediate failure, got {:?}", other.is_pending()),
    }
}

#[test]
fn an_endpoint_refuses_to_be_built_without_a_source_of_randomness() {
    // Refused loudly rather than defaulted quietly. Connection identifiers and stateless
    // reset tokens come from this, and a predictable source lets an observer link or forge
    // connections -- so guessing on the caller's behalf would be a security decision made
    // in silence.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (socket, _other) = socket_pair(client_addr, server_addr);

    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");

    let err = EndpointBuilder::new(socket, clock, backend)
        .build()
        .map(|_| ())
        .expect_err("an endpoint without randomness must be refused");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn a_client_endpoint_does_not_answer_strangers() {
    // `accepts` is off by default, so a datagram from nobody in particular must produce no
    // connection and no reply. An endpoint that accepted by default would turn every client
    // into a server.
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, _server_socket) = socket_pair(client_addr, server_addr);

    client_socket.deliver(server_addr, &[0xc0; 1200]);
    let sent_before = client_socket.sent();

    let (handle, driver) = client(client_socket, clock.clone(), &credentials);
    let mut drivers = vec![Box::pin(driver)];
    let mut accepting = Box::pin(handle.accept());

    let accepted = run(&mut drivers, &clock, |cx| accepting.as_mut().poll(cx), 16);
    assert!(
        accepted.is_none(),
        "a client endpoint accepted a connection it was never asked to accept"
    );
    assert_eq!(sent_before, 0);
}

#[test]
fn the_endpoint_handle_is_cloneable_and_both_clones_reach_the_same_driver() {
    let credentials = TestCredentials::generate();
    let (client_addr, server_addr) = addrs();
    let clock = TestClock::new();
    let (client_socket, server_socket) = socket_pair(client_addr, server_addr);

    let (client_handle, client_driver) = client(client_socket, clock.clone(), &credentials);
    let (server_handle, server_driver) = server(server_socket, clock.clone(), &credentials);
    let mut drivers = vec![Box::pin(client_driver), Box::pin(server_driver)];

    // The clone is what issues the connect, so if the two did not share a driver nothing
    // would ever happen.
    let clone = client_handle.clone();
    let mut connecting = Box::pin(clone.connect(server_addr, Some(TEST_SERVER_NAME)));
    let mut accepting = Box::pin(server_handle.accept());
    let mut server_side = None;

    let result = run(
        &mut drivers,
        &clock,
        |cx| {
            if server_side.is_none()
                && let Poll::Ready(Ok(connection)) = accepting.as_mut().poll(cx)
            {
                server_side = Some(connection);
            }
            connecting.as_mut().poll(cx)
        },
        400,
    );
    assert!(
        result.expect("the handshake stalled").is_ok(),
        "a connect issued from a cloned handle did not reach the driver"
    );
}
