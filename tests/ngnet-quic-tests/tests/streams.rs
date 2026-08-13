//! Moving bytes on streams through the asynchronous API.
//!
//! The handshake tests prove two endpoints can agree keys. These prove they can then carry
//! data, which is where the properties that produce no error when broken live: flow
//! control that must be extended at two levels, an end-of-stream flag that belongs on the
//! last write rather than the first, and pacing that turns a bulk transfer into a stall if
//! the driver stops rearming its timer.
//!
//! Over real loopback UDP on tokio, because a stall in a bounded in-process loop is
//! indistinguishable from a bound that was too small.

use core::future::Future;
use core::task::Poll;
use std::time::Duration as StdDuration;

use ngnet_quic::endpoint::{
    Config, Connection, Endpoint, EndpointBuilder, EndpointDriver, ErrorKind, TokioClock,
    TokioSocket,
};
use ngnet_quic::{ApplicationErrorCode, Directionality, OsslBackend, OsslSession, Role};
use ngnet_quic_tests::{TEST_ALPN, TEST_SERVER_NAME, TestCredentials, TestEntropy};

/// The endpoint driver these tests run.
type Driver = EndpointDriver<TokioSocket, TokioClock, OsslBackend>;

/// How long a test waits before declaring a transfer stalled.
const PATIENCE: StdDuration = StdDuration::from_secs(20);

async fn client(credentials: &TestCredentials, seed: u64, config: Config) -> (Endpoint<OsslSession>, Driver) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a client socket");
    let backend = OsslBackend::builder(Role::Client)
        .alpn(TEST_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");

    EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(config)
        .entropy(move || TestEntropy::new(seed))
        .build()
        .expect("a client endpoint")
}

async fn server(
    credentials: &TestCredentials,
    config: Config,
) -> (Endpoint<OsslSession>, Driver, core::net::SocketAddr) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a server socket");
    let address = socket.inner().local_addr().expect("a bound address");
    let backend = OsslBackend::builder(Role::Server)
        .alpn(TEST_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");

    let (handle, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(config)
        .entropy(|| TestEntropy::new(0x8765_4321))
        .accepts(true)
        .build()
        .expect("a server endpoint");
    (handle, driver, address)
}

/// A connected pair, with both drivers spawned.
struct Pair {
    client: Connection,
    server: Connection,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Pair {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn connect_with(credentials: &TestCredentials, config: Config) -> Pair {
    let (server_handle, server_driver, server_addr) = server(credentials, config).await;
    let (client_handle, client_driver) = client(credentials, 0x1234_5678, config).await;

    let tasks = vec![
        tokio::spawn(async move {
            let _ = client_driver.await;
        }),
        tokio::spawn(async move {
            let _ = server_driver.await;
        }),
    ];

    let accepting = tokio::spawn(async move { server_handle.accept().await });
    let client = tokio::time::timeout(
        PATIENCE,
        client_handle.connect(server_addr, Some(TEST_SERVER_NAME)),
    )
    .await
    .expect("the handshake stalled")
    .expect("the handshake failed");

    let server = tokio::time::timeout(PATIENCE, accepting)
        .await
        .expect("the server never accepted")
        .expect("the accept task panicked")
        .expect("the accept failed");

    Pair {
        client,
        server,
        tasks,
    }
}

async fn connect(credentials: &TestCredentials) -> Pair {
    connect_with(credentials, Config::new()).await
}

/// Reads from a stream until end-of-stream, returning everything that arrived.
async fn read_to_end(
    connection: &mut Connection,
    stream: ngnet_quic::StreamId,
) -> ngnet_quic::endpoint::Result<Vec<u8>> {
    let mut collected = Vec::new();
    loop {
        let chunk = connection.read(stream).await?;
        collected.extend_from_slice(&chunk.bytes);
        if chunk.fin {
            return Ok(collected);
        }
    }
}

#[tokio::test]
async fn a_bidirectional_stream_carries_bytes_end_to_end() {
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    let payload = b"hello over quic".to_vec();
    pair.client
        .write(stream, &payload, true)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting the stream failed");

    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("the read stalled")
        .expect("the read failed");

    assert_eq!(received, payload);
}

#[tokio::test]
async fn a_payload_larger_than_one_datagram_arrives_intact_and_in_order() {
    // The segmentation path. Also the pacing path: a payload this size cannot leave in one
    // sending opportunity, so completing it at all means the driver kept rearming its timer
    // and kept sending.
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    // Distinct bytes rather than a repeated pattern, so a reordering or a duplicated chunk
    // is detectable rather than invisible.
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    pair.client
        .write(stream, &payload, true)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("the transfer stalled, which is what a driver that stops sending looks like")
        .expect("the read failed");

    assert_eq!(received.len(), payload.len(), "the transfer was truncated");
    assert_eq!(received, payload, "the bytes arrived reordered or corrupted");
}

#[tokio::test]
async fn a_payload_larger_than_the_connection_window_still_completes() {
    // Flow control, and the reason credit must be extended at BOTH levels. With a window
    // this small the connection-level allowance is exhausted several times over, so a
    // driver that extended only the stream-level credit would stall part way -- late, and
    // with no error to explain it.
    let credentials = TestCredentials::generate();
    let config = Config::new()
        .initial_max_stream_data(16 * 1024)
        .initial_max_data(24 * 1024);
    let mut pair = connect_with(&credentials, config).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    let payload: Vec<u8> = (0..120_000u32).map(|i| (i % 241) as u8).collect();
    pair.client
        .write(stream, &payload, true)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect(
            "the transfer stalled once the connection window was exhausted, which is what \
             extending only stream-level credit produces",
        )
        .expect("the read failed");

    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);
}

#[tokio::test]
async fn a_unidirectional_stream_carries_bytes_one_way() {
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_uni())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    let payload = b"one way only".to_vec();
    pair.client
        .write(stream, &payload, true)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("the read stalled")
        .expect("the read failed");
    assert_eq!(received, payload);
}

#[tokio::test]
async fn an_end_of_stream_with_no_bytes_is_still_an_end_of_stream() {
    // Legal and easy to get wrong. A reader that treated a zero-length final delivery as
    // "nothing yet" would wait forever for bytes that are not coming.
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    pair.client
        .write(stream, b"", true)
        .expect("queueing an empty final write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("a zero-length end-of-stream was never delivered")
        .expect("the read failed");
    assert!(received.is_empty());
}

#[tokio::test]
async fn a_callers_buffer_is_reusable_the_moment_a_write_returns() {
    // The retain contract, from the caller's side. ngtcp2 keeps a pointer to what it
    // accepts until the peer acknowledges it, so this crate copies -- and the whole point
    // of the copy is that the caller need not think about any of that.
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    let mut buffer = vec![7u8; 4096];
    pair.client
        .write(stream, &buffer, true)
        .expect("queueing the write");
    // Overwritten immediately. If the transport were reading through the caller's memory,
    // the peer would receive this instead.
    buffer.fill(0xff);
    drop(buffer);

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");
    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("the read stalled")
        .expect("the read failed");

    assert_eq!(received.len(), 4096);
    assert!(
        received.iter().all(|b| *b == 7),
        "the peer received the bytes the caller wrote *after* the call returned, which \
         means the transport was reading through the caller's buffer"
    );
}

#[tokio::test]
async fn resetting_a_stream_reaches_the_peer_with_its_code() {
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    // Send something first, so the peer learns the stream exists before it is reset.
    pair.client
        .write(stream, b"partial", false)
        .expect("queueing");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    // Drain what was already sent, so the reset is what the next read finds rather than
    // being masked by bytes that had already arrived.
    let first = tokio::time::timeout(PATIENCE, pair.server.read(accepted))
        .await
        .expect("the first read stalled")
        .expect("the first read failed");
    assert_eq!(first.bytes, b"partial");

    pair.client
        .reset(stream, ApplicationErrorCode::new(0x2a))
        .expect("queueing the reset");

    let outcome = tokio::time::timeout(PATIENCE, pair.server.read(accepted))
        .await
        .expect("the reset never reached the peer");

    let err = outcome.expect_err("a reset stream must not read as a clean end");
    assert_eq!(err.kind(), ErrorKind::StreamReset);
    assert_eq!(
        err.stream_code(),
        Some(ApplicationErrorCode::new(0x2a)),
        "the application code the peer chose did not survive"
    );
}

#[tokio::test]
async fn asking_the_peer_to_stop_sending_reaches_it() {
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");
    pair.client
        .write(stream, b"starting", false)
        .expect("queueing");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    pair.server
        .stop_sending(accepted, ApplicationErrorCode::new(0x99))
        .expect("queueing stop-sending");

    // The client should learn it must stop. Poll until the request has crossed.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let code = loop {
        if let Some(code) = pair.client.stop_sending_code(stream) {
            break Some(code);
        }
        if tokio::time::Instant::now() > deadline {
            break None;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    };

    assert_eq!(
        code,
        Some(ApplicationErrorCode::new(0x99)),
        "the peer's stop-sending code never reached the sender"
    );

    // And a further write must be refused rather than spending the connection's window on
    // bytes nothing will read.
    let refused = pair.client.write(stream, b"more", false);
    let err = refused.expect_err("writing to a stopped stream must be refused");
    assert_eq!(err.kind(), ErrorKind::StreamStopped);
}

#[tokio::test]
async fn two_streams_on_one_connection_do_not_block_each_other() {
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let first = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");
    let second = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    pair.client.write(first, b"first", true).expect("queueing");
    pair.client.write(second, b"second", true).expect("queueing");

    let a = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("stalled")
        .expect("accepting failed");
    let b = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the second stream never arrived")
        .expect("accepting failed");
    assert_ne!(a, b, "the same stream was accepted twice");

    let first_bytes = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, a))
        .await
        .expect("stalled")
        .expect("failed");
    let second_bytes = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, b))
        .await
        .expect("stalled")
        .expect("failed");

    let mut both = vec![first_bytes, second_bytes];
    both.sort();
    assert_eq!(both, vec![b"first".to_vec(), b"second".to_vec()]);
}

#[tokio::test]
async fn acknowledgement_releases_what_the_transport_was_holding() {
    // retained_bytes is the honest signal that memory is held on the peer's behalf. It
    // must go up when data is sent and back down once the peer acknowledges it; a crate
    // that never released would hold every byte it ever sent.
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    let payload: Vec<u8> = (0..30_000u32).map(|i| (i % 233) as u8).collect();
    pair.client
        .write(stream, &payload, true)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("stalled")
        .expect("accepting failed");
    let received = tokio::time::timeout(PATIENCE, read_to_end(&mut pair.server, accepted))
        .await
        .expect("the transfer stalled")
        .expect("the read failed");
    assert_eq!(received.len(), payload.len());

    // Both of these are asynchronous signals that arrive as acknowledgements come back, so
    // both are waited for rather than sampled once.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while tokio::time::Instant::now() < deadline {
        let released = pair.client.retained_bytes() == 0;
        let counted = pair.client.acked_bytes(stream) >= payload.len() as u64;
        if released && counted {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }

    assert_eq!(
        pair.client.retained_bytes(),
        0,
        "the transport is still holding bytes the peer has read, so acknowledgement is \
         not releasing them"
    );
    assert!(
        pair.client.acked_bytes(stream) >= payload.len() as u64,
        "only {} of {} bytes were reported acknowledged",
        pair.client.acked_bytes(stream),
        payload.len()
    );
}

#[tokio::test]
async fn a_configured_stream_limit_takes_effect() {
    // SC-023 from the caller's side: a limit set low enough must change observed
    // behaviour, or the setter is decoration.
    let credentials = TestCredentials::generate();
    let config = Config::new().max_streams_bidi(1);
    let mut pair = connect_with(&credentials, config).await;

    let first = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening the first stream stalled")
        .expect("opening the first stream failed");
    pair.client.write(first, b"one", true).expect("queueing");

    // The peer advertised room for exactly one, so a second must not be granted
    // immediately. It may become available later once the first is retired, which is why
    // this asserts on a bounded wait rather than on an error.
    let second = tokio::time::timeout(StdDuration::from_millis(500), pair.client.open_bidi()).await;
    assert!(
        second.is_err(),
        "a second stream was opened despite the peer advertising room for one"
    );
}

#[tokio::test]
async fn a_peer_cannot_outrun_a_reader_that_never_reads() {
    // The flow-control window is only a bound if credit is returned when the application
    // *consumes* bytes rather than when they arrive. Returning it on delivery makes the
    // window advisory: a peer streams indefinitely past a reader that never reads, and the
    // bytes accumulate in this process until it runs out of memory.
    //
    // So: write far more than the window, never read, wait, then read once. What comes back
    // is everything that was allowed to arrive, and it must be bounded by the window rather
    // than by what was written.
    let credentials = TestCredentials::generate();
    let window = 32 * 1024u64;
    let config = Config::new()
        .initial_max_stream_data(16 * 1024)
        .initial_max_data(window);
    let mut pair = connect_with(&credentials, config).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");

    // An order of magnitude more than the window allows.
    let payload = vec![0x5au8; 512 * 1024];
    pair.client
        .write(stream, &payload, false)
        .expect("queueing the write");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");

    // Long enough that an unbounded sender would have delivered the whole payload.
    tokio::time::sleep(StdDuration::from_secs(2)).await;

    let first = tokio::time::timeout(PATIENCE, pair.server.read(accepted))
        .await
        .expect("the read stalled")
        .expect("the read failed");

    assert!(
        !first.bytes.is_empty(),
        "nothing arrived at all, so this proves nothing about flow control"
    );

    // A little slack over the window: ngtcp2 may have a packet or two in hand beyond what
    // the window strictly permits. An order of magnitude of slack would not be a bound.
    let allowed = (window * 3) as usize;
    assert!(
        first.bytes.len() <= allowed,
        "{} bytes were buffered against a {window}-byte connection window while the \
         application had read nothing -- credit is being returned on delivery rather than \
         on consumption, so the window is bounding nothing",
        first.bytes.len()
    );
    assert!(
        first.bytes.len() < payload.len(),
        "the entire payload arrived despite the reader never reading"
    );
}

#[tokio::test]
async fn a_write_on_a_quiescent_connection_is_serviced_promptly() {
    // The lost-wakeup test, and the reason it has to be written this way.
    //
    // Queuing a command does not make the driver run. A driver with nothing to read and no
    // timer due is parked, and a command touches neither -- so unless queuing also *wakes*
    // it, the write waits for some unrelated event. On a connection that has gone quiet the
    // next such event is the idle timeout, which closes the connection rather than serving
    // the write.
    //
    // The in-process harness cannot catch this: it re-polls every driver on every pass with
    // a no-op waker, so it never depends on a wake happening at all. This runs on a real
    // runtime, where a driver that is not woken simply does not run.
    let credentials = TestCredentials::generate();
    let mut pair = connect(&credentials).await;

    let stream = tokio::time::timeout(PATIENCE, pair.client.open_bidi())
        .await
        .expect("opening stalled")
        .expect("opening failed");
    pair.client.write(stream, b"first", false).expect("queueing");

    let accepted = tokio::time::timeout(PATIENCE, pair.server.accept_stream())
        .await
        .expect("the server never saw the stream")
        .expect("accepting failed");
    let first = tokio::time::timeout(PATIENCE, pair.server.read(accepted))
        .await
        .expect("the first read stalled")
        .expect("the first read failed");
    assert_eq!(first.bytes, b"first");

    // Let everything settle: data acknowledged, nothing in flight, no timer due but the
    // idle timeout. This is the state in which a missing wake is fatal rather than merely
    // slow.
    tokio::time::sleep(StdDuration::from_millis(750)).await;

    pair.client
        .write(stream, b"second", true)
        .expect("queueing the second write");

    // A tight bound on purpose. Thirty seconds would pass even with the wake missing,
    // because the idle timer would eventually fire; two seconds only passes if queuing the
    // write actually woke the driver.
    let second = tokio::time::timeout(
        StdDuration::from_secs(2),
        read_to_end(&mut pair.server, accepted),
    )
    .await
    .expect(
        "a write on a quiescent connection was not serviced within two seconds, which means \
         queuing it did not wake the driver",
    )
    .expect("the second read failed");

    assert_eq!(second, b"second");
}

#[tokio::test]
async fn opening_two_streams_of_different_kinds_at_once_does_not_cross_them() {
    // Both opens are outstanding together, and each must resolve with a stream of the kind
    // it asked for. Resolving from a shared queue of "a stream was opened" events let these
    // swap, and a caller would then write to a unidirectional stream believing it could
    // read the reply.
    let credentials = TestCredentials::generate();
    let pair = connect(&credentials).await;

    let (bidi, uni) = tokio::time::timeout(
        PATIENCE,
        futures_join(pair.client.open_bidi(), pair.client.open_uni()),
    )
    .await
    .expect("opening stalled");

    let bidi = bidi.expect("opening a bidirectional stream failed");
    let uni = uni.expect("opening a unidirectional stream failed");

    assert_eq!(
        bidi.directionality(),
        Directionality::Bidirectional,
        "open_bidi resolved with {bidi:?}, which is not bidirectional"
    );
    assert_eq!(
        uni.directionality(),
        Directionality::Unidirectional,
        "open_uni resolved with {uni:?}, which is not unidirectional"
    );
    assert_ne!(bidi, uni);
}

/// Polls two futures to completion together, without pulling in a futures crate.
async fn futures_join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut first = None;
    let mut second = None;
    core::future::poll_fn(|cx| {
        if first.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(cx)
        {
            first = Some(value);
        }
        if second.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(cx)
        {
            second = Some(value);
        }
        if first.is_some() && second.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    (first.expect("a"), second.expect("b"))
}
