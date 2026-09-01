use std::future::poll_fn;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use h3::{client, server};
use h3_ngnet_qmux::{OpenStreams, from_qmux};
use http::{Request, Response, StatusCode};
use ngnet_qmux::io::testing::{Fault, FaultControl, TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{
    AsyncByteStream, Clock, Config, Connection as QmuxConnection, TokioClock, TokioStream,
};
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(30);

type MemorySender = client::SendRequest<OpenStreams<TestByteStream, TestClock>, Bytes>;

#[derive(Clone, Copy, Default)]
struct MemoryIo {
    read_cap: Option<usize>,
    write_cap: Option<usize>,
    capacity: Option<usize>,
}

fn configure(stream: &TestByteStream, io: MemoryIo) {
    stream.set_read_cap(io.read_cap);
    stream.set_write_cap(io.write_cap);
    stream.set_capacity(io.capacity);
}

async fn serve<C>(connection: C)
where
    C: h3::quic::Connection<Bytes>,
{
    let mut connection = server::builder()
        .send_grease(false)
        .max_field_section_size(16 * 1024)
        .build::<_, Bytes>(connection)
        .await
        .expect("H3 server");
    'requests: loop {
        let Some(resolver) = connection.accept().await.ok().flatten() else {
            return;
        };
        let (request, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(_) => continue,
        };
        let action = request
            .headers()
            .get("x-qmux-test")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let mut body = BytesMut::new();
        loop {
            match stream.recv_data().await {
                Ok(Some(chunk)) => body.put(chunk),
                Ok(None) => break,
                Err(_) => continue 'requests,
            }
        }
        if action == "reset" {
            stream.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
            continue;
        }
        if stream
            .send_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .body(())
                    .expect("response"),
            )
            .await
            .is_err()
        {
            continue;
        }
        if !body.is_empty() && stream.send_data(body.freeze()).await.is_err() {
            continue;
        }
        let _ = stream.finish().await;
        if action == "close" {
            let _ = connection.shutdown(0).await;
        }
    }
}

fn adapt<S, C>(lower: QmuxConnection<S, C>) -> h3_ngnet_qmux::Connection<S, C>
where
    S: AsyncByteStream + 'static,
    C: Clock + 'static,
{
    let (connection, driver) = from_qmux(lower, 256);
    tokio::task::spawn_local(async move {
        let _ = driver.await;
    });
    connection
}

async fn client<S, C>(lower: QmuxConnection<S, C>) -> client::SendRequest<OpenStreams<S, C>, Bytes>
where
    S: AsyncByteStream + 'static,
    C: Clock + 'static,
{
    let (mut driver, sender) = client::builder()
        .send_grease(false)
        .max_field_section_size(16 * 1024)
        .build(adapt(lower))
        .await
        .expect("H3 client");
    tokio::task::spawn_local(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    sender
}

async fn memory_pair(
    transport: Config,
    io: MemoryIo,
) -> (MemorySender, FaultControl, FaultControl) {
    let (client_io, server_io) = stream_pair();
    configure(&client_io, io);
    configure(&server_io, io);
    let faults = (client_io.fault_control(), server_io.fault_control());
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), transport).expect("client QMux");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), transport).expect("server QMux");
    tokio::task::spawn_local(serve(adapt(server_lower)));
    (client(client_lower).await, faults.0, faults.1)
}

async fn exchange<O>(
    sender: &client::SendRequest<O, Bytes>,
    body: Bytes,
    action: &str,
) -> Result<(Response<()>, Bytes), h3::error::StreamError>
where
    O: h3::quic::OpenStreams<Bytes> + Clone,
{
    let mut sender = sender.clone();
    let mut stream = sender
        .send_request(
            Request::builder()
                .method("POST")
                .uri("https://qmux.test/echo")
                .header("x-qmux-test", action)
                .body(())
                .expect("request"),
        )
        .await?;
    if body.has_remaining() {
        stream.send_data(body).await?;
    }
    stream.finish().await?;
    let response = stream.recv_response().await?;
    let mut received = BytesMut::new();
    while let Some(chunk) = stream.recv_data().await? {
        received.put(chunk);
    }
    Ok((response, received.freeze()))
}

#[tokio::test]
async fn fragmented_window_limited_round_trip_uses_non_send_lower_io() {
    LocalSet::new()
        .run_until(async {
            let transport = Config::new()
                .initial_max_stream_data(257)
                .initial_max_data(509)
                .read_ahead(509);
            let io = MemoryIo {
                read_cap: Some(31),
                write_cap: Some(37),
                capacity: Some(257),
            };
            let (sender, _, _) = memory_pair(transport, io).await;
            let expected = Bytes::from(
                (0..4_097)
                    .map(|index| (index % 251) as u8)
                    .collect::<Vec<_>>(),
            );
            let (response, body) =
                timeout(LIMIT, exchange(&sender, expected.clone(), "round-trip"))
                    .await
                    .expect("fragmented exchange")
                    .expect("round trip");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()["content-type"],
                "application/octet-stream"
            );
            assert_eq!(body, expected);
        })
        .await;
}

#[tokio::test]
async fn reset_close_and_lower_failure_have_stable_outcomes() {
    LocalSet::new()
        .run_until(async {
            let (sender, _, _) = memory_pair(Config::new(), MemoryIo::default()).await;
            let reset = exchange(&sender, Bytes::new(), "reset")
                .await
                .expect_err("peer reset");
            assert!(matches!(
                reset,
                h3::error::StreamError::RemoteTerminate { code }
                    if code == h3::error::Code::H3_REQUEST_CANCELLED
            ));
            let (_, sibling) = exchange(&sender, Bytes::from_static(b"sibling"), "round-trip")
                .await
                .expect("sibling");
            assert_eq!(sibling, b"sibling"[..]);

            let (closing, _, _) = memory_pair(Config::new(), MemoryIo::default()).await;
            let _ = exchange(&closing, Bytes::new(), "close")
                .await
                .expect("final response");
            let after_close = exchange(&closing, Bytes::new(), "round-trip").await;
            assert!(matches!(
                after_close,
                Err(h3::error::StreamError::RemoteClosing)
            ));

            let (failing, fault, _) = memory_pair(Config::new(), MemoryIo::default()).await;
            let _ = exchange(&failing, Bytes::new(), "round-trip")
                .await
                .expect("warm exchange");
            fault.inject(Fault::Broken);
            let mut trigger = failing.clone();
            if let Ok(stream) = trigger
                .send_request(
                    Request::builder()
                        .uri("https://qmux.test/failure-trigger")
                        .body(())
                        .expect("request"),
                )
                .await
            {
                drop(stream);
            }
            tokio::task::yield_now().await;
            let first = exchange(&failing, Bytes::new(), "round-trip")
                .await
                .expect_err("first terminal");
            let second = exchange(&failing, Bytes::new(), "round-trip")
                .await
                .expect_err("stable terminal");
            assert_eq!(
                std::mem::discriminant(&first),
                std::mem::discriminant(&second)
            );
        })
        .await;
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let connecting = tokio::spawn(async move { TcpStream::connect(address).await });
    let (server, _) = listener.accept().await.expect("accept loopback");
    let client = connecting.await.expect("connect task").expect("connect");
    client.set_nodelay(true).expect("client TCP_NODELAY");
    server.set_nodelay(true).expect("server TCP_NODELAY");
    (client, server)
}

#[tokio::test]
async fn tokio_socket_smoke_round_trip_is_exact() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io) = tcp_pair().await;
            let client_lower = QmuxConnection::client(
                TokioStream::new(client_io),
                TokioClock::new(),
                Config::new(),
            )
            .expect("client QMux");
            let server_lower = QmuxConnection::server(
                TokioStream::new(server_io),
                TokioClock::new(),
                Config::new(),
            )
            .expect("server QMux");
            tokio::task::spawn_local(serve(adapt(server_lower)));
            let sender = client(client_lower).await;
            let expected = Bytes::from_static(b"loopback socket");
            let (_, body) = timeout(LIMIT, exchange(&sender, expected.clone(), "round-trip"))
                .await
                .expect("socket timeout")
                .expect("socket exchange");
            assert_eq!(body, expected);
        })
        .await;
}
