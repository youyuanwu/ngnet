//! End-to-end hyperium H3 fixtures over established QMux byte streams.
//!
//! The fixtures deliberately construct QMux first, adapt it second, and run exactly one
//! adapter driver plus one hyperium connection driver per endpoint. Runtime dependencies
//! stay in this integration crate; `h3-ngnet-qmux` itself owns no executor or socket.

use std::future::poll_fn;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use h3::client;
use h3::server;
use h3_ngnet_qmux::{AdapterConfig, Observer, OpenStreams, from_qmux_with_config};
use http::{Request, Response, StatusCode};
use ngnet_qmux::io::testing::{FaultControl, TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection as QmuxConnection, TokioClock, TokioStream};
use tokio::net::TcpStream;

/// Maximum time a test may wait before reporting a hang.
pub const LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Hyperium client handle over the deliberately non-`Send` in-memory QMux stream.
pub type MemorySender = client::SendRequest<OpenStreams<TestByteStream, TestClock, Bytes>, Bytes>;

/// Read-only state observation for an in-memory endpoint.
pub type MemoryObserver = Observer<TestByteStream, TestClock, Bytes>;

/// Hyperium client handle over a Tokio TCP-backed QMux stream.
pub type TokioSender =
    client::SendRequest<OpenStreams<TokioStream<TcpStream>, TokioClock, Bytes>, Bytes>;

/// Controls applied before a `TestByteStream` moves into QMux.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryIoConfig {
    /// Maximum bytes returned by one lower read.
    pub read_cap: Option<usize>,
    /// Maximum bytes accepted by one lower write.
    pub write_cap: Option<usize>,
    /// Maximum bytes queued in one lower pipe.
    pub capacity: Option<usize>,
}

fn configure_memory(stream: &TestByteStream, config: MemoryIoConfig) {
    stream.set_read_cap(config.read_cap);
    stream.set_write_cap(config.write_cap);
    stream.set_capacity(config.capacity);
}

async fn serve<C>(connection: C)
where
    C: h3::quic::Connection<Bytes>,
{
    let mut builder = server::builder();
    builder.send_grease(false);
    builder.max_field_section_size(16 * 1024);
    let mut connection = builder
        .build::<_, Bytes>(connection)
        .await
        .expect("build upstream H3 server");

    'requests: loop {
        let resolver = match connection.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) | Err(_) => return,
        };
        let (request, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(_) => continue,
        };
        let action = request
            .headers()
            .get("x-qmux-test")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
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
        let marker = request
            .headers()
            .get("x-qmux-test")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("absent");
        if stream
            .send_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .header("x-qmux-test", marker)
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
            continue;
        }
    }
}

fn spawn_local_endpoint<S, C>(
    lower: QmuxConnection<S, C>,
) -> (
    h3_ngnet_qmux::Connection<S, C, Bytes>,
    Observer<S, C, Bytes>,
)
where
    S: ngnet_qmux::io::AsyncByteStream + 'static,
    C: ngnet_qmux::io::Clock + 'static,
{
    let (connection, driver) =
        from_qmux_with_config::<Bytes, _, _>(lower, AdapterConfig::new().pending_accept_limit(256));
    let observer = connection.observer();
    tokio::task::spawn_local(async move {
        let _ = driver.await;
    });
    (connection, observer)
}

async fn build_local_client(
    lower: QmuxConnection<TestByteStream, TestClock>,
) -> (MemorySender, MemoryObserver) {
    let (connection, observer) = spawn_local_endpoint(lower);
    let mut builder = client::builder();
    builder.send_grease(false);
    builder.max_field_section_size(16 * 1024);
    let (mut driver, sender) = builder
        .build(connection)
        .await
        .expect("build upstream H3 client");
    tokio::task::spawn_local(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    (sender, observer)
}

/// Starts an in-memory client/server pair with explicit lower and adapter configuration.
///
/// Must be called inside a Tokio [`tokio::task::LocalSet`].
pub async fn memory_pair_with(
    transport: Config,
    io: MemoryIoConfig,
) -> (MemorySender, FaultControl, FaultControl) {
    let (sender, _, _, client_fault, server_fault) = memory_pair_observed(transport, io).await;
    (sender, client_fault, server_fault)
}

/// Starts an observed in-memory pair for state-bound and failure assertions.
pub async fn memory_pair_observed(
    transport: Config,
    io: MemoryIoConfig,
) -> (
    MemorySender,
    MemoryObserver,
    MemoryObserver,
    FaultControl,
    FaultControl,
) {
    let (client_io, server_io) = stream_pair();
    configure_memory(&client_io, io);
    configure_memory(&server_io, io);
    let client_fault = client_io.fault_control();
    let server_fault = server_io.fault_control();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), transport).expect("client QMux");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), transport).expect("server QMux");

    let (server_connection, server_observer) = spawn_local_endpoint(server_lower);
    tokio::task::spawn_local(serve(server_connection));
    let (sender, client_observer) = build_local_client(client_lower).await;
    (
        sender,
        client_observer,
        server_observer,
        client_fault,
        server_fault,
    )
}

/// Starts an in-memory client/server pair using working defaults.
pub async fn memory_pair() -> MemorySender {
    memory_pair_with(Config::new(), MemoryIoConfig::default())
        .await
        .0
}

async fn tcp_stream_pair() -> (TcpStream, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let connecting = tokio::spawn(async move { TcpStream::connect(address).await });
    let (server, _) = listener.accept().await.expect("accept loopback");
    let client = connecting
        .await
        .expect("connect task")
        .expect("connect loopback");
    client.set_nodelay(true).expect("client TCP_NODELAY");
    server.set_nodelay(true).expect("server TCP_NODELAY");
    (client, server)
}

fn spawn_send_endpoint<S, C>(
    lower: QmuxConnection<S, C>,
) -> (
    h3_ngnet_qmux::Connection<S, C, Bytes>,
    Observer<S, C, Bytes>,
)
where
    S: ngnet_qmux::io::AsyncByteStream + Send + 'static,
    C: ngnet_qmux::io::Clock + Send + 'static,
{
    let (connection, driver) =
        from_qmux_with_config::<Bytes, _, _>(lower, AdapterConfig::new().pending_accept_limit(256));
    let observer = connection.observer();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    (connection, observer)
}

/// Starts a sendable client/server pair over loopback TCP.
pub async fn socket_pair() -> TokioSender {
    let (client_io, server_io) = tcp_stream_pair().await;
    let transport = Config::new();
    let client_lower =
        QmuxConnection::client(TokioStream::new(client_io), TokioClock::new(), transport)
            .expect("client QMux");
    let server_lower =
        QmuxConnection::server(TokioStream::new(server_io), TokioClock::new(), transport)
            .expect("server QMux");
    let (server_connection, _) = spawn_send_endpoint(server_lower);
    tokio::spawn(serve(server_connection));

    let (client_connection, _) = spawn_send_endpoint(client_lower);
    let mut builder = client::builder();
    builder.send_grease(false);
    builder.max_field_section_size(16 * 1024);
    let (mut driver, sender) = builder
        .build(client_connection)
        .await
        .expect("build upstream H3 client");
    tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    sender
}

/// Sends one request, finishes it, and returns the exact response head and body.
pub async fn exchange(
    sender: &MemorySender,
    body: Bytes,
) -> (Response<()>, Bytes, h3::quic::StreamId) {
    exchange_with(sender, body).await
}

/// Generic form of [`exchange`] for both fixture substrates.
pub async fn exchange_with<O>(
    sender: &client::SendRequest<O, Bytes>,
    body: Bytes,
) -> (Response<()>, Bytes, h3::quic::StreamId)
where
    O: h3::quic::OpenStreams<Bytes> + Clone,
{
    let mut sender = sender.clone();
    let mut stream = sender
        .send_request(
            Request::builder()
                .method("POST")
                .uri("https://qmux.test/echo")
                .header("x-qmux-test", "round-trip")
                .body(())
                .expect("request"),
        )
        .await
        .expect("send request");
    let id = stream.id();
    if body.has_remaining() {
        stream.send_data(body).await.expect("request data");
    }
    stream.finish().await.expect("finish request");
    let response = stream.recv_response().await.expect("response head");
    let mut received = BytesMut::new();
    while let Some(chunk) = stream.recv_data().await.expect("response data") {
        received.put(chunk);
    }
    (response, received.freeze(), id)
}
