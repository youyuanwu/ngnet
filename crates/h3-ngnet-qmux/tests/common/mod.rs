#![allow(dead_code)]

use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::{Buf, Bytes};
use h3::quic;
use h3_ngnet_qmux::{AdapterConfig, Connection, Driver, from_qmux_with_config};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection as QmuxConnection};

pub type TestConnection = Connection<TestByteStream, TestClock, Bytes>;
pub type TestDriver = Driver<TestByteStream, TestClock, Bytes>;

const MAX_PASSES: usize = 1_000_000;

#[derive(Default)]
struct Flag {
    woken: AtomicBool,
}

impl Flag {
    fn take(&self) -> bool {
        self.woken.swap(false, Ordering::SeqCst)
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

pub fn pair(lower: Config) -> (TestConnection, TestDriver, TestConnection, TestDriver) {
    pair_with(lower, AdapterConfig::new(), AdapterConfig::new())
}

pub fn pair_with(
    lower: Config,
    client_adapter: AdapterConfig,
    server_adapter: AdapterConfig,
) -> (TestConnection, TestDriver, TestConnection, TestDriver) {
    let (client_io, server_io) = stream_pair();
    let client = QmuxConnection::client(client_io, TestClock::new(), lower).expect("client QMux");
    let server = QmuxConnection::server(server_io, TestClock::new(), lower).expect("server QMux");
    let (client, client_driver) = from_qmux_with_config::<Bytes, _, _>(client, client_adapter);
    let (server, server_driver) = from_qmux_with_config::<Bytes, _, _>(server, server_adapter);
    (client, client_driver, server, server_driver)
}

pub fn run_pair<A, B>(
    client: A,
    client_driver: &mut (impl Future<Output = Result<(), h3_ngnet_qmux::Error>> + Unpin),
    server: B,
    server_driver: &mut (impl Future<Output = Result<(), h3_ngnet_qmux::Error>> + Unpin),
) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let flag = Arc::new(Flag::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    let mut client = Box::pin(client);
    let mut server = Box::pin(server);
    let mut client_driver = std::pin::Pin::new(client_driver);
    let mut server_driver = std::pin::Pin::new(server_driver);
    let mut client_out = None;
    let mut server_out = None;

    for _ in 0..MAX_PASSES {
        if client_out.is_none()
            && let Poll::Ready(output) = client.as_mut().poll(&mut cx)
        {
            client_out = Some(output);
        }
        if server_out.is_none()
            && let Poll::Ready(output) = server.as_mut().poll(&mut cx)
        {
            server_out = Some(output);
        }
        if let Poll::Ready(Err(error)) = client_driver.as_mut().poll(&mut cx) {
            panic!("client adapter driver failed: {error}");
        }
        if let Poll::Ready(Err(error)) = server_driver.as_mut().poll(&mut cx) {
            panic!("server adapter driver failed: {error}");
        }
        match (client_out.take(), server_out.take()) {
            (Some(client), Some(server)) => return (client, server),
            (client_pending, server_pending) => {
                client_out = client_pending;
                server_out = server_pending;
            }
        }
        assert!(
            flag.take(),
            "adapter pair stalled with neither completion nor a registered wake"
        );
    }
    panic!("adapter pair exceeded the deterministic pass bound")
}

pub async fn open_bidi(
    connection: &mut TestConnection,
) -> h3_ngnet_qmux::BidiStream<TestByteStream, TestClock, Bytes> {
    poll_fn(|cx| quic::OpenStreams::poll_open_bidi(connection, cx))
        .await
        .expect("open bidi")
}

pub async fn open_uni(
    connection: &mut TestConnection,
) -> h3_ngnet_qmux::SendStream<TestByteStream, TestClock, Bytes> {
    poll_fn(|cx| quic::OpenStreams::poll_open_send(connection, cx))
        .await
        .expect("open uni")
}

pub async fn accept_bidi(
    connection: &mut TestConnection,
) -> h3_ngnet_qmux::BidiStream<TestByteStream, TestClock, Bytes> {
    poll_fn(|cx| quic::Connection::poll_accept_bidi(connection, cx))
        .await
        .expect("accept bidi")
}

pub async fn accept_uni(
    connection: &mut TestConnection,
) -> h3_ngnet_qmux::RecvStream<TestByteStream, TestClock, Bytes> {
    poll_fn(|cx| quic::Connection::poll_accept_recv(connection, cx))
        .await
        .expect("accept uni")
}

pub async fn send_all_unframed<S>(stream: &mut S, mut data: Bytes)
where
    S: quic::SendStreamUnframed<Bytes>,
{
    while data.has_remaining() {
        let accepted = poll_fn(|cx| quic::SendStreamUnframed::poll_send(stream, cx, &mut data))
            .await
            .expect("unframed send");
        assert!(accepted > 0);
    }
}

pub async fn finish<S>(stream: &mut S)
where
    S: quic::SendStream<Bytes>,
{
    poll_fn(|cx| quic::SendStream::poll_finish(stream, cx))
        .await
        .expect("finish");
}

pub async fn receive_all<R>(stream: &mut R) -> Vec<u8>
where
    R: quic::RecvStream<Buf = Bytes>,
{
    let mut body = Vec::new();
    while let Some(data) = poll_fn(|cx| quic::RecvStream::poll_data(stream, cx))
        .await
        .expect("receive")
    {
        body.extend_from_slice(&data);
    }
    body
}
