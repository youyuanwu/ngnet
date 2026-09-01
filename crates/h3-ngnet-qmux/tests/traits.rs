mod common;

use std::convert::Infallible;
use std::task::{Context, Poll};

use bytes::Bytes;
use h3::quic;
use h3_ngnet_qmux::{BidiStream, Connection, Driver, OpenStreams, RecvStream, SendStream};
use ngnet_qmux::io::{AsyncByteStream, Clock, Written};

fn assert_send<T: Send>() {}

fn assert_connection_traits<S, C>()
where
    S: AsyncByteStream,
    C: Clock,
    Connection<S, C>: quic::Connection<Bytes>
        + quic::OpenStreams<Bytes, SendStream = SendStream<S, C>, BidiStream = BidiStream<S, C>>,
    OpenStreams<S, C>: Clone
        + quic::OpenStreams<Bytes, SendStream = SendStream<S, C>, BidiStream = BidiStream<S, C>>,
    SendStream<S, C>: quic::SendStream<Bytes> + quic::SendStreamUnframed<Bytes>,
    RecvStream<S, C>: quic::RecvStream<Buf = Bytes>,
    BidiStream<S, C>: quic::BidiStream<Bytes, SendStream = SendStream<S, C>, RecvStream = RecvStream<S, C>>
        + quic::SendStreamUnframed<Bytes>
        + quic::RecvStream<Buf = Bytes>,
{
}

#[derive(Default)]
struct SendStreamIo;

impl AsyncByteStream for SendStreamIo {
    type Error = Infallible;

    fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        _buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        Poll::Pending
    }

    fn poll_write(
        &mut self,
        _cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        Poll::Ready(Ok(Written::Accepted(buffer.len())))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct SendClock;

impl Clock for SendClock {
    fn now(&self) -> ngnet_qmux::Timestamp {
        ngnet_qmux::Timestamp::from_nanos(0)
    }
}

#[test]
fn every_hyperium_trait_and_associated_type_is_implemented() {
    assert_connection_traits::<SendStreamIo, SendClock>();
}

#[test]
fn sendable_lower_types_produce_sendable_handles() {
    assert_send::<Connection<SendStreamIo, SendClock>>();
    assert_send::<OpenStreams<SendStreamIo, SendClock>>();
    assert_send::<SendStream<SendStreamIo, SendClock>>();
    assert_send::<RecvStream<SendStreamIo, SendClock>>();
    assert_send::<BidiStream<SendStreamIo, SendClock>>();
    assert_send::<Driver<SendStreamIo, SendClock>>();
}

#[test]
fn rc_based_qmux_test_stream_constructs_without_send_bounds() {
    let (connection, _driver, _peer, _peer_driver) = common::pair(ngnet_qmux::io::Config::new());
    let _ = connection;
}
