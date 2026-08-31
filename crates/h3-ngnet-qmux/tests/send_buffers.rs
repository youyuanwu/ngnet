mod common;

use std::collections::VecDeque;
use std::future::poll_fn;

use bytes::{Buf, Bytes};
use h3::proto::frame::Frame;
use h3::quic::{self, SendStream as _};
use h3_ngnet_qmux::{BidiStream, from_qmux};
use ngnet_qmux::io::testing::{TestByteStream, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection as QmuxConnection};

#[derive(Debug)]
struct MultiChunk {
    chunks: VecDeque<Bytes>,
    remaining: usize,
}

impl MultiChunk {
    fn new(chunks: &[&'static [u8]]) -> Self {
        let chunks: VecDeque<_> = chunks.iter().copied().map(Bytes::from_static).collect();
        let remaining = chunks.iter().map(Bytes::len).sum();
        Self { chunks, remaining }
    }
}

impl Buf for MultiChunk {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.chunks.front().map_or(&[], Bytes::as_ref)
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.remaining);
        self.remaining -= count;
        while count != 0 {
            let front = self.chunks.front_mut().expect("remaining chunk");
            let take = count.min(front.len());
            front.advance(take);
            count -= take;
            if front.is_empty() {
                self.chunks.pop_front();
            }
        }
    }
}

type MultiBidi = BidiStream<TestByteStream, TestClock, MultiChunk>;

#[test]
fn framed_send_walks_header_and_every_payload_chunk_exactly_once() {
    let lower = Config::new().initial_max_stream_data(5).initial_max_data(5);
    let (client_io, server_io) = stream_pair();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), lower).expect("client QMux");
    let server_lower =
        QmuxConnection::server(server_io, TestClock::new(), lower).expect("server QMux");
    let (mut client, mut client_driver) = from_qmux::<MultiChunk, _, _>(client_lower);
    let (mut server, mut server_driver) = from_qmux::<MultiChunk, _, _>(server_lower);

    let client_task = async {
        let mut stream: MultiBidi =
            poll_fn(|cx| quic::OpenStreams::poll_open_bidi(&mut client, cx))
                .await
                .expect("open bidi");
        stream
            .send_data(Frame::Data(MultiChunk::new(&[b"abc", b"defgh", b"ijkl"])))
            .expect("retain framed send");
        assert!(
            stream
                .send_data(Frame::Data(MultiChunk::new(&[b"later"])))
                .is_err(),
            "a second logical send cannot overtake retained data"
        );
        let mut raw = Bytes::from_static(b"raw");
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(matches!(
            quic::SendStreamUnframed::poll_send(&mut stream, &mut cx, &mut raw),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(raw, b"raw"[..], "rejected unframed data was not advanced");
        poll_fn(|cx| stream.poll_ready(cx))
            .await
            .expect("drain framed send");
        poll_fn(|cx| stream.poll_finish(cx))
            .await
            .expect("finish behind framed data");
    };
    let server_task = async {
        let mut stream: MultiBidi =
            poll_fn(|cx| quic::Connection::poll_accept_bidi(&mut server, cx))
                .await
                .expect("accept bidi");
        let mut received = Vec::new();
        while let Some(bytes) = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect("receive")
        {
            received.extend_from_slice(&bytes);
        }
        received
    };

    let (_, received) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(
        received,
        [vec![0, 12], b"abcdefghijkl".to_vec()].concat(),
        "the H3 DATA header precedes every payload chunk with no duplication"
    );
    assert_eq!(client.snapshot().retained_send_bytes, 0);
    assert_eq!(client.snapshot().retained_send_high_water, 14);
}

#[test]
fn unframed_send_advances_only_the_exact_accepted_prefix() {
    let lower = Config::new().initial_max_stream_data(3).initial_max_data(3);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        let mut body = MultiChunk::new(&[b"ab", b"cdef", b"ghi"]);
        let mut accepted = 0;
        while body.has_remaining() {
            let before = body.remaining();
            let count =
                poll_fn(|cx| quic::SendStreamUnframed::poll_send(&mut stream, cx, &mut body))
                    .await
                    .expect("unframed send");
            assert_eq!(body.remaining(), before - count);
            accepted += count;
        }
        common::finish(&mut stream).await;
        accepted
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        common::receive_all(&mut stream).await
    };
    let (accepted, received) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(accepted, 9);
    assert_eq!(received, b"abcdefghi");
}
