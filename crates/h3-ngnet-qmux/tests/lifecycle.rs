mod common;

use std::future::{Future, poll_fn};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use h3::error::Code;
use h3::proto::frame::Frame;
use h3::quic::{self, SendStream as _};
use h3_ngnet_qmux::from_qmux;
use ngnet_qmux::io::testing::{Fault, TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection as QmuxConnection};

#[test]
fn peer_stop_terminates_send_and_discards_retained_framed_data() {
    const CODE: u64 = 0x55;
    let lower = Config::new().initial_max_stream_data(3).initial_max_data(3);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        stream
            .send_data(Frame::Data(Bytes::from_static(b"retained body")))
            .expect("retain body");
        let error = poll_fn(|cx| stream.poll_ready(cx))
            .await
            .expect_err("peer stop terminates the send");
        (error, client.snapshot())
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        quic::RecvStream::stop_sending(&mut stream, CODE);
    };
    let ((error, snapshot), _) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert!(matches!(
        error,
        quic::StreamErrorIncoming::StreamTerminated { error_code: CODE }
    ));
    assert_eq!(snapshot.retained_send_bytes, 0);
}

#[test]
fn peer_reset_is_ordered_after_already_delivered_data_and_is_stable() {
    const CODE: u64 = 0x77;
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"prefix")).await;
        quic::SendStream::reset(&mut stream, CODE);
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        let first = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect("queued data")
            .expect("data before reset");
        let first_error = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect_err("reset after data");
        let second_error = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect_err("stable reset");
        (first, first_error, second_error)
    };
    let (_, (first, first_error, second_error)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(first, b"prefix"[..]);
    for error in [first_error, second_error] {
        assert!(matches!(
            error,
            quic::StreamErrorIncoming::StreamTerminated { error_code: CODE }
        ));
    }
}

#[test]
fn dropping_one_split_half_does_not_invalidate_the_other() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let stream = common::open_bidi(&mut client).await;
        let (send, mut recv) = quic::BidiStream::split(stream);
        drop(send);
        common::receive_all(&mut recv).await
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"still alive")).await;
        common::finish(&mut stream).await;
    };
    let (received, _) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(received, b"still alive");
}

#[test]
fn finish_is_idempotent_and_emits_one_fin() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::finish(&mut stream).await;
        common::finish(&mut stream).await;
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        let first = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect("fin");
        let second = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect("stable fin");
        (first, second)
    };
    let (_, (first, second)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert!(first.is_none());
    assert!(second.is_none());
}

#[test]
fn synchronous_close_preserves_first_reason_and_driver_completes_delivery() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    quic::OpenStreams::<Bytes>::close(&mut client, Code::H3_NO_ERROR, b"first");
    quic::OpenStreams::<Bytes>::close(&mut client, Code::H3_INTERNAL_ERROR, b"second");

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let immediate = quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut cx);
    assert!(matches!(
        immediate,
        Poll::Ready(Err(
            quic::StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: quic::ConnectionErrorIncoming::ApplicationClose {
                    error_code
                }
            }
        )) if error_code == Code::H3_NO_ERROR.value()
    ));
    assert!(
        matches!(
            quic::Connection::<Bytes>::poll_accept_bidi(&mut server, &mut cx),
            Poll::Pending
        ),
        "synchronous close must not claim delivery before the driver is polled"
    );

    let mut local_complete = false;
    let mut peer_code = None;
    let mut peer_driver_done = false;
    for _ in 0..128 {
        if !local_complete
            && let Poll::Ready(result) = std::pin::Pin::new(&mut client_driver).poll(&mut cx)
        {
            result.expect("local close driver");
            local_complete = true;
        }
        if !peer_driver_done
            && let Poll::Ready(result) = std::pin::Pin::new(&mut server_driver).poll(&mut cx)
        {
            assert!(result.is_err(), "peer driver reports the incoming close");
            peer_driver_done = true;
        }
        if peer_code.is_none()
            && let Poll::Ready(Err(quic::ConnectionErrorIncoming::ApplicationClose { error_code })) =
                quic::Connection::<Bytes>::poll_accept_bidi(&mut server, &mut cx)
        {
            peer_code = Some(error_code);
        }
        if local_complete && peer_code.is_some() {
            break;
        }
    }
    assert!(local_complete);
    assert_eq!(peer_code, Some(Code::H3_NO_ERROR.value()));
}

#[test]
fn lower_failure_fans_out_one_stable_connection_category_to_all_openers() {
    let (client_io, server_io) = stream_pair();
    let failure = client_io.fault_control();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let _server_lower =
        QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server");
    let (client, mut driver) = from_qmux::<Bytes, _, _>(client_lower);
    let mut first = quic::Connection::<Bytes>::opener(&client);
    let mut second = first.clone();
    failure.inject(Fault::Broken);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        std::pin::Pin::new(&mut driver).poll(&mut cx),
        Poll::Ready(Err(_))
    ));
    for result in [
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut first, &mut cx),
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut second, &mut cx),
    ] {
        assert!(matches!(
            result,
            Poll::Ready(Err(quic::StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: quic::ConnectionErrorIncoming::Undefined(_)
            }))
        ));
    }
}

#[test]
fn dropping_unread_data_after_reset_returns_connection_credit_and_reclaims_state() {
    const CODE: u64 = 0x91;
    let lower = Config::new().initial_max_stream_data(6).initial_max_data(6);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"queued")).await;
        quic::SendStream::reset(&mut stream, CODE);
    };
    let server_task = async {
        let stream = common::accept_bidi(&mut server).await;
        poll_fn(|cx| {
            let snapshot = server.snapshot();
            if snapshot.receive_bytes == 6 && snapshot.receive_terminals == 1 {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        drop(stream);
        server.snapshot()
    };
    let (_, snapshot) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(snapshot.receive_bytes, 0);
    assert_eq!(snapshot.streams, 0);
}

#[test]
fn reset_stream_does_not_prevent_an_unrelated_sibling_from_completing() {
    const CODE: u64 = 0xa1;
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut reset = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut reset, Bytes::from_static(b"x")).await;
        quic::SendStream::reset(&mut reset, CODE);

        let mut sibling = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut sibling, Bytes::from_static(b"sibling")).await;
        common::finish(&mut sibling).await;
    };
    let server_task = async {
        let mut reset = common::accept_bidi(&mut server).await;
        let first = poll_fn(|cx| quic::RecvStream::poll_data(&mut reset, cx))
            .await
            .expect("prefix")
            .expect("prefix bytes");
        let reset_error = poll_fn(|cx| quic::RecvStream::poll_data(&mut reset, cx))
            .await
            .expect_err("reset");
        let mut sibling = common::accept_bidi(&mut server).await;
        let sibling = common::receive_all(&mut sibling).await;
        (first, reset_error, sibling)
    };
    let (_, (first, reset_error, sibling)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(first, b"x"[..]);
    assert!(matches!(
        reset_error,
        quic::StreamErrorIncoming::StreamTerminated { error_code: CODE }
    ));
    assert_eq!(sibling, b"sibling");
}

#[test]
fn dropping_an_unfinished_send_is_observed_as_one_reset() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"partial")).await;
        drop(stream);
    };
    let server_task = async {
        let mut stream = common::accept_bidi(&mut server).await;
        let data = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect("data")
            .expect("partial data");
        let reset = poll_fn(|cx| quic::RecvStream::poll_data(&mut stream, cx))
            .await
            .expect_err("abandonment reset");
        (data, reset)
    };
    let (_, (data, reset)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(data, b"partial"[..]);
    assert!(matches!(
        reset,
        quic::StreamErrorIncoming::StreamTerminated { error_code: 0x10c }
    ));
}
