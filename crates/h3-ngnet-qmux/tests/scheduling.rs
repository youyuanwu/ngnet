mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::{Buf, Bytes};
use h3::proto::frame::Frame;
use h3::quic;
use h3_ngnet_qmux::from_qmux;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection as QmuxConnection};

#[derive(Default)]
struct Count {
    wakes: AtomicUsize,
}

impl Count {
    fn get(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_context() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    (count, waker)
}

fn handles_do_no_lower_io_and_the_driver_owns_the_first_flight() {
    let (client_io, _peer_io) = stream_pair();
    let reads = client_io.read_log();
    let writes = client_io.write_log();
    let lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client QMux");
    let (mut client, mut driver) = from_qmux(lower, 4);
    let (handle_count, handle_waker) = counting_context();
    let mut handle_cx = Context::from_waker(&handle_waker);

    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut handle_cx),
        Poll::Pending
    ));
    assert_eq!(reads.reads(), 0, "an H3 handle must not poll lower input");
    assert_eq!(
        writes.writes(),
        0,
        "an H3 handle must not poll lower output"
    );
    assert_eq!(
        handle_count.get(),
        0,
        "a blocked handle must park, not spin"
    );

    let (driver_count, driver_waker) = counting_context();
    let mut driver_cx = Context::from_waker(&driver_waker);
    assert!(
        std::pin::Pin::new(&mut driver)
            .poll(&mut driver_cx)
            .is_pending()
    );
    assert_eq!(reads.reads(), 1, "the central driver owns lower input");
    assert_eq!(
        writes.writes(),
        1,
        "the central driver publishes first-flight output"
    );
    assert_eq!(
        driver_count.get(),
        0,
        "a pending lower source does not make the driver spin"
    );
}

fn a_productive_driver_read_schedules_one_poll_to_register_lower_readiness() {
    let (client_io, server_io) = stream_pair();
    let client_lower =
        QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client QMux");
    let mut server_lower =
        QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server QMux");
    let (_client, mut driver) = from_qmux(client_lower, 4);
    let noop = Waker::noop();
    let mut lower_cx = Context::from_waker(noop);
    assert!(server_lower.poll_pump(&mut lower_cx).is_ready());

    let (count, waker) = counting_context();
    let mut cx = Context::from_waker(&waker);
    assert!(std::pin::Pin::new(&mut driver).poll(&mut cx).is_pending());
    assert_eq!(
        count.get(),
        1,
        "a Ready internal-only lower batch needs one driver continuation"
    );

    assert!(std::pin::Pin::new(&mut driver).poll(&mut cx).is_pending());
    assert_eq!(
        count.get(),
        1,
        "the continuation registers lower readiness without scheduling an idle spin"
    );
}

fn cloned_openers_keep_independent_current_wakers() {
    let (client, mut client_driver, _server, mut server_driver) = common::pair(Config::new());
    let mut first = quic::Connection::<Bytes>::opener(&client);
    let mut second = first.clone();
    let (first_count, first_waker) = counting_context();
    let (second_count, second_waker) = counting_context();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);

    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut first, &mut first_cx),
        Poll::Pending
    ));
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut second, &mut second_cx),
        Poll::Pending
    ));
    assert_eq!(first_count.get(), 0);
    assert_eq!(second_count.get(), 0);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        if first_count.get() != 0 && second_count.get() != 0 {
            break;
        }
    }
    assert!(first_count.get() > 0, "first opener retained its waiter");
    assert!(second_count.get() > 0, "second opener retained its waiter");
}

fn replacing_one_serialized_opener_waker_keeps_only_the_current_task() {
    let (client, mut client_driver, _server, mut server_driver) = common::pair(Config::new());
    let mut opener = quic::Connection::<Bytes>::opener(&client);
    let (old_count, old_waker) = counting_context();
    let (new_count, new_waker) = counting_context();
    let mut old_cx = Context::from_waker(&old_waker);
    let mut new_cx = Context::from_waker(&new_waker);
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_send(&mut opener, &mut old_cx),
        Poll::Pending
    ));
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_send(&mut opener, &mut new_cx),
        Poll::Pending
    ));
    assert_eq!(
        old_count.get(),
        0,
        "serialized ownership replaces a stale waiter without a courtesy wake"
    );

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        if new_count.get() != 0 {
            break;
        }
    }
    assert!(
        new_count.get() > 0,
        "the replacement waiter observed capacity"
    );
}

fn two_busy_streams_both_make_progress_under_small_connection_credit() {
    let lower = Config::new().initial_max_stream_data(2).initial_max_data(4);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut first = common::open_bidi(&mut client).await;
        let mut second = common::open_bidi(&mut client).await;
        let mut first_body = Bytes::from_static(b"first");
        let mut second_body = Bytes::from_static(b"second");
        let mut first_finished = false;
        let mut second_finished = false;
        std::future::poll_fn(|cx| {
            if first_body.has_remaining() {
                match quic::SendStreamUnframed::poll_send(&mut first, cx, &mut first_body) {
                    Poll::Ready(Ok(_)) | Poll::Pending => {}
                    Poll::Ready(Err(error)) => panic!("first stream failed: {error}"),
                }
            } else if !first_finished {
                match quic::SendStream::poll_finish(&mut first, cx) {
                    Poll::Ready(Ok(())) => first_finished = true,
                    Poll::Pending => {}
                    Poll::Ready(Err(error)) => panic!("first finish failed: {error}"),
                }
            }
            if second_body.has_remaining() {
                match quic::SendStreamUnframed::poll_send(&mut second, cx, &mut second_body) {
                    Poll::Ready(Ok(_)) | Poll::Pending => {}
                    Poll::Ready(Err(error)) => panic!("second stream failed: {error}"),
                }
            } else if !second_finished {
                match quic::SendStream::poll_finish(&mut second, cx) {
                    Poll::Ready(Ok(())) => second_finished = true,
                    Poll::Pending => {}
                    Poll::Ready(Err(error)) => panic!("second finish failed: {error}"),
                }
            }
            if first_finished && second_finished {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    };
    let server_task = async {
        let mut first = common::accept_bidi(&mut server).await;
        let first_body = common::receive_all(&mut first).await;
        let mut second = common::accept_bidi(&mut server).await;
        let second_body = common::receive_all(&mut second).await;
        (first_body, second_body)
    };
    let (_, (first, second)) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(first, b"first");
    assert_eq!(second, b"second");
}

fn credit_blocked_writer_and_driver_do_not_wake_each_other_without_progress() {
    let lower = Config::new().initial_max_stream_data(1).initial_max_data(1);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        let mut first = Bytes::from_static(b"a");
        let accepted = std::future::poll_fn(|cx| {
            quic::SendStreamUnframed::poll_send(&mut stream, cx, &mut first)
        })
        .await
        .expect("initial byte");
        assert_eq!(accepted, 1);
        stream
    };
    let server_task = async { common::accept_bidi(&mut server).await };
    let (mut stream, _server_stream) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );

    let (writer_count, writer_waker) = counting_context();
    let (driver_count, driver_waker) = counting_context();
    let mut writer_cx = Context::from_waker(&writer_waker);
    let mut driver_cx = Context::from_waker(&driver_waker);
    let mut blocked = Bytes::from_static(b"b");
    assert!(matches!(
        quic::SendStreamUnframed::poll_send(&mut stream, &mut writer_cx, &mut blocked),
        Poll::Pending
    ));
    assert_eq!(writer_count.get(), 0);

    assert!(matches!(
        std::pin::Pin::new(&mut client_driver).poll(&mut driver_cx),
        Poll::Pending
    ));
    let driver_after_requested_turn = driver_count.get();
    for _ in 0..10 {
        assert!(matches!(
            std::pin::Pin::new(&mut client_driver).poll(&mut driver_cx),
            Poll::Pending
        ));
    }
    assert_eq!(
        writer_count.get(),
        0,
        "driver cannot wake a credit-blocked writer without lower progress"
    );
    assert_eq!(
        driver_count.get(),
        driver_after_requested_turn,
        "idle driver polls do not schedule another driver turn"
    );
}

fn local_output_drain_wakes_the_parked_framed_sender() {
    let (mut client, mut client_driver, mut server, mut server_driver) =
        common::pair(Config::new());
    let client_task = async {
        let mut stream = common::open_bidi(&mut client).await;
        let mut discovery = Bytes::from_static(b"x");
        std::future::poll_fn(|cx| {
            quic::SendStreamUnframed::poll_send(&mut stream, cx, &mut discovery)
        })
        .await
        .expect("discovery byte");
        stream
    };
    let server_task = async { common::accept_bidi(&mut server).await };
    let (mut stream, _server_stream) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );

    let (driver_count, driver_waker) = counting_context();
    let (sender_count, sender_waker) = counting_context();
    let mut driver_cx = Context::from_waker(&driver_waker);
    let mut sender_cx = Context::from_waker(&sender_waker);
    assert!(matches!(
        std::pin::Pin::new(&mut client_driver).poll(&mut driver_cx),
        Poll::Pending
    ));

    quic::SendStream::send_data(
        &mut stream,
        Frame::Data(Bytes::from(vec![0x7b; 200 * 1024])),
    )
    .expect("retain framed body");
    assert!(matches!(
        quic::SendStream::poll_ready(&mut stream, &mut sender_cx),
        Poll::Pending
    ));
    assert_eq!(sender_count.get(), 0);
    assert!(
        driver_count.get() > 0,
        "sender scheduled the central driver"
    );

    assert!(matches!(
        std::pin::Pin::new(&mut client_driver).poll(&mut driver_cx),
        Poll::Pending
    ));
    assert!(
        sender_count.get() > 0,
        "draining the local output ceiling wakes the specifically parked sender"
    );
}

#[test]
fn lower_and_driver_work_bounds() {
    handles_do_no_lower_io_and_the_driver_owns_the_first_flight();
    a_productive_driver_read_schedules_one_poll_to_register_lower_readiness();
}

#[test]
fn independent_operation_waiters() {
    cloned_openers_keep_independent_current_wakers();
    replacing_one_serialized_opener_waker_keeps_only_the_current_task();
}

#[test]
fn no_spin_output_and_credit_liveness() {
    two_busy_streams_both_make_progress_under_small_connection_credit();
    credit_blocked_writer_and_driver_do_not_wake_each_other_without_progress();
    local_output_drain_wakes_the_parked_framed_sender();
}
