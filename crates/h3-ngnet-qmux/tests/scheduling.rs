mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::{Buf, Bytes};
use h3::quic;
use ngnet_qmux::io::Config;

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

#[test]
fn blocked_open_and_idle_driver_do_not_self_wake() {
    let (mut client, mut client_driver, _server, _server_driver) = common::pair(Config::new());
    let (open_count, open_waker) = counting_context();
    let mut open_cx = Context::from_waker(&open_waker);
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, &mut open_cx),
        Poll::Pending
    ));
    assert_eq!(open_count.get(), 0, "a blocked open must park, not spin");

    let (driver_count, driver_waker) = counting_context();
    let mut driver_cx = Context::from_waker(&driver_waker);
    assert!(matches!(
        std::pin::Pin::new(&mut client_driver).poll(&mut driver_cx),
        Poll::Pending
    ));
    assert_eq!(
        driver_count.get(),
        0,
        "an idle lower poll must not wake its own driver"
    );
}

#[test]
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

#[test]
fn two_cloned_unidirectional_openers_keep_independent_wakers() {
    let (client, mut client_driver, _server, mut server_driver) = common::pair(Config::new());
    let mut first = quic::Connection::<Bytes>::opener(&client);
    let mut second = first.clone();
    let (first_count, first_waker) = counting_context();
    let (second_count, second_waker) = counting_context();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_send(&mut first, &mut first_cx),
        Poll::Pending
    ));
    assert!(matches!(
        quic::OpenStreams::<Bytes>::poll_open_send(&mut second, &mut second_cx),
        Poll::Pending
    ));

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..16 {
        let _ = std::pin::Pin::new(&mut server_driver).poll(&mut cx);
        let _ = std::pin::Pin::new(&mut client_driver).poll(&mut cx);
        if first_count.get() != 0 && second_count.get() != 0 {
            break;
        }
    }
    assert!(first_count.get() > 0);
    assert!(second_count.get() > 0);
}

#[test]
fn replacing_one_opener_waker_wakes_the_displaced_task_without_losing_the_new_one() {
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
    assert_eq!(old_count.get(), 1, "the stale waiter was not silently lost");

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

#[test]
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

#[test]
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

#[test]
fn two_credit_blocked_writers_retain_independent_waiters() {
    let lower = Config::new().initial_max_stream_data(1).initial_max_data(2);
    let (mut client, mut client_driver, mut server, mut server_driver) = common::pair(lower);
    let client_task = async {
        let mut first = common::open_bidi(&mut client).await;
        let mut first_byte = Bytes::from_static(b"a");
        std::future::poll_fn(|cx| {
            quic::SendStreamUnframed::poll_send(&mut first, cx, &mut first_byte)
        })
        .await
        .expect("first initial byte");

        let mut second = common::open_bidi(&mut client).await;
        let mut second_byte = Bytes::from_static(b"b");
        std::future::poll_fn(|cx| {
            quic::SendStreamUnframed::poll_send(&mut second, cx, &mut second_byte)
        })
        .await
        .expect("second initial byte");
        (first, second)
    };
    let server_task = async {
        let first = common::accept_bidi(&mut server).await;
        let second = common::accept_bidi(&mut server).await;
        (first, second)
    };
    let ((mut first, mut second), _server_streams) = common::run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    let (first_count, first_waker) = counting_context();
    let (second_count, second_waker) = counting_context();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    let mut first_blocked = Bytes::from_static(b"c");
    let mut second_blocked = Bytes::from_static(b"d");
    assert!(matches!(
        quic::SendStreamUnframed::poll_send(&mut first, &mut first_cx, &mut first_blocked),
        Poll::Pending
    ));
    assert!(matches!(
        quic::SendStreamUnframed::poll_send(&mut second, &mut second_cx, &mut second_blocked),
        Poll::Pending
    ));
    assert_eq!(first_count.get(), 0);
    assert_eq!(second_count.get(), 0);
}
