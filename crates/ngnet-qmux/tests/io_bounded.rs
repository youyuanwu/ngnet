#![cfg(feature = "io")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux::io::{Config, Connection, Event, StreamOpen, StreamWrite};

fn poll_once<T>(poll: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    poll(&mut cx)
}

fn established_pair() -> (
    Connection<ngnet_qmux::io::testing::TestByteStream, TestClock>,
    Connection<ngnet_qmux::io::testing::TestByteStream, TestClock>,
    ngnet_qmux::io::testing::ReadLog,
) {
    let (client_io, server_io) = stream_pair();
    server_io.set_read_cap(Some(64));
    let reads = server_io.read_log();
    let mut client =
        Connection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let mut server =
        Connection::server(server_io, TestClock::new(), Config::new()).expect("server");
    let mut client_params = false;
    let mut server_params = false;
    for _ in 0..64 {
        if let Poll::Ready(Ok(Event::PeerTransportParams(_))) =
            poll_once(|cx| client.poll_next_event_bounded(cx))
        {
            client_params = true;
        }
        if let Poll::Ready(Ok(Event::PeerTransportParams(_))) =
            poll_once(|cx| server.poll_next_event_bounded(cx))
        {
            server_params = true;
        }
        if client_params && server_params {
            while client.try_next_event().is_some() {}
            while server.try_next_event().is_some() {}
            return (client, server, reads);
        }
    }
    panic!("QMux pair did not exchange transport parameters");
}

fn queue_many_events(client: &mut Connection<ngnet_qmux::io::testing::TestByteStream, TestClock>) {
    for value in 0..24_u8 {
        let stream = match client.try_open_bidi().expect("open attempt") {
            StreamOpen::Opened(stream) => stream,
            StreamOpen::Blocked => panic!("default stream allowance was exhausted"),
        };
        assert!(matches!(
            client
                .try_write_stream(stream, &[value], true)
                .expect("stream write"),
            StreamWrite::Accepted(1)
        ));
    }
    assert!(matches!(
        poll_once(|cx| client.poll_pump(cx)),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn bounded_event_poll_drains_before_reading_and_reads_at_most_one_batch() {
    let (mut client, mut server, reads) = established_pair();
    queue_many_events(&mut client);
    let before = reads.reads();

    assert!(matches!(
        poll_once(|cx| server.poll_next_event_bounded(cx)),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(
        reads.reads(),
        before + 1,
        "exactly one lower read was issued"
    );

    assert!(matches!(
        poll_once(|cx| server.poll_next_event_bounded(cx)),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(
        reads.reads(),
        before + 1,
        "a decoded event was drained before another lower read"
    );

    while server.try_next_event().is_some() {}
    assert_eq!(
        reads.reads(),
        before + 1,
        "draining decoded events performs no lower I/O"
    );

    let _ = poll_once(|cx| server.poll_next_event_bounded(cx));
    assert_eq!(
        reads.reads(),
        before + 2,
        "the next bounded turn admitted one further read batch"
    );
}

#[test]
fn legacy_event_poll_retains_its_ready_source_draining_behavior() {
    let (mut client, mut server, reads) = established_pair();
    queue_many_events(&mut client);
    let before = reads.reads();
    assert!(matches!(
        poll_once(|cx| server.poll_next_event(cx)),
        Poll::Ready(Ok(_))
    ));
    assert!(
        reads.reads() > before + 1,
        "the existing unbounded API must continue draining an always-ready source"
    );
}

#[test]
fn immediate_open_never_polls_the_lower_stream() {
    let (mut client, _server, _reads) = established_pair();
    let pumps = client.pump_calls();
    assert!(matches!(
        client.try_open_uni().expect("immediate open"),
        StreamOpen::Opened(_)
    ));
    assert_eq!(client.pump_calls(), pumps);
}

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn bounded_peer_read_preserves_the_blocked_lower_writer_wake() {
    let (client_io, server_io) = stream_pair();
    client_io.set_capacity(Some(128));
    server_io.set_read_cap(Some(64));
    let mut client =
        Connection::client(client_io, TestClock::new(), Config::new()).expect("client");
    let mut server =
        Connection::server(server_io, TestClock::new(), Config::new()).expect("server");

    for _ in 0..64 {
        let _ = poll_once(|cx| client.poll_next_event_bounded(cx));
        let _ = poll_once(|cx| server.poll_next_event_bounded(cx));
        if client
            .try_open_bidi()
            .is_ok_and(|open| matches!(open, StreamOpen::Opened(_)))
        {
            break;
        }
    }
    for value in 0..24_u8 {
        let stream = match client.try_open_bidi().expect("open") {
            StreamOpen::Opened(stream) => stream,
            StreamOpen::Blocked => panic!("stream allowance"),
        };
        assert!(matches!(
            client.try_write_stream(stream, &[value], true),
            Ok(StreamWrite::Accepted(1))
        ));
    }
    while server.try_next_event().is_some() {}

    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(client.poll_pump(&mut cx), Poll::Pending));
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    let _ = poll_once(|cx| server.poll_next_event_bounded(cx));
    assert!(
        count.0.load(Ordering::SeqCst) > 0,
        "draining one bounded peer batch wakes the lower writer whose capacity returned"
    );
}
