use super::*;

fn stream(index: usize) -> StreamId {
    StreamId::new((index as i64) * 4).expect("a request stream id")
}

fn assert_synchronized(closed: &ClosedStreams) {
    let ordered: HashSet<StreamId> = closed.order.iter().copied().collect();
    assert_eq!(
        ordered.len(),
        closed.order.len(),
        "the queue has duplicates"
    );
    assert_eq!(ordered, closed.members);
    assert!(closed.order.len() <= CLOSED_TOMBSTONES);
}

#[test]
fn closed_streams_membership_is_recorded_after_insert() {
    let mut closed = ClosedStreams::new();
    let stream = stream(7);

    assert!(closed.insert(stream));
    assert!(closed.contains(stream));
    assert_eq!(closed.order.iter().copied().collect::<Vec<_>>(), [stream]);
}

#[test]
fn closed_streams_duplicate_insertions_do_not_change_order() {
    for fill in [1, CLOSED_TOMBSTONES / 2, CLOSED_TOMBSTONES] {
        let mut closed = ClosedStreams::new();
        for index in 0..fill {
            assert!(closed.insert(stream(index)));
        }
        let before = closed.order.clone();

        assert!(!closed.insert(stream(0)));
        assert!(!closed.insert(stream(fill - 1)));
        assert_eq!(closed.order, before);
        assert_eq!(closed.members.len(), fill);
        assert_synchronized(&closed);

        assert!(closed.insert(stream(fill)));
        assert_synchronized(&closed);
        if fill == CLOSED_TOMBSTONES {
            assert_eq!(closed.order.front(), Some(&stream(1)));
            assert!(!closed.members.contains(&stream(0)));
        }
    }
}

#[test]
fn closed_streams_oldest_entry_is_evicted_at_the_bound() {
    let mut closed = ClosedStreams::new();
    for index in 0..=CLOSED_TOMBSTONES {
        assert!(closed.insert(stream(index)));
    }

    assert_eq!(closed.order.len(), CLOSED_TOMBSTONES);
    assert_eq!(closed.members.len(), CLOSED_TOMBSTONES);
    assert!(!closed.contains(stream(0)), "the oldest set entry is stale");
    assert_eq!(closed.order.front(), Some(&stream(1)));
    assert!(closed.contains(stream(CLOSED_TOMBSTONES)));
}

#[test]
fn closed_streams_membership_and_order_stay_synchronized() {
    let mut closed = ClosedStreams::new();
    for index in 0..(CLOSED_TOMBSTONES * 3) {
        assert!(closed.insert(stream(index)));
        assert_synchronized(&closed);
    }

    for index in 0..(CLOSED_TOMBSTONES * 2) {
        assert!(!closed.contains(stream(index)));
    }
    for index in (CLOSED_TOMBSTONES * 2)..(CLOSED_TOMBSTONES * 3) {
        assert!(closed.contains(stream(index)));
    }
}

#[derive(Default)]
struct CountingRole {
    closes: usize,
}

impl Role for CountingRole {
    fn advance(&mut self, _conn: &mut Conn<Events>, _events: &mut Events) -> Result<()> {
        Ok(())
    }

    fn settle(&mut self, _conn: &mut Conn<Events>) -> Result<()> {
        Ok(())
    }

    fn head(
        &mut self,
        _conn: &mut Conn<Events>,
        _events: &mut Events,
        _stream: StreamId,
        _fields: &[crate::http::head::ReceivedField],
    ) -> Result<()> {
        Ok(())
    }

    fn closed(&mut self, _stream: StreamId) {
        self.closes += 1;
    }

    fn busy(&self) -> bool {
        false
    }

    fn done(&self) -> bool {
        false
    }

    fn abandon(&mut self) {}
}

#[test]
fn closed_streams_duplicate_close_notifies_the_role_only_once() {
    let (backend, _peer, _knobs) = crate::http::testing::loopback();
    let config = Config::default();
    let shared = Arc::new(Shared::new());
    let registry = Arc::new(Registry::new());
    let mut conn =
        build_conn(CoreRole::Client, &config, &shared).expect("building a test connection");
    conn.bind_control_stream(StreamId::new(2).expect("control stream"))
        .expect("binding control stream");
    conn.bind_qpack_streams(
        StreamId::new(6).expect("encoder stream"),
        StreamId::new(10).expect("decoder stream"),
    )
    .expect("binding qpack streams");
    conn.submit_request(
        stream(1),
        &[crate::Header::new(":method", "GET").expect("a request field")],
        None,
    )
    .expect("submitting a positive-control request");
    let mut positive_events = Events::default();
    conn.close_stream_with(
        stream(1),
        crate::handlers::StreamClosed::clean(),
        &mut positive_events,
    )
    .expect("closing the positive-control stream");
    assert!(
        positive_events
            .drain()
            .into_iter()
            .any(|observation| matches!(
                observation,
                Observation::Closed {
                    stream: observed,
                    ..
                } if observed == stream(1)
            )),
        "the test connection did not record its state-machine close callback"
    );
    conn.submit_request(
        stream(0),
        &[crate::Header::new(":method", "GET").expect("a request field")],
        None,
    )
    .expect("submitting a test request");
    let mut driver = Driver::new(backend, conn, shared, registry, config);
    let mut role = CountingRole::default();
    let stream = stream(0);

    driver
        .close_stream(stream, crate::handlers::StreamClosed::clean(), &mut role)
        .expect("first close");
    assert!(
        driver.events.is_empty(),
        "a driver-initiated close observation was left queued for replay"
    );
    driver
        .close_stream(stream, crate::handlers::StreamClosed::clean(), &mut role)
        .expect("duplicate close");

    assert_eq!(role.closes, 1);
    assert_eq!(driver.closed.order.len(), 1);
    assert_eq!(driver.closed.members.len(), 1);
}

#[test]
fn closed_streams_large_close_batch_does_not_replay_evicted_observations() {
    let (backend, _peer, _knobs) = crate::http::testing::loopback();
    let config = Config::default();
    let shared = Arc::new(Shared::new());
    let registry = Arc::new(Registry::new());
    let mut conn =
        build_conn(CoreRole::Client, &config, &shared).expect("building a test connection");
    conn.bind_control_stream(StreamId::new(2).expect("control stream"))
        .expect("binding control stream");
    conn.bind_qpack_streams(
        StreamId::new(6).expect("encoder stream"),
        StreamId::new(10).expect("decoder stream"),
    )
    .expect("binding qpack streams");
    let field = crate::Header::new(":method", "GET").expect("a request field");
    for index in 0..=CLOSED_TOMBSTONES {
        conn.submit_request(stream(index), core::slice::from_ref(&field), None)
            .expect("submitting a test request");
    }

    let mut driver = Driver::new(backend, conn, shared, registry, config);
    let mut role = CountingRole::default();
    for index in 0..=CLOSED_TOMBSTONES {
        driver
            .close_stream(
                stream(index),
                crate::handlers::StreamClosed::clean(),
                &mut role,
            )
            .expect("closing a test stream");
    }

    assert!(driver.events.is_empty());
    assert_eq!(role.closes, CLOSED_TOMBSTONES + 1);
    assert_synchronized(&driver.closed);
    assert!(!driver.closed.contains(stream(0)));
    assert!(driver.closed.contains(stream(CLOSED_TOMBSTONES)));
}
