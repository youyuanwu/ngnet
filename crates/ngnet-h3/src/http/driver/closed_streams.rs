fn stream(index: usize) -> StreamId {
    StreamId::new((index as i64) * 4).expect("a request stream id")
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

        assert!(closed.insert(stream(fill)));
        assert_eq!(closed.members.len(), closed.order.len());
        assert!(
            closed
                .order
                .iter()
                .all(|stream| closed.members.contains(stream))
        );
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
        assert!(closed.order.len() <= CLOSED_TOMBSTONES);
        assert_eq!(closed.members.len(), closed.order.len());
        assert!(
            closed
                .order
                .iter()
                .all(|stream| closed.members.contains(stream))
        );
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
        _fields: &[(Vec<u8>, Vec<u8>)],
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
    let observed = driver.events.drain();
    assert!(
        !observed.is_empty(),
        "state-machine close recorded no close observation"
    );
    dispatch(&mut driver, &mut role, observed).expect("dispatching the recorded close");
    driver
        .close_stream(stream, crate::handlers::StreamClosed::clean(), &mut role)
        .expect("duplicate close");

    assert_eq!(role.closes, 1);
    assert_eq!(driver.closed.order.len(), 1);
    assert_eq!(driver.closed.members.len(), 1);
}
