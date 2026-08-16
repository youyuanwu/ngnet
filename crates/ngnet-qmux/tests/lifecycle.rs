//! Construction, destruction, and the properties that hold across them.

use std::cell::Cell;
use std::rc::Rc;

use ngnet_qmux::{Conn, ErrorKind, Handlers, Role, TransportParams};

fn params() -> TransportParams {
    TransportParams::new().with_all_limits(1 << 20, 16)
}

#[test]
fn builds_and_drops_in_both_roles() {
    for role in [Role::Client, Role::Server] {
        let conn = Conn::builder(role)
            .transport_params(params())
            .build()
            .unwrap();
        assert_eq!(conn.role(), role);
        assert_eq!(conn.is_server(), role == Role::Server);
    }
}

/// Repeated construction and drop, which is where a double free or a leaked box would show up.
#[test]
fn construct_and_drop_many_times() {
    for _ in 0..512 {
        let client = Conn::builder(Role::Client)
            .transport_params(params())
            .build()
            .unwrap();
        // Touch the connection so the allocator sees real work, not just construction.
        let _ = client.streams_bidi_left();
        drop(client);
    }
}

/// Handlers that own state are dropped exactly once along with the connection.
///
/// The connection holds boxed handlers whose closures may own resources; if `Drop` freed the
/// C connection but leaked the boxes, or ran them twice, this counter would disagree.
#[test]
fn handler_state_is_dropped_exactly_once() {
    struct Sentinel(Rc<Cell<u32>>);
    impl Drop for Sentinel {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let drops = Rc::new(Cell::new(0));
    {
        let sentinel = Sentinel(Rc::clone(&drops));
        let conn = Conn::builder(Role::Client)
            .transport_params(params())
            .handlers(Handlers::new().on_stream_open(move |_| {
                // Captures the sentinel, so it lives as long as the handler.
                let _ = &sentinel;
                Ok(())
            }))
            .build()
            .unwrap();
        assert_eq!(drops.get(), 0, "dropped while the connection was alive");
        drop(conn);
    }
    assert_eq!(drops.get(), 1, "handler state should drop exactly once");
}

/// A connection built from dwnx's untouched defaults is legal, if not useful.
#[test]
fn unmodified_defaults_construct() {
    let conn = Conn::builder(Role::Client).build().unwrap();
    // No capacity at all, because dwnx's defaults advertise none.
    assert_eq!(conn.streams_bidi_left(), 0);
    assert_eq!(conn.max_data_left(), 0);
}

/// Parameters that would trip a C assertion never reach C.
#[test]
fn parameters_that_would_abort_are_rejected_in_rust() {
    for bad in [
        TransportParams::new().with_initial_max_data(u64::MAX),
        TransportParams::new().with_initial_max_streams_bidi(u64::MAX),
        TransportParams::new().with_initial_max_stream_data_bidi_local(u64::MAX),
        TransportParams::new().with_initial_max_stream_data_uni(u64::MAX),
    ] {
        let error = Conn::builder(Role::Client)
            .transport_params(bad)
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(
            error.native().is_none(),
            "validation should happen before dwnx is called"
        );
    }
}

/// A connection may move between threads but may not be shared.
///
/// Not `Sync` because the bridge slot is written on every entry point without
/// synchronisation, which is sound only because those entry points take `&mut self`.
#[test]
fn conn_is_send_but_not_sync() {
    fn assert_send<T: Send>() {}
    assert_send::<Conn<'static>>();

    // The negative half is checked by the compile-fail case in `compile_fail.rs`.
}

/// Moving a connection does not invalidate what dwnx holds.
///
/// dwnx keeps the address of the boxed bridge slot for the life of the connection. Boxing it
/// is what makes moving the `Conn` safe; this exercises that.
#[test]
fn a_connection_survives_being_moved() {
    let conn = Conn::builder(Role::Client)
        .transport_params(params())
        .build()
        .unwrap();

    let moved = Box::new(conn);
    let mut moved = *moved;
    let mut buf = [0u8; 4096];
    let (record, _) = moved
        .write(
            &mut buf,
            ngnet_qmux::WriteRequest::control_only(),
            ngnet_qmux::Timestamp::from_nanos(0),
        )
        .unwrap();
    assert!(record.bytes().is_some(), "a moved connection should work");
}
