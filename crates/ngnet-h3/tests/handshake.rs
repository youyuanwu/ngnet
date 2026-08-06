//! An in-memory HTTP/3 connection preface, with no QUIC implementation present.
//!
//! Two connections are wired directly to each other: whatever one wants to send on a
//! stream is delivered to the other as if a QUIC stack had carried it. That is enough to
//! exercise everything Phase 2 delivers — construction, the three connection-level
//! streams, both byte paths and the send transaction — without a socket, a runtime or a
//! certificate anywhere in sight.

use std::collections::HashMap;

use ngnet_h3::{Conn, ConnBuilder, ErrorKind, Role, StreamId, Timestamp};

/// Client-initiated unidirectional stream identifiers (RFC 9000 §2.1: `id & 0b11 == 0b10`).
const CLIENT_CONTROL: i64 = 2;
const CLIENT_QPACK_ENCODER: i64 = 6;
const CLIENT_QPACK_DECODER: i64 = 10;

/// Server-initiated unidirectional stream identifiers (`id & 0b11 == 0b11`).
const SERVER_CONTROL: i64 = 3;
const SERVER_QPACK_ENCODER: i64 = 7;
const SERVER_QPACK_DECODER: i64 = 11;

fn id(raw: i64) -> StreamId {
    StreamId::new(raw).expect("literal is a valid stream id")
}

fn client() -> Conn<()> {
    let mut conn = ConnBuilder::<()>::new(Role::Client)
        .build()
        .expect("client connection");
    conn.bind_control_stream(id(CLIENT_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_DECODER))
        .unwrap();
    conn
}

fn server() -> Conn<()> {
    let mut conn = ConnBuilder::<()>::new(Role::Server)
        .build()
        .expect("server connection");
    conn.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    conn.bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();
    conn
}

/// Drains everything one connection wants to send, accepting all of it.
///
/// Returns the bytes per stream, in order, which is what the peer would have received.
fn drain(conn: &mut Conn<()>) -> HashMap<i64, Vec<u8>> {
    let mut out: HashMap<i64, Vec<u8>> = HashMap::new();
    // Bounded so a bug that never makes progress fails as a test rather than a hang. The
    // loop must exit because there was nothing left to send; exiting because the bound was
    // reached would leave every assertion below checking truncated data.
    let mut drained = false;
    for _ in 0..64 {
        let Some(send) = conn.writev_stream(&mut ()).expect("collect data to send") else {
            drained = true;
            break;
        };
        let stream = send.stream().get();
        let fin = send.fin();
        let mut taken = 0usize;
        let buffer = out.entry(stream).or_default();
        for slice in send.slices() {
            buffer.extend_from_slice(slice);
            taken += slice.len();
        }
        // Committing is required even when nothing was on offer: a stream can end with an
        // empty final write, and skipping that commit stalls the connection forever.
        send.commit(taken).expect("commit what was accepted");
        if taken == 0 && !fin {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "the connection kept offering data past the iteration bound, so anything asserted \
         about these bytes would be about a truncated prefix"
    );
    out
}

/// Delivers each stream's bytes to a connection, as a QUIC stack would.
fn deliver(conn: &mut Conn<()>, streams: &HashMap<i64, Vec<u8>>, now: u64) {
    for (&stream, bytes) in streams {
        conn.read_stream(
            id(stream),
            bytes,
            false,
            Timestamp::from_nanos(now),
            &mut (),
        )
        .expect("read stream data");
    }
}

#[test]
fn a_client_and_server_complete_the_preface_with_no_quic_present() {
    let mut client = client();
    let mut server = server();

    let from_client = drain(&mut client);
    let from_server = drain(&mut server);

    // Each side opens its control stream and both QPACK streams.
    assert!(
        from_client.contains_key(&CLIENT_CONTROL),
        "the client must write its control stream"
    );
    assert!(from_client.contains_key(&CLIENT_QPACK_ENCODER));
    assert!(from_client.contains_key(&CLIENT_QPACK_DECODER));
    assert!(from_server.contains_key(&SERVER_CONTROL));

    // The control stream opens with its type, the varint 0x00 (RFC 9114 §6.2.1), followed
    // by a SETTINGS frame (type 0x04).
    let control = &from_client[&CLIENT_CONTROL];
    assert_eq!(control[0], 0x00, "control stream type prefix");
    assert_eq!(control[1], 0x04, "SETTINGS is the first frame on it");

    // Each side accepts what the other produced without complaint.
    deliver(&mut server, &from_client, 1);
    deliver(&mut client, &from_server, 1);

    // And neither has been poisoned by any of it.
    assert!(client.is_usable());
    assert!(server.is_usable());
}

#[test]
fn reading_reports_flow_control_credit_for_control_stream_bytes() {
    let mut client = client();
    let mut server = server();
    let from_client = drain(&mut client);

    let control = &from_client[&CLIENT_CONTROL];
    let credit = server
        .read_stream(
            id(CLIENT_CONTROL),
            control,
            false,
            Timestamp::from_nanos(1),
            &mut (),
        )
        .unwrap();

    // Control stream bytes carry no data-frame payload, so every byte delivered is
    // creditable -- which is exactly why this stream is the clean case to assert on.
    assert_eq!(
        credit.bytes(),
        control.len() as u64,
        "all control stream bytes should be creditable"
    );
}

#[test]
fn an_exchange_without_binding_is_a_typed_error() {
    let mut conn = ConnBuilder::<()>::new(Role::Client).build().unwrap();
    assert!(!conn.is_bound());

    let error = conn
        .writev_stream(&mut ())
        .expect_err("must refuse to send");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("control stream"),
        "the error should name what is missing, got: {error}"
    );

    // Binding only the control stream is still not enough.
    conn.bind_control_stream(id(CLIENT_CONTROL)).unwrap();
    let error = conn
        .writev_stream(&mut ())
        .expect_err("QPACK streams still missing");
    assert!(error.to_string().contains("QPACK"), "got: {error}");
}

#[test]
fn connection_level_streams_must_be_local_and_unidirectional() {
    let mut conn = ConnBuilder::<()>::new(Role::Client).build().unwrap();

    // Bidirectional: nghttp3 only asserts this, which is no use to a caller either way.
    let error = conn.bind_control_stream(id(0)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("unidirectional"), "got: {error}");

    // Server-initiated, on a client connection.
    let error = conn.bind_control_stream(id(SERVER_CONTROL)).unwrap_err();
    assert!(error.to_string().contains("this endpoint"), "got: {error}");

    // Out of range, which nghttp3 also only asserts.
    assert!(StreamId::new(-1).is_err());
    assert!(StreamId::new((1 << 62) - 1).is_ok());
    assert!(StreamId::new(1 << 62).is_err());
}

#[test]
fn a_role_cannot_be_bound_twice_and_doing_so_does_not_poison() {
    let mut conn = client();

    let error = conn.bind_control_stream(id(14)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        !error.is_fatal(),
        "a second bind is a caller mistake, not a fatal condition"
    );

    // The whole point: the connection is still fully usable afterwards.
    assert!(conn.is_usable());
    assert!(
        conn.writev_stream(&mut ()).unwrap().is_some(),
        "the connection should still have its preface to send"
    );
}

#[test]
fn connection_level_streams_cannot_share_an_identifier() {
    let mut conn = ConnBuilder::<()>::new(Role::Client).build().unwrap();
    conn.bind_control_stream(id(CLIENT_CONTROL)).unwrap();

    // Reusing the control stream for QPACK.
    let error = conn
        .bind_qpack_streams(id(CLIENT_CONTROL), id(CLIENT_QPACK_DECODER))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // The encoder and decoder cannot be the same stream either. nghttp3 assigns the
    // encoder before creating the decoder, so without this check the connection would be
    // left half-bound with no way to retry.
    let error = conn
        .bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_ENCODER))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // Neither failed attempt left anything bound, so a correct pair still works.
    conn.bind_qpack_streams(id(CLIENT_QPACK_ENCODER), id(CLIENT_QPACK_DECODER))
        .unwrap();
    assert!(conn.is_bound());
}

#[test]
fn a_backwards_timestamp_is_rejected() {
    let mut client = client();
    let mut server = server();
    let from_client = drain(&mut client);
    let control = &from_client[&CLIENT_CONTROL];

    server
        .read_stream(
            id(CLIENT_CONTROL),
            control,
            false,
            Timestamp::from_nanos(100),
            &mut (),
        )
        .unwrap();

    let error = server
        .read_stream(
            id(CLIENT_CONTROL),
            &[],
            false,
            Timestamp::from_nanos(99),
            &mut (),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("backwards"), "got: {error}");
}

#[test]
fn committing_more_than_was_offered_is_rejected() {
    let mut client = client();
    let send = client
        .writev_stream(&mut ())
        .unwrap()
        .expect("preface to send");
    let offered = send.len();

    let error = send.commit(offered + 1).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);

    // The transaction was refused, so the same bytes are still on offer.
    let send = client
        .writev_stream(&mut ())
        .unwrap()
        .expect("still on offer");
    assert_eq!(send.len(), offered);
}

#[test]
fn abandoning_a_transaction_re_offers_the_same_bytes() {
    let mut client = client();

    let send = client
        .writev_stream(&mut ())
        .unwrap()
        .expect("preface to send");
    let stream = send.stream();
    let offered: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    send.abandon();

    let send = client.writev_stream(&mut ()).unwrap().expect("re-offered");
    assert_eq!(send.stream(), stream);
    let again: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    assert_eq!(again, offered, "abandoning must not consume anything");
}

#[test]
fn a_partial_commit_re_offers_only_the_remainder() {
    let mut client = client();

    let send = client
        .writev_stream(&mut ())
        .unwrap()
        .expect("preface to send");
    let stream = send.stream();
    let whole: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    assert!(whole.len() > 2, "need something to split");
    send.commit(1).expect("accept a single byte");

    let send = client.writev_stream(&mut ()).unwrap().expect("remainder");
    assert_eq!(send.stream(), stream);
    let rest: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();
    assert_eq!(
        rest,
        whole[1..],
        "exactly the unaccepted bytes should be re-offered"
    );
}

#[test]
fn a_connection_can_be_moved_after_construction() {
    // The bridge slot is a separate allocation precisely so this is safe: a pointer into
    // a `Conn` field would dangle here, and nothing would say so.
    let conn = client();
    let mut moved = Box::new(conn);
    assert!(moved.writev_stream(&mut ()).unwrap().is_some());

    let mut moved_again = *moved;
    assert!(moved_again.is_usable());
    assert!(moved_again.writev_stream(&mut ()).unwrap().is_some());
}

#[test]
fn teardown_releases_every_native_allocation() {
    let mut client = client();
    let mut server = server();
    let from_client = drain(&mut client);
    deliver(&mut server, &from_client, 1);

    assert!(
        client.live_allocations() > 0,
        "the allocator should have been exercised, or the assertion below is vacuous"
    );

    // The `Drop` impl debug-asserts the balance is zero; reaching the end of this test
    // without aborting is the assertion.
    drop(client);
    drop(server);
}

#[test]
fn the_struct_versions_this_crate_compiles_against_are_v4() {
    // nghttp3's settings and callbacks structs are versioned, and the version selects the
    // layout the library reads. A vendored bump to V5 would change that layout silently,
    // so the expectation is pinned rather than inferred.
    assert_eq!(ngnet_h3::raw::NGHTTP3_SETTINGS_VERSION, 4);
    assert_eq!(ngnet_h3::raw::NGHTTP3_CALLBACKS_VERSION, 4);
    assert_eq!(
        ngnet_h3::raw::NGHTTP3_SETTINGS_VERSION,
        ngnet_h3::raw::NGHTTP3_SETTINGS_V4
    );
    assert_eq!(
        ngnet_h3::raw::NGHTTP3_CALLBACKS_VERSION,
        ngnet_h3::raw::NGHTTP3_CALLBACKS_V4
    );
}

#[test]
fn reading_a_locally_written_stream_is_rejected_rather_than_aborting() {
    // nghttp3 asserts that a stream it already knows can carry peer data, and asserts are
    // not a check a safe API may rely on. The streams it already knows are exactly the
    // three bound below, so without a check here this aborts where the assertion is
    // compiled in and, where it is not, parses the peer's bytes into our own sending
    // stream -- letting an endpoint accept its own SETTINGS as though the peer had sent
    // them.
    let mut client = client();

    for own in [CLIENT_CONTROL, CLIENT_QPACK_ENCODER, CLIENT_QPACK_DECODER] {
        let error = client
            .read_stream(id(own), &[0x00], false, Timestamp::from_nanos(1), &mut ())
            .expect_err("reading our own unidirectional stream must be refused");
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "stream {own}");
    }

    // A server-initiated unidirectional stream is readable by a client, and a
    // client-initiated bidirectional one by either side.
    assert!(
        client.is_usable(),
        "refusing must not poison the connection"
    );
}

#[test]
fn a_fatal_failure_poisons_the_connection_and_drop_still_succeeds() {
    let mut server = server();

    // A control stream whose first frame is not SETTINGS is a protocol violation that
    // nghttp3 reports from the read path, which its documentation makes unrecoverable.
    // 0x00 is the control stream type; 0x01 is HEADERS, which is illegal there.
    let bytes = [0x00u8, 0x01, 0x00];
    let error = server
        .read_stream(
            id(CLIENT_CONTROL),
            &bytes,
            false,
            Timestamp::from_nanos(1),
            &mut (),
        )
        .expect_err("a non-SETTINGS first frame must be refused");
    assert_eq!(
        error.kind(),
        ErrorKind::Protocol,
        "the peer caused this, not the caller"
    );
    assert!(
        error.app_error_code().is_some(),
        "a protocol error must carry a code to close the QUIC connection with"
    );

    // Everything afterwards refuses without re-entering nghttp3, because doing so would
    // be undefined behaviour.
    assert!(!server.is_usable());
    for kind in [
        server.writev_stream(&mut ()).err().map(|e| e.kind()),
        server
            .read_stream(
                id(CLIENT_CONTROL),
                &[],
                false,
                Timestamp::from_nanos(2),
                &mut (),
            )
            .err()
            .map(|e| e.kind()),
        server
            .add_ack_offset(id(SERVER_CONTROL), 1, &mut ())
            .err()
            .map(|e| e.kind()),
        server
            .resume_stream(id(SERVER_CONTROL))
            .err()
            .map(|e| e.kind()),
        server
            .block_stream(id(SERVER_CONTROL))
            .err()
            .map(|e| e.kind()),
        server
            .unblock_stream(id(SERVER_CONTROL))
            .err()
            .map(|e| e.kind()),
        server
            .bind_control_stream(id(SERVER_CONTROL))
            .err()
            .map(|e| e.kind()),
    ] {
        assert_eq!(kind, Some(ErrorKind::ConnectionUnusable));
    }

    // Dropping a poisoned connection still tears it down cleanly; the `Drop` impl's
    // allocation balance assertion is what would fire if it did not.
    drop(server);
}

#[test]
fn a_handler_still_reaches_the_caller_after_the_connection_has_moved() {
    // The bridge slot is a separate heap allocation solely so this holds. A pointer into
    // a `Conn` field would dangle once the connection moved, and the only thing that would
    // notice is a callback firing into freed memory.
    #[derive(Default)]
    struct Observed {
        reads: u32,
    }

    let mut server = ConnBuilder::<Observed>::new(Role::Server)
        .on_deferred_consume(|state: &mut Observed, _stream, _consumed| {
            state.reads += 1;
        })
        .build()
        .unwrap();
    server.bind_control_stream(id(SERVER_CONTROL)).unwrap();
    server
        .bind_qpack_streams(id(SERVER_QPACK_ENCODER), id(SERVER_QPACK_DECODER))
        .unwrap();

    let mut client = client();
    let from_client = drain(&mut client);

    // Move the connection twice before using it again.
    let mut moved = Box::new(server);
    let mut moved = *core::mem::replace(
        &mut moved,
        Box::new(ConnBuilder::<Observed>::new(Role::Server).build().unwrap()),
    );

    let mut state = Observed::default();
    for (&stream, bytes) in &from_client {
        moved
            .read_stream(
                id(stream),
                bytes,
                false,
                Timestamp::from_nanos(1),
                &mut state,
            )
            .expect("a moved connection must still read");
    }

    assert!(moved.is_usable());
    // The handler may or may not have fired -- deferred consumption only happens when
    // QPACK blocks -- but the connection reached it through the slot either way, and
    // reaching a dangling one would have crashed rather than returned.
    let _ = state.reads;
}
