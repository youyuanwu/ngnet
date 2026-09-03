//! Retransmitting stream data that was only partially accepted.
//!
//! ngtcp2 does not copy what it accepts — it keeps the caller's pointer and reads through it
//! again if the packet is lost (`ngtcp2.h:5244-5248`). `src/retain.rs` exists to make that
//! safe by holding a copy at a fixed address.
//!
//! The case that matters is a **partial** acceptance, which is the ordinary one rather than
//! an edge: the caller offers a payload, a packet fills before the offer is exhausted, and
//! ngtcp2 takes a prefix. The remainder comes back as a separate write. If anything moves
//! the accepted prefix afterwards — shrinking the allocation to fit is the obvious tidy-up,
//! and is what this crate used to do — ngtcp2 is left pointing at freed memory, and it reads
//! that memory only when it retransmits.
//!
//! Nothing on a lossless loopback retransmits, which is exactly why that defect survived. The
//! unit test in `src/retain.rs` asserts the address directly and is the real guard, because
//! reading freed memory usually still yields the old bytes and so proves nothing. These tests
//! cover the surrounding behaviour: that a lost packet carrying a partially accepted write is
//! retransmitted at all, and that the unaccepted remainder comes back exactly once.

use std::sync::{Arc, Mutex};

use ngnet_quic::{Handlers, Inspection, StreamWrite, inspect};
use ngnet_quic_tests::{
    PACING_STEP_NANOS, TEST_SERVER_NAME, TestClock, TestConn, TestCredentials, client_backend,
    client_conn, drain, pump, server_backend, server_conn,
};

/// Stream bytes a connection's handlers saw, in arrival order.
type Received = Arc<Mutex<Vec<(i64, Vec<u8>)>>>;

fn recording(sink: &Received) -> Handlers<'_> {
    let seen = Arc::clone(sink);
    Handlers::new().on_stream_data(move |id, data, _fin| {
        seen.lock().unwrap().push((id.get(), data.to_vec()));
    })
}

fn addrs() -> (std::net::SocketAddr, std::net::SocketAddr) {
    (
        "127.0.0.1:41051".parse().unwrap(),
        "127.0.0.1:41052".parse().unwrap(),
    )
}

/// Brings up a handshaked pair, with the server recording what arrives.
fn connected<'h>(
    credentials: &'h TestCredentials,
    server_sink: &'h Received,
) -> (TestConn<'h>, TestConn<'h>, TestClock) {
    let clock = TestClock::new();
    let (client_addr, server_addr) = addrs();

    let cb = client_backend(&credentials.certificate_pem);
    let sb = server_backend(credentials);

    let mut client = client_conn(
        &cb,
        &clock,
        Handlers::new(),
        client_addr,
        server_addr,
        Some(TEST_SERVER_NAME),
    )
    .expect("building the client");

    let first = drain(&mut client, &clock).expect("first flight");
    let (odcid, scid) = match inspect(&first[0], 8).expect("decoding the first flight") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("unexpected first flight: {other:?}"),
    };

    let mut server = server_conn(
        &sb,
        &clock,
        recording(server_sink),
        server_addr,
        client_addr,
        &odcid,
        scid,
    )
    .expect("building the server");

    for datagram in &first {
        let _ = server.read_pkt(datagram, clock.now());
    }
    let _ = pump(&mut client, &mut server, &clock, 32);

    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the handshake must complete before stream data means anything"
    );

    (client, server, clock)
}

#[test]
fn a_lost_packet_carrying_a_partially_accepted_write_is_retransmitted() {
    let credentials = TestCredentials::generate();
    let seen: Received = Arc::default();
    let (mut client, mut server, clock) = connected(&credentials, &seen);

    let stream = client.open_bidi_stream().expect("opening a stream");

    // Offer far more than one packet can carry, so acceptance is certain to be partial and
    // the retained chunk therefore has an unaccepted tail — the shape that used to move.
    let payload = vec![0xA5u8; 8 * 1024];
    let mut buf = vec![0u8; 1500];

    let accepted = match client
        .write_stream(&mut buf, stream, &payload, false, clock.now())
        .expect("writing stream data")
    {
        StreamWrite::Datagram { len, accepted } => {
            // The packet is lost: it is simply never delivered. Everything the connection
            // knows about those bytes now lives only in its retained copy.
            let _lost = &buf[..len];
            accepted
        }
        other => panic!("expected a datagram carrying stream data, got {other:?}"),
    };

    assert!(
        accepted > 0 && accepted < payload.len(),
        "this test is about a partial acceptance; got {accepted} of {}",
        payload.len()
    );

    // Let the connection's own probe deadline elapse. Guessing a duration would make this
    // either flaky or slow; the connection knows when it has given up on the packet.
    //
    // Recovery is not one step. A single lost packet is not detected by acknowledgement of
    // later ones -- there are none -- so ngtcp2 falls back to a probe timeout, sends a probe,
    // and only learns the packet was lost once the probe draws an acknowledgement back. The
    // stream data is retransmitted after that. Driving one expiry and expecting the bytes is
    // what an earlier version of this test did, and it failed for that reason rather than
    // because retransmission was broken.
    let mut retransmitted = 0usize;
    for _ in 0..32 {
        let next = [client.expiry(), server.expiry()]
            .into_iter()
            .flatten()
            .min();
        if let Some(deadline) = next {
            clock.advance_to(deadline);
            clock.advance(PACING_STEP_NANOS);
            client.handle_expiry(clock.now()).expect("client expiry");
            server.handle_expiry(clock.now()).expect("server expiry");
        }

        for datagram in drain(&mut client, &clock).expect("draining the client") {
            retransmitted += 1;
            server
                .read_pkt(&datagram, clock.now())
                .expect("delivering to the server");
        }
        for datagram in drain(&mut server, &clock).expect("draining the server") {
            client
                .read_pkt(&datagram, clock.now())
                .expect("delivering to the client");
        }

        let arrived = seen
            .lock()
            .unwrap()
            .iter()
            .any(|(id, data)| *id == stream.get() && !data.is_empty());
        if arrived {
            break;
        }
    }

    assert!(
        retransmitted > 0,
        "a lost packet must eventually be retransmitted, or this test proves nothing"
    );

    let received = seen.lock().unwrap();
    let bytes: Vec<u8> = received
        .iter()
        .filter(|(id, _)| *id == stream.get())
        .flat_map(|(_, data)| data.clone())
        .collect();

    assert!(
        !bytes.is_empty(),
        "the retransmission must actually carry the stream data"
    );
    assert!(
        bytes.len() <= accepted,
        "no more than was accepted can have been sent: {} > {accepted}",
        bytes.len()
    );
    assert!(
        bytes.iter().all(|b| *b == 0xA5),
        "retransmitted bytes must be the ones originally accepted"
    );
}

#[test]
fn a_partially_accepted_write_offers_its_remainder_again_exactly_once() {
    // The companion property. The bytes ngtcp2 did not take must come back, once and in
    // order. An accounting slip here is how a stream silently truncates or duplicates, and
    // the offset bookkeeping sits in the same code as the retention.
    let credentials = TestCredentials::generate();
    let seen: Received = Arc::default();
    let (mut client, mut server, clock) = connected(&credentials, &seen);

    let stream = client.open_bidi_stream().expect("opening a stream");

    let payload: Vec<u8> = (0..16u32 * 1024).map(|i| (i % 251) as u8).collect();
    let mut buf = vec![0u8; 1500];
    let mut offset = 0usize;
    let mut partial_acceptances = 0usize;

    for _ in 0..1024 {
        if offset >= payload.len() {
            break;
        }
        let remaining = &payload[offset..];
        match client
            .write_stream(&mut buf, stream, remaining, true, clock.now())
            .expect("writing stream data")
        {
            StreamWrite::Datagram { len, accepted } => {
                if accepted < remaining.len() {
                    partial_acceptances += 1;
                }
                offset += accepted;
                server
                    .read_pkt(&buf[..len], clock.now())
                    .expect("delivering");
                clock.advance(PACING_STEP_NANOS);
            }
            // A produced packet that carried nothing of this stream: the offer stands and is
            // made again unchanged. It counts as a partial acceptance for the assertion
            // below — the payload was not taken in one go — but consumes no offset.
            StreamWrite::DatagramWithoutStream { len } => {
                partial_acceptances += 1;
                server
                    .read_pkt(&buf[..len], clock.now())
                    .expect("delivering");
                clock.advance(PACING_STEP_NANOS);
            }
            StreamWrite::Idle
            | StreamWrite::Blocked
            | StreamWrite::StreamBlocked
            | StreamWrite::ConnectionBlocked => {
                // Let the peer acknowledge, which reopens the window and releases retention.
                let mut moved = false;
                for datagram in drain(&mut server, &clock).expect("draining the server") {
                    client
                        .read_pkt(&datagram, clock.now())
                        .expect("delivering an acknowledgement");
                    moved = true;
                }
                clock.advance(PACING_STEP_NANOS);
                if !moved {
                    let Some(deadline) = client.expiry() else {
                        break;
                    };
                    clock.advance_to(deadline);
                    clock.advance(PACING_STEP_NANOS);
                    client.handle_expiry(clock.now()).expect("handling expiry");
                }
            }
        }
    }

    assert_eq!(offset, payload.len(), "every byte must eventually be taken");
    assert!(
        partial_acceptances > 0,
        "a payload this size must have been accepted in pieces, or the test is not \
         exercising the path it claims to"
    );

    let received = seen.lock().unwrap();
    let bytes: Vec<u8> = received
        .iter()
        .filter(|(id, _)| *id == stream.get())
        .flat_map(|(_, data)| data.clone())
        .collect();

    assert_eq!(
        bytes, payload,
        "the stream must arrive byte for byte: no gap where a partial acceptance was, and \
         no duplicate where a remainder was offered again"
    );
}
