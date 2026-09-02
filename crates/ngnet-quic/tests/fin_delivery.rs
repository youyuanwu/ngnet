//! A `fin` is only finished when ngtcp2 says it serialised one.
//!
//! # What this file pins down
//!
//! `ngtcp2_conn_writev_stream` can return a packet that contains no STREAM frame at all —
//! "The packet might not contain STREAM frame if other frames occupy the packet. In that
//! case, `*pdatalen` would be -1"
//! (`crates/ngnet-quic-sys/vendor/ngtcp2/lib/includes/ngtcp2/ngtcp2.h:5233-5236`). It is
//! also allowed to serialise a *zero-length* STREAM frame, and does so exactly when the
//! offer carries nothing but `fin`: "If 0 length STREAM frame is successfully serialized,
//! `*pdatalen` would be 0" (`ngtcp2.h:5240-5243`).
//!
//! On a `fin`-only write those two are opposites — one ended the stream, the other did not
//! touch it — and both would arrive at a caller as "zero bytes accepted" if the sign were
//! thrown away. A caller that reads the second as the first stops sending. Nothing is in
//! flight, so loss recovery has nothing to retransmit, and the peer waits for an end that
//! was never written until its idle timeout ends the connection.
//!
//! # How the condition is produced without a race
//!
//! Deterministically, from ngtcp2's own rules rather than from timing:
//!
//! * writing packets back to back at one instant moves the pacing deadline into the future,
//!   after which ngtcp2 will not put new ack-eliciting data in a packet; and
//! * an acknowledgement is not congestion controlled or paced, so a connection that owes one
//!   still produces a packet.
//!
//! Arrange both at the same instant and ngtcp2 produces an ACK-only packet in answer to a
//! `fin` write. No sockets, no threads and no wall clock are involved, so the interleaving is
//! fixed.

#![cfg(feature = "tls-ossl")]

use std::cell::Cell;
use std::io::IoSlice;
use std::sync::Mutex;

use ngnet_quic::{
    Backend as TlsBackend, ConnBuilder, EntropySource, Handlers, Inspection, OsslBackend,
    OsslSession, Result, Role, Settings, StreamId, StreamWrite, Timestamp, TransportParams, Verify,
    WriteOutcome, inspect,
};

const TEST_CERT_PEM: &str = include_str!("data/test-cert.pem");
const TEST_KEY_PEM: &str = include_str!("data/test-key.pem");

/// A counter, adequate because this test does not depend on unpredictability.
struct StubEntropy(u8);

impl EntropySource for StubEntropy {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        for slot in dest.iter_mut() {
            self.0 = self.0.wrapping_add(1);
            *slot = self.0;
        }
        Ok(())
    }
}

/// A clock that only moves when the test says so.
struct HandClock {
    now: Cell<u64>,
}

impl HandClock {
    fn new() -> Self {
        // Non-zero: ngtcp2 reads a zero start as a real instant an eternity ago.
        Self {
            now: Cell::new(1_000_000_000),
        }
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now.get()).expect("a timestamp")
    }

    fn advance(&self, nanos: u64) {
        self.now.set(self.now.get() + nanos);
    }
}

/// Everything the client's handlers saw, so the test can ask what actually arrived.
#[derive(Default)]
struct Seen {
    /// One entry per `on_stream_data` call: the stream, the byte count, and the `fin` flag.
    data: Vec<(i64, usize, bool)>,
}

impl Seen {
    fn saw_fin(&self, stream: StreamId) -> bool {
        self.data
            .iter()
            .any(|(id, _, fin)| *id == stream.get() && *fin)
    }

    /// How many `on_stream_data` calls named one stream.
    fn events_for(&self, stream: StreamId) -> usize {
        self.data
            .iter()
            .filter(|(id, _, _)| *id == stream.get())
            .count()
    }
}

fn client_backend() -> OsslBackend {
    OsslBackend::builder(Role::Client)
        .alpn("h3")
        .verify(Verify::DangerouslyAcceptAnyCertificate)
        .build()
        .expect("a client backend")
}

fn server_backend() -> OsslBackend {
    OsslBackend::builder(Role::Server)
        .alpn("h3")
        .certificate_chain_pem(TEST_CERT_PEM)
        .private_key_pem(TEST_KEY_PEM)
        .build()
        .expect("a server backend")
}

/// Drains a connection's outbound datagrams, advancing the clock so pacing does not stop it.
fn drain(conn: &mut ngnet_quic::Conn<'_, OsslSession>, clock: &HandClock) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 1500];
    for _ in 0..64 {
        match conn
            .write_pkt(&mut buf, clock.now())
            .expect("writing a packet")
        {
            WriteOutcome::Datagram { len } => {
                out.push(buf[..len].to_vec());
                clock.advance(2_000_000);
            }
            WriteOutcome::Idle | WriteOutcome::Blocked => break,
        }
    }
    out
}

/// Relays datagrams both ways until neither side has anything more to say.
fn relay(
    client: &mut ngnet_quic::Conn<'_, OsslSession>,
    server: &mut ngnet_quic::Conn<'_, OsslSession>,
    clock: &HandClock,
    rounds: usize,
) {
    for _ in 0..rounds {
        let mut progressed = false;
        for datagram in drain(client, clock) {
            progressed = true;
            server
                .read_pkt(&datagram, clock.now())
                .expect("server read");
        }
        for datagram in drain(server, clock) {
            progressed = true;
            client
                .read_pkt(&datagram, clock.now())
                .expect("client read");
        }
        if !progressed {
            break;
        }
    }
}

#[test]
fn a_packet_that_carried_no_stream_frame_is_not_reported_as_a_written_fin() {
    let client_backend = client_backend();
    let server_backend = server_backend();
    let clock = HandClock::new();
    let seen = Mutex::new(Seen::default());

    // --- establish, in memory -------------------------------------------------------
    let client_session = client_backend
        .new_session(Role::Client, Some("localhost"))
        .expect("a client session");
    let mut client = ConnBuilder::new(
        Role::Client,
        Settings::new(clock.now()),
        // Room for several packets on one stream: the trigger below needs enough of them
        // in flight for ngtcp2's packet-threshold loss rule to fire.
        TransportParams::new()
            .initial_max_data(1 << 20)
            .initial_max_stream_data_uni(1 << 20)
            .initial_max_stream_data_bidi_local(1 << 20)
            .initial_max_stream_data_bidi_remote(1 << 20)
            .initial_max_streams_uni(16)
            .initial_max_streams_bidi(16),
        Box::new(StubEntropy(0)),
        client_session,
        "127.0.0.1:1000".parse().expect("a local address"),
        "127.0.0.1:2000".parse().expect("a remote address"),
    )
    .build(Handlers::new().on_stream_data(|stream, bytes, fin| {
        seen.lock()
            .expect("the handler is the only writer")
            .data
            .push((stream.get(), bytes.len(), fin));
    }))
    .expect("building the client");

    let first_flight = drain(&mut client, &clock);
    assert!(
        !first_flight.is_empty(),
        "a fresh client must have a first flight to send"
    );
    let (original_dcid, client_scid) = match inspect(&first_flight[0], 8).expect("decoding") {
        Inspection::Supported { dcid, scid, .. } => (dcid, scid),
        other => panic!("the first flight should be a supported long header, got {other:?}"),
    };

    let server_session = server_backend
        .new_session(Role::Server, None)
        .expect("a server session");
    let mut server = ConnBuilder::new(
        Role::Server,
        Settings::new(clock.now()),
        TransportParams::new()
            .original_dcid(&original_dcid)
            .initial_max_data(1 << 20)
            .initial_max_stream_data_uni(1 << 20)
            .initial_max_stream_data_bidi_local(1 << 20)
            .initial_max_stream_data_bidi_remote(1 << 20)
            .initial_max_streams_uni(16)
            .initial_max_streams_bidi(16),
        Box::new(StubEntropy(64)),
        server_session,
        "127.0.0.1:2000".parse().expect("a local address"),
        "127.0.0.1:1000".parse().expect("a remote address"),
    )
    .dcid(client_scid)
    .build(Handlers::new())
    .expect("building the server");

    for datagram in &first_flight {
        server.read_pkt(datagram, clock.now()).expect("server read");
    }
    relay(&mut client, &mut server, &clock, 32);
    assert!(
        client.is_handshake_completed() && server.is_handshake_completed(),
        "the harness did not establish a connection"
    );

    // --- a server-opened stream the client will read --------------------------------
    let stream = server.open_uni_stream().expect("opening a stream");
    let mut dest = vec![0u8; 1500];

    // Body first, so the `fin` is a write of its own -- which is how an HTTP/3 layer that
    // ends a response after its last DATA frame reaches the transport.
    let body = [0x5au8; 512];
    let mut sent = 0usize;
    for _ in 0..64 {
        if sent == body.len() {
            break;
        }
        match server
            .write_stream(&mut dest, stream, &body[sent..], false, clock.now())
            .expect("writing the body")
        {
            StreamWrite::Datagram { len, accepted } => {
                sent += accepted;
                client
                    .read_pkt(&dest[..len], clock.now())
                    .expect("client read");
            }
            StreamWrite::DatagramWithoutStream { len } => {
                client
                    .read_pkt(&dest[..len], clock.now())
                    .expect("client read");
            }
            _ => clock.advance(1_000_000),
        }
    }
    assert_eq!(sent, body.len(), "the whole body should have been offered");

    // --- put the server in the state that produced the defect -----------------------
    //
    // ngtcp2 appends the caller's STREAM frame only to a packet built while no *other*
    // stream is queued for transmission: `ngtcp2_pq_empty(&conn->tx.strmq)` guards the whole
    // stream-writing block (`ngtcp2_conn.c:4251-4253`). When another stream is queued, that
    // stream's data takes the packet, the caller's offer is skipped, and `*pdatalen` keeps
    // the `-1` the entry point set (`ngtcp2_conn.c:12145-12147`).
    //
    // A second stream gets queued by having one of its packets lost, which an in-memory
    // harness arranges exactly rather than statistically: produce the datagram and simply
    // do not deliver it, then let loss detection fire.
    const LOST_PACKETS: usize = 4;
    let sibling = server.open_uni_stream().expect("a second server stream");
    let filler = [0x77u8; 16_384];
    let mut staged = 0usize;
    let mut produced = 0usize;
    for _ in 0..64 {
        if staged == filler.len() {
            break;
        }
        match server
            .write_stream(&mut dest, sibling, &filler[staged..], false, clock.now())
            .expect("writing the sibling stream")
        {
            StreamWrite::Datagram { len, accepted } => {
                staged += accepted;
                produced += 1;
                // The first few packets are produced and then simply not delivered. Every
                // later one is, so the client's acknowledgement names packets after the gap
                // and ngtcp2's packet-threshold rule declares the missing ones lost.
                //
                // More than one, deliberately: a single lost packet's worth of rescheduled
                // data fits alongside the `fin` in the next packet, and then the `fin` is
                // written after all. Several packets' worth cannot fit, so the send queue is
                // still occupied when ngtcp2 reaches the caller's offer.
                if produced > LOST_PACKETS {
                    client
                        .read_pkt(&dest[..len], clock.now())
                        .expect("client read");
                }
                clock.advance(1_000_000);
            }
            StreamWrite::DatagramWithoutStream { len } => {
                client
                    .read_pkt(&dest[..len], clock.now())
                    .expect("client read");
                clock.advance(1_000_000);
            }
            _ => clock.advance(2_000_000),
        }
    }
    assert!(
        produced > LOST_PACKETS + 3,
        "the sibling stream should have produced enough packets for a packet-threshold loss, \
         got {produced}"
    );

    // The acknowledgement is what makes ngtcp2 notice the gap, reschedule the lost STREAM
    // frame, and put its stream back on the send queue.
    let mut acknowledged = false;
    for datagram in drain(&mut client, &clock) {
        server
            .read_pkt(&datagram, clock.now())
            .expect("server read");
        acknowledged = true;
    }
    assert!(
        acknowledged,
        "the client should have acknowledged the packets it did receive"
    );

    // --- the write under test -------------------------------------------------------
    let before = seen.lock().expect("no writer is live").events_for(stream);
    let outcome = server
        .write_stream_vectored(&mut dest, stream, &[IoSlice::new(&[])], true, clock.now())
        .expect("offering the fin");

    // Whatever came out of that call, hand it to the client so the test can ask what the
    // packet actually contained rather than assuming.
    if let StreamWrite::Datagram { len, .. } | StreamWrite::DatagramWithoutStream { len } = outcome
    {
        client
            .read_pkt(&dest[..len], clock.now())
            .expect("client read");
    }

    let fin_arrived = seen.lock().expect("no writer is live").saw_fin(stream);

    // Non-vacuity first. The whole point of the construction above is to make ngtcp2 skip
    // the caller's stream, and a change that stopped it doing so would leave every
    // assertion below trivially true while proving nothing.
    assert!(
        !fin_arrived,
        "the construction no longer displaces the fin -- ngtcp2 wrote it after all, so this \
         test would pass for the wrong reason; stream events seen: {:?}",
        seen.lock().expect("no writer is live").data,
    );
    assert_eq!(
        seen.lock().expect("no writer is live").events_for(stream),
        before,
        "the packet under test should have carried nothing for this stream at all"
    );

    // The packet did not carry the end of the stream. Reporting it as a datagram that took
    // the offer -- which for a `fin`-only offer means "the stream ended" -- is the
    // conflation this test exists to prevent: the caller stops, ngtcp2 has nothing in flight
    // to retransmit, and the peer waits until the idle timeout.
    assert!(
        !matches!(outcome, StreamWrite::Datagram { .. }),
        "the fin was not serialised, yet the write was reported as {outcome:?}, which tells a \
         caller the stream ended"
    );
    assert!(
        matches!(outcome, StreamWrite::DatagramWithoutStream { .. }),
        "a produced packet that carried no STREAM frame should say so, got {outcome:?}"
    );

    // --- and the fin must still be deliverable --------------------------------------
    //
    // The point of not claiming it was written is that offering it again works. Time is
    // allowed to move here, which is what lifts the pacing block.
    for _ in 0..64 {
        if seen.lock().expect("no writer is live").saw_fin(stream) {
            break;
        }
        clock.advance(5_000_000);
        match server
            .write_stream_vectored(&mut dest, stream, &[IoSlice::new(&[])], true, clock.now())
            .expect("re-offering the fin")
        {
            StreamWrite::Datagram { len, .. } | StreamWrite::DatagramWithoutStream { len } => {
                client
                    .read_pkt(&dest[..len], clock.now())
                    .expect("client read");
            }
            _ => {}
        }
        relay(&mut client, &mut server, &clock, 4);
    }

    assert!(
        seen.lock().expect("no writer is live").saw_fin(stream),
        "the fin never reached the client; stream events seen: {:?}",
        seen.lock().expect("no writer is live").data
    );
}
