//! Stream and connection termination, observed from the other side.
//!
//! Every test here drives one termination path on one peer and asserts the other peer sees
//! the matching outcome with the matching code. These exercise the paths the end-to-end
//! tests never reach, and they are where the two stream directions must stay apart: a peer's
//! RESET_STREAM ends the direction the peer sends on, a peer's STOP_SENDING ends ours, and
//! confusing the two breaks ordinary exchanges rather than exotic ones.

mod common;

use bytes::{Buf, Bytes};
use common::{Pair, within};
use h3::quic::{
    BidiStream as _, Connection as _, ConnectionErrorIncoming, OpenStreams as _, RecvStream as _,
    SendStream as _, SendStreamUnframed as _, StreamErrorIncoming,
};

/// Writes `payload` on a raw stream, offering the remainder until all of it is accepted.
async fn write_all<T: SendStreamUnframedExt>(stream: &mut T, payload: &[u8]) {
    let mut pending = Bytes::copy_from_slice(payload);
    while pending.has_remaining() {
        within(
            "poll_send",
            std::future::poll_fn(|cx| stream.poll_send_any(cx, &mut pending)),
        )
        .await
        .expect("writing raw stream bytes");
    }
}

/// A tiny shim so both handle types can be written to by the helper above.
trait SendStreamUnframedExt {
    fn poll_send_any(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut Bytes,
    ) -> std::task::Poll<Result<usize, StreamErrorIncoming>>;
}

impl<T: h3::quic::SendStreamUnframed<Bytes>> SendStreamUnframedExt for T {
    fn poll_send_any(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut Bytes,
    ) -> std::task::Poll<Result<usize, StreamErrorIncoming>> {
        self.poll_send(cx, buf)
    }
}

/// Reads until a clean end or a terminal, returning both.
async fn read_to_end<R: h3::quic::RecvStream>(
    stream: &mut R,
) -> (Bytes, Option<StreamErrorIncoming>) {
    let mut out = bytes::BytesMut::new();
    loop {
        match within("poll_data", std::future::poll_fn(|cx| stream.poll_data(cx))).await {
            Ok(Some(mut chunk)) => {
                while chunk.has_remaining() {
                    let piece = chunk.chunk().to_vec();
                    chunk.advance(piece.len());
                    out.extend_from_slice(&piece);
                }
            }
            Ok(None) => return (out.freeze(), None),
            Err(err) => return (out.freeze(), Some(err)),
        }
    }
}

/// Opens a bidirectional stream on the client and accepts it on the server.
async fn opened_pair(
    pair: &mut Pair,
) -> (
    h3_ngnet_quic::BidiStream<ngnet_quic::OsslSession>,
    h3_ngnet_quic::BidiStream<ngnet_quic::OsslSession>,
) {
    let client = pair.client.as_mut().expect("a client");
    let mut opened = within(
        "poll_open_bidi",
        std::future::poll_fn(|cx| client.poll_open_bidi(cx)),
    )
    .await
    .expect("opening a bidirectional stream");

    // A stream the peer has never heard of is not observable, so put a byte on it.
    write_all(&mut opened, b"open").await;

    let server = pair.server.as_mut().expect("a server");
    let accepted = within(
        "poll_accept_bidi",
        std::future::poll_fn(|cx| server.poll_accept_bidi(cx)),
    )
    .await
    .expect("accepting a bidirectional stream");
    (opened, accepted)
}

// ---------------------------------------------------------------------------
// Clean finish
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_finished_stream_ends_cleanly_for_the_peer() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    write_all(&mut sender, b" and more").await;
    within(
        "poll_finish",
        std::future::poll_fn(|cx| sender.poll_finish(cx)),
    )
    .await
    .expect("finishing the stream");

    let (received, terminal) = read_to_end(&mut receiver).await;
    assert_eq!(&received[..], b"open and more");
    assert!(
        terminal.is_none(),
        "a finished stream must end cleanly, got {terminal:?}"
    );
}

#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn finish_is_idempotent_and_emits_one_end_of_stream() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    for _ in 0..3 {
        within(
            "poll_finish",
            std::future::poll_fn(|cx| sender.poll_finish(cx)),
        )
        .await
        .expect("finishing the stream is idempotent");
    }

    let (received, terminal) = read_to_end(&mut receiver).await;
    assert_eq!(&received[..], b"open");
    assert!(terminal.is_none(), "got {terminal:?}");

    // A second read past the end stays at the end rather than erroring.
    let again = within(
        "poll_data",
        std::future::poll_fn(|cx| receiver.poll_data(cx)),
    )
    .await
    .expect("reading past a clean end");
    assert!(again.is_none(), "the end of a stream is stable");
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

/// A reset is observed with its code, *after* the data that was already delivered.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_peer_reset_is_observed_with_its_code_after_delivered_data() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    // Let the already-written bytes land before resetting, so the ordering is meaningful.
    let first = within(
        "poll_data",
        std::future::poll_fn(|cx| receiver.poll_data(cx)),
    )
    .await
    .expect("reading delivered data")
    .expect("a chunk");
    assert_eq!(first.chunk(), b"open");

    sender.reset(0x1234);

    let (rest, terminal) = read_to_end(&mut receiver).await;
    assert!(rest.is_empty(), "no further data was sent");
    match terminal {
        Some(StreamErrorIncoming::StreamTerminated { error_code }) => {
            assert_eq!(error_code, 0x1234, "the peer's reset code must survive");
        }
        other => panic!("expected a stream termination carrying the code, got {other:?}"),
    }

    // And the outcome is stable on repeated observation.
    let repeated = within(
        "poll_data",
        std::future::poll_fn(|cx| receiver.poll_data(cx)),
    )
    .await;
    match repeated {
        Err(StreamErrorIncoming::StreamTerminated { error_code }) => {
            assert_eq!(error_code, 0x1234, "the reset outcome must be stable");
        }
        other => panic!("expected the same termination again, got {other:?}"),
    }
}

/// A peer's reset ends the direction the peer sends on, and not ours.
///
/// This is the regression test for the direction conflation: our sending side is untouched by
/// the peer abandoning its own.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_peer_reset_does_not_terminate_our_sending_side() {
    let mut pair = Pair::new().await;
    let (mut client_stream, mut server_stream) = opened_pair(&mut pair).await;

    // Server resets the direction it sends on.
    server_stream.reset(0x99);

    // The client observes the reset on its receiving side.
    let (_data, terminal) = read_to_end(&mut client_stream).await;
    assert!(
        matches!(
            terminal,
            Some(StreamErrorIncoming::StreamTerminated { error_code: 0x99 })
        ),
        "the client's receiving side must observe the reset, got {terminal:?}"
    );

    // But the client's own sending side is still usable.
    let mut pending = Bytes::from_static(b"still sending");
    let written = within(
        "poll_send",
        std::future::poll_fn(|cx| client_stream.poll_send(cx, &mut pending)),
    )
    .await
    .expect("our sending side must survive the peer resetting its own");
    assert!(
        written > 0,
        "the peer resetting its sending direction must not close ours"
    );
}

// ---------------------------------------------------------------------------
// Stop sending
// ---------------------------------------------------------------------------

/// Stop-sending reaches the peer's send side with its code, and discards what it retained.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn stop_sending_terminates_the_peers_send_side_with_its_code() {
    let mut pair = Pair::new().await;
    let (mut client_stream, mut server_stream) = opened_pair(&mut pair).await;

    server_stream.stop_sending(0x4242);

    // The client's sending side eventually reports the peer's code. It takes a poll or two for
    // the STOP_SENDING frame to arrive, so drive until it does.
    let mut observed = None;
    for _ in 0..200 {
        let mut pending = Bytes::from_static(b"payload");
        match std::future::poll_fn(|cx| client_stream.poll_send(cx, &mut pending)).await {
            Ok(_) => tokio::task::yield_now().await,
            Err(err) => {
                observed = Some(err);
                break;
            }
        }
    }
    match observed {
        Some(StreamErrorIncoming::StreamTerminated { error_code }) => {
            assert_eq!(
                error_code, 0x4242,
                "the peer's stop-sending code must survive"
            );
        }
        other => panic!("expected the send side to terminate with the code, got {other:?}"),
    }
}

/// A peer's stop-sending must not make our *receiving* side fail.
///
/// The regression test for the other half of the direction conflation. A server that sends a
/// complete response and then stop-sends the request stream is ordinary; the client must still
/// read that response to a clean end.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn stop_sending_does_not_terminate_our_receiving_side() {
    let mut pair = Pair::new().await;
    let (mut client_stream, mut server_stream) = opened_pair(&mut pair).await;

    // The server sends a complete reply and finishes it, then stop-sends the client.
    write_all(&mut server_stream, b"complete reply").await;
    within(
        "poll_finish",
        std::future::poll_fn(|cx| server_stream.poll_finish(cx)),
    )
    .await
    .expect("finishing the reply");
    server_stream.stop_sending(0x10c);

    let (received, terminal) = read_to_end(&mut client_stream).await;
    assert_eq!(
        &received[..],
        b"complete reply",
        "a reply that arrived intact must be readable in full"
    );
    assert!(
        terminal.is_none(),
        "a stop-sending on our send side must not fail our receiving side, got {terminal:?}"
    );
}

// ---------------------------------------------------------------------------
// Send-side misuse
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn an_overlapping_logical_send_is_rejected() {
    let mut pair = Pair::new().await;
    let client = pair.client.as_mut().expect("a client");
    let mut stream = within(
        "poll_open_bidi",
        std::future::poll_fn(|cx| client.poll_open_bidi(cx)),
    )
    .await
    .expect("opening a bidirectional stream");

    // A large body so the first logical send cannot be fully accepted in one go.
    let first = h3::quic::WriteBuf::from(h3::proto::stream::StreamType::CONTROL);
    stream.send_data(first).expect("the first logical send");
    let second = h3::quic::WriteBuf::from(h3::proto::stream::StreamType::CONTROL);
    let err = stream
        .send_data(second)
        .expect_err("a second logical send while one is outstanding must be rejected");
    assert!(
        matches!(err, StreamErrorIncoming::ConnectionErrorIncoming { .. }),
        "expected an internal error, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn an_unframed_send_while_framed_data_is_retained_is_rejected() {
    let mut pair = Pair::new().await;
    let client = pair.client.as_mut().expect("a client");
    let mut stream = within(
        "poll_open_bidi",
        std::future::poll_fn(|cx| client.poll_open_bidi(cx)),
    )
    .await
    .expect("opening a bidirectional stream");

    stream
        .send_data(h3::quic::WriteBuf::from(
            h3::proto::stream::StreamType::CONTROL,
        ))
        .expect("the framed send");

    let mut raw = Bytes::from_static(b"raw");
    let outcome = std::future::poll_fn(|cx| stream.poll_send(cx, &mut raw)).await;
    let err = outcome.expect_err("mixing framed and unframed sends must be rejected");
    assert!(
        matches!(err, StreamErrorIncoming::ConnectionErrorIncoming { .. }),
        "expected an internal error, got {err:?}"
    );
    assert_eq!(
        raw.len(),
        3,
        "a rejected unframed send must consume nothing"
    );
}

// ---------------------------------------------------------------------------
// Split halves and dropped sends
// ---------------------------------------------------------------------------

/// Dropping one split half leaves the other working.
///
/// The regression test for the shared stream state. The receiving half is dropped *while the
/// sending half still holds an undrained logical send*, which is the case that used to lose
/// data: destroying the shared state took `writing` with it, `poll_ready` then found nothing
/// outstanding and reported success, and the bytes were silently dropped on the floor.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn dropping_one_split_half_does_not_invalidate_the_other() {
    let mut pair = Pair::new().await;
    let (client_stream, mut server_stream) = opened_pair(&mut pair).await;

    let (mut send_half, recv_half) = client_stream.split();

    // Hand over a logical send and deliberately do not drain it yet.
    send_half
        .send_data(h3::quic::WriteBuf::from(
            h3::proto::stream::StreamType::CONTROL,
        ))
        .expect("storing a logical send");

    // Drop the other half while that send is still retained.
    drop(recv_half);

    within(
        "poll_ready",
        std::future::poll_fn(|cx| send_half.poll_ready(cx)),
    )
    .await
    .expect("the retained send must survive the other half being dropped");
    within(
        "poll_finish",
        std::future::poll_fn(|cx| send_half.poll_finish(cx)),
    )
    .await
    .expect("the surviving half must still finish cleanly");

    let (received, terminal) = read_to_end(&mut server_stream).await;
    assert_eq!(
        &received[..],
        // "open", then the encoded CONTROL stream type the retained send carried.
        b"open\x00",
        "the retained send must still be delivered in full"
    );
    assert!(
        terminal.is_none(),
        "dropping the receiving half must not reset a cleanly finished stream, got {terminal:?}"
    );
}

/// Dropping an unfinished sending half is observed as exactly one reset, not an endless wait.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn dropping_an_unfinished_send_is_observed_as_one_reset() {
    let mut pair = Pair::new().await;
    let (client_stream, mut server_stream) = opened_pair(&mut pair).await;

    drop(client_stream);

    let (received, terminal) = read_to_end(&mut server_stream).await;
    assert_eq!(
        &received[..],
        b"open",
        "already-delivered data still arrives"
    );
    match terminal {
        Some(StreamErrorIncoming::StreamTerminated { error_code }) => {
            // H3_REQUEST_CANCELLED: an abandoned send is a cancelled request.
            assert_eq!(error_code, 0x010c);
        }
        other => panic!("expected exactly one reset, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Connection termination
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_connection_close_is_observed_with_its_application_code() {
    let mut pair = Pair::new().await;
    let (client, mut server) = pair.split();

    let mut opener = client.opener();
    opener.close(h3::error::Code::H3_EXCESSIVE_LOAD, b"enough");

    let observed = within(
        "poll_accept_bidi",
        std::future::poll_fn(|cx| server.poll_accept_bidi(cx)),
    )
    .await;
    match observed {
        Err(ConnectionErrorIncoming::ApplicationClose { error_code }) => {
            assert_eq!(
                error_code,
                h3::error::Code::H3_EXCESSIVE_LOAD.value(),
                "the application code must survive the close"
            );
        }
        other => panic!("expected an application close, got {other:?}"),
    }
}

/// A terminated connection resolves everything outstanding rather than leaving it pending.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_terminated_connection_resolves_every_outstanding_operation() {
    let mut pair = Pair::new().await;
    let (mut client, mut server) = pair.split();

    let (_client_stream, mut server_stream) = {
        let mut opened = within(
            "poll_open_bidi",
            std::future::poll_fn(|cx| client.poll_open_bidi(cx)),
        )
        .await
        .expect("opening a stream");
        write_all(&mut opened, b"in flight").await;
        let accepted = within(
            "poll_accept_bidi",
            std::future::poll_fn(|cx| server.poll_accept_bidi(cx)),
        )
        .await
        .expect("accepting a stream");
        (opened, accepted)
    };

    let mut opener = client.opener();
    opener.close(h3::error::Code::H3_NO_ERROR, b"done");

    // The server's outstanding stream read resolves with a connection-category error, and its
    // opener does too. Neither is allowed to hang; `within` bounds both.
    let (_data, terminal) = read_to_end(&mut server_stream).await;
    assert!(
        terminal.is_some(),
        "an outstanding read must resolve when the connection ends"
    );

    let mut server_opener = server.opener();
    let opened = within(
        "poll_open_bidi",
        std::future::poll_fn(|cx| server_opener.poll_open_bidi(cx)),
    )
    .await;
    assert!(
        opened.is_err(),
        "an opener must not hand out streams on a connection that has ended"
    );
}
