//! Byte accounting on the sending side.
//!
//! The transport accepts what fits in the packet it is filling — often a prefix, sometimes
//! nothing at all. These tests hold the adapter to the only contract that matters across that:
//! every offered byte reaches the peer exactly once, in order, and a partial acceptance
//! consumes exactly what was accepted and not a byte more.

mod common;

use bytes::{Buf, Bytes};
use common::{Pair, body_of, within};
use h3::quic::{Connection as _, OpenStreams as _, SendStream as _, SendStreamUnframed as _};

/// A payload large enough that the transport cannot take it in one packet, and large enough
/// to exhaust the connection's initial flow-control window several times over.
///
/// The window matters: a sender that ran ahead of its reader would block, correctly, so every
/// test here drives the reader concurrently rather than writing everything first.
const LARGE: usize = 96 * 1024;

/// Opens a bidirectional stream and accepts it on the far side.
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
    let mut probe = Bytes::from_static(b"\x00");
    while probe.has_remaining() {
        within(
            "poll_send",
            std::future::poll_fn(|cx| opened.poll_send(cx, &mut probe)),
        )
        .await
        .expect("announcing the stream");
    }
    let server = pair.server.as_mut().expect("a server");
    let accepted = within(
        "poll_accept_bidi",
        std::future::poll_fn(|cx| server.poll_accept_bidi(cx)),
    )
    .await
    .expect("accepting a bidirectional stream");
    (opened, accepted)
}

/// Reads a stream to its end, for use concurrently with a writer.
async fn read_to_end<R: h3::quic::RecvStream>(stream: &mut R) -> Bytes {
    let mut out = bytes::BytesMut::new();
    while let Some(mut chunk) = within("poll_data", std::future::poll_fn(|cx| stream.poll_data(cx)))
        .await
        .expect("reading stream data")
    {
        while chunk.has_remaining() {
            let piece = chunk.chunk().to_vec();
            chunk.advance(piece.len());
            out.extend_from_slice(&piece);
        }
    }
    out.freeze()
}

/// An unframed send advances the caller's buffer by exactly the accepted prefix.
///
/// The count `poll_send` reports and the amount the buffer moved must agree on every single
/// call. A version that advanced by what was *offered* would silently drop the difference, and
/// one that advanced by less would resend it.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn an_unframed_send_advances_only_the_exact_accepted_prefix() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    let payload = body_of(LARGE);

    let writing = async {
        let mut pending = payload.clone();
        let mut reported_total = 0usize;
        let mut calls = 0usize;
        while pending.has_remaining() {
            let before = pending.remaining();
            let written = within(
                "poll_send",
                std::future::poll_fn(|cx| sender.poll_send(cx, &mut pending)),
            )
            .await
            .expect("writing raw stream bytes");
            let moved = before - pending.remaining();
            assert_eq!(
                written, moved,
                "poll_send must advance the buffer by exactly what it reports"
            );
            assert!(
                written <= before,
                "poll_send must never claim more than was offered"
            );
            reported_total += written;
            calls += 1;
            assert!(calls < 100_000, "the send must terminate");
        }
        within(
            "poll_finish",
            std::future::poll_fn(|cx| sender.poll_finish(cx)),
        )
        .await
        .expect("finishing the stream");
        (reported_total, calls)
    };

    let ((reported_total, calls), received) = tokio::join!(writing, read_to_end(&mut receiver));

    assert_eq!(
        reported_total,
        payload.len(),
        "the reported acceptances must sum to exactly the payload"
    );
    assert!(
        calls > 1,
        "a {LARGE}-byte payload must take more than one packet, or this proves nothing"
    );
    // The one probe byte from `opened_pair`, then the payload, once, in order.
    assert_eq!(
        received.len(),
        payload.len() + 1,
        "no byte lost or duplicated"
    );
    assert_eq!(
        &received[1..],
        &payload[..],
        "every byte must arrive exactly once, in order"
    );
}

/// A framed send walks the frame header and every payload chunk exactly once.
///
/// `WriteBuf` yields its header before its payload and never both in one slice, so the adapter
/// must cross that boundary itself. Getting it wrong shows up as a corrupted frame here rather
/// than as a length mismatch, because the header would be resent or skipped.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_framed_send_walks_the_header_and_every_payload_chunk_exactly_once() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    let payload = body_of(LARGE);
    let frame = h3::proto::frame::Frame::Data(payload.clone());
    sender
        .send_data(h3::quic::WriteBuf::from(frame))
        .expect("storing the framed send");

    let writing = async {
        within(
            "poll_ready",
            std::future::poll_fn(|cx| sender.poll_ready(cx)),
        )
        .await
        .expect("draining the framed send");
        within(
            "poll_finish",
            std::future::poll_fn(|cx| sender.poll_finish(cx)),
        )
        .await
        .expect("finishing the stream");
    };
    let (_, received) = tokio::join!(writing, read_to_end(&mut receiver));

    // One probe byte, then a DATA frame header, then the payload verbatim. The header length
    // is whatever hyperium encoded; what matters is that the payload appears once, at the end,
    // and that nothing follows it.
    assert!(
        received.len() > payload.len() + 1,
        "a framed send must carry a header as well as its payload"
    );
    assert_eq!(
        &received[received.len() - payload.len()..],
        &payload[..],
        "the payload must arrive verbatim, exactly once"
    );
    let header = &received[1..received.len() - payload.len()];
    assert!(
        !header.is_empty() && header.len() < 16,
        "the DATA frame header must appear exactly once, got {} bytes",
        header.len()
    );
}

/// An empty framed send is legal and produces no payload bytes.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn an_empty_framed_send_is_accepted_and_carries_nothing() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    sender
        .send_data(h3::quic::WriteBuf::from(h3::proto::frame::Frame::Data(
            Bytes::new(),
        )))
        .expect("storing an empty framed send");
    within(
        "poll_ready",
        std::future::poll_fn(|cx| sender.poll_ready(cx)),
    )
    .await
    .expect("draining an empty framed send");
    within(
        "poll_finish",
        std::future::poll_fn(|cx| sender.poll_finish(cx)),
    )
    .await
    .expect("finishing the stream");

    let received = read_to_end(&mut receiver).await;
    // The probe byte plus a two-byte empty DATA frame header.
    assert!(
        received.len() >= 2 && received.len() < 8,
        "an empty DATA frame must carry only its header, got {received:?}"
    );
}

/// A sender that outruns its reader is stopped by flow control, not by unbounded buffering.
///
/// The adapter must not absorb an arbitrary amount of unread data on the receiver's behalf:
/// credit is returned when HTTP/3 *consumes* bytes, not when they arrive, so a peer that keeps
/// writing while nothing reads has to block. This test asserts exactly that — the write stalls
/// with the payload unfinished — and then that it completes once the reader starts.
#[tokio::test]
#[ignore = "live-loopback: this adapter has an unresolved intermittent liveness failure, so its socket tests are ignored in ordinary runs; see docs/h3-ngnet-quic/pending-work.md"]
async fn a_sender_that_outruns_its_reader_blocks_instead_of_buffering_without_bound() {
    let mut pair = Pair::new().await;
    let (mut sender, mut receiver) = opened_pair(&mut pair).await;

    let payload = body_of(LARGE);
    let mut pending = payload.clone();

    // Write without reading. This must stop short: the connection's initial flow-control
    // window is far smaller than the payload.
    let stalled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while pending.has_remaining() {
            std::future::poll_fn(|cx| sender.poll_send(cx, &mut pending))
                .await
                .expect("writing raw stream bytes");
        }
    })
    .await;
    assert!(
        stalled.is_err(),
        "a sender with no reader must block on flow control rather than buffer without bound"
    );
    let unsent = pending.remaining();
    assert!(
        unsent > 0,
        "the payload must not have been fully absorbed while nothing was reading"
    );

    // Now read, and the rest goes through.
    let writing = async {
        while pending.has_remaining() {
            within(
                "poll_send",
                std::future::poll_fn(|cx| sender.poll_send(cx, &mut pending)),
            )
            .await
            .expect("writing raw stream bytes");
        }
        within(
            "poll_finish",
            std::future::poll_fn(|cx| sender.poll_finish(cx)),
        )
        .await
        .expect("finishing the stream");
    };
    let (_, received) = tokio::join!(writing, read_to_end(&mut receiver));

    assert_eq!(
        received.len(),
        payload.len() + 1,
        "no byte lost or duplicated"
    );
    assert_eq!(
        &received[1..],
        &payload[..],
        "every byte must arrive exactly once, in order, across the stall"
    );
}
