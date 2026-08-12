//! Turning what ngtcp2's handlers recorded into the events the HTTP/3 layer reads.

use bytes::Bytes;
use ngnet_h3::ErrorCode;
use ngnet_h3::http::QuicEvent;
use ngnet_h3::StreamId as H3StreamId;
use ngnet_quic::{ApplicationErrorCode, Directionality, Initiator, StreamId};

/// One thing that happened, in the shape the HTTP/3 layer expects.
///
/// Kept separate from the layer's own event type so the translation has somewhere to live
/// and can be tested without a connection.
#[derive(Debug)]
pub(crate) enum Recorded {
    /// Bytes arrived on a stream, and whether they end it.
    Data(StreamId, Vec<u8>, bool),
    /// The peer opened a stream.
    PeerOpened(StreamId),
    /// The peer acknowledged stream data. A delta, not a cumulative offset.
    Acked(StreamId, u64),
    /// The peer reset a stream it was sending on.
    Reset(StreamId, ApplicationErrorCode),
    /// The peer asked this endpoint to stop sending on a stream.
    StopSending(StreamId, ApplicationErrorCode),
    /// A stream ended, with a code per direction where each exists.
    Closed(
        StreamId,
        Option<ApplicationErrorCode>,
        Option<ApplicationErrorCode>,
    ),
    /// The connection ended.
    ConnectionClosed(Option<ApplicationErrorCode>),
}

/// Converts a QUIC stream identifier into the HTTP/3 layer's.
///
/// Both are the wire's 62-bit value; the two crates each wrap it in their own type because
/// neither depends on the other.
pub(crate) fn stream_id(id: StreamId) -> H3StreamId {
    H3StreamId::new(id.get()).expect("a valid QUIC stream identifier is a valid HTTP/3 one")
}

fn code(value: ApplicationErrorCode) -> ErrorCode {
    ErrorCode::new(value.get())
}

/// Whether a stream the peer opened needs announcing to the HTTP/3 layer.
///
/// Only bidirectional ones do. The layer's own documentation is explicit: peer-opened
/// *unidirectional* streams need no event, because nghttp3 reads the HTTP/3 stream-type
/// prefix itself to discover whether it is looking at the peer's control stream or one of
/// its QPACK streams. Announcing those would tell the layer to answer on a stream that
/// exists to be read.
pub(crate) fn announces(id: StreamId, local: Initiator) -> bool {
    id.directionality() == Directionality::Bidirectional && id.initiator() != local
}

/// Translates a record into the event the HTTP/3 layer reads.
///
/// Returns `None` for records that carry no event of their own — a stream limit rising
/// matters to this crate's own waiting, not to the layer.
pub(crate) fn into_event(record: Recorded, local: Initiator) -> Option<QuicEvent> {
    Some(match record {
        Recorded::Data(id, bytes, fin) => QuicEvent::Data {
            stream: stream_id(id),
            bytes: Bytes::from(bytes),
            fin,
        },
        Recorded::PeerOpened(id) => {
            if !announces(id, local) {
                return None;
            }
            QuicEvent::Accepted {
                stream: stream_id(id),
            }
        }
        Recorded::Acked(id, bytes) => QuicEvent::Released {
            stream: stream_id(id),
            bytes,
            // ngtcp2 reports acknowledgement and nothing else; it has no notion of handing
            // a buffer back for data that was cancelled, so this is never false.
            delivered: true,
        },
        Recorded::Reset(id, value) => QuicEvent::Reset {
            stream: stream_id(id),
            code: code(value),
        },
        Recorded::StopSending(id, value) => QuicEvent::StopSending {
            stream: stream_id(id),
            code: code(value),
        },
        Recorded::Closed(id, rx, tx) => QuicEvent::StreamClosed {
            stream: stream_id(id),
            rx_code: rx.map(code),
            tx_code: tx.map(code),
        },
        Recorded::ConnectionClosed(value) => QuicEvent::Closed {
            code: value.map(code),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(id: i64) -> StreamId {
        StreamId::new(id).expect("a valid identifier")
    }

    #[test]
    fn a_peer_opened_bidirectional_stream_is_announced() {
        // Stream 0 is client-initiated and bidirectional, so a server announces it.
        assert!(announces(sid(0), Initiator::Server));
    }

    #[test]
    fn a_locally_opened_stream_is_never_announced() {
        // The same stream, seen by the client that opened it. Announcing it would make the
        // layer answer on a stream it initiated.
        assert!(!announces(sid(0), Initiator::Client));
    }

    #[test]
    fn a_peer_opened_unidirectional_stream_is_not_announced() {
        // Stream 2 is client-initiated and unidirectional -- an HTTP/3 control or QPACK
        // stream. nghttp3 identifies those from their own type prefix, and announcing one
        // tells the layer to write to something that exists to be read.
        assert!(!announces(sid(2), Initiator::Server));
    }

    #[test]
    fn both_close_directions_survive_translation() {
        let event = into_event(
            Recorded::Closed(
                sid(0),
                Some(ApplicationErrorCode::new(0x1111)),
                Some(ApplicationErrorCode::new(0x2222)),
            ),
            Initiator::Client,
        )
        .expect("a close is an event");
        match event {
            QuicEvent::StreamClosed { rx_code, tx_code, .. } => {
                assert_eq!(rx_code.map(|c| c.get()), Some(0x1111));
                assert_eq!(tx_code.map(|c| c.get()), Some(0x2222));
            }
            other => panic!("expected a stream close, got {other:?}"),
        }
    }
}
