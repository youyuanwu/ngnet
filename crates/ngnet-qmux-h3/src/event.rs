//! Turning what the QMux layer reports into the events the HTTP/3 layer reads.

use bytes::Bytes;
use ngnet_h3::ErrorCode;
use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::http::QuicEvent;
use ngnet_qmux::io::Event;
use ngnet_qmux::{Directionality, Initiator, StreamId};

/// Converts a QMux stream identifier into the HTTP/3 layer's.
///
/// Both are the wire's 62-bit value; the two crates each wrap it in their own type because
/// neither depends on the other.
pub(crate) fn stream_id(id: StreamId) -> H3StreamId {
    H3StreamId::new(id.get()).expect("a valid QMux stream identifier is a valid HTTP/3 one")
}

/// Converts an HTTP/3 stream identifier into the QMux layer's.
///
/// The reverse direction cannot fail either, and does not: an identifier the HTTP/3 layer is
/// holding came from this crate in the first place.
pub(crate) fn qmux_stream(id: H3StreamId) -> StreamId {
    StreamId::new(id.get()).expect("an HTTP/3 stream identifier is a valid QMux one")
}

/// Whether a stream the peer opened needs announcing to the HTTP/3 layer.
///
/// Only bidirectional ones do. The layer's own documentation is explicit: peer-opened
/// *unidirectional* streams need no event, because nghttp3 reads the HTTP/3 stream-type
/// prefix itself to discover whether it is looking at the peer's control stream or one of
/// its QPACK streams. Announcing one would tell the layer to answer on a stream that exists
/// to be read, and the answer would be a protocol violation on a stream the peer will never
/// read.
///
/// The initiator test matters as much as the directionality one. QMux raises its open event
/// for peer opens only, but a translation that trusted that and dropped the check would
/// announce this endpoint's own streams the day the layer below started reporting them —
/// and the failure would look like the HTTP/3 layer answering its own requests.
pub(crate) fn announces(id: StreamId, local: Initiator) -> bool {
    id.directionality() == Directionality::Bidirectional && id.initiator() != local
}

/// Whether an event ends a stream, and so must start a batch of its own.
///
/// See `Inner::emitted_since_pending` for what goes wrong when one does not.
pub(crate) fn ends_a_stream(event: &QuicEvent) -> bool {
    matches!(
        event,
        QuicEvent::StreamClosed { .. } | QuicEvent::Closed { .. }
    )
}

/// Translates one QMux event into the event the HTTP/3 layer reads.
///
/// Returns `None` for events that carry nothing the layer acts on. A window or a stream
/// limit rising releases *this crate's* waiting and the QMux layer's own writers; the HTTP/3
/// layer has no representation for either and would only be woken to discover it.
pub(crate) fn translate(event: Event, local: Initiator) -> Option<QuicEvent> {
    Some(match event {
        Event::StreamData {
            stream_id: id,
            data,
            fin,
            ..
        } => QuicEvent::Data {
            stream: stream_id(id),
            // A zero-length delivery with `fin` is passed through rather than filtered. It
            // is how a peer that already sent everything ends a stream, and the HTTP/3 layer
            // requires it: suppressing it leaves a request body that never ends.
            bytes: Bytes::from(data),
            fin,
        },
        Event::StreamOpened { stream_id: id } => {
            if !announces(id, local) {
                return None;
            }
            QuicEvent::Accepted {
                stream: stream_id(id),
            }
        }
        Event::StreamClosed {
            stream_id: id,
            rx_app_error_code,
            tx_app_error_code,
        } => QuicEvent::StreamClosed {
            stream: stream_id(id),
            // `None` and `Some(0)` are different at both layers, and the distinction is
            // carried rather than normalised: the first is a stream that ended, the second
            // one that was reset with the code zero.
            rx_code: rx_app_error_code.map(ErrorCode::new),
            tx_code: tx_app_error_code.map(ErrorCode::new),
        },
        Event::StreamReset {
            stream_id: id,
            app_error_code,
            ..
        } => QuicEvent::Reset {
            stream: stream_id(id),
            code: ErrorCode::new(app_error_code),
        },
        Event::StopSending {
            stream_id: id,
            app_error_code,
        } => QuicEvent::StopSending {
            stream: stream_id(id),
            code: ErrorCode::new(app_error_code),
        },
        // Flow-control credit, stream limits, and the peer's transport parameters. All three
        // matter to the layer below and to this crate's own blocked writes; none is
        // something the HTTP/3 state machine can be told about.
        _ => return None,
    })
}
