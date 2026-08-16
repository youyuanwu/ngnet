//! The one operation that drives the connection.
//!
//! Called from every entry point, for the reason set out in the crate documentation: the
//! HTTP/3 driver's first action is to open three unidirectional streams, and it reaches
//! nothing else until it has them. Stream capacity arrives in the peer's transport
//! parameters, and those arrive only if something reads the byte stream. If bytes only moved
//! in `poll_transmit`, the first flight would never be sent and the connection would
//! deadlock before it began.

use core::task::{Context, Poll};

use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::connection::Inner;

/// Drives the connection one pass, and reports whether it is caught up.
///
/// The pass is the QMux layer's own: flush whatever is queued, produce whatever the state
/// machine now owes, then read whatever has arrived. `cx` is registered on both halves of
/// the byte stream, so the caller is polled again when either becomes ready.
///
/// `true` means everything queued has reached the byte stream. `false` means either the
/// connection has ended — the ending is latched on `inner`, not returned, because every
/// caller here reports it in its own shape — or the byte stream is not taking more yet.
///
/// The distinction is not cosmetic. [`Connection::try_write_stream`] refuses every offer
/// while a record is still outstanding, so a transmit pass that kept offering after a
/// `false` would collect a run of spurious `Blocked` verdicts and teach the HTTP/3 layer
/// that streams are stalled when only the socket is.
///
/// [`Connection::try_write_stream`]: ngnet_qmux::io::Connection::try_write_stream
pub(crate) fn pump<S: AsyncByteStream, C: Clock>(
    inner: &mut Inner<S, C>,
    cx: &mut Context<'_>,
) -> bool {
    if inner.has_ended() {
        return false;
    }
    match inner.conn.poll_pump(cx) {
        Poll::Ready(Ok(())) => true,
        Poll::Pending => false,
        Poll::Ready(Err(error)) => {
            inner.end(&error);
            false
        }
    }
}
