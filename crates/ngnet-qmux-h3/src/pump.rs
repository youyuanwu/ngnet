//! The two operations that drive the connection.
//!
//! Called from every entry point, for the reason set out in the crate documentation: the
//! HTTP/3 driver's first action is to open three unidirectional streams, and it reaches
//! nothing else until it has them. Stream capacity arrives in the peer's transport
//! parameters, and those arrive only if something reads the byte stream. If bytes only moved
//! in `poll_transmit`, the first flight would never be sent and the connection would
//! deadlock before it began.
//!
//! # Why there are two
//!
//! There used to be one, and the reason it was enough was a rule the layer below no longer
//! keeps: QMux allowed one record to be outstanding at a time, so until the record an offer
//! produced had reached the byte stream, the next offer was refused. Pumping between offers
//! was therefore what let a pass move more than a single record, and it flushed because
//! flushing was the only way to make room.
//!
//! The layer below now accumulates records up to a documented ceiling and writes them
//! together. The pump between offers must accumulate with it — [`pump_buffered`] — and the one
//! at the end of the turn must not — [`pump`]. Keeping a single flushing pump would have left
//! the driver turn paying one write per record with the connection underneath it coalescing
//! perfectly, which is a null result that looks like a working change.

use core::task::{Context, Poll};

use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::connection::Inner;

/// Drives the connection one pass, writing out everything it produced.
///
/// The pass is the QMux layer's own: produce whatever the state machine owes, write it, then
/// read whatever has arrived. `cx` is registered on both halves of the byte stream, so the
/// caller is polled again when either becomes ready.
///
/// `true` means everything queued has reached the byte stream. `false` means either the
/// connection has ended — the ending is latched on `inner`, not returned, because every
/// caller here reports it in its own shape — or the byte stream is not taking more yet.
///
/// **This is the forced form, and it is what discharges the obligation
/// [`pump_buffered`] leaves.** Every caller of it is a place the HTTP/3 driver may stop
/// polling this connection: the end of a transmit pass, an event poll that returns nothing,
/// an open that is about to be reported. Output left behind at one of those waits for a pass
/// that nothing is obliged to make, and a connection that has said everything it means to say
/// and written none of it is indistinguishable, from the peer, from one that has hung.
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

/// Drives the connection one pass, leaving what it produced to accumulate.
///
/// The form for the middle of a transmit pass, where the caller is going to offer again and
/// will finish with [`pump`]. The QMux layer writes only when its output buffer can no longer
/// take another record, so a run of offers becomes a run of records in one write instead of a
/// write apiece. Since a single offer now fills records until the buffer or the peer's window
/// stops it, that run is as often one large offer's worth of records as several streams'.
///
/// `true` here means the connection can take another record, which is the question an offer
/// loop is asking; `false` means it cannot until the byte stream takes some of what is already
/// buffered, or that the connection has ended. Both answers end the pass, for the same reason
/// they always did: offering into a connection that will refuse collects a run of spurious
/// `Blocked` verdicts and teaches the HTTP/3 layer that its streams are stalled when only the
/// socket is.
///
/// This distinction is the whole of the write-count reduction, and it is invisible from below.
/// A connection that coalesced perfectly would still write once per record if every pump
/// between two offers flushed it, and every test written against the connection alone would
/// still pass. The guard that can see it is at the driver level:
/// `tests/ngnet-qmux-h3-tests/tests/driver_writes.rs`.
///
/// [`Connection::try_write_stream`]: ngnet_qmux::io::Connection::try_write_stream
pub(crate) fn pump_buffered<S: AsyncByteStream, C: Clock>(
    inner: &mut Inner<S, C>,
    cx: &mut Context<'_>,
) -> bool {
    if inner.has_ended() {
        return false;
    }
    match inner.conn.poll_pump_buffered(cx) {
        Poll::Ready(Ok(())) => true,
        Poll::Pending => false,
        Poll::Ready(Err(error)) => {
            inner.end(&error);
            false
        }
    }
}
