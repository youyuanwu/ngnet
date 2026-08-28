//! Filling records from what the HTTP/3 layer has to write.

use core::task::Context;

use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::http::{StreamSource, WriteOutcome};
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
use ngnet_qmux::io::{AsyncByteStream, Clock, Error as LayerError, OUTBOUND_CEILING, StreamWrite};

use crate::connection::Inner;
use crate::event::qmux_stream;
use crate::pump;

/// How many offers one pass will take before returning.
///
/// Bounded so a layer with an endless supply — a large body, say — cannot keep this pass
/// from returning to the driver, which has acknowledgements and a peer to attend to. Each
/// accepted offer becomes as many records as the connection's outbound buffer will hold
/// rather than one, so the bound on what a full pass moves is this count times that buffer:
/// a few megabytes, written out as it goes rather than held.
const MAX_OFFERS: usize = 64;

/// Pulls from the HTTP/3 layer while the connection can carry more.
///
/// The connection is pumped only when another offer would exceed its bounded output room.
///
/// The event pass before transmit has already pumped and registered the transport read. A
/// pump before every offer would poll the same pending read while neither this task nor the
/// source can make inbound bytes appear. The loop instead compares the lower connection's
/// bounded output against the room one record requires and calls [`pump::pump_buffered`] only
/// to make room. Records otherwise accumulate until the separate `QuicConnection::poll_flush`
/// suspension hook writes everything and registers the read again before the task can park.
pub(crate) fn drain<S: AsyncByteStream, C: Clock, Src: StreamSource>(
    inner: &mut Inner<S, C>,
    cx: &mut Context<'_>,
    source: &mut Src,
) {
    for _ in 0..MAX_OFFERS {
        // Ends the pass when the connection has ended, and equally when it can take no
        // further record: offering into a buffer with no room collects nothing but
        // `Blocked`, which would tell the layer its streams are stalled when only the socket
        // is, and take them out of the running until something else woke them.
        //
        // The event pass immediately before this one and the suspension flush immediately
        // after it own transport progress. This loop needs a pump only when a large preceding
        // offer consumed the remaining output room within the pass. The constants are the
        // exact predicate used by the lower connection's `room_for_record`; testing it here
        // prevents a buffer-full refusal from being reported as peer flow-control
        // backpressure.
        if inner.conn.queued_output() + DEFAULT_MAX_RECORD_SIZE as usize > OUTBOUND_CEILING
            && !pump::pump_buffered(inner, cx)
        {
            break;
        }

        let conn = &mut inner.conn;
        let mut released: Option<(H3StreamId, u64)> = None;
        let mut failure: Option<LayerError> = None;

        let offered = source.write_next(&mut |stream, slices, fin| {
            let id = qmux_stream(stream);

            // An offer of nothing, with no end-of-stream to carry, is answered without
            // touching the connection: there is nothing to write and nothing to retry.
            //
            // Conditioned on `!fin` and it must stay so. An otherwise-empty offer that carries
            // the marker still has to produce its record, because that record is the only way
            // a stream which has finished writing is ever ended; short-circuiting it would
            // leave the peer waiting out an idle timeout for a body that had in fact arrived.
            if !fin && slices.iter().all(|slice| slice.is_empty()) {
                return WriteOutcome::Accepted(0);
            }

            // One call for the whole offer, where this used to be a call per slice. A slice
            // boundary is then no longer a record boundary: the fragments are concatenated
            // into as few records as the maximum record size permits, which for the ordinary
            // two-fragment offer -- a request's headers and the first of its body -- is one
            // record where it used to be two.
            //
            // Three properties came out of the per-slice loop and are worth naming, because
            // each was computed there and is now structural.
            //
            // The **end-of-stream marker** rides the record that takes the last byte of the
            // whole offer, and only when the whole offer was taken. The loop used to find the
            // last non-empty slice by index, because a trailing empty slice would otherwise
            // take the marker, be refused, and have the driver commit the stream as ended
            // while QMux had sent no FIN. An empty fragment is now not submitted at all and
            // there is nothing for the marker to land on wrongly; dwnx applies it only when
            // the data it was handed fits entirely.
            //
            // The **refusals** are still only an offer's answer while nothing has been taken.
            // The layer below reports a non-zero count in preference to any refusal, so a
            // `Blocked` or a `Closed` arriving here means nothing was packed -- which is what
            // makes returning it safe, since a refusal that lost a count would have the layer
            // offer those bytes again and the stream would carry them twice.
            //
            // The **release and the verdict** are one number from one place, which is the same
            // property the loop maintained and the reason it kept a running total: a second
            // source for either is a second chance to disagree with the first, and disagreeing
            // in one direction holds the application's buffers for the connection's life while
            // disagreeing in the other frees memory nghttp3 is still reading through.
            match conn.try_write_stream_vectored(id, slices, fin) {
                Ok(StreamWrite::Accepted(taken)) => {
                    if taken > 0 {
                        released = Some((stream, taken as u64));
                    }
                    WriteOutcome::Accepted(taken)
                }
                Ok(StreamWrite::Blocked) => WriteOutcome::Blocked,
                Ok(StreamWrite::Closed) => WriteOutcome::Gone,
                Err(error) => {
                    // Fatal, and reported as a refusal only because the closure has no way to
                    // say so: the connection is ended below, before the pass takes another
                    // offer, so the re-offer this invites never happens.
                    failure = Some(error);
                    WriteOutcome::Blocked
                }
            }
        });

        if let Some((stream, bytes)) = released {
            inner.record_released(stream, bytes);
        }
        if let Some(error) = failure {
            inner.end(&error);
            break;
        }
        if !offered {
            break;
        }
    }

    // Keep a sub-ceiling tail across the driver's internal passes. The event poll has already
    // registered the read wake; the driver's explicit suspension hook flushes the tail and
    // registers again before a real park, and capacity pressure still writes above.
}
