//! Filling records from what the HTTP/3 layer has to write.

use core::task::Context;

use ngnet_h3::StreamId as H3StreamId;
use ngnet_h3::http::{StreamSource, WriteOutcome};
use ngnet_qmux::io::{AsyncByteStream, Clock, Error as LayerError, StreamWrite};

use crate::connection::Inner;
use crate::event::qmux_stream;
use crate::pump;

/// How many offers one pass will take before returning.
///
/// Bounded so a layer with an endless supply — a large body, say — cannot keep this pass
/// from returning to the driver, which has acknowledgements and a peer to attend to. Each
/// accepted offer becomes at most one record, so a full pass moves on the order of a
/// megabyte and then yields.
const MAX_OFFERS: usize = 64;

/// Pulls from the HTTP/3 layer while the connection can carry more.
///
/// The connection is pumped *between* offers rather than only at the end. QMux allows one
/// record to be outstanding at a time: until the record an offer produced has reached the
/// byte stream, the next offer is refused. Pumping between them is therefore what makes a
/// pass move more than a single record, and skipping it would turn a large body into one
/// record per wakeup.
pub(crate) fn drain<S: AsyncByteStream, C: Clock, Src: StreamSource>(
    inner: &mut Inner<S, C>,
    cx: &mut Context<'_>,
    source: &mut Src,
) {
    for _ in 0..MAX_OFFERS {
        // Ends the pass when the connection has ended, and equally when the byte stream is
        // not taking more: offering into a full outbound buffer collects nothing but
        // `Blocked`, which would tell the layer its streams are stalled when only the socket
        // is, and take them out of the running until something else woke them.
        if !pump::pump(inner, cx) {
            break;
        }

        let conn = &mut inner.conn;
        let mut released: Option<(H3StreamId, u64)> = None;
        let mut failure: Option<LayerError> = None;

        let offered = source.write_next(&mut |stream, slices, fin| {
            let id = qmux_stream(stream);

            // An offer of nothing, with no end-of-stream to carry, is answered without
            // touching the connection: there is nothing to write and nothing to retry.
            if !fin && slices.iter().all(|slice| slice.is_empty()) {
                return WriteOutcome::Accepted(0);
            }

            let mut total: usize = 0;
            let mut refusal: Option<WriteOutcome> = None;
            let count = slices.len().max(1);

            for index in 0..count {
                let last = index + 1 == count;
                // The end-of-stream marker rides on the final slice and only there. QMux
                // applies it only when it takes the whole of what it was offered, so a
                // partial accept cannot end the stream early.
                let end = fin && last;
                let slice: &[u8] = slices.get(index).map_or(&[], |slice| &slice[..]);
                if slice.is_empty() && !end {
                    continue;
                }

                match conn.try_write_stream(id, slice, end) {
                    Ok(StreamWrite::Accepted(taken)) => {
                        total += taken;
                        // A short accept means the record filled or the peer's window did.
                        // Nothing after it can be taken this pass, and offering it anyway
                        // would put the stream's bytes out of order.
                        if taken < slice.len() {
                            break;
                        }
                    }
                    // Blocked and closed are only the offer's answer while nothing has been
                    // taken. Once bytes are accepted the layer must hear the count, or it
                    // would offer them a second time and the stream would carry them twice.
                    Ok(StreamWrite::Blocked) => {
                        if total == 0 {
                            refusal = Some(WriteOutcome::Blocked);
                        }
                        break;
                    }
                    Ok(StreamWrite::Closed) => {
                        if total == 0 {
                            refusal = Some(WriteOutcome::Gone);
                        }
                        break;
                    }
                    Err(error) => {
                        failure = Some(error);
                        if total == 0 {
                            refusal = Some(WriteOutcome::Blocked);
                        }
                        break;
                    }
                }
            }

            if let Some(refusal) = refusal {
                return refusal;
            }
            // The release and the verdict are computed here, from the same running total, and
            // this is the only place either is produced. That is what makes every accepted
            // byte released exactly once: a second source for either number is a second
            // chance to disagree with the first, and disagreeing in one direction holds the
            // application's buffers for the connection's life while disagreeing in the other
            // frees memory nghttp3 is still reading through.
            if total > 0 {
                released = Some((stream, total as u64));
            }
            WriteOutcome::Accepted(total)
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

    // Whatever the last offer produced is still sitting in the outbound buffer, and no other
    // call is obliged to come along and move it.
    pump::pump(inner, cx);
}
