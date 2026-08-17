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
/// accepted offer becomes as many records as the connection's outbound buffer will hold
/// rather than one, so the bound on what a full pass moves is this count times that buffer:
/// a few megabytes, written out as it goes rather than held.
const MAX_OFFERS: usize = 64;

/// Pulls from the HTTP/3 layer while the connection can carry more.
///
/// The connection is pumped *between* offers rather than only at the end, and the two pumps
/// are deliberately different operations.
///
/// Between offers it is [`pump::pump_buffered`]: it lets the connection read, and lets it write
/// when its output buffer has no room for another record, but leaves what a pass has produced
/// to accumulate. That is what turns a run of offers into a run of records in one write. It
/// used to be a flushing pump, because the layer below refused the next offer until the last
/// record had reached the byte stream; that rule is gone, and a flushing pump here would now
/// buy nothing while costing a write per record.
///
/// After the loop it is [`pump::pump`], which writes everything. That is the pass's obligation
/// to its driver: whatever the offers produced is on the byte stream by the time this returns,
/// because no other call is obliged to come along and move it.
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
        if !pump::pump_buffered(inner, cx) {
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
            // The end-of-stream marker must ride on the last slice that is actually written,
            // not on the last slice offered. A trailing empty slice would otherwise take the
            // marker, be refused -- the earlier slices in this offer may have filled the
            // connection's output buffer, and a refusal is what a full buffer answers -- and
            // contribute nothing to `total`, so the closure would answer `Accepted(offered)`
            // and the driver would commit the stream as ended while QMux had sent no FIN,
            // leaving the peer waiting for an end that never comes. The refusal is rarer than
            // it was, since the buffer now holds several records rather than one, which makes
            // this more worth computing rather than less: a hazard that fires occasionally is
            // one nothing reproduces. nghttp3 does not currently emit zero-length vectors, but
            // `ngnet-h3` does not rely on that, and the failure is silent, so the index is
            // computed rather than assumed.
            let last_written = slices
                .iter()
                .rposition(|slice| !slice.is_empty())
                .unwrap_or(0);
            let count = slices.len().max(1);

            for index in 0..count {
                let last = index == last_written;
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
                        // A short accept means the peer's window is exhausted or the
                        // connection's output buffer has no room for a further record --
                        // backpressure either way, because one call fills as many records as
                        // the buffer will hold. Nothing after it can be taken this pass, and
                        // offering it anyway would put the stream's bytes out of order.
                        //
                        // It used to mean a third thing, and that third thing is why this
                        // break was wrong for a while: while a call took one record, a large
                        // offer answered short with the buffer three-quarters empty, and this
                        // break stood the stream down over a record boundary. That reading is
                        // gone from the layer below rather than compensated for here, because
                        // the difference between a filled record and a shut window is visible
                        // only there.
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

    // Everything the pass produced is still sitting in the connection's outbound buffer, and
    // no other call is obliged to come along and move it. This is the forced flush of the whole
    // arrangement: the offers above left their records to accumulate on the promise that this
    // line writes them, and a pass that returned without it would leave a driver waiting on a
    // peer that had heard nothing.
    pump::pump(inner, cx);
}
