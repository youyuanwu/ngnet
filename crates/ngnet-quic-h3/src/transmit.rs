//! Filling packets from what the HTTP/3 layer has to write.

use ngnet_h3::http::{StreamSource, WriteOutcome as H3WriteOutcome};
use ngnet_quic::endpoint::DetachedConnection;
use ngnet_quic::{ErrorKind as QuicErrorKind, Session, StreamId, StreamWrite};

use crate::connection::{Shared, State};
use crate::error::{Error, Result};
use crate::pump::MAX_DATAGRAM;

/// Pulls from the HTTP/3 layer while the connection can carry more.
///
/// Each accepted offer does two things at once: it consumes stream bytes *and* produces a
/// datagram. The abstraction's verdict has no room for the datagram, so it is queued from
/// inside the closure and the accepted count is reported back.
pub(crate) fn drain<S: Session, Src: StreamSource>(
    detached: &mut DetachedConnection<S>,
    shared: &Shared,
    state: &mut State,
    source: &mut Src,
    cx: &core::task::Context<'_>,
) -> Result<()> {
    let mut failure: Option<Error> = None;
    let mut blocked = false;
    #[cfg(feature = "diagnostics")]
    let role = detached.conn.role();
    #[cfg(feature = "diagnostics")]
    let connection_id = detached.conn.diagnostic_id();

    #[cfg(feature = "diagnostics")]
    if state.capacity_parked && detached.poll_outbound_capacity(cx.waker()) {
        ngnet_quic::diagnostics::record_retry(connection_id, role);
        state.capacity_parked = false;
    }

    // Bounded so a layer with an endless supply cannot keep this pass from returning.
    for _ in 0..64 {
        // Room is checked *before* asking for an offer, never after taking one. A datagram
        // that has been produced cannot be withdrawn: the connection has already accounted
        // for the stream bytes in it, so re-offering them would send them twice and
        // discarding it would lose them until a retransmission timer noticed.
        if !detached.poll_outbound_capacity(cx.waker()) {
            #[cfg(feature = "diagnostics")]
            {
                state.capacity_parked = true;
            }
            break;
        }

        let now = detached.now();
        // Write directly into the buffer that will be handed over -- the same reused buffer
        // `produce` uses, and for the same reason: the endpoint's queue takes ownership, so
        // one owned allocation per datagram is forced, but the copy out of a scratch that
        // used to sit beside it is not.
        let mut datagram = core::mem::take(&mut state.scratch);
        datagram.resize(MAX_DATAGRAM, 0);
        let conn = &mut detached.conn;
        let mut produced_len: Option<usize> = None;
        let mut released: Option<(StreamId, usize)> = None;

        let offered = source.write_next(&mut |stream, slices, fin| {
            let Ok(id) = StreamId::new(stream.get()) else {
                return H3WriteOutcome::Gone;
            };

            // The source's slices are already `IoSlice`s, and `write_stream_vectored` now
            // takes them as such -- so they pass straight through with no per-offer vector
            // to collect them into.
            match conn.write_stream_vectored(&mut datagram, id, slices, fin, now) {
                Ok(StreamWrite::Datagram { len, accepted }) => {
                    produced_len = Some(len);
                    if accepted > 0 {
                        released = Some((id, accepted));
                    } else {
                        // A zero-byte acceptance here is a *serialised* zero-length STREAM
                        // frame, which ngtcp2 writes only for an offer carrying nothing but
                        // `fin`. The stream really did end, so the offer is committed.
                        blocked = true;
                    }
                    H3WriteOutcome::Accepted(accepted)
                }
                // The packet carried only transport work -- an acknowledgement, most often
                // -- and no STREAM frame at all. Let it reach the peer and wait for an
                // enabling event before offering the same stream prefix again; retrying
                // inside this pass cannot create stream capacity.
                //
                // Reported as `Blocked` rather than `Accepted(0)`, and the difference is
                // load-bearing for a `fin`-only offer: the layer commits an acceptance and
                // marks the stream ended, but abandons a block and offers it again. Ending
                // a stream on a packet that never carried the FIN leaves the peer waiting
                // for an end ngtcp2 has nothing in flight to retransmit.
                Ok(StreamWrite::DatagramWithoutStream { len }) => {
                    produced_len = Some(len);
                    blocked = true;
                    H3WriteOutcome::Blocked
                }
                // Every blocked condition is the same to the layer: nothing can be taken for
                // this stream now, so it is set aside and offered again later.
                Ok(
                    StreamWrite::Blocked
                    | StreamWrite::StreamBlocked
                    | StreamWrite::ConnectionBlocked
                    | StreamWrite::Idle,
                ) => {
                    blocked = true;
                    H3WriteOutcome::Blocked
                }
                // A stream whose write side is finished will never take more. Saying so lets
                // the layer stop offering rather than retrying forever.
                Err(err) if err.kind() == QuicErrorKind::StreamClosed => H3WriteOutcome::Gone,
                Err(err) => {
                    failure = Some(Error::transport(err));
                    H3WriteOutcome::Blocked
                }
            }
        });

        if let Some(len) = produced_len {
            datagram.truncate(len);
            #[cfg(feature = "diagnostics")]
            ngnet_quic::diagnostics::record_packet(connection_id, role, released.is_some());
            detached.send(datagram);
        } else {
            // No datagram was produced, so this is untouched storage: keep it for reuse
            // rather than dropping and reallocating one next time.
            datagram.clear();
            state.scratch = datagram;
        }
        // The transport has taken a copy of these bytes, so the layer's own buffer is its
        // again. Reporting it here rather than on acknowledgement is what keeps a body in
        // flight from being held twice -- see `RETAINS_BUFFERS`.
        if let Some((stream, bytes)) = released.take() {
            #[cfg(feature = "diagnostics")]
            ngnet_quic::diagnostics::record_release(connection_id, role, bytes);
            shared.record_released(stream, bytes as u64);
        }
        if let Some(err) = failure.take() {
            state.closed = true;
            return Err(err);
        }
        if blocked {
            break;
        }
        if !offered {
            break;
        }
    }
    Ok(())
}
