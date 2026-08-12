//! Filling packets from what the HTTP/3 layer has to write.

use ngnet_h3::http::{StreamSource, WriteOutcome as H3WriteOutcome};
use ngnet_quic::endpoint::DetachedConnection;
use ngnet_quic::{ErrorKind as QuicErrorKind, StreamId, StreamWrite, TlsSession};

use crate::connection::State;
use crate::error::{Error, Result};
use crate::pump::MAX_DATAGRAM;

/// Pulls from the HTTP/3 layer while the connection can carry more.
///
/// Each accepted offer does two things at once: it consumes stream bytes *and* produces a
/// datagram. The abstraction's verdict has no room for the datagram, so it is queued from
/// inside the closure and the accepted count is reported back.
pub(crate) fn drain<S: TlsSession, Src: StreamSource>(
    detached: &mut DetachedConnection<S>,
    state: &mut State,
    source: &mut Src,
) -> Result<()> {
    let mut failure: Option<Error> = None;
    let mut buffer = vec![0u8; MAX_DATAGRAM];

    // Bounded so a layer with an endless supply cannot keep this pass from returning.
    for _ in 0..64 {
        // Room is checked *before* asking for an offer, never after taking one. A datagram
        // that has been produced cannot be withdrawn: the connection has already accounted
        // for the stream bytes in it, so re-offering them would send them twice and
        // discarding it would lose them until a retransmission timer noticed.
        if !detached.outbound_has_room() {
            break;
        }

        let now = detached.now();
        let conn = &mut detached.conn;
        let mut produced: Option<Vec<u8>> = None;

        let offered = source.write_next(&mut |stream, slices, fin| {
            let Ok(id) = StreamId::new(stream.get()) else {
                return H3WriteOutcome::Gone;
            };
            let ranges: Vec<&[u8]> = slices.iter().map(|s| &s[..]).collect();

            match conn.write_stream_vectored(&mut buffer, id, &ranges, fin, now) {
                Ok(StreamWrite::Datagram { len, accepted }) => {
                    produced = Some(buffer[..len].to_vec());
                    H3WriteOutcome::Accepted(accepted)
                }
                // Every blocked condition is the same to the layer: nothing can be taken for
                // this stream now, so it is set aside and offered again later.
                Ok(
                    StreamWrite::Blocked
                    | StreamWrite::StreamBlocked
                    | StreamWrite::ConnectionBlocked
                    | StreamWrite::Idle,
                ) => H3WriteOutcome::Blocked,
                // A stream whose write side is finished will never take more. Saying so lets
                // the layer stop offering rather than retrying forever.
                Err(err) if err.kind() == QuicErrorKind::StreamClosed => H3WriteOutcome::Gone,
                Err(err) => {
                    failure = Some(Error::transport(err));
                    H3WriteOutcome::Blocked
                }
            }
        });

        if let Some(datagram) = produced.take() {
            detached.send(datagram);
        }
        if let Some(err) = failure.take() {
            state.closed = true;
            return Err(err);
        }
        if !offered {
            break;
        }
    }
    Ok(())
}
