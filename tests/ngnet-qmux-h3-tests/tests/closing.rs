//! A connection closed through the HTTP/3 layer, watched from the transport underneath.
//!
//! The far end here is a bare QMux connection rather than a second HTTP/3 endpoint, and that
//! is the point: it can be asked what actually arrived. A test written between two HTTP/3
//! endpoints cannot tell a close that was written from a close that was encoded and dropped,
//! because both leave the other end quiet — one because it was told, and one because it is
//! waiting out an idle timeout with nothing to report.
//!
//! The failure this guards against is the shape of the join. `QuicConnection::close` has no
//! context to park on, so it can only record; the HTTP/3 driver then calls it and returns
//! without polling the transport again. If this crate's own connection future did not run a
//! tail, the close would sit in a buffer for the rest of the process's life.

use core::future::poll_fn;

use ngnet_qmux::io::{Config, Connection as LayerConnection, Error as LayerError};
use ngnet_qmux::{CloseKind, CloseReason};
use ngnet_qmux_h3_tests::{LIMIT, Payload, drain, memory_streams};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// What HTTP/3 closes with when it has nothing to complain about: H3_NO_ERROR.
const H3_NO_ERROR: u64 = 0x100;

#[tokio::test]
async fn a_close_through_the_http3_layer_reaches_the_peer() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let mut peer = LayerConnection::server(server_io, clock.clone(), Config::new())
                .expect("a bare QMux server");

            let (sender, connection) = ngnet_qmux_h3::connect::<_, _, Payload>(client_io, clock)
                .expect("starting the client");
            let driving = tokio::task::spawn_local(connection);

            // Dropped without making a request. The driver has nothing left to do, which is
            // the shortest path to the close this test is about: it opens its control
            // streams, finds no handles and no exchanges, and closes.
            drop(sender);

            // Everything up to the close is ordinary traffic -- the peer's own announcement
            // and the client's control streams -- and none of it is what this test is
            // watching for. The close arrives as an ending, because that is what a close is
            // to the connection that receives it.
            let watching = async {
                loop {
                    match poll_fn(|cx| peer.poll_next_event(cx)).await {
                        Ok(_event) => {}
                        Err(error) => return error,
                    }
                }
            };

            let ending: LayerError = timeout(LIMIT, watching)
                .await
                .expect("the peer must be told, rather than left to time out");
            drop(driving);

            let reason: &CloseReason = ending.close_reason().expect(
                "the ending must carry the peer's close, not merely be a byte stream that \
                 stopped: a close nobody wrote and a socket that vanished are the same thing \
                 to a reader, and only one of them is this crate doing its job",
            );
            assert_eq!(
                reason.kind(),
                CloseKind::Application,
                "an HTTP/3 close is an application close; a transport close would mean the \
                 code came from somewhere other than the layer that asked for it",
            );
            assert_eq!(
                reason.error_code(),
                H3_NO_ERROR,
                "and it must carry the code the HTTP/3 layer chose",
            );
        })
        .await;
}

/// The same connection, with the tail never run.
///
/// Not a test of a bug but of the reasoning behind the design: [`ngnet_qmux_h3::Connection`]
/// is what writes the close, and a caller who drops it instead gets exactly the silence this
/// crate exists to avoid. Asserting it keeps the tail from being quietly removed as
/// redundant.
#[tokio::test]
async fn a_connection_dropped_before_its_tail_tells_the_peer_nothing() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let mut peer = LayerConnection::server(server_io, clock.clone(), Config::new())
                .expect("a bare QMux server");

            let (sender, connection) = ngnet_qmux_h3::connect::<_, _, Payload>(client_io, clock)
                .expect("starting the client");
            drop(connection);
            drop(sender);

            // Nothing was ever polled, so nothing was ever written. The peer has no close,
            // and -- because the byte stream was dropped rather than shut down -- nothing at
            // all.
            let quiet = timeout(
                core::time::Duration::from_millis(50),
                poll_fn(|cx| peer.poll_next_event(cx)),
            )
            .await;
            assert!(
                quiet.is_err(),
                "a connection nobody polled cannot have told the peer anything: {:?}",
                quiet.map(|event| format!("{event:?}")),
            );
        })
        .await;
}

/// A served connection tells its peer when the caller closes it, too.
#[tokio::test]
async fn a_served_connection_also_closes_when_it_is_done() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let mut peer = LayerConnection::client(client_io, clock.clone(), Config::new())
                .expect("a bare QMux client");

            let server = ngnet_qmux_h3::serve(server_io, clock, |request| async move {
                let _ = drain(request.into_body()).await;
                ngnet_qmux_h3_tests::ok("answered")
            })
            .expect("serving");
            let serving = tokio::task::spawn_local(server);

            // The handshake first. A QMux endpoint that closed before its announcement had
            // been exchanged would be sending a record the peer cannot parse, and the
            // ending under test would be a protocol error of the test's own making.
            let handshake = async {
                while peer.peer_transport_params().is_none() {
                    let _ = poll_fn(|cx| peer.poll_next_event(cx)).await;
                }
            };
            timeout(LIMIT, handshake)
                .await
                .expect("the handshake must not hang");

            // A server closes when its peer does, so the peer has to go first. Its close is
            // the ordinary end of an HTTP/3 connection.
            let closing = async {
                let reason = CloseReason::application(H3_NO_ERROR, b"");
                poll_fn(|cx| peer.poll_close(cx, &reason)).await
            };
            timeout(LIMIT, closing)
                .await
                .expect("closing must not hang")
                .expect("the close must be written");

            let served = timeout(LIMIT, serving)
                .await
                .expect("the served connection must notice the close and return")
                .expect("the server task");
            assert!(
                served.is_ok(),
                "a peer closing politely is the end of the connection, not a failure: \
                 {served:?}",
            );
        })
        .await;
}
