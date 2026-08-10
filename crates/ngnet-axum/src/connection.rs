//! Driving one accepted connection's worth of requests through an axum [`Router`].
//!
//! This is the whole integration, and it is smaller than it looks like it should be. The
//! obvious design for "run axum on a different HTTP engine" is a pair of body adapters --
//! one wrapping the engine's request body so axum will accept it, one unwrapping axum's
//! response body so the engine will send it. There is no such thing in this file, because
//! neither is needed.
//!
//! `ngnet-h2`'s [`IncomingBody`] already *is* an [`http_body::Body`] whose `Data` is
//! [`Bytes`](bytes::Bytes), which is exactly the bound axum puts on the request bodies its
//! `Router` will serve. So `http::Request<IncomingBody>` is handed to the router unchanged.
//! In the other direction axum's own `Body` has `Data = Bytes` too, which is the bound
//! [`serve_shared_with`] needs to hand response octets to the transport without copying
//! them. The two stacks were written against the same two traits, and meet in the middle
//! with nothing in between.
//!
//! What is left is a closure: insert the peer address, clone the router, call it.
//!
//! [`Router`]: axum::Router
//! [`serve_shared_with`]: ngnet_h2::http::serve_shared_with

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use ngnet_h2::http::transport::TokioIo;
use ngnet_h2::http::{Config, Connection, IncomingBody, Result, serve_shared_with};
use tokio::net::TcpStream;
use tower_service::Service;

use crate::peer::PeerAddr;

/// Serves one accepted connection's requests with `router`, over cleartext HTTP/2.
///
/// The returned [`Connection`] is a future that must be polled to completion: it *is* the
/// connection, and the handlers run inside it rather than on tasks of their own. It resolves
/// when the peer goes away or the connection fails.
///
/// `peer` is required rather than optional. Every accepted TCP connection has one, and
/// making it an `Option` here would invent a hole in what handlers can rely on rather than
/// reflect one in the stack underneath.
///
/// # Errors
///
/// Fails if the HTTP/2 session cannot be created. This happens before the connection
/// exists, so there is no future to report it -- failures after this point are reported by
/// awaiting the returned connection.
pub fn serve_connection(
    stream: TcpStream,
    router: Router,
    peer: SocketAddr,
    config: Config,
) -> Result<Connection<impl Future<Output = Result<()>>>> {
    serve_shared_with(
        TokioIo::new(stream),
        move |mut request: http::Request<IncomingBody>| {
            // Handlers see the peer here rather than through `ConnectInfo`; see `PeerAddr`.
            request.extensions_mut().insert(PeerAddr(peer));

            // Cloning a `Router` bumps an `Arc` rather than rebuilding the routing table,
            // which is what makes calling it per request affordable. The clone is needed
            // because `Service::call` takes `&mut self` while this closure is `FnMut` and
            // the future it returns outlives the call.
            let mut router = router.clone();

            async move {
                match router.call(request).await {
                    Ok(response) => response,
                    // `Router`'s error type is uninhabited: routing failures are responses
                    // with a status, not errors. The empty match is not a shortcut for a
                    // panic -- it is the compiler agreeing there is no value to handle, and
                    // it stops compiling if axum ever gives the impl a real error type.
                    Err(never) => match never {},
                }
            }
        },
        config,
    )
}
