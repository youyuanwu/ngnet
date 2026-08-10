//! Turning an [`Origin`] into a live HTTP/2 connection.
//!
//! Small, because there is genuinely little to do: no TLS, no ALPN, no protocol negotiation
//! and no HTTP/1 fallback, since `ngnet-h2` speaks cleartext HTTP/2 and nothing else. What
//! `hyper-util`'s connector spends most of its size on — deciding which protocol both ends
//! agreed to — has no analogue here.
//!
//! # This is the layer that spawns
//!
//! `ngnet-h2` never spawns. It returns a driver future and lets the caller decide where it
//! runs, which is what makes it runtime-agnostic. That property stops here: this crate
//! requires tokio and spawns one task per connection, because the alternative is handing the
//! driver back to the caller, and a caller managing driver tasks is a caller managing
//! connections — the thing this crate exists to stop doing.

use bytes::Bytes;
use http_body::Body;
use ngnet_h2::http::Config;
use ngnet_h2::http::client::{SendRequest, handshake_shared_with};
use ngnet_h2::http::transport::TokioIo;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::error::{Context, Error};
use crate::origin::Origin;

/// Opens a connection to `origin` and puts its driver on a task.
///
/// Returns the handle to send requests through, and the [`JoinHandle`] of the driver, which
/// the pool keeps so that shutdown can wait for the connection to actually end.
///
/// # What can fail here, and what cannot
///
/// Only two things: the TCP connect (which includes name resolution), and construction of the
/// local protocol session. Both are genuine [`ErrorKind::Connect`](crate::ErrorKind::Connect)
/// failures — nothing reached a peer.
///
/// Notably *absent* is the HTTP/2 handshake. [`handshake_shared_with`] is synchronous and
/// fails only if the session cannot be built locally; the settings exchange happens
/// afterwards, on the driver. So a peer that accepts the TCP connection and then says nothing
/// produces a connection that looks perfectly good here and fails later, as an exchange. That
/// is not a wart to be papered over — see [`ErrorKind::Connect`](crate::ErrorKind::Connect)
/// for why reporting it as a connect failure would be actively harmful.
pub(crate) async fn dial<B>(
    origin: &Origin,
    config: Config,
) -> Result<(SendRequest<B>, JoinHandle<()>), Error>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    // `connect` resolves the name and tries the resolved addresses in turn, which is why
    // this crate does not iterate them itself. Reimplementing that — or layering
    // happy-eyeballs over it — would be this crate taking on the runtime's job in order to
    // do it slightly differently, with no evidence that the difference is wanted.
    //
    // The host is unbracketed by `Origin`, because `[::1]` is URI syntax and does not
    // resolve. That single detail is the difference between IPv6 working and every IPv6
    // origin failing as though the host were unreachable.
    let stream = TcpStream::connect((origin.host(), origin.port()))
        .await
        .map_err(|source| {
            Error::connect(Context::new(format!("connecting to {origin} failed"), source))
        })?;

    // Nagle off. Every write this stack makes is a deliberately framed HTTP/2 frame, so
    // coalescing them adds latency to a request that is already complete in order to wait for
    // one that may never come. hyper sets this for the same reason.
    //
    // A failure here is not fatal: the connection works, it is merely less prompt. Reporting
    // it as a connect failure would refuse a usable connection over a performance hint.
    let _ = stream.set_nodelay(true);

    let (handle, connection) =
        handshake_shared_with(TokioIo::new(stream), config).map_err(|source| {
            Error::connect(Context::new(
                format!("starting HTTP/2 to {origin} failed"),
                source,
            ))
        })?;

    // The driver's outcome is deliberately discarded. A connection failing is not news the
    // pool can deliver to anyone: it reaches the callers who had exchanges on it, through
    // those exchanges, which is where they can act on it. A pool that logged it here would be
    // reporting the same failure twice, in a place with no context about what was affected.
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    Ok((handle, driver))
}
