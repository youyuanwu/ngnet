//! Properties of the public API that are invisible to the other tests.
//!
//! Everything else in this suite drives the server over a real socket, which is the right
//! way to test behaviour but says nothing about whether the API can be *used* from outside.
//! A caller depending only on `ngnet-axum` is a situation no behavioural test reproduces,
//! because the tests are inside the crate and can reach anything.

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use ngnet_axum::{Config, Connection, EngineResult, serve_connection};
use tokio::net::TcpStream;

/// `serve_connection`'s return type can be written down by a caller (Rust API guideline
/// C-UNNAMEABLE).
///
/// The function returns `ngnet-h2`'s `Connection` wrapped in `ngnet-h2`'s `Result`. Both
/// were once reachable only through a crate a caller has no other reason to depend on,
/// which meant the function could be called but its result could not be stored in a struct,
/// returned from a function, or named in a signature. Re-exporting fixes it; this compiles
/// only while that stays true, and it deliberately uses *only* `ngnet_axum` paths.
#[allow(dead_code)]
fn the_connection_type_is_nameable(
    stream: TcpStream,
    router: Router,
    peer: SocketAddr,
    config: Config,
) -> EngineResult<Connection<impl Future<Output = EngineResult<()>>>> {
    serve_connection(stream, router, peer, config)
}

#[test]
fn the_public_types_compose_from_this_crate_alone() {
    // The assertion is the signature above; reaching here means it compiled.
}
