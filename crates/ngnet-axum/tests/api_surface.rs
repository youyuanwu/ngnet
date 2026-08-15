//! Properties of the public API that are invisible to the other tests.
//!
//! Everything else in this suite drives the server over a real socket, which is the right
//! way to test behaviour but says nothing about whether the API can be *used* from outside.
//! A caller depending only on `ngnet-axum` is a situation no behavioural test reproduces,
//! because the tests are inside the crate and can reach anything.

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use ngnet_axum::{Config, Connection, EngineResult, TokioIo, serve, serve_connection};
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
    serve_connection(TokioIo::new(stream), router, peer, config)
}

#[test]
fn the_public_types_compose_from_this_crate_alone() {
    // The assertion is the signature above; reaching here means it compiled.
}

/// SC-017: the same property holds for a transport that is not a TCP stream.
///
/// The signature above would compile even if the crate had stayed TCP-shaped, since a
/// `TcpStream` is what it always took. This one uses an in-memory pipe and an address type
/// that is not a socket address, so it compiles only while `serve_connection` is genuinely
/// generic over both -- and, as above, using only `ngnet_axum` paths.
#[allow(dead_code)]
fn the_connection_type_is_nameable_for_a_non_socket_transport(
    pipe: tokio::io::DuplexStream,
    router: Router,
    peer: String,
    config: Config,
) -> EngineResult<Connection<impl Future<Output = EngineResult<()>>>> {
    serve_connection(TokioIo::new(pipe), router, peer, config)
}

/// A third-party listener is writable using only this crate's public API.
///
/// `transports.rs` proves this behaviourally; this pins it as a compile-time property, so
/// that a change which made `FallibleListener` unimplementable from outside would fail here
/// even if the behavioural test were deleted.
#[allow(dead_code)]
fn a_listener_is_implementable_from_outside() {
    struct Outside;

    impl ngnet_axum::FallibleListener for Outside {
        type Io = TokioIo<tokio::io::DuplexStream>;
        type Addr = String;

        async fn accept(&mut self) -> std::io::Result<(Self::Io, Self::Addr)> {
            std::future::pending().await
        }
    }

    // And that wrapping it yields something `serve` accepts.
    let _ = |listener: ngnet_axum::RetryingListener<Outside>, router: Router| {
        serve(listener, router)
    };
}
